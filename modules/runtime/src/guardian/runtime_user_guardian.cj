/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Runtime - User Guardian实现
 * 用户级Guardian，管理用户创建的Actor
 * 从Core层移动到Runtime层
 */
protected package cactor_runtime.guardian

import std.sync.*
import std.collection.*
import std.collection.concurrent.*
import std.time.*
import cactor_core.actor.{Actor, ActorRef, Props, ActorPath}
import cactor_foundation.logging.{Logging}
import cactor_core.message.{Message, MessageResult}
import cactor_core.context.{ActorContext}
import cactor_core.supervision.{SupervisionStrategy, DefaultSupervisionStrategy, SupervisionDirective, ActorFailure, BackoffSupervisorStrategy}
import cactor_runtime.supervision.{BackoffSupervisor}
import cactor_core.system.{ActorSystem}
import cactor_core.guardian.{GuardianType}

/**
 * Runtime层的用户Guardian实现
 * 管理用户创建的Actor
 */
public class RuntimeUserGuardian <: BaseGuardianActor {
    private let actorSystem: ActorSystem
    private let userActors: HashMap<String, ActorRef>
    private let defaultSupervisionStrategy: SupervisionStrategy
    private let actorPathPrefix: String
    // 缓存每个子Actor的BackoffSupervisor保持重试计数
    private let backoffSupervisors: HashMap<String, BackoffSupervisor>
    // 全部存活 Cell（含子 Actor）的路径索引，供 lookup / watch 使用
    private let cellsByPath: ConcurrentHashMap<String, SimpleUserActorRef> = ConcurrentHashMap<String, SimpleUserActorRef>()
    private let pathLock: Mutex = Mutex()
    // 死信监听器（写时复制）
    private var deadLetterListeners: Array<(Message, String) -> Unit> = Array<(Message, String) -> Unit>()
    private let deadLetterTotal: AtomicInt64 = AtomicInt64(0)
    // 顶层 Actor 自行停止（PoisonPill、MessageResult.Stop、监督 Stop 等）时通知运行时清理注册信息
    private var topLevelTerminatedHook: Option<(ActorRef) -> Unit> = None<(ActorRef) -> Unit>
    private let log = Logging.logger("cactor.guardian")

    public init(system: ActorSystem) {
        super(GuardianType.User, None<ActorRef>)  // 用户Guardian通常没有父Guardian
        this.actorSystem = system
        this.userActors = HashMap<String, ActorRef>()
        this.defaultSupervisionStrategy = DefaultSupervisionStrategy.oneForOne()
        this.actorPathPrefix = "/user"
        this.backoffSupervisors = HashMap<String, BackoffSupervisor>()
    }

    /**
     * 创建用户Actor
     */
    public func createUserActor(props: Props<Actor>, name: String): ActorRef {
        // 验证用户Actor名称
        if (!isValidUserActorName(name)) {
            throw Exception("非法的用户Actor名称: ${name}")
        }
        // 名称检查与登记在同一把锁内完成，避免并发创建同名 Actor
        synchronized(guardianLock) {
            if (userActors.contains(name)) {
                throw Exception("Actor名称已存在: ${name}")
            }
            let userActor = doCreateChild(props, name)
            userActors.add(name, userActor)
            childCounter.fetchAdd(1)
            supervise(userActor, props.getSupervisorStrategy() ?? defaultSupervisionStrategy)
            return userActor
        }
    }

    /**
     * 移除用户Actor并发送停止消息
     * @return 该 Actor 是否仍在本 Guardian 管理下（重复停止返回 false）
     */
    public func removeUserActor(child: ActorRef): Bool {
        let removed = synchronized(guardianLock) {
            var found: Option<String> = None
            for ((name, actorRef) in userActors) {
                if (actorRef.equals(child)) {
                    found = Some(name)
                    break
                }
            }
            match (found) {
                case Some(name) =>
                    userActors.remove(name)
                    supervisionStrategies.remove(child.path.toString())
                    backoffSupervisors.remove(child.path.toString())
                    stopCounter.fetchAdd(1)
                    true
                case None => false
            }
        }
        if (removed) {
            child.tell(StopMessage())
        }
        return removed
    }
    
    /**
     * 获取用户Actor
     */
    public func getUserActor(name: String): Option<ActorRef> {
        synchronized(guardianLock) {
            userActors.get(name)
        }
    }
    
    /**
     * 获取所有用户Actor
     */
    public func getAllUserActors(): Array<ActorRef> {
        synchronized(guardianLock) {
            let actorList = ArrayList<ActorRef>()
            for ((_, actorRef) in userActors) {
                actorList.add(actorRef)
            }
            return actorList.toArray()
        }
    }
    
    /**
     * 处理Actor失败
     */
    public func handleActorFailure(failure: ActorFailure): Unit {
        failureCounter.fetchAdd(1)
        let childPath = failure.actor.path.toString()
        let strategy = synchronized(guardianLock) {
            supervisionStrategies.get(childPath) ?? defaultSupervisionStrategy
        }
        let directive = strategy.decide(failure)
        executeSupervisionDirective(failure.actor, directive, failure.cause)
    }
    
    /**
     * 在 Actor Cell 内处理失败（与该 Actor 的消息处理串行）
     * Restart / Stop 立即生效，邮箱中尚未处理的消息由新实例处理或被丢弃
     */
    func handleFailureInCell(cell: SimpleUserActorRef, cause: Exception): Unit {
        failureCounter.fetchAdd(1)
        let key = cell.path.toString()
        let failure = ActorFailure(cell, cause, 1)
        let strategy = synchronized(guardianLock) {
            supervisionStrategies.get(key) ?? defaultSupervisionStrategy
        }
        match (strategy.decide(failure)) {
            case SupervisionDirective.Resume => ()
            case SupervisionDirective.Restart =>
                match (strategy) {
                    case _: BackoffSupervisorStrategy =>
                        // 退避重启由 BackoffSupervisor 按延迟调度
                        executeSupervisionDirective(cell, SupervisionDirective.Restart, cause)
                    case _ =>
                        restartCounter.fetchAdd(1)
                        cell.restartInCell(cause)
                }
            case SupervisionDirective.Stop =>
                // 先停止（包括子 Actor），停止完成后由 onCellTerminated 清理登记信息
                cell.terminateInCell()
            case SupervisionDirective.Escalate =>
                // 顶层 Actor 没有可以上报的父 Actor：停止它
                log.error("顶层 Actor ${cell.pathString} 上报失败（${cause}），已停止")
                cell.terminateInCell()
        }
    }

    /**
     * 执行监督指令
     */
    private func executeSupervisionDirective(actor: ActorRef, directive: SupervisionDirective, cause: Exception): Unit {
        match (directive) {
            case SupervisionDirective.Resume =>
                // 保留 Actor 状态，继续处理后续消息
                ()

            case SupervisionDirective.Restart =>
                let key = actor.path.toString()
                let backoff: Option<BackoffSupervisor> = synchronized(guardianLock) {
                    match (supervisionStrategies.get(key)) {
                        case Some(s: BackoffSupervisorStrategy) =>
                            // 获取/创建缓存的监督器以保持重试计数
                            match (backoffSupervisors.get(key)) {
                                case Some(existing) => Some(existing)
                                case None =>
                                    let created = BackoffSupervisor(s, this)
                                    backoffSupervisors.add(key, created)
                                    Some(created)
                            }
                        case _ => None<BackoffSupervisor>
                    }
                }
                match (backoff) {
                    case Some(bof) => bof.handleFailure(ActorFailure(actor, cause, 1))
                    case None => restartChild(actor, cause)
                }

            case SupervisionDirective.Stop =>
                removeUserActor(actor)

            case SupervisionDirective.Escalate =>
                escalateFailure(actor, cause)
        }
    }

    /**
     * 升级失败到系统Guardian
     */
    private func escalateFailure(actor: ActorRef, cause: Exception): Unit {
        log.error("顶层 Actor ${actor.path.toString()} 上报失败（${cause}），已停止")
        removeUserActor(actor)
    }
    
    /**
     * 验证用户Actor名称
     */
    private func isValidUserActorName(name: String): Bool {
        // 用户Actor名称不能以system-开头
        !name.startsWith("system-") && 
        !name.isEmpty() && 
        name.size <= 100 &&
        isValidActorNameCharacters(name)
    }
    
    /**
     * 验证Actor名称字符
     */
    private func isValidActorNameCharacters(name: String): Bool {
        // 简化实现：只检查长度和基本字符
        return name.size > 0 && name.size <= 100
    }
    
    // 实现抽象方法
    protected func doCreateChild(props: Props<Actor>, name: String): ActorRef {
        createCell(props, name, "${actorPathPrefix}/${name}", None<SimpleUserActorRef>)
    }

    /**
     * 创建 Cell：调用工厂创建实例并执行 preStart，然后登记路径。
     * preStart 抛出异常时创建失败，异常传给调用方。
     */
    func createCell(props: Props<Actor>, name: String, path: String, parent: Option<SimpleUserActorRef>): SimpleUserActorRef {
        let actor = props.create()
        actor.preStart()
        let cell = SimpleUserActorRef(name, actor, { => props.create() }, ActorPath("user-system", path), this,
            parent, props)
        // 第一条系统消息：保证 onStarted 先于任何用户消息执行
        cell.sendSystem(SysStarted())
        synchronized(pathLock) {
            cellsByPath.add(path, cell)
        }
        cell
    }

    /** 默认监督策略（父 Actor 未指定时使用）：OneForOne，异常时重启 */
    public func defaultStrategy(): SupervisionStrategy {
        defaultSupervisionStrategy
    }

    /** 按路径查找存活的 Actor，例如 "/user/parent/child" */
    public func lookup(path: String): Option<ActorRef> {
        match (lookupCell(path)) {
            case Some(c) => Some<ActorRef>(c)
            case None => None<ActorRef>
        }
    }

    func lookupCell(path: String): Option<SimpleUserActorRef> {
        match (cellsByPath.get(path)) {
            case Some(c) where !c.isTerminated() => Some(c)
            case _ => None<SimpleUserActorRef>
        }
    }

    /** 当前存活的 Actor 总数（含子 Actor） */
    public func liveCellCount(): Int64 {
        cellsByPath.size
    }

    /** Cell 完全停止后调用：注销路径；顶层 Actor 还要从 Guardian 中移除并通知运行时 */
    func onCellTerminated(cell: SimpleUserActorRef): Unit {
        synchronized(pathLock) {
            match (cellsByPath.get(cell.pathString)) {
                case Some(c) where refEq(c, cell) => cellsByPath.remove(cell.pathString)
                case _ => ()
            }
        }
        if (cell.parentCell.isNone()) {
            let removed = synchronized(guardianLock) {
                match (userActors.get(cell.getName())) {
                    case Some(r) where r.equals(cell) =>
                        userActors.remove(cell.getName())
                        supervisionStrategies.remove(cell.path.toString())
                        backoffSupervisors.remove(cell.path.toString())
                        stopCounter.fetchAdd(1)
                        true
                    case _ => false
                }
            }
            if (removed) {
                if (let Some(hook) <- topLevelTerminatedHook) {
                    hook(cell)
                }
            }
        }
    }

    /** 运行时注册：顶层 Actor 自行停止时回调，用于清理注册表和计数 */
    public func setTopLevelTerminatedHook(hook: (ActorRef) -> Unit): Unit {
        topLevelTerminatedHook = Some(hook)
    }

    // ---------------- 死信 ----------------

    /** 订阅死信：参数为消息和原目标路径。回调在发送方线程上执行，应尽快返回 */
    public func subscribeDeadLetters(listener: (Message, String) -> Unit): Unit {
        synchronized(pathLock) {
            let next = ArrayList<(Message, String) -> Unit>()
            next.add(all: deadLetterListeners)
            next.add(listener)
            deadLetterListeners = next.toArray()
        }
    }

    public func deadLetterCount(): Int64 {
        deadLetterTotal.load()
    }

    func publishDeadLetter(message: Message, path: String): Unit {
        deadLetterTotal.fetchAdd(1)
        let listeners = deadLetterListeners
        for (l in listeners) {
            try {
                l(message, path)
            } catch (e: Exception) {
                log.warn("死信监听器异常: ${e}")
            }
        }
        log.debug({ => "死信: ${message.messageType()} -> ${path}" })
    }
    
    protected func doStopChild(child: ActorRef): Unit {
        removeUserActor(child)
    }
    
    protected func doRestartChild(child: ActorRef, reason: Exception): Unit {
        // 重启通过邮箱串行执行：在 Actor Cell 中 preRestart → 重新创建实例 → postRestart
        child.tell(RestartMessage(reason))
    }
    
    // 重写Actor属性
    public prop name: String {
        get() { "user" }
    }
    
    public prop description: String {
        get() { "Runtime User Guardian - manages user-created actors" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case userMsg: CreateUserActorMessage =>
                try {
                    createUserActor(userMsg.getProps(), userMsg.getName())
                    MessageResult.Handled
                } catch (e: Exception) {
                    log.error("创建用户Actor失败: ${e}")
                    MessageResult.Unhandled
                }
            case failureMsg: ActorFailureMessage =>
                try {
                    handleActorFailure(failureMsg.getFailure())
                    MessageResult.Handled
                } catch (e: Exception) {
                    log.error("处理Actor失败异常: ${e}")
                    MessageResult.Unhandled
                }
            case _ => 
                // 委托给基类处理
                super.receive(message, context)
        }
    }
}

/**
 * 创建用户Actor的消息
 */
public class CreateUserActorMessage <: Message {
    private let props: Props<Actor>
    private let actorName: String

    public init(props: Props<Actor>, name: String) {
        this.props = props
        this.actorName = name
    }

    public func getProps(): Props<Actor> { props }
    public func getName(): String { actorName }

    public func getType(): String { "CreateUserActor" }
    public func getPayload(): String { "CreateUserActor(${actorName})" }
    public func messageType(): String { "CreateUserActor" }
}

/**
 * Actor失败消息
 */
public class ActorFailureMessage <: Message {
    private let failure: ActorFailure

    public init(failure: ActorFailure) {
        this.failure = failure
    }

    public func getFailure(): ActorFailure { failure }

    public func getType(): String { "ActorFailure" }
    public func getPayload(): String { "ActorFailure(${failure.actor.path.toString()})" }
    public func messageType(): String { "ActorFailure" }
}

/**
 * 停止消息
 */
public class StopMessage <: Message {
    public func getType(): String { "Stop" }
    public func getPayload(): String { "Stop" }
    public func messageType(): String { "Stop" }
}

/**
 * 重启消息
 */
public class RestartMessage <: Message {
    private let reason: Exception

    public init(reason: Exception) {
        this.reason = reason
    }

    public func getReason(): Exception { reason }

    public func getType(): String { "Restart" }
    public func getPayload(): String { "Restart(${reason})" }
    public func messageType(): String { "Restart" }
}
