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
import std.runtime.*
import cactor_core.actor.{Actor, ActorRef, Props, ActorPath}
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
            supervise(userActor, defaultSupervisionStrategy)
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
                cell.terminateInCell()
                removeUserActor(cell)
            case SupervisionDirective.Escalate =>
                escalateFailure(cell, cause)
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
        // 创建升级失败消息并发送给系统Guardian
        // 这里需要与ActorSystem集成
        println("升级失败: Actor=${actor.path.toString()}, 原因=${cause}")
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
        let actor = props.create()
        actor.preStart()
        let actorPath = ActorPath("user-system", "${actorPathPrefix}/${name}")
        return SimpleUserActorRef(name, actor, { => props.create() }, actorPath, this)
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
                    println("创建用户Actor失败: ${e}")
                    MessageResult.Unhandled
                }
            case failureMsg: ActorFailureMessage =>
                try {
                    handleActorFailure(failureMsg.getFailure())
                    MessageResult.Handled
                } catch (e: Exception) {
                    println("处理Actor失败异常: ${e}")
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

/**
 * 用户ActorRef：Actor Cell 实现
 *
 * - 每个 Actor 拥有独立的并发邮箱（ConcurrentLinkedQueue）
 * - tell 只负责入队并按需调度，立即返回
 * - scheduled 标志保证同一时刻只有一个执行体在处理该 Actor 的消息，
 *   从而满足 Actor 模型“串行处理、单发送方有序”的语义
 * - 处理失败交给用户 Guardian 监督
 */
public class SimpleUserActorRef <: ActorRef {
    // 单次调度最多处理的消息数，超出后让出执行权，保证 Actor 间公平
    private static let THROUGHPUT: Int64 = 64

    private let actorName: String
    private var actor: Actor
    private let actorFactory: () -> Actor
    private let actorPath: ActorPath
    private let guardian: RuntimeUserGuardian
    private let mailbox: ConcurrentLinkedQueue<(Message, Option<String>)>
    private let scheduled: AtomicBool
    private let terminated: AtomicBool
    private let processedCount: AtomicInt64
    private let deadLetterCount: AtomicInt64
    private let restartCount: AtomicInt64

    public init(name: String, actor: Actor, path: ActorPath, guardian: RuntimeUserGuardian) {
        this(name, actor, { => actor }, path, guardian)
    }

    public init(name: String, actor: Actor, factory: () -> Actor, path: ActorPath, guardian: RuntimeUserGuardian) {
        this.actorName = name
        this.actor = actor
        this.actorFactory = factory
        this.actorPath = path
        this.guardian = guardian
        this.mailbox = ConcurrentLinkedQueue<(Message, Option<String>)>()
        this.scheduled = AtomicBool(false)
        this.terminated = AtomicBool(false)
        this.processedCount = AtomicInt64(0)
        this.deadLetterCount = AtomicInt64(0)
        this.restartCount = AtomicInt64(0)
    }

    public prop path: ActorPath {
        get() { actorPath }
    }

    public func tell(message: Message): Unit {
        tell(message, None<String>)
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        if (terminated.load()) {
            deadLetterCount.fetchAdd(1)
            return
        }
        mailbox.add((message, sender))
        trySchedule()
    }

    /** 已处理的消息数 */
    public func processed(): Int64 {
        processedCount.load()
    }

    /** 停止后被丢弃的消息数 */
    public func deadLetters(): Int64 {
        deadLetterCount.load()
    }

    /** 邮箱中待处理的消息数 */
    public func pending(): Int64 {
        mailbox.size
    }

    /** 被监督重启的次数 */
    public func restarts(): Int64 {
        restartCount.load()
    }

    /** Actor 是否已停止 */
    public func isTerminated(): Bool {
        terminated.load()
    }

    private func trySchedule(): Unit {
        if (scheduled.compareAndSwap(false, true)) {
            ActorCellDispatcher.execute(this)
        }
    }

    /** 由调度器工作线程调用 */
    func run(): Unit {
        var count: Int64 = 0
        while (count < THROUGHPUT) {
            match (mailbox.remove()) {
                case Some((message, sender)) =>
                    invoke(message, sender)
                    count++
                case None => break
            }
        }
        scheduled.store(false)
        // 释放调度权后再检查一次，防止与并发 tell 之间丢失唤醒
        if (mailbox.size > 0) {
            trySchedule()
        }
    }

    private func invoke(message: Message, sender: Option<String>): Unit {
        if (terminated.load()) {
            deadLetterCount.fetchAdd(1)
            return
        }
        if (let Some(restartMsg) <- (message as RestartMessage)) {
            doRestart(restartMsg.getReason())
            return
        }
        let context = SimpleUserActorContext(this, sender)
        try {
            actor.receive(message, context)
        } catch (e: Exception) {
            // 在本 Cell 内同步执行监督决策
            guardian.handleFailureInCell(this, e)
        }
        processedCount.fetchAdd(1)
        if (message is StopMessage) {
            terminateInCell()
        }
    }

    /**
     * 重启：旧实例 preRestart → 用工厂创建新实例 → 新实例 postRestart
     * 在 Actor Cell 内执行，与消息处理串行，邮箱中的后续消息由新实例处理
     */
    func restartInCell(reason: Exception): Unit {
        doRestart(reason)
    }

    /** 立即停止（在 Cell 内调用） */
    func terminateInCell(): Unit {
        if (!terminated.swap(true)) {
            try {
                actor.preStop()
                actor.postStop()
            } catch (e: Exception) {
                println("Actor ${actorName} 停止回调失败: ${e}")
            }
        }
    }

    private func doRestart(reason: Exception): Unit {
        try {
            actor.preRestart(reason)
        } catch (e: Exception) {
            println("Actor ${actorName} preRestart 失败: ${e}")
        }
        try {
            let fresh = actorFactory()
            actor = fresh
            fresh.postRestart(reason)
            restartCount.fetchAdd(1)
        } catch (e: Exception) {
            // 无法重建实例时停止该 Actor，避免继续用损坏的状态处理消息
            println("Actor ${actorName} 重启失败，已停止: ${e}")
            terminated.store(true)
        }
    }

    public func equals(other: ActorRef): Bool {
        match (other) {
            case userRef: SimpleUserActorRef =>
                actorName == userRef.actorName && actorPath.toString() == userRef.actorPath.toString()
            case _ => false
        }
    }

    public func hashCode(): Int32 {
        Int32(actorName.hashCode() & 0x7FFFFFFF)
    }
}

/**
 * 简化的用户ActorContext实现
 */
public class SimpleUserActorContext <: ActorContext {
    private let selfRef: ActorRef
    private let currentSender: Option<String>

    public init(self: ActorRef) {
        this.selfRef = self
        this.currentSender = None<String>
    }

    public init(self: ActorRef, sender: Option<String>) {
        this.selfRef = self
        this.currentSender = sender
    }

    public func sender(): Option<String> {
        currentSender
    }
}

/**
 * Actor Cell 调度器（进程级共享）
 *
 * 固定数量的工作线程（CPU 核数，至少 4、至多 64）从就绪队列取出 Cell 执行，
 * 避免每次调度都创建新线程。Cell 每次最多处理 THROUGHPUT 条消息后让出，保证公平。
 * 注意：在 receive 中执行阻塞操作会占用一个工作线程。
 */
public class ActorCellDispatcher {
    private static let readyQueue: LinkedBlockingQueue<SimpleUserActorRef> = LinkedBlockingQueue<SimpleUserActorRef>()
    private static let started: AtomicBool = AtomicBool(false)
    private static let workerCount: AtomicInt64 = AtomicInt64(0)
    private static let executedCount: AtomicInt64 = AtomicInt64(0)

    static func execute(cell: SimpleUserActorRef): Unit {
        ensureStarted()
        readyQueue.add(cell)
    }

    private static func ensureStarted(): Unit {
        if (started.load() || !started.compareAndSwap(false, true)) {
            return
        }
        var n = Int64(getProcessorCount())
        if (n < 4) {
            n = 4
        }
        if (n > 64) {
            n = 64
        }
        for (_ in 0..n) {
            spawn {
                workerLoop()
            }
        }
        workerCount.store(n)
    }

    private static func workerLoop(): Unit {
        while (true) {
            let cell = readyQueue.remove()
            try {
                cell.run()
            } catch (e: Exception) {
                println("Actor Cell 执行异常: ${e}")
            }
            executedCount.fetchAdd(1)
        }
    }

    /** 工作线程数（未启动时为 0） */
    public static func getWorkerCount(): Int64 {
        workerCount.load()
    }

    /** 已执行的调度次数 */
    public static func getExecutedCount(): Int64 {
        executedCount.load()
    }

    /** 等待执行的 Cell 数 */
    public static func getQueuedCount(): Int64 {
        readyQueue.size
    }
}
