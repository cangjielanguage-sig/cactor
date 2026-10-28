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
        
        let userActor = doCreateChild(props, name)
        userActors.add(name, userActor)
        
        // 应用用户级监督策略
        let supervisionStrategy = defaultSupervisionStrategy  // 简化实现，使用默认策略
        supervise(userActor, supervisionStrategy)
        
        return userActor
    }
    
    /**
     * 获取用户Actor
     */
    public func getUserActor(name: String): Option<ActorRef> {
        userActors.get(name)
    }
    
    /**
     * 获取所有用户Actor
     */
    public func getAllUserActors(): Array<ActorRef> {
        let actorList = ArrayList<ActorRef>()
        for ((_, actorRef) in userActors) {
            actorList.add(actorRef)
        }
        return actorList.toArray()
    }
    
    /**
     * 处理Actor失败
     */
    public func handleActorFailure(failure: ActorFailure): Unit {
        let childPath = failure.actor.path.toString()
        
        match (supervisionStrategies.get(childPath)) {
            case Some(strategy) =>
                let directive = strategy.decide(failure)
                executeSupervisionDirective(failure.actor, directive, failure.cause)
            case None =>
                // 使用默认策略
                let directive = defaultSupervisionStrategy.decide(failure)
                executeSupervisionDirective(failure.actor, directive, failure.cause)
        }
    }
    
    /**
     * 执行监督指令
     */
    private func executeSupervisionDirective(actor: ActorRef, directive: SupervisionDirective, cause: Exception): Unit {
        match (directive) {
            case SupervisionDirective.Resume =>
                println("恢复Actor: ${actor.path.toString()}")
                // 恢复Actor执行

            case SupervisionDirective.Restart =>
                println("重启Actor: ${actor.path.toString()}")
                match (supervisionStrategies.get(actor.path.toString())) {
                    case Some(s: BackoffSupervisorStrategy) =>
                        // 获取/创建缓存的监督器以保持重试计数
                        let key = actor.path.toString()
                        let bof = match (backoffSupervisors.get(key)) {
                            case Some(existing) => existing
                            case None =>
                                let created = BackoffSupervisor(s, this)
                                backoffSupervisors.add(key, created)
                                created
                        }
                        bof.handleFailure(ActorFailure(actor, cause, 1))
                    case _ =>
                        restartChild(actor, cause)
                }

            case SupervisionDirective.Stop =>
                println("停止Actor: ${actor.path.toString()}")
                stopChild(actor)

            case SupervisionDirective.Escalate =>
                println("升级失败到系统Guardian: ${actor.path.toString()}")
                // 将失败升级到系统Guardian
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
        
        // 创建用户ActorRef
        return createUserActorRef(actor, name, actorPath)
    }
    
    protected func doStopChild(child: ActorRef): Unit {
        println("停止用户Actor: ${child.path.toString()}")
        
        // 从用户Actor列表中移除
        for ((name, actorRef) in userActors) {
            if (actorRef.equals(child)) {
                userActors.remove(name)
                break
            }
        }
        
        // 执行Actor停止逻辑
        child.tell(StopMessage())
    }
    
    protected func doRestartChild(child: ActorRef, reason: Exception): Unit {
        println("重启用户Actor: ${child.path.toString()}, 原因: ${reason}")
        
        // 用户Actor重启逻辑
        // 1. 保存Actor状态
        // 2. 停止当前Actor
        // 3. 重新创建Actor
        // 4. 恢复Actor状态
        
        // 发送重启消息
        child.tell(RestartMessage(reason))
    }
    
    /**
     * 创建用户ActorRef
     */
    private func createUserActorRef(actor: Actor, name: String, path: ActorPath): ActorRef {
        // 这里应该与Runtime层集成，使用真正的邮箱和调度器
        // 最小改造：将guardian引用注入子ActorRef以路由失败
        return SimpleUserActorRef(name, actor, path, this)
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
    private let actor: Actor
    private let actorPath: ActorPath
    private let guardian: RuntimeUserGuardian
    private let mailbox: ConcurrentLinkedQueue<(Message, Option<String>)>
    private let scheduled: AtomicBool
    private let terminated: AtomicBool
    private let processedCount: AtomicInt64
    private let deadLetterCount: AtomicInt64

    public init(name: String, actor: Actor, path: ActorPath, guardian: RuntimeUserGuardian) {
        this.actorName = name
        this.actor = actor
        this.actorPath = path
        this.guardian = guardian
        this.mailbox = ConcurrentLinkedQueue<(Message, Option<String>)>()
        this.scheduled = AtomicBool(false)
        this.terminated = AtomicBool(false)
        this.processedCount = AtomicInt64(0)
        this.deadLetterCount = AtomicInt64(0)
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

    /** Actor 是否已停止 */
    public func isTerminated(): Bool {
        terminated.load()
    }

    private func trySchedule(): Unit {
        if (scheduled.compareAndSwap(false, true)) {
            spawn {
                run()
            }
        }
    }

    private func run(): Unit {
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
        let context = SimpleUserActorContext(this, sender)
        try {
            actor.receive(message, context)
        } catch (e: Exception) {
            // 将失败上报给用户Guardian进行监督处理
            let failure = ActorFailure(this, e, 1)
            guardian.receive(ActorFailureMessage(failure), context)
        }
        processedCount.fetchAdd(1)
        if (message is StopMessage) {
            terminated.store(true)
            try {
                actor.preStop()
                actor.postStop()
            } catch (e: Exception) {
                println("Actor ${actorName} 停止回调失败: ${e}")
            }
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
