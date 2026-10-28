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
import std.env.*
import std.time.*
import std.convert.*
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
    private let mailbox: CellMailbox
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
        this.mailbox = CellMailbox()
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
        mailbox.add(message, sender)
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
        mailbox.count()
    }

    /** 被监督重启的次数 */
    public func restarts(): Int64 {
        restartCount.load()
    }

    /** Actor 是否已停止 */
    public func isTerminated(): Bool {
        terminated.load()
    }

    // 先读后 CAS：已被调度时只做一次读，避免每次 tell 都写共享缓存行
    private func trySchedule(): Unit {
        if (!scheduled.load() && scheduled.compareAndSwap(false, true)) {
            ActorCellDispatcher.execute(this)
        }
    }

    /** 固定分配到的工作线程（亲和调度） */
    let affinity: Int64 = ActorCellDispatcher.nextAffinity()

    /** 由调度器工作线程调用 */
    func run(): Unit {
        var count: Int64 = 0
        while (count < THROUGHPUT) {
            match (mailbox.poll()) {
                case Some(message) =>
                    invoke(message, mailbox.lastSender)
                    count++
                case None =>
                    if (mailbox.nonEmpty()) {
                        // 生产者正在链接节点：让出处理器给它，之后由下面的 nonEmpty 检查重新调度
                        sleep(Duration.Zero)
                    }
                    break
            }
        }
        scheduled.store(false)
        // 释放调度权后再检查一次，防止与并发 tell 之间丢失唤醒
        if (mailbox.nonEmpty()) {
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
 * Actor 邮箱：Vyukov 侵入式 MPSC 链表
 * - 多个发送方 wait-free 入队（一次 swap + 一次 store），节点直接保存消息和发送方，不再额外分配元组
 * - 只有持有调度权的工作线程出队（由 scheduled 标志保证单消费者），因此出队无锁
 */
class MailboxNode {
    var message: Option<Message>
    var sender: Option<String>
    let next: AtomicOptionReference<MailboxNode> = AtomicOptionReference<MailboxNode>()
    init(message: Option<Message>, sender: Option<String>) {
        this.message = message
        this.sender = sender
    }
}

class CellMailbox {
    private let head: AtomicReference<MailboxNode>
    private var tail: MailboxNode

    init() {
        let stub = MailboxNode(None<Message>, None<String>)
        this.head = AtomicReference<MailboxNode>(stub)
        this.tail = stub
    }

    func add(message: Message, sender: Option<String>): Unit {
        let node = MailboxNode(Some(message), sender)
        let prev = head.swap(node)
        prev.next.store(node)
    }

    /** 最近一次 poll 出的消息的发送方（仅消费者使用） */
    var lastSender: Option<String> = None<String>

    /**
     * 仅由持有调度权的线程调用；出队节点成为新的哨兵。
     * 生产者已 swap 但尚未链接 next 时同样返回 None（此时 nonEmpty() 为 true），
     * 由调用方稍后重试，而不是原地自旋：在 M:N 调度下，自旋可能恰好挡住那个尚未完成链接的生产者。
     */
    func poll(): Option<Message> {
        match (tail.next.load()) {
            case Some(next) =>
                let message = next.message
                lastSender = next.sender
                next.message = None<Message>
                next.sender = None<String>
                tail = next
                message
            case None => None<Message>
        }
    }

    func nonEmpty(): Bool {
        !refEq(head.load(), tail)
    }

    /** 近似计数（O(n)，仅用于诊断） */
    func count(): Int64 {
        var n: Int64 = 0
        var cur = tail.next.load()
        while (let Some(node) <- cur) {
            n++
            cur = node.next.load()
        }
        n
    }
}

/**
 * 工作线程的就绪队列：与邮箱相同的 Vyukov MPSC 结构（任意线程提交，只有所属工作线程取出）。
 * std ConcurrentLinkedQueue 单次入队+出队约 190ns，这里约 30–40ns。
 */
class RunNode {
    var cell: Option<SimpleUserActorRef>
    let next: AtomicOptionReference<RunNode> = AtomicOptionReference<RunNode>()
    init(cell: Option<SimpleUserActorRef>) {
        this.cell = cell
    }
}

class RunQueue {
    private let head: AtomicReference<RunNode>
    private var tail: RunNode
    private let pendingCount: AtomicInt64 = AtomicInt64(0)

    init() {
        let stub = RunNode(None<SimpleUserActorRef>)
        this.head = AtomicReference<RunNode>(stub)
        this.tail = stub
    }

    func add(cell: SimpleUserActorRef): Unit {
        let node = RunNode(Some(cell))
        let prev = head.swap(node)
        prev.next.store(node)
    }

    /** 只由所属工作线程调用；遇到尚未链接完成的节点时返回 None，由工作线程让出后重试 */
    func remove(): Option<SimpleUserActorRef> {
        match (tail.next.load()) {
            case Some(next) =>
                let cell = next.cell
                next.cell = None<SimpleUserActorRef>
                tail = next
                cell
            case None => None<SimpleUserActorRef>
        }
    }

    func isEmpty(): Bool {
        refEq(head.load(), tail)
    }

    /** 近似长度（O(n)，仅诊断用） */
    prop size: Int64 {
        get() {
            var n: Int64 = 0
            var cur = tail.next.load()
            while (let Some(node) <- cur) {
                n++
                cur = node.next.load()
            }
            n
        }
    }
}

/**
 * 工作线程：独立的就绪队列 + 自旋后休眠
 */
class DispatcherWorker {
    let queue: RunQueue = RunQueue()
    let sleeping: AtomicBool = AtomicBool(false)
    let lock: Mutex = Mutex()
    let cond: Condition
    let executed: AtomicInt64 = AtomicInt64(0)

    init() {
        this.cond = synchronized(lock) { lock.condition() }
    }

    func submit(cell: SimpleUserActorRef): Unit {
        queue.add(cell)
        if (sleeping.load()) {
            synchronized(lock) {
                cond.notifyAll()
            }
        }
    }

    func loop(spinTime: Duration): Unit {
        var idle: Int64 = 0
        var idleSince = MonoTime.now()
        while (true) {
            match (queue.remove()) {
                case Some(cell) =>
                    idle = 0
                    try {
                        cell.run()
                    } catch (e: Exception) {
                        println("Actor Cell 执行异常: ${e}")
                    }
                    executed.fetchAdd(1)
                case None =>
                    if (idle == 0) {
                        idleSince = MonoTime.now()
                    }
                    idle++
                    if (MonoTime.now() - idleSince < spinTime) {
                        // 按时间自旋（而不是按次数），并让出处理器：
                        // 被本线程唤醒的仓颉线程（例如 ask 的等待方）可能排在当前处理器上
                        sleep(Duration.Zero)
                        continue
                    }
                    idle = 0
                    synchronized(lock) {
                        sleeping.store(true)
                        // 设置 sleeping 之后再检查一次，避免与 submit 之间丢失唤醒
                        if (queue.isEmpty()) {
                            cond.wait(timeout: Duration.millisecond * 10)
                        }
                        sleeping.store(false)
                    }
            }
        }
    }
}

/**
 * Actor Cell 调度器（进程级共享）
 *
 * 亲和调度（与 Akka AffinityPool 同类）：每个 Actor 固定分配给一个工作线程，
 * 每个工作线程有自己的就绪队列，调度时不再争用全局锁。
 * 工作线程空闲时先自旋（默认 50µs，环境变量 CACTOR_DISPATCHER_SPIN_US）再休眠，降低唤醒延迟。
 * 线程数默认取 CPU 核数的 1/4（4–64；发送方、I/O 等其他仓颉线程也需要处理器），可用环境变量 CACTOR_DISPATCHER_THREADS 覆盖。
 * 注意：在 receive 中执行阻塞操作会占用工作线程，并阻塞同一线程上的其他 Actor。
 */
public class ActorCellDispatcher {
    private static let started: AtomicBool = AtomicBool(false)
    private static let ready: AtomicBool = AtomicBool(false)
    private static var workers: Array<DispatcherWorker> = Array<DispatcherWorker>()
    private static let affinitySeq: AtomicInt64 = AtomicInt64(0)
    private static let SPIN_US_DEFAULT: Int64 = 50

    static func nextAffinity(): Int64 {
        affinitySeq.fetchAdd(1)
    }

    static func execute(cell: SimpleUserActorRef): Unit {
        if (!ready.load()) {
            ensureStarted()
        }
        workers[cell.affinity % workers.size].submit(cell)
    }

    private static func configuredThreads(): Int64 {
        if (let Some(v) <- getVariable("CACTOR_DISPATCHER_THREADS")) {
            if (let Some(n) <- Int64.tryParse(v)) {
                if (n > 0) {
                    return n
                }
            }
        }
        var n = Int64(getProcessorCount()) / 4
        if (n < 4) {
            n = 4
        }
        if (n > 64) {
            n = 64
        }
        n
    }

    private static func configuredSpin(): Duration {
        if (let Some(v) <- getVariable("CACTOR_DISPATCHER_SPIN_US")) {
            if (let Some(n) <- Int64.tryParse(v)) {
                if (n >= 0) {
                    return Duration.microsecond * n
                }
            }
        }
        Duration.microsecond * SPIN_US_DEFAULT
    }

    private static func ensureStarted(): Unit {
        if (started.compareAndSwap(false, true)) {
            let n = configuredThreads()
            let spin = configuredSpin()
            let ws = Array<DispatcherWorker>(n, { _ => DispatcherWorker() })
            workers = ws
            for (w in ws) {
                spawn {
                    w.loop(spin)
                }
            }
            ready.store(true)
        } else {
            while (!ready.load()) {
                sleep(Duration.Zero)
            }
        }
    }

    /** 工作线程数（未启动时为 0） */
    public static func getWorkerCount(): Int64 {
        if (ready.load()) { workers.size } else { 0 }
    }

    /** 已执行的调度次数 */
    public static func getExecutedCount(): Int64 {
        var total: Int64 = 0
        if (ready.load()) {
            for (w in workers) {
                total += w.executed.load()
            }
        }
        total
    }

    /** 等待执行的 Cell 数 */
    public static func getQueuedCount(): Int64 {
        var total: Int64 = 0
        if (ready.load()) {
            for (w in workers) {
                total += w.queue.size
            }
        }
        total
    }
}
