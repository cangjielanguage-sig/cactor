/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Runtime - Actor Cell
 *
 * 对标 Akka 的 ActorCell：
 * - 用户消息邮箱（无界无锁 / 有界 / 优先级）+ 独立的系统消息队列（优先处理）
 * - 父子层级与监督：子 Actor 失败后挂起并通知父 Actor，由父 Actor 的策略决定 Resume / Restart / Stop / Escalate
 * - 停止协议：先停止全部子 Actor，全部结束后才执行 postStop，再通知父 Actor 与监视者
 * - 死亡监视（watch / unwatch / Terminated）、行为切换（become / unbecome）、
 *   接收超时、Actor 级定时器、Stash、PoisonPill / Kill
 *
 * 线程模型：同一时刻只有一个调度线程执行某个 Cell（scheduled 标志），
 * 因此除系统消息队列和邮箱外，Cell 的字段都只在 Cell 线程内访问，不需要加锁。
 */
protected package cactor_runtime.guardian

import std.sync.*
import std.collection.*
import std.runtime.*
import std.env.*
import std.time.*
import std.convert.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult, AutoReceivedMessage}
import cactor_foundation.logging.{Logger, Logging, LogLevel}

let cellLog = Logging.logger("cactor.cell")

// ======================= 系统消息 =======================

abstract class SysMsg {}

/**
 * 系统消息队列的节点：无锁栈（Treiber stack），出队时整体取下并反转为先进先出。
 * 系统消息很少，用无锁栈代替“互斥锁 + ArrayList”，每个 Cell 省下约 200 字节。
 */
class SysNode {
    let msg: SysMsg
    let next: Option<SysNode>

    init(msg: SysMsg, next: Option<SysNode>) {
        this.msg = msg
        this.next = next
    }
}

class SysClosed <: SysMsg {}

/** Cell 终止后栈顶被换成这个哨兵，之后发来的系统消息按“死亡后”规则处理 */
let SYS_CLOSED: SysNode = SysNode(SysClosed(), None<SysNode>)

/** 标记 Actor 调度线程：系统终止时据此判断调用方是否就在某个 Actor 中 */
let actorThreadFlag: ThreadLocal<Bool> = ThreadLocal<Bool>()

func markActorThread(): Unit {
    actorThreadFlag.set(true)
}

/** 当前线程是否是 Actor 调度线程（在 Actor 的消息处理中） */
public func isActorThread(): Bool {
    actorThreadFlag.get() ?? false
}

class SysTerminate <: SysMsg {}

/** 创建后的第一条系统消息：在 Cell 线程中调用 Actor.onStarted */
class SysStarted <: SysMsg {}

class SysResume <: SysMsg {}

class SysRecreate <: SysMsg {
    let cause: Exception
    init(cause: Exception) { this.cause = cause }
}

class SysWatch <: SysMsg {
    let watcher: SimpleUserActorRef
    init(watcher: SimpleUserActorRef) { this.watcher = watcher }
}

class SysUnwatch <: SysMsg {
    let watcher: SimpleUserActorRef
    init(watcher: SimpleUserActorRef) { this.watcher = watcher }
}

class SysDeathNotification <: SysMsg {
    let actor: SimpleUserActorRef
    init(actor: SimpleUserActorRef) { this.actor = actor }
}

class SysChildFailed <: SysMsg {
    let child: SimpleUserActorRef
    let cause: Exception
    init(child: SimpleUserActorRef, cause: Exception) {
        this.child = child
        this.cause = cause
    }
}

class SysChildTerminated <: SysMsg {
    let child: SimpleUserActorRef
    init(child: SimpleUserActorRef) { this.child = child }
}

class SysReceiveTimeout <: SysMsg {
    let generation: Int64
    init(generation: Int64) { this.generation = generation }
}

/** 定时器投递的包装消息：投递时校验代次，已取消或已被替换的定时器消息会被丢弃 */
class TimerEnvelope <: AutoReceivedMessage & NotInfluenceReceiveTimeout {
    let key: String
    let generation: Int64
    let payload: Message
    init(key: String, generation: Int64, payload: Message) {
        this.key = key
        this.generation = generation
        this.payload = payload
    }
    public func messageType(): String { "TimerEnvelope" }
    public func priority(): Int32 { payload.priority() }
    public func messageId(): String { payload.messageId() }
    public func timestamp(): Int64 { payload.timestamp() }
}

/** Stash / 优先级邮箱中保存的一条消息 */
class Envelope {
    let message: Message
    let sender: Option<String>
    let senderRef: Option<ActorRef>
    init(message: Message, sender: Option<String>, senderRef: Option<ActorRef>) {
        this.message = message
        this.sender = sender
        this.senderRef = senderRef
    }
}

/** Actor 请求重启（receive 返回 MessageResult.Restart） */
class RestartRequestedException <: Exception {
    init() { super("receive 返回 MessageResult.Restart") }
}

/** Actor 请求上报（receive 返回 MessageResult.Escalate） */
class EscalateRequestedException <: Exception {
    init() { super("receive 返回 MessageResult.Escalate") }
}

// ======================= 邮箱 =======================

/**
 * 无界邮箱：Vyukov 侵入式 MPSC 链表
 * - 多个发送方 wait-free 入队（一次 swap + 一次 store），节点直接保存消息和发送方，不额外分配元组
 * - 只有持有调度权的工作线程出队（由 scheduled 标志保证单消费者），因此出队无锁
 */
class MailboxNode {
    var message: Option<Message>
    var sender: Option<String>
    var senderRef: Option<ActorRef>
    let next: AtomicOptionReference<MailboxNode> = AtomicOptionReference<MailboxNode>()
    init(message: Option<Message>, sender: Option<String>, senderRef: Option<ActorRef>) {
        this.message = message
        this.sender = sender
        this.senderRef = senderRef
    }
}

class CellMailbox {
    private let head: AtomicReference<MailboxNode>
    private var tail: MailboxNode

    init() {
        let stub = MailboxNode(None<Message>, None<String>, None<ActorRef>)
        this.head = AtomicReference<MailboxNode>(stub)
        this.tail = stub
    }

    func add(message: Message, sender: Option<String>, senderRef: Option<ActorRef>): Unit {
        let node = MailboxNode(Some(message), sender, senderRef)
        let prev = head.swap(node)
        prev.next.store(node)
    }

    /** 最近一次 poll 出的消息的发送方（仅消费者使用） */
    var lastSender: Option<String> = None<String>
    var lastSenderRef: Option<ActorRef> = None<ActorRef>

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
                lastSenderRef = next.senderRef
                next.message = None<Message>
                next.sender = None<String>
                next.senderRef = None<ActorRef>
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
 * 优先级邮箱：按 Message.priority() 从高到低处理，同优先级先进先出。
 * 列表按优先级升序保存，新消息插在同优先级元素之前，因此 removeLast 取到的是最高优先级里最早的消息。
 */
class PriorityCellMailbox {
    private let lock = Mutex()
    private let items = ArrayList<Envelope>()
    private let size = AtomicInt64(0)

    func add(message: Message, sender: Option<String>, senderRef: Option<ActorRef>): Unit {
        let env = Envelope(message, sender, senderRef)
        let p = message.priority()
        synchronized(lock) {
            // 第一个 priority >= p 的位置
            var lo: Int64 = 0
            var hi: Int64 = items.size
            while (lo < hi) {
                let mid = (lo + hi) / 2
                if (items[mid].message.priority() < p) {
                    lo = mid + 1
                } else {
                    hi = mid
                }
            }
            items.add(env, at: lo)
            size.fetchAdd(1)
        }
    }

    func poll(): Option<Envelope> {
        synchronized(lock) {
            if (items.size == 0) {
                return None<Envelope>
            }
            size.fetchAdd(-1)
            items.remove(at: items.size - 1)
        }
    }

    func nonEmpty(): Bool {
        size.load() > 0
    }

    func count(): Int64 {
        size.load()
    }
}

// ======================= Actor Cell =======================

let STATE_RUNNING: Int64 = 0
let STATE_SUSPENDED: Int64 = 1
let STATE_TERMINATING: Int64 = 2
let STATE_TERMINATED: Int64 = 3

let MAILBOX_UNBOUNDED: Int64 = 0
let MAILBOX_BOUNDED: Int64 = 1
let MAILBOX_PRIORITY: Int64 = 2

/**
 * 用户 Actor 的引用与执行单元（Actor Cell）
 */
public class SimpleUserActorRef <: RunLink & ActorRef {
    private let actorName: String
    private var actor: Actor
    private let actorFactory: () -> Actor
    private let actorPath: ActorPath
    let pathString: String
    let guardian: RuntimeUserGuardian
    let parentCell: Option<SimpleUserActorRef>
    let childStrategy: Option<SupervisionStrategy>
    private let executor: CellExecutor

    // ---- 邮箱（任意线程写） ----
    private let mailbox: CellMailbox
    private let mailboxKind: Int64
    private let capacity: Int64
    private let overflow: MailboxOverflow
    private let boundedCount: AtomicInt64
    private let priorityMailboxValue: Option<PriorityCellMailbox>
    // 只在 mailboxKind == MAILBOX_PRIORITY 时使用
    private prop priorityMailbox: PriorityCellMailbox {
        get() {
            priorityMailboxValue.getOrThrow()
        }
    }

    // ---- 系统消息队列（任意线程写，加锁；系统消息很少） ----
    private let sysHead: AtomicOptionReference<SysNode>

    // ---- 调度与状态 ----
    private let scheduled: AtomicBool
    private let terminated: AtomicBool
    private let processedCount: AtomicInt64
    private let deadLetterCount: AtomicInt64
    private let restartCount: AtomicInt64
    private let unhandledCount: AtomicInt64

    // ---- 只在 Cell 线程内访问 ----
    private var state: Int64 = STATE_RUNNING
    // 容量 0：大多数 Actor 没有子 Actor / 不监视别人，默认容量的 HashMap 每个约 900 字节
    private let childMap: HashMap<String, SimpleUserActorRef> = HashMap<String, SimpleUserActorRef>(0)
    private var anonymousSeq: Int64 = 0
    private let watchers: ArrayList<SimpleUserActorRef> = ArrayList<SimpleUserActorRef>(0)
    private let watching: HashMap<String, SimpleUserActorRef> = HashMap<String, SimpleUserActorRef>(0)
    private let behaviors: ArrayList<Receive> = ArrayList<Receive>(0)
    private let stashBuffer: ArrayList<Envelope> = ArrayList<Envelope>(0)
    private var unstashed: ArrayList<Envelope> = ArrayList<Envelope>(0)
    private var unstashIndex: Int64 = 0
    private var currentMessage: Option<Message> = None<Message>
    private var currentSender: Option<String> = None<String>
    private var currentSenderRef: Option<ActorRef> = None<ActorRef>
    private var receiveTimeoutValue: Duration = Duration.Zero
    private var lastActivity: MonoTime = MonoTime.now()
    private var receiveTimeoutGeneration: Int64 = 0
    private var receiveTimeoutTimer: Option<Timer> = None<Timer>
    private var cellTimersValue: Option<CellTimers> = None<CellTimers>
    private var contextValue: Option<CellContext> = None<CellContext>

    /** 固定分配到的工作线程（亲和调度） */
    let affinity: Int64

    public init(name: String, actor: Actor, path: ActorPath, guardian: RuntimeUserGuardian) {
        this(name, actor, { => actor }, path, guardian)
    }

    public init(name: String, actor: Actor, factory: () -> Actor, path: ActorPath, guardian: RuntimeUserGuardian) {
        this(name, actor, factory, path, guardian, None<SimpleUserActorRef>, PropsFactory.create<Actor>(factory))
    }

    init(name: String, actor: Actor, factory: () -> Actor, path: ActorPath, guardian: RuntimeUserGuardian,
        parent: Option<SimpleUserActorRef>, props: Props<Actor>) {
        this.actorName = name
        this.actor = actor
        this.actorFactory = factory
        this.actorPath = path
        this.pathString = path.getPath()
        this.guardian = guardian
        this.parentCell = parent
        this.childStrategy = props.getSupervisorStrategy()
        this.executor = ActorCellDispatcher.forName(props.getDispatcher())
        this.affinity = executor.nextAffinity()
        this.mailbox = CellMailbox()
        this.mailboxKind = match (props.getMailboxType()) {
            case "bounded" => if (props.getMailboxCapacity() > 0) { MAILBOX_BOUNDED } else { MAILBOX_UNBOUNDED }
            case "priority" => MAILBOX_PRIORITY
            case _ => MAILBOX_UNBOUNDED
        }
        this.capacity = props.getMailboxCapacity()
        this.overflow = props.getMailboxOverflow()
        this.boundedCount = AtomicInt64(0)
        this.priorityMailboxValue = if (mailboxKind == MAILBOX_PRIORITY) { Some(PriorityCellMailbox()) } else { None<PriorityCellMailbox> }
        this.sysHead = AtomicOptionReference<SysNode>()
        this.scheduled = AtomicBool(false)
        this.terminated = AtomicBool(false)
        this.processedCount = AtomicInt64(0)
        this.deadLetterCount = AtomicInt64(0)
        this.restartCount = AtomicInt64(0)
        this.unhandledCount = AtomicInt64(0)
    }

    public prop path: ActorPath {
        get() { actorPath }
    }

    public func getName(): String {
        actorName
    }

    // ---------------- 发送 ----------------

    public func tell(message: Message): Unit {
        enqueue(message, None<String>, None<ActorRef>)
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        enqueue(message, sender, None<ActorRef>)
    }

    public func tell(message: Message, sender: ActorRef): Unit {
        enqueue(message, None<String>, Some(sender))
    }

    private func enqueue(message: Message, sender: Option<String>, senderRef: Option<ActorRef>): Unit {
        if (terminated.load()) {
            deadLetter(message)
            return
        }
        if (mailboxKind == MAILBOX_UNBOUNDED) {
            mailbox.add(message, sender, senderRef)
        } else if (mailboxKind == MAILBOX_BOUNDED) {
            if (!reserveSlot()) {
                deadLetter(message)
                match (overflow) {
                    case MailboxOverflow.Fail => throw MailboxFullException("Actor ${pathString} 的邮箱已满（容量 ${capacity}）")
                    case _ => ()
                }
                return
            }
            mailbox.add(message, sender, senderRef)
        } else {
            priorityMailbox.add(message, sender, senderRef)
        }
        trySchedule()
    }

    /** 有界邮箱占位；Block 策略下等待空位直到超时 */
    private func reserveSlot(): Bool {
        if (boundedCount.fetchAdd(1) < capacity) {
            return true
        }
        boundedCount.fetchAdd(-1)
        match (overflow) {
            case MailboxOverflow.Block(timeout) =>
                let deadline = MonoTime.now() + timeout
                var spins: Int64 = 0
                while (MonoTime.now() < deadline && !terminated.load()) {
                    if (boundedCount.fetchAdd(1) < capacity) {
                        return true
                    }
                    boundedCount.fetchAdd(-1)
                    spins++
                    // 先让出处理器，之后短暂休眠，避免空转占用 CPU
                    sleep(if (spins < 64) { Duration.Zero } else { Duration.microsecond * 50 })
                }
                false
            case _ => false
        }
    }

    func deadLetter(message: Message): Unit {
        deadLetterCount.fetchAdd(1)
        guardian.publishDeadLetter(message, pathString)
    }

    /** 投递系统消息；目标已停止时按“死亡后”规则处理（例如对已停止的 Actor 发起 watch 会立即收到通知） */
    func sendSystem(message: SysMsg): Unit {
        while (true) {
            let cur = sysHead.load()
            if (let Some(c) <- cur) {
                if (refEq(c, SYS_CLOSED)) {
                    handleSystemAfterDeath(message)
                    return
                }
            }
            if (sysHead.compareAndSwap(cur, SysNode(message, cur))) {
                break
            }
        }
        trySchedule()
    }

    private func hasSystemMessages(): Bool {
        match (sysHead.load()) {
            case Some(c) => !refEq(c, SYS_CLOSED)
            case None => false
        }
    }

    /** 取下栈中全部消息并按发送顺序返回；closing 为 true 时同时关闭队列 */
    private func takeSystemMessages(closing: Bool): ArrayList<SysMsg> {
        let taken = if (closing) {
            sysHead.swap(SYS_CLOSED)
        } else {
            sysHead.swap(None<SysNode>)
        }
        let result = ArrayList<SysMsg>(0)
        var cur = taken
        while (let Some(node) <- cur) {
            if (refEq(node, SYS_CLOSED)) {
                // 只有 Cell 线程会关闭队列，正常不会走到这里；保持关闭状态
                sysHead.store(SYS_CLOSED)
                break
            }
            result.add(node.msg)
            cur = node.next
        }
        result.reverse()
        result
    }

    private func handleSystemAfterDeath(message: SysMsg): Unit {
        match (message) {
            case w: SysWatch => w.watcher.sendSystem(SysDeathNotification(this))
            case _ => ()
        }
    }

    // ---------------- 诊断 ----------------

    /** 已处理的消息数 */
    public func processed(): Int64 {
        processedCount.load()
    }

    /** 被丢弃的消息数（停止后收到、有界邮箱已满等） */
    public func deadLetters(): Int64 {
        deadLetterCount.load()
    }

    /** 邮箱中待处理的消息数 */
    public func pending(): Int64 {
        if (mailboxKind == MAILBOX_PRIORITY) {
            priorityMailbox.count()
        } else {
            mailbox.count()
        }
    }

    /** 被监督重启的次数 */
    public func restarts(): Int64 {
        restartCount.load()
    }

    /** receive 返回 Unhandled 的次数 */
    public func unhandled(): Int64 {
        unhandledCount.load()
    }

    /** Actor 是否已完全停止 */
    public func isTerminated(): Bool {
        terminated.load()
    }

    // ---------------- 调度 ----------------

    // 先读后 CAS：已被调度时只做一次读，避免每次 tell 都写共享缓存行
    private func trySchedule(): Unit {
        if (!scheduled.load() && scheduled.compareAndSwap(false, true)) {
            executor.execute(this)
        }
    }

    private func userNonEmpty(): Bool {
        if (mailboxKind == MAILBOX_PRIORITY) {
            priorityMailbox.nonEmpty()
        } else {
            mailbox.nonEmpty()
        }
    }

    private func hasUnstashed(): Bool {
        unstashIndex < unstashed.size
    }

    /** 由调度器工作线程调用 */
    func run(): Unit {
        let throughput = executor.throughput()
        var count: Int64 = 0
        while (true) {
            if (hasSystemMessages()) {
                processSystemMessages()
            }
            if (state != STATE_RUNNING || count >= throughput) {
                break
            }
            if (hasUnstashed()) {
                let env = unstashed[unstashIndex]
                unstashIndex++
                if (unstashIndex >= unstashed.size) {
                    unstashed = ArrayList<Envelope>(0)
                    unstashIndex = 0
                }
                invoke(env.message, env.sender, env.senderRef)
                count++
                continue
            }
            if (mailboxKind == MAILBOX_PRIORITY) {
                match (priorityMailbox.poll()) {
                    case Some(env) =>
                        invoke(env.message, env.sender, env.senderRef)
                        count++
                    case None => break
                }
            } else {
                match (mailbox.poll()) {
                    case Some(message) =>
                        if (mailboxKind == MAILBOX_BOUNDED) {
                            boundedCount.fetchAdd(-1)
                        }
                        invoke(message, mailbox.lastSender, mailbox.lastSenderRef)
                        count++
                    case None =>
                        if (mailbox.nonEmpty()) {
                            // 生产者正在链接节点：让出处理器给它，之后由下面的检查重新调度
                            sleep(Duration.Zero)
                        }
                        break
                }
            }
        }
        scheduled.store(false)
        // 释放调度权后再检查一次，防止与并发 tell 之间丢失唤醒
        if (hasSystemMessages() || (state == STATE_RUNNING && (userNonEmpty() || hasUnstashed()))) {
            trySchedule()
        }
    }

    // ---------------- 用户消息处理 ----------------

    private func invoke(message: Message, sender: Option<String>, senderRef: Option<ActorRef>): Unit {
        currentMessage = Some(message)
        currentSender = sender
        currentSenderRef = senderRef
        var payload = message
        // 只对控制消息做具体类型匹配（普通消息只付出这一次类型检查）
        let control = message is AutoReceivedMessage
        if (control) {
            match (message) {
                case te: TimerEnvelope =>
                    if (!timersImpl().accept(te)) {
                        clearCurrent()
                        return
                    }
                    payload = te.payload
                    currentMessage = Some(payload)
                case _: PoisonPill =>
                    processedCount.fetchAdd(1)
                    clearCurrent()
                    initiateTerminate()
                    return
                case _: StopMessage =>
                    // system.stop：在已入队的消息之后停止，不交给用户的 receive
                    processedCount.fetchAdd(1)
                    clearCurrent()
                    initiateTerminate()
                    return
                case _: Kill =>
                    processedCount.fetchAdd(1)
                    clearCurrent()
                    handleFailure(ActorKilledException(pathString))
                    return
                case restart: RestartMessage =>
                    // 兼容旧路径：guardian.restartChild / BackoffSupervisor 通过邮箱请求重启
                    clearCurrent()
                    doRestart(restart.getReason())
                    return
                case _ => ()
            }
        }
        try {
            let result = if (behaviors.size > 0) {
                behaviors[behaviors.size - 1](payload, contextImpl())
            } else {
                actor.receive(payload, contextImpl())
            }
            match (result) {
                case MessageResult.Handled => ()
                case MessageResult.Unhandled =>
                    unhandledCount.fetchAdd(1)
                    if (payload is Terminated) {
                        // 与 Akka 一致：监视者不处理 Terminated 视为失败
                        let t = (payload as Terminated).getOrThrow()
                        throw DeathPactException(t.actor.path.toString())
                    }
                case MessageResult.Stop => initiateTerminate()
                case MessageResult.Restart => handleFailure(RestartRequestedException())
                case MessageResult.Escalate => handleFailure(EscalateRequestedException())
            }
        } catch (e: Exception) {
            handleFailure(e)
        }
        processedCount.fetchAdd(1)
        if (receiveTimeoutValue > Duration.Zero && !(payload is NotInfluenceReceiveTimeout)) {
            lastActivity = MonoTime.now()
        }
        clearCurrent()
    }

    private func clearCurrent(): Unit {
        currentMessage = None<Message>
        currentSender = None<String>
        currentSenderRef = None<ActorRef>
    }

    // ---------------- 失败与监督 ----------------

    private func handleFailure(cause: Exception): Unit {
        if (state == STATE_TERMINATING || state == STATE_TERMINATED) {
            return
        }
        match (parentCell) {
            case Some(parent) =>
                // 子 Actor：挂起，等待父 Actor 的决定
                state = STATE_SUSPENDED
                parent.sendSystem(SysChildFailed(this, cause))
            case None =>
                // 顶层 Actor：由用户 Guardian 在本 Cell 内同步决定
                guardian.handleFailureInCell(this, cause)
        }
    }

    /** 父 Actor 处理子 Actor 的失败（在父 Actor 的 Cell 线程中执行） */
    private func handleChildFailed(child: SimpleUserActorRef, cause: Exception): Unit {
        match (childMap.get(child.getName())) {
            case Some(c) where refEq(c, child) => ()
            case _ => return // 已经停止或已被替换
        }
        let strategy: SupervisionStrategy = child.childStrategy ?? (actor.supervisorStrategy() ?? guardian.defaultStrategy())
        let directive = try {
            strategy.decide(ActorFailure(child, cause, 1))
        } catch (e: Exception) {
            cellLog.error("监督策略执行异常，停止子 Actor ${child.pathString}: ${e}")
            SupervisionDirective.Stop
        }
        let targets = if (strategy.appliesToAllChildren()) {
            childMap.values().toArray()
        } else {
            [child]
        }
        cellLog.error("Actor ${child.pathString} 失败：${cause}，监督指令：${directiveName(directive)}")
        match (directive) {
            case SupervisionDirective.Resume => child.sendSystem(SysResume())
            case SupervisionDirective.Restart =>
                for (t in targets) {
                    t.sendSystem(SysRecreate(cause))
                }
            case SupervisionDirective.Stop =>
                for (t in targets) {
                    t.sendSystem(SysTerminate())
                }
            case SupervisionDirective.Escalate =>
                // 父 Actor 自身以同样的原因失败，交给它的父 Actor 处理
                handleFailure(cause)
        }
    }

    /** 重启：旧实例 preRestart → 停止子 Actor、取消定时器 → 用工厂创建新实例 → postRestart */
    private func doRestart(reason: Exception): Unit {
        try {
            actor.preRestart(reason)
        } catch (e: Exception) {
            cellLog.warn("Actor ${pathString} preRestart 失败: ${e}")
        }
        // 与 Akka 默认 preRestart 一致：重启时停止全部子 Actor
        for (c in childMap.values()) {
            c.sendSystem(SysTerminate())
        }
        childMap.clear()
        timersImpl().cancelAll()
        cancelReceiveTimeoutInternal()
        behaviors.clear()
        // 暂存的消息放回队首，由新实例处理
        if (stashBuffer.size > 0) {
            unstashAllInternal()
        }
        try {
            let fresh = actorFactory()
            actor = fresh
            fresh.postRestart(reason)
            restartCount.fetchAdd(1)
            state = STATE_RUNNING
        } catch (e: Exception) {
            // 无法重建实例时停止该 Actor，避免继续用损坏的状态处理消息
            cellLog.error("Actor ${pathString} 重启失败，已停止: ${e}")
            initiateTerminate()
            return
        }
        startInstance()
    }

    /** 在 Cell 线程中调用 onStarted；失败按普通失败交给监督 */
    private func startInstance(): Unit {
        try {
            actor.onStarted(contextImpl())
        } catch (e: Exception) {
            handleFailure(e)
        }
    }

    /** 供顶层监督（用户 Guardian）调用 */
    func restartInCell(reason: Exception): Unit {
        doRestart(reason)
    }

    /** 供顶层监督调用：停止（会先停止子 Actor） */
    func terminateInCell(): Unit {
        initiateTerminate()
    }

    // ---------------- 系统消息 ----------------

    private func processSystemMessages(): Unit {
        let batch = takeSystemMessages(false)
        for (m in batch) {
            handleSystem(m)
        }
    }

    private func handleSystem(message: SysMsg): Unit {
        match (message) {
            case _: SysTerminate => initiateTerminate()
            case _: SysStarted =>
                if (state == STATE_RUNNING) {
                    startInstance()
                }
            case _: SysResume =>
                if (state == STATE_SUSPENDED) {
                    state = STATE_RUNNING
                    for (c in childMap.values()) {
                        c.sendSystem(SysResume())
                    }
                }
            case r: SysRecreate =>
                if (state != STATE_TERMINATING && state != STATE_TERMINATED) {
                    doRestart(r.cause)
                }
            case w: SysWatch => addWatcher(w.watcher)
            case u: SysUnwatch => removeWatcher(u.watcher)
            case d: SysDeathNotification => onDeathNotification(d.actor)
            case f: SysChildFailed => handleChildFailed(f.child, f.cause)
            case t: SysChildTerminated => onChildTerminated(t.child)
            case rt: SysReceiveTimeout => onReceiveTimeoutCheck(rt.generation)
            case _ => ()
        }
    }

    private func addWatcher(watcher: SimpleUserActorRef): Unit {
        if (refEq(watcher, this)) {
            return
        }
        for (w in watchers) {
            if (refEq(w, watcher)) {
                return
            }
        }
        watchers.add(watcher)
    }

    private func removeWatcher(watcher: SimpleUserActorRef): Unit {
        var i: Int64 = 0
        while (i < watchers.size) {
            if (refEq(watchers[i], watcher)) {
                watchers.remove(at: i)
                return
            }
            i++
        }
    }

    private func onDeathNotification(dead: SimpleUserActorRef): Unit {
        // 自己的子 Actor：先释放名称再投递 Terminated（与 Akka 相同），
        // 这样 Terminated 处理中可以立即用同一名称重建子 Actor（例如分片实体钝化后重建）
        onChildTerminated(dead)
        // 已取消监视（或从未监视）的不投递
        match (watching.get(dead.pathString)) {
            case Some(w) where refEq(w, dead) => watching.remove(dead.pathString)
            case _ => return
        }
        if (state == STATE_TERMINATING || state == STATE_TERMINATED) {
            return
        }
        if (state == STATE_SUSPENDED) {
            // 挂起期间暂存，恢复后再处理
            unstashed.add(Envelope(Terminated(dead), None<String>, None<ActorRef>))
            return
        }
        invoke(Terminated(dead), None<String>, None<ActorRef>)
    }

    private func onChildTerminated(child: SimpleUserActorRef): Unit {
        match (childMap.get(child.getName())) {
            case Some(c) where refEq(c, child) => childMap.remove(child.getName())
            case _ => ()
        }
        if (state == STATE_TERMINATING && childMap.size == 0) {
            finishTerminate()
        }
    }

    // ---------------- 停止协议 ----------------

    private func initiateTerminate(): Unit {
        if (state == STATE_TERMINATING || state == STATE_TERMINATED) {
            return
        }
        state = STATE_TERMINATING
        if (let Some(t) <- cellTimersValue) {
            t.cancelAll()
        }
        cancelReceiveTimeoutInternal()
        try {
            actor.preStop()
        } catch (e: Exception) {
            cellLog.warn("Actor ${pathString} preStop 失败: ${e}")
        }
        if (childMap.size == 0) {
            finishTerminate()
        } else {
            for (c in childMap.values()) {
                c.sendSystem(SysTerminate())
            }
        }
    }

    private func finishTerminate(): Unit {
        if (state == STATE_TERMINATED) {
            return
        }
        state = STATE_TERMINATED
        try {
            actor.postStop()
        } catch (e: Exception) {
            cellLog.warn("Actor ${pathString} postStop 失败: ${e}")
        }
        terminated.store(true)
        let remaining = takeSystemMessages(true)
        guardian.onCellTerminated(this)
        for (w in watchers) {
            w.sendSystem(SysDeathNotification(this))
        }
        watchers.clear()
        for ((_, target) in watching) {
            target.sendSystem(SysUnwatch(this))
        }
        watching.clear()
        if (let Some(parent) <- parentCell) {
            parent.sendSystem(SysChildTerminated(this))
        }
        for (m in remaining) {
            handleSystemAfterDeath(m)
        }
        // 未处理的消息计为死信
        while (hasUnstashed()) {
            deadLetter(unstashed[unstashIndex].message)
            unstashIndex++
        }
        for (env in stashBuffer) {
            deadLetter(env.message)
        }
        stashBuffer.clear()
        if (mailboxKind == MAILBOX_PRIORITY) {
            while (let Some(env) <- priorityMailbox.poll()) {
                deadLetter(env.message)
            }
        } else {
            while (let Some(m) <- mailbox.poll()) {
                deadLetter(m)
            }
        }
    }

    // ---------------- 接收超时 ----------------

    func setReceiveTimeoutInternal(timeout: Duration): Unit {
        cancelReceiveTimeoutInternal()
        if (timeout <= Duration.Zero) {
            return
        }
        receiveTimeoutValue = timeout
        lastActivity = MonoTime.now()
        scheduleReceiveTimeoutCheck(timeout)
    }

    func cancelReceiveTimeoutInternal(): Unit {
        receiveTimeoutGeneration++
        receiveTimeoutValue = Duration.Zero
        if (let Some(t) <- receiveTimeoutTimer) {
            t.cancel()
        }
        receiveTimeoutTimer = None<Timer>
    }

    private func scheduleReceiveTimeoutCheck(delay: Duration): Unit {
        let gen = receiveTimeoutGeneration
        receiveTimeoutTimer = Some(Timer.once(delay, { => this.sendSystem(SysReceiveTimeout(gen)) }))
    }

    private func onReceiveTimeoutCheck(generation: Int64): Unit {
        if (generation != receiveTimeoutGeneration || receiveTimeoutValue <= Duration.Zero || state != STATE_RUNNING) {
            return
        }
        let elapsed = MonoTime.now() - lastActivity
        if (elapsed >= receiveTimeoutValue) {
            invoke(ReceiveTimeout(), None<String>, None<ActorRef>)
            // 空闲期间每隔 timeout 重复投递
            lastActivity = MonoTime.now()
            if (receiveTimeoutValue > Duration.Zero && generation == receiveTimeoutGeneration) {
                scheduleReceiveTimeoutCheck(receiveTimeoutValue)
            }
        } else {
            scheduleReceiveTimeoutCheck(receiveTimeoutValue - elapsed)
        }
    }

    // ---------------- 上下文能力（只在 Cell 线程内调用） ----------------

    func contextImpl(): CellContext {
        match (contextValue) {
            case Some(c) => c
            case None =>
                let c = CellContext(this)
                contextValue = Some(c)
                c
        }
    }

    func timersImpl(): CellTimers {
        match (cellTimersValue) {
            case Some(t) => t
            case None =>
                let t = CellTimers(this)
                cellTimersValue = Some(t)
                t
        }
    }

    func senderPathInternal(): Option<String> {
        match (currentSender) {
            case Some(s) => Some(s)
            case None => match (currentSenderRef) {
                case Some(r) => Some(r.path.toString())
                case None => None<String>
            }
        }
    }

    func senderRefInternal(): Option<ActorRef> {
        match (currentSenderRef) {
            case Some(r) => Some(r)
            case None => match (currentSender) {
                case Some(p) => guardian.lookup(p)
                case None => None<ActorRef>
            }
        }
    }

    func replyInternal(message: Message): Unit {
        match (senderRefInternal()) {
            case Some(r) => r.tell(message, this)
            case None => deadLetter(message)
        }
    }

    func spawnChild(props: Props<Actor>, name: String): ActorRef {
        if (state == STATE_TERMINATING || state == STATE_TERMINATED) {
            throw IllegalStateException("Actor ${pathString} 正在停止，不能创建子 Actor")
        }
        validateChildName(name)
        if (childMap.contains(name)) {
            throw InvalidActorNameException("子 Actor 名称已存在: ${pathString}/${name}")
        }
        let child = guardian.createCell(props, name, "${pathString}/${name}", Some(this))
        childMap.add(name, child)
        child
    }

    func spawnAnonymousChild(creator: () -> Actor): ActorRef {
        anonymousSeq++
        spawnChild(PropsFactory.create<Actor>(creator), "$${anonymousSeq}")
    }

    func childrenInternal(): Array<ActorRef> {
        let result = ArrayList<ActorRef>()
        for (c in childMap.values()) {
            result.add(c)
        }
        result.toArray()
    }

    func childInternal(name: String): Option<ActorRef> {
        match (childMap.get(name)) {
            case Some(c) => Some<ActorRef>(c)
            case None => None<ActorRef>
        }
    }

    func stopInternal(ref: ActorRef): Unit {
        let isSelf = match (ref) {
            case c: SimpleUserActorRef => refEq(c, this)
            case _ => ref.path.toString() == actorPath.toString()
        }
        if (isSelf) {
            // 停止自身：当前消息处理完后生效
            sendSystem(SysTerminate())
            return
        }
        match (ref) {
            case c: SimpleUserActorRef =>
                match (childMap.get(c.getName())) {
                    case Some(existing) where refEq(existing, c) => c.sendSystem(SysTerminate())
                    case _ => throw IllegalArgumentException("context.stop 只能停止自身或子 Actor: ${c.pathString}")
                }
            case _ => throw IllegalArgumentException("context.stop 只能停止自身或子 Actor: ${ref.path.toString()}")
        }
    }

    func watchInternal(ref: ActorRef): Unit {
        let target = asLocalCell(ref, "watch")
        if (refEq(target, this)) {
            return
        }
        if (!watching.contains(target.pathString)) {
            watching.add(target.pathString, target)
            target.sendSystem(SysWatch(this))
        }
    }

    func unwatchInternal(ref: ActorRef): Unit {
        let target = asLocalCell(ref, "unwatch")
        match (watching.get(target.pathString)) {
            case Some(w) where refEq(w, target) =>
                watching.remove(target.pathString)
                target.sendSystem(SysUnwatch(this))
            case _ => ()
        }
    }

    private func asLocalCell(ref: ActorRef, op: String): SimpleUserActorRef {
        match (ref) {
            case c: SimpleUserActorRef => c
            case _ =>
                match (guardian.lookupCell(ref.path.getPath())) {
                    case Some(c) => c
                    case None => throw IllegalArgumentException("${op}: 目标不是本系统内的 Actor: ${ref.path.toString()}")
                }
        }
    }

    func becomeInternal(behavior: Receive, discardOld: Bool): Unit {
        if (discardOld && behaviors.size > 0) {
            behaviors[behaviors.size - 1] = behavior
        } else {
            behaviors.add(behavior)
        }
    }

    func unbecomeInternal(): Unit {
        if (behaviors.size > 0) {
            behaviors.remove(at: behaviors.size - 1)
        }
    }

    func stashInternal(): Unit {
        match (currentMessage) {
            case Some(m) => stashBuffer.add(Envelope(m, currentSender, currentSenderRef))
            case None => throw IllegalStateException("stash 只能在处理消息时调用")
        }
    }

    func unstashAllInternal(): Unit {
        if (stashBuffer.size == 0) {
            return
        }
        let merged = ArrayList<Envelope>()
        merged.add(all: stashBuffer)
        while (hasUnstashed()) {
            merged.add(unstashed[unstashIndex])
            unstashIndex++
        }
        stashBuffer.clear()
        unstashed = merged
        unstashIndex = 0
    }

    /** 定时器触发时由定时器线程调用 */
    func deliverTimer(envelope: TimerEnvelope): Unit {
        tell(envelope)
    }

    func validateChildName(name: String): Unit {
        if (name.isEmpty() || name.contains("/") || name.size > 200) {
            throw InvalidActorNameException("非法的 Actor 名称: '${name}'（不能为空、不能包含 '/'、长度不超过 200）")
        }
    }

    public func equals(other: ActorRef): Bool {
        match (other) {
            case userRef: SimpleUserActorRef => refEq(this, userRef)
            case _ => false
        }
    }

    public func hashCode(): Int32 {
        Int32(pathString.hashCode() & 0x7FFFFFFF)
    }

    public func toString(): String {
        "Actor[${pathString}]"
    }
}

func directiveName(d: SupervisionDirective): String {
    match (d) {
        case SupervisionDirective.Resume => "Resume"
        case SupervisionDirective.Restart => "Restart"
        case SupervisionDirective.Stop => "Stop"
        case SupervisionDirective.Escalate => "Escalate"
    }
}

// ======================= 上下文 =======================

/**
 * Actor Cell 的上下文：每个 Cell 一个实例（不再为每条消息分配），只在 Cell 线程内使用
 */
public class CellContext <: ActorContext {
    private let cell: SimpleUserActorRef
    private var loggerValue: Option<Logger> = None<Logger>

    init(cell: SimpleUserActorRef) {
        this.cell = cell
    }

    public func sender(): Option<String> {
        cell.senderPathInternal()
    }

    public prop self: ActorRef {
        get() { cell }
    }

    public func senderRef(): Option<ActorRef> {
        cell.senderRefInternal()
    }

    public func parent(): Option<ActorRef> {
        match (cell.parentCell) {
            case Some(p) => Some<ActorRef>(p)
            case None => None<ActorRef>
        }
    }

    public func reply(message: Message): Unit {
        cell.replyInternal(message)
    }

    public func send(target: ActorRef, message: Message): Unit {
        target.tell(message, cell)
    }

    public func actorOf(props: Props<Actor>, name: String): ActorRef {
        cell.spawnChild(props, name)
    }

    public func actorOf(creator: () -> Actor): ActorRef {
        cell.spawnAnonymousChild(creator)
    }

    public func children(): Array<ActorRef> {
        cell.childrenInternal()
    }

    public func child(name: String): Option<ActorRef> {
        cell.childInternal(name)
    }

    public func stop(ref: ActorRef): Unit {
        cell.stopInternal(ref)
    }

    public func watch(ref: ActorRef): Unit {
        cell.watchInternal(ref)
    }

    public func unwatch(ref: ActorRef): Unit {
        cell.unwatchInternal(ref)
    }

    public func become(behavior: Receive, discardOld: Bool): Unit {
        cell.becomeInternal(behavior, discardOld)
    }

    public func unbecome(): Unit {
        cell.unbecomeInternal()
    }

    public func setReceiveTimeout(timeout: Duration): Unit {
        cell.setReceiveTimeoutInternal(timeout)
    }

    public func cancelReceiveTimeout(): Unit {
        cell.cancelReceiveTimeoutInternal()
    }

    public prop timers: TimerScheduler {
        get() { cell.timersImpl() }
    }

    public func stash(): Unit {
        cell.stashInternal()
    }

    public func unstashAll(): Unit {
        cell.unstashAllInternal()
    }

    public func log(): Logger {
        match (loggerValue) {
            case Some(l) => l
            case None =>
                let l = Logging.logger(cell.pathString)
                loggerValue = Some(l)
                l
        }
    }

    public func lookup(path: String): Option<ActorRef> {
        cell.guardian.lookup(path)
    }
}

// ======================= 定时器 =======================

class TimerEntry {
    let generation: Int64
    let timer: Timer
    let single: Bool
    init(generation: Int64, timer: Timer, single: Bool) {
        this.generation = generation
        this.timer = timer
        this.single = single
    }
}

/**
 * Actor 级定时器：基于 std.sync.Timer。
 * 定时器线程只负责把带代次的 TimerEnvelope 放进邮箱；投递时在 Cell 内校验代次，
 * 因此 cancel 或同 key 重新 start 之后，已在途的旧消息不会被处理。
 */
class CellTimers <: TimerScheduler {
    private let cell: SimpleUserActorRef
    private let entries = HashMap<String, TimerEntry>()
    private var generationSeq: Int64 = 0

    init(cell: SimpleUserActorRef) {
        this.cell = cell
    }

    private func nextGeneration(): Int64 {
        generationSeq++
        generationSeq
    }

    public func startSingleTimer(key: String, message: Message, delay: Duration): Unit {
        cancel(key)
        let gen = nextGeneration()
        let target = cell
        let t = Timer.once(delay, { => target.deliverTimer(TimerEnvelope(key, gen, message)) })
        entries.add(key, TimerEntry(gen, t, true))
    }

    public func startTimerWithFixedDelay(key: String, message: Message, delay: Duration): Unit {
        cancel(key)
        let gen = nextGeneration()
        let target = cell
        let t = Timer.repeat(delay, delay, { => target.deliverTimer(TimerEnvelope(key, gen, message)) },
            style: CatchupStyle.Delay)
        entries.add(key, TimerEntry(gen, t, false))
    }

    public func startTimerAtFixedRate(key: String, message: Message, interval: Duration): Unit {
        cancel(key)
        let gen = nextGeneration()
        let target = cell
        let t = Timer.repeat(interval, interval, { => target.deliverTimer(TimerEnvelope(key, gen, message)) },
            style: CatchupStyle.Burst)
        entries.add(key, TimerEntry(gen, t, false))
    }

    public func isTimerActive(key: String): Bool {
        entries.contains(key)
    }

    public func cancel(key: String): Unit {
        if (let Some(e) <- entries.remove(key)) {
            e.timer.cancel()
        }
    }

    public func cancelAll(): Unit {
        for ((_, e) in entries) {
            e.timer.cancel()
        }
        entries.clear()
    }

    /** 投递时校验：返回 true 表示应交给 receive 处理 */
    func accept(envelope: TimerEnvelope): Bool {
        match (entries.get(envelope.key)) {
            case Some(e) where e.generation == envelope.generation =>
                if (e.single) {
                    entries.remove(envelope.key)
                }
                true
            case _ => false
        }
    }
}

// ======================= 调度器 =======================

/** Cell 执行器：默认的亲和调度池，或供阻塞 Actor 使用的共享池 */
interface CellExecutor {
    func execute(cell: SimpleUserActorRef): Unit
    func nextAffinity(): Int64
    func throughput(): Int64
}

/**
 * 工作线程的就绪队列：与邮箱相同的 Vyukov MPSC 结构（任意线程提交，只有所属工作线程取出）。
 * std ConcurrentLinkedQueue 单次入队+出队约 190ns，这里约 30–40ns。
 */
/**
 * 就绪队列的侵入式链接：Cell 自身就是队列节点（同一时刻最多入队一次，由 scheduled 标志保证），
 * 调度时不再为每次入队分配节点
 */
public abstract class RunLink {
    let runNext: AtomicOptionReference<RunLink> = AtomicOptionReference<RunLink>()
}

class RunStub <: RunLink {}

/**
 * 侵入式 MPSC 队列（Vyukov）：多个生产者 add，只有所属工作线程 remove
 */
class RunQueue {
    private let head: AtomicReference<RunLink>
    private var tail: RunLink
    private let stub: RunStub

    init() {
        let s = RunStub()
        this.stub = s
        this.head = AtomicReference<RunLink>(s)
        this.tail = s
    }

    private func push(node: RunLink): Unit {
        node.runNext.store(None<RunLink>)
        let prev = head.swap(node)
        prev.runNext.store(Some(node))
    }

    func add(cell: SimpleUserActorRef): Unit {
        push(cell)
    }

    /** 只由所属工作线程调用；遇到尚未链接完成的节点时返回 None，由工作线程让出后重试 */
    func remove(): Option<SimpleUserActorRef> {
        var t = tail
        var next = t.runNext.load()
        if (refEq(t, stub)) {
            match (next) {
                case Some(n) =>
                    tail = n
                    t = n
                    next = n.runNext.load()
                case None => return None<SimpleUserActorRef>
            }
        }
        if (let Some(n) <- next) {
            tail = n
            return (t as SimpleUserActorRef)
        }
        // t 是最后一个节点：生产者正在入队时稍后重试
        if (!refEq(t, head.load())) {
            return None<SimpleUserActorRef>
        }
        // 放回哨兵，使 t 可以出队
        push(stub)
        if (let Some(n) <- t.runNext.load()) {
            tail = n
            return (t as SimpleUserActorRef)
        }
        None<SimpleUserActorRef>
    }

    /** 只有哨兵、且没有生产者正在入队时为空 */
    func isEmpty(): Bool {
        refEq(tail, stub) && refEq(head.load(), stub)
    }

    /** 近似长度（O(n)，仅诊断用） */
    prop size: Int64 {
        get() {
            var n: Int64 = 0
            var cur: Option<RunLink> = Some(tail)
            while (let Some(node) <- cur) {
                if (!refEq(node, stub)) {
                    n++
                }
                cur = node.runNext.load()
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
                        cellLog.error("Actor Cell 执行异常: ${e}")
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

func envInt(name: String, defaultValue: Int64, minValue: Int64): Int64 {
    if (let Some(v) <- getVariable(name)) {
        if (let Some(n) <- Int64.tryParse(v)) {
            if (n >= minValue) {
                return n
            }
        }
    }
    defaultValue
}

/**
 * 默认调度器：亲和调度（与 Akka AffinityPool 同类）
 *
 * 每个 Actor 固定分配给一个工作线程，每个工作线程有自己的就绪队列，调度时不争用全局锁。
 * 工作线程空闲时先自旋（默认 50µs，CACTOR_DISPATCHER_SPIN_US）再休眠，降低唤醒延迟。
 * 线程数默认取 CPU 核数的 1/4（4–64），可用 CACTOR_DISPATCHER_THREADS 覆盖。
 * 每次调度最多处理的消息数默认 64，可用 CACTOR_DISPATCHER_THROUGHPUT 调整（对应 Akka 的 throughput）。
 * 会阻塞的 Actor 应使用 Props.withDispatcher("blocking")。
 */
public class ActorCellDispatcher <: CellExecutor {
    private static let defaultInstance: ActorCellDispatcher = ActorCellDispatcher()
    private static let blockingInstance: BlockingCellExecutor = BlockingCellExecutor()

    private let started: AtomicBool = AtomicBool(false)
    private let ready: AtomicBool = AtomicBool(false)
    private var workers: Array<DispatcherWorker> = Array<DispatcherWorker>()
    private let affinitySeq: AtomicInt64 = AtomicInt64(0)
    private let throughputValue: Int64

    init() {
        this.throughputValue = envInt("CACTOR_DISPATCHER_THROUGHPUT", 64, 1)
    }

    static func forName(name: String): CellExecutor {
        match (name) {
            case "blocking" | "blocking-io" => blockingInstance
            case _ => defaultInstance
        }
    }

    public func nextAffinity(): Int64 {
        affinitySeq.fetchAdd(1)
    }

    public func throughput(): Int64 {
        throughputValue
    }

    public func execute(cell: SimpleUserActorRef): Unit {
        if (!ready.load()) {
            ensureStarted()
        }
        workers[cell.affinity % workers.size].submit(cell)
    }

    private func ensureStarted(): Unit {
        if (started.compareAndSwap(false, true)) {
            var n = Int64(getProcessorCount()) / 4
            if (n < 4) {
                n = 4
            }
            if (n > 64) {
                n = 64
            }
            n = envInt("CACTOR_DISPATCHER_THREADS", n, 1)
            let spin = Duration.microsecond * envInt("CACTOR_DISPATCHER_SPIN_US", 50, 0)
            let ws = Array<DispatcherWorker>(n, { _ => DispatcherWorker() })
            workers = ws
            for (w in ws) {
                spawn {
                    markActorThread()
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

    /** 默认调度器的工作线程数（未启动时为 0） */
    public static func getWorkerCount(): Int64 {
        defaultInstance.workerCount()
    }

    func workerCount(): Int64 {
        if (ready.load()) { workers.size } else { 0 }
    }

    /** 已执行的调度次数 */
    public static func getExecutedCount(): Int64 {
        var total: Int64 = 0
        let d = defaultInstance
        if (d.ready.load()) {
            for (w in d.workers) {
                total += w.executed.load()
            }
        }
        total
    }

    /** 等待执行的 Cell 数 */
    public static func getQueuedCount(): Int64 {
        var total: Int64 = 0
        let d = defaultInstance
        if (d.ready.load()) {
            for (w in d.workers) {
                total += w.queue.size
            }
        }
        total
    }

    /** 阻塞调度器的工作线程数（未启动时为 0） */
    public static func getBlockingWorkerCount(): Int64 {
        blockingInstance.workerCount()
    }
}

/**
 * 阻塞调度器：所有工作线程共享一个就绪队列（谁空闲谁取），
 * 某个 Actor 在 receive 中阻塞时，只占用一个线程，不影响默认调度器上的 Actor。
 * 线程数默认 16，可用 CACTOR_BLOCKING_THREADS 调整。
 */
class BlockingCellExecutor <: CellExecutor {
    private let lock = Mutex()
    private let cond: Condition
    private let queue = ArrayList<SimpleUserActorRef>()
    private var head: Int64 = 0
    private let started = AtomicBool(false)
    private let threads = AtomicInt64(0)

    init() {
        this.cond = synchronized(lock) { lock.condition() }
    }

    public func nextAffinity(): Int64 {
        0
    }

    public func throughput(): Int64 {
        // 阻塞型 Actor 每次只处理少量消息就让出，避免长时间独占线程
        8
    }

    public func execute(cell: SimpleUserActorRef): Unit {
        if (!started.load()) {
            ensureStarted()
        }
        synchronized(lock) {
            queue.add(cell)
            cond.notify()
        }
    }

    private func ensureStarted(): Unit {
        if (started.compareAndSwap(false, true)) {
            let n = envInt("CACTOR_BLOCKING_THREADS", 16, 1)
            threads.store(n)
            for (_ in 0..n) {
                spawn {
                    markActorThread()
                    workerLoop()
                }
            }
        }
    }

    private func take(): SimpleUserActorRef {
        synchronized(lock) {
            while (head >= queue.size) {
                if (queue.size > 0) {
                    queue.clear()
                    head = 0
                }
                cond.wait()
            }
            let c = queue[head]
            head++
            if (head > 1024 && head * 2 > queue.size) {
                // 定期压缩已消费的前缀
                let rest = ArrayList<SimpleUserActorRef>()
                var i = head
                while (i < queue.size) {
                    rest.add(queue[i])
                    i++
                }
                queue.clear()
                queue.add(all: rest)
                head = 0
            }
            c
        }
    }

    private func workerLoop(): Unit {
        while (true) {
            let cell = take()
            try {
                cell.run()
            } catch (e: Exception) {
                cellLog.error("阻塞调度器执行异常: ${e}")
            }
        }
    }

    func workerCount(): Int64 {
        threads.load()
    }
}
