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
import std.fs.{File, exists}
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

// schedState 状态字位定义（见 SimpleUserActorRef.schedState 说明）
let SCHED_BIT: Int64 = 1
let TERM_BIT: Int64 = 2
let SCHED_COUNT_SHIFT: Int64 = 2
// 清除调度位（bit0）的掩码：除 bit0 外全 1（仓颉整数没有按位取反运算符，用补码常量）
let SCHED_CLEAR_MASK: Int64 = -2

// 诊断计数器位域（P1-5）：processed / deadLetters / restarts / unhandled 各占 16 位。
// 四个独立原子对象每个约 46 字节，合并后每 Actor 省下约 138 字节；
// 16 位封顶（65535），诊断计数不需要无界精度。每次 fetchAdd 一个"预先移位好的增量"，
// 各字段相互独立（只要不越过 65535，就不会进位污染相邻字段）。
let CTR_FIELD_MASK: Int64 = 0xFFFF
let CTR_PROCESSED_SHIFT: Int64 = 0
let CTR_DEADLETTER_SHIFT: Int64 = 16
let CTR_RESTART_SHIFT: Int64 = 32
let CTR_UNHANDLED_SHIFT: Int64 = 48

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

/** 停止合批（P5-2）：一批子 Actor 全部终止的汇总信号（替代每子一条 SysChildTerminated） */
class SysChildrenTerminated <: SysMsg {}

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
 * 无界邮箱：双缓冲数组（与 protoactor-go 的 goring 同类），稳态下不分配内存
 * - 发送方持锁把消息追加到 incoming；消费方把本地的 draining 读空后，持锁与 incoming 整体交换，之后无锁处理整批
 * - 不用链表的原因：仓颉的原子变量是独立的堆对象，链表每条消息至少两次分配（节点 + 原子引用）。
 *   实测多线程下分配与 GC 不能随线程数扩展（8 线程的总吞吐与 1 线程相当），数组在稳态下零分配，可以扩展；
 *   并且链表里已出队的节点仍指向后继，GC 压力大时会成串滞留，吞吐偶发跌到 1/10
 * - 不用自旋锁：扇入（多发送方争用）时持锁方被抢占，其余发送方会空转，实测比 Mutex 慢 3 倍以上
 * - 缓冲区在第一条消息到达时才分配，从未收到消息的 Actor 不占这部分内存；突发后超过阈值的缓冲区在交换时丢弃
 */
class CellMailbox {
    // 初始容量 4：多数 Actor 每条消息处理完即空（drain 场景），数组越小、长期占用越少；
    // 突发按 2× 增长，超过 RETAIN_CAPACITY 的大缓冲在换批时丢弃
    private static let INITIAL_CAPACITY: Int64 = 4
    private static let RETAIN_CAPACITY: Int64 = 256

    private let lock = Mutex()
    // cell 的调度状态字（见 SimpleUserActorRef.schedState 说明）：本邮箱的 incoming 计数占用 bit2..，
    // 由发送方在锁内 fetchAdd、消费方换批时 fetchSub。消费方释放调度权后不加锁检查"是否还有消息"，
    // 与发送方"入队后置调度位"组成 Dekker 式配对；两侧都是同一原子字上的全序操作，不会丢失唤醒
    private let state: AtomicInt64
    // 已关闭：Actor 停止后不再接受新消息。判定在锁内与入队原子完成，
    // 否则"发送方刚检查完 terminated"与"Cell 排空邮箱"之间的窗口会让消息滞留（不投递也不计死信）
    private var closed: Bool = false
    // in* 只在持锁时访问；首次入队时才分配（注释原先说"缓冲区在第一条消息到达时才分配"，
    // 实测此前字段初始化就分配了 6 个空数组，每个约 79 字节，从不收消息的 Actor 白占这部分内存）
    private var inMessages: Option<Array<Option<Message>>> = None<Array<Option<Message>>>
    private var inSenders: Option<Array<Option<String>>> = None<Array<Option<String>>>
    private var inSenderRefs: Option<Array<Option<ActorRef>>> = None<Array<Option<ActorRef>>>
    private var inCount: Int64 = 0
    // dr* 只由持有调度权的消费方访问；首次 refill 时才分配
    private var drMessages: Option<Array<Option<Message>>> = None<Array<Option<Message>>>
    private var drSenders: Option<Array<Option<String>>> = None<Array<Option<String>>>
    private var drSenderRefs: Option<Array<Option<ActorRef>>> = None<Array<Option<ActorRef>>>
    private var drCount: Int64 = 0
    private var drIndex: Int64 = 0

    init(state: AtomicInt64) {
        this.state = state
    }

    /** 入队；邮箱已关闭时返回 false（消息应计为死信） */
    func add(message: Message, sender: Option<String>, senderRef: Option<ActorRef>): Bool {
        synchronized(lock) {
            if (closed) {
                return false
            }
            if (inMessages.isNone()) {
                // 首条消息：按初始容量分配
                inMessages = Some(Array<Option<Message>>(INITIAL_CAPACITY, repeat: None<Message>))
            }
            let msgs = inMessages.getOrThrow()
            if (inCount == msgs.size) {
                growIncoming()
            }
            // Array 是引用类型：通过局部引用写入与字段里的是同一个数组
            let slotMsgs = inMessages.getOrThrow()
            slotMsgs[inCount] = Some(message)
            // sender 两件套懒分配：绝大多数消息不带发送方（普通 tell），
            // 只有第一条带发送方的消息才分配这两个数组（各约 100–140 字节）
            if (sender.isSome()) {
                if (inSenders.isNone() || inSenders.getOrThrow().size != slotMsgs.size) {
                    inSenders = Some(Array<Option<String>>(slotMsgs.size, repeat: None<String>))
                }
                let slotSenders = inSenders.getOrThrow()
                slotSenders[inCount] = sender
            }
            if (senderRef.isSome()) {
                if (inSenderRefs.isNone() || inSenderRefs.getOrThrow().size != slotMsgs.size) {
                    inSenderRefs = Some(Array<Option<ActorRef>>(slotMsgs.size, repeat: None<ActorRef>))
                }
                let slotSenderRefs = inSenderRefs.getOrThrow()
                slotSenderRefs[inCount] = senderRef
            }
            inCount++
            state.fetchAdd(1 << SCHED_COUNT_SHIFT)
            true
        }
    }

    /** 关闭邮箱：与入队在同一把锁内判定，关闭后不再接受新消息 */
    func close(): Unit {
        synchronized(lock) {
            closed = true
        }
    }

    private func growIncoming(): Unit {
        let oldMsgs = inMessages.getOrThrow()
        let old = oldMsgs.size
        let capacity = if (old == 0) { INITIAL_CAPACITY } else { old * 2 }
        let messages = Array<Option<Message>>(capacity, repeat: None<Message>)
        oldMsgs.copyTo(messages, 0, 0, inCount)
        inMessages = Some(messages)
        // sender 数组可能尚未分配（从未有过带发送方的消息）；已分配则同步扩容
        if (let Some(oldSenders) <- inSenders) {
            let senders = Array<Option<String>>(capacity, repeat: None<String>)
            oldSenders.copyTo(senders, 0, 0, inCount)
            inSenders = Some(senders)
        }
        if (let Some(oldRefs) <- inSenderRefs) {
            let senderRefs = Array<Option<ActorRef>>(capacity, repeat: None<ActorRef>)
            oldRefs.copyTo(senderRefs, 0, 0, inCount)
            inSenderRefs = Some(senderRefs)
        }
    }

    /** 最近一次 poll 出的消息的发送方（仅消费者使用） */
    var lastSender: Option<String> = None<String>
    var lastSenderRef: Option<ActorRef> = None<ActorRef>

    /** 仅由持有调度权的线程调用 */
    func poll(): Option<Message> {
        if (drIndex == drCount && !refill()) {
            return None<Message>
        }
        // refill 成功后 drMessages 必然已分配；sender 数组可能仍未分配（无发送方）
        let msgs = drMessages.getOrThrow()
        let i = drIndex
        drIndex++
        let message = msgs[i]
        // 逐项清空：已处理的消息不能被缓冲区继续引用（消息可能很大），交换回 incoming 时也就是干净的
        msgs[i] = None<Message>
        lastSender = match (drSenders) {
            case Some(senders) =>
                let s = senders[i]
                if (s.isSome()) { senders[i] = None<String> }
                s
            case None => None<String>
        }
        lastSenderRef = match (drSenderRefs) {
            case Some(senderRefs) =>
                let r = senderRefs[i]
                if (r.isSome()) { senderRefs[i] = None<ActorRef> }
                r
            case None => None<ActorRef>
        }
        message
    }

    /** 本地批已读完：与 incoming 交换；incoming 为空时返回 false */
    private func refill(): Bool {
        drIndex = 0
        drCount = 0
        if ((state.load() >> SCHED_COUNT_SHIFT) == 0) {
            return false
        }
        synchronized(lock) {
            if (drMessages.isNone()) {
                // 首次消费：准备用于交换的本地数组（先给空的，交换后即持有 incoming 的数组）
                drMessages = Some(Array<Option<Message>>())
                drSenders = Some(Array<Option<String>>())
                drSenderRefs = Some(Array<Option<ActorRef>>())
            }
            // sender 数组可能为 None（从未有带发送方的消息）：换出的"空数组"同样用 None 表示
            let messages = drMessages.getOrThrow()
            let oldSenders = drSenders
            let oldSenderRefs = drSenderRefs
            drMessages = inMessages
            drSenders = inSenders
            drSenderRefs = inSenderRefs
            drCount = inCount
            if (messages.size > RETAIN_CAPACITY) {
                // 突发留下的大缓冲区不再复用，下次入队时再按初始容量分配
                inMessages = None<Array<Option<Message>>>
                inSenders = None<Array<Option<String>>>
                inSenderRefs = None<Array<Option<ActorRef>>>
            } else {
                inMessages = Some(messages)
                // 换回的空数组用 None 表示，由下一次带发送方的 add 按当前容量重新分配
                inSenders = match (oldSenders) {
                    case Some(a) => if (a.size > 0) { Some(a) } else { None<Array<Option<String>>> }
                    case None => None<Array<Option<String>>>
                }
                inSenderRefs = match (oldSenderRefs) {
                    case Some(a) => if (a.size > 0) { Some(a) } else { None<Array<Option<ActorRef>>> }
                    case None => None<Array<Option<ActorRef>>>
                }
            }
            inCount = 0
            state.fetchSub(drCount << SCHED_COUNT_SHIFT)
        }
        drCount > 0
    }

    func nonEmpty(): Bool {
        drIndex < drCount || (state.load() >> SCHED_COUNT_SHIFT) > 0
    }

    /** 近似计数（仅用于诊断，可从任意线程调用） */
    func count(): Int64 {
        let local = drCount - drIndex
        (state.load() >> SCHED_COUNT_SHIFT) + (if (local > 0) { local } else { 0 })
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
    // 已关闭：与入队在同一把锁内判定，避免停止与并发发送之间出现滞留消息
    private var closed: Bool = false

    /** 入队；邮箱已关闭时返回 false（消息应计为死信） */
    func add(message: Message, sender: Option<String>, senderRef: Option<ActorRef>): Bool {
        let env = Envelope(message, sender, senderRef)
        let p = message.priority()
        synchronized(lock) {
            if (closed) {
                return false
            }
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
            true
        }
    }

    func close(): Unit {
        synchronized(lock) {
            closed = true
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
    private var actor: Actor
    private let actorFactory: () -> Actor
    let pathString: String
    let guardian: RuntimeUserGuardian
    let parentCell: Option<SimpleUserActorRef>
    let childStrategy: Option<SupervisionStrategy>
    private let executor: CellExecutor

    // 共享的空容器：只读路径返回它们，写路径一律先分配真实容器
    private static let EMPTY_CHILD_MAP: HashMap<String, SimpleUserActorRef> = HashMap<String, SimpleUserActorRef>(0)
    private static let EMPTY_WATCHING: HashMap<String, SimpleUserActorRef> = HashMap<String, SimpleUserActorRef>(0)
    private static let EMPTY_WATCHERS: ArrayList<SimpleUserActorRef> = ArrayList<SimpleUserActorRef>(0)
    private static let EMPTY_BEHAVIORS: ArrayList<Receive> = ArrayList<Receive>(0)
    private static let EMPTY_STASH: ArrayList<Envelope> = ArrayList<Envelope>(0)
    private static let EMPTY_UNSTASHED: ArrayList<Envelope> = ArrayList<Envelope>(0)

    // ---- 邮箱（任意线程写） ----
    private let mailbox: CellMailbox
    private let mailboxKind: Int64
    private let capacity: Int64
    private let overflow: MailboxOverflow
    // 仅 MAILBOX_BOUNDED 使用：懒分配，无界/优先级邮箱（绝大多数 Actor）不占这个原子对象
    private var boundedCountValue: Option<AtomicInt64> = None<AtomicInt64>
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
    // 调度状态字（P2-1/P1-6：scheduled + terminated + 邮箱 incoming 计数合并为一个原子字）
    //   bit0 = SCHED_BIT：同一时刻最多一个线程持有本 Cell 的调度权（CAS 0→1 后 execute）
    //   bit1 = TERM_BIT：已终止（terminated 标志的镜像，热路径每条 tell 读一次）
    //   bit2.. = 无界邮箱 incoming 计数（发送方在邮箱锁内 fetchAdd，见 CellMailbox.state）
    // 每条 tell 由"terminated.load + incomingSize.store + scheduled.load + scheduled.CAS"
    // （三个独立原子对象 = 三次跨核缓存行往返）减为同一字上的 1 次 load + 1 次 CAS。
    // Dekker 配对不变且更严格：发送方"计数++ 后查/置调度位"、消费方"清调度位 后查计数"
    // 都是同一原子字上的顺序一致操作，任一交错都不会丢失唤醒。
    private let schedState: AtomicInt64
    // 诊断计数器合并为一个位域原子字（见文件头的 CTR_* 定义与 P1-5）
    private let counters: AtomicInt64

    // ---- 只在 Cell 线程内访问 ----
    private var state: Int64 = STATE_RUNNING
    // 以下容器按需分配：没有子 Actor / 不监视别人 / 没有 stash 的 Actor 不再各自分配一个空容器
    // （实测空的 HashMap 约 280 字节、空的 ArrayList 约 96 字节，六个容器合计约 944 字节，
    //   而绝大多数 Actor 从不使用它们）
    private var childMapValue: Option<HashMap<String, SimpleUserActorRef>> = None<HashMap<String, SimpleUserActorRef>>
    private var anonymousSeq: Int64 = 0
    // 停止合批（P5-2）：父进入"停止整棵子树"时把子数记入 stoppingChildren 并置 childBatchStopping；
    // 每个子终止恰好把计数减 1，减到 0 的那一方让父收尾。这样百万子 Actor 不必各发一条
    // SysChildTerminated 在父的 sysHead 上做百万次 CAS 争用与百万次 childMap.remove。
    // 懒分配：只有真正进入"停止有子 Actor 的子树"时才创建这两个原子对象，
    // 叶 Actor（绝大多数）不为它们付费。childBatchStopping 用 AtomicInt64（0/1）而非 AtomicBool，
    // 与 stoppingChildren 同生命周期，避免多一个对象。
    private var stoppingChildrenValue: Option<AtomicInt64> = None<AtomicInt64>
    private var childBatchStoppingValue: Option<AtomicInt64> = None<AtomicInt64>
    private prop stoppingChildren: AtomicInt64 {
        get() { stoppingChildrenValue.getOrThrow() }
    }
    private prop childBatchStopping: AtomicInt64 {
        get() { childBatchStoppingValue.getOrThrow() }
    }
    /** 子侧读父的合批标志：父未分配即视为未合批（返回 false） */
    private func batchStopping(): Bool {
        match (childBatchStoppingValue) {
            case Some(f) => f.load() != 0
            case None => false
        }
    }
    private var watchersValue: Option<ArrayList<SimpleUserActorRef>> = None<ArrayList<SimpleUserActorRef>>
    private var watchingValue: Option<HashMap<String, SimpleUserActorRef>> = None<HashMap<String, SimpleUserActorRef>>
    private var behaviorsValue: Option<ArrayList<Receive>> = None<ArrayList<Receive>>
    private var stashBufferValue: Option<ArrayList<Envelope>> = None<ArrayList<Envelope>>
    private var unstashedValue: Option<ArrayList<Envelope>> = None<ArrayList<Envelope>>
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


    // ---- 按需容器的访问器 ----
    // 只读：没有容器时返回共享的空容器（只读，不会被修改）
    private func childMap(): HashMap<String, SimpleUserActorRef> {
        match (childMapValue) {
            case Some(m) => m
            case None => EMPTY_CHILD_MAP
        }
    }

    // 写：首次写入时才分配真实容器
    private func childMapMut(): HashMap<String, SimpleUserActorRef> {
        match (childMapValue) {
            case Some(m) => m
            case None =>
                let m = HashMap<String, SimpleUserActorRef>(4)
                childMapValue = Some(m)
                m
        }
    }

    private func watchers(): ArrayList<SimpleUserActorRef> {
        match (watchersValue) {
            case Some(l) => l
            case None => EMPTY_WATCHERS
        }
    }

    private func watchersMut(): ArrayList<SimpleUserActorRef> {
        match (watchersValue) {
            case Some(l) => l
            case None =>
                let l = ArrayList<SimpleUserActorRef>(0)
                watchersValue = Some(l)
                l
        }
    }

    private func watching(): HashMap<String, SimpleUserActorRef> {
        match (watchingValue) {
            case Some(m) => m
            case None => EMPTY_WATCHING
        }
    }

    private func watchingMut(): HashMap<String, SimpleUserActorRef> {
        match (watchingValue) {
            case Some(m) => m
            case None =>
                let m = HashMap<String, SimpleUserActorRef>(4)
                watchingValue = Some(m)
                m
        }
    }

    private func behaviors(): ArrayList<Receive> {
        match (behaviorsValue) {
            case Some(l) => l
            case None => EMPTY_BEHAVIORS
        }
    }

    private func behaviorsMut(): ArrayList<Receive> {
        match (behaviorsValue) {
            case Some(l) => l
            case None =>
                let l = ArrayList<Receive>(0)
                behaviorsValue = Some(l)
                l
        }
    }

    private func stashBuffer(): ArrayList<Envelope> {
        match (stashBufferValue) {
            case Some(l) => l
            case None => EMPTY_STASH
        }
    }

    private func stashBufferMut(): ArrayList<Envelope> {
        match (stashBufferValue) {
            case Some(l) => l
            case None =>
                let l = ArrayList<Envelope>(0)
                stashBufferValue = Some(l)
                l
        }
    }

    private func unstashed(): ArrayList<Envelope> {
        match (unstashedValue) {
            case Some(l) => l
            case None => EMPTY_UNSTASHED
        }
    }

    private func unstashedMut(): ArrayList<Envelope> {
        match (unstashedValue) {
            case Some(l) => l
            case None =>
                let l = ArrayList<Envelope>(0)
                unstashedValue = Some(l)
                l
        }
    }

    // path 属性按需构造时使用的 system 名（与 createCell 构造 ActorPath 时一致）
    private static let actorPathSystem: String = "user-system"

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
        this.actor = actor
        this.actorFactory = factory
        this.pathString = path.getPath()
        this.guardian = guardian
        this.parentCell = parent
        this.childStrategy = props.getSupervisorStrategy()
        this.executor = ActorCellDispatcher.forName(props.getDispatcher())
        this.affinity = executor.nextAffinity()
        this.schedState = AtomicInt64(0)
        this.mailbox = CellMailbox(schedState)
        this.mailboxKind = match (props.getMailboxType()) {
            case "bounded" => if (props.getMailboxCapacity() > 0) { MAILBOX_BOUNDED } else { MAILBOX_UNBOUNDED }
            case "priority" => MAILBOX_PRIORITY
            case _ => MAILBOX_UNBOUNDED
        }
        this.capacity = props.getMailboxCapacity()
        this.overflow = props.getMailboxOverflow()
        this.boundedCountValue = if (mailboxKind == MAILBOX_BOUNDED) { Some(AtomicInt64(0)) } else { None<AtomicInt64> }
        this.priorityMailboxValue = if (mailboxKind == MAILBOX_PRIORITY) { Some(PriorityCellMailbox()) } else { None<PriorityCellMailbox> }
        this.sysHead = AtomicOptionReference<SysNode>()
        this.counters = AtomicInt64(0)
    }

    // 按需构造：pathString 已覆盖全部用途，单独保存 ActorPath 每个多占约 150 字节
    public prop path: ActorPath {
        get() { ActorPath(actorPathSystem, pathString) }
    }

    public func getName(): String {
        // 名字即路径最后一段（顶层与子 Actor 均由 createCell 以 "${prefix}/${name}" 构造路径）；
        // 调用点只在注销与诊断，按需截取，不再为每个 Actor 单独保存一份名字字符串
        let i = pathString.lastIndexOf('/')
        if (let Some(idx) <- i) {
            return pathString[(idx + 1)..]
        }
        pathString
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
        if ((schedState.load() & TERM_BIT) != 0) {
            deadLetter(message)
            return
        }
        if (mailboxKind == MAILBOX_UNBOUNDED) {
            // 邮箱已关闭（Actor 正在停止）：入队被拒绝，计为死信而不是滞留在邮箱里
            if (!mailbox.add(message, sender, senderRef)) {
                deadLetter(message)
                return
            }
        } else if (mailboxKind == MAILBOX_BOUNDED) {
            if (!reserveSlot()) {
                deadLetter(message)
                match (overflow) {
                    case MailboxOverflow.Fail => throw MailboxFullException("Actor ${pathString} 的邮箱已满（容量 ${capacity}）")
                    case _ => ()
                }
                return
            }
            if (!mailbox.add(message, sender, senderRef)) {
                boundedCount.fetchAdd(-1)
                deadLetter(message)
                return
            }
        } else {
            if (!priorityMailbox.add(message, sender, senderRef)) {
                deadLetter(message)
                return
            }
        }
        trySchedule()
    }

    /** 有界邮箱的占位计数（仅 MAILBOX_BOUNDED 会调用，此时必然已分配） */
    private prop boundedCount: AtomicInt64 {
        get() { boundedCountValue.getOrThrow() }
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
                while (MonoTime.now() < deadline && (schedState.load() & TERM_BIT) == 0) {
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
        this.incCounter(CTR_DEADLETTER_SHIFT)
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

    /** 位域计数器自增：字段饱和于 65535，不越过则不污染相邻字段 */
    private func incCounter(shift: Int64): Unit {
        let w = counters.load()
        let field = (w >> shift) & CTR_FIELD_MASK
        if (field < CTR_FIELD_MASK) {
            counters.fetchAdd(1 << shift)
        }
    }

    private func readCounter(shift: Int64): Int64 {
        (counters.load() >> shift) & CTR_FIELD_MASK
    }

    /** 已处理的消息数 */
    public func processed(): Int64 {
        this.readCounter(CTR_PROCESSED_SHIFT)
    }

    /** 被丢弃的消息数（停止后收到、有界邮箱已满等） */
    public func deadLetters(): Int64 {
        this.readCounter(CTR_DEADLETTER_SHIFT)
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
        this.readCounter(CTR_RESTART_SHIFT)
    }

    /** receive 返回 Unhandled 的次数 */
    public func unhandled(): Int64 {
        this.readCounter(CTR_UNHANDLED_SHIFT)
    }

    /** Actor 是否已完全停止 */
    public func isTerminated(): Bool {
        (schedState.load() & TERM_BIT) != 0
    }

    // ---------------- 调度 ----------------

    // 先读后 CAS：已被调度时只做一次读，避免每次 tell 都写共享缓存行。
    // CAS 失败只可能是计数并发变化（位0 仍是 0），重读重试即可
    private func trySchedule(): Unit {
        var s = schedState.load()
        while ((s & SCHED_BIT) == 0) {
            if (schedState.compareAndSwap(s, s | SCHED_BIT)) {
                executor.execute(this)
                return
            }
            s = schedState.load()
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
        unstashIndex < unstashed().size
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
                let env = unstashed()[unstashIndex]
                unstashIndex++
                if (unstashIndex >= unstashed().size) {
                    unstashedValue = Some(ArrayList<Envelope>(0))
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
                    case None => break
                }
            }
        }
        // 释放调度权（清位0）后再检查一次，防止与并发 tell 之间丢失唤醒：
        // 清位与随后的计数检查在同一原子字上顺序一致，发送方"计数++ 后置位"不会落空
        schedState.fetchAnd(SCHED_CLEAR_MASK)
        if (hasSystemMessages() || (state == STATE_RUNNING && (userNonEmpty() || hasUnstashed()))) {
            var s = schedState.load()
            while ((s & SCHED_BIT) == 0) {
                if (schedState.compareAndSwap(s, s | SCHED_BIT)) {
                    // 运行结束时一定在某个工作线程上：留在本线程，排到队尾（先轮到其他 Actor）
                    executor.executeAgain(this)
                    break
                }
                s = schedState.load()
            }
        }
    }

    // ---------------- 用户消息处理 ----------------

    private func invoke(message: Message, sender: Option<String>, senderRef: Option<ActorRef>): Unit {
        currentMessage = Some(message)
        currentSender = sender
        currentSenderRef = senderRef
        var payload = message
        // 控制消息按具体类匹配，不先做 `message is AutoReceivedMessage`：
        // 实测仓颉判断"别的包里定义的类是否实现某接口"（结果为否时）单线程约 70ns，
        // 4 个工作线程并发时每次约 0.8µs CPU、1µs 以上墙钟，线程之间互相阻塞（推测运行时要查全局的类型扩展信息）。
        // 用户消息都定义在别的包，每条消息一次这样的检查曾让 4 线程的吞吐低于单线程；
        // 按具体类匹配不争用，5 个合计约 30ns
        match (message) {
            case te: TimerEnvelope =>
                if (!timersImpl().accept(te)) {
                    clearCurrent()
                    return
                }
                payload = te.payload
                currentMessage = Some(payload)
            case _: PoisonPill =>
                this.incCounter(CTR_PROCESSED_SHIFT)
                clearCurrent()
                initiateTerminate()
                return
            case _: StopMessage =>
                // system.stop：在已入队的消息之后停止，不交给用户的 receive
                this.incCounter(CTR_PROCESSED_SHIFT)
                clearCurrent()
                initiateTerminate()
                return
            case _: Kill =>
                this.incCounter(CTR_PROCESSED_SHIFT)
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
        try {
            let result = if (behaviors().size > 0) {
                behaviors()[behaviors().size - 1](payload, contextImpl())
            } else {
                actor.receive(payload, contextImpl())
            }
            match (result) {
                case MessageResult.Handled => ()
                case MessageResult.Unhandled =>
                    this.incCounter(CTR_UNHANDLED_SHIFT)
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
        this.incCounter(CTR_PROCESSED_SHIFT)
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
        match (childMap().get(child.getName())) {
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
            childMap().values().toArray()
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
        for (c in childMap().values()) {
            c.sendSystem(SysTerminate())
        }
        childMapMut().clear()
        timersImpl().cancelAll()
        cancelReceiveTimeoutInternal()
        behaviorsMut().clear()
        // 暂存的消息放回队首，由新实例处理
        if (stashBuffer().size > 0) {
            unstashAllInternal()
        }
        try {
            let fresh = actorFactory()
            actor = fresh
            fresh.postRestart(reason)
            this.incCounter(CTR_RESTART_SHIFT)
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
                    for (c in childMap().values()) {
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
            case _: SysChildrenTerminated =>
                // 停止合批：整批子 Actor 已终止，直接收尾（childMap 一并丢弃）
                if (let Some(f) <- childBatchStoppingValue) { f.store(0) }
                childMapMut().clear()
                if (state == STATE_TERMINATING) {
                    finishTerminate()
                }
            case rt: SysReceiveTimeout => onReceiveTimeoutCheck(rt.generation)
            case _ => ()
        }
    }

    private func addWatcher(watcher: SimpleUserActorRef): Unit {
        if (refEq(watcher, this)) {
            return
        }
        for (w in watchers()) {
            if (refEq(w, watcher)) {
                return
            }
        }
        watchersMut().add(watcher)
    }

    private func removeWatcher(watcher: SimpleUserActorRef): Unit {
        var i: Int64 = 0
        while (i < watchers().size) {
            if (refEq(watchers()[i], watcher)) {
                watchersMut().remove(at: i)
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
        match (watching().get(dead.pathString)) {
            case Some(w) where refEq(w, dead) => watchingMut().remove(dead.pathString)
            case _ => return
        }
        if (state == STATE_TERMINATING || state == STATE_TERMINATED) {
            return
        }
        if (state == STATE_SUSPENDED) {
            // 挂起期间暂存，恢复后再处理
            unstashedMut().add(Envelope(Terminated(dead), None<String>, None<ActorRef>))
            return
        }
        invoke(Terminated(dead), None<String>, None<ActorRef>)
    }

    private func onChildTerminated(child: SimpleUserActorRef): Unit {
        let existed = match (childMap().get(child.getName())) {
            case Some(c) where refEq(c, child) =>
                childMapMut().remove(child.getName())
                true
            case _ => false
        }
        if (state == STATE_TERMINATING && childMap().size == 0) {
            finishTerminate()
            return
        }
        // 合批兜底：父已进入合批、但该子在此之前就自行终止（发的是旧式 SysChildTerminated，
        // 未递减计数）。它在初始化子数时已被计入，这里补一次递减，否则计数永远到不了 0。
        // 每个子只会走两条路径之一（旧式消息 或 原子递减），不会重复扣减。
        if (existed && state == STATE_TERMINATING && batchStopping()) {
            if (stoppingChildren.fetchAdd(-1) <= 1) {
                if (let Some(f) <- childBatchStoppingValue) { f.store(0) }
                childMapMut().clear()
                finishTerminate()
            }
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
        if (childMap().size == 0) {
            finishTerminate()
        } else {
            // 启动"子 Actor 全部终止"的计数（P5-2）：每个子终止时递减，
            // 减到 0 的最后一个子发一条汇总消息，而不是每子一条
            if (stoppingChildrenValue.isNone()) {
                stoppingChildrenValue = Some(AtomicInt64(0))
            }
            if (childBatchStoppingValue.isNone()) {
                childBatchStoppingValue = Some(AtomicInt64(0))
            }
            stoppingChildren.store(childMap().size)
            childBatchStopping.store(1)
            for (c in childMap().values()) {
                // 空闲的子直接在当前线程领养并完成停止（省一次调度入队 + 工作线程唤醒）；
                // 正被调度/运行中的子回退到系统消息，保证同一 Cell 不会被两个线程同时处理
                c.terminateIfIdle()
            }
        }
    }

    /**
     * 停止整棵子树时的快速路径（P5-2 深化）：若本 Cell 当前空闲（调度位为 0），
     * 用 CAS 取得调度权后在**调用方线程**直接执行停止流程；否则退化为投递 SysTerminate。
     *
     * 正确性：SCHED_BIT 只在 trySchedule 时置位、由持有者在 run() 末尾清除；
     * CAS 0→1 成功即证明没有任何工作线程持有或即将持有本 Cell，独占成立。
     */
    func terminateIfIdle(): Unit {
        var s = schedState.load()
        while ((s & SCHED_BIT) == 0) {
            if (schedState.compareAndSwap(s, s | SCHED_BIT)) {
                // 只有叶子（无子 Actor）才能内联完成：它的 initiateTerminate 会同步走到
                // finishTerminate，不依赖"子 Actor 全部终止"的系统消息。
                // 非叶子必须交给自己的 run() 循环处理——它的完成信号是投递给自己的系统消息，
                // 内联调用后若立刻释放调度权，这条消息就再也没人处理，Actor 会永远停在 TERMINATING。
                if (childMap().size == 0) {
                    try {
                        initiateTerminate()
                    } catch (e: Exception) {
                        cellLog.error("Actor ${pathString} 停止失败: ${e}")
                    }
                    // 与 run() 末尾一致：释放调度权（保留 TERM_BIT，不动计数位）
                    schedState.fetchAnd(SCHED_CLEAR_MASK)
                    return
                }
                // 非叶子：释放调度权后走常规系统消息路径（由本 Cell 的 run() 收尾）
                schedState.fetchAnd(SCHED_CLEAR_MASK)
                break
            }
            s = schedState.load()
        }
        sendSystem(SysTerminate())
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
        schedState.fetchOr(TERM_BIT)
        // 先关闭邮箱再排空：关闭与入队在同一把锁内判定，
        // 因此此后到达的消息一律被拒绝（计死信），不会滞留在邮箱里
        if (mailboxKind == MAILBOX_PRIORITY) {
            priorityMailbox.close()
        } else {
            mailbox.close()
        }
        let remaining = takeSystemMessages(true)
        guardian.onCellTerminated(this)
        for (w in watchers()) {
            w.sendSystem(SysDeathNotification(this))
        }
        watchersMut().clear()
        for ((_, target) in watching()) {
            target.sendSystem(SysUnwatch(this))
        }
        watchingMut().clear()
        if (let Some(parent) <- parentCell) {
            // 停止合批（P5-2）：父正在停止整棵子树时，每个子只递减计数，最后一个子唤醒父。
            // 判定与递减都在父的原子字上：读到标志为真才递减（标志在计数写入之前发布）。
            // 若在标志发布之前就已读到假（父刚进入终止、标志尚未置位），走旧式消息，
            // 由父的 onChildTerminated 做同样的补递减，两条路径不会双重扣减。
            if (parent.batchStopping()) {
                if (parent.stoppingChildren.fetchAdd(-1) <= 1) {
                    parent.sendSystem(SysChildrenTerminated())
                }
            } else {
                parent.sendSystem(SysChildTerminated(this))
            }
        }
        for (m in remaining) {
            handleSystemAfterDeath(m)
        }
        // 未处理的消息计为死信
        while (hasUnstashed()) {
            deadLetter(unstashed()[unstashIndex].message)
            unstashIndex++
        }
        for (env in stashBuffer()) {
            deadLetter(env.message)
        }
        stashBufferMut().clear()
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
        if (childMap().contains(name)) {
            throw InvalidActorNameException("子 Actor 名称已存在: ${pathString}/${name}")
        }
        let child = guardian.createCell(props, name, "${pathString}/${name}", Some(this))
        childMapMut().add(name, child)
        child
    }

    func spawnAnonymousChild(creator: () -> Actor): ActorRef {
        anonymousSeq++
        spawnChild(PropsFactory.create<Actor>(creator), "$${anonymousSeq}")
    }

    func childrenInternal(): Array<ActorRef> {
        let result = ArrayList<ActorRef>()
        for (c in childMap().values()) {
            result.add(c)
        }
        result.toArray()
    }

    func childInternal(name: String): Option<ActorRef> {
        match (childMap().get(name)) {
            case Some(c) => Some<ActorRef>(c)
            case None => None<ActorRef>
        }
    }

    func stopInternal(ref: ActorRef): Unit {
        let isSelf = match (ref) {
            case c: SimpleUserActorRef => refEq(c, this)
            case _ => ref.path.getPath() == pathString
        }
        if (isSelf) {
            // 停止自身：当前消息处理完后生效
            sendSystem(SysTerminate())
            return
        }
        match (ref) {
            case c: SimpleUserActorRef =>
                match (childMap().get(c.getName())) {
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
        if (!watching().contains(target.pathString)) {
            watchingMut().add(target.pathString, target)
            target.sendSystem(SysWatch(this))
        }
    }

    func unwatchInternal(ref: ActorRef): Unit {
        let target = asLocalCell(ref, "unwatch")
        match (watching().get(target.pathString)) {
            case Some(w) where refEq(w, target) =>
                watchingMut().remove(target.pathString)
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
        if (discardOld && behaviors().size > 0) {
            behaviorsMut()[behaviorsMut().size - 1] = behavior
        } else {
            behaviorsMut().add(behavior)
        }
    }

    func unbecomeInternal(): Unit {
        if (behaviors().size > 0) {
            behaviorsMut().remove(at: behaviorsMut().size - 1)
        }
    }

    func stashInternal(): Unit {
        match (currentMessage) {
            case Some(m) => stashBufferMut().add(Envelope(m, currentSender, currentSenderRef))
            case None => throw IllegalStateException("stash 只能在处理消息时调用")
        }
    }

    func unstashAllInternal(): Unit {
        if (stashBuffer().size == 0) {
            return
        }
        let merged = ArrayList<Envelope>()
        merged.add(all: stashBuffer())
        while (hasUnstashed()) {
            merged.add(unstashed()[unstashIndex])
            unstashIndex++
        }
        stashBufferMut().clear()
        unstashedValue = Some(merged)
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

    public func system(): ActorSystemView {
        match (cell.guardian.systemView()) {
            case Some(v) => v
            case None => throw UnsupportedContextException("system")
        }
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
    /** 有新消息使 Cell 变为可运行 */
    func execute(cell: SimpleUserActorRef): Unit
    /** Cell 本次运行用完配额后仍有消息：排到队尾，让同线程的其他 Actor 先运行 */
    func executeAgain(cell: SimpleUserActorRef): Unit
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
 * 工作线程：独立的就绪队列 + runNext 槽位 + 自旋后休眠
 *
 * runNext（与 Go 调度器的 runnext 同理）：本线程上运行的 Actor 唤醒了一个空闲的 Actor 时，
 * 被唤醒的 Actor 不去它的亲和线程，而是在本线程上紧接着运行。
 * 一来一回的两个 Actor 因此留在同一线程，邮箱、调度标志和消息都在本核缓存里，不跨核同步，也不需要唤醒别的线程。
 * 槽位里原有的 Actor 被挤到它自己的亲和线程，所以一个 Actor 向多个 Actor 扇出时仍然分散到各线程并行。
 *
 * runNext 可被窃取（与 Go 调度器窃取 runnext 同理）：Actor 在 receive 里同步等待刚唤醒的 Actor 时
 * （例如 Replicator.update、阻塞的 ask），被等的 Actor 正排在等待者所在线程的 runNext 里，两者互相等待。
 * 空闲线程发现某个线程的执行计数长时间不变（卡在同一次运行里）且 runNext 非空，就把它取走运行。
 * 只放入由本线程完成，取出用原子交换，本线程与窃取方只有一方拿到。
 *
 * 负载均衡（工作捐赠）：就绪队列是单消费者的，空闲线程不能直接去别的队列里取（窃取需要多消费者队列）。
 * 改为空闲线程举手（hungry），忙的线程每运行一个 Cell 前看一眼有没有人举手，有就把自己队列里的下一个 Cell 交给它。
 * 被交出的 Actor 下次与伙伴往来时，伙伴会经 runNext 跟过来，整组 Actor 迁到空闲线程上。
 */
class DispatcherWorker {
    // 连续从 runNext 取 Cell 的上限：一对频繁往来的 Actor 不能一直插队，饿死队列里的其他 Actor
    static let RUN_NEXT_LIMIT: Int64 = 16
    // 别的线程的执行计数超过这个时长没有变化，才窃取它的 runNext：一次运行最多处理 throughput 条消息，
    // 正常情况下远短于此；Go 只等 3µs，这里取大一些，避免空闲线程拆散正在往来的一对 Actor
    static let RUN_NEXT_STEAL_AFTER_US: Int64 = 100
    // 空闲线程检查别的线程的间隔：检查要读别的线程每次运行都会写的执行计数，太频繁会拖慢忙线程。
    // 之前 10µs 一次 = 每个空闲线程每秒 10 万次跨核缓存行读，14 线程就是 140 万次/秒；
    // 卡死救援（卡住的 runNext 被别的线程取走）的等待从最多 110µs 变为最多约 1.25ms，
    // 分片已改为不阻塞的 addToSetAsync，真实场景不再依赖这条快速救援路径
    static let STEAL_CHECK_INTERVAL_US: Int64 = 250
    // 空闲等待退避：200µs 起步，×5 到 100ms 封顶
    static let IDLE_BACKOFF_START_US: Int64 = 200
    static let IDLE_BACKOFF_MAX_US: Int64 = 100_000

    let dispatcher: ActorCellDispatcher
    let queue: RunQueue = RunQueue()
    let sleeping: AtomicBool = AtomicBool(false)
    let lock: Mutex = Mutex()
    let cond: Condition
    let executed: AtomicInt64 = AtomicInt64(0)
    let runNextSlot: AtomicOptionReference<SimpleUserActorRef> = AtomicOptionReference<SimpleUserActorRef>()
    // 空闲、等待别的线程捐赠 Cell；由本线程置位，由本线程或捐赠方用 CAS 清除（各自负责把 hungryCount 减一）
    let hungry: AtomicBool = AtomicBool(false)
    private var runNextStreak: Int64 = 0
    // 空闲时观察到的别的线程的执行计数与开始观察的时刻（下标同 dispatcher.allWorkers()，只由本线程读写）
    private var seenExecuted: Array<Int64> = Array<Int64>()
    private var seenSince: Array<MonoTime> = Array<MonoTime>()

    init(dispatcher: ActorCellDispatcher) {
        this.dispatcher = dispatcher
        this.cond = synchronized(lock) { lock.condition() }
    }

    /** 只由本线程调用：放入 runNext，原有的 Cell 挤回它的亲和线程 */
    func pushRunNext(cell: SimpleUserActorRef): Unit {
        if (let Some(prev) <- runNextSlot.swap(Some(cell))) {
            dispatcher.submitToAffinity(prev)
        }
    }

    /** 只由本线程调用：排到本线程队尾 */
    func pushLocal(cell: SimpleUserActorRef): Unit {
        queue.add(cell)
    }

    private func markHungry(): Unit {
        // 用 CAS 做状态转移：只有真正从 false 变 true 的那次才计数，
        // 否则"置位与计数"之间被捐赠线程抢先清位时，hungryCount 会永久偏大，
        // 忙线程每次运行前都会白跑一次捐赠扫描（取出一个 Cell 再放回队列，热路径上多两次原子操作）
        if (hungry.compareAndSwap(false, true)) {
            dispatcher.hungryCount.fetchAdd(1)
        }
    }

    private func clearHungry(): Unit {
        if (hungry.compareAndSwap(true, false)) {
            dispatcher.hungryCount.fetchAdd(-1)
        }
    }

    /** 有空闲线程时，把本线程队列里的下一个 Cell（不含 runNext）交给它 */
    private func donateIfHungry(): Unit {
        if (dispatcher.hungryCount.load() <= 0) {
            return
        }
        if (let Some(cell) <- queue.remove()) {
            if (!dispatcher.donate(cell, this)) {
                queue.add(cell)
            }
        }
    }

    private func takeNext(): Option<SimpleUserActorRef> {
        if (let Some(cell) <- runNextSlot.swap(None<SimpleUserActorRef>)) {
            if (runNextStreak < RUN_NEXT_LIMIT) {
                runNextStreak++
                return Some(cell)
            }
            // 插队次数用完：排到队尾，先轮到队列里的其他 Actor
            queue.add(cell)
        }
        runNextStreak = 0
        queue.remove()
    }

    func submit(cell: SimpleUserActorRef): Unit {
        queue.add(cell)
        if (sleeping.load()) {
            synchronized(lock) {
                cond.notifyAll()
            }
        }
    }

    /**
     * 空闲时调用：别的线程卡在同一次运行里（执行计数 RUN_NEXT_STEAL_AFTER_US 内没有变化）且 runNext 非空时，取走它的 runNext
     */
    private func stealStuckRunNext(now: MonoTime): Option<SimpleUserActorRef> {
        let ws = dispatcher.allWorkers()
        if (seenExecuted.size != ws.size) {
            seenExecuted = Array<Int64>(ws.size, repeat: -1)
            seenSince = Array<MonoTime>(ws.size, repeat: now)
        }
        for (i in 0..ws.size) {
            let w = ws[i]
            if (refEq(w, this) || w.runNextSlot.load().isNone()) {
                seenExecuted[i] = -1
                continue
            }
            let count = w.executed.load()
            if (count != seenExecuted[i]) {
                seenExecuted[i] = count
                seenSince[i] = now
                continue
            }
            if (now - seenSince[i] >= Duration.microsecond * RUN_NEXT_STEAL_AFTER_US) {
                seenExecuted[i] = -1
                if (let Some(cell) <- w.runNextSlot.swap(None<SimpleUserActorRef>)) {
                    return Some(cell)
                }
            }
        }
        None<SimpleUserActorRef>
    }

    private func runCell(cell: SimpleUserActorRef): Unit {
        donateIfHungry()
        try {
            cell.run()
        } catch (e: Exception) {
            cellLog.error("Actor Cell 执行异常: ${e}")
        }
        executed.fetchAdd(1)
    }

    func loop(spinTime: Duration): Unit {
        var idle: Int64 = 0
        var idleSince = MonoTime.now()
        var lastStealCheck = idleSince
        let stealInterval = Duration.microsecond * STEAL_CHECK_INTERVAL_US
        // 空闲等待的指数退避：自旋到期后从 200µs 起，逐级 ×5 到 100ms 封顶。
        // submit 在 sleeping 为真时必然 notifyAll，cond.wait 的超时只是丢失唤醒的安全网，
        // 之前固定 10ms 让空闲线程每秒无条件醒来 100 次；14 个线程的空转自旋+唤醒
        // 曾把空闲系统推到约 800% CPU（同吞吐是 protoactor-go 的 10 倍能耗）
        var backoffUs: Int64 = IDLE_BACKOFF_START_US
        while (true) {
            match (takeNext()) {
                case Some(cell) =>
                    if (idle > 0) {
                        idle = 0
                        clearHungry()
                        backoffUs = IDLE_BACKOFF_START_US
                    }
                    runCell(cell)
                case None =>
                    let now = MonoTime.now()
                    if (idle == 0) {
                        idleSince = now
                        markHungry()
                    }
                    idle++
                    if (now - lastStealCheck >= stealInterval) {
                        lastStealCheck = now
                        if (let Some(cell) <- stealStuckRunNext(now)) {
                            idle = 0
                            clearHungry()
                            backoffUs = IDLE_BACKOFF_START_US
                            runCell(cell)
                            continue
                        }
                    }
                    if (now - idleSince < spinTime) {
                        // 按时间自旋（而不是按次数），并让出处理器：
                        // 被本线程唤醒的仓颉线程（例如 ask 的等待方）可能排在当前处理器上
                        sleep(Duration.Zero)
                        continue
                    }
                    // 休眠期间仍然举手；idle 保持大于 0，醒来拿到 Cell 时再清除
                    idleSince = MonoTime.now() - spinTime
                    let waitUs = backoffUs
                    if (backoffUs < IDLE_BACKOFF_MAX_US) {
                        backoffUs = backoffUs * 5
                        if (backoffUs > IDLE_BACKOFF_MAX_US) {
                            backoffUs = IDLE_BACKOFF_MAX_US
                        }
                    }
                    synchronized(lock) {
                        sleeping.store(true)
                        // 设置 sleeping 之后再检查一次，避免与 submit 之间丢失唤醒
                        if (queue.isEmpty()) {
                            cond.wait(timeout: Duration.microsecond * waitUs)
                        }
                        sleeping.store(false)
                    }
            }
        }
    }
}

/**
 * 容器的 CPU 限额（向上取整的核数）；没有限额或读不到时为 None。
 * cgroup v2 读 cpu.max（"400000 100000" 或 "max 100000"），v1 读 cpu.cfs_quota_us / cpu.cfs_period_us。
 */
func cgroupCpuQuota(): Option<Int64> {
    func readText(path: String): Option<String> {
        try {
            if (!exists(path)) {
                return None<String>
            }
            Some(String.fromUtf8(File.readFrom(path)).trimAscii())
        } catch (_: Exception) {
            None<String>
        }
    }
    func toCpus(quota: Int64, period: Int64): Option<Int64> {
        if (quota <= 0 || period <= 0) {
            return None<Int64>
        }
        Some((quota + period - 1) / period)
    }
    if (let Some(text) <- readText("/sys/fs/cgroup/cpu.max")) {
        let parts = text.split(" ", removeEmpty: true)
        if (parts.size == 2) {
            if (let (Some(q), Some(p)) <- (Int64.tryParse(parts[0]), Int64.tryParse(parts[1]))) {
                return toCpus(q, p)
            }
        }
        return None<Int64>
    }
    if (let Some(q) <- readText("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")) {
        if (let Some(p) <- readText("/sys/fs/cgroup/cpu/cpu.cfs_period_us")) {
            if (let (Some(qv), Some(pv)) <- (Int64.tryParse(q), Int64.tryParse(p))) {
                return toCpus(qv, pv)
            }
        }
    }
    None<Int64>
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
    // 举手等待捐赠的空闲线程数（忙线程每次运行前读一次，大于 0 才去找）
    let hungryCount: AtomicInt64 = AtomicInt64(0)
    // 捐赠扫描起点轮转序列
    let donateSeq: AtomicInt64 = AtomicInt64(0)
    // 仓颉线程 id → 工作线程下标（见 currentWorker）；启动时写一次，在 ready 置位前发布
    private var workerIds: Array<Int64> = Array<Int64>()
    private var workerSlots: Array<Int64> = Array<Int64>()

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
        if (let Some(w) <- currentWorker()) {
            w.pushRunNext(cell)
            return
        }
        submitToAffinity(cell)
    }

    public func executeAgain(cell: SimpleUserActorRef): Unit {
        // 运行结束时一定在某个工作线程上：留在本线程，排到队尾
        if (let Some(w) <- currentWorker()) {
            w.pushLocal(cell)
            return
        }
        submitToAffinity(cell)
    }

    /**
     * 当前线程是本调度器的哪个工作线程（不是工作线程时为 None）。
     * 不用 ThreadLocal：仓颉的 ThreadLocal.get 实测单线程约 130–330ns，多线程并发时会互相阻塞（每次数微秒），
     * 每次调度都查一次会让多线程吞吐跌到单线程以下；Thread.currentThread.id 约 25ns 且不争用。
     * 线程 id 到工作线程的表在启动时建好，之后只读。
     */
    private func currentWorker(): Option<DispatcherWorker> {
        let ids = workerIds
        if (ids.size == 0) {
            return None<DispatcherWorker>
        }
        let id = Thread.currentThread.id
        let mask = ids.size - 1
        var i = id & mask
        while (true) {
            let key = ids[i]
            if (key == id) {
                return Some(workers[workerSlots[i]])
            }
            if (key < 0) {
                return None<DispatcherWorker>
            }
            i = (i + 1) & mask
        }
        None<DispatcherWorker>
    }

    func allWorkers(): Array<DispatcherWorker> {
        workers
    }

    func submitToAffinity(cell: SimpleUserActorRef): Unit {
        workers[cell.affinity % workers.size].submit(cell)
    }

    /** 把 Cell 交给一个举手的空闲线程；没有抢到返回 false */
    func donate(cell: SimpleUserActorRef, from: DispatcherWorker): Bool {
        // 起点轮转：固定从 0 号开始会让低编号线程成为捐赠热点
        let n = workers.size
        let start = donateSeq.fetchAdd(1)
        for (k in 0..n) {
            let w = workers[(start + k) % n]
            if (!refEq(w, from) && w.hungry.load() && w.hungry.compareAndSwap(true, false)) {
                hungryCount.fetchAdd(-1)
                w.submit(cell)
                return true
            }
        }
        false
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
            // 容器限额（cgroup CPU quota）小于核数时，线程数不超过限额：
            // 超出限额的线程只会互相抢时间片、被整组限流（与 Go 1.25 起按 cgroup 设置 GOMAXPROCS 同理）
            if (let Some(quota) <- cgroupCpuQuota()) {
                if (n > quota) {
                    n = quota
                }
            }
            n = envInt("CACTOR_DISPATCHER_THREADS", n, 1)
            let spin = Duration.microsecond * envInt("CACTOR_DISPATCHER_SPIN_US", 50, 0)
            let ws = Array<DispatcherWorker>(n, { _ => DispatcherWorker(this) })
            workers = ws
            // 开放寻址表（容量为 2 的幂且至少是线程数的 4 倍），键为仓颉线程 id，值为工作线程下标
            var capacity: Int64 = 8
            while (capacity < n * 4) {
                capacity *= 2
            }
            let ids = Array<Int64>(capacity, repeat: -1)
            let slots = Array<Int64>(capacity, repeat: 0)
            for (index in 0..n) {
                let w = ws[index]
                let future = spawn {
                    markActorThread()
                    markAffinityWorkerThread()
                    // 等线程 id 表发布后再开始运行 Cell，保证本线程上的调度都能找到自己
                    while (!ready.load()) {
                        sleep(Duration.Zero)
                    }
                    w.loop(spin)
                }
                let id = future.thread.id
                var i = id & (capacity - 1)
                while (ids[i] >= 0) {
                    i = (i + 1) & (capacity - 1)
                }
                ids[i] = id
                slots[i] = index
            }
            workerSlots = slots
            workerIds = ids
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
 * 初始线程数默认 16（CACTOR_BLOCKING_THREADS）；阻塞的 Actor 超过线程数时按需扩容，
 * 上限 CACTOR_BLOCKING_THREADS_MAX（默认 64），避免后来的 Actor 无限期排队。
 */
class BlockingCellExecutor <: CellExecutor {
    private let lock = Mutex()
    private let cond: Condition
    private let queue = ArrayList<SimpleUserActorRef>()
    private var head: Int64 = 0
    private let started = AtomicBool(false)
    private let threads = AtomicInt64(0)
    // 已启动的工作线程数（按需扩容时用 CAS 递增，避免多个线程同时扩容）
    private let workers = AtomicInt64(0)
    private let maxWorkers: Int64

    init() {
        this.cond = synchronized(lock) { lock.condition() }
        this.maxWorkers = envInt("CACTOR_BLOCKING_THREADS_MAX", 64, 1)
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
        var pending: Int64 = 0
        synchronized(lock) {
            queue.add(cell)
            pending = queue.size - head
            cond.notify()
        }
        // 排队任务多于工作线程数（说明线程都被阻塞中的 Actor 占住）时按需扩容
        let w = workers.load()
        if (pending > w && w < maxWorkers && workers.compareAndSwap(w, w + 1)) {
            spawn {
                markActorThread()
                workerLoop()
            }
        }
    }

    public func executeAgain(cell: SimpleUserActorRef): Unit {
        execute(cell)
    }

    private func ensureStarted(): Unit {
        if (started.compareAndSwap(false, true)) {
            let n = envInt("CACTOR_BLOCKING_THREADS", 16, 1)
            threads.store(n)
            workers.store(n)
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
