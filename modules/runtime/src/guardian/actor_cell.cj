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
// P2-4：incoming 是否有新消息的乐观提示位。权威判定仍是邮箱锁内的 inCount，
// 这个位只让消费方在锁外走"邮箱为空"的快速路径。只在"一批的第一条消息"时置位、
// 在消费方换批/确认空批时清除，因此不再有每条 tell 一次的原子 RMW。
let INCOMING_BIT: Int64 = 4
// 清除 INCOMING_BIT 的掩码（仓颉整数没有按位取反运算符，用补码常量：~4 == -5）
let INCOMING_CLEAR_MASK: Int64 = -5
// SYS_BIT（P1-17）：系统消息队列"已分配"标记。系统消息队列按需分配：
// 从未收到系统消息的 Actor 不占那个 AtomicOptionReference（约 55 B）。
// 置位前队列对象已在邮箱锁内发布，消费方看到 SYS_BIT 后即可无锁读取该引用。
let SYS_BIT: Int64 = 8
// START_BIT（P1-17）：启动待处理标记，替代每个 Actor 创建时的 SysStarted 系统消息。
let START_BIT: Int64 = 16
// 清除 START_BIT 的掩码（~16 == -17）
let START_CLEAR_MASK: Int64 = -17
// 清除调度位（bit0）的掩码：除 bit0 外全 1（仓颉整数没有按位取反运算符，用补码常量）
let SCHED_CLEAR_MASK: Int64 = -2
// 停止子树时"父线程内联领养"的子数上限：超过则改为并行分发到各子自己的 worker。
// 可用 CACTOR_INLINE_STOP_LIMIT 覆盖（设为 0 则总是并行，设为很大则总是内联），便于对照实测。
let INLINE_STOP_LIMIT: Int64 = envInt("CACTOR_INLINE_STOP_LIMIT", 4096, 0)

// BATCH_BIT（R15）：父 Actor 正在"合批停止"子树。此前是父的懒分配 Option<AtomicInt64> 字段，
// 子 Actor 在自己的线程里读它，与父的首次写入并发——多字 Option 可能读到撕裂值。
// 改为父调度字上的一位：父先写好计数（stoppingChildrenValue），再 fetchOr 置位发布；子看到该位后才读计数。
let BATCH_BIT: Int64 = 32
let BATCH_CLEAR_MASK: Int64 = -33
// CHILDREN_BIT（R8）：子 Actor 表已分配。lookup / liveCells 在别的线程读子表，先看到该位再读字段（同 SYS_BIT 的发布方式）
let CHILDREN_BIT: Int64 = 64
// STATS_BIT（R19）：异常计数（死信 / 重启 / 未处理）已分配，见 CellMailbox.anomalies
let STATS_BIT: Int64 = 128
// PAUSE_BIT（E33）：上层 Actor 失败待决，本 Actor 随之暂停处理用户消息（Akka faultSuspend 的 suspendChildren）。
// 与自身失败的 STATE_SUSPENDED 分开记：自身失败由父的决定解除，随父暂停由父恢复解除，两者互不覆盖——
// 否则父恢复时会把一个尚未得到决定的失败子 Actor 提前放行。
// 只在本 Cell 线程处理系统消息时改写；与其他线程的调度位共用一个原子字，所以用 fetchOr / fetchAnd
let PAUSE_BIT: Int64 = 256
let PAUSE_CLEAR_MASK: Int64 = -257

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

/** 父 Actor 对本 Actor 失败的决定：恢复 */
class SysResume <: SysMsg {}

/** 父 Actor 进入 / 离开"失败待决"：整棵子树随之暂停 / 继续处理用户消息（E33） */
class SysParentSuspend <: SysMsg {}

class SysParentResume <: SysMsg {}

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

/**
 * 本 Actor 发起的死亡监视（冷数据，首次 watch 时才分配；多数 Actor 用不到，不占 Cell 字段）。
 *
 * 与 Akka 的 DeathWatch 相同（E32）：
 * - 目标死亡时 Terminated 入**用户邮箱**队尾，而不是在系统消息处理中直接调用 receive——
 *   目标死前发来的消息已经在邮箱里，必然先于 Terminated 被处理（R6）；
 * - 入队后尚未处理时 unwatch，该 Terminated 被丢弃（queued）；
 * - watchWith 的自定义消息保存在 Cell 上，重启后的新实例收到的仍是它（T3）。
 */
class WatchingState {
    let targets = HashMap<String, SimpleUserActorRef>(4)
    // watchWith 登记的自定义消息（按目标路径）
    let messages = HashMap<String, Message>()
    // 已入队、尚未处理的 Terminated：目标与（取出时替换成的）自定义消息
    let queued = ArrayList<(SimpleUserActorRef, Option<Message>)>()

    /** 取出并移除 target 的待处理记录；没有（已 unwatch 或不是 DeathWatch 产生的）返回 None */
    func takeQueued(target: SimpleUserActorRef): Option<Option<Message>> {
        var i: Int64 = 0
        while (i < queued.size) {
            let (t, m) = queued[i]
            if (refEq(t, target)) {
                queued.remove(at: i)
                return Some(m)
            }
            i++
        }
        None
    }
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
 * 自适应自旋锁：先用少量纯自旋，超过阈值后让出处理器（sleep(0)）。
 *
 * 用于邮箱：每 Cell 一把、临界区极短（写一个数组槽 + 一次普通自增）。
 * 实测（8 发送方 × 100 Cell，即扇出的真实访问模式）比 std Mutex 快约 2.3 倍
 * （16ms vs 35ms / 160 万次），因为 Mutex 每次加解锁都要进运行时的重量级路径，
 * 而这里通常一次 CAS 就拿到锁。
 *
 * 2026-11-04 复核：本锁的等待路径曾改为 TTAS（先 load 再 CAS），用独立微基准
 * （8 线程 × 100 锁 × 20 万次/线程）和整机交替 A/B 实测均**无收益或略慢**
 * （CAS 177–181ms vs TTAS 180–186ms），说明本场景争用主要来自临界区本身而非
 * 等待者的 RMW；已保留原实现，避免未经验证的复杂度。
 *
 * 为什么不直接用纯自旋：扇入（多个发送方同时打同一个 Cell）时纯自旋会让持锁方被抢占后
 * 其余线程空转；32 次自旋后 sleep(0) 让出处理器，避免活锁。这是原注释"自旋锁慢 3 倍"的
 * 场景——那是纯自旋测得的，加上让出后不成立。
 *
 * 实现 Lock 接口，因此仍可继续用 synchronized(lock) { } 语法。
 */
class SpinLock <: Lock {
    private let held = AtomicBool(false)

    public func lock(): Unit {
        var spins: Int64 = 0
        while (!held.compareAndSwap(false, true)) {
            spins++
            // 前 32 次纯自旋（临界区短，通常很快释放）；之后让出处理器避免空转
            if (spins > 32) {
                sleep(Duration.Zero)
            }
        }
    }

    public func tryLock(): Bool {
        held.compareAndSwap(false, true)
    }

    public func unlock(): Unit {
        held.store(false)
    }
}

// 邮箱缓冲区参数与共享空数组哨兵（P1-13/P1-14）。放在包级：struct 不支持静态字段，
// 且哨兵只读、全进程一份。
let MAILBOX_INITIAL_CAPACITY: Int64 = 4
let MAILBOX_RETAIN_CAPACITY: Int64 = 256
let EMPTY_MESSAGES: Array<Option<Message>> = Array<Option<Message>>()
let EMPTY_SENDERS: Array<Option<String>> = Array<Option<String>>()
let EMPTY_SENDER_REFS: Array<Option<ActorRef>> = Array<Option<ActorRef>>()

/**
 * 无界邮箱：双缓冲数组（与 protoactor-go 的 goring 同类），稳态下不分配内存
 * - 发送方持锁把消息追加到 incoming；消费方把本地的 draining 读空后，持锁与 incoming 整体交换，之后无锁处理整批
 * - 不用链表的原因：仓颉的原子变量是独立的堆对象，链表每条消息至少两次分配（节点 + 原子引用）。
 *   实测多线程下分配与 GC 不能随线程数扩展（8 线程的总吞吐与 1 线程相当），数组在稳态下零分配，可以扩展；
 *   并且链表里已出队的节点仍指向后继，GC 压力大时会成串滞留，吞吐偶发跌到 1/10
 * - 不用 std Mutex：扇入时 Mutex 的加解锁要走运行时重量级路径；这里用内联自适应自旋锁
 *   （32 次自旋后 sleep(0)，扇入时避免持锁方被抢占后其余线程空转）
 * - 缓冲区在第一条消息到达时才分配，从未收到消息的 Actor 不占这部分内存；突发后超过阈值的缓冲区在交换时丢弃
 *
 * P1-18 尝试（已否决，留档备查）：曾把本类改为 struct 内联进 Cell——值类型虽省约 16 B
 * 对象头，但 `this.mailbox.add(...)` 等 mut 方法每次调用都按值复制整个 struct（双缓冲
 * 8 字段 + 锁），交替 A/B 实测 [1] 13M → 4.3M、[6] 62M → 30M msg/s，吞吐损失过半，
 * 故回退为 class。空 class 对象实测约 38 B，与本类字段合计的间接层开销远小于此损失。
 */
/** 父 Actor 的子表与它的锁（R8）：只为有子 Actor 的 Cell 分配 */
class ChildTable {
    let map = HashMap<String, SimpleUserActorRef>(4)
    // 子 Actor 的创建顺序（RestForOne 语义需要：失败的子 Actor 及其之后创建的兄弟一起重启）
    let creationOrder = ArrayList<String>()
    let lock = Mutex()
    // 重启等待旧子 Actor 终止期间保存重启原因（只在父线程访问）。放在这里而不是 Cell 上：
    // 只有有子 Actor 的 Actor 才会进入等待，叶 Actor 不为它多付一个字段
    var recreateCause: Option<Exception> = None<Exception>
}

/**
 * 异常计数（R19）：此前与已处理数共用一个 64 位字、各占 16 位，在 65535 饱和后却仍按 counter 导出
 * （rate() 变成 0）；死信由多个发送方并发自增，"先读后加"的饱和判断有竞态，会进位污染相邻的重启计数。
 * 现在各自 64 位、只对出过异常的 Actor 分配
 */
class CellAnomalies {
    let deadLetters = AtomicInt64(0)
    let restarts = AtomicInt64(0)
    let unhandled = AtomicInt64(0)
    // 最近一次**实际**完成重启（新实例创建好）的单调时刻，相对 MONO_EPOCH 的纳秒；0 = 从未重启。
    // 退避监督据此计算"重启后稳定运行了多久"：此前用"失败时刻 + 退避延迟"估算，定时器或调度一晚到，
    // 估算的运行时长就虚增，计数被误清零，maxRestarts 永远到不了（负载下无限重启）。
    // 父 Actor 在自己的线程读它（子的失败决策），所以用原子量
    let lastRestartNanos = AtomicInt64(0)
}

// Block 等待的分段长度：目标停止时没有人归还占位，等待者靠分段超时发现并放弃
let BLOCK_RECHECK: Duration = Duration.millisecond * 50

/**
 * 有界邮箱的占位（只有 MAILBOX_BOUNDED 的 Actor 分配）：计数 + Block 策略的等待者。
 * 唤醒协议：等待者先 waiters+1 再尝试占位，接收方先归还占位再读 waiters（都是 seq_cst 原子量）——
 * 两者至少有一方看到对方：要么等待者占位成功，要么接收方看到 waiters>0 去唤醒；
 * 唤醒在锁内，等待者从"占位失败"到 wait 也一直持锁，所以不会漏掉
 */
class BoundedSlots {
    let count = AtomicInt64(0)
    let waiters = AtomicInt64(0)
    private let lock = Mutex()
    private let cond: Condition
    private let warnedOnWorker = AtomicBool(false)

    init() {
        this.cond = synchronized(lock) { lock.condition() }
    }

    func signal(): Unit {
        synchronized(lock) {
            cond.notify()
        }
    }

    /** 等到占到一个位置（true）、超时或 Actor 停止（false） */
    func awaitSlot(capacity: Int64, timeout: Duration, stopped: () -> Bool): Bool {
        let deadline = MonoTime.now() + timeout
        waiters.fetchAdd(1)
        try {
            synchronized(lock) {
                while (!stopped()) {
                    if (count.fetchAdd(1) < capacity) {
                        return true
                    }
                    count.fetchAdd(-1)
                    let remaining = deadline - MonoTime.now()
                    if (remaining <= Duration.Zero) {
                        return false
                    }
                    // 分段等待：Actor 停止时没有人归还占位，靠这一段超时重新检查
                    cond.wait(timeout: if (remaining < BLOCK_RECHECK) { remaining } else { BLOCK_RECHECK })
                }
                false
            }
        } finally {
            waiters.fetchAdd(-1)
        }
    }

    /** 每个邮箱只告警一次：说明为什么没有等待 */
    func warnBlockedOnWorker(path: String): Unit {
        if (warnedOnWorker.compareAndSwap(false, true)) {
            cellLog.warn("Actor ${path} 的有界邮箱已满（Block 策略），发送方在 Actor 处理方法内：不等待，按超时计死信。" +
                "Actor 之间的背压请用 ask / 确认消息实现，Block 只适用于 Actor 之外的生产者（main、外部线程、blocking 调度器）")
        }
    }
}

/** 单调时钟的固定基点：把 MonoTime 存成原子 Int64 用 */
let MONO_EPOCH: MonoTime = MonoTime.now()

func monoNanos(): Int64 {
    let n = (MonoTime.now() - MONO_EPOCH).toNanoseconds()
    if (n <= 0) { 1 } else { n }
}

class CellMailbox {
    // 初始容量 4：多数 Actor 每条消息处理完即空（drain 场景），数组越小、长期占用越少；
    // 突发按 2× 增长，超过 MAILBOX_RETAIN_CAPACITY 的大缓冲在换批时丢弃

    // 内联自适应自旋锁（P1-14）：不再持有独立的 SpinLock 对象（每实例约 25 B），
    // 用 AtomicBool 字段 + 手动 lock/unlock。语义与原 SpinLock 相同（32 次自旋后 sleep(0)）。
    private let held = AtomicBool(false)
    // Cell 的调度状态字：INCOMING_BIT 是"incoming 可能有消息"的乐观提示（权威判定见下方 inCount）
    private let state: AtomicInt64
    // 待处理计数（P2-4）：发送方本来就持有邮箱锁才写缓冲区，计数放在同一临界区内做普通自增；
    // 每条 tell 因此不再对 schedState 做一次 fetchAdd（少一次跨核缓存行的 RMW）。
    // 锁外的"邮箱是否非空"快速判定改读 schedState 的 INCOMING_BIT，该位只在
    // "一批的第一条消息"入队时置位、在消费方换批/确认空批时清除，也是每批一次而非每条一次。
    //
    // 丢失唤醒的配对关系（INCOMING_BIT 只是提示，权威仍是锁内 inCount）：
    //   发送方：lock { 写槽位；inCount++；首条时置 INCOMING_BIT } → 读/置 SCHED_BIT
    //   消费方：清 SCHED_BIT → 读 INCOMING_BIT（refill 快速路径 / nonEmpty）
    // INCOMING_BIT 与 SCHED_BIT 在同一个原子字上，两侧读的都是同一字的全序值：
    // 若消费方没看到提示，说明它的读排在发送方置位之前，而它清 SCHED_BIT 在其读之前，
    // 发送方之后的置位必然成功；若发送方先入队，则提示必然可见。任一交错都不会丢失唤醒。
    // 已关闭：Actor 停止后不再接受新消息。判定在锁内与入队原子完成，
    // 否则"发送方刚检查完 terminated"与"Cell 排空邮箱"之间的窗口会让消息滞留（不投递也不计死信）
    private var closed: Bool = false
    // 共享空数组哨兵（P1-13）：仓颉里 `Option<Array<T>>` 字段占 32 字节，
    // 而普通数组引用只占 8 字节。邮箱有 6 个这样的字段，全部改成"共享空数组 + size==0 判定"
    // 后每 Actor 省约 144 字节。哨兵只读：size==0 时一律换成新分配的真实数组后再写入。
    // in* 只在持锁时访问；首次入队时才分配
    private var inMessages: Array<Option<Message>> = EMPTY_MESSAGES
    private var inSenders: Array<Option<String>> = EMPTY_SENDERS
    private var inSenderRefs: Array<Option<ActorRef>> = EMPTY_SENDER_REFS
    private var inCount: Int64 = 0
    // dr* 只由持有调度权的消费方访问；首次 refill 时才分配
    private var drMessages: Array<Option<Message>> = EMPTY_MESSAGES
    private var drSenders: Array<Option<String>> = EMPTY_SENDERS
    private var drSenderRefs: Array<Option<ActorRef>> = EMPTY_SENDER_REFS
    private var drCount: Int64 = 0
    private var drIndex: Int64 = 0
    // 系统消息栈（P1-17）：首次收到系统消息时在锁内分配，SYS_BIT 置位后发布。
    // 从未收到系统消息的 Actor（常见叶 Actor）不占这个原子对象（约 55 B）。
    private var sysHeadValue: Option<AtomicOptionReference<SysNode>> = None<AtomicOptionReference<SysNode>>
    // 异常计数（R19）：绝大多数 Actor 永远为 0，按需分配；可能由任意线程首次触发（死信来自发送方），
    // 因此在邮箱锁内分配、经 STATS_BIT 发布
    private var anomalyValue: Option<CellAnomalies> = None<CellAnomalies>

    init(state: AtomicInt64) {
        this.state = state
    }

    func anomalies(): CellAnomalies {
        if ((state.load() & STATS_BIT) != 0) {
            return anomalyValue.getOrThrow()
        }
        lockBox()
        try {
            match (anomalyValue) {
                case Some(a) => a
                case None =>
                    let a = CellAnomalies()
                    anomalyValue = Some(a)
                    state.fetchOr(STATS_BIT)
                    a
            }
        } finally {
            unlockBox()
        }
    }

    /** 读取方：没有发布过就是全 0 */
    func anomaliesIfAny(): Option<CellAnomalies> {
        if ((state.load() & STATS_BIT) == 0) {
            return None<CellAnomalies>
        }
        anomalyValue
    }

    /** 调度权持有者在释放调度位之前调用：出队缓冲里是否还有没处理的消息（dr* 只属于持有者） */
    func ownerBacklog(): Bool {
        drIndex < drCount
    }

    /** 释放调度位之后可以安全调用：只读原子提示位 */
    func incomingHint(): Bool {
        (state.load() & INCOMING_BIT) != 0
    }

    private func lockBox(): Unit {
        var spins: Int64 = 0
        while (!held.compareAndSwap(false, true)) {
            spins++
            if (spins > 32) {
                sleep(Duration.Zero)
            }
        }
    }

    private func unlockBox(): Unit {
        held.store(false)
    }

    /** 入队；邮箱已关闭时返回 false（消息应计为死信） */
    func add(message: Message, sender: Option<String>, senderRef: Option<ActorRef>): Bool {
        lockBox()
        try {
            if (closed) {
                return false
            }
            if (inMessages.size == 0) {
                // 首条消息（或上一批换回了空数组）：按初始容量分配
                inMessages = Array<Option<Message>>(MAILBOX_INITIAL_CAPACITY, repeat: None<Message>)
            }
            let msgs = inMessages
            if (inCount == msgs.size) {
                growIncoming()
            }
            // Array 是引用类型：通过局部引用写入与字段里的是同一个数组
            let slotMsgs = inMessages
            slotMsgs[inCount] = Some(message)
            // sender 两件套懒分配：绝大多数消息不带发送方（普通 tell），
            // 只有第一条带发送方的消息才分配这两个数组（各约 100–140 字节）
            if (sender.isSome()) {
                if (inSenders.size != slotMsgs.size) {
                    inSenders = Array<Option<String>>(slotMsgs.size, repeat: None<String>)
                }
                inSenders[inCount] = sender
            }
            if (senderRef.isSome()) {
                if (inSenderRefs.size != slotMsgs.size) {
                    inSenderRefs = Array<Option<ActorRef>>(slotMsgs.size, repeat: None<ActorRef>)
                }
                inSenderRefs[inCount] = senderRef
            }
            inCount++
            // 一批的第一条消息置提示位；后续消息只做普通自增，不再碰原子字。
            if (inCount == 1) {
                state.fetchOr(INCOMING_BIT)
            }
            // 调度位由调用方 trySchedule 单独做一次 fetchOr 选举（不能与计数合并自加：
            // 整数自加遇到 bit0 已置位会向上进位，污染终止位）。
            true
        } finally {
            unlockBox()
        }
    }

    /** 关闭邮箱：与入队在同一把锁内判定，关闭后不再接受新消息 */
    func close(): Unit {
        lockBox()
        try {
            closed = true
        } finally {
            unlockBox()
        }
    }

    /**
     * 在邮箱锁内投递系统消息（P1-17）：与 close 互斥，所以停止后的消息不会滞留在系统栈里。
     * 首次投递时分配系统栈并置 SYS_BIT；返回 false 表示邮箱已关闭，调用方按"死亡后"规则处理。
     */
    func pushSystem(message: SysMsg): Bool {
        lockBox()
        try {
            if (closed) {
                return false
            }
            let head = match (sysHeadValue) {
                case Some(h) => h
                case None =>
                    let h = AtomicOptionReference<SysNode>()
                    sysHeadValue = Some(h)
                    // 发布顺序：先写字段，再置 SYS_BIT（seq_cst RMW）。
                    // 消费方看到 SYS_BIT 后读该字段，acquire 语义保证能看到上面的写。
                    state.fetchOr(SYS_BIT)
                    h
            }
            // 必须用 CAS：邮箱锁只串行化"推送方之间"，消费方 takeSystemMessages 的 swap 不持锁。
            // 若用 load + swap，消费方在两步之间取走 cur，随后 swap 会把已取走的节点重新挂回栈上，
            // 同一条系统消息被处理两次（R4：监督指令重复执行、ReceiveTimeout 定时链倍增）。
            while (true) {
                let cur = head.load()
                if (let Some(c) <- cur) {
                    if (refEq(c, SYS_CLOSED)) {
                        return false
                    }
                }
                if (head.compareAndSwap(cur, SysNode(message, cur))) {
                    return true
                }
            }
            return false
        } finally {
            unlockBox()
        }
    }

    /** 已发布的系统栈（调用方必须先观察到 SYS_BIT；此后该引用不再变化） */
    func sysHeadPublished(): AtomicOptionReference<SysNode> {
        sysHeadValue.getOrThrow()
    }

    private func growIncoming(): Unit {
        let oldMsgs = inMessages
        let old = oldMsgs.size
        let capacity = if (old == 0) { MAILBOX_INITIAL_CAPACITY } else { old * 2 }
        let messages = Array<Option<Message>>(capacity, repeat: None<Message>)
        oldMsgs.copyTo(messages, 0, 0, inCount)
        inMessages = messages
        // sender 数组可能仍是共享空哨兵（从未有过带发送方的消息）；已分配则同步扩容
        if (inSenders.size > 0) {
            let senders = Array<Option<String>>(capacity, repeat: None<String>)
            inSenders.copyTo(senders, 0, 0, inCount)
            inSenders = senders
        }
        if (inSenderRefs.size > 0) {
            let senderRefs = Array<Option<ActorRef>>(capacity, repeat: None<ActorRef>)
            inSenderRefs.copyTo(senderRefs, 0, 0, inCount)
            inSenderRefs = senderRefs
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
        // refill 成功后 drMessages 必然非空（inCount > 0）；sender 数组可能仍为空哨兵（无发送方）
        let msgs = drMessages
        let i = drIndex
        drIndex++
        let message = msgs[i]
        // 逐项清空：已处理的消息不能被缓冲区继续引用（消息可能很大），交换回 incoming 时也就是干净的
        msgs[i] = None<Message>
        if (i < drSenders.size) {
            let s = drSenders[i]
            if (s.isSome()) { drSenders[i] = None<String> }
            lastSender = s
        } else {
            lastSender = None<String>
        }
        if (i < drSenderRefs.size) {
            let r = drSenderRefs[i]
            if (r.isSome()) { drSenderRefs[i] = None<ActorRef> }
            lastSenderRef = r
        } else {
            lastSenderRef = None<ActorRef>
        }
        message
    }

    /** 本地批已读完：与 incoming 交换；incoming 为空时返回 false */
    private func refill(): Bool {
        drIndex = 0
        drCount = 0
        // 锁外快速路径：提示位未置说明上一批之后没有新消息（位与 SCHED_BIT 同字，全序可靠）
        if ((state.load() & INCOMING_BIT) == 0) {
            return false
        }
        lockBox()
        try {
            if (inCount == 0) {
                // 提示位过期（换批与清位之间被并发读到）：清位后返回空
                state.fetchAnd(INCOMING_CLEAR_MASK)
                return false
            }
            // 与 dr 交换：incoming 的数组交给消费方，上一轮消费完的数组换回 incoming 复用。
            // 空数组（哨兵或首次的空 dr）换回去表示"尚无缓冲区"，下一次 add 按初始容量重新分配。
            let messages = drMessages
            let oldSenders = drSenders
            let oldSenderRefs = drSenderRefs
            drMessages = inMessages
            drSenders = inSenders
            drSenderRefs = inSenderRefs
            drCount = inCount
            if (messages.size > MAILBOX_RETAIN_CAPACITY) {
                // 突发留下的大缓冲区不再复用，下次入队时再按初始容量分配
                inMessages = EMPTY_MESSAGES
                inSenders = EMPTY_SENDERS
                inSenderRefs = EMPTY_SENDER_REFS
            } else {
                inMessages = messages
                inSenders = oldSenders
                inSenderRefs = oldSenderRefs
            }
            inCount = 0
            // 与换批在同一临界区内清提示位：之后的发送方会重新置位
            state.fetchAnd(INCOMING_CLEAR_MASK)
        } finally {
            unlockBox()
        }
        drCount > 0
    }

    func nonEmpty(): Bool {
        drIndex < drCount || (state.load() & INCOMING_BIT) != 0
    }

    /** 近似计数（仅用于诊断，可从任意线程调用） */
    func count(): Int64 {
        let local = drCount - drIndex
        var incoming: Int64 = 0
        lockBox()
        try {
            incoming = inCount
        } finally {
            unlockBox()
        }
        incoming + (if (local > 0) { local } else { 0 })
    }
}

/**
 * 优先级邮箱：按 Message.priority() 从高到低处理，同优先级先进先出。
 * 列表按优先级升序保存，新消息插在同优先级元素之前，因此 removeLast 取到的是最高优先级里最早的消息。
 */
class PriorityCellMailbox {
    private let lock = SpinLock()
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
// 重启中（E33）：旧实例已 preRestart、子 Actor 已收到停止请求，等它们全部终止后才创建新实例（Akka faultRecreate）。
// 期间不处理用户消息；此前不等就立即建新实例，新旧子 Actor 同名并存，旧子 Actor 还可能在处理消息
let STATE_RECREATING: Int64 = 4

let MAILBOX_UNBOUNDED: Int64 = 0
let MAILBOX_BOUNDED: Int64 = 1
let MAILBOX_PRIORITY: Int64 = 2

/**
 * 用户 Actor 的引用与执行单元（Actor Cell）
 */
public class SimpleUserActorRef <: RunLink & ActorRef {
    private var actor: Actor
    private let actorFactory: () -> Actor
    // 名字（= 路径最后一段）：与父的 childMap 键是同一个 String 实例，子 Actor 不额外占内存。
    // 完整路径不再常驻——按需由"父路径 + 名字"拼出（P1-11），每 Actor 省约 143 字节。
    private let actorName: String
    // 路径哈希：创建时算一次（名字 + 父链混合），避免 hashCode() 每次都拼路径
    private let pathHashValue: Int32
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
    private var mailbox: CellMailbox
    private let mailboxKind: Int64
    private let capacity: Int64
    private let overflow: MailboxOverflow
    // 仅 MAILBOX_BOUNDED 使用：懒分配，无界/优先级邮箱（绝大多数 Actor）不占这个原子对象
    private var boundedCountValue: Option<BoundedSlots> = None<BoundedSlots>
    private let priorityMailboxValue: Option<PriorityCellMailbox>
    // 只在 mailboxKind == MAILBOX_PRIORITY 时使用
    private prop priorityMailbox: PriorityCellMailbox {
        get() {
            priorityMailboxValue.getOrThrow()
        }
    }

    // 系统消息队列存放在 CellMailbox 里（P1-17：按需分配，见 CellMailbox.pushSystem）

    // ---- 调度与状态 ----
    // 调度状态字（P2-1/P1-6，P2-4 后为两个标志 + 一个提示位）
    //   bit0 = SCHED_BIT：同一时刻最多一个线程持有本 Cell 的调度权（CAS 0→1 后 execute）
    //   bit1 = TERM_BIT：已终止（terminated 标志的镜像，热路径每条 tell 读一次）
    //   bit2 = INCOMING_BIT：incoming 有新消息的乐观提示（每批首条置位、换批清除）
    // 旧版本把 incoming 计数本身放在 bit2..，发送方每条 tell 都要 fetchAdd 一次；
    // 现在每批只写一次提示位，权威计数在邮箱锁内（见 CellMailbox）。
    // 丢失唤醒论证：提示位与 SCHED_BIT 同字，发送方"锁内入队/置提示 → 读置 SCHED"、
    // 消费方"清 SCHED → 读提示"，两读在同一原子字的总序上，任一交错不会双丢。
    private let schedState: AtomicInt64
    // 已处理消息数（R19）：64 位，不再在 65535 饱和。死信 / 重启 / 未处理是异常计数，按需分配在邮箱里
    private let processedCount: AtomicInt64

    // ---- 只在 Cell 线程内访问 ----
    private var state: Int64 = STATE_RUNNING
    // 以下容器按需分配：没有子 Actor / 不监视别人 / 没有 stash 的 Actor 不再各自分配一个空容器
    // （实测空的 HashMap 约 280 字节、空的 ArrayList 约 96 字节，六个容器合计约 944 字节，
    //   而绝大多数 Actor 从不使用它们）
    // 子 Actor 表（R8）：只有父线程写，写时持表锁；lookup / liveCells 从别的线程持表锁读。首次分配后经 CHILDREN_BIT 发布
    private var childTableValue: Option<ChildTable> = None<ChildTable>
    private var anonymousSeq: Int64 = 0
    // 停止合批（P5-2）：父进入"停止整棵子树"时把子数记入 stoppingChildren 并置 BATCH_BIT；
    // 每个子终止恰好把计数减 1，减到 0 的那一方让父收尾。这样百万子 Actor 不必各发一条
    // SysChildTerminated 在父的 sysHead 上做百万次 CAS 争用与百万次 childMap.remove。
    // 懒分配：只有真正进入"停止有子 Actor 的子树"时才创建计数对象，叶 Actor（绝大多数）不为它付费。
    // 只有父写一次（置 BATCH_BIT 之前）；子看到 BATCH_BIT 之后才读
    private var stoppingChildrenValue: Option<AtomicInt64> = None<AtomicInt64>
    private prop stoppingChildren: AtomicInt64 {
        get() { stoppingChildrenValue.getOrThrow() }
    }
    /** 子侧读父的合批标志：只读原子调度字（R15） */
    private func batchStopping(): Bool {
        (schedState.load() & BATCH_BIT) != 0
    }
    private var watchersValue: Option<ArrayList<SimpleUserActorRef>> = None<ArrayList<SimpleUserActorRef>>
    private var watchingValue: Option<WatchingState> = None<WatchingState>
    private var behaviorsValue: Option<ArrayList<Receive>> = None<ArrayList<Receive>>
    private var stashBufferValue: Option<ArrayList<Envelope>> = None<ArrayList<Envelope>>
    private var unstashedValue: Option<ArrayList<Envelope>> = None<ArrayList<Envelope>>
    private var unstashIndex: Int64 = 0
    private var currentMessage: Option<Message> = None<Message>
    private var currentSender: Option<String> = None<String>
    private var currentSenderRef: Option<ActorRef> = None<ActorRef>
    private var timeoutStateValue: Option<ReceiveTimeoutState> = None<ReceiveTimeoutState>
    private var cellTimersValue: Option<CellTimers> = None<CellTimers>
    private var contextValue: Option<CellContext> = None<CellContext>


    // ---- 按需容器的访问器 ----
    // 只读：没有容器时返回共享的空容器（只读，不会被修改）
    // 父线程自己读子表不需要加锁：唯一的写者就是它自己
    private func childMap(): HashMap<String, SimpleUserActorRef> {
        match (childTableValue) {
            case Some(t) => t.map
            case None => EMPTY_CHILD_MAP
        }
    }

    // 写：首次写入时才分配真实容器，赋值后置 CHILDREN_BIT 发布给其他线程
    private func childTable(): ChildTable {
        match (childTableValue) {
            case Some(t) => t
            case None =>
                let t = ChildTable()
                childTableValue = Some(t)
                schedState.fetchOr(CHILDREN_BIT)
                t
        }
    }

    private func childPut(name: String, child: SimpleUserActorRef): Unit {
        let t = childTable()
        synchronized(t.lock) {
            t.map.add(name, child)
            if (!t.creationOrder.contains(name)) {
                t.creationOrder.add(name)
            }
        }
    }

    private func childRemove(name: String): Unit {
        if (let Some(t) <- childTableValue) {
            synchronized(t.lock) {
                t.map.remove(name)
                t.creationOrder.removeIf({ x => x == name })
            }
        }
    }

    private func childClear(): Unit {
        if (let Some(t) <- childTableValue) {
            synchronized(t.lock) {
                t.map.clear()
            }
        }
    }

    /** 其他线程读子表：先确认已发布，再持表锁读 */
    private func publishedChildTable(): Option<ChildTable> {
        if ((schedState.load() & CHILDREN_BIT) == 0) {
            return None<ChildTable>
        }
        childTableValue
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
            case Some(w) => w.targets
            case None => EMPTY_WATCHING
        }
    }

    private func watchingMut(): HashMap<String, SimpleUserActorRef> {
        watchState().targets
    }

    private func watchState(): WatchingState {
        match (watchingValue) {
            case Some(w) => w
            case None =>
                let w = WatchingState()
                watchingValue = Some(w)
                w
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

    // 兼容性构造器：path 参数保留（旧调用方签名不变），但 Cell 的完整路径由
    // "guardian 顶层前缀 + name"惰性派生（P1-11）；仓库内唯一的构造点是 createCell。
    public init(name: String, actor: Actor, path: ActorPath, guardian: RuntimeUserGuardian) {
        this(name, actor, { => actor }, guardian, None<SimpleUserActorRef>, PropsFactory.create<Actor>({ => actor }))
    }

    public init(name: String, actor: Actor, factory: () -> Actor, path: ActorPath, guardian: RuntimeUserGuardian) {
        this(name, actor, factory, guardian, None<SimpleUserActorRef>, PropsFactory.create<Actor>(factory))
    }

    init(name: String, actor: Actor, factory: () -> Actor, guardian: RuntimeUserGuardian,
        parent: Option<SimpleUserActorRef>, props: Props<Actor>) {
        this.actor = actor
        this.actorFactory = factory
        this.actorName = name
        this.pathHashValue = match (parent) {
            case Some(p) => mixPathHash(p.pathHash(), name)
            case None => mixPathHash(0, name)
        }
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
        this.boundedCountValue = if (mailboxKind == MAILBOX_BOUNDED) { Some(BoundedSlots()) } else { None<BoundedSlots> }
        this.priorityMailboxValue = if (mailboxKind == MAILBOX_PRIORITY) { Some(PriorityCellMailbox()) } else { None<PriorityCellMailbox> }
        this.processedCount = AtomicInt64(0)
    }

    // 完整路径按需拼接：顶层 = "/user/<名字>"，子 Actor = 父路径 + "/" + 名字。
    // 路径字符串不再常驻每个 Cell（原先每个 Actor 常驻一份完整路径，约 143 字节）。
    public func fullPath(): String {
        match (parentCell) {
            case Some(p) => "${p.fullPath()}/${actorName}"
            case None => "${guardian.topLevelPathPrefix()}/${actorName}"
        }
    }

    /** 完整路径（兼容旧字段访问；按需拼接，勿在热路径反复取） */
    public prop pathString: String {
        get() { fullPath() }
    }

    // 按需构造：pathString 已覆盖全部用途，单独保存 ActorPath 每个多占约 150 字节
    public prop path: ActorPath {
        get() { ActorPath(actorPathSystem, fullPath()) }
    }

    public func getName(): String {
        // 名字即路径最后一段：创建时由调用方传入，与父的 childMap 键是同一个 String 实例
        actorName
    }

    /** 路径哈希：创建时由名字与父链混合算出，避免 hashCode() 每次拼路径 */
    func pathHash(): Int32 {
        pathHashValue
    }

    /**
     * 路径哈希混合：把父链哈希与本级名字揉成一个非负 Int32。
     * 与"完整路径的 String.hashCode"一样保证"同一路径同一哈希"，但不需要常驻路径字符串。
     */
    private static func mixPathHash(parentHash: Int32, name: String): Int32 {
        // 只用乘加与掩码（避开异或/幂运算的溢出与语义歧义）
        let h = (Int64(parentHash) * 31 + Int64(name.hashCode())) & 0x7FFFFFFF
        Int32(h)
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
                    case MailboxOverflow.Fail => throw MailboxFullException("Actor ${fullPath()} 的邮箱已满（容量 ${capacity}）")
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

    /**
     * 运行时自己生成、发给本 Actor 的用户消息（如 DeathWatch 的 Terminated）：
     * 不受有界邮箱容量限制——邮箱满时丢掉 Terminated 会让监视者永远不知道目标已死。
     */
    private func enqueueGenerated(message: Message): Unit {
        if (mailboxKind != MAILBOX_BOUNDED) {
            enqueue(message, None<String>, None<ActorRef>)
            return
        }
        if ((schedState.load() & TERM_BIT) != 0) {
            deadLetter(message)
            return
        }
        boundedCount.fetchAdd(1)
        if (!mailbox.add(message, None<String>, None<ActorRef>)) {
            boundedCount.fetchAdd(-1)
            deadLetter(message)
            return
        }
        trySchedule()
    }

    /** 有界邮箱的占位计数（仅 MAILBOX_BOUNDED 会调用，此时必然已分配） */
    private prop boundedCount: AtomicInt64 {
        get() { boundedCountValue.getOrThrow().count }
    }

    /**
     * 有界邮箱占位。Block 策略（E39）：
     * - 在亲和工作线程上（发送方是默认调度器上的 Actor）**不等待**，直接按超时处理（DropNew 计死信）：
     *   等待会占住工作线程，接收方若亲和到同一线程就互相等到超时，期间该线程上的其他 Actor 全部停摆；
     * - 其他线程（main、外部生产者、blocking 调度器）在条件变量上等待，接收方每出队一条、有等待者时唤醒一个，
     *   不再"让出 64 次后每 50µs 醒一次"地轮询。
     */
    private func reserveSlot(): Bool {
        let slots = boundedCountValue.getOrThrow()
        if (slots.count.fetchAdd(1) < capacity) {
            return true
        }
        slots.count.fetchAdd(-1)
        match (overflow) {
            case MailboxOverflow.Block(timeout) =>
                if (isAffinityWorkerThread()) {
                    slots.warnBlockedOnWorker(fullPath())
                    return false
                }
                slots.awaitSlot(capacity, timeout, { => (schedState.load() & TERM_BIT) != 0 })
            case _ => false
        }
    }

    /** 出队一条后归还占位；有发送方在等空位时唤醒一个 */
    private func releaseSlot(): Unit {
        let slots = boundedCountValue.getOrThrow()
        slots.count.fetchAdd(-1)
        if (slots.waiters.load() > 0) {
            slots.signal()
        }
    }

    func deadLetter(message: Message): Unit {
        mailbox.anomalies().deadLetters.fetchAdd(1)
        guardian.publishDeadLetter(message, fullPath())
    }

    /** 投递系统消息；目标已停止时按“死亡后”规则处理（例如对已停止的 Actor 发起 watch 会立即收到通知） */
    func sendSystem(message: SysMsg): Unit {
        // pushSystem 在邮箱锁内与 close 互斥：close 前的入队一定被随后的排空看到，
        // close 后的入队直接走死亡后规则，不会滞留在系统栈里。
        if (!mailbox.pushSystem(message)) {
            handleSystemAfterDeath(message)
            return
        }
        trySchedule()
    }

    private func hasSystemMessages(): Bool {
        if ((schedState.load() & SYS_BIT) == 0) {
            return false
        }
        match (mailbox.sysHeadPublished().load()) {
            case Some(c) => !refEq(c, SYS_CLOSED)
            case None => false
        }
    }

    /** 取下栈中全部消息并按发送顺序返回；closing 为 true 时同时关闭队列 */
    private func takeSystemMessages(closing: Bool): ArrayList<SysMsg> {
        let result = ArrayList<SysMsg>(0)
        // 从未分配过系统栈：没有消息可取（closing 时也不需要额外标记——
        // 之后的 sendSystem 会因邮箱已关闭而走死亡后规则）
        if ((schedState.load() & SYS_BIT) == 0) {
            return result
        }
        let head = mailbox.sysHeadPublished()
        let taken = if (closing) {
            head.swap(SYS_CLOSED)
        } else {
            head.swap(None<SysNode>)
        }
        var cur = taken
        while (let Some(node) <- cur) {
            if (refEq(node, SYS_CLOSED)) {
                // 只有 Cell 线程会关闭队列，正常不会走到这里；保持关闭状态
                head.store(SYS_CLOSED)
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

    /** 已处理的消息数（64 位单调计数） */
    public func processed(): Int64 {
        processedCount.load()
    }

    /** 被丢弃的消息数（停止后收到、有界邮箱已满等） */
    public func deadLetters(): Int64 {
        match (mailbox.anomaliesIfAny()) {
            case Some(a) => a.deadLetters.load()
            case None => 0
        }
    }

    /** 邮箱中待处理的消息数 */
    public func pending(): Int64 {
        if (mailboxKind == MAILBOX_PRIORITY) {
            priorityMailbox.count()
        } else {
            mailbox.count()
        }
    }

    /** ActorRef.pendingCount：SmallestMailbox 路由策略用（E9，2026-11-06） */
    public func pendingCount(): Int64 {
        pending()
    }

    /** 被监督重启的次数 */
    public func restarts(): Int64 {
        match (mailbox.anomaliesIfAny()) {
            case Some(a) => a.restarts.load()
            case None => 0
        }
    }

    /** 调度器时钟相对 MONO_EPOCH 的纳秒数（> 0），与 lastRestartTime 的还原方式配对 */
    private func schedulerNanos(): Int64 {
        let n = (guardian.scheduler().monotonicNow() - MONO_EPOCH).toNanoseconds()
        if (n <= 0) { 1 } else { n }
    }

    /** 最近一次实际完成重启的时刻（调度器时钟）；从未重启返回 None。可从任意线程调用 */
    func lastRestartTime(): Option<MonoTime> {
        match (mailbox.anomaliesIfAny()) {
            case Some(a) =>
                let n = a.lastRestartNanos.load()
                if (n > 0) {
                    Some(MONO_EPOCH + Duration.nanosecond * n)
                } else {
                    None<MonoTime>
                }
            case None => None<MonoTime>
        }
    }

    /**
     * 交给监督策略的失败信息：带上实际重启时刻（退避据此判断重启后稳定运行了多久）与失败时刻，
     * 两者都按系统调度器的时钟（E52），注入虚拟时钟时退避复位 / 次数窗口随虚拟时间计算
     */
    func failureInfo(cause: Exception): ActorFailure {
        ActorFailure(this, cause, 1, lastRestartAt: lastRestartTime(), failedAt: Some(guardian.scheduler().monotonicNow()))
    }

    /** receive 返回 Unhandled 的次数 */
    public func unhandled(): Int64 {
        match (mailbox.anomaliesIfAny()) {
            case Some(a) => a.unhandled.load()
            case None => 0
        }
    }

    /** Actor 是否已完全停止 */
    public func isTerminated(): Bool {
        (schedState.load() & TERM_BIT) != 0
    }

    // ---------------- 调度 ----------------

    /**
     * 创建后启动（P1-17）：替代创建时的 `SysStarted` 系统消息。
     * 置 START_BIT 并调度；run() 在进入消息循环前先执行 onStarted，
     * 因此用户消息一定在启动之后处理（createCell 在发布引用之前调用本方法）。
     */
    func scheduleStart(): Unit {
        schedState.fetchOr(START_BIT)
        trySchedule()
    }

    // 读优先 + 单次 fetchOr，不重试：
    // ① 不能 load+CAS 循环——schedState 低位承载 incoming 计数，并发计数自增会让别人的 CAS
    //    失败，多发送方扇出同一 Actor 时退化成重试风暴（实测 T1 25.0M → T14 2.8M）。
    // ② 也不能无条件 fetchOr——扇出稳态下目标通常已被调度，这次写 RMW 纯属让缓存行在核间弹跳。
    //    先做一次只读 load（已调度时命中共享副本、不产生失效），仅在位0 为 0 时才 fetchOr。
    private func trySchedule(): Unit {
        if ((schedState.load() & SCHED_BIT) != 0) {
            return
        }
        if ((schedState.fetchOr(SCHED_BIT) & SCHED_BIT) == 0) {
            executor.execute(this)
        }
    }

    private func ownedMailboxBacklog(): Bool {
        if (mailboxKind == MAILBOX_PRIORITY) {
            false
        } else {
            mailbox.ownerBacklog()
        }
    }

    /** 优先级邮箱的计数本身是原子量；普通邮箱只看 incoming 提示位 */
    private func incomingAfterRelease(): Bool {
        if (mailboxKind == MAILBOX_PRIORITY) {
            priorityMailbox.nonEmpty()
        } else {
            mailbox.incomingHint()
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

    /**
     * 由调度器工作线程调用。
     *
     * 释放调度权放在 finally 里（R5）：消息循环中任何逃逸的异常或 Error 都不能让 SCHED_BIT 永远置位——
     * 那样该 Actor 再也不会被调度（不处理消息、不计死信，terminateAll 只能等到超时）。
     */
    func run(): Unit {
        try {
            runLoop()
        } catch (e: Exception) {
            cellLog.error("Actor ${fullPath()} 消息循环异常（已隔离，继续调度）: ${e}")
        } catch (e: Error) {
            cellLog.error("Actor ${fullPath()} 消息循环发生 Error（已隔离，继续调度）: ${e}")
        } finally {
            releaseAndReschedule()
        }
    }

    private func runLoop(): Unit {
        let throughput = executor.throughput()
        var count: Int64 = 0
        // 首次运行先执行 onStarted（P1-17：替代 SysStarted 系统消息）。热路径上只多一次共享读。
        let s0 = schedState.load()
        if ((s0 & START_BIT) != 0) {
            if ((schedState.fetchAnd(START_CLEAR_MASK) & START_BIT) != 0 && state == STATE_RUNNING) {
                startInstance(true)
            }
        }
        // PAUSE_BIT 只在本线程处理系统消息时改变：入口读一次，处理完系统消息再读，消息循环里不再逐条读原子量
        var paused = (s0 & PAUSE_BIT) != 0
        while (true) {
            if (hasSystemMessages()) {
                processSystemMessages()
                paused = pausedByParent()
            }
            if (state != STATE_RUNNING || paused || count >= throughput) {
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
                            releaseSlot()
                        }
                        invoke(message, mailbox.lastSender, mailbox.lastSenderRef)
                        count++
                    case None => break
                }
            }
        }
    }

    private func releaseAndReschedule(): Unit {
        // R15：state、unstash 游标、邮箱出队缓冲只属于调度权持有者，必须在释放调度位**之前**读——
        // 释放后下一个持有者可能已在另一线程改写它们（unstashedValue 是多字 Option，跨线程读可能撕裂）。
        // 这些快照之后不会被别人改变出"需要我们再调度"的结果：改变它们的只能是下一个持有者，由它自己负责。
        // 随父暂停时不算"可运行"：否则邮箱有积压就会反复自我调度、空转
        let running = state == STATE_RUNNING && !pausedByParent()
        let ownedWork = running && (hasUnstashed() || ownedMailboxBacklog())
        // 释放调度权（清位0）后再检查一次，防止与并发 tell 之间丢失唤醒：
        // 清位与随后的提示位检查在同一原子字上顺序一致，发送方"入队置提示后置位"不会落空。释放后只读原子量
        schedState.fetchAnd(SCHED_CLEAR_MASK)
        if (ownedWork || hasSystemMessages() || (running && incomingAfterRelease())) {
            // 同 trySchedule：单次 fetchOr 选举，不重试（理由见 trySchedule 注释）
            if ((schedState.fetchOr(SCHED_BIT) & SCHED_BIT) == 0) {
                // 运行结束时一定在某个工作线程上：留在本线程，排到队尾（先轮到其他 Actor）
                executor.executeAgain(this)
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
                handleFailure(ActorKilledException(fullPath()))
                return
            case t: Terminated =>
                // DeathWatch 产生的 Terminated：入队后已 unwatch 的丢弃；watchWith 的换成自定义消息（与 Akka receivedTerminated 相同）。
                // 远程 watch 投递的 Terminated 目标不是本地 Cell，原样交给 receive
                if (let Some(dead) <- (t.actor as SimpleUserActorRef)) {
                    let entry: Option<Option<Message>> = match (watchingValue) {
                        case Some(w) => w.takeQueued(dead)
                        case None => None
                    }
                    match (entry) {
                        case Some(Some(custom)) =>
                            payload = custom
                            currentMessage = Some(custom)
                        case Some(None) => ()
                        case None =>
                            clearCurrent()
                            return
                    }
                }
            case restart: RestartMessage =>
                // 兼容旧路径：guardian.restartChild 通过邮箱请求重启
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
                    mailbox.anomalies().unhandled.fetchAdd(1)
                    if (payload is Terminated) {
                        // 与 Akka 一致：监视者不处理 Terminated 视为失败
                        let t = (payload as Terminated).getOrThrow()
                        throw DeathPactException(t.actor.path.toString())
                    }
                    // 与 Akka 一致：未处理的消息以 UnhandledMessage 交给死信订阅者并限速记日志
                    // （此前只加一个计数，类型化 Actor 收到协议之外的消息也同样静默消失，T11）
                    guardian.publishUnhandled(payload, fullPath())
                case MessageResult.Stop => initiateTerminate()
                case MessageResult.Restart => handleFailure(RestartRequestedException())
                case MessageResult.Escalate => handleFailure(EscalateRequestedException())
            }
        } catch (e: Exception) {
            handleFailure(e)
        } catch (e: Error) {
            // 用户代码中的 Error（如递归过深的 StackOverflowError）按失败交给监督，不杀死工作线程
            handleFailure(ActorErrorException(fullPath(), e))
        }
        processedCount.fetchAdd(1)
        match (timeoutStateValue) {
            case Some(t) =>
                if (t.value > Duration.Zero && !(payload is NotInfluenceReceiveTimeout)) {
                    // 用系统调度器的时钟（E52）：虚拟时钟下空闲时长按虚拟时间计算
                    t.lastActivity = guardian.scheduler().monotonicNow()
                }
            case None => ()
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
                // 子 Actor：挂起（连同整棵子树），等待父 Actor 的决定。
                // 先发暂停、再上报：父的决定（及之后的恢复）一定排在子树收到暂停之后
                suspendSelf()
                parent.sendSystem(SysChildFailed(this, cause))
            case None =>
                // 顶层 Actor：由用户 Guardian 在本 Cell 内同步决定
                guardian.handleFailureInCell(this, cause)
        }
    }

    /** 父 Actor 处理子 Actor 的失败（在父 Actor 的 Cell 线程中执行） */
    private func handleChildFailed(child: SimpleUserActorRef, cause: Exception): Unit {
        // 自己正在重启或停止：子 Actor 都已收到停止请求，它们的失败不再处理
        if (state == STATE_RECREATING || state == STATE_TERMINATING || state == STATE_TERMINATED) {
            return
        }
        match (childMap().get(child.getName())) {
            case Some(c) where refEq(c, child) => ()
            case _ => return // 已经停止或已被替换
        }
        // 取策略与决策都可能执行用户代码（覆写的 supervisorStrategy / decider），异常一律按 Stop 处理，
        // 不能让它逃出系统消息处理（否则调度位无法释放，见 run() 的 finally）
        var strategy: SupervisionStrategy = guardian.defaultStrategy()
        let directive = try {
            strategy = child.childStrategy ?? (actor.supervisorStrategy() ?? guardian.defaultStrategy())
            strategy.decide(child.failureInfo(cause))
        } catch (e: Exception) {
            cellLog.error("监督策略执行异常，停止子 Actor ${child.fullPath()}: ${e}")
            SupervisionDirective.Stop
        }
        let targets = if (strategy.appliesToAllChildren()) {
            childMap().values().toArray()
        } else if (strategy.restForOne()) {
            // RestForOne：失败的子 Actor 及其之后创建的兄弟一起重启
            let order = childTable().creationOrder
            var startIdx: Int64 = 0
            var found = false
            for (i in 0..order.size) {
                if (order[i] == child.getName()) {
                    startIdx = i
                    found = true
                    break
                }
            }
            if (!found) {
                startIdx = 0
            }
            let namesAfter = ArrayList<String>()
            for (i in startIdx..order.size) {
                namesAfter.add(order[i])
            }
            let t = childMap()
            let arr = ArrayList<SimpleUserActorRef>()
            for (n in namesAfter) {
                match (t.get(n)) {
                    case Some(r) => arr.add(r)
                    case None => ()
                }
            }
            if (arr.isEmpty()) {
                [child]
            } else {
                arr.toArray()
            }
        } else {
            [child]
        }
        cellLog.error("Actor ${child.fullPath()} 失败：${cause}，监督指令：${directiveName(directive)}")
        match (directive) {
            case SupervisionDirective.Resume => child.sendSystem(SysResume())
            case SupervisionDirective.Restart =>
                for (t in targets) {
                    t.sendSystem(SysRecreate(cause))
                }
            case SupervisionDirective.RestartAfter(delay) =>
                // E6（2026-11-06）：Backoff 监督的真实延迟重启——延迟后投递 SysRecreate，
                // 不再"算好延迟只打日志就立即重启"
                for (t in targets) {
                    scheduleRecreate(t, cause, delay)
                }
            case SupervisionDirective.Stop =>
                for (t in targets) {
                    t.sendSystem(SysTerminate())
                }
            case SupervisionDirective.Escalate =>
                // 父 Actor 自身以同样的原因失败，交给它的父 Actor 处理：先挂起自己（子树随之暂停，含肇事者），
                // 再解除肇事者自身的失败挂起——之后它随本 Actor 的结局恢复 / 重启 / 停止
                // （Akka 恢复时把 causedByFailure 交给肇事者，效果相同）。顺序不能反：反过来肇事者可能在两条
                // 系统消息之间处理用户消息
                handleFailure(cause)
                child.sendSystem(SysResume())
        }
    }

    // ---------------- 子树暂停（E33，对标 Akka faultSuspend / faultResume） ----------------

    private func pausedByParent(): Bool {
        (schedState.load() & PAUSE_BIT) != 0
    }

    /** 子 Actor 是否应暂停：自身失败待决，或自身随上层暂停（RECREATING / 停止中的子 Actor 正在被停止，不必通知） */
    private func childrenShouldPause(): Bool {
        state == STATE_SUSPENDED || (state == STATE_RUNNING && pausedByParent())
    }

    /** 状态改变后，"子树是否应暂停"若随之改变，通知全部子 Actor（它们各自再向下传递） */
    private func afterPauseChange(before: Bool): Unit {
        let after = childrenShouldPause()
        if (before == after) {
            return
        }
        for (c in childMap().values()) {
            if (after) {
                c.sendSystem(SysParentSuspend())
            } else {
                c.sendSystem(SysParentResume())
            }
        }
    }

    /** 因自身失败挂起（等待监督决定） */
    private func suspendSelf(): Unit {
        let before = childrenShouldPause()
        state = STATE_SUSPENDED
        afterPauseChange(before)
    }

    /** 延迟后在目标 Actor 的系统栈上投递 SysRecreate（Backoff 监督用） */
    private func scheduleRecreate(target: SimpleUserActorRef, cause: Exception, delay: Duration): Unit {
        if (delay.toNanoseconds() <= 0) {
            target.sendSystem(SysRecreate(cause))
            return
        }
        guardian.scheduler().scheduleOnce(delay, { =>
            target.sendSystem(SysRecreate(cause))
        }) |> discardTask
    }

    /**
     * 重启（对标 Akka faultRecreate）：旧实例 preRestart（默认 postStop）→ 取消定时器 → 停止全部子 Actor
     * → **等它们全部终止**（STATE_RECREATING）→ 用工厂创建新实例 → postRestart（默认 preStart）→ onStarted
     */
    private func doRestart(reason: Exception): Unit {
        try {
            actor.preRestart(reason)
        } catch (e: Exception) {
            cellLog.warn("Actor ${fullPath()} preRestart 失败: ${e}")
        }
        timersImpl().cancelAll()
        cancelReceiveTimeoutInternal()
        behaviorsMut().clear()
        // 暂存的消息放回队首，由新实例处理
        if (stashBuffer().size > 0) {
            unstashAllInternal()
        }
        let kids = childMap().values().toArray()
        if (kids.isEmpty()) {
            finishRecreate(reason)
            return
        }
        // 先 unwatch、再停止全部子 Actor。不 unwatch 的话，旧子的 Terminated 会投给新实例
        // （新实例没有登记过它，类型化 Actor 由此触发 DeathPact，T3）
        for (c in kids) {
            unwatchTarget(c)
            c.sendSystem(SysTerminate())
        }
        state = STATE_RECREATING
        childTable().recreateCause = Some(reason)
    }

    /** 旧子 Actor 已全部终止（或本来就没有）：创建新实例 */
    private func finishRecreate(reason: Exception): Unit {
        if (let Some(t) <- childTableValue) {
            t.recreateCause = None<Exception>
        }
        try {
            let fresh = actorFactory()
            actor = fresh
            fresh.postRestart(reason)
            let stats = mailbox.anomalies()
            stats.restarts.fetchAdd(1)
            stats.lastRestartNanos.store(schedulerNanos())
            state = STATE_RUNNING
        } catch (e: Exception) {
            // 无法重建实例时停止该 Actor，避免继续用损坏的状态处理消息
            cellLog.error("Actor ${fullPath()} 重启失败，已停止: ${e}")
            initiateTerminate()
            return
        }
        // 新实例的 preStart 已由 postRestart（默认实现）调用，这里只执行 onStarted
        startInstance(false)
    }

    /**
     * 在 Cell 线程中调用 onStarted；失败包成 ActorInitializationException 交给监督。
     * 默认策略对它返回 Stop（与 Akka 相同）；类型化监督会解开后按原异常匹配规则。
     */
    private func startInstance(initial: Bool): Unit {
        try {
            // preStart 在 Cell 线程执行（E33，对标 Akka）：此前在创建方线程同步调用，与 Actor 自己的线程并发，
            // 抛出的异常也直接从 actorOf 抛给创建方。现在失败与 onStarted 一样包成 ActorInitializationException 交给监督
            if (initial) {
                actor.preStart()
            }
            actor.onStarted(contextImpl())
        } catch (e: ActorInitializationException) {
            handleFailure(e)
        } catch (e: Exception) {
            handleFailure(ActorInitializationException(fullPath(), e))
        } catch (e: Error) {
            handleFailure(ActorInitializationException(fullPath(), ActorErrorException(fullPath(), e)))
        }
    }

    /**
     * 供顶层监督（用户 Guardian）在本 Cell 线程内调用：先挂起，再经系统队列重启（E27）。
     *
     * 与子 Actor 的路径（父 Actor 投递 SysRecreate）一致，重启永远发生在本 Cell 线程的系统消息处理中：
     * - 不再在失败处理的调用栈里同步 doRestart——启动恒失败时那会同栈无限递归直到 StackOverflowError；
     * - delay > 0（Backoff）时挂起期间不处理用户消息，到期由定时器线程只投递 SysRecreate，
     *   不触碰 Cell 状态；若此间 Actor 已停止，SysRecreate 走死亡后规则被忽略，不会"复活"。
     */
    func restartInCell(reason: Exception, delay: Duration): Unit {
        if (state == STATE_TERMINATING || state == STATE_TERMINATED) {
            return
        }
        // 退避期间子树同样暂停（它们将随重启被停止，不应继续处理消息）
        suspendSelf()
        scheduleRecreate(this, reason, delay)
    }

    /** 供不在本 Cell 线程上的调用方请求重启：只投递系统消息，不修改 Cell 状态 */
    func requestRestart(reason: Exception, delay: Duration): Unit {
        scheduleRecreate(this, reason, delay)
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
                    startInstance(true)
                }
            case _: SysResume =>
                // 自身失败得到 Resume：子树是否继续还取决于上层是否仍暂停（afterPauseChange 判断）。
                // 此前直接向子 Actor 转发 SysResume，会把"失败后尚未得到决定"的子 Actor 提前放行
                if (state == STATE_SUSPENDED) {
                    let before = childrenShouldPause()
                    state = STATE_RUNNING
                    afterPauseChange(before)
                }
            case _: SysParentSuspend =>
                if (state != STATE_TERMINATING && state != STATE_TERMINATED) {
                    let before = childrenShouldPause()
                    schedState.fetchOr(PAUSE_BIT)
                    afterPauseChange(before)
                }
            case _: SysParentResume =>
                let before = childrenShouldPause()
                schedState.fetchAnd(PAUSE_CLEAR_MASK)
                afterPauseChange(before)
            case r: SysRecreate =>
                // 已在重启中（等待旧子 Actor 终止）时再来的重启请求合并到这一次
                if (state != STATE_TERMINATING && state != STATE_TERMINATED && state != STATE_RECREATING) {
                    doRestart(r.cause)
                }
            case w: SysWatch => addWatcher(w.watcher)
            case u: SysUnwatch => removeWatcher(u.watcher)
            case d: SysDeathNotification => onDeathNotification(d.actor)
            case f: SysChildFailed => handleChildFailed(f.child, f.cause)
            case t: SysChildTerminated => onChildTerminated(t.child)
            case _: SysChildrenTerminated =>
                // 停止合批：整批子 Actor 已终止，直接收尾（childMap 一并丢弃）
                schedState.fetchAnd(BATCH_CLEAR_MASK)
                childClear()
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
        // 同一批系统消息里 SysTerminate 排在 SysWatch 之前时，处理到这里本 Actor 已经终止、
        // 监视者列表也已通知并清空：此时登记会让监视者永远收不到 Terminated（R20），按"死亡后"规则立即通知
        if (state == STATE_TERMINATED) {
            watcher.sendSystem(SysDeathNotification(this))
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
        // 本 Actor 正在停止（停止合批）：子 Actor 的终止只由合批计数统计——子要么自己原子递减，
        // 要么发旧式 SysChildTerminated 由 onChildTerminated 补递减，二者恰好一次。
        // 死亡通知若再走 onChildTerminated，被监视的子会被扣两次，父在其余子仍存活时就 postStop（R3）。
        // 停止中的 Actor 也不再投递 Terminated，watching 由 finishTerminate 统一清理。
        if (state == STATE_TERMINATING) {
            return
        }
        // 自己的子 Actor：先释放名称再投递 Terminated（与 Akka 相同），
        // 这样 Terminated 处理中可以立即用同一名称重建子 Actor（例如分片实体钝化后重建）
        onChildTerminated(dead)
        // 已取消监视（或从未监视）的不投递
        match (watching().get(dead.fullPath())) {
            case Some(w) where refEq(w, dead) => watchingMut().remove(dead.fullPath())
            case _ => return
        }
        if (state == STATE_TERMINATING || state == STATE_TERMINATED) {
            return
        }
        // 入用户邮箱队尾（R6）：dead 死前发给本 Actor 的消息已在邮箱里，因此先于 Terminated 被处理。
        // 挂起期间用户邮箱不处理，恢复后自然按序处理，不需要另外暂存
        let ws = watchState()
        ws.queued.add((dead, ws.messages.remove(dead.fullPath())))
        enqueueGenerated(Terminated(dead))
    }

    private func onChildTerminated(child: SimpleUserActorRef): Unit {
        let existed = match (childMap().get(child.getName())) {
            case Some(c) where refEq(c, child) =>
                childRemove(child.getName())
                true
            case _ => false
        }
        if (state == STATE_TERMINATING && childMap().size == 0) {
            finishTerminate()
            return
        }
        // 重启等待中：最后一个旧子 Actor 终止后创建新实例
        if (existed && state == STATE_RECREATING && childMap().size == 0) {
            state = STATE_SUSPENDED
            match (childTableValue) {
                case Some(t) =>
                    match (t.recreateCause) {
                        case Some(cause) => finishRecreate(cause)
                        case None => finishRecreate(IllegalStateException("重启原因丢失"))
                    }
                case None => ()
            }
            return
        }
        // 合批兜底：父已进入合批、但该子在此之前就自行终止（发的是旧式 SysChildTerminated，
        // 未递减计数）。它在初始化子数时已被计入，这里补一次递减，否则计数永远到不了 0。
        // 每个子只会走两条路径之一（旧式消息 或 原子递减）；TERMINATING 期间的死亡通知
        // 不进入本函数（见 onDeathNotification），因此不会重复扣减。
        if (existed && state == STATE_TERMINATING && batchStopping()) {
            if (stoppingChildren.fetchAdd(-1) <= 1) {
                schedState.fetchAnd(BATCH_CLEAR_MASK)
                childClear()
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
            cellLog.warn("Actor ${fullPath()} preStop 失败: ${e}")
        }
        if (childMap().size == 0) {
            finishTerminate()
        } else {
            // 启动"子 Actor 全部终止"的计数（P5-2）：每个子终止时递减，
            // 减到 0 的最后一个子发一条汇总消息，而不是每子一条
            if (stoppingChildrenValue.isNone()) {
                stoppingChildrenValue = Some(AtomicInt64(0))
            }
            // 发布顺序：先写计数，再置 BATCH_BIT（seq_cst RMW）；子看到该位后读计数，能看到上面的写
            stoppingChildren.store(childMap().size)
            schedState.fetchOr(BATCH_BIT)
            let kids = childMap()
            // 小子树：父线程内联领养（省每子一次调度入队+唤醒，短突发最快）；
            // 大子树：交给各子自己的亲和 worker 并行处理（sendSystem 进的是每个子自己的系统队列）。
            // 纯内联在百万级是单线程串行，1M→2M 停止耗时 1039→2856ms 超线性（实测）；
            // 纯并行又要为每子付一次入队+唤醒。按规模切换，二者兼得。
            if (kids.size <= INLINE_STOP_LIMIT) {
                for (c in kids.values()) {
                    c.terminateIfIdle()
                }
            } else {
                for (c in kids.values()) {
                    c.sendSystem(SysTerminate())
                }
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
                        cellLog.error("Actor ${fullPath()} 停止失败: ${e}")
                    }
                    // 与 run() 末尾一致：释放调度权（保留 TERM_BIT，不动计数位）。
                    // 但停止过程中可能已被投递系统消息（例如 stop 与并发 tell/父级终止交错），
                    // 释放前必须先交给自己的 run() 收取，否则该消息无人处理，
                    // Actor 停在 TERMINATING（soak 实测：创建 195550 / 停止 195500，差 50）。
                    if (hasSystemMessages()) {
                        executor.execute(this)
                        return
                    }
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
            cellLog.warn("Actor ${fullPath()} postStop 失败: ${e}")
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

    private func timeoutState(): ReceiveTimeoutState {
        match (timeoutStateValue) {
            case Some(s) => s
            case None =>
                let s = ReceiveTimeoutState()
                timeoutStateValue = Some(s)
                s
        }
    }

    func setReceiveTimeoutInternal(timeout: Duration): Unit {
        cancelReceiveTimeoutInternal()
        if (timeout <= Duration.Zero) {
            return
        }
        let s = timeoutState()
        s.value = timeout
        s.lastActivity = guardian.scheduler().monotonicNow()
        scheduleReceiveTimeoutCheck(s.lastActivity + timeout)
    }

    func cancelReceiveTimeoutInternal(): Unit {
        match (timeoutStateValue) {
            case Some(s) =>
                // 不清空 timeoutStateValue：generation 必须单调，否则在途的旧超时消息可能与新状态同代次
                s.generation++
                s.value = Duration.Zero
                if (let Some(t) <- s.timer) {
                    t.cancel()
                }
                s.timer = None<ScheduledTask>
            case None => ()
        }
    }

    /** 在调度器时钟的 deadline 检查一次是否空闲超时（按绝对时刻登记，见 ActorScheduler.scheduleOnceAt） */
    private func scheduleReceiveTimeoutCheck(deadline: MonoTime): Unit {
        let s = timeoutState()
        let gen = s.generation
        s.timer = Some(guardian.scheduler().scheduleOnceAt(deadline, { => this.sendSystem(SysReceiveTimeout(gen)) }))
    }

    private func onReceiveTimeoutCheck(generation: Int64): Unit {
        let s = match (timeoutStateValue) {
            case Some(v) => v
            case None => return
        }
        if (generation != s.generation || s.value <= Duration.Zero || state != STATE_RUNNING) {
            return
        }
        // 随上层暂停：不执行用户代码，稍后再查（不能直接返回，否则恢复后接收超时再也不会触发）
        let clock = guardian.scheduler()
        if (pausedByParent()) {
            scheduleReceiveTimeoutCheck(clock.monotonicNow() + s.value)
            return
        }
        let elapsed = clock.monotonicNow() - s.lastActivity
        if (elapsed >= s.value) {
            invoke(ReceiveTimeout(), None<String>, None<ActorRef>)
            // 空闲期间每隔 timeout 重复投递
            s.lastActivity = clock.monotonicNow()
            if (s.value > Duration.Zero && generation == s.generation) {
                scheduleReceiveTimeoutCheck(s.lastActivity + s.value)
            }
        } else {
            // 期间有过活动：到"最后一次活动 + timeout"再查（绝对时刻，与检查本身何时被处理无关）
            scheduleReceiveTimeoutCheck(s.lastActivity + s.value)
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
            throw IllegalStateException("Actor ${fullPath()} 正在停止，不能创建子 Actor")
        }
        validateChildName(name)
        if (childMap().contains(name)) {
            throw InvalidActorNameException("子 Actor 名称已存在: ${fullPath()}/${name}")
        }
        let child = guardian.createCell(props, name, Some(this))
        childPut(name, child)
        // 暂停中创建的子 Actor 一出生就随之暂停（Akka makeChild 按父的挂起计数挂起新子）：
        // 例如上层失败待决时本 Actor 重启，新实例在 onStarted 里建的子 Actor。暂停在引用交给调用方之前发出
        if (childrenShouldPause()) {
            child.sendSystem(SysParentSuspend())
        }
        child
    }

    func spawnAnonymousChild(creator: () -> Actor): ActorRef {
        anonymousSeq++
        spawnChild(PropsFactory.create<Actor>(creator), "$${anonymousSeq}")
    }

    /** 子 Actor 快照；可从任意线程调用（R8：此前跨线程无锁遍历父的 HashMap，与父的增删并发） */
    func childrenInternal(): Array<ActorRef> {
        match (publishedChildTable()) {
            case Some(t) =>
                synchronized(t.lock) {
                    let result = ArrayList<ActorRef>(t.map.size)
                    for (c in t.map.values()) {
                        result.add(c)
                    }
                    result.toArray()
                }
            case None => Array<ActorRef>()
        }
    }

    /** 按名字取子 Actor；可从任意线程调用 */
    func childInternal(name: String): Option<ActorRef> {
        match (publishedChildTable()) {
            case Some(t) =>
                synchronized(t.lock) {
                    match (t.map.get(name)) {
                        case Some(c) => Some<ActorRef>(c)
                        case None => None<ActorRef>
                    }
                }
            case None => None<ActorRef>
        }
    }

    func stopInternal(ref: ActorRef): Unit {
        let isSelf = match (ref) {
            case c: SimpleUserActorRef => refEq(c, this)
            case _ => ref.path.getPath() == fullPath()
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
                    case _ => throw IllegalArgumentException("context.stop 只能停止自身或子 Actor: ${c.fullPath()}")
                }
            case _ => throw IllegalArgumentException("context.stop 只能停止自身或子 Actor: ${ref.path.toString()}")
        }
    }

    func watchInternal(ref: ActorRef): Unit {
        watchTarget(asLocalCell(ref, "watch"))
    }

    private func watchTarget(target: SimpleUserActorRef): Unit {
        if (refEq(target, this)) {
            return
        }
        match (watching().get(target.fullPath())) {
            case Some(w) where refEq(w, target) => ()
            // 同路径的旧目标（已死或正在死，例如重启后同名重建的子 Actor）：换成新目标（R18）。
            // 此前按路径判重会跳过新目标，旧目标的 Terminated 反而投给了新实例
            case _ =>
                watchingMut()[target.fullPath()] = target
                target.sendSystem(SysWatch(this))
        }
    }

    /** watchWith：目标死亡时收到 message 而不是 Terminated（与 Akka 相同；映射保存在 Cell，重启后仍有效） */
    func watchWithInternal(ref: ActorRef, message: Message): Unit {
        let target = asLocalCell(ref, "watchWith")
        if (refEq(target, this)) {
            return
        }
        watchState().messages[target.fullPath()] = message
        watchTarget(target)
    }

    func unwatchInternal(ref: ActorRef): Unit {
        unwatchTarget(asLocalCell(ref, "unwatch"))
    }

    private func unwatchTarget(target: SimpleUserActorRef): Unit {
        let ws = match (watchingValue) {
            case Some(w) => w
            case None => return
        }
        match (ws.targets.get(target.fullPath())) {
            case Some(w) where refEq(w, target) =>
                ws.targets.remove(target.fullPath())
                ws.messages.remove(target.fullPath())
                target.sendSystem(SysUnwatch(this))
            case _ => ()
        }
        // 已入队尚未处理的 Terminated 一并作废（取出时被丢弃）
        let _ = ws.takeQueued(target)
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
            case Some(m) =>
                // 暂存的 Terminated 出队时已注销了待处理记录：重新登记，否则 unstash 后会被当作"已 unwatch"丢弃（与 Akka 相同）
                if (let Some(t) <- (m as Terminated)) {
                    if (let Some(dead) <- (t.actor as SimpleUserActorRef)) {
                        watchState().queued.add((dead, None<Message>))
                    }
                }
                stashBufferMut().add(Envelope(m, currentSender, currentSenderRef))
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
        pathHashValue
    }

    public func toString(): String {
        "Actor[${fullPath()}]"
    }
}

func directiveName(d: SupervisionDirective): String {
    match (d) {
        case SupervisionDirective.Resume => "Resume"
        case SupervisionDirective.Restart => "Restart"
        case SupervisionDirective.RestartAfter(delay) => "RestartAfter(${delay.toMilliseconds()}ms)"
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

    public func watchWith(ref: ActorRef, message: Message): Unit {
        cell.watchWithInternal(ref, message)
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

    public func scheduler(): ActorScheduler {
        cell.guardian.scheduler()
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
                let l = Logging.logger(cell.fullPath())
                loggerValue = Some(l)
                l
        }
    }

    public func lookup(path: String): Option<ActorRef> {
        cell.guardian.lookup(path)
    }
}

// ======================= 定时器 =======================

func discardTask(_: ScheduledTask): Unit {}

class TimerEntry {
    let generation: Int64
    let timer: ScheduledTask
    let single: Bool
    init(generation: Int64, timer: ScheduledTask, single: Bool) {
        this.generation = generation
        this.timer = timer
        this.single = single
    }
}

/**
 * Actor 级定时器：经系统调度器（ActorScheduler，默认 std.sync.Timer）安排（E52）。
 * 定时器线程只负责把带代次的 TimerEnvelope 放进邮箱；投递时在 Cell 内校验代次，
 * 因此 cancel 或同 key 重新 start 之后，已在途的旧消息不会被处理。
 */
/**
 * receive-timeout 状态（P1-16）：只有调用过 setReceiveTimeout 的 Actor 才分配。
 * 之前 4 个字段（value/lastActivity/generation/timer，约 32 B）内联在 Cell 里，
 * 而绝大多数 Actor 从不使用 receive timeout；改为 Option<ReceiveTimeoutState>（内联 8 B）。
 */
class ReceiveTimeoutState {
    var value: Duration = Duration.Zero
    var lastActivity: MonoTime = MonoTime.now()
    var generation: Int64 = 0
    var timer: Option<ScheduledTask> = None<ScheduledTask>
}

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
        let t = target.guardian.scheduler().scheduleOnce(delay, { => target.deliverTimer(TimerEnvelope(key, gen, message)) })
        entries.add(key, TimerEntry(gen, t, true))
    }

    public func startTimerWithFixedDelay(key: String, message: Message, delay: Duration): Unit {
        cancel(key)
        let gen = nextGeneration()
        let target = cell
        let t = target.guardian.scheduler().scheduleWithFixedDelay(delay, delay,
            { => target.deliverTimer(TimerEnvelope(key, gen, message)) })
        entries.add(key, TimerEntry(gen, t, false))
    }

    public func startTimerAtFixedRate(key: String, message: Message, interval: Duration): Unit {
        cancel(key)
        let gen = nextGeneration()
        let target = cell
        let t = target.guardian.scheduler().scheduleAtFixedRate(interval, interval,
            { => target.deliverTimer(TimerEnvelope(key, gen, message)) })
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

/** 工作线程尚未运行过任何 Cell 时 currentRun 的占位 */
let NO_RUN: RunStub = RunStub()

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
    // 正在（或最近一次）运行的 Cell：饥饿检测线程据此报告是哪个 Actor 占住了本线程（E39）。
    // 每次运行一次原子写，不按消息计；是否"正在运行"由 executed 是否推进、是否空闲判断
    let currentRun: AtomicReference<RunLink> = AtomicReference<RunLink>(NO_RUN)
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
            // 槽位被顶替出来：待取计数不变（换了一个节点而已），转交亲和线程
            dispatcher.submitToAffinity(prev)
        } else {
            dispatcher.runNextPending.fetchAdd(1)
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
            dispatcher.runNextPending.fetchAdd(-1)
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
        // 没有待取的 runNext 时直接返回：避免空闲线程全量扫描所有 worker 的
        // runNextSlot/executed（跨核读风暴，且会偷走忙线程正在热的 Cell）
        if (dispatcher.runNextPending.load() <= 0) {
            return None<SimpleUserActorRef>
        }
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
                    dispatcher.runNextPending.fetchAdd(-1)
                    return Some(cell)
                }
            }
        }
        None<SimpleUserActorRef>
    }

    private func runCell(cell: SimpleUserActorRef): Unit {
        donateIfHungry()
        currentRun.store(cell)
        try {
            cell.run()
        } catch (e: Exception) {
            cellLog.error("Actor Cell 执行异常: ${e}")
        } catch (e: Error) {
            // 工作线程绑定着一条运行队列，线程退出等于让亲和到它的所有 Actor 失联：Error 也必须兜住
            cellLog.error("Actor Cell 执行发生 Error: ${e}")
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
    // 全局"有 runNext 待取"计数：push 时 +1、被取走时 -1。
    // 空闲线程仅在其 > 0 时才去扫描各线程的 runNextSlot/executed——
    // 否则 14 个空闲线程每 250µs 全量扫一遍所有 worker，是跨核读风暴，
    // 还会偷走忙线程正在热的 Cell（实测扇出 T14 3.0M vs T4 11.7M 的主因）
    let runNextPending: AtomicInt64 = AtomicInt64(0)
    // 饥饿检测（E39）：某个工作线程卡在同一次 Cell 运行里超过阈值的次数，与最近一次的报告
    let starvationCount: AtomicInt64 = AtomicInt64(0)
    private let starvationLock: Mutex = Mutex()
    private var lastStarvationValue: Option<String> = None<String>
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
            let warnMs = envInt("CACTOR_STARVATION_WARN_MS", 1000, 0)
            if (warnMs > 0) {
                let me = this
                spawn {
                    me.watchStarvation(Duration.millisecond * warnMs)
                }
            }
        } else {
            while (!ready.load()) {
                sleep(Duration.Zero)
            }
        }
    }

    /**
     * 饥饿检测线程（E39，同 Akka 的 starvation detector）：周期性查看每个工作线程的执行计数，
     * 不空闲、计数却超过 threshold 没有推进 = 卡在同一次 Cell 运行里（处理方法在阻塞或长时间计算）。
     * 亲和到该线程的其他 Actor 这段时间都得不到运行，所以每次卡住告警一次并点名占住线程的 Actor。
     * 热路径上只有 runCell 的一次原子写；检测线程每 threshold/4（10–250ms）醒一次
     */
    private func watchStarvation(threshold: Duration): Unit {
        let ws = workers
        var interval = threshold / 4
        if (interval < Duration.millisecond * 10) {
            interval = Duration.millisecond * 10
        }
        if (interval > Duration.millisecond * 250) {
            interval = Duration.millisecond * 250
        }
        let seen = Array<Int64>(ws.size, repeat: -1)
        let since = Array<MonoTime>(ws.size, repeat: MonoTime.now())
        let warned = Array<Int64>(ws.size, repeat: -1)
        while (true) {
            sleep(interval)
            let now = MonoTime.now()
            for (i in 0..ws.size) {
                let w = ws[i]
                let executed = w.executed.load()
                // 空闲（举手等捐赠、或已休眠）不算卡住；计数推进说明运行正常
                if (executed != seen[i] || w.hungry.load() || w.sleeping.load()) {
                    seen[i] = executed
                    since[i] = now
                    continue
                }
                let stuck = now - since[i]
                if (stuck >= threshold && warned[i] != executed) {
                    warned[i] = executed
                    reportStarvation(i, w, stuck)
                }
            }
        }
    }

    private func reportStarvation(index: Int64, w: DispatcherWorker, stuck: Duration): Unit {
        let path = match (w.currentRun.load() as SimpleUserActorRef) {
            case Some(c) => c.fullPath()
            case None => "(未知)"
        }
        synchronized(starvationLock) {
            lastStarvationValue = Some(path)
        }
        starvationCount.fetchAdd(1)
        cellLog.warn("工作线程 #${index} 卡在 Actor ${path} 的一次运行里已 ${stuck.toMilliseconds()} ms，" +
            "亲和到该线程的其他 Actor 都在等待。阻塞调用（IO、sleep、同步等待 ask / Future）请放到 blocking 调度器" +
            "（Props.withDispatcher(\"blocking\") / TypedProps 的 dispatcher），或改用 pipeToSelf 异步接收结果；" +
            "阈值由 CACTOR_STARVATION_WARN_MS 设置（0 关闭）")
    }

    /** 饥饿告警次数（默认调度器；E39） */
    public static func starvationWarningCount(): Int64 {
        defaultInstance.starvationCount.load()
    }

    /** 最近一次饥饿告警点名的 Actor 路径 */
    public static func lastStarvationPath(): Option<String> {
        synchronized(defaultInstance.starvationLock) {
            defaultInstance.lastStarvationValue
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
 * 初始线程数默认 16（CACTOR_BLOCKING_THREADS）；没有空闲线程可接手新任务时按需扩容，
 * 上限 CACTOR_BLOCKING_THREADS_MAX（默认 64），避免后来的 Actor 无限期排队。
 *
 * 扩容依据是"空闲线程数"而不是"线程总数"（E39，R16）：此前条件是排队数 > 线程总数，
 * 16 个线程全被阻塞的 Actor 占住时第 17 个任务的排队数只有 1，永远不扩容，只能无限期等待。
 */
class BlockingCellExecutor <: CellExecutor {
    private let lock = Mutex()
    private let cond: Condition
    private let queue = ArrayList<SimpleUserActorRef>()
    private var head: Int64 = 0
    private let started = AtomicBool(false)
    // 已启动的工作线程数（按需扩容时用 CAS 递增，避免多个线程同时扩容）
    private let workers = AtomicInt64(0)
    private let maxWorkers: Int64
    // 正在 take() 中等待任务的空闲线程数（只在 lock 内读写）
    private var idle: Int64 = 0

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
        let starved = synchronized(lock) {
            queue.add(cell)
            cond.notify()
            // 排队任务多于空闲线程：其余线程都在执行（可能正被阻塞），新任务没人接
            queue.size - head > idle
        }
        if (starved) {
            let w = workers.load()
            if (w < maxWorkers && workers.compareAndSwap(w, w + 1)) {
                spawn {
                    markActorThread()
                    workerLoop()
                }
            }
        }
    }

    public func executeAgain(cell: SimpleUserActorRef): Unit {
        execute(cell)
    }

    private func ensureStarted(): Unit {
        if (started.compareAndSwap(false, true)) {
            let n = envInt("CACTOR_BLOCKING_THREADS", 16, 1)
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
                idle++
                cond.wait()
                idle--
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
            } catch (e: Error) {
                cellLog.error("阻塞调度器执行发生 Error: ${e}")
            }
        }
    }

    /** 当前工作线程数（含按需扩容的线程） */
    func workerCount(): Int64 {
        workers.load()
    }
}
