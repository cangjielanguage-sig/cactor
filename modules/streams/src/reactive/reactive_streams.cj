/*
 * CActor Streams - Reactive Streams 语义的最小实现（对照 docs/plan/20261028.md §8.9 R7 / dsl20261030.md §7.4 L5）
 *
 * 与 dsl（内存数组批处理）不同，本文件是真正的流式实现：
 *
 *   Source ──元素──▶ Processor（Actor，异步边界）──元素──▶ Sink
 *     ▲                                                    │
 *     └──────────────────── 需求（request(n)）─────────────┘
 *
 * 语义（对齐 Reactive Streams 1.0 的子集）：
 *   - 订阅者用 request(n) 声明需求，需求逐级传到源头；源最多发出已请求的条数，慢消费者不会被打爆；
 *   - 完成 / 错误是终止信号，不消耗需求；终止之后的信号被忽略；
 *   - Processor 跑在自己的 Actor 上，信号串行处理。元素只在下游有需求时才会到达，
 *     所以 Processor 直接交给下游，不在 receive 里等待（在 Actor 里阻塞会占住工作线程）；
 *   - 源在调用方以外的线程上等待需求；只要了部分元素时，运行结束会取消源（close）。
 *
 * 物化值：runVia / runStream 返回"本次运行"的结果（与 dsl 的 StreamGraph.run 一致）。
 * 实验性：只支持 String 元素和单个 Processor，操作符见 ROADMAP R7。
 */
package cactor_streams.reactive

import std.sync.*
import std.collection.*
import std.time.*
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult}
import cactor.api.CActor
import cactor_core.dsl.*
import cactor_macros.*

// ======================= 需求信号 =======================

/**
 * 需求信箱：下游声明需求（request(n)），上游按需求投递。
 * 投递的消息放进本地队列（poll / take 取走），或者用 forwardTo 直接转发（例如转给 Actor）。
 */
public class DemandBox {
    private let lock = Mutex()
    private let cond: Condition
    private let items = ArrayDeque<Message>()
    private var demand: Int64 = 0
    private var closed = false
    private var forward: Option<(Message) -> Unit> = None<(Message) -> Unit>

    public init() {
        this.cond = synchronized(lock) { lock.condition() }
    }

    /** 声明需求：可以累计（request(1) + request(1) == request(2)）；非正数忽略 */
    public func request(n: Int64): Unit {
        if (n <= 0) {
            return
        }
        synchronized(lock) {
            demand += n
            cond.notifyAll()
        }
    }

    /** 投递不再进本地队列，而是交给 f（在投递方线程上调用，不持有信箱的锁） */
    public func forwardTo(f: (Message) -> Unit): Unit {
        synchronized(lock) {
            forward = Some(f)
        }
    }

    /** 取消：等待需求的上游立即返回 false，之后的投递被丢弃 */
    public func close(): Unit {
        synchronized(lock) {
            closed = true
            cond.notifyAll()
        }
    }

    /** 等待并占用 1 个需求；超时或已取消时返回 false */
    public func acquire(timeout: Duration): Bool {
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (demand <= 0 && !closed) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    return false
                }
                cond.wait(timeout: remaining)
            }
            if (closed) {
                return false
            }
            demand -= 1
            true
        }
    }

    /** 投递已经占用过需求的元素，或不需要需求的终止信号 */
    public func deliver(message: Message): Unit {
        let target = synchronized(lock) {
            if (closed) {
                return
            }
            match (forward) {
                case Some(f) => Some(f)
                case None =>
                    items.addLast(message)
                    cond.notifyAll()
                    None<(Message) -> Unit>
            }
        }
        if (let Some(f) <- target) {
            f(message)
        }
    }

    /** 元素投递：没有需求时等待（尊重需求信号，不无限缓冲）；超时或已取消返回 false */
    public func send(message: Message, timeout: Duration): Bool {
        if (!acquire(timeout)) {
            return false
        }
        deliver(message)
        true
    }

    /** 取一条（非阻塞） */
    public func poll(): Option<Message> {
        synchronized(lock) {
            items.removeFirst()
        }
    }

    /** 取一条，没有时等待 */
    public func take(timeout: Duration): Option<Message> {
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (items.isEmpty()) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    return None<Message>
                }
                cond.wait(timeout: remaining)
            }
            items.removeFirst()
        }
    }

    /** 本地队列中尚未取走的消息数 */
    public func pending(): Int64 {
        synchronized(lock) { Int64(items.size) }
    }

    /** 尚未使用的需求 */
    public func outstandingDemand(): Int64 {
        synchronized(lock) { demand }
    }
}

// ======================= 流消息 =======================

/** 元素信号 */
public class Next <: Message {
    public let value: String

    public init(value: String) {
        this.value = value
    }

    public func messageType(): String {
        "Next"
    }
}

/** 完成信号 */
public class Completed <: Message {
    public func messageType(): String {
        "Completed"
    }
}

/** 错误信号 */
public class Failed <: Message {
    public let cause: String

    public init(cause: String) {
        this.cause = cause
    }

    public func messageType(): String {
        "Failed"
    }
}

// ======================= Source / Processor / Sink =======================

/**
 * 流源（Publisher）：按下游需求逐条发出元素，最后发出完成信号。
 * 下游没有需求时 publishTo 等待，内存不会无限增长（背压）。
 */
public class ReactiveSource {
    private let values: Array<String>
    // 已发出的条数：运行方在别的线程上读它，判断是否只发了一部分
    private let pos = AtomicInt64(0)

    public init(values: Array<String>) {
        this.values = values
    }

    /**
     * 按需求逐条发给下游，全部发完后发出完成信号。
     * 返回本次发出的条数（需求不足超时、或下游取消时小于剩余元素数）。
     */
    public func publishTo(downstream: DemandBox, timeout: Duration): Int64 {
        var sent: Int64 = 0
        while (pos.load() < values.size) {
            if (!downstream.acquire(timeout)) {
                return sent
            }
            // 先计数再投递：下游收到这条时，remaining() 已经不再包含它
            let v = values[pos.fetchAdd(1)]
            sent += 1
            downstream.deliver(Next(v))
        }
        downstream.deliver(Completed())
        sent
    }

    public func remaining(): Int64 {
        values.size - pos.load()
    }
}

/**
 * 处理器（Processor）：跑在自己的 Actor 上（异步边界），对每个元素应用转换后交给 sink。
 * 上游只在 sink 有需求时才发元素，所以这里直接交出，不等待；转换抛出的异常变成错误信号。
 */
@actor[cases = [Next, Completed, Failed]]
public class ProcessorActor <: Actor {
    private let transform: (String) -> String
    private let sink: ReactiveSink

    public init(transform: (String) -> String, sink: ReactiveSink) {
        this.transform = transform
        this.sink = sink
    }

    func onNext(m: Next): Unit {
        let out: ?String = try {
            Some(transform(m.value))
        } catch (e: Exception) {
            // 终止之后 sink 忽略后续元素（没有向上游的 cancel，剩余元素被丢弃）
            sink.accept(Failed(e.message))
            None<String>
        }
        if (let Some(v) <- out) {
            sink.accept(Next(v))
        }
    }

    func onCompleted(m: Completed): Unit {
        sink.accept(m)
    }

    func onFailed(m: Failed): Unit {
        sink.accept(m)
    }
}

/**
 * 流汇（Subscriber）：声明需求、收集元素与最终结果。
 * 连接上游后（attachUpstream），request(n) 会把需求传给上游。
 */
public class ReactiveSink {
    private let lock = Mutex()
    private let cond: Condition
    private let results = ArrayList<String>()
    private var demand: Int64 = 0
    private var upstream: Option<DemandBox> = None<DemandBox>
    private var error: ?String = None
    private var done: Bool = false

    public init() {
        this.cond = synchronized(lock) { lock.condition() }
    }

    /** 声明需求（订阅动作）；非正数忽略 */
    public func request(n: Int64): Unit {
        if (n <= 0) {
            return
        }
        let up = synchronized(lock) {
            demand += n
            upstream
        }
        if (let Some(box) <- up) {
            box.request(n)
        }
    }

    /**
     * 连接上游：此前声明、尚未满足的需求一并传给它。
     * 与 request 在同一把锁下读写 demand / upstream，并发时需求不会重复或遗漏
     */
    public func attachUpstream(box: DemandBox): Unit {
        let outstanding = synchronized(lock) {
            upstream = Some(box)
            demand
        }
        box.request(outstanding)
    }

    /** 尚未满足的需求 */
    public func outstandingDemand(): Int64 {
        synchronized(lock) { demand }
    }

    /** 上游写入信号（元素 / 完成 / 错误）；终止之后的信号被忽略 */
    public func accept(signal: Message): Unit {
        synchronized(lock) {
            if (done || error.isSome()) {
                return
            }
            match (signal) {
                case n: Next =>
                    results.add(n.value)
                    if (demand > 0) {
                        demand -= 1
                    }
                case f: Failed => error = Some(f.cause)
                case _: Completed => done = true
                case _ => return
            }
            cond.notifyAll()
        }
    }

    /** 已收集的元素（物化值的一部分） */
    public func collected(): Array<String> {
        synchronized(lock) {
            results.toArray()
        }
    }

    public func errorMessage(): ?String {
        synchronized(lock) { error }
    }

    public func isCompleted(): Bool {
        synchronized(lock) { done }
    }

    /** 收到完成或错误信号 */
    public func isTerminated(): Bool {
        synchronized(lock) { done || error.isSome() }
    }

    /** 等待收集满 n 条 */
    public func awaitCount(n: Int64, timeout: Duration): Bool {
        awaitUntil(timeout, { => Int64(results.size) >= n })
    }

    /** 等待 condition 成立（在本对象的锁下求值；每收到一个信号重新检查） */
    func awaitUntil(timeout: Duration, condition: () -> Bool): Bool {
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (!condition()) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    return false
                }
                cond.wait(timeout: remaining)
            }
            true
        }
    }
}

// ======================= 运行入口（物化） =======================

/** 一次运行的物化值 */
public class ReactiveRun {
    public let collected: Array<String>
    public let error: ?String
    public let completed: Bool

    init(collected: Array<String>, error: ?String, completed: Bool) {
        this.collected = collected
        this.error = error
        this.completed = completed
    }

    public func isSuccess(): Bool {
        completed && error.isNone()
    }
}

/**
 * 运行一条流：Source -> Processor（Actor）-> Sink。processor 必须是以同一个 sink 创建的 ProcessorActor。
 *
 *   let sink = ReactiveSink()
 *   let processor = system.actorOf({ => ProcessorActor({ v => v.toAsciiUpper() }, sink) }, "upper")
 *   sink.request(10)                       // 需求信号：只要 10 条
 *   let run = runVia(ReactiveSource(values), processor, sink, 3.seconds)
 *
 * 在以下任一情况返回：收到完成 / 错误信号；需求已全部满足而源还有剩余（只要了部分元素，随后取消源）；超时。
 */
public func runVia(
    source: ReactiveSource,
    processor: ActorRef,
    sink: ReactiveSink,
    timeout: Duration
): ReactiveRun {
    let inbound = DemandBox()
    inbound.forwardTo({ m => processor.tell(m) })
    sink.attachUpstream(inbound)
    // 源在独立线程上等待需求：调用方只等结果，Actor 也不被占住
    spawn {
        source.publishTo(inbound, timeout)
    }
    let _ = sink.awaitUntil(timeout, { => sink.isTerminated() || (sink.outstandingDemand() == 0 && source.remaining() > 0) })
    // 不再需要更多元素：取消源（正在等需求的 publishTo 立即返回）
    inbound.close()
    ReactiveRun(sink.collected(), sink.errorMessage(), sink.isCompleted())
}

private let streamSeq = AtomicInt64(0)

/**
 * 一步组装 + 运行（推荐入口）：需求等于元素数，在临时的 Actor 系统上运行
 *
 *   let run = runStream(values, 3.seconds) { v => "n${v}" }
 */
public func runStream(values: Array<String>, timeout: Duration, transform: (String) -> String): ReactiveRun {
    let system = CActor.system("reactive-stream-${streamSeq.fetchAdd(1)}")
    try {
        let sink = ReactiveSink()
        let processor = system.actorOf({ => ProcessorActor(transform, sink) }, "processor")
        sink.request(Int64(values.size))
        runVia(ReactiveSource(values), processor, sink, timeout)
    } finally {
        system.terminate()
    }
}
