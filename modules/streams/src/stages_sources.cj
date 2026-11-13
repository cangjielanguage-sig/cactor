/*
 * CActor Streams - 源阶段
 *
 * 所有源都只在下游 pull 时才产生元素：无限源（repeat / unfold / iterate）不会预先生成任何东西，
 * 外部输入源（queue / actorRef）用有界缓冲区 + 溢出策略承接生产者。
 */
package cactor_streams

import std.collection.*
import std.sync.*
import std.time.*
import cactor_core.typed.*

/** 按需拉取迭代器（single / from / range / repeat / unfold / iterate 共用） */
class IteratorSourceLogic<T> <: SourceLogic<T> {
    let mk: () -> Iterator<T>
    var it: Option<Iterator<T>> = None<Iterator<T>>

    init(island: Island, mk: () -> Iterator<T>) {
        super(island)
        this.mk = mk
    }

    public override func preStart(): Unit {
        // 每次物化重新取迭代器：同一蓝图多次运行互不影响
        it = Some(mk())
    }

    public func onPull(): Unit {
        match (it.getOrThrow().next()) {
            case Some(e) => outlet.push(e)
            case None => completeStage()
        }
    }
}

/** 空源：不等需求，启动即完成（与 Akka 一致，asPublisher 等不发需求的下游也能看到完成） */
class EmptySourceLogic<T> <: SourceLogic<T> {
    init(island: Island) {
        super(island)
    }

    public override func preStart(): Unit {
        completeStage()
    }

    public func onPull(): Unit {}
}

class FailedSourceLogic<T> <: SourceLogic<T> {
    let cause: Exception

    init(island: Island, cause: Exception) {
        super(island)
        this.cause = cause
    }

    public override func preStart(): Unit {
        failStage(cause)
    }

    public func onPull(): Unit {}
}

/** 单个 Promise 的结果作为唯一元素 */
class PromiseSourceLogic<T> <: SourceLogic<T> {
    let promise: TypedPromise<T>
    var result: Option<AskOutcome<T>> = None<AskOutcome<T>>

    init(island: Island, promise: TypedPromise<T>) {
        super(island)
        this.promise = promise
    }

    public override func preStart(): Unit {
        let logic = this
        promise.onComplete({ o => logic.asyncCallback({ => logic.onResult(o) }) })
    }

    func onResult(o: AskOutcome<T>): Unit {
        result = Some(o)
        match (o) {
            case Failed(e) => failStage(e)
            case Replied(_) => tryEmit()
        }
    }

    public func onPull(): Unit {
        tryEmit()
    }

    private func tryEmit(): Unit {
        if (outlet.isAvailable()) {
            if (let Some(Replied(v)) <- result) {
                outlet.push(v)
                completeStage()
            }
        }
    }
}

/** 可取消的物化值（Sources.tick） */
public interface Cancellable {
    /** 首次取消返回 true */
    func cancel(): Bool

    func isCancelled(): Bool
}

class TickCancellable <: Cancellable {
    let flag = AtomicBool(false)
    var logic: Option<StageLogic> = None<StageLogic>

    public func cancel(): Bool {
        if (flag.compareAndSwap(false, true)) {
            if (let Some(l) <- logic) {
                l.asyncCallback({ => l.completeStage() })
            }
            return true
        }
        false
    }

    public func isCancelled(): Bool {
        flag.load()
    }
}

/** 定时发出同一个元素；下游没有需求时该次 tick 被丢弃（不积压） */
class TickSourceLogic<T> <: SourceLogic<T> {
    let initialDelay: Duration
    let interval: Duration
    let element: T
    let cancellable: TickCancellable

    init(island: Island, initialDelay: Duration, interval: Duration, element: T, cancellable: TickCancellable) {
        super(island)
        this.initialDelay = initialDelay
        this.interval = interval
        this.element = element
        this.cancellable = cancellable
    }

    public override func preStart(): Unit {
        if (cancellable.isCancelled()) {
            completeStage()
            return
        }
        let logic = this
        schedulePeriodically(initialDelay, interval, { => logic.tick() })
    }

    func tick(): Unit {
        if (outlet.isAvailable()) {
            outlet.push(element)
        }
    }

    public func onPull(): Unit {}
}

// ========== Source.queue / Source.actorRef ==========

/** 生产者线程与岛共享的队列状态（锁内只做簿记，信号与 Promise 完成都在锁外） */
class QueueShared<T> {
    let lock = Mutex()
    let buffer = ArrayDeque<T>()
    let pendingOffers = ArrayDeque<(T, TypedPromise<QueueOfferResult>)>()
    /** 用户调用了 complete()：排空缓冲区后完成 */
    var completing = false
    /** 用户调用了 fail() 或 Fail 策略溢出：立即失败 */
    var failure: Option<Exception> = None<Exception>
    /** 阶段已结束 */
    var closed = false
    /** 岛里有未满足的 pull 且缓冲区为空：下一次 offer 需要唤醒岛 */
    var waiting = false
}

class QueueSourceLogic<T> <: SourceLogic<T> {
    let shared = QueueShared<T>()
    let capacity: Int64
    let strategy: OverflowStrategy
    let maxConcurrentOffers: Int64
    let completion = TypedPromise<Unit>()

    init(island: Island, capacity: Int64, strategy: OverflowStrategy, maxConcurrentOffers: Int64) {
        super(island)
        this.capacity = capacity
        this.strategy = strategy
        this.maxConcurrentOffers = maxConcurrentOffers
    }

    /** 任意线程 */
    func offer(e: T): TypedPromise<QueueOfferResult> {
        let promise = TypedPromise<QueueOfferResult>()
        // 0 = 无需唤醒，1 = 唤醒岛去 push，2 = 唤醒岛让流失败
        let (immediate, wake) = synchronized(shared.lock) {
            if (shared.closed || shared.completing || shared.failure.isSome()) {
                (Some(closedResult()), 0)
            } else if (shared.buffer.size < capacity) {
                shared.buffer.addLast(e)
                let w = if (shared.waiting) { 1 } else { 0 }
                (Some(QueueOfferResult.Enqueued), w)
            } else {
                match (strategy) {
                    case DropHead =>
                        shared.buffer.removeFirst()
                        shared.buffer.addLast(e)
                        (Some(QueueOfferResult.Enqueued), 0)
                    case DropTail =>
                        shared.buffer.removeLast()
                        shared.buffer.addLast(e)
                        (Some(QueueOfferResult.Enqueued), 0)
                    case DropBuffer =>
                        shared.buffer.clear()
                        shared.buffer.addLast(e)
                        (Some(QueueOfferResult.Enqueued), 0)
                    case DropNew => (Some(QueueOfferResult.Dropped), 0)
                    case Fail =>
                        let ex = BufferOverflowException("Source.queue 缓冲区已满（${capacity}）")
                        shared.failure = Some(ex)
                        (Some(QueueOfferResult.QueueFailure(ex)), 2)
                    case Backpressure =>
                        if (shared.pendingOffers.size >= maxConcurrentOffers) {
                            (Some(QueueOfferResult.QueueFailure(IllegalStateException(
                                "并发未完成的 offer 超过 ${maxConcurrentOffers} 个：请等前一个 offer 的 Promise 完成后再 offer"))), 0)
                        } else {
                            shared.pendingOffers.addLast((e, promise))
                            (None<QueueOfferResult>, 0)
                        }
                }
            }
        }
        if (let Some(r) <- immediate) {
            promise.trySuccess(r)
        }
        if (wake != 0) {
            wakeUp()
        }
        promise
    }

    private func closedResult(): QueueOfferResult {
        match (shared.failure) {
            case Some(ex) => QueueOfferResult.QueueFailure(ex)
            case None => QueueOfferResult.QueueClosed
        }
    }

    func requestComplete(): Unit {
        let wake = synchronized(shared.lock) {
            if (shared.closed || shared.completing || shared.failure.isSome()) {
                false
            } else {
                shared.completing = true
                true
            }
        }
        if (wake) {
            wakeUp()
        }
    }

    func requestFail(cause: Exception): Unit {
        let wake = synchronized(shared.lock) {
            if (shared.closed || shared.failure.isSome()) {
                false
            } else {
                shared.failure = Some(cause)
                true
            }
        }
        if (wake) {
            wakeUp()
        }
    }

    func bufferedCount(): Int64 {
        synchronized(shared.lock) { shared.buffer.size }
    }

    private func wakeUp(): Unit {
        let logic = this
        asyncCallback({ => logic.serve() })
    }

    public func onPull(): Unit {
        serve()
    }

    /** 岛线程：处理失败 / 发出一个元素 / 补入一个背压中的 offer / 排空后完成 */
    func serve(): Unit {
        if (let Some(ex) <- synchronized(shared.lock) { shared.failure }) {
            completion.tryFailure(ex)
            failStage(ex)
            return
        }
        if (!outlet.isAvailable()) {
            return
        }
        // 0 = 等待，1 = push，2 = 完成
        let (action, element, admitted) = synchronized(shared.lock) {
            match (shared.buffer.removeFirst()) {
                case Some(e) =>
                    shared.waiting = false
                    // 腾出一个位置：放入一个被背压的 offer
                    let adm = match (shared.pendingOffers.removeFirst()) {
                        case Some((pe, pp)) =>
                            shared.buffer.addLast(pe)
                            Some(pp)
                        case None => None<TypedPromise<QueueOfferResult>>
                    }
                    (1, Some(e), adm)
                case None =>
                    if (shared.completing) {
                        (2, None<T>, None<TypedPromise<QueueOfferResult>>)
                    } else {
                        shared.waiting = true
                        (0, None<T>, None<TypedPromise<QueueOfferResult>>)
                    }
            }
        }
        if (let Some(p) <- admitted) {
            p.trySuccess(QueueOfferResult.Enqueued)
        }
        if (action == 1) {
            outlet.push(element.getOrThrow())
        } else if (action == 2) {
            completeStage()
        }
    }

    public override func postStop(): Unit {
        let (pending, result) = synchronized(shared.lock) {
            shared.closed = true
            shared.buffer.clear()
            let ps = ArrayList<TypedPromise<QueueOfferResult>>()
            while (let Some((_, p)) <- shared.pendingOffers.removeFirst()) {
                ps.add(p)
            }
            (ps, closedResult())
        }
        for (p in pending) {
            p.trySuccess(result)
        }
        completion.trySuccess(())
    }

    public override func onAbort(cause: Exception): Unit {
        completion.tryFailure(cause)
    }
}

/**
 * Sources.queue 的物化值：从流外向流里送元素（线程安全）
 */
public class SourceQueue<T> {
    let logic: QueueSourceLogic<T>

    init(logic: QueueSourceLogic<T>) {
        this.logic = logic
    }

    /**
     * 送入一个元素。非背压策略下返回的 Promise 已完成；Backpressure 策略下缓冲区满时
     * Promise 在腾出位置后才以 Enqueued 完成——生产者应等它完成再 offer 下一个。
     */
    public func offer(element: T): TypedPromise<QueueOfferResult> {
        logic.offer(element)
    }

    /** 不再有新元素：缓冲区排空后流正常完成 */
    public func complete(): Unit {
        logic.requestComplete()
    }

    /** 立即以 cause 让流失败，缓冲区里的元素丢弃 */
    public func fail(cause: Exception): Unit {
        logic.requestFail(cause)
    }

    /** 源阶段结束（完成、失败或下游取消）时完成 */
    public func watchCompletion(): TypedPromise<Unit> {
        logic.completion
    }

    /** 缓冲区当前长度（观测用） */
    public func bufferedCount(): Int64 {
        logic.bufferedCount()
    }
}

/**
 * Sources.actorRef 的物化值：一个轻量的类型化引用（不是 Actor，不能被 watch 或跨节点传递）。
 * tell 进来的消息按溢出策略进入缓冲区；completeWhen / failWhen 匹配的消息用于结束流。
 */
class QueueActorRef<T> <: TypedActorRef<T> {
    private static let seq = AtomicInt64(0)
    let logic: QueueSourceLogic<T>
    let completeWhen: (T) -> Bool
    let failWhen: (T) -> Option<Exception>
    let path: String

    init(logic: QueueSourceLogic<T>, completeWhen: (T) -> Bool, failWhen: (T) -> Option<Exception>) {
        this.logic = logic
        this.completeWhen = completeWhen
        this.failWhen = failWhen
        this.path = "/temp/stream-actor-source-${seq.fetchAdd(1) + 1}"
    }

    public func tell(message: T): Unit {
        if (completeWhen(message)) {
            logic.requestComplete()
            return
        }
        if (let Some(ex) <- failWhen(message)) {
            logic.requestFail(ex)
            return
        }
        logic.offer(message)
        ()
    }

    public func pathString(): String {
        path
    }

    public func equals(other: TypedActorRef<T>): Bool {
        other.pathString() == path
    }
}

/** unfold：从状态 s 起，f 返回 None 时结束 */
class UnfoldIterator<S, T> <: Iterator<T> {
    var state: S
    let f: (S) -> Option<(S, T)>
    var done = false

    init(seed: S, f: (S) -> Option<(S, T)>) {
        this.state = seed
        this.f = f
    }

    public func next(): Option<T> {
        if (done) {
            return None<T>
        }
        match (f(state)) {
            case Some((s, e)) =>
                state = s
                Some(e)
            case None =>
                done = true
                None<T>
        }
    }
}

/** 无限重复同一个元素 */
class RepeatIterator<T> <: Iterator<T> {
    let element: T

    init(element: T) {
        this.element = element
    }

    public func next(): Option<T> {
        Some(element)
    }
}

/** [start, end] 闭区间，stride 可为负 */
class RangeIterator <: Iterator<Int64> {
    var cursor: Int64
    let bound: Int64
    let stride: Int64
    var exhausted = false

    init(start: Int64, end: Int64, stride: Int64) {
        this.cursor = start
        this.bound = end
        this.stride = stride
    }

    public func next(): Option<Int64> {
        if (exhausted || (stride > 0 && cursor > bound) || (stride < 0 && cursor < bound)) {
            exhausted = true
            return None<Int64>
        }
        let v = cursor
        // 防止越过 Int64 边界后回绕成无限序列
        if ((stride > 0 && cursor > Int64.Max - stride) || (stride < 0 && cursor < Int64.Min - stride)) {
            exhausted = true
        } else {
            cursor += stride
        }
        Some(v)
    }
}
