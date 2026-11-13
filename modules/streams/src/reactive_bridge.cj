/*
 * CActor Streams - 流与 Reactive Streams 接口的桥接
 *
 * PublisherSinkLogic：把上游变成 Publisher（Sink.asPublisher / Sink.fromSubscriber / 异步边界的上游侧）。
 * SubscriberSourceLogic：把 Publisher 变成上游（Source.fromPublisher / Source.asSubscriber / 异步边界的下游侧）。
 *
 * 异步边界就是这两者的组合：下游岛一次 request(bufferSize)，每消耗一半再补一次，上游岛只在有需求时 pull，
 * 所以两个岛之间在途的元素永远不超过 bufferSize。跨节点的信用制流（StreamRef 式）只需实现同样的
 * Publisher / Subscriber 接口即可通过 Source.fromPublisher / Sink.fromSubscriber 接入。
 */
package cactor_streams

import std.collection.*
import std.sync.*
import cactor_streams.reactive.*

// ========== 上游 → Publisher ==========

/** 订阅者与终止信号的共享状态：subscribe 可能来自任意线程，与岛内的终止竞争，由锁决定由谁投递 */
class PublisherShared<T> {
    let lock = Mutex()
    var subscriber: Option<Subscriber<T>> = None<Subscriber<T>>
    /** 已经向订阅者发过 onSubscribe */
    var subscribedSent = false
    /** 流已结束：None 表示仍在运行；Some(None) 完成；Some(Some(e)) 失败 */
    var terminal: Option<Option<Exception>> = None<Option<Exception>>
    /** 终止信号已投递给订阅者 */
    var terminalSent = false
}

/** Sink.asPublisher 的物化值（只允许一个订阅者） */
class StagePublisher<T> <: Publisher<T> {
    let logic: PublisherSinkLogic<T>

    init(logic: PublisherSinkLogic<T>) {
        this.logic = logic
    }

    public func subscribe(subscriber: Subscriber<T>): Unit {
        logic.subscribeFromOutside(subscriber)
    }
}

/**
 * 订阅：request / cancel 都投递回岛里执行，订阅者在 onNext 里同步 request 也不会重入。
 * 不做成泛型类：只持有两个在主包里构造好的闭包。
 */
class StageSubscription <: Subscription {
    let requestFn: (Int64) -> Unit
    let cancelFn: () -> Unit

    init(requestFn: (Int64) -> Unit, cancelFn: () -> Unit) {
        this.requestFn = requestFn
        this.cancelFn = cancelFn
    }

    public func request(n: Int64): Unit {
        requestFn(n)
    }

    public func cancel(): Unit {
        cancelFn()
    }
}

class PublisherSinkLogic<T> <: SinkLogic<T> {
    let shared = PublisherShared<T>()
    var demand: Int64 = 0
    var cancelled = false
    private var publisherOpt: Option<StagePublisher<T>> = None<StagePublisher<T>>

    init(island: Island, inlet: Conn<T>, preset: Option<Subscriber<T>>) {
        super(island, inlet)
        shared.subscriber = preset
        publisherOpt = Some(StagePublisher<T>(this))
    }

    prop publisher: StagePublisher<T> {
        get() { publisherOpt.getOrThrow() }
    }

    public override func preStart(): Unit {
        // Sink.fromSubscriber：订阅者在物化时就确定
        let has = synchronized(shared.lock) { shared.subscriber.isSome() && !shared.subscribedSent }
        if (has) {
            sendOnSubscribe()
        }
    }

    /** 任意线程：处理一次 subscribe */
    func subscribeFromOutside(s: Subscriber<T>): Unit {
        // 0 = 拒绝（已有订阅者），1 = 投递回岛，2 = 流已结束、在调用方线程立即发终止信号
        let decision = synchronized(shared.lock) {
            if (shared.subscriber.isSome()) {
                0
            } else {
                shared.subscriber = Some(s)
                if (shared.terminal.isSome()) {
                    shared.subscribedSent = true
                    shared.terminalSent = true
                    2
                } else {
                    1
                }
            }
        }
        if (decision == 0) {
            s.onSubscribe(ReactiveStreams.cancelledSubscription())
            s.onError(ReactiveStreamsSpecException("Sink.asPublisher 只允许一个订阅者（规范 1.10）"))
        } else if (decision == 2) {
            s.onSubscribe(ReactiveStreams.cancelledSubscription())
            signalTerminal(s, shared.terminal.getOrThrow())
        } else {
            let logic = this
            // 阶段若在投递前结束，由 deliverTerminal 负责补发 onSubscribe + 终止信号
            asyncCallback({ => logic.sendOnSubscribe() })
        }
    }

    func sendOnSubscribe(): Unit {
        let s = synchronized(shared.lock) {
            if (shared.subscribedSent) {
                None<Subscriber<T>>
            } else {
                shared.subscribedSent = true
                shared.subscriber
            }
        }
        if (let Some(sub) <- s) {
            let logic = this
            sub.onSubscribe(StageSubscription(
                { n => logic.asyncCallback({ => logic.onRequest(n) }) },
                { => logic.asyncCallback({ => logic.onCancel() }) }))
        }
    }

    func onRequest(n: Int64): Unit {
        if (cancelled) {
            return
        }
        if (n <= 0) {
            // 规范 3.9：以 onError 通知订阅者，并取消上游
            deliverTerminal(Some(ReactiveStreams.nonPositiveRequest(n)))
            completeStage()
            return
        }
        demand = ReactiveStreams.addDemand(demand, n)
        if (!inlet.hasBeenPulled() && !inlet.isClosedForIn()) {
            inlet.pull()
        }
    }

    func onCancel(): Unit {
        if (cancelled) {
            return
        }
        cancelled = true
        synchronized(shared.lock) {
            // 取消后不再发任何信号（规范 3.5 / 3.6）
            shared.terminal = Some(None<Exception>)
            shared.terminalSent = true
        }
        completeStage()
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        demand--
        let s = shared.subscriber.getOrThrow()
        try {
            s.onNext(e)
        } catch (ex: Exception) {
            // 规范 2.13：订阅者抛异常视为它已取消
            cancelled = true
            synchronized(shared.lock) {
                shared.terminal = Some(None<Exception>)
                shared.terminalSent = true
            }
            completeStage()
            return
        }
        if (demand > 0 && !cancelled) {
            inlet.pull()
        }
    }

    public override func onUpstreamFinish(): Unit {
        deliverTerminal(None<Exception>)
        completeStage()
    }

    public override func onUpstreamFailure(cause: Exception): Unit {
        deliverTerminal(Some(cause))
        failStage(cause)
    }

    public override func onAbort(cause: Exception): Unit {
        deliverTerminal(Some(cause))
    }

    /** 岛线程：记录终止状态；若订阅者已到，补发尚未发出的 onSubscribe 并发终止信号（只发一次） */
    func deliverTerminal(t: Option<Exception>): Unit {
        let action = synchronized(shared.lock) {
            if (shared.terminal.isNone()) {
                shared.terminal = Some(t)
            }
            if (shared.terminalSent || shared.subscriber.isNone()) {
                None<(Subscriber<T>, Bool)>
            } else {
                shared.terminalSent = true
                let needSubscribe = !shared.subscribedSent
                shared.subscribedSent = true
                Some((shared.subscriber.getOrThrow(), needSubscribe))
            }
        }
        if (let Some((s, needSubscribe)) <- action) {
            if (needSubscribe) {
                s.onSubscribe(ReactiveStreams.cancelledSubscription())
            }
            signalTerminal(s, t)
        }
    }

    private func signalTerminal(s: Subscriber<T>, t: Option<Exception>): Unit {
        try {
            match (t) {
                case Some(e) => s.onError(e)
                case None => s.onComplete()
            }
        } catch (_: Exception) {}
    }
}

// ========== Publisher → 上游 ==========

/** Source.asSubscriber 的物化值；也是异步边界 / fromPublisher 交给上游的订阅者 */
class StageSubscriber<T> <: Subscriber<T> {
    let logic: SubscriberSourceLogic<T>

    init(logic: SubscriberSourceLogic<T>) {
        this.logic = logic
    }

    public func onSubscribe(subscription: Subscription): Unit {
        let l = logic
        // 阶段已结束：不再需要这个订阅，立即取消（规范 2.5 的精神）
        l.asyncCallbackOr({ => l.onSubscribed(subscription) }, { => subscription.cancel() })
    }

    public func onNext(element: T): Unit {
        let l = logic
        l.asyncCallback({ => l.onElement(element) })
    }

    public func onError(cause: Exception): Unit {
        let l = logic
        l.asyncCallback({ => l.onUpstreamError(cause) })
    }

    public func onComplete(): Unit {
        let l = logic
        l.asyncCallback({ => l.onUpstreamComplete() })
    }
}

class SubscriberSourceLogic<T> <: SourceLogic<T> {
    let bufferSize: Int64
    let batch: Int64
    let buffer = ArrayDeque<T>()
    private var subscriberOpt: Option<StageSubscriber<T>> = None<StageSubscriber<T>>
    let toSubscribe: Option<Publisher<T>>
    var subscription: Option<Subscription> = None<Subscription>
    /** 已交给下游、尚未补请求的数量 */
    var freed: Int64 = 0
    var upstreamDone = false
    /** 观测用：缓冲区达到过的最大长度（测试据此验证内存有界） */
    var peakBuffered: Int64 = 0

    init(island: Island, bufferSize: Int64, toSubscribe: Option<Publisher<T>>) {
        super(island)
        if (bufferSize <= 0) {
            throw IllegalArgumentException("bufferSize 必须为正数，收到 ${bufferSize}")
        }
        this.bufferSize = bufferSize
        this.batch = if (bufferSize / 2 > 0) { bufferSize / 2 } else { 1 }
        this.toSubscribe = toSubscribe
        subscriberOpt = Some(StageSubscriber<T>(this))
    }

    prop subscriber: StageSubscriber<T> {
        get() { subscriberOpt.getOrThrow() }
    }

    public override func preStart(): Unit {
        if (let Some(p) <- toSubscribe) {
            p.subscribe(subscriber)
        }
    }

    func onSubscribed(s: Subscription): Unit {
        if (subscription.isSome()) {
            // 规范 2.5：已有订阅，取消新来的
            s.cancel()
            return
        }
        subscription = Some(s)
        s.request(bufferSize)
    }

    func onElement(e: T): Unit {
        if (buffer.size >= bufferSize) {
            failStage(ReactiveStreamsSpecException("上游发出的元素超过了请求数量（规范 1.1）"))
            return
        }
        buffer.addLast(e)
        if (buffer.size > peakBuffered) {
            peakBuffered = buffer.size
        }
        tryPush()
    }

    func onUpstreamComplete(): Unit {
        upstreamDone = true
        tryPush()
    }

    func onUpstreamError(cause: Exception): Unit {
        // 与 Akka 一致：上游失败时立即失败，缓冲区里尚未交付的元素丢弃
        upstreamDone = true
        subscription = None<Subscription>
        failStage(cause)
    }

    public func onPull(): Unit {
        tryPush()
    }

    private func tryPush(): Unit {
        if (outlet.isAvailable()) {
            match (buffer.removeFirst()) {
                case Some(e) =>
                    outlet.push(e)
                    freed++
                    if (freed >= batch && !upstreamDone) {
                        if (let Some(s) <- subscription) {
                            s.request(freed)
                        }
                        freed = 0
                    }
                case None => ()
            }
        }
        if (upstreamDone && buffer.isEmpty()) {
            completeStage()
        }
    }

    public override func postStop(): Unit {
        if (!upstreamDone) {
            if (let Some(s) <- subscription) {
                s.cancel()
            }
        }
        buffer.clear()
    }
}

// ========== Processor ==========

/** Flow.toProcessor 的结果：订阅者一侧与发布者一侧分别是一次物化的入口与出口 */
class FlowProcessor<In, Out> <: Processor<In, Out> {
    let inSide: Subscriber<In>
    let outSide: Publisher<Out>

    init(inSide: Subscriber<In>, outSide: Publisher<Out>) {
        this.inSide = inSide
        this.outSide = outSide
    }

    public func onSubscribe(subscription: Subscription): Unit {
        inSide.onSubscribe(subscription)
    }

    public func onNext(element: In): Unit {
        inSide.onNext(element)
    }

    public func onError(cause: Exception): Unit {
        inSide.onError(cause)
    }

    public func onComplete(): Unit {
        inSide.onComplete()
    }

    public func subscribe(subscriber: Subscriber<Out>): Unit {
        outSide.subscribe(subscriber)
    }
}
