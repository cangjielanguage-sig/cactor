/*
 * CActor Cluster.StreamRef - Reactive Streams 适配
 *
 * 发布端复用 StreamRef 的信用窗口；订阅端只在生产句柄有空位时请求一个元素。
 * 请求通过控制 Actor 串行执行，避免同步 Publisher 在 request/onNext 之间无限递归。
 */
package cactor_cluster.streamref

import std.sync.*
import cactor_foundation.reactive.*
import cactor_core.typed.*

class RefSubscription <: Subscription {
    let underlying: StreamRefSubscription
    let invalidRequest: AtomicInt64

    init(underlying: StreamRefSubscription, invalidRequest: AtomicInt64) {
        this.underlying = underlying
        this.invalidRequest = invalidRequest
    }

    public func request(n: Int64): Unit {
        if (n <= 0) { invalidRequest.store(n) }
        underlying.request(n)
    }
    public func cancel(): Unit { underlying.cancel() }
}

class RefSubscriber<T> <: StreamRefSubscriber<T> {
    let underlying: Subscriber<T>
    let invalidRequest = AtomicInt64(1)

    init(underlying: Subscriber<T>) {
        this.underlying = underlying
    }

    public func onSubscribe(subscription: StreamRefSubscription): Unit {
        underlying.onSubscribe(RefSubscription(subscription, invalidRequest))
    }
    public func onNext(element: T): Unit { underlying.onNext(element) }
    public func onComplete(): Unit { underlying.onComplete() }
    public func onError(error: StreamRefException): Unit {
        let n = invalidRequest.load()
        match (error) {
            case _: StreamRefSubscriberException where n <= 0 =>
                underlying.onError(ReactiveStreams.nonPositiveRequest(n))
            case _ => underlying.onError(error)
        }
    }
}

class RefPublisher<T> <: Publisher<T> {
    let refs: StreamRefs
    let ref: SourceRef<T>
    let settings: StreamRefSettings
    let claimed = AtomicBool(false)

    init(refs: StreamRefs, ref: SourceRef<T>, settings: StreamRefSettings) {
        this.refs = refs
        this.ref = ref
        this.settings = settings
    }

    public func subscribe(subscriber: Subscriber<T>): Unit {
        if (!claimed.compareAndSwap(false, true)) {
            reject(subscriber, ReactiveStreamsSpecException("SourceRef Publisher 只允许一个订阅者"))
            return
        }
        try {
            refs.subscribe<T>(ref, RefSubscriber<T>(subscriber), settings: settings)
        } catch (e: Exception) {
            // 解析远程位置失败时还没有消费阶段，仍须遵守 onSubscribe 最先的信号顺序。
            reject(subscriber, e)
        }
    }

    private func reject(subscriber: Subscriber<T>, cause: Exception): Unit {
        try {
            subscriber.onSubscribe(ReactiveStreams.cancelledSubscription())
            subscriber.onError(cause)
        } catch (_: Exception) {}
    }
}

enum RefPump {
    | Pull
    | Stop
}

/** 锁只保护跨线程准入；任何 Publisher 回调均在锁外执行。 */
class ProducerSubscriber<T> <: Subscriber<T> {
    let producer: StreamWriter<T>
    let lock = Mutex()
    var upstream: Option<Subscription> = None<Subscription>
    var closed = false
    var outstanding = false
    var pump: Option<TypedActorRef<RefPump>> = None<TypedActorRef<RefPump>>

    init(producer: StreamWriter<T>) {
        this.producer = producer
    }

    func bind(ref: TypedActorRef<RefPump>): Unit {
        pump = Some(ref)
        let self = this
        producer.termination().onComplete({ _ =>
            self.cancelUpstream()
            ref.tell(RefPump.Stop)
        })
    }

    public func onSubscribe(subscription: Subscription): Unit {
        let accept = synchronized(lock) {
            if (closed || upstream.isSome()) {
                false
            } else {
                upstream = Some(subscription)
                true
            }
        }
        if (!accept) {
            subscription.cancel()
        } else {
            schedulePull()
        }
    }

    func schedulePull(): Unit {
        let self = this
        producer.whenReady().onComplete({ result =>
            if (result.toOption() ?? false) {
                if (let Some(p) <- self.pump) { p.tell(RefPump.Pull) }
            } else {
                self.cancelUpstream()
            }
        })
    }

    func pull(): Unit {
        let target = synchronized(lock) {
            if (closed || outstanding) {
                None<Subscription>
            } else {
                outstanding = true
                upstream
            }
        }
        if (let Some(s) <- target) {
            try {
                s.request(1)
            } catch (e: Exception) {
                onError(e)
            }
        }
    }

    public func onNext(element: T): Unit {
        let allowed = synchronized(lock) {
            if (closed) { return }
            let credit = outstanding
            outstanding = false
            credit
        }
        if (!allowed) {
            onError(ReactiveStreamsSpecException("StreamRef 上游未获需求就发出元素"))
            return
        }
        match (producer.offer(element)) {
            case OfferResult.Enqueued => schedulePull()
            case OfferResult.Backpressured =>
                onError(ReactiveStreamsSpecException("StreamRef 生产句柄被其他写入者占用；适配器必须独占句柄"))
            case OfferResult.Closed => cancelUpstream()
        }
    }

    public func onComplete(): Unit {
        let first = synchronized(lock) {
            if (closed) { false } else { closed = true; true }
        }
        if (first) {
            producer.complete()
            stopPump()
        }
    }

    public func onError(cause: Exception): Unit {
        let first = synchronized(lock) {
            if (closed) { false } else { closed = true; true }
        }
        if (first) {
            producer.fail(cause.message)
            cancelSubscription()
            stopPump()
        }
    }

    func cancelSubscription(): Unit {
        let target = synchronized(lock) { upstream }
        if (let Some(s) <- target) {
            try { s.cancel() } catch (_: Exception) {}
        }
    }

    func cancelUpstream(): Unit {
        let first = synchronized(lock) {
            if (closed) { false } else { closed = true; true }
        }
        if (first) { cancelSubscription() }
    }

    func stopPump(): Unit {
        if (let Some(p) <- pump) { p.tell(RefPump.Stop) }
    }
}

func producerSubscriberBehavior<T>(subscriber: ProducerSubscriber<T>): TypedBehavior<RefPump> {
    TypedBehaviors.receive<RefPump>({ _, msg =>
        match (msg) {
            case RefPump.Pull =>
                subscriber.pull()
                TypedBehaviors.same<RefPump>()
            case RefPump.Stop => TypedBehaviors.stopped<RefPump>()
        }
    }).receiveSignal({ _, signal =>
        match (signal) {
            case _: PostStop =>
                subscriber.onError(StreamRefPeerTerminatedException("StreamRef 上游适配阶段已停止"))
            case _ => ()
        }
        TypedBehaviors.same<RefPump>()
    })
}
