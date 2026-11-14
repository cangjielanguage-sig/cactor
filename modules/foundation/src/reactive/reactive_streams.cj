/*
 * CActor Foundation.Reactive - Reactive Streams 接口（对标 org.reactivestreams 1.0.4）
 *
 * 接口下沉到零运行时依赖的基础层，让集群信用流与本地流共用协议，避免同层模块互相依赖。
 * cactor_streams.reactive 保留兼容重导出。
 */
package cactor_foundation.reactive

/** 线程安全的需求与取消句柄；request 不在调用线程同步回调下游。 */
public interface Subscription {
    func request(n: Int64): Unit
    func cancel(): Unit
}

/** 四种信号须串行发出，onSubscribe 最先，终态至多一次；回调不得抛异常。 */
public interface Subscriber<T> {
    func onSubscribe(subscription: Subscription): Unit
    func onNext(element: T): Unit
    func onError(cause: Exception): Unit
    func onComplete(): Unit
}

/** 每次订阅都先发送 onSubscribe；单次物化发布者可随后拒绝额外订阅。 */
public interface Publisher<T> {
    func subscribe(subscriber: Subscriber<T>): Unit
}

public interface Processor<In, Out> <: Subscriber<In> & Publisher<Out> {}

/** 需求超发、重复订阅等 Reactive Streams 协议违例。 */
public class ReactiveStreamsSpecException <: Exception {
    public init(message: String) { super(message) }
}

public struct ReactiveStreams {
    /** 正数需求饱和累加，Int64.Max 表示无界。 */
    public static func addDemand(current: Int64, n: Int64): Int64 {
        if (current >= Int64.Max - n) { Int64.Max } else { current + n }
    }

    public static func nonPositiveRequest(n: Int64): IllegalArgumentException {
        IllegalArgumentException("Reactive Streams 规范 3.9：request 的数量必须为正数，收到 ${n}")
    }

    public static func cancelledSubscription(): Subscription { CancelledSubscription() }
}

class CancelledSubscription <: Subscription {
    public func request(n: Int64): Unit {}
    public func cancel(): Unit {}
}
