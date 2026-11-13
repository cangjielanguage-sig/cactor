/*
 * CActor Streams - Reactive Streams 接口（对标 org.reactivestreams 1.0.4）
 *
 * 只放四个接口与少量规范辅助，不依赖任何运行时：流 DSL（cactor_streams）用它们做异步边界、
 * Source.fromPublisher / Sink.asPublisher；跨节点适配（例如集群里的信用制流式回复）只要实现这几个接口，
 * 就能接进本地流而不需要了解流内部的解释器。
 *
 * 本库实现遵守的规范要点（编号同官方规范）：
 * - 1.1  onNext 的次数不超过累计 request 的总和
 * - 1.3  同一订阅者上的信号串行发出（不并发）
 * - 1.7  onComplete / onError 只发一次，之后不再有任何信号
 * - 1.9  subscribe 总是先回调 onSubscribe
 * - 2.5  已有订阅时再收到 onSubscribe，取消新的订阅
 * - 3.5 / 3.7  cancel 幂等；取消后 request 无效果
 * - 3.9  request(n) 中 n <= 0 时以 IllegalArgumentException 发出 onError
 * - 3.17 需求累加到 Int64.Max 视为"无界"，不溢出
 */
package cactor_streams.reactive

/** 订阅：订阅者用它向发布者声明需求或取消。实现必须线程安全，且不能在调用方线程里同步回调订阅者（防重入）。 */
public interface Subscription {
    /** 声明再接收 n 个元素；n <= 0 违反规范 3.9，发布者以 onError 通知 */
    func request(n: Int64): Unit

    /** 取消订阅；幂等，之后发布者最终停止发信号 */
    func cancel(): Unit
}

/** 订阅者：四个回调都不得抛出异常（规范 2.13），且只会被串行调用 */
public interface Subscriber<T> {
    func onSubscribe(subscription: Subscription): Unit

    func onNext(element: T): Unit

    func onError(cause: Exception): Unit

    func onComplete(): Unit
}

/** 发布者：subscribe 可被多次调用，每次都会先回调 onSubscribe */
public interface Publisher<T> {
    func subscribe(subscriber: Subscriber<T>): Unit
}

/** 处理器：既是订阅者也是发布者 */
public interface Processor<In, Out> <: Subscriber<In> & Publisher<Out> {}

/** 规范违例（例如 request(n <= 0)、同一发布者被第二个订阅者订阅） */
public class ReactiveStreamsSpecException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** 规范辅助 */
public struct ReactiveStreams {
    /**
     * 需求累加（规范 3.17）：两个非负需求相加，溢出时饱和到 Int64.Max（表示"无界"）
     */
    public static func addDemand(current: Int64, n: Int64): Int64 {
        if (current >= Int64.Max - n) { Int64.Max } else { current + n }
    }

    /** 规范 3.9 的标准异常 */
    public static func nonPositiveRequest(n: Int64): IllegalArgumentException {
        IllegalArgumentException("Reactive Streams 规范 3.9：request 的数量必须为正数，收到 ${n}")
    }

    /** 已取消的订阅（规范 1.9/1.10：拒绝订阅时先给一个无效订阅再发 onError） */
    public static func cancelledSubscription(): Subscription {
        CancelledSubscription()
    }
}

class CancelledSubscription <: Subscription {
    public func request(n: Int64): Unit {}

    public func cancel(): Unit {}
}
