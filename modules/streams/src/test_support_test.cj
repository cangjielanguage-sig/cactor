/*
 * CActor Streams - 测试辅助（只参与测试编译）
 *
 * streams 与 testkit 同层，不能依赖它；这里放一个按条件轮询的等待函数和两个手动控制的
 * Reactive Streams 端点。等待都以条件为准，不用固定时长的 sleep。
 */
package cactor_streams

import std.collection.*
import std.sync.*
import std.time.*
import cactor_core.typed.*
import cactor_streams.reactive.*

/** 等待条件成立（每毫秒检查一次），超时返回最后一次检查结果 */
func awaitTrue(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        if (cond()) {
            return true
        }
        sleep(Duration.millisecond)
    }
    cond()
}

/** Promise 的失败原因（成功时返回 None） */
func failureOf<T>(p: TypedPromise<T>, timeout: Duration): Option<Exception> {
    try {
        p.get(timeout)
        None<Exception>
    } catch (e: Exception) {
        Some(e)
    }
}

/** 线程安全的计数器 / 列表 */
class Recorder<T> {
    let lock = Mutex()
    let items = ArrayList<T>()

    func add(x: T): Unit {
        synchronized(lock) { items.add(x) }
    }

    func size(): Int64 {
        synchronized(lock) { items.size }
    }

    func snapshot(): Array<T> {
        synchronized(lock) { items.toArray() }
    }
}

/**
 * 手动 request 的订阅者：记录信号顺序，并在 onNext 时检查是否超出了累计需求（规范 1.1）
 */
class TestSubscriber<T> <: Subscriber<T> {
    let lock = Mutex()
    var subscription: Option<Subscription> = None<Subscription>
    let elements = ArrayList<T>()
    let signals = ArrayList<String>()
    var requested: Int64 = 0
    var demandViolations: Int64 = 0
    var signalsAfterTerminal: Int64 = 0
    var terminalCount: Int64 = 0
    var error: Option<Exception> = None<Exception>

    public func onSubscribe(s: Subscription): Unit {
        synchronized(lock) {
            if (terminalCount > 0) { signalsAfterTerminal++ }
            subscription = Some(s)
            signals.add("subscribe")
        }
    }

    public func onNext(e: T): Unit {
        synchronized(lock) {
            if (terminalCount > 0) { signalsAfterTerminal++ }
            elements.add(e)
            signals.add("next")
            if (elements.size > requested) { demandViolations++ }
        }
    }

    public func onError(cause: Exception): Unit {
        synchronized(lock) {
            if (terminalCount > 0) { signalsAfterTerminal++ }
            terminalCount++
            error = Some(cause)
            signals.add("error")
        }
    }

    public func onComplete(): Unit {
        synchronized(lock) {
            if (terminalCount > 0) { signalsAfterTerminal++ }
            terminalCount++
            signals.add("complete")
        }
    }

    /** 先记账再发出需求：任何超出需求的 onNext 都会被计为违规 */
    func request(n: Int64): Unit {
        let s = synchronized(lock) {
            if (n > 0) {
                requested = ReactiveStreams.addDemand(requested, n)
            }
            subscription
        }
        s.getOrThrow().request(n)
    }

    func cancel(): Unit {
        let s = synchronized(lock) { subscription }
        s.getOrThrow().cancel()
    }

    func received(): Int64 {
        synchronized(lock) { elements.size }
    }

    func hasSubscription(): Bool {
        synchronized(lock) { subscription.isSome() }
    }

    func terminals(): Int64 {
        synchronized(lock) { terminalCount }
    }

    func signalList(): Array<String> {
        synchronized(lock) { signals.toArray() }
    }

    func snapshot(): Array<T> {
        synchronized(lock) { elements.toArray() }
    }
}

/** 记录 request / cancel 的订阅 */
class ManualSubscription <: Subscription {
    let lock = Mutex()
    var requested: Int64 = 0
    var cancelled = false
    let requests = ArrayList<Int64>()

    public func request(n: Int64): Unit {
        synchronized(lock) {
            requests.add(n)
            requested = ReactiveStreams.addDemand(requested, n)
        }
    }

    public func cancel(): Unit {
        synchronized(lock) { cancelled = true }
    }

    func totalRequested(): Int64 {
        synchronized(lock) { requested }
    }

    func isCancelled(): Bool {
        synchronized(lock) { cancelled }
    }

    func requestList(): Array<Int64> {
        synchronized(lock) { requests.toArray() }
    }
}

/** 由测试手动发信号的发布者 */
class ManualPublisher<T> <: Publisher<T> {
    let lock = Mutex()
    var subscriber: Option<Subscriber<T>> = None<Subscriber<T>>
    let subscription = ManualSubscription()

    public func subscribe(s: Subscriber<T>): Unit {
        synchronized(lock) { subscriber = Some(s) }
        s.onSubscribe(subscription)
    }

    func sub(): Subscriber<T> {
        synchronized(lock) { subscriber.getOrThrow() }
    }

    func isSubscribed(): Bool {
        synchronized(lock) { subscriber.isSome() }
    }
}
