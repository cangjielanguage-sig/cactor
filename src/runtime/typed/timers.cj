/*
 * CActor Runtime - Timers for Typed Actors (once, periodic, backoff)
 */
package cactor.runtime.typed

import std.time.*
import std.collection.*
import std.sync.*
import cactor.core.typed.{TypedActorRef, TypedScheduler, TypedCancellable, TypedAdapters}

/** 将 TypedActorRef<T> 适配为 TypedActorRef<Any> 以供调度器使用 */
internal class AnyRefAdapter<X> <: TypedActorRef<Any> {
    private let underlying: TypedActorRef<X>
    public init(underlying: TypedActorRef<X>) { this.underlying = underlying }
    public func tell(message: Any): Unit {
        match (message) {
            case m: X => underlying.tell(m)
            case _ => ()
        }
    }
    public func pathString(): String { underlying.pathString() }
    public func equals(other: TypedActorRef<Any>): Bool { pathString() == other.pathString() }
}


/** 简单一次性/周期性定时器实现（基于 TypedScheduler 包装） */
public class TypedTimers<T> {
    private let scheduler: TypedScheduler
    private let owner: TypedActorRef<T>
    private let tasks: HashMap<String, TypedCancellable>
    private let mutex: Mutex

    public init(scheduler: TypedScheduler, owner: TypedActorRef<T>) {
        this.scheduler = scheduler
        this.owner = owner
        this.tasks = HashMap<String, TypedCancellable>()
        this.mutex = Mutex()
    }

    /** 一次性定时发送 */
    public func startSingleTimer(key: String, msg: T, delay: Duration): Unit {
        mutex.lock()
        cancelTimerInternal(key)
        // scheduleOnce 无返回（当前接口），直接派发一次
        let anyRef = TypedAdapters.messageAdapter<Any, T>(owner, { _ => msg })
        scheduler.scheduleOnce(delay, anyRef, msg as Any)
        mutex.unlock()
    }

    /** 周期性定时发送 */
    public func startPeriodicTimer(key: String, msg: T, initialDelay: Duration, interval: Duration): Unit {
        mutex.lock()
        cancelTimerInternal(key)
        let anyRef = TypedAdapters.messageAdapter<Any, T>(owner, { _ => msg })
        let c = scheduler.schedule(initialDelay, interval, anyRef, msg as Any)
        tasks.add(key, c)
        mutex.unlock()
    }

    /** Backoff 指数退避 + 抖动：返回可取消 */
    public func startBackoffTimer(key: String, msg: T, minBackoff: Duration, maxBackoff: Duration, randomFactor: Float64, maxRetries: Int32): TypedCancellable {
        mutex.lock()
        cancelTimerInternal(key)
        let backoff = BackoffTimer<T>(scheduler, owner, msg, minBackoff, maxBackoff, randomFactor, maxRetries)
        let canc = backoff.start()
        tasks.add(key, canc)
        mutex.unlock()
        canc
    }


    /** 取消定时器 */
    public func cancel(key: String): Unit {
        mutex.lock()
        cancelTimerInternal(key)
        mutex.unlock()
    }

    /** 取消所有定时器 */
    public func cancelAll(): Unit {
        mutex.lock()
        for (key in tasks.keys()) { cancelTimerInternal(key) }
        mutex.unlock()
    }

    private func cancelTimerInternal(key: String): Unit {
        match (tasks.get(key)) {
            case Some(c) => c.cancel(); tasks.remove(key)
            case None => ()
        }
    }
}

/** BackoffTimer 内部实现：指数退避 + 抖动 */
internal class BackoffTimer<X> {
    private let scheduler: TypedScheduler
    private let target: TypedActorRef<X>
    private let message: X
    private let minBackoff: Duration
    private let maxBackoff: Duration
    private let randomFactor: Float64
    private let maxRetries: Int32

    public init(scheduler: TypedScheduler, target: TypedActorRef<X>, message: X, minBackoff: Duration, maxBackoff: Duration, randomFactor: Float64, maxRetries: Int32) {
        this.scheduler = scheduler
        this.target = target
        this.message = message
        this.minBackoff = minBackoff
        this.maxBackoff = maxBackoff
        this.randomFactor = randomFactor
        this.maxRetries = maxRetries
    }

    public func start(): TypedCancellable {
        let c = BackoffCancellable()
        // 由于 TypedScheduler.scheduleOnce 无取消句柄，这里预先调度所有尝试
        // 实际延迟由 scheduler 实现；若 scheduler 忽略延迟，则按当前实现立即触发（最小实现）
        var attempt: Int32 = 0
        while (attempt < maxRetries && !c.isCancelled()) {
            let delay = computeDelay(attempt)
            let anyRef = TypedAdapters.messageAdapter<Any, X>(target, { _ => message })
            scheduler.scheduleOnce(delay, anyRef, message as Any)
            attempt += 1
        }
        c
    }

    private func computeDelay(n: Int32): Duration {
        let baseMs = minBackoff.toMilliseconds() * (1 << n)
        let capped = if (baseMs > maxBackoff.toMilliseconds()) { maxBackoff.toMilliseconds() } else { baseMs }
        if (randomFactor <= 0.0) { return Duration.millisecond * capped }
        let jitter = (2.0 * Rand.random() - 1.0) * randomFactor // [-f, +f]
        let final = Int64(Float64(capped) * (1.0 + jitter))
        Duration.millisecond * final
    }
}

/** 可取消对象（最小） */
internal class BackoffCancellable <: TypedCancellable {
    private var cancelled: Bool = false
    public func cancel(): Bool { if (!cancelled) { cancelled = true; return true } else { return false } }
    public func isCancelled(): Bool { cancelled }
}

/** 简化随机 */
internal struct Rand { public static func random(): Float64 { 0.5 } }


