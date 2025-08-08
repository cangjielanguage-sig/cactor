/*
 * CActor Runtime - Timers for Typed Actors (once, periodic, backoff)
 */
package cactor.runtime.typed

import std.time.*
import std.collection.*
import std.sync.*
import cactor.core.typed.{TypedActorRef, TypedScheduler, TypedCancellable}

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
        let anyRef = AnyRefAdapter<T>(owner)
        scheduler.scheduleOnce(delay, anyRef, msg as Any)
        mutex.unlock()
    }

    /** 周期性定时发送 */
    public func startPeriodicTimer(key: String, msg: T, initialDelay: Duration, interval: Duration): Unit {
        mutex.lock()
        cancelTimerInternal(key)
        let anyRef = AnyRefAdapter<T>(owner)
        let c = scheduler.schedule(initialDelay, interval, anyRef, msg as Any)
        tasks.add(key, c)
        mutex.unlock()
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

