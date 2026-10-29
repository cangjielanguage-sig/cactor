/*
 * CActor Core.Typed - Actor 级定时器与暂存缓冲区（对标 Akka TimerScheduler / StashBuffer）
 */
package cactor_core.typed

import std.time.*
import std.collection.*
import cactor_core.actor.{TimerScheduler}

/**
 * 类型化的 Actor 定时器
 *
 * 基于经典 Cell 定时器实现：同一 key 只保留一个定时器，再次 start 会替换旧定时器；
 * 已取消的定时器即使消息已在途也不会被处理；Actor 停止或重启时全部取消。
 * 只能在 Actor 内部使用。
 */
public class TypedTimerScheduler<T> {
    private let classic: TimerScheduler

    public init(classic: TimerScheduler) {
        this.classic = classic
    }

    /** delay 之后向自己投递一次 message */
    public func startSingleTimer(key: String, message: T, delay: Duration): Unit {
        classic.startSingleTimer(key, TypedEnvelope<T>(message), delay)
    }

    /** 每次投递之间至少间隔 delay */
    public func startTimerWithFixedDelay(key: String, message: T, delay: Duration): Unit {
        classic.startTimerWithFixedDelay(key, TypedEnvelope<T>(message), delay)
    }

    /** 按固定频率投递 */
    public func startTimerAtFixedRate(key: String, message: T, interval: Duration): Unit {
        classic.startTimerAtFixedRate(key, TypedEnvelope<T>(message), interval)
    }

    public func isTimerActive(key: String): Bool {
        classic.isTimerActive(key)
    }

    public func cancel(key: String): Unit {
        classic.cancel(key)
    }

    public func cancelAll(): Unit {
        classic.cancelAll()
    }
}

/** 暂存缓冲区已满 */
public class StashOverflowException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 类型化暂存缓冲区
 *
 * 典型用法：初始化完成前把消息 stash 起来，完成后 unstashAll(新行为) 依次处理。
 * 只能在 Actor 内部使用（非线程安全）。
 */
public class TypedStashBuffer<T> {
    private let capacityValue: Int64
    private let context: TypedActorContext<T>
    private let buffer = ArrayList<T>()

    public init(capacity: Int64, context: TypedActorContext<T>) {
        if (capacity <= 0) {
            throw IllegalArgumentException("stash 容量必须为正数: ${capacity}")
        }
        this.capacityValue = capacity
        this.context = context
    }

    /** 暂存一条消息；已满时抛出 StashOverflowException */
    public func stash(message: T): Unit {
        if (buffer.size >= capacityValue) {
            throw StashOverflowException("stash 已满（容量 ${capacityValue}）")
        }
        buffer.add(message)
    }

    public func size(): Int64 {
        buffer.size
    }

    public func capacity(): Int64 {
        capacityValue
    }

    public func isEmpty(): Bool {
        buffer.isEmpty()
    }

    public func isFull(): Bool {
        buffer.size >= capacityValue
    }

    public func clear(): Unit {
        buffer.clear()
    }

    /** 对每条暂存消息执行 f（不移除） */
    public func forEach(f: (T) -> Unit): Unit {
        for (m in buffer) {
            f(m)
        }
    }

    /**
     * 用 behavior 按原顺序处理全部暂存消息，返回处理后的行为。
     * 处理过程中再次 stash 的消息保留在缓冲区中；行为变为 stopped 时剩余消息被丢弃。
     */
    public func unstashAll(behavior: TypedBehavior<T>): TypedBehavior<T> {
        let messages = buffer.toArray()
        buffer.clear()
        TypedBehaviors.interpretAll(behavior, messages, context)
    }

    /** 只取出前 n 条处理 */
    public func unstash(behavior: TypedBehavior<T>, n: Int64): TypedBehavior<T> {
        let count = if (n < buffer.size) { n } else { buffer.size }
        let messages = Array<T>(count, { i => buffer[i] })
        buffer.remove(0..count)
        TypedBehaviors.interpretAll(behavior, messages, context)
    }
}
