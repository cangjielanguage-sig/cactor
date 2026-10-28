/*
 * CActor Runtime - Stash Buffer for Typed Actors
 */
package cactor_runtime.typed

import std.collection.*
import cactor_core.typed.{TypedActorRef}

/**
 * 简单的类型安全暂存缓冲区
 * - 按插入顺序保序
 * - 容量限制，溢出返回 false
 */
public class StashBuffer<T> {
    private let capacity: Int32
    private let buffer: ArrayList<T>

    public init(capacity: Int32) {
        this.capacity = capacity
        this.buffer = ArrayList<T>()
    }

    /** 暂存消息；当达到容量上限时返回 false */
    public func stash(msg: T): Bool {
        if (buffer.size >= Int64(capacity)) { return false }
        buffer.add(msg)
        true
    }

    /** 当前暂存数量 */
    public func size(): Int32 { Int32(buffer.size) }

    public func isEmpty(): Bool { buffer.isEmpty() }

    /** 清空缓冲区 */
    public func clear(): Unit { buffer.clear() }

    /**
     * 将所有暂存消息按顺序发送给目标Actor，然后清空
     */
    public func drainTo(target: TypedActorRef<T>): Unit {
        // 逐个发送，保持顺序
        for (i in 0..buffer.size) {
            target.tell(buffer[i])
        }
        buffer.clear()
    }
}

