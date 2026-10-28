/*
 * Copyright (c) 2024. All rights reserved.
 * Foundation Layer - 无锁队列实现
 * 零依赖的高性能无锁队列，基于环形缓冲区
 *
 * 优化版本 - 整合SPSC和MPSC设计优势：
 * 1. 缓存行填充避免false sharing
 * 2. 适应性模式切换（SPSC/MPSC）
 * 3. 批量操作优化
 * 4. 减少原子操作竞争
 */
package cactor_foundation.queue

import std.sync.*
import std.collection.*
import std.time.*

/**
 * 队列运行模式枚举
 */
public enum QueueMode {
    | SPSC  // 单生产者单消费者
    | MPSC  // 多生产者单消费者
    | SPMC  // 单生产者多消费者
    | MPMC  // 多生产者多消费者
}

/**
 * 原子槽位实现
 * 专门用于环形缓冲区的槽位管理
 */
public class AtomicSlot<T> {
    private let state: AtomicInt64  // 0=空, 1=有数据, 2=正在写入, 3=正在读取
    private var data: Option<T>     // 存储的数据
    private let mutex: Mutex  // 保护data的访问

    // 状态常量
    public static let EMPTY: Int64 = 0
    public static let FILLED: Int64 = 1
    public static let WRITING: Int64 = 2
    public static let READING: Int64 = 3

    public init() {
        this.state = AtomicInt64(EMPTY)
        this.data = None
        this.mutex = Mutex()
    }

    /**
     * 尝试开始写入
     */
    public func tryStartWrite(): Bool {
        return state.compareAndSwap(EMPTY, WRITING)
    }

    /**
     * 完成写入
     */
    public func finishWrite(item: T): Unit {
        mutex.lock()
        data = Some(item)
        mutex.unlock()
        state.store(FILLED)
    }

    /**
     * 取消写入
     */
    public func cancelWrite(): Unit {
        state.store(EMPTY)
    }

    /**
     * 尝试开始读取
     */
    public func tryStartRead(): Bool {
        return state.compareAndSwap(FILLED, READING)
    }

    /**
     * 完成读取
     */
    public func finishRead(): Option<T> {
        mutex.lock()
        let result = data
        data = None
        mutex.unlock()
        state.store(EMPTY)
        return result
    }

    /**
     * 取消读取
     */
    public func cancelRead(): Unit {
        state.store(FILLED)
    }

    /**
     * 获取当前状态
     */
    public func getState(): Int64 {
        return state.load()
    }

    /**
     * 检查是否为空
     */
    public func isEmpty(): Bool {
        return state.load() == EMPTY
    }

    /**
     * 检查是否已填充
     */
    public func isFilled(): Bool {
        return state.load() == FILLED
    }
}

/**
 * 有界 MPMC 无锁队列
 *
 * 实现：Dmitry Vyukov 的 bounded MPMC 队列
 * - 每个槽位带一个序号（AtomicInt64），由序号决定槽位当前可写/可读
 * - 入队：CAS 推进 tail 获得槽位 → 写数据 → 发布序号 pos+1
 * - 出队：CAS 推进 head 获得槽位 → 读数据 → 发布序号 pos+capacity
 * - 只有真正“满”时 enqueue 才返回 false，只有真正“空”时 dequeue 才返回 None，
 *   不会因竞争而虚假失败
 * - 容量向上取整为 2 的幂
 */
public class LockFreeQueue<T> <: Queue<T> {
    private let slots: Array<Option<T>>
    private let sequences: Array<AtomicInt64>
    private let queueCapacity: Int64
    private let mask: Int64
    private let head: AtomicInt64  // 下一个出队位置
    private let tail: AtomicInt64  // 下一个入队位置

    private let enqueueCount: AtomicInt64
    private let dequeueCount: AtomicInt64
    private let failedEnqueues: AtomicInt64
    private let failedDequeues: AtomicInt64

    public init(requestedCapacity: Int64) {
        let actualCapacity = LockFreeQueue<T>.nextPowerOfTwo(requestedCapacity)
        this.queueCapacity = actualCapacity
        this.mask = actualCapacity - 1
        this.slots = Array<Option<T>>(actualCapacity, { _ => None<T> })
        this.sequences = Array<AtomicInt64>(actualCapacity, { i => AtomicInt64(i) })
        this.head = AtomicInt64(0)
        this.tail = AtomicInt64(0)
        this.enqueueCount = AtomicInt64(0)
        this.dequeueCount = AtomicInt64(0)
        this.failedEnqueues = AtomicInt64(0)
        this.failedDequeues = AtomicInt64(0)
    }

    /**
     * 入队；队列满时返回 false
     */
    public func enqueue(item: T): Bool {
        while (true) {
            let pos = tail.load()
            let idx = pos & mask
            let dif = sequences[idx].load() - pos
            if (dif == 0) {
                if (tail.compareAndSwap(pos, pos + 1)) {
                    slots[idx] = Some(item)
                    sequences[idx].store(pos + 1)
                    enqueueCount.fetchAdd(1)
                    return true
                }
            } else if (dif < 0) {
                failedEnqueues.fetchAdd(1)
                return false
            }
            // dif > 0：其他生产者已推进 tail，重新读取
        }
        return false
    }

    /**
     * 出队；队列空时返回 None
     */
    public func dequeue(): Option<T> {
        while (true) {
            let pos = head.load()
            let idx = pos & mask
            let dif = sequences[idx].load() - (pos + 1)
            if (dif == 0) {
                if (head.compareAndSwap(pos, pos + 1)) {
                    let item = slots[idx]
                    slots[idx] = None<T>
                    sequences[idx].store(pos + queueCapacity)
                    dequeueCount.fetchAdd(1)
                    return item
                }
            } else if (dif < 0) {
                failedDequeues.fetchAdd(1)
                return None<T>
            }
        }
        return None<T>
    }

    public func isEmpty(): Bool {
        return size() == 0
    }

    /**
     * 当前元素个数（并发下为近似值，范围 [0, capacity]）
     */
    public func size(): Int64 {
        let n = tail.load() - head.load()
        if (n < 0) { 0 } else if (n > queueCapacity) { queueCapacity } else { n }
    }

    public func capacity(): Int64 {
        return queueCapacity
    }

    /**
     * 清空队列（与并发入队同时调用时，只保证清掉调用时已存在的元素）
     */
    public func clear(): Unit {
        while (dequeue().isSome()) {}
    }

    /**
     * 批量入队，返回成功数量（遇到满即停止）
     */
    public func enqueueBatch(items: Array<T>): Int32 {
        var n: Int32 = 0
        for (item in items) {
            if (!enqueue(item)) {
                break
            }
            n++
        }
        return n
    }

    /**
     * 批量出队
     */
    public func dequeueBatch(maxCount: Int32): Array<T> {
        let results = ArrayList<T>()
        while (Int32(results.size) < maxCount) {
            match (dequeue()) {
                case Some(item) => results.add(item)
                case None => break
            }
        }
        return results.toArray()
    }

    /** 与 enqueue 相同（保留旧 API） */
    public func fastEnqueue(item: T): Bool {
        enqueue(item)
    }

    /** 与 dequeue 相同（保留旧 API） */
    public func fastDequeue(): Option<T> {
        dequeue()
    }

    public func getStats(): QueueStats {
        QueueStats(
            enqueueCount.load(),
            dequeueCount.load(),
            failedEnqueues.load(),
            failedDequeues.load(),
            size(),
            queueCapacity
        )
    }

    /**
     * 向上取整为 2 的幂，范围 [1, 2^30]
     */
    private static func nextPowerOfTwo(n: Int64): Int64 {
        var c: Int64 = 1
        while (c < n && c < (1 << 30)) {
            c = c << 1
        }
        return c
    }
}
