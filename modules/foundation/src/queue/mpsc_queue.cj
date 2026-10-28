/*
 * MPSC (Multi Producer Single Consumer) 无锁队列
 *
 * 实现：Dmitry Vyukov 的侵入式 MPSC 链表队列（Akka 邮箱同款）
 * - 生产者：一次原子 swap 把新节点挂到 head，再链接 prev.next —— 无等待（wait-free）
 * - 消费者：沿 tail.next 前进；生产者处于 swap 与链接之间时短暂自旋等待
 * - 无界（受 maxCapacity 保护），天然 FIFO，不存在扩容竞态
 * - 消费端用 Mutex 串行化，多个消费者误用时也保持正确
 */
package cactor_foundation.queue

import std.sync.*
import std.collection.*

/** 链表节点 */
class MPSCNode<T> {
    var value: Option<T>
    let next: AtomicOptionReference<MPSCNode<T>>

    init(value: Option<T>) {
        this.value = value
        this.next = AtomicOptionReference<MPSCNode<T>>()
    }
}

/**
 * MPSC 无锁队列
 */
public class MPSCQueue<T> {
    // 生产者端：最新节点
    private let head: AtomicReference<MPSCNode<T>>
    // 消费者端：已消费的哨兵节点
    private var tail: MPSCNode<T>
    private let consumerLock: Mutex

    private let initialCapacity: Int64
    private let maxCapacity: Int64 = 16777216  // 16M，防止无限增长
    private let enqueueCount: AtomicInt64 = AtomicInt64(0)
    private let dequeueCount: AtomicInt64 = AtomicInt64(0)
    private let expansionCount: AtomicInt64 = AtomicInt64(0)

    public init(capacity: Int32) {
        let stub = MPSCNode<T>(None<T>)
        this.head = AtomicReference<MPSCNode<T>>(stub)
        this.tail = stub
        this.consumerLock = Mutex()
        this.initialCapacity = if (capacity > 0) { Int64(capacity) } else { 1 }
    }

    /**
     * 多生产者入队（wait-free）
     * @return 超过 maxCapacity 时返回 false
     */
    public func offer(item: T): Bool {
        if (size() >= maxCapacity) {
            return false
        }
        let node = MPSCNode<T>(Some(item))
        let prev = head.swap(node)
        prev.next.store(node)
        enqueueCount.fetchAdd(1)
        return true
    }

    /**
     * 出队；队列为空时返回 None
     */
    public func poll(): Option<T> {
        consumerLock.lock()
        try {
            return pollLocked()
        } finally {
            consumerLock.unlock()
        }
    }

    private func pollLocked(): Option<T> {
        var spins = 0
        while (true) {
            match (tail.next.load()) {
                case Some(next) =>
                    tail = next
                    let v = next.value
                    next.value = None<T>
                    dequeueCount.fetchAdd(1)
                    return v
                case None =>
                    // head 与 tail 相同才是真正为空；否则有生产者正在链接节点
                    if (refEq(head.load(), tail)) {
                        return None<T>
                    }
                    spins++
                    if (spins > 64) {
                        sleep(Duration.Zero)
                        spins = 0
                    }
            }
        }
        return None<T>
    }

    /**
     * 批量入队，返回实际入队数量
     */
    public func offerBatch(items: Array<T>): Int32 {
        var n: Int32 = 0
        for (item in items) {
            if (!offer(item)) {
                break
            }
            n++
        }
        return n
    }

    /**
     * 批量出队
     */
    public func pollBatch(maxCount: Int32): Array<T> {
        let results = ArrayList<T>()
        consumerLock.lock()
        try {
            while (Int32(results.size) < maxCount) {
                match (pollLocked()) {
                    case Some(item) => results.add(item)
                    case None => break
                }
            }
        } finally {
            consumerLock.unlock()
        }
        return results.toArray()
    }

    /**
     * 当前元素个数（并发下为近似值）
     */
    public func size(): Int64 {
        let n = enqueueCount.load() - dequeueCount.load()
        if (n < 0) { 0 } else { n }
    }

    public func isEmpty(): Bool {
        return size() == 0
    }

    /**
     * 容量上限（链表实现无需扩容）
     */
    public func getCapacity(): Int64 {
        return maxCapacity
    }

    public func getPerformanceStats(): MPSCPerformanceStats {
        MPSCPerformanceStats(
            enqueueCount.load(),
            dequeueCount.load(),
            expansionCount.load(),
            getCapacity(),
            size()
        )
    }
}

/**
 * MPSC队列性能统计
 */
public struct MPSCPerformanceStats {
    public let totalEnqueues: Int64
    public let totalDequeues: Int64
    public let expansionCount: Int64
    public let currentCapacity: Int64
    public let currentSize: Int64
    
    public init(totalEnqueues: Int64, totalDequeues: Int64, expansionCount: Int64, 
               currentCapacity: Int64, currentSize: Int64) {
        this.totalEnqueues = totalEnqueues
        this.totalDequeues = totalDequeues
        this.expansionCount = expansionCount
        this.currentCapacity = currentCapacity
        this.currentSize = currentSize
    }
    
    public func getThroughput(durationSeconds: Float64): Float64 {
        return Float64(totalEnqueues + totalDequeues) / durationSeconds
    }
    
    public func getUtilization(): Float64 {
        return if (currentCapacity > 0) { Float64(currentSize) / Float64(currentCapacity) } else { 0.0 }
    }
    
    public func getExpansionRate(): Float64 {
        return Float64(currentCapacity) / Float64(1024)  // 相对于初始1K容量的扩展倍数
    }
}
