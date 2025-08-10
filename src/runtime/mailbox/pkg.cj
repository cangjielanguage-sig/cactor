/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 邮箱包导出
 */
package cactor.runtime.mailbox

// 邮箱系统包 - 避免循环依赖，通过文件自动导出

import std.sync.*
import cactor.core.message.{Envelope, Message}


/**
 * 批处理可选语义接口
 * 支持批量出队操作，提高消息处理吞吐量
 */
public interface BatchDequeueSemantics {
    /**
     * 批量出队消息
     * @param max 最大出队数量
     * @return 实际出队的消息数组，可能少于max
     */
    func dequeueBatch(max: Int32): Array<Envelope>
}

/**
 * 邮箱指标数据结构
 * 统一的邮箱性能指标定义
 */
public struct MailboxMetrics {
    public let totalEnqueued: Int64      // 总入队数量
    public let totalDequeued: Int64      // 总出队数量
    public let currentSize: Int64        // 当前队列大小
    public let maxSize: Int64           // 历史最大队列大小
    public let avgProcessingTime: Int64  // 平均处理时间（纳秒）
    public let dropCount: Int64         // 丢弃消息数量
    public let lastUpdateTime: Int64    // 最后更新时间戳

    public init(totalEnqueued: Int64, totalDequeued: Int64, currentSize: Int64,
                maxSize: Int64, avgProcessingTime: Int64, dropCount: Int64, lastUpdateTime: Int64) {
        this.totalEnqueued = totalEnqueued
        this.totalDequeued = totalDequeued
        this.currentSize = currentSize
        this.maxSize = maxSize
        this.avgProcessingTime = avgProcessingTime
        this.dropCount = dropCount
        this.lastUpdateTime = lastUpdateTime
    }
}

/**
 * 邮箱指标语义接口
 * 提供统一的指标收集和查询能力
 */
public interface MetricsSemantics {
    /**
     * 获取邮箱指标
     * @return 当前的邮箱指标数据
     */
    func getMetrics(): MailboxMetrics

    /**
     * 重置指标计数器
     */
    func resetMetrics(): Unit

    /**
     * 是否启用指标收集
     * @return true表示启用指标收集
     */
    func isMetricsEnabled(): Bool
}

/**
 * 有界邮箱语义接口
 * 提供容量限制和背压控制能力
 */
public interface BoundedMailboxSemantics {
    /**
     * 检查邮箱是否已满
     * @return true表示邮箱已满
     */
    func isFull(): Bool

    /**
     * 获取剩余容量
     * @return 剩余可用容量
     */
    func remainingCapacity(): Int64

    /**
     * 获取最大容量
     * @return 邮箱最大容量
     */
    func maxCapacity(): Int64
}

/**
 * 优先级邮箱语义接口
 * 提供消息优先级处理能力
 */
public interface PriorityMailboxSemantics {
    /**
     * 获取消息的优先级
     * @param message 消息对象
     * @return 优先级值，数值越大优先级越高
     */
    func priorityOf(message: Message): Int32
}

/**
 * 控制消息感知语义接口
 * 提供控制/系统消息优先处理能力
 */
public interface ControlAwareSemantics {
    /**
     * 检查消息是否为控制消息
     * @param message 消息对象
     * @return true表示是控制消息
     */
    func isControlMessage(message: Message): Bool

    /**
     * 获取控制消息队列大小
     * @return 控制消息队列中的消息数量
     */
    func controlQueueSize(): Int64
}

/**
 * 背压控制语义接口
 * 提供负载控制和流量管理能力
 */
public interface BackpressureSemantics {
    /**
     * 获取当前负载比率
     * @return 负载比率，0.0-1.0之间
     */
    func getLoadRatio(): Float64

    /**
     * 获取背压阈值
     * @return 触发背压的阈值
     */
    func getBackpressureThreshold(): Float64

    /**
     * 检查是否触发背压
     * @return true表示当前处于背压状态
     */
    func isBackpressureTriggered(): Bool

    /**
     * 获取被拒绝的消息数量
     * @return 因背压而被拒绝的消息总数
     */
    func getRejectedCount(): Int64
}

/**
 * 背压策略枚举
 * 定义当邮箱达到容量限制时的处理策略
 */
public enum BackpressureStrategy {
    | DropHead    // 丢弃队列头部的旧消息
    | DropTail    // 丢弃队列尾部的旧消息
    | DropNew     // 丢弃新消息
    | Block       // 阻塞直到有空间
    | Fail        // 抛出异常
}

/**
 * 有界邮箱包装器
 * 为任意邮箱添加容量限制和背压控制功能
 */
public class BoundedMailboxWrapper <: Mailbox & BoundedMailboxSemantics & BackpressureSemantics {
    private let innerMailbox: Mailbox
    private let capacity: Int64
    private let strategy: BackpressureStrategy
    private let currentSize: AtomicInt64
    private let rejectedCount: AtomicInt64
    private let backpressureThreshold: Float64

    public init(innerMailbox: Mailbox, capacity: Int64, strategy: BackpressureStrategy, backpressureThreshold: Float64) {
        this.innerMailbox = innerMailbox
        this.capacity = capacity
        this.strategy = strategy
        this.currentSize = AtomicInt64(0)
        this.rejectedCount = AtomicInt64(0)
        this.backpressureThreshold = backpressureThreshold
    }

    public func enqueue(envelope: Envelope): Bool {
        let currentCount = currentSize.load()

        // 检查是否超过容量限制
        if (currentCount >= capacity) {
            match (strategy) {
                case BackpressureStrategy.DropNew =>
                    rejectedCount.fetchAdd(1)
                    return false
                case BackpressureStrategy.DropHead =>
                    // 先出队一个旧消息，再入队新消息
                    innerMailbox.dequeue()
                    currentSize.fetchAdd(-1)
                case BackpressureStrategy.DropTail =>
                    // 简化实现：直接拒绝新消息
                    rejectedCount.fetchAdd(1)
                    return false
                case BackpressureStrategy.Block =>
                    // 简化实现：返回false表示需要重试
                    return false
                case BackpressureStrategy.Fail =>
                    rejectedCount.fetchAdd(1)
                    return false
            }
        }

        if (innerMailbox.enqueue(envelope)) {
            currentSize.fetchAdd(1)
            return true
        }
        return false
    }

    public func dequeue(): Option<Envelope> {
        match (innerMailbox.dequeue()) {
            case Some(envelope) =>
                currentSize.fetchAdd(-1)
                Some(envelope)
            case None => None
        }
    }

    public func isEmpty(): Bool {
        innerMailbox.isEmpty()
    }

    public func size(): Int64 {
        currentSize.load()
    }

    public func clear(): Unit {
        innerMailbox.clear()
        currentSize.store(0)
    }

    // BoundedMailboxSemantics 实现
    public func isFull(): Bool {
        currentSize.load() >= capacity
    }

    public func remainingCapacity(): Int64 {
        capacity - currentSize.load()
    }

    public func maxCapacity(): Int64 {
        capacity
    }

    // BackpressureSemantics 实现
    public func getLoadRatio(): Float64 {
        Float64(currentSize.load()) / Float64(capacity)
    }

    public func getBackpressureThreshold(): Float64 {
        backpressureThreshold
    }

    public func isBackpressureTriggered(): Bool {
        getLoadRatio() >= backpressureThreshold
    }

    public func getRejectedCount(): Int64 {
        rejectedCount.load()
    }
}

/**
 * 控制消息感知包装器
 * 为任意邮箱添加控制消息优先处理功能
 */
public class ControlAwareMailboxWrapper <: Mailbox & ControlAwareSemantics {
    private let innerMailbox: Mailbox
    private let controlQueue: SimpleMailbox
    private let controlCount: AtomicInt64

    public init(innerMailbox: Mailbox) {
        this.innerMailbox = innerMailbox
        this.controlQueue = SimpleMailbox(0)  // 无限容量的控制消息队列
        this.controlCount = AtomicInt64(0)
    }

    public func enqueue(envelope: Envelope): Bool {
        if (isControlMessage(envelope.getMessage())) {
            if (controlQueue.enqueue(envelope)) {
                controlCount.fetchAdd(1)
                return true
            }
            return false
        } else {
            return innerMailbox.enqueue(envelope)
        }
    }

    public func dequeue(): Option<Envelope> {
        // 优先处理控制消息
        match (controlQueue.dequeue()) {
            case Some(envelope) =>
                controlCount.fetchAdd(-1)
                Some(envelope)
            case None =>
                innerMailbox.dequeue()
        }
    }

    public func isEmpty(): Bool {
        controlQueue.isEmpty() && innerMailbox.isEmpty()
    }

    public func size(): Int64 {
        controlQueue.size() + innerMailbox.size()
    }

    public func clear(): Unit {
        controlQueue.clear()
        innerMailbox.clear()
        controlCount.store(0)
    }

    // ControlAwareSemantics 实现
    public func isControlMessage(message: Message): Bool {
        ControlClassifier.isControl(message)
    }

    public func controlQueueSize(): Int64 {
        controlCount.load()
    }
}

/**
 * 优先级邮箱包装器
 * 为任意邮箱添加消息优先级处理功能
 */
public class PriorityMailboxWrapper <: Mailbox & PriorityMailboxSemantics {
    private let highPriorityQueue: SimpleMailbox
    private let normalPriorityQueue: SimpleMailbox
    private let lowPriorityQueue: SimpleMailbox

    public init() {
        this.highPriorityQueue = SimpleMailbox(0)
        this.normalPriorityQueue = SimpleMailbox(0)
        this.lowPriorityQueue = SimpleMailbox(0)
    }

    public func enqueue(envelope: Envelope): Bool {
        let priority = priorityOf(envelope.getMessage())

        if (priority > 0) {
            return highPriorityQueue.enqueue(envelope)
        } else if (priority < 0) {
            return lowPriorityQueue.enqueue(envelope)
        } else {
            return normalPriorityQueue.enqueue(envelope)
        }
    }

    public func dequeue(): Option<Envelope> {
        // 按优先级顺序处理：高 -> 普通 -> 低
        match (highPriorityQueue.dequeue()) {
            case Some(envelope) => Some(envelope)
            case None =>
                match (normalPriorityQueue.dequeue()) {
                    case Some(envelope) => Some(envelope)
                    case None => lowPriorityQueue.dequeue()
                }
        }
    }

    public func isEmpty(): Bool {
        highPriorityQueue.isEmpty() && normalPriorityQueue.isEmpty() && lowPriorityQueue.isEmpty()
    }

    public func size(): Int64 {
        highPriorityQueue.size() + normalPriorityQueue.size() + lowPriorityQueue.size()
    }

    public func clear(): Unit {
        highPriorityQueue.clear()
        normalPriorityQueue.clear()
        lowPriorityQueue.clear()
    }

    // PriorityMailboxSemantics 实现
    public func priorityOf(message: Message): Int32 {
        if (ControlClassifier.isControl(message)) {
            return 100  // 控制消息最高优先级
        } else if (message.messageType().contains("Low")) {
            return -10  // 低优先级消息
        } else {
            return 0    // 普通优先级消息
        }
    }
}
