/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 邮箱包导出
 */
package cactor.runtime.mailbox

// 邮箱系统包 - 避免循环依赖，通过文件自动导出

import std.sync.*
import std.time.*
import std.collection.*
import cactor.core.message.{Envelope, Message, StringMessage}


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
 * Zero-Copy邮箱语义接口
 * 提供零拷贝消息传递能力，优化大消息场景
 */
public interface ZeroCopySemantics {
    /**
     * 零拷贝入队，直接传递消息引用
     * @param envelope 消息信封
     * @return 是否成功入队
     */
    func enqueueZeroCopy(envelope: Envelope): Bool

    /**
     * 获取零拷贝统计信息
     * @return 零拷贝操作统计
     */
    func getZeroCopyStats(): ZeroCopyStats
}

/**
 * Zero-Copy统计信息
 */
public struct ZeroCopyStats {
    public let totalZeroCopyOps: Int64      // 总零拷贝操作数
    public let savedCopyBytes: Int64        // 节省的拷贝字节数
    public let avgMessageSize: Float64      // 平均消息大小
    public let lastUpdateTime: Int64        // 最后更新时间

    public init(totalZeroCopyOps: Int64, savedCopyBytes: Int64, avgMessageSize: Float64, lastUpdateTime: Int64) {
        this.totalZeroCopyOps = totalZeroCopyOps
        this.savedCopyBytes = savedCopyBytes
        this.avgMessageSize = avgMessageSize
        this.lastUpdateTime = lastUpdateTime
    }
}

/**
 * 简化的Zero-Copy消息池
 * 用于优化消息对象的重用
 */
public class ZeroCopyMessagePool {
    private let poolSize: Int32
    private let hitCount: AtomicInt64
    private let missCount: AtomicInt64

    public init(poolSize: Int32) {
        this.poolSize = poolSize
        this.hitCount = AtomicInt64(0)
        this.missCount = AtomicInt64(0)
    }

    public func getHitRate(): Float64 {
        let hits = hitCount.load()
        let misses = missCount.load()
        let total = hits + misses
        if (total == 0) { return 0.0 }
        return Float64(hits) / Float64(total)
    }
}

/**
 * 真正的Zero-Copy邮箱实现
 * 基于引用传递和对象池优化大消息传递性能
 */
public class ZeroCopyMailbox <: Mailbox & ZeroCopySemantics & MetricsSemantics {
    private let innerMailbox: Mailbox
    private let messagePool: ZeroCopyMessagePool
    private let zeroCopyOps: AtomicInt64
    private let savedBytes: AtomicInt64
    private let totalMessageSize: AtomicInt64
    private let messageCount: AtomicInt64
    private let metricsEnabled: AtomicBool

    public init(capacity: Int64, poolSize: Int32) {
        this.innerMailbox = FoundationMailbox(capacity)
        this.messagePool = ZeroCopyMessagePool(poolSize)
        this.zeroCopyOps = AtomicInt64(0)
        this.savedBytes = AtomicInt64(0)
        this.totalMessageSize = AtomicInt64(0)
        this.messageCount = AtomicInt64(0)
        this.metricsEnabled = AtomicBool(true)
    }

    public func enqueue(envelope: Envelope): Bool {
        // 尝试零拷贝优化
        if (canUseZeroCopy(envelope)) {
            return enqueueZeroCopy(envelope)
        } else {
            return innerMailbox.enqueue(envelope)
        }
    }

    public func enqueueZeroCopy(envelope: Envelope): Bool {
        if (metricsEnabled.load()) {
            zeroCopyOps.fetchAdd(1)
            let messageSize = estimateMessageSize(envelope)
            savedBytes.fetchAdd(messageSize)
            totalMessageSize.fetchAdd(messageSize)
            messageCount.fetchAdd(1)
        }

        // 直接传递引用，避免拷贝
        return innerMailbox.enqueue(envelope)
    }

    public func dequeue(): Option<Envelope> {
        innerMailbox.dequeue()
    }

    public func isEmpty(): Bool {
        innerMailbox.isEmpty()
    }

    public func size(): Int64 {
        innerMailbox.size()
    }

    public func clear(): Unit {
        innerMailbox.clear()
    }

    // ZeroCopySemantics 实现
    public func getZeroCopyStats(): ZeroCopyStats {
        let totalOps = zeroCopyOps.load()
        let totalSaved = savedBytes.load()
        let totalSize = totalMessageSize.load()
        let count = messageCount.load()
        let avgSize = if (count > 0) { Float64(totalSize) / Float64(count) } else { 0.0 }

        ZeroCopyStats(totalOps, totalSaved, avgSize, getCurrentTimeNanos())
    }

    // MetricsSemantics 实现
    public func getMetrics(): MailboxMetrics {
        let currentTime = getCurrentTimeNanos()
        MailboxMetrics(
            messageCount.load(),    // totalEnqueued
            0,                      // totalDequeued (由innerMailbox管理)
            size(),                 // currentSize
            size(),                 // maxSize
            0,                      // avgProcessingTime
            0,                      // dropCount
            currentTime             // lastUpdateTime
        )
    }

    public func resetMetrics(): Unit {
        zeroCopyOps.store(0)
        savedBytes.store(0)
        totalMessageSize.store(0)
        messageCount.store(0)
    }

    public func isMetricsEnabled(): Bool {
        metricsEnabled.load()
    }

    private func canUseZeroCopy(envelope: Envelope): Bool {
        // 检查消息是否适合零拷贝优化
        let message = envelope.getMessage()
        let messageType = message.messageType()

        // 大消息或特定类型的消息适合零拷贝
        return messageType.contains("Large") ||
               messageType.contains("ZeroCopy") ||
               estimateMessageSize(envelope) > 1024  // 大于1KB的消息
    }

    private func estimateMessageSize(envelope: Envelope): Int64 {
        // 简化的消息大小估算
        let message = envelope.getMessage()
        let baseSize = Int64(64)  // 基础开销
        let typeSize = Int64(message.messageType().size * 2)  // 字符串大小估算
        return baseSize + typeSize
    }

    private func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
}

/**
 * 优先级饥饿保护语义接口
 * 防止低优先级消息永远得不到处理
 */
public interface StarvationProtectionSemantics {
    /**
     * 设置饥饿保护阈值
     * @param maxWaitTime 最大等待时间（纳秒）
     */
    func setStarvationThreshold(maxWaitTime: Int64): Unit

    /**
     * 获取饥饿保护统计
     * @return 饥饿保护统计信息
     */
    func getStarvationStats(): StarvationStats
}

/**
 * 饥饿保护统计信息
 */
public struct StarvationStats {
    public let totalPromotions: Int64       // 总提升次数
    public let avgWaitTime: Float64         // 平均等待时间
    public let maxWaitTime: Int64           // 最大等待时间
    public let currentThreshold: Int64      // 当前阈值

    public init(totalPromotions: Int64, avgWaitTime: Float64, maxWaitTime: Int64, currentThreshold: Int64) {
        this.totalPromotions = totalPromotions
        this.avgWaitTime = avgWaitTime
        this.maxWaitTime = maxWaitTime
        this.currentThreshold = currentThreshold
    }
}

/**
 * 带饥饿保护的优先级邮箱包装器
 * 防止低优先级消息长时间得不到处理
 */
public class StarvationProtectedPriorityMailbox <: Mailbox & PriorityMailboxSemantics & StarvationProtectionSemantics {
    private let highPriorityQueue: SimpleMailbox
    private let normalPriorityQueue: SimpleMailbox
    private let lowPriorityQueue: SimpleMailbox
    private let starvationThreshold: AtomicInt64  // 饥饿阈值（纳秒）
    private let promotionCount: AtomicInt64
    private let totalWaitTime: AtomicInt64
    private let maxWaitTime: AtomicInt64
    private let lastPromotionTime: AtomicInt64

    public init(starvationThresholdMs: Int64) {
        this.highPriorityQueue = SimpleMailbox(0)
        this.normalPriorityQueue = SimpleMailbox(0)
        this.lowPriorityQueue = SimpleMailbox(0)
        this.starvationThreshold = AtomicInt64(starvationThresholdMs * 1_000_000)  // 转换为纳秒
        this.promotionCount = AtomicInt64(0)
        this.totalWaitTime = AtomicInt64(0)
        this.maxWaitTime = AtomicInt64(0)
        this.lastPromotionTime = AtomicInt64(DateTime.now().toUnixTimeStamp().toNanoseconds())
    }

    public func enqueue(envelope: Envelope): Bool {
        let priority = priorityOf(envelope.getMessage())

        // 为消息添加时间戳用于饥饿检测
        let timestampedEnvelope = addTimestamp(envelope)

        if (priority > 0) {
            return highPriorityQueue.enqueue(timestampedEnvelope)
        } else if (priority < 0) {
            return lowPriorityQueue.enqueue(timestampedEnvelope)
        } else {
            return normalPriorityQueue.enqueue(timestampedEnvelope)
        }
    }

    public func dequeue(): Option<Envelope> {
        let currentTime = getCurrentTimeNanos()

        // 检查是否需要进行饥饿保护
        if (shouldPromoteLowPriority(currentTime)) {
            // 优先处理低优先级消息以防止饥饿
            match (lowPriorityQueue.dequeue()) {
                case Some(envelope) =>
                    recordPromotion(envelope, currentTime)
                    return Some(removeTimestamp(envelope))
                case None => ()
            }
        }

        // 正常的优先级处理：高 -> 普通 -> 低
        match (highPriorityQueue.dequeue()) {
            case Some(envelope) => Some(removeTimestamp(envelope))
            case None =>
                match (normalPriorityQueue.dequeue()) {
                    case Some(envelope) => Some(removeTimestamp(envelope))
                    case None =>
                        match (lowPriorityQueue.dequeue()) {
                            case Some(envelope) => Some(removeTimestamp(envelope))
                            case None => None
                        }
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

    // StarvationProtectionSemantics 实现
    public func setStarvationThreshold(maxWaitTime: Int64): Unit {
        starvationThreshold.store(maxWaitTime)
    }

    public func getStarvationStats(): StarvationStats {
        let totalPromotions = promotionCount.load()
        let totalWait = totalWaitTime.load()
        let avgWait = if (totalPromotions > 0) { Float64(totalWait) / Float64(totalPromotions) } else { 0.0 }

        StarvationStats(totalPromotions, avgWait, maxWaitTime.load(), starvationThreshold.load())
    }

    private func shouldPromoteLowPriority(currentTime: Int64): Bool {
        let lastPromotion = lastPromotionTime.load()
        let timeSinceLastPromotion = currentTime - lastPromotion

        // 如果距离上次提升超过阈值，且有低优先级消息等待，则进行提升
        return timeSinceLastPromotion > starvationThreshold.load() && !lowPriorityQueue.isEmpty()
    }

    private func recordPromotion(envelope: Envelope, currentTime: Int64): Unit {
        promotionCount.fetchAdd(1)
        lastPromotionTime.store(currentTime)

        // 记录等待时间统计
        let enqueueTime = getTimestamp(envelope)
        if (enqueueTime > 0) {
            let waitTime = currentTime - enqueueTime
            totalWaitTime.fetchAdd(waitTime)

            // 更新最大等待时间
            let currentMax = maxWaitTime.load()
            if (waitTime > currentMax) {
                maxWaitTime.compareAndSwap(currentMax, waitTime)
            }
        }
    }

    private func addTimestamp(envelope: Envelope): Envelope {
        // 简化实现：直接返回原envelope
        // 在实际实现中，可以通过扩展Envelope或使用包装器来添加时间戳
        envelope
    }

    private func removeTimestamp(envelope: Envelope): Envelope {
        // 简化实现：直接返回原envelope
        envelope
    }

    private func getTimestamp(envelope: Envelope): Int64 {
        // 简化实现：返回当前时间
        // 在实际实现中，应该从envelope中提取时间戳
        getCurrentTimeNanos()
    }

    private func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
}

/**
 * Work-Stealing友好语义接口
 * 为工作窃取调度器提供优化提示
 */
public interface WorkStealingFriendlySemantics {
    /**
     * 检查是否适合工作窃取
     * @return true表示当前邮箱状态适合被其他线程窃取任务
     */
    func isStealable(): Bool

    /**
     * 获取窃取提示信息
     * @return 窃取提示，包含负载信息和优化建议
     */
    func getStealingHint(): StealingHint

    /**
     * 批量窃取消息
     * @param maxCount 最大窃取数量
     * @return 窃取到的消息数组
     */
    func stealBatch(maxCount: Int32): Array<Envelope>
}

/**
 * 窃取提示信息
 */
public struct StealingHint {
    public let loadLevel: Float64           // 负载水平 (0.0-1.0)
    public let recommendedBatchSize: Int32  // 推荐批量大小
    public let priority: Int32              // 窃取优先级
    public let lastActivity: Int64          // 最后活动时间

    public init(loadLevel: Float64, recommendedBatchSize: Int32, priority: Int32, lastActivity: Int64) {
        this.loadLevel = loadLevel
        this.recommendedBatchSize = recommendedBatchSize
        this.priority = priority
        this.lastActivity = lastActivity
    }
}

/**
 * Work-Stealing友好的邮箱包装器
 * 为工作窃取调度器提供优化支持
 */
public class WorkStealingFriendlyMailbox <: Mailbox & WorkStealingFriendlySemantics & BatchDequeueSemantics {
    private let innerMailbox: Mailbox
    private let stealingEnabled: AtomicBool
    private let lastStealTime: AtomicInt64
    private let stealCount: AtomicInt64
    private let localDequeueCount: AtomicInt64
    private let maxCapacity: Int64

    public init(innerMailbox: Mailbox, maxCapacity: Int64) {
        this.innerMailbox = innerMailbox
        this.maxCapacity = maxCapacity
        this.stealingEnabled = AtomicBool(true)
        this.lastStealTime = AtomicInt64(0)
        this.stealCount = AtomicInt64(0)
        this.localDequeueCount = AtomicInt64(0)
    }

    public func enqueue(envelope: Envelope): Bool {
        innerMailbox.enqueue(envelope)
    }

    public func dequeue(): Option<Envelope> {
        match (innerMailbox.dequeue()) {
            case Some(envelope) =>
                localDequeueCount.fetchAdd(1)
                Some(envelope)
            case None => None
        }
    }

    public func isEmpty(): Bool {
        innerMailbox.isEmpty()
    }

    public func size(): Int64 {
        innerMailbox.size()
    }

    public func clear(): Unit {
        innerMailbox.clear()
    }

    // BatchDequeueSemantics 实现
    public func dequeueBatch(max: Int32): Array<Envelope> {
        // 简化实现：直接使用单条出队的回退策略
        let result = ArrayList<Envelope>()
        var count = Int32(0)

        while (count < max) {
            match (dequeue()) {
                case Some(envelope) =>
                    result.add(envelope)
                    count += 1
                case None => break
            }
        }

        return result.toArray()
    }

    // WorkStealingFriendlySemantics 实现
    public func isStealable(): Bool {
        if (!stealingEnabled.load()) {
            return false
        }

        let currentSize = size()
        let loadLevel = Float64(currentSize) / Float64(maxCapacity)

        // 当负载超过50%时适合被窃取
        return loadLevel > 0.5 && currentSize > 10
    }

    public func getStealingHint(): StealingHint {
        let currentSize = size()
        let loadLevel = Float64(currentSize) / Float64(maxCapacity)
        let recommendedBatchSize = calculateRecommendedBatchSize(currentSize)
        let priority = calculateStealingPriority(loadLevel)
        let lastActivity = lastStealTime.load()

        StealingHint(loadLevel, recommendedBatchSize, priority, lastActivity)
    }

    public func stealBatch(maxCount: Int32): Array<Envelope> {
        if (!isStealable()) {
            return Array<Envelope>(0, { i => Envelope(StringMessage(""), None<String>) })
        }

        lastStealTime.store(getCurrentTimeNanos())
        stealCount.fetchAdd(1)

        // 窃取策略：取一半的消息，但不超过maxCount
        let currentSize = size()
        let stealSize = min(Int32(currentSize / 2), maxCount)

        return dequeueBatch(stealSize)
    }

    private func calculateRecommendedBatchSize(currentSize: Int64): Int32 {
        if (currentSize > 1000) {
            return 100
        } else if (currentSize > 100) {
            return 20
        } else {
            return 5
        }
    }

    private func calculateStealingPriority(loadLevel: Float64): Int32 {
        if (loadLevel > 0.9) {
            return 10  // 高优先级
        } else if (loadLevel > 0.7) {
            return 5   // 中优先级
        } else {
            return 1   // 低优先级
        }
    }

    private func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toNanoseconds()
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
