/*
 * CActor Runtime - 智能背压邮箱
 * 实现动态背压控制和智能流量管理
 */
package cactor.runtime.mailbox

import std.sync.*
import std.time.*
import std.collection.*
import cactor.core.message.*

/**
 * 背压控制级别
 */
public enum BackpressureLevel <: ToString {
    | NoBackpressure  // 无背压
    | Low             // 低级背压
    | Medium          // 中级背压
    | High            // 高级背压
    | Critical        // 临界背压

    public func toString(): String {
        match (this) {
            case NoBackpressure => "NoBackpressure"
            case Low => "Low"
            case Medium => "Medium"
            case High => "High"
            case Critical => "Critical"
        }
    }
}

/**
 * 背压控制策略
 */
public struct BackpressureControlStrategy {
    public let lowThreshold: Float64      // 低级背压阈值 (0.6)
    public let mediumThreshold: Float64   // 中级背压阈值 (0.75)
    public let highThreshold: Float64     // 高级背压阈值 (0.9)
    public let criticalThreshold: Float64 // 临界背压阈值 (0.95)
    public let recoveryFactor: Float64    // 恢复因子 (0.8)

    public init(lowThreshold: Float64, mediumThreshold: Float64, highThreshold: Float64, criticalThreshold: Float64, recoveryFactor: Float64) {
        this.lowThreshold = lowThreshold
        this.mediumThreshold = mediumThreshold
        this.highThreshold = highThreshold
        this.criticalThreshold = criticalThreshold
        this.recoveryFactor = recoveryFactor
    }

    public static func createDefault(): BackpressureControlStrategy {
        BackpressureControlStrategy(0.6, 0.75, 0.9, 0.95, 0.8)
    }

    public static func createAggressive(): BackpressureControlStrategy {
        BackpressureControlStrategy(0.5, 0.65, 0.8, 0.9, 0.7)
    }

    public static func createConservative(): BackpressureControlStrategy {
        BackpressureControlStrategy(0.7, 0.8, 0.9, 0.95, 0.85)
    }
}

/**
 * 智能背压邮箱
 * 根据负载情况动态调整背压策略
 */
public class IntelligentBackpressureMailbox <: Mailbox & BackpressureSemantics {
    private let innerMailbox: Mailbox
    private let capacity: Int64
    private let strategy: BackpressureControlStrategy
    private let currentSize: AtomicInt64
    private let rejectedCount: AtomicInt64
    private let acceptedCount: AtomicInt64
    private let currentLevel: AtomicInt32  // BackpressureLevel as Int32
    private let lastLevelChange: AtomicInt64
    private let controlMutex: Mutex

    // 动态统计
    private let recentRejections: AtomicInt64
    private let recentAcceptances: AtomicInt64
    private let lastStatsReset: AtomicInt64

    public init(innerMailbox: Mailbox, capacity: Int64, strategy: BackpressureControlStrategy) {
        this.innerMailbox = innerMailbox
        this.capacity = capacity
        this.strategy = strategy
        this.currentSize = AtomicInt64(0)
        this.rejectedCount = AtomicInt64(0)
        this.acceptedCount = AtomicInt64(0)
        this.currentLevel = AtomicInt32(0)  // BackpressureLevel.None
        this.controlMutex = Mutex()
        this.recentRejections = AtomicInt64(0)
        this.recentAcceptances = AtomicInt64(0)

        // 初始化时间相关字段
        let now = DateTime.now().toUnixTimeStamp().toMilliseconds() * 1_000_000
        this.lastLevelChange = AtomicInt64(now)
        this.lastStatsReset = AtomicInt64(now)
    }

    public func enqueue(envelope: Envelope): Bool {
        let currentLoad = getLoadRatio()
        let level = determineBackpressureLevel(currentLoad)
        
        // 更新背压级别
        updateBackpressureLevel(level)
        
        // 根据背压级别决定是否接受消息
        if (shouldRejectMessage(level, currentLoad)) {
            recentRejections.fetchAdd(1)
            rejectedCount.fetchAdd(1)
            return false
        }

        // 尝试入队
        if (innerMailbox.enqueue(envelope)) {
            currentSize.fetchAdd(1)
            acceptedCount.fetchAdd(1)
            recentAcceptances.fetchAdd(1)
            return true
        }
        
        return false
    }

    public func dequeue(): Option<Envelope> {
        match (innerMailbox.dequeue()) {
            case Some(envelope) =>
                currentSize.fetchAdd(-1)
                
                // 检查是否需要重置统计
                resetStatsIfNeeded()
                
                Some(envelope)
            case None => None
        }
    }

    private func determineBackpressureLevel(loadRatio: Float64): BackpressureLevel {
        if (loadRatio >= strategy.criticalThreshold) {
            BackpressureLevel.Critical
        } else if (loadRatio >= strategy.highThreshold) {
            BackpressureLevel.High
        } else if (loadRatio >= strategy.mediumThreshold) {
            BackpressureLevel.Medium
        } else if (loadRatio >= strategy.lowThreshold) {
            BackpressureLevel.Low
        } else {
            BackpressureLevel.NoBackpressure
        }
    }

    private func shouldRejectMessage(level: BackpressureLevel, loadRatio: Float64): Bool {
        match (level) {
            case BackpressureLevel.NoBackpressure => false
            case BackpressureLevel.Low => 
                // 10% 概率拒绝
                (getCurrentTimeNanos() % 10) == 0
            case BackpressureLevel.Medium => 
                // 25% 概率拒绝
                (getCurrentTimeNanos() % 4) == 0
            case BackpressureLevel.High => 
                // 50% 概率拒绝
                (getCurrentTimeNanos() % 2) == 0
            case BackpressureLevel.Critical => 
                // 90% 概率拒绝
                (getCurrentTimeNanos() % 10) != 0
        }
    }

    private func updateBackpressureLevel(newLevel: BackpressureLevel): Unit {
        let newLevelInt = backpressureLevelToInt(newLevel)
        let oldLevelInt = currentLevel.load()
        
        if (newLevelInt != oldLevelInt) {
            currentLevel.store(newLevelInt)
            lastLevelChange.store(getCurrentTimeNanos())
            
            let oldLevel = intToBackpressureLevel(oldLevelInt)
            println("[IntelligentBackpressureMailbox] 背压级别变更: ${oldLevel.toString()} -> ${newLevel.toString()}")
        }
    }

    private func backpressureLevelToInt(level: BackpressureLevel): Int32 {
        match (level) {
            case BackpressureLevel.NoBackpressure => 0
            case BackpressureLevel.Low => 1
            case BackpressureLevel.Medium => 2
            case BackpressureLevel.High => 3
            case BackpressureLevel.Critical => 4
        }
    }

    private func intToBackpressureLevel(levelInt: Int32): BackpressureLevel {
        match (levelInt) {
            case 0 => BackpressureLevel.NoBackpressure
            case 1 => BackpressureLevel.Low
            case 2 => BackpressureLevel.Medium
            case 3 => BackpressureLevel.High
            case 4 => BackpressureLevel.Critical
            case _ => BackpressureLevel.NoBackpressure
        }
    }

    private func resetStatsIfNeeded(): Unit {
        let now = getCurrentTimeNanos()
        let lastReset = lastStatsReset.load()
        
        // 每10秒重置一次统计
        if (now - lastReset > 10_000_000_000) {
            recentRejections.store(0)
            recentAcceptances.store(0)
            lastStatsReset.store(now)
        }
    }

    private func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds() * 1_000_000
    }

    // Mailbox接口实现
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

    // 注意：基础Mailbox接口可能不包含isClosed和close方法
    // 这里提供默认实现
    public func isClosed(): Bool {
        false  // 默认实现
    }

    public func close(): Unit {
        // 默认实现：什么都不做
    }

    // BackpressureSemantics接口实现
    public func getLoadRatio(): Float64 {
        if (capacity == 0) { return 0.0 }
        Float64(currentSize.load()) / Float64(capacity)
    }

    public func getBackpressureThreshold(): Float64 {
        strategy.lowThreshold
    }

    public func isBackpressureTriggered(): Bool {
        getLoadRatio() > strategy.lowThreshold
    }

    public func getRejectedCount(): Int64 {
        rejectedCount.load()
    }

    /**
     * 获取当前背压级别
     */
    public func getCurrentBackpressureLevel(): BackpressureLevel {
        intToBackpressureLevel(currentLevel.load())
    }

    /**
     * 获取背压统计信息
     */
    public func getBackpressureStats(): BackpressureStats {
        BackpressureStats(
            getCurrentBackpressureLevel(),
            getLoadRatio(),
            acceptedCount.load(),
            rejectedCount.load(),
            recentAcceptances.load(),
            recentRejections.load(),
            capacity,
            currentSize.load()
        )
    }
}

/**
 * 背压统计信息
 */
public struct BackpressureStats {
    public let currentLevel: BackpressureLevel
    public let loadRatio: Float64
    public let totalAccepted: Int64
    public let totalRejected: Int64
    public let recentAccepted: Int64
    public let recentRejected: Int64
    public let capacity: Int64
    public let currentSize: Int64

    public init(currentLevel: BackpressureLevel, loadRatio: Float64, totalAccepted: Int64, totalRejected: Int64,
                recentAccepted: Int64, recentRejected: Int64, capacity: Int64, currentSize: Int64) {
        this.currentLevel = currentLevel
        this.loadRatio = loadRatio
        this.totalAccepted = totalAccepted
        this.totalRejected = totalRejected
        this.recentAccepted = recentAccepted
        this.recentRejected = recentRejected
        this.capacity = capacity
        this.currentSize = currentSize
    }

    public func getRejectionRate(): Float64 {
        let total = totalAccepted + totalRejected
        if (total == 0) { return 0.0 }
        Float64(totalRejected) / Float64(total)
    }

    public func getRecentRejectionRate(): Float64 {
        let recentTotal = recentAccepted + recentRejected
        if (recentTotal == 0) { return 0.0 }
        Float64(recentRejected) / Float64(recentTotal)
    }

    public func getDescription(): String {
        "BackpressureStats(level=${currentLevel.toString()}, load=${loadRatio}, rejected=${totalRejected}/${totalAccepted}, recent=${recentRejected}/${recentAccepted})"
    }
}
