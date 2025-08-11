/*
 * Copyright (c) 2024. All rights reserved.
 * Foundation Layer - Phi Accrual故障检测器
 * 基于Phi Accrual算法的自适应故障检测器，参考Akka实现
 */
package cactor.foundation.network

import std.collection.*
import std.sync.*
import std.time.*
import std.math.*

/**
 * 心跳记录
 */
public struct HeartbeatRecord {
    public let timestamp: Int64
    public let intervalMs: Int64

    public init(timestamp: Int64, intervalMs: Int64) {
        this.timestamp = timestamp
        this.intervalMs = intervalMs
    }
}

/**
 * Phi Accrual故障检测器统计信息
 */
public struct PhiAccrualStatistics {
    public let nodeId: String
    public let heartbeatCount: Int64
    public let lastHeartbeatTime: Int64
    public let currentPhi: Float64
    public let threshold: Float64
    public let isAvailable: Bool
    public let meanInterval: Float64
    public let variance: Float64

    public init(nodeId: String, heartbeatCount: Int64, lastHeartbeatTime: Int64, 
                currentPhi: Float64, threshold: Float64, isAvailable: Bool,
                meanInterval: Float64, variance: Float64) {
        this.nodeId = nodeId
        this.heartbeatCount = heartbeatCount
        this.lastHeartbeatTime = lastHeartbeatTime
        this.currentPhi = currentPhi
        this.threshold = threshold
        this.isAvailable = isAvailable
        this.meanInterval = meanInterval
        this.variance = variance
    }
}

/**
 * Phi Accrual故障检测器配置
 */
public struct PhiAccrualConfig {
    public let threshold: Float64           // Phi阈值，默认8.0
    public let maxSampleSize: Int32         // 最大样本数量，默认200
    public let minStdDeviation: Float64     // 最小标准差，默认100ms
    public let acceptableHeartbeatPause: Int64  // 可接受的心跳暂停时间，默认3000ms
    public let firstHeartbeatEstimate: Int64    // 首次心跳估计时间，默认1000ms

    public init(threshold: Float64, maxSampleSize: Int32,
                minStdDeviation: Float64, acceptableHeartbeatPause: Int64,
                firstHeartbeatEstimate: Int64) {
        this.threshold = threshold
        this.maxSampleSize = maxSampleSize
        this.minStdDeviation = minStdDeviation
        this.acceptableHeartbeatPause = acceptableHeartbeatPause
        this.firstHeartbeatEstimate = firstHeartbeatEstimate
    }

    public static func createDefault(): PhiAccrualConfig {
        PhiAccrualConfig(8.0, 200, 100.0, 3000, 1000)
    }

    public static func createSensitive(): PhiAccrualConfig {
        PhiAccrualConfig(5.0, 100, 50.0, 2000, 500)
    }

    public static func createTolerant(): PhiAccrualConfig {
        PhiAccrualConfig(12.0, 300, 200.0, 5000, 2000)
    }
}

/**
 * 心跳历史记录管理器
 */
public class HeartbeatHistory {
    private let intervals: ArrayList<Int64>
    private let maxSize: Int32
    private let mutex: Mutex
    private let firstHeartbeatEstimate: Int64

    public init(maxSize: Int32, firstHeartbeatEstimate: Int64) {
        this.intervals = ArrayList<Int64>()
        this.maxSize = maxSize
        this.mutex = Mutex()
        this.firstHeartbeatEstimate = firstHeartbeatEstimate
    }

    /**
     * 记录心跳间隔
     */
    public func recordInterval(intervalMs: Int64): Unit {
        mutex.lock()
        try {
            intervals.add(intervalMs)

            // 保持最大样本数量
            while (Int32(intervals.size) > maxSize) {
                if (intervals.size > 0) {
                    // 简化实现：清空并重新添加最新的样本
                    let newIntervals = ArrayList<Int64>()
                    let startIndex = intervals.size - Int64(maxSize)
                    for (i in startIndex..intervals.size) {
                        if (i >= 0 && i < intervals.size) {
                            newIntervals.add(intervals[i])
                        }
                    }
                    intervals.clear()
                    for (interval in newIntervals) {
                        intervals.add(interval)
                    }
                    break
                }
            }
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 计算平均间隔
     */
    public func getMeanInterval(): Float64 {
        mutex.lock()
        try {
            if (intervals.isEmpty()) {
                return Float64(firstHeartbeatEstimate)
            }

            var sum = Int64(0)
            for (interval in intervals) {
                sum = sum + interval
            }
            Float64(sum) / Float64(intervals.size)
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 计算方差
     */
    public func getVariance(): Float64 {
        mutex.lock()
        try {
            if (intervals.size < 2) {
                return Float64(firstHeartbeatEstimate * firstHeartbeatEstimate)
            }

            let mean = getMeanInterval()
            var sumSquaredDiffs = Float64(0.0)
            for (interval in intervals) {
                let diff = Float64(interval) - mean
                sumSquaredDiffs = sumSquaredDiffs + (diff * diff)
            }

            sumSquaredDiffs / Float64(intervals.size - 1)
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 获取标准差
     */
    public func getStdDeviation(): Float64 {
        sqrt(getVariance())
    }

    /**
     * 获取样本数量
     */
    public func getSampleSize(): Int32 {
        mutex.lock()
        try {
            Int32(intervals.size)
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 清空历史记录
     */
    public func clear(): Unit {
        mutex.lock()
        try {
            intervals.clear()
        } finally {
            mutex.unlock()
        }
    }
}

/**
 * Phi Accrual故障检测器
 * 基于统计学的自适应故障检测算法
 */
public class PhiAccrualFailureDetector {
    private let config: PhiAccrualConfig
    private let nodeHistories: HashMap<String, HeartbeatHistory>
    private let lastHeartbeats: HashMap<String, Int64>
    private let heartbeatCounts: HashMap<String, AtomicInt64>
    private let mutex: Mutex

    public init(config: PhiAccrualConfig) {
        this.config = config
        this.nodeHistories = HashMap<String, HeartbeatHistory>()
        this.lastHeartbeats = HashMap<String, Int64>()
        this.heartbeatCounts = HashMap<String, AtomicInt64>()
        this.mutex = Mutex()
    }

    public init() {
        this.config = PhiAccrualConfig.createDefault()
        this.nodeHistories = HashMap<String, HeartbeatHistory>()
        this.lastHeartbeats = HashMap<String, Int64>()
        this.heartbeatCounts = HashMap<String, AtomicInt64>()
        this.mutex = Mutex()
    }

    /**
     * 记录心跳
     */
    public func heartbeat(nodeId: String): Unit {
        let currentTime = getCurrentTimeNanos() / 1_000_000  // 转换为毫秒
        
        mutex.lock()
        try {
            // 获取或创建心跳历史
            let history = match (nodeHistories.get(nodeId)) {
                case Some(h) => h
                case None =>
                    let newHistory = HeartbeatHistory(config.maxSampleSize, config.firstHeartbeatEstimate)
                    nodeHistories.add(nodeId, newHistory)
                    heartbeatCounts.add(nodeId, AtomicInt64(0))
                    newHistory
            }
            
            // 记录心跳间隔
            match (lastHeartbeats.get(nodeId)) {
                case Some(lastTime) =>
                    let interval = currentTime - lastTime
                    if (interval > 0) {
                        history.recordInterval(interval)
                    }
                case None => ()
            }
            
            // 更新最后心跳时间和计数
            lastHeartbeats.add(nodeId, currentTime)
            match (heartbeatCounts.get(nodeId)) {
                case Some(counter) => counter.fetchAdd(1)
                case None => ()
            }
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 计算节点的Phi值
     */
    public func phi(nodeId: String): Float64 {
        mutex.lock()
        try {
            match (lastHeartbeats.get(nodeId)) {
                case Some(lastTime) =>
                    let currentTime = getCurrentTimeNanos() / 1_000_000
                    let timeSinceLastHeartbeat = currentTime - lastTime
                    
                    match (nodeHistories.get(nodeId)) {
                        case Some(history) =>
                            calculatePhi(timeSinceLastHeartbeat, history)
                        case None => 1000000.0  // 使用大数值代替infinity
                    }
                case None => 1000000.0  // 使用大数值代替infinity
            }
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 检查节点是否可用
     */
    public func isAvailable(nodeId: String): Bool {
        phi(nodeId) < config.threshold
    }

    /**
     * 检查节点是否被怀疑故障
     */
    public func isMonitoring(nodeId: String): Bool {
        mutex.lock()
        try {
            match (lastHeartbeats.get(nodeId)) {
                case Some(_) => true
                case None => false
            }
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 移除节点监控
     */
    public func remove(nodeId: String): Unit {
        mutex.lock()
        try {
            nodeHistories.remove(nodeId)
            lastHeartbeats.remove(nodeId)
            heartbeatCounts.remove(nodeId)
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 获取节点统计信息
     */
    public func getStatistics(nodeId: String): Option<PhiAccrualStatistics> {
        mutex.lock()
        try {
            match (nodeHistories.get(nodeId)) {
                case Some(history) =>
                    let lastTime = match (lastHeartbeats.get(nodeId)) {
                        case Some(time) => time
                        case None => 0
                    }
                    let count = match (heartbeatCounts.get(nodeId)) {
                        case Some(counter) => counter.load()
                        case None => 0
                    }
                    let currentPhi = phi(nodeId)
                    let isAvail = isAvailable(nodeId)
                    let mean = history.getMeanInterval()
                    let variance = history.getVariance()
                    
                    Some(PhiAccrualStatistics(nodeId, count, lastTime, currentPhi, 
                                            config.threshold, isAvail, mean, variance))
                case None => None<PhiAccrualStatistics>
            }
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 获取所有监控节点的统计信息
     */
    public func getAllStatistics(): Array<PhiAccrualStatistics> {
        mutex.lock()
        try {
            let stats = ArrayList<PhiAccrualStatistics>()
            for ((nodeId, _) in nodeHistories) {
                match (getStatistics(nodeId)) {
                    case Some(stat) => stats.add(stat)
                    case None => ()
                }
            }
            stats.toArray()
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 计算Phi值的核心算法
     */
    private func calculatePhi(timeSinceLastHeartbeat: Int64, history: HeartbeatHistory): Float64 {
        let mean = history.getMeanInterval()
        let stdDev = max(history.getStdDeviation(), config.minStdDeviation)
        
        // 使用正态分布的累积分布函数计算Phi
        let y = (Float64(timeSinceLastHeartbeat) - mean) / stdDev
        let e = exp(-y * (1.5976 + 0.070566 * y * y))
        
        if (Float64(timeSinceLastHeartbeat) > mean) {
            -log10(e / (1.0 + e))
        } else {
            -log10(1.0 - 1.0 / (1.0 + e))
        }
    }

    private func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds() * 1_000_000
    }

    private func max(a: Float64, b: Float64): Float64 {
        if (a > b) { a } else { b }
    }

    private func exp(x: Float64): Float64 {
        // 简化的指数函数实现
        // 在实际实现中应该使用更精确的数学库
        if (x > 700.0) { 1000000.0 }  // 使用大数值代替infinity
        else if (x < -700.0) { 0.0 }
        else {
            // 泰勒级数近似
            let result = 1.0 + x + (x * x) / 2.0 + (x * x * x) / 6.0
            if (result < 0.0) { 0.0 } else { result }
        }
    }

    private func log10(x: Float64): Float64 {
        if (x <= 0.0) { -1000000.0 }  // 使用大负数代替negativeInfinity
        else if (x == 1.0) { 0.0 }
        else {
            // 简化的对数函数实现
            // 在实际实现中应该使用更精确的数学库
            let ln2 = 0.693147180559945
            let lnx = if (x > 1.0) {
                let y = x - 1.0
                y - (y * y) / 2.0 + (y * y * y) / 3.0
            } else {
                let y = 1.0 - x
                -(y + (y * y) / 2.0 + (y * y * y) / 3.0)
            }
            lnx / (ln2 * 2.302585092994046)  // ln(10)
        }
    }

    private func sqrt(x: Float64): Float64 {
        if (x < 0.0) { -1.0 }  // 使用-1代替nan
        else if (x == 0.0) { 0.0 }
        else {
            // 牛顿法求平方根
            var result = x / 2.0
            for (i in 0..10) {
                result = (result + x / result) / 2.0
            }
            result
        }
    }
}
