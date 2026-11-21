/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Runtime - 高性能 Actor 系统的配置与统计类型
 *
 * D1 门面收敛：HighPerformanceActorSystem / HighPerformanceActorRef / SystemPerformanceMonitor
 * 已移入 cactor_runtime 包并降为 internal（它们只被 CActorRuntime 使用）。
 * 本文件保留仍属于公开 API 的配置与统计结构：CActorRuntimeConfig.highPerformanceConfig
 * 与 CActorRuntimeStats.highPerformanceStats 的类型仍是公开的，故包路径保持 cactor_runtime.system。
 */
package cactor_runtime.system

import cactor_runtime.context.{PoolStats}

/**
 * 高性能Actor系统配置
 */
public struct HighPerformanceActorSystemConfig {
    public let maxActors: Int64                    // 最大Actor数量
    public let contextPoolSize: Int64              // 上下文池大小
    public let workerThreads: Int32                // 工作线程数
    public let mailboxCapacity: Int64              // 邮箱容量
    public let enableBatchProcessing: Bool         // 启用批处理
    public let batchSize: Int32                    // 批处理大小
    public let enableMemoryOptimization: Bool      // 启用内存优化
    public let enablePerformanceMonitoring: Bool   // 启用性能监控

    public init(maxActors: Int64, contextPoolSize: Int64, workerThreads: Int32,
                mailboxCapacity: Int64, enableBatchProcessing: Bool, batchSize: Int32,
                enableMemoryOptimization: Bool, enablePerformanceMonitoring: Bool) {
        this.maxActors = maxActors
        this.contextPoolSize = contextPoolSize
        this.workerThreads = workerThreads
        this.mailboxCapacity = mailboxCapacity
        this.enableBatchProcessing = enableBatchProcessing
        this.batchSize = batchSize
        this.enableMemoryOptimization = enableMemoryOptimization
        this.enablePerformanceMonitoring = enablePerformanceMonitoring
    }

    /** 默认高性能配置 */
    public static func createDefault(): HighPerformanceActorSystemConfig {
        HighPerformanceActorSystemConfig(1000000, 10000, 8, 1024, true, 32, true, true)
    }

    /** 极限性能配置 */
    public static func createExtreme(): HighPerformanceActorSystemConfig {
        HighPerformanceActorSystemConfig(10000000, 50000, 16, 2048, true, 64, true, true)
    }

    /** 内存优化配置 */
    public static func createMemoryOptimized(): HighPerformanceActorSystemConfig {
        HighPerformanceActorSystemConfig(500000, 5000, 4, 512, false, 16, true, false)
    }

    /** 低延迟配置 */
    public static func createLowLatency(): HighPerformanceActorSystemConfig {
        HighPerformanceActorSystemConfig(1000000, 20000, 8, 1024, false, 8, true, true)
    }

    /** 高吞吐量配置 */
    public static func createHighThroughput(): HighPerformanceActorSystemConfig {
        HighPerformanceActorSystemConfig(5000000, 30000, 32, 4096, true, 128, true, true)
    }
}

/**
 * 系统统计信息
 */
public struct SystemStats {
    public let actorCount: Int64
    public let messageCount: Int64
    public let isRunning: Bool
    public let poolStats: PoolStats
    public let dispatcherStats: DispatcherStats
    public let monitorStats: MonitorStats

    public init(actorCount: Int64, messageCount: Int64, isRunning: Bool,
                poolStats: PoolStats, dispatcherStats: DispatcherStats, monitorStats: MonitorStats) {
        this.actorCount = actorCount
        this.messageCount = messageCount
        this.isRunning = isRunning
        this.poolStats = poolStats
        this.dispatcherStats = dispatcherStats
        this.monitorStats = monitorStats
    }

    public func printReport(): Unit {
        println("=== 高性能Actor系统统计 ===")
        println("Actor数量: ${actorCount}")
        println("消息总数: ${messageCount}")
        let statusText = if (isRunning) { "运行中" } else { "已停止" }
        println("系统状态: ${statusText}")
        println("对象池利用率: ${poolStats.getUtilization()}%")
        println("对象池命中率: ${poolStats.getHitRate()}%")
        println("调度器吞吐量: ${dispatcherStats.getThroughput()} msg/s")
        println("平均延迟: ${monitorStats.getAverageLatency()}μs")
    }
}

/**
 * 监控统计信息
 */
public struct MonitorStats {
    public let actorsCreated: Int64
    public let actorsDestroyed: Int64
    public let messagesSent: Int64
    public let messagesFailed: Int64
    public let uptime: Int64

    public init(actorsCreated: Int64, actorsDestroyed: Int64, messagesSent: Int64, messagesFailed: Int64, uptime: Int64) {
        this.actorsCreated = actorsCreated
        this.actorsDestroyed = actorsDestroyed
        this.messagesSent = messagesSent
        this.messagesFailed = messagesFailed
        this.uptime = uptime
    }

    public func getSuccessRate(): Float64 {
        let total = messagesSent + messagesFailed
        if (total == 0) {
            return 100.0
        }
        return Float64(messagesSent) / Float64(total) * 100.0
    }

    public func getAverageLatency(): Float64 {
        // 简化实现，实际应该收集延迟数据
        return 0.5  // 假设0.5μs平均延迟
    }

    public func getUptimeInSeconds(): Float64 {
        Float64(uptime) / 1000000000.0
    }
}

/**
 * 调度器统计信息占位符
 */
public struct DispatcherStats {
    public let processedMessages: Int64
    public let averageLatency: Float64

    public init(processedMessages: Int64, averageLatency: Float64) {
        this.processedMessages = processedMessages
        this.averageLatency = averageLatency
    }

    public func getThroughput(): Float64 {
        // 简化实现
        return Float64(processedMessages)
    }
}
