/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 调度器监控和指标系统
 * 提供实时调度器性能监控和指标收集
 */
package cactor_runtime.dispatcher.monitoring

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*
import cactor_runtime.dispatcher.advanced.{AdvancedMessageDispatcher, Priority}

/**
 * 调度器指标收集器
 * 收集和聚合调度器性能指标
 */
public class DispatcherMetricsCollector {
    private let metrics: ConcurrentHashMap<String, DispatcherMetrics>
    private let globalMetrics: GlobalMetrics
    private let isRunning: AtomicBool
    private let collectionInterval: Duration
    
    public init() {
        this.metrics = ConcurrentHashMap<String, DispatcherMetrics>()
        this.globalMetrics = GlobalMetrics()
        this.isRunning = AtomicBool(false)
        this.collectionInterval = Duration.second * 1  // 1秒收集间隔
    }
    
    /**
     * 启动指标收集
     */
    public func start(): Unit {
        if (isRunning.compareAndSwap(false, true)) {
            println("[METRICS] 调度器指标收集器启动")
        }
    }
    
    /**
     * 停止指标收集
     */
    public func stop(): Unit {
        if (isRunning.compareAndSwap(true, false)) {
            println("[METRICS] 调度器指标收集器停止")
        }
    }
    
    /**
     * 注册调度器
     */
    public func registerDispatcher(name: String, dispatcher: AdvancedMessageDispatcher): Unit {
        let dispatcherMetrics = DispatcherMetrics(name, dispatcher)
        metrics.add(name, dispatcherMetrics)
        println("[METRICS] 注册调度器指标: ${name}")
    }
    
    /**
     * 注销调度器
     */
    public func unregisterDispatcher(name: String): Unit {
        metrics.remove(name)
        println("[METRICS] 注销调度器指标: ${name}")
    }
    
    /**
     * 记录消息分发
     */
    public func recordDispatch(dispatcherName: String, latencyMs: Int64, priority: Priority, success: Bool): Unit {
        if (!isRunning.load()) { return }
        
        match (metrics.get(dispatcherName)) {
            case Some(dispatcherMetrics) =>
                dispatcherMetrics.recordDispatch(latencyMs, priority, success)
                globalMetrics.recordDispatch(latencyMs, priority, success)
            case None =>
                println("[METRICS] 未找到调度器指标: ${dispatcherName}")
        }
    }
    
    /**
     * 记录批量分发
     */
    public func recordBatchDispatch(dispatcherName: String, latencyMs: Int64, batchSize: Int64, success: Bool): Unit {
        if (!isRunning.load()) { return }
        
        match (metrics.get(dispatcherName)) {
            case Some(dispatcherMetrics) =>
                dispatcherMetrics.recordBatchDispatch(latencyMs, batchSize, success)
                globalMetrics.recordBatchDispatch(latencyMs, batchSize, success)
            case None =>
                println("[METRICS] 未找到调度器指标: ${dispatcherName}")
        }
    }
    
    /**
     * 获取调度器指标
     */
    public func getDispatcherMetrics(name: String): Option<DispatcherMetrics> {
        metrics.get(name)
    }
    
    /**
     * 获取全局指标
     */
    public func getGlobalMetrics(): GlobalMetrics {
        globalMetrics
    }
    
    /**
     * 获取所有调度器的指标摘要
     */
    public func getMetricsSummary(): MetricsSummary {
        let dispatcherSummaries = ArrayList<DispatcherSummary>()
        
        for ((name, dispatcherMetrics) in metrics) {
            let summary = dispatcherMetrics.getSummary()
            dispatcherSummaries.add(summary)
        }
        
        MetricsSummary(dispatcherSummaries.toArray(), globalMetrics.getSummary())
    }
    
    /**
     * 重置所有指标
     */
    public func resetAllMetrics(): Unit {
        for ((_, dispatcherMetrics) in metrics) {
            dispatcherMetrics.reset()
        }
        globalMetrics.reset()
        println("[METRICS] 所有指标已重置")
    }
    
    /**
     * 生成指标报告
     */
    public func generateReport(): String {
        let summary = getMetricsSummary()
        let report = StringBuilder()
        
        report.append("=== 调度器指标报告 ===\n")
        report.append("生成时间: ${DateTime.now().toString()}\n\n")

        // 全局指标
        report.append("全局指标:\n")
        report.append("  总分发数: ${summary.globalSummary.totalDispatches}\n")
        report.append("  成功率: ${summary.globalSummary.successRate}\n")
        report.append("  平均延迟: ${summary.globalSummary.averageLatency}ms\n")
        report.append("  总吞吐量: ${summary.globalSummary.throughput}\n\n")

        // 各调度器指标
        report.append("各调度器指标:\n")
        for (dispatcherSummary in summary.dispatcherSummaries) {
            report.append("  调度器: ${dispatcherSummary.name}\n")
            report.append("    分发数: ${dispatcherSummary.totalDispatches}\n")
            report.append("    成功率: ${dispatcherSummary.successRate}\n")
            report.append("    平均延迟: ${dispatcherSummary.averageLatency}ms\n")
            report.append("    吞吐量: ${dispatcherSummary.throughput}\n\n")
        }
        
        report.toString()
    }
}

/**
 * 调度器指标
 */
public class DispatcherMetrics {
    private let name: String
    private let dispatcher: AdvancedMessageDispatcher
    private let totalDispatches: AtomicInt64
    private let successfulDispatches: AtomicInt64
    private let totalLatency: AtomicInt64
    private let priorityCounters: ConcurrentHashMap<String, AtomicInt64>
    private let batchDispatches: AtomicInt64
    private let totalBatchSize: AtomicInt64
    private let startTime: Int64
    
    public init(name: String, dispatcher: AdvancedMessageDispatcher) {
        this.name = name
        this.dispatcher = dispatcher
        this.totalDispatches = AtomicInt64(0)
        this.successfulDispatches = AtomicInt64(0)
        this.totalLatency = AtomicInt64(0)
        this.priorityCounters = ConcurrentHashMap<String, AtomicInt64>()
        this.batchDispatches = AtomicInt64(0)
        this.totalBatchSize = AtomicInt64(0)
        this.startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()

        // 初始化优先级计数器
        priorityCounters.add("Critical", AtomicInt64(0))
        priorityCounters.add("High", AtomicInt64(0))
        priorityCounters.add("Normal", AtomicInt64(0))
        priorityCounters.add("Low", AtomicInt64(0))
    }
    
    /**
     * 记录分发操作
     */
    public func recordDispatch(latencyMs: Int64, priority: Priority, success: Bool): Unit {
        totalDispatches.fetchAdd(1)
        totalLatency.fetchAdd(latencyMs)
        
        if (success) {
            successfulDispatches.fetchAdd(1)
        }
        
        let priorityKey = match (priority) {
            case Priority.Critical => "Critical"
            case Priority.High => "High"
            case Priority.Normal => "Normal"
            case Priority.Low => "Low"
        }

        match (priorityCounters.get(priorityKey)) {
            case Some(counter) => counter.fetchAdd(1)
            case None => ()
        }
    }
    
    /**
     * 记录批量分发操作
     */
    public func recordBatchDispatch(latencyMs: Int64, batchSize: Int64, success: Bool): Unit {
        batchDispatches.fetchAdd(1)
        totalBatchSize.fetchAdd(batchSize)
        
        // 批量分发也计入总分发数
        recordDispatch(latencyMs, Priority.Normal, success)
    }
    
    /**
     * 获取指标摘要
     */
    public func getSummary(): DispatcherSummary {
        let total = totalDispatches.load()
        let successful = successfulDispatches.load()
        let latency = totalLatency.load()
        
        let successRate = if (total == 0) { 1.0 } else { Float64(successful) / Float64(total) }
        let averageLatency = if (total == 0) { 0.0 } else { Float64(latency) / Float64(total) }
        
        // 计算吞吐量（每秒分发数）
        let currentTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        let elapsedSeconds = Float64(currentTime - startTime) / 1000.0
        let throughput = if (elapsedSeconds == 0.0) { 0.0 } else { Float64(total) / elapsedSeconds }
        
        DispatcherSummary(name, total, successRate, averageLatency, throughput)
    }
    
    /**
     * 重置指标
     */
    public func reset(): Unit {
        totalDispatches.store(0)
        successfulDispatches.store(0)
        totalLatency.store(0)
        batchDispatches.store(0)
        totalBatchSize.store(0)
        
        for ((_, counter) in priorityCounters) {
            counter.store(0)
        }
    }
    
    /**
     * 获取调度器名称
     */
    public func getName(): String {
        name
    }
}

/**
 * 全局指标
 */
public class GlobalMetrics {
    private let totalDispatches: AtomicInt64
    private let successfulDispatches: AtomicInt64
    private let totalLatency: AtomicInt64
    private let totalBatchDispatches: AtomicInt64
    private let totalBatchSize: AtomicInt64
    private let startTime: Int64
    
    public init() {
        this.totalDispatches = AtomicInt64(0)
        this.successfulDispatches = AtomicInt64(0)
        this.totalLatency = AtomicInt64(0)
        this.totalBatchDispatches = AtomicInt64(0)
        this.totalBatchSize = AtomicInt64(0)
        this.startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
    }
    
    public func recordDispatch(latencyMs: Int64, priority: Priority, success: Bool): Unit {
        totalDispatches.fetchAdd(1)
        totalLatency.fetchAdd(latencyMs)
        
        if (success) {
            successfulDispatches.fetchAdd(1)
        }
    }
    
    public func recordBatchDispatch(latencyMs: Int64, batchSize: Int64, success: Bool): Unit {
        totalBatchDispatches.fetchAdd(1)
        totalBatchSize.fetchAdd(batchSize)
        recordDispatch(latencyMs, Priority.Normal, success)
    }
    
    public func getSummary(): GlobalSummary {
        let total = totalDispatches.load()
        let successful = successfulDispatches.load()
        let latency = totalLatency.load()
        
        let successRate = if (total == 0) { 1.0 } else { Float64(successful) / Float64(total) }
        let averageLatency = if (total == 0) { 0.0 } else { Float64(latency) / Float64(total) }
        
        // 计算吞吐量
        let currentTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        let elapsedSeconds = Float64(currentTime - startTime) / 1000.0
        let throughput = if (elapsedSeconds == 0.0) { 0.0 } else { Float64(total) / elapsedSeconds }
        
        GlobalSummary(total, successRate, averageLatency, throughput)
    }
    
    public func reset(): Unit {
        totalDispatches.store(0)
        successfulDispatches.store(0)
        totalLatency.store(0)
        totalBatchDispatches.store(0)
        totalBatchSize.store(0)
    }
}

/**
 * 调度器摘要
 */
public struct DispatcherSummary {
    public let name: String
    public let totalDispatches: Int64
    public let successRate: Float64
    public let averageLatency: Float64
    public let throughput: Float64
    
    public init(name: String, totalDispatches: Int64, successRate: Float64, 
                averageLatency: Float64, throughput: Float64) {
        this.name = name
        this.totalDispatches = totalDispatches
        this.successRate = successRate
        this.averageLatency = averageLatency
        this.throughput = throughput
    }
}

/**
 * 全局摘要
 */
public struct GlobalSummary {
    public let totalDispatches: Int64
    public let successRate: Float64
    public let averageLatency: Float64
    public let throughput: Float64
    
    public init(totalDispatches: Int64, successRate: Float64, averageLatency: Float64, throughput: Float64) {
        this.totalDispatches = totalDispatches
        this.successRate = successRate
        this.averageLatency = averageLatency
        this.throughput = throughput
    }
}

/**
 * 指标摘要
 */
public struct MetricsSummary {
    public let dispatcherSummaries: Array<DispatcherSummary>
    public let globalSummary: GlobalSummary
    
    public init(dispatcherSummaries: Array<DispatcherSummary>, globalSummary: GlobalSummary) {
        this.dispatcherSummaries = dispatcherSummaries
        this.globalSummary = globalSummary
    }
}
