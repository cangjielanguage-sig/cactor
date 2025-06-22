/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 自适应调度器
 * 根据负载自动调整调度策略
 */
package cactor.runtime.dispatcher.adaptive

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*
import cactor.core.config.{DispatcherConfig, DispatcherType}
import cactor.core.message.{Message, Envelope}
import cactor.core.actor.ActorRef
import cactor.runtime.dispatcher.advanced.{AdvancedMessageDispatcher, Priority}
import cactor.runtime.dispatcher.dynamic.{DynamicDispatcherConfigManager, ConfigUpdate}

/**
 * 自适应调度器
 * 根据系统负载和性能指标自动调整调度策略
 */
public class AdaptiveDispatcher <: AdvancedMessageDispatcher {
    private let name: String
    private let baseDispatcher: AdvancedMessageDispatcher
    private let configManager: DynamicDispatcherConfigManager
    private let performanceMonitor: PerformanceMonitor
    private let adaptationEngine: AdaptationEngine
    private let running: AtomicBool
    
    public init(name: String, baseDispatcher: AdvancedMessageDispatcher, 
                configManager: DynamicDispatcherConfigManager) {
        this.name = name
        this.baseDispatcher = baseDispatcher
        this.configManager = configManager
        this.performanceMonitor = PerformanceMonitor()
        this.adaptationEngine = AdaptationEngine(configManager, performanceMonitor)
        this.running = AtomicBool(false)
    }
    
    public func dispatch(envelope: Envelope, actorRef: ActorRef): Unit {
        if (!running.load()) {
            throw Exception("AdaptiveDispatcher ${name} is not running")
        }
        
        let startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        
        try {
            baseDispatcher.dispatch(envelope, actorRef)
            
            // 记录成功的分发
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            performanceMonitor.recordDispatch(endTime - startTime, true)
            
        } catch (e: Exception) {
            // 记录失败的分发
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            performanceMonitor.recordDispatch(endTime - startTime, false)
            throw e
        }
    }
    
    public func dispatchWithPriority(envelope: Envelope, actorRef: ActorRef, priority: Priority): Unit {
        if (!running.load()) {
            throw Exception("AdaptiveDispatcher ${name} is not running")
        }
        
        let startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        
        try {
            baseDispatcher.dispatchWithPriority(envelope, actorRef, priority)
            
            // 记录成功的优先级分发
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            performanceMonitor.recordPriorityDispatch(endTime - startTime, priority, true)
            
        } catch (e: Exception) {
            // 记录失败的优先级分发
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            performanceMonitor.recordPriorityDispatch(endTime - startTime, priority, false)
            throw e
        }
    }
    
    public func dispatchBatch(envelopes: Array<Envelope>, actorRefs: Array<ActorRef>): Unit {
        if (!running.load()) {
            throw Exception("AdaptiveDispatcher ${name} is not running")
        }
        
        let startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        let batchSize = Int64(envelopes.size)
        
        try {
            baseDispatcher.dispatchBatch(envelopes, actorRefs)
            
            // 记录批量分发
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            performanceMonitor.recordBatchDispatch(endTime - startTime, batchSize, true)
            
        } catch (e: Exception) {
            // 记录失败的批量分发
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            performanceMonitor.recordBatchDispatch(endTime - startTime, batchSize, false)
            throw e
        }
    }
    
    public func start(): Unit {
        if (running.compareAndSwap(false, true)) {
            baseDispatcher.start()
            performanceMonitor.start()
            adaptationEngine.start()
            println("[ADAPTIVE] 自适应调度器 ${name} 启动")
        }
    }
    
    public func stop(): Unit {
        if (running.compareAndSwap(true, false)) {
            adaptationEngine.stop()
            performanceMonitor.stop()
            baseDispatcher.stop()
            println("[ADAPTIVE] 自适应调度器 ${name} 停止")
        }
    }
    
    public func getConfig(): DispatcherConfig {
        baseDispatcher.getConfig()
    }
    
    public func getPerformanceStats(): String {
        let baseStats = baseDispatcher.getPerformanceStats()
        let adaptiveStats = performanceMonitor.getStats()
        "${baseStats}\n自适应统计:\n${adaptiveStats}"
    }
    
    public func isRunning(): Bool {
        running.load()
    }
    
    /**
     * 获取性能监控器
     */
    public func getPerformanceMonitor(): PerformanceMonitor {
        performanceMonitor
    }
    
    /**
     * 获取适应引擎
     */
    public func getAdaptationEngine(): AdaptationEngine {
        adaptationEngine
    }
}

/**
 * 性能监控器
 * 收集和分析调度器性能指标
 */
public class PerformanceMonitor {
    private let totalDispatches: AtomicInt64
    private let successfulDispatches: AtomicInt64
    private let failedDispatches: AtomicInt64
    private let totalLatency: AtomicInt64
    private let priorityStats: ConcurrentHashMap<String, PriorityStats>
    private let batchStats: BatchStats
    private let isRunning: AtomicBool
    
    public init() {
        this.totalDispatches = AtomicInt64(0)
        this.successfulDispatches = AtomicInt64(0)
        this.failedDispatches = AtomicInt64(0)
        this.totalLatency = AtomicInt64(0)
        this.priorityStats = ConcurrentHashMap<String, PriorityStats>()
        this.batchStats = BatchStats()
        this.isRunning = AtomicBool(false)

        // 初始化优先级统计
        priorityStats.put("Critical", PriorityStats())
        priorityStats.put("High", PriorityStats())
        priorityStats.put("Normal", PriorityStats())
        priorityStats.put("Low", PriorityStats())
    }
    
    public func start(): Unit {
        isRunning.store(true)
    }
    
    public func stop(): Unit {
        isRunning.store(false)
    }
    
    /**
     * 记录分发操作
     */
    public func recordDispatch(latencyMs: Int64, success: Bool): Unit {
        if (!isRunning.load()) { return }
        
        totalDispatches.fetchAdd(1)
        totalLatency.fetchAdd(latencyMs)
        
        if (success) {
            successfulDispatches.fetchAdd(1)
        } else {
            failedDispatches.fetchAdd(1)
        }
    }
    
    /**
     * 记录优先级分发操作
     */
    public func recordPriorityDispatch(latencyMs: Int64, priority: Priority, success: Bool): Unit {
        recordDispatch(latencyMs, success)
        
        let priorityKey = match (priority) {
            case Priority.Critical => "Critical"
            case Priority.High => "High"
            case Priority.Normal => "Normal"
            case Priority.Low => "Low"
        }

        match (priorityStats.get(priorityKey)) {
            case Some(stats) => stats.record(latencyMs, success)
            case None => ()
        }
    }
    
    /**
     * 记录批量分发操作
     */
    public func recordBatchDispatch(latencyMs: Int64, batchSize: Int64, success: Bool): Unit {
        recordDispatch(latencyMs, success)
        batchStats.record(latencyMs, batchSize, success)
    }
    
    /**
     * 获取平均延迟
     */
    public func getAverageLatency(): Float64 {
        let total = totalDispatches.load()
        if (total == 0) {
            0.0
        } else {
            Float64(totalLatency.load()) / Float64(total)
        }
    }
    
    /**
     * 获取成功率
     */
    public func getSuccessRate(): Float64 {
        let total = totalDispatches.load()
        if (total == 0) {
            1.0
        } else {
            Float64(successfulDispatches.load()) / Float64(total)
        }
    }
    
    /**
     * 获取吞吐量（每秒分发数）
     */
    public func getThroughput(): Float64 {
        // 简化实现：返回总分发数
        Float64(totalDispatches.load())
    }
    
    /**
     * 获取统计信息
     */
    public func getStats(): String {
        let avgLatency = getAverageLatency()
        let successRate = getSuccessRate()
        let throughput = getThroughput()
        
        "平均延迟: ${avgLatency}ms, 成功率: ${successRate}, 吞吐量: ${throughput}"
    }
    
    /**
     * 重置统计信息
     */
    public func reset(): Unit {
        totalDispatches.store(0)
        successfulDispatches.store(0)
        failedDispatches.store(0)
        totalLatency.store(0)
        
        for ((_, stats) in priorityStats) {
            stats.reset()
        }
        
        batchStats.reset()
    }
}

/**
 * 优先级统计
 */
public class PriorityStats {
    private let count: AtomicInt64
    private let successCount: AtomicInt64
    private let totalLatency: AtomicInt64
    
    public init() {
        this.count = AtomicInt64(0)
        this.successCount = AtomicInt64(0)
        this.totalLatency = AtomicInt64(0)
    }
    
    public func record(latencyMs: Int64, success: Bool): Unit {
        count.fetchAdd(1)
        totalLatency.fetchAdd(latencyMs)
        
        if (success) {
            successCount.fetchAdd(1)
        }
    }
    
    public func reset(): Unit {
        count.store(0)
        successCount.store(0)
        totalLatency.store(0)
    }
    
    public func getAverageLatency(): Float64 {
        let total = count.load()
        if (total == 0) {
            0.0
        } else {
            Float64(totalLatency.load()) / Float64(total)
        }
    }
    
    public func getSuccessRate(): Float64 {
        let total = count.load()
        if (total == 0) {
            1.0
        } else {
            Float64(successCount.load()) / Float64(total)
        }
    }
}

/**
 * 批量统计
 */
public class BatchStats {
    private let batchCount: AtomicInt64
    private let totalItems: AtomicInt64
    private let successfulBatches: AtomicInt64
    
    public init() {
        this.batchCount = AtomicInt64(0)
        this.totalItems = AtomicInt64(0)
        this.successfulBatches = AtomicInt64(0)
    }
    
    public func record(latencyMs: Int64, batchSize: Int64, success: Bool): Unit {
        batchCount.fetchAdd(1)
        totalItems.fetchAdd(batchSize)
        
        if (success) {
            successfulBatches.fetchAdd(1)
        }
    }
    
    public func reset(): Unit {
        batchCount.store(0)
        totalItems.store(0)
        successfulBatches.store(0)
    }
    
    public func getAverageBatchSize(): Float64 {
        let batches = batchCount.load()
        if (batches == 0) {
            0.0
        } else {
            Float64(totalItems.load()) / Float64(batches)
        }
    }
}

/**
 * 适应引擎
 * 根据性能指标自动调整配置
 */
public class AdaptationEngine {
    private let configManager: DynamicDispatcherConfigManager
    private let performanceMonitor: PerformanceMonitor
    private let isRunning: AtomicBool
    private let adaptationInterval: Duration
    
    public init(configManager: DynamicDispatcherConfigManager, performanceMonitor: PerformanceMonitor) {
        this.configManager = configManager
        this.performanceMonitor = performanceMonitor
        this.isRunning = AtomicBool(false)
        this.adaptationInterval = Duration.second * 5  // 5秒适应间隔
    }
    
    public func start(): Unit {
        if (isRunning.compareAndSwap(false, true)) {
            println("[ADAPTATION] 适应引擎启动")
            // 在实际实现中，这里会启动一个后台线程定期执行适应逻辑
        }
    }
    
    public func stop(): Unit {
        if (isRunning.compareAndSwap(true, false)) {
            println("[ADAPTATION] 适应引擎停止")
        }
    }
    
    /**
     * 执行适应逻辑
     */
    public func adapt(dispatcherName: String): Unit {
        if (!isRunning.load()) { return }
        
        let avgLatency = performanceMonitor.getAverageLatency()
        let successRate = performanceMonitor.getSuccessRate()
        let throughput = performanceMonitor.getThroughput()
        
        // 简化的适应逻辑
        if (avgLatency > 100.0) {  // 延迟过高
            // 增加吞吐量
            let update = ConfigUpdate.throughputUpdate(150)
            configManager.updateConfig(dispatcherName, update)
            println("[ADAPTATION] 检测到高延迟，增加吞吐量")
            
        } else if (successRate < 0.95) {  // 成功率过低
            // 减少吞吐量，增加稳定性
            let update = ConfigUpdate.throughputUpdate(50)
            configManager.updateConfig(dispatcherName, update)
            println("[ADAPTATION] 检测到低成功率，减少吞吐量")
        }
    }
}
