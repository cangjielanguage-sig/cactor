/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 增强调度器工厂
 * 支持动态配置、自适应和监控的调度器创建
 */
package cactor_runtime.dispatcher.factory

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*
import cactor_core.config.{DispatcherConfig, DispatcherType}
import cactor_core.message.Envelope
import cactor_core.actor.ActorRef
import cactor_runtime.dispatcher.advanced.{AdvancedMessageDispatcher, AdvancedWorkStealingDispatcher, ThreadPoolDispatcher, PinnedDispatcher, CallingThreadDispatcher, Priority}
import cactor_runtime.dispatcher.dynamic.{DynamicDispatcherConfigManager, DynamicDispatcherConfig}
import cactor_runtime.dispatcher.adaptive.AdaptiveDispatcher
import cactor_runtime.dispatcher.monitoring.DispatcherMetricsCollector

/**
 * 增强调度器工厂
 * 创建支持动态配置、自适应和监控的调度器
 */
public class EnhancedDispatcherFactory {
    private let configManager: DynamicDispatcherConfigManager
    private let metricsCollector: DispatcherMetricsCollector
    private let createdDispatchers: ConcurrentHashMap<String, AdvancedMessageDispatcher>
    
    public init() {
        this.configManager = DynamicDispatcherConfigManager()
        this.metricsCollector = DispatcherMetricsCollector()
        this.createdDispatchers = ConcurrentHashMap<String, AdvancedMessageDispatcher>()
    }
    
    /**
     * 启动工厂
     */
    public func start(): Unit {
        configManager.start()
        metricsCollector.start()
        println("[ENHANCED-FACTORY] 增强调度器工厂启动")
    }
    
    /**
     * 停止工厂
     */
    public func stop(): Unit {
        // 停止所有创建的调度器
        for ((name, dispatcher) in createdDispatchers) {
            dispatcher.stop()
            metricsCollector.unregisterDispatcher(name)
        }
        
        metricsCollector.stop()
        configManager.stop()
        println("[ENHANCED-FACTORY] 增强调度器工厂停止")
    }
    
    /**
     * 创建标准调度器
     */
    public func createDispatcher(config: DispatcherConfig): AdvancedMessageDispatcher {
        let baseDispatcher = createBaseDispatcher(config)
        let name = config.name
        
        // 注册到指标收集器
        metricsCollector.registerDispatcher(name, baseDispatcher)
        createdDispatchers.add(name, baseDispatcher)
        
        println("[ENHANCED-FACTORY] 创建标准调度器: ${name}")
        baseDispatcher
    }
    
    /**
     * 创建动态配置调度器
     */
    public func createDynamicDispatcher(config: DispatcherConfig): AdvancedMessageDispatcher {
        let baseDispatcher = createBaseDispatcher(config)
        let name = config.name
        
        // 注册动态配置
        let dynamicConfig = DynamicDispatcherConfig(config)
        configManager.registerConfig(name, dynamicConfig)
        
        // 注册到指标收集器
        metricsCollector.registerDispatcher(name, baseDispatcher)
        createdDispatchers.add(name, baseDispatcher)
        
        println("[ENHANCED-FACTORY] 创建动态配置调度器: ${name}")
        baseDispatcher
    }
    
    /**
     * 创建自适应调度器
     */
    public func createAdaptiveDispatcher(config: DispatcherConfig): AdaptiveDispatcher {
        let baseDispatcher = createBaseDispatcher(config)
        let name = config.name
        
        // 注册动态配置
        let dynamicConfig = DynamicDispatcherConfig(config)
        configManager.registerConfig(name, dynamicConfig)
        
        // 创建自适应调度器
        let adaptiveDispatcher = AdaptiveDispatcher(name, baseDispatcher, configManager)
        
        // 注册到指标收集器
        metricsCollector.registerDispatcher(name, adaptiveDispatcher)
        createdDispatchers.add(name, adaptiveDispatcher)
        
        println("[ENHANCED-FACTORY] 创建自适应调度器: ${name}")
        adaptiveDispatcher
    }
    
    /**
     * 创建监控调度器
     */
    public func createMonitoredDispatcher(config: DispatcherConfig): MonitoredDispatcher {
        let baseDispatcher = createBaseDispatcher(config)
        let name = config.name
        
        // 创建监控调度器
        let monitoredDispatcher = MonitoredDispatcher(name, baseDispatcher, metricsCollector)
        
        // 注册到指标收集器
        metricsCollector.registerDispatcher(name, monitoredDispatcher)
        createdDispatchers.add(name, monitoredDispatcher)
        
        println("[ENHANCED-FACTORY] 创建监控调度器: ${name}")
        monitoredDispatcher
    }
    
    /**
     * 创建完全增强的调度器（动态配置 + 自适应 + 监控）
     */
    public func createFullyEnhancedDispatcher(config: DispatcherConfig): AdaptiveDispatcher {
        let baseDispatcher = createBaseDispatcher(config)
        let name = config.name
        
        // 注册动态配置
        let dynamicConfig = DynamicDispatcherConfig(config)
        configManager.registerConfig(name, dynamicConfig)
        
        // 创建自适应调度器（已包含监控功能）
        let adaptiveDispatcher = AdaptiveDispatcher(name, baseDispatcher, configManager)
        
        // 注册到指标收集器
        metricsCollector.registerDispatcher(name, adaptiveDispatcher)
        createdDispatchers.add(name, adaptiveDispatcher)
        
        println("[ENHANCED-FACTORY] 创建完全增强调度器: ${name}")
        adaptiveDispatcher
    }
    
    /**
     * 创建基础调度器
     */
    private func createBaseDispatcher(config: DispatcherConfig): AdvancedMessageDispatcher {
        match (config.dispatcherType) {
            case DispatcherType.ThreadPool =>
                ThreadPoolDispatcher(config)
            case DispatcherType.WorkStealing =>
                AdvancedWorkStealingDispatcher(config)
            case DispatcherType.PinnedDispatcher =>
                PinnedDispatcher(config)
            case DispatcherType.CallingThread =>
                CallingThreadDispatcher(config)
            case DispatcherType.Custom =>
                // 默认使用工作窃取调度器
                AdvancedWorkStealingDispatcher(config)
        }
    }
    
    /**
     * 获取配置管理器
     */
    public func getConfigManager(): DynamicDispatcherConfigManager {
        configManager
    }
    
    /**
     * 获取指标收集器
     */
    public func getMetricsCollector(): DispatcherMetricsCollector {
        metricsCollector
    }
    
    /**
     * 获取调度器
     */
    public func getDispatcher(name: String): Option<AdvancedMessageDispatcher> {
        createdDispatchers.get(name)
    }
    
    /**
     * 移除调度器
     */
    public func removeDispatcher(name: String): Bool {
        match (createdDispatchers.get(name)) {
            case Some(dispatcher) =>
                dispatcher.stop()
                createdDispatchers.remove(name)
                metricsCollector.unregisterDispatcher(name)
                println("[ENHANCED-FACTORY] 移除调度器: ${name}")
                true
            case None =>
                false
        }
    }
    
    /**
     * 获取所有调度器名称
     */
    public func getAllDispatcherNames(): Array<String> {
        let names = ArrayList<String>()
        for ((name, _) in createdDispatchers) {
            names.add(name)
        }
        names.toArray()
    }
    
    /**
     * 生成工厂状态报告
     */
    public func generateStatusReport(): String {
        let report = StringBuilder()
        
        report.append("=== 增强调度器工厂状态报告 ===\n")
        report.append("生成时间: ${DateTime.now().toString()}\n\n")
        
        report.append("已创建调度器数量: ${createdDispatchers.size}\n")
        report.append("调度器列表:\n")
        
        for ((name, dispatcher) in createdDispatchers) {
            let isRunning = dispatcher.isRunning()
            let status = if (isRunning) { "运行中" } else { "已停止" }
            report.append("  - ${name}: ${status}\n")
        }
        
        report.append("\n")
        report.append(metricsCollector.generateReport())
        
        report.toString()
    }
}

/**
 * 监控调度器包装器
 * 为现有调度器添加监控功能
 */
public class MonitoredDispatcher <: AdvancedMessageDispatcher {
    private let name: String
    private let baseDispatcher: AdvancedMessageDispatcher
    private let metricsCollector: DispatcherMetricsCollector
    
    public init(name: String, baseDispatcher: AdvancedMessageDispatcher, 
                metricsCollector: DispatcherMetricsCollector) {
        this.name = name
        this.baseDispatcher = baseDispatcher
        this.metricsCollector = metricsCollector
    }
    
    public func dispatch(envelope: Envelope, actorRef: ActorRef): Unit {
        let startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        
        try {
            baseDispatcher.dispatch(envelope, actorRef)
            
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            metricsCollector.recordDispatch(name, endTime - startTime, Priority.Normal, true)
            
        } catch (e: Exception) {
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            metricsCollector.recordDispatch(name, endTime - startTime, Priority.Normal, false)
            throw e
        }
    }
    
    public func dispatchWithPriority(envelope: Envelope, actorRef: ActorRef, priority: Priority): Unit {
        let startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        
        try {
            baseDispatcher.dispatchWithPriority(envelope, actorRef, priority)
            
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            metricsCollector.recordDispatch(name, endTime - startTime, priority, true)
            
        } catch (e: Exception) {
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            metricsCollector.recordDispatch(name, endTime - startTime, priority, false)
            throw e
        }
    }
    
    public func dispatchBatch(envelopes: Array<Envelope>, actorRefs: Array<ActorRef>): Unit {
        let startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        let batchSize = Int64(envelopes.size)
        
        try {
            baseDispatcher.dispatchBatch(envelopes, actorRefs)
            
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            metricsCollector.recordBatchDispatch(name, endTime - startTime, batchSize, true)
            
        } catch (e: Exception) {
            let endTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
            metricsCollector.recordBatchDispatch(name, endTime - startTime, batchSize, false)
            throw e
        }
    }
    
    public func start(): Unit {
        baseDispatcher.start()
    }
    
    public func stop(): Unit {
        baseDispatcher.stop()
    }
    
    public func getConfig(): DispatcherConfig {
        baseDispatcher.getConfig()
    }
    
    public func getPerformanceStats(): String {
        baseDispatcher.getPerformanceStats()
    }
    
    public func isRunning(): Bool {
        baseDispatcher.isRunning()
    }
}
