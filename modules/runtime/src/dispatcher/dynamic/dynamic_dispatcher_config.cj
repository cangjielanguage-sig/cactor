/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 动态调度器配置
 * 支持运行时动态调整调度器参数
 */
package cactor_runtime.dispatcher.dynamic

import std.sync.*
import std.collection.*
import std.collection.concurrent.*
import cactor_core.config.{DispatcherConfig, ExecutorConfig}

/**
 * 动态调度器配置管理器
 * 支持运行时动态调整调度器参数
 */
public class DynamicDispatcherConfigManager {
    private let configs: ConcurrentHashMap<String, DynamicDispatcherConfig>
    private let listeners: ConcurrentHashMap<String, Array<ConfigChangeListener>>
    private let isRunning: AtomicBool
    private let configVersion: AtomicInt64
    
    public init() {
        this.configs = ConcurrentHashMap<String, DynamicDispatcherConfig>()
        this.listeners = ConcurrentHashMap<String, Array<ConfigChangeListener>>()
        this.isRunning = AtomicBool(false)
        this.configVersion = AtomicInt64(0)
    }
    
    /**
     * 启动配置管理器
     */
    public func start(): Unit {
        if (isRunning.compareAndSwap(false, true)) {
            println("[DYNAMIC-CONFIG] 动态调度器配置管理器启动")
        }
    }
    
    /**
     * 停止配置管理器
     */
    public func stop(): Unit {
        if (isRunning.compareAndSwap(true, false)) {
            println("[DYNAMIC-CONFIG] 动态调度器配置管理器停止")
        }
    }
    
    /**
     * 注册动态配置
     */
    public func registerConfig(name: String, config: DynamicDispatcherConfig): Unit {
        configs.add(name, config)
        configVersion.fetchAdd(1)
        println("[DYNAMIC-CONFIG] 注册动态配置: ${name}")
    }
    
    /**
     * 更新配置参数
     */
    public func updateConfig(name: String, updates: ConfigUpdate): Bool {
        match (configs.get(name)) {
            case Some(config) =>
                let oldConfig = config.getCurrentConfig()
                config.applyUpdate(updates)
                let newConfig = config.getCurrentConfig()
                
                // 通知监听器
                notifyConfigChange(name, oldConfig, newConfig)
                configVersion.fetchAdd(1)
                
                println("[DYNAMIC-CONFIG] 配置已更新: ${name}")
                true
            case None =>
                println("[DYNAMIC-CONFIG] 配置不存在: ${name}")
                false
        }
    }
    
    /**
     * 获取当前配置
     */
    public func getConfig(name: String): Option<DispatcherConfig> {
        match (configs.get(name)) {
            case Some(config) => Some(config.getCurrentConfig())
            case None => None<DispatcherConfig>
        }
    }
    
    /**
     * 添加配置变更监听器
     */
    public func addConfigChangeListener(name: String, listener: ConfigChangeListener): Unit {
        match (listeners.get(name)) {
            case Some(listenerArray) =>
                let newArray = Array<ConfigChangeListener>(Int64(listenerArray.size) + 1, { i =>
                    if (i < Int64(listenerArray.size)) {
                        listenerArray[i]
                    } else {
                        listener
                    }
                })
                listeners.add(name, newArray)
            case None =>
                let newArray = Array<ConfigChangeListener>(1, { i => listener })
                listeners.add(name, newArray)
        }
    }
    
    /**
     * 通知配置变更
     */
    private func notifyConfigChange(name: String, oldConfig: DispatcherConfig, newConfig: DispatcherConfig): Unit {
        match (listeners.get(name)) {
            case Some(listenerArray) =>
                for (listener in listenerArray) {
                    try {
                        listener.onConfigChanged(name, oldConfig, newConfig)
                    } catch (e: Exception) {
                        println("[DYNAMIC-CONFIG] 监听器通知失败: ${e.toString()}")
                    }
                }
            case None => ()
        }
    }
    
    /**
     * 获取所有配置名称
     */
    public func getAllConfigNames(): Array<String> {
        let names = ArrayList<String>()
        for ((name, _) in configs) {
            names.add(name)
        }
        names.toArray()
    }
    
    /**
     * 获取配置版本号
     */
    public func getConfigVersion(): Int64 {
        configVersion.load()
    }
}

/**
 * 动态调度器配置
 * 支持运行时修改的调度器配置
 */
public class DynamicDispatcherConfig {
    private let baseConfig: DispatcherConfig
    private var currentThroughput: AtomicInt32
    private var currentDeadlineMs: AtomicInt64
    private var currentCorePoolSize: AtomicInt32
    private var currentMaxPoolSize: AtomicInt32
    private let mutex: Mutex

    public init(baseConfig: DispatcherConfig) {
        this.baseConfig = baseConfig
        this.currentThroughput = AtomicInt32(baseConfig.throughput)
        this.currentDeadlineMs = AtomicInt64(baseConfig.throughputDeadlineTime.toMilliseconds())
        this.currentCorePoolSize = AtomicInt32(baseConfig.executorConfig.corePoolSize)
        this.currentMaxPoolSize = AtomicInt32(baseConfig.executorConfig.maxPoolSize)
        this.mutex = Mutex()
    }
    
    /**
     * 应用配置更新
     */
    public func applyUpdate(update: ConfigUpdate): Unit {
        mutex.lock()
        try {
            match (update.throughput) {
                case Some(value) => currentThroughput.store(value)
                case None => ()
            }
            
            match (update.deadlineMs) {
                case Some(value) => currentDeadlineMs.store(value)
                case None => ()
            }
            
            match (update.corePoolSize) {
                case Some(value) => currentCorePoolSize.store(value)
                case None => ()
            }
            
            match (update.maxPoolSize) {
                case Some(value) => currentMaxPoolSize.store(value)
                case None => ()
            }
        } finally {
            mutex.unlock()
        }
    }
    
    /**
     * 获取当前配置
     */
    public func getCurrentConfig(): DispatcherConfig {
        let currentExecutorConfig = ExecutorConfig(
            currentCorePoolSize.load(),
            currentMaxPoolSize.load(),
            baseConfig.executorConfig.keepAliveTime,
            baseConfig.executorConfig.queueCapacity
        )
        
        DispatcherConfig(
            baseConfig.dispatcherType,
            currentThroughput.load(),
            Duration.millisecond * currentDeadlineMs.load(),
            currentExecutorConfig,
            baseConfig.name
        )
    }
    
    /**
     * 重置为基础配置
     */
    public func resetToBase(): Unit {
        mutex.lock()
        try {
            currentThroughput.store(baseConfig.throughput)
            currentDeadlineMs.store(baseConfig.throughputDeadlineTime.toMilliseconds())
            currentCorePoolSize.store(baseConfig.executorConfig.corePoolSize)
            currentMaxPoolSize.store(baseConfig.executorConfig.maxPoolSize)
        } finally {
            mutex.unlock()
        }
    }
}

/**
 * 配置更新结构
 */
public struct ConfigUpdate {
    public let throughput: Option<Int32>
    public let deadlineMs: Option<Int64>
    public let corePoolSize: Option<Int32>
    public let maxPoolSize: Option<Int32>
    
    public init(throughput: Option<Int32>, deadlineMs: Option<Int64>, 
                corePoolSize: Option<Int32>, maxPoolSize: Option<Int32>) {
        this.throughput = throughput
        this.deadlineMs = deadlineMs
        this.corePoolSize = corePoolSize
        this.maxPoolSize = maxPoolSize
    }
    
    /**
     * 创建吞吐量更新
     */
    public static func throughputUpdate(value: Int32): ConfigUpdate {
        ConfigUpdate(Some(value), None<Int64>, None<Int32>, None<Int32>)
    }
    
    /**
     * 创建线程池大小更新
     */
    public static func poolSizeUpdate(coreSize: Int32, maxSize: Int32): ConfigUpdate {
        ConfigUpdate(None<Int32>, None<Int64>, Some(coreSize), Some(maxSize))
    }
    
    /**
     * 创建截止时间更新
     */
    public static func deadlineUpdate(deadlineMs: Int64): ConfigUpdate {
        ConfigUpdate(None<Int32>, Some(deadlineMs), None<Int32>, None<Int32>)
    }
}

/**
 * 配置变更监听器接口
 */
public interface ConfigChangeListener {
    /**
     * 配置变更回调
     */
    func onConfigChanged(name: String, oldConfig: DispatcherConfig, newConfig: DispatcherConfig): Unit
}

/**
 * 默认配置变更监听器
 */
public class DefaultConfigChangeListener <: ConfigChangeListener {
    private let name: String
    
    public init(name: String) {
        this.name = name
    }
    
    public func onConfigChanged(name: String, oldConfig: DispatcherConfig, newConfig: DispatcherConfig): Unit {
        println("[${this.name}] 配置变更通知:")
        println("  调度器: ${name}")
        println("  吞吐量: ${oldConfig.throughput} -> ${newConfig.throughput}")
        println("  截止时间: ${oldConfig.throughputDeadlineTime.toMilliseconds()}ms -> ${newConfig.throughputDeadlineTime.toMilliseconds()}ms")
        println("  核心线程数: ${oldConfig.executorConfig.corePoolSize} -> ${newConfig.executorConfig.corePoolSize}")
        println("  最大线程数: ${oldConfig.executorConfig.maxPoolSize} -> ${newConfig.executorConfig.maxPoolSize}")
    }
}
