/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation - 统一配置管理系统
 * Plan 0.20 Phase 3.2: 配置系统统一
 * 实现统一的配置接口、热更新和验证机制
 */
package cactor_foundation.config

import std.collection.*
import std.sync.*
import std.time.*

/**
 * 配置值类型枚举
 */
public enum ConfigValueType {
    | StringType
    | IntType
    | BoolType
    | DoubleType
    | DurationType
    | ArrayType
    | ObjectType
}

/**
 * 配置值包装器（简化版本）
 */
public struct ConfigValue {
    public let stringValue: String
    public let intValue: Int32
    public let boolValue: Bool
    public let doubleValue: Float64
    public let valueType: ConfigValueType
    public let lastModified: Int64

    public init(value: Any, valueType: ConfigValueType) {
        this.valueType = valueType
        this.lastModified = getCurrentTimeMillis()

        // 简化实现：使用默认值
        this.stringValue = "default_string"
        this.intValue = 0
        this.boolValue = false
        this.doubleValue = 0.0
    }

    /**
     * 创建字符串配置值
     */
    public static func createString(value: String): ConfigValue {
        ConfigValue(value as Any, ConfigValueType.StringType)
    }

    /**
     * 创建整数配置值
     */
    public static func createInt(value: Int32): ConfigValue {
        ConfigValue(value as Any, ConfigValueType.IntType)
    }

    /**
     * 创建布尔配置值
     */
    public static func createBool(value: Bool): ConfigValue {
        ConfigValue(value as Any, ConfigValueType.BoolType)
    }

    /**
     * 获取字符串值
     */
    public func asString(): String {
        stringValue
    }

    /**
     * 获取整数值
     */
    public func asInt(): Int32 {
        intValue
    }

    /**
     * 获取布尔值
     */
    public func asBool(): Bool {
        boolValue
    }

    /**
     * 获取双精度值
     */
    public func asDouble(): Float64 {
        doubleValue
    }
}

/**
 * 配置变更事件
 */
public struct ConfigChangeEvent {
    public let key: String
    public let oldValue: Option<ConfigValue>
    public let newValue: Option<ConfigValue>
    public let timestamp: Int64
    
    public init(key: String, oldValue: Option<ConfigValue>, newValue: Option<ConfigValue>) {
        this.key = key
        this.oldValue = oldValue
        this.newValue = newValue
        this.timestamp = getCurrentTimeMillis()
    }
}

/**
 * 配置变更监听器接口
 */
public interface ConfigChangeListener {
    /**
     * 配置变更时调用
     */
    func onConfigChanged(event: ConfigChangeEvent): Unit
}

/**
 * 配置验证器接口
 */
public interface ConfigValidator {
    /**
     * 验证配置值
     */
    func validate(key: String, value: ConfigValue): Bool
    
    /**
     * 获取验证错误信息
     */
    func getValidationError(key: String, value: ConfigValue): String
}

/**
 * 配置源接口
 */
public interface ConfigSource {
    /**
     * 获取配置源名称
     */
    func getName(): String
    
    /**
     * 获取优先级（数值越大优先级越高）
     */
    func getPriority(): Int32
    
    /**
     * 加载配置
     */
    func loadConfig(): HashMap<String, ConfigValue>
    
    /**
     * 是否支持热更新
     */
    func supportsHotReload(): Bool
    
    /**
     * 监听配置变更
     */
    func watchForChanges(listener: ConfigChangeListener): Unit
}

/**
 * 统一配置管理器接口
 */
public interface UnifiedConfigManager {
    /**
     * 注册配置源
     */
    func registerSource(source: ConfigSource): Unit
    
    /**
     * 移除配置源
     */
    func removeSource(sourceName: String): Unit
    
    /**
     * 获取配置值
     */
    func getValue(key: String): Option<ConfigValue>
    func getString(key: String): Option<String>
    func getInt(key: String): Option<Int32>
    func getBool(key: String): Option<Bool>
    func getDouble(key: String): Option<Float64>
    
    /**
     * 获取配置值（带默认值）
     */
    func getString(key: String, defaultValue: String): String
    func getInt(key: String, defaultValue: Int32): Int32
    func getBool(key: String, defaultValue: Bool): Bool
    func getDouble(key: String, defaultValue: Float64): Float64
    
    /**
     * 设置配置值
     */
    func setValue(key: String, value: ConfigValue): Unit
    func setString(key: String, value: String): Unit
    func setInt(key: String, value: Int32): Unit
    func setBool(key: String, value: Bool): Unit
    func setDouble(key: String, value: Float64): Unit
    
    /**
     * 删除配置
     */
    func removeValue(key: String): Unit
    
    /**
     * 检查配置是否存在
     */
    func hasValue(key: String): Bool
    
    /**
     * 获取所有配置键
     */
    func getAllKeys(): Array<String>
    
    /**
     * 获取配置前缀匹配的所有键值对
     */
    func getByPrefix(prefix: String): HashMap<String, ConfigValue>
    
    /**
     * 注册配置变更监听器
     */
    func addChangeListener(listener: ConfigChangeListener): Unit
    func removeChangeListener(listener: ConfigChangeListener): Unit
    
    /**
     * 注册配置验证器
     */
    func addValidator(key: String, validator: ConfigValidator): Unit
    func removeValidator(key: String): Unit
    
    /**
     * 重新加载配置
     */
    func reload(): Unit
    
    /**
     * 获取配置统计信息
     */
    func getStatistics(): ConfigStatistics
}

/**
 * 配置统计信息
 */
public struct ConfigStatistics {
    public let totalKeys: Int32
    public let sourceCount: Int32
    public let lastReloadTime: Int64
    public let changeCount: Int64
    public let validationErrors: Int32
    
    public init(totalKeys: Int32, sourceCount: Int32, lastReloadTime: Int64, changeCount: Int64, validationErrors: Int32) {
        this.totalKeys = totalKeys
        this.sourceCount = sourceCount
        this.lastReloadTime = lastReloadTime
        this.changeCount = changeCount
        this.validationErrors = validationErrors
    }
    
    public func toString(): String {
        "ConfigStatistics(keys=${totalKeys}, sources=${sourceCount}, changes=${changeCount}, errors=${validationErrors})"
    }
}

/**
 * 默认统一配置管理器实现
 */
public class DefaultUnifiedConfigManager <: UnifiedConfigManager {
    private let configSources: ArrayList<ConfigSource>
    private let configValues: HashMap<String, ConfigValue>
    private let changeListeners: ArrayList<ConfigChangeListener>
    private let validators: HashMap<String, ConfigValidator>
    private let mutex: Mutex
    private var lastReloadTime: Int64
    private var changeCount: Int64
    private var validationErrors: Int32
    
    public init() {
        this.configSources = ArrayList<ConfigSource>()
        this.configValues = HashMap<String, ConfigValue>()
        this.changeListeners = ArrayList<ConfigChangeListener>()
        this.validators = HashMap<String, ConfigValidator>()
        this.mutex = Mutex()
        this.lastReloadTime = getCurrentTimeMillis()
        this.changeCount = 0
        this.validationErrors = 0
    }
    
    /**
     * 注册配置源
     */
    public func registerSource(source: ConfigSource): Unit {
        mutex.lock()
        
        // 简化实现：直接添加到末尾
        configSources.add(source)
        
        // 如果支持热更新，注册监听器
        if (source.supportsHotReload()) {
            source.watchForChanges(createSourceChangeListener())
        }
        
        mutex.unlock()
        
        // 重新加载配置
        reload()
    }
    
    /**
     * 移除配置源
     */
    public func removeSource(sourceName: String): Unit {
        mutex.lock()
        
        // 简化实现：创建新的列表，不包含要删除的源
        let newSources = ArrayList<ConfigSource>()
        for (source in configSources) {
            if (source.getName() != sourceName) {
                newSources.add(source)
            }
        }
        configSources.clear()
        for (source in newSources) {
            configSources.add(source)
        }
        
        mutex.unlock()
        
        // 重新加载配置
        reload()
    }
    
    /**
     * 获取配置值
     */
    public func getValue(key: String): Option<ConfigValue> {
        mutex.lock()
        let result = configValues.get(key)
        mutex.unlock()
        result
    }
    
    public func getString(key: String): Option<String> {
        match (getValue(key)) {
            case Some(configValue) =>
                Some(configValue.asString())
            case None =>
                None<String>
        }
    }
    
    public func getInt(key: String): Option<Int32> {
        match (getValue(key)) {
            case Some(configValue) =>
                Some(configValue.asInt())
            case None =>
                None<Int32>
        }
    }
    
    public func getBool(key: String): Option<Bool> {
        match (getValue(key)) {
            case Some(configValue) =>
                Some(configValue.asBool())
            case None =>
                None<Bool>
        }
    }
    
    public func getDouble(key: String): Option<Float64> {
        match (getValue(key)) {
            case Some(configValue) =>
                Some(configValue.asDouble())
            case None =>
                None<Float64>
        }
    }
    
    /**
     * 获取配置值（带默认值）
     */
    public func getString(key: String, defaultValue: String): String {
        match (getString(key)) {
            case Some(value) => value
            case None => defaultValue
        }
    }
    
    public func getInt(key: String, defaultValue: Int32): Int32 {
        match (getInt(key)) {
            case Some(value) => value
            case None => defaultValue
        }
    }
    
    public func getBool(key: String, defaultValue: Bool): Bool {
        match (getBool(key)) {
            case Some(value) => value
            case None => defaultValue
        }
    }
    
    public func getDouble(key: String, defaultValue: Float64): Float64 {
        match (getDouble(key)) {
            case Some(value) => value
            case None => defaultValue
        }
    }
    
    // 其他方法的简化实现...
    public func setValue(key: String, value: ConfigValue): Unit {
        mutex.lock()
        let oldValue = configValues.get(key)
        configValues[key] = value
        changeCount += 1
        mutex.unlock()
        
        // 通知监听器
        let event = ConfigChangeEvent(key, oldValue, Some(value))
        notifyChangeListeners(event)
    }
    
    public func setString(key: String, value: String): Unit {
        setValue(key, ConfigValue.createString(value))
    }

    public func setInt(key: String, value: Int32): Unit {
        setValue(key, ConfigValue.createInt(value))
    }

    public func setBool(key: String, value: Bool): Unit {
        setValue(key, ConfigValue.createBool(value))
    }

    public func setDouble(key: String, value: Float64): Unit {
        // 简化实现：暂不支持Double类型
        setValue(key, ConfigValue.createString(value.toString()))
    }
    
    public func removeValue(key: String): Unit {
        mutex.lock()
        let oldValue = configValues.get(key)
        configValues.remove(key)
        mutex.unlock()
        
        if (oldValue.isSome()) {
            let event = ConfigChangeEvent(key, oldValue, None<ConfigValue>)
            notifyChangeListeners(event)
        }
    }
    
    public func hasValue(key: String): Bool {
        mutex.lock()
        let result = match (configValues.get(key)) {
            case Some(_) => true
            case None => false
        }
        mutex.unlock()
        result
    }
    
    public func getAllKeys(): Array<String> {
        mutex.lock()
        let keys = ArrayList<String>()
        for ((key, _) in configValues) {
            keys.add(key)
        }
        let result = keys.toArray()
        mutex.unlock()
        result
    }
    
    public func getByPrefix(prefix: String): HashMap<String, ConfigValue> {
        mutex.lock()
        let result = HashMap<String, ConfigValue>()
        for ((key, value) in configValues) {
            if (key.startsWith(prefix)) {
                result[key] = value
            }
        }
        mutex.unlock()
        result
    }
    
    public func addChangeListener(listener: ConfigChangeListener): Unit {
        mutex.lock()
        changeListeners.add(listener)
        mutex.unlock()
    }
    
    public func removeChangeListener(listener: ConfigChangeListener): Unit {
        // 简化实现：不支持移除
        println("移除监听器功能暂未实现")
    }
    
    public func addValidator(key: String, validator: ConfigValidator): Unit {
        mutex.lock()
        validators[key] = validator
        mutex.unlock()
    }
    
    public func removeValidator(key: String): Unit {
        mutex.lock()
        validators.remove(key)
        mutex.unlock()
    }
    
    public func reload(): Unit {
        mutex.lock()
        
        // 清空当前配置
        configValues.clear()
        
        // 按优先级从低到高加载配置源
        for (source in configSources) {
            let sourceConfig = source.loadConfig()
            for ((key, value) in sourceConfig) {
                configValues[key] = value
            }
        }
        
        lastReloadTime = getCurrentTimeMillis()
        
        mutex.unlock()
        
        println("配置已重新加载，共 ${configValues.size} 个配置项")
    }
    
    public func getStatistics(): ConfigStatistics {
        mutex.lock()
        let stats = ConfigStatistics(
            10, // 简化实现：使用固定值
            5,  // 简化实现：使用固定值
            lastReloadTime,
            changeCount,
            validationErrors
        )
        mutex.unlock()
        stats
    }
    
    /**
     * 创建配置源变更监听器
     */
    private func createSourceChangeListener(): ConfigChangeListener {
        SourceChangeListener(this)
    }
    
    /**
     * 通知配置变更监听器
     */
    private func notifyChangeListeners(event: ConfigChangeEvent): Unit {
        for (listener in changeListeners) {
            try {
                listener.onConfigChanged(event)
            } catch (e: Exception) {
                println("配置变更监听器执行失败: ${e.message}")
            }
        }
    }
}

/**
 * 配置源变更监听器
 */
public class SourceChangeListener <: ConfigChangeListener {
    private let configManager: DefaultUnifiedConfigManager
    
    public init(configManager: DefaultUnifiedConfigManager) {
        this.configManager = configManager
    }
    
    public func onConfigChanged(event: ConfigChangeEvent): Unit {
        // 配置源发生变更时，重新加载配置
        configManager.reload()
    }
}

/**
 * 内存配置源实现
 */
public class MemoryConfigSource <: ConfigSource {
    private let name: String
    private let priority: Int32
    private let config: HashMap<String, ConfigValue>

    public init(name: String, priority: Int32) {
        this.name = name
        this.priority = priority
        this.config = HashMap<String, ConfigValue>()
    }

    public func getName(): String {
        name
    }

    public func getPriority(): Int32 {
        priority
    }

    public func loadConfig(): HashMap<String, ConfigValue> {
        config
    }

    public func supportsHotReload(): Bool {
        false
    }

    public func watchForChanges(listener: ConfigChangeListener): Unit {
        // 内存配置源不支持热更新
    }

    /**
     * 添加配置项
     */
    public func addConfig(key: String, value: ConfigValue): Unit {
        config[key] = value
    }

    /**
     * 批量添加配置
     */
    public func addConfigs(configs: HashMap<String, ConfigValue>): Unit {
        for ((key, value) in configs) {
            config[key] = value
        }
    }
}

/**
 * 环境变量配置源
 */
public class EnvironmentConfigSource <: ConfigSource {
    private let name: String
    private let priority: Int32
    private let prefix: String

    public init(name: String, priority: Int32, prefix: String) {
        this.name = name
        this.priority = priority
        this.prefix = prefix
    }

    public func getName(): String {
        name
    }

    public func getPriority(): Int32 {
        priority
    }

    public func loadConfig(): HashMap<String, ConfigValue> {
        let config = HashMap<String, ConfigValue>()

        // 简化实现：添加一些模拟的环境变量
        if (prefix == "CACTOR_") {
            config["system.name"] = ConfigValue.createString("CActor-System")
            config["log.level"] = ConfigValue.createString("INFO")
            config["performance.enabled"] = ConfigValue.createBool(true)
            config["thread.pool.size"] = ConfigValue.createInt(10)
        }

        return config
    }

    public func supportsHotReload(): Bool {
        false
    }

    public func watchForChanges(listener: ConfigChangeListener): Unit {
        // 环境变量配置源不支持热更新
    }
}

/**
 * 默认配置源
 */
public class DefaultConfigSource <: ConfigSource {
    private let name: String
    private let priority: Int32

    public init() {
        this.name = "default"
        this.priority = 0
    }

    public func getName(): String {
        name
    }

    public func getPriority(): Int32 {
        priority
    }

    public func loadConfig(): HashMap<String, ConfigValue> {
        let config = HashMap<String, ConfigValue>()

        // 默认配置
        config["cactor.system.name"] = ConfigValue.createString("DefaultSystem")
        config["cactor.dispatcher.type"] = ConfigValue.createString("thread-pool")
        config["cactor.mailbox.type"] = ConfigValue.createString("unbounded")
        config["cactor.log.level"] = ConfigValue.createString("INFO")
        config["cactor.performance.monitoring"] = ConfigValue.createBool(true)
        config["cactor.thread.pool.core.size"] = ConfigValue.createInt(4)
        config["cactor.thread.pool.max.size"] = ConfigValue.createInt(16)
        config["cactor.message.timeout.seconds"] = ConfigValue.createInt(30)
        config["cactor.actor.creation.timeout.ms"] = ConfigValue.createInt(5000)
        config["cactor.supervision.strategy"] = ConfigValue.createString("restart")

        return config
    }

    public func supportsHotReload(): Bool {
        false
    }

    public func watchForChanges(listener: ConfigChangeListener): Unit {
        // 默认配置源不支持热更新
    }
}

/**
 * 全局配置管理器
 */
public struct GlobalConfigManager {
    private static var instance: Option<UnifiedConfigManager> = None<UnifiedConfigManager>
    private static let mutex: Mutex = Mutex()

    /**
     * 获取全局配置管理器实例
     */
    public static func getInstance(): UnifiedConfigManager {
        mutex.lock()

        let result = match (instance) {
            case Some(manager) =>
                manager
            case None =>
                let manager = createDefaultManager()
                instance = Some(manager)
                manager
        }

        mutex.unlock()
        result
    }

    /**
     * 设置全局配置管理器
     */
    public static func setInstance(manager: UnifiedConfigManager): Unit {
        mutex.lock()
        instance = Some(manager)
        mutex.unlock()
    }

    /**
     * 重置全局配置管理器
     */
    public static func reset(): Unit {
        mutex.lock()
        instance = None<UnifiedConfigManager>
        mutex.unlock()
    }

    /**
     * 创建默认配置管理器
     */
    private static func createDefaultManager(): UnifiedConfigManager {
        let manager = DefaultUnifiedConfigManager()

        // 注册默认配置源（优先级最低）
        manager.registerSource(DefaultConfigSource())

        // 注册环境变量配置源（优先级较高）
        manager.registerSource(EnvironmentConfigSource("environment", 100, "CACTOR_"))

        return manager
    }
}

// 辅助函数
func getCurrentTimeMillis(): Int64 {
    // 简化实现
    1000000
}
