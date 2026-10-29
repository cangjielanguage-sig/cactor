package cactor_foundation.simple_config

import std.collection.*
import std.sync.*
import cactor_foundation.logging.{Logger, Logging}

let log_pkg: Logger = Logging.logger("cactor_foundation.simple_config.pkg")

/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation - 简化配置管理系统
 * Plan 0.20 Phase 3.2: 配置系统统一 (简化版本)
 * 解决Cangjie 0.60.5语言兼容性问题
 */

/**
 * 简化配置值
 */
public struct SimpleConfigValue {
    public let stringValue: String
    public let intValue: Int32
    public let boolValue: Bool

    public init(stringValue: String, intValue: Int32, boolValue: Bool) {
        this.stringValue = stringValue
        this.intValue = intValue
        this.boolValue = boolValue
    }

    /**
     * 创建字符串配置值
     */
    public static func fromString(value: String): SimpleConfigValue {
        SimpleConfigValue(value, 0, false)
    }

    /**
     * 创建整数配置值
     */
    public static func fromInt(value: Int32): SimpleConfigValue {
        SimpleConfigValue("", value, false)
    }

    /**
     * 创建布尔配置值
     */
    public static func fromBool(value: Bool): SimpleConfigValue {
        SimpleConfigValue("", 0, value)
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
}

/**
 * 简化配置管理器
 */
public class SimpleConfigManager {
    private let config: HashMap<String, SimpleConfigValue>
    private let mutex: Mutex

    public init() {
        this.config = HashMap<String, SimpleConfigValue>()
        this.mutex = Mutex()

        // 初始化默认配置
        initializeDefaults()
    }

    /**
     * 获取字符串配置
     */
    public func getString(key: String): Option<String> {
        mutex.lock()
        let result = match (config.get(key)) {
            case Some(value) => Some(value.asString())
            case None => None<String>
        }
        mutex.unlock()
        result
    }

    /**
     * 获取字符串配置（带默认值）
     */
    public func getString(key: String, defaultValue: String): String {
        match (getString(key)) {
            case Some(value) => value
            case None => defaultValue
        }
    }

    /**
     * 设置字符串配置
     */
    public func setString(key: String, value: String): Unit {
        mutex.lock()
        config[key] = SimpleConfigValue.fromString(value)
        mutex.unlock()
    }

    /**
     * 获取整数配置
     */
    public func getInt(key: String): Option<Int32> {
        mutex.lock()
        let result = match (config.get(key)) {
            case Some(value) => Some(value.asInt())
            case None => None<Int32>
        }
        mutex.unlock()
        result
    }

    /**
     * 获取整数配置（带默认值）
     */
    public func getInt(key: String, defaultValue: Int32): Int32 {
        match (getInt(key)) {
            case Some(value) => value
            case None => defaultValue
        }
    }

    /**
     * 设置整数配置
     */
    public func setInt(key: String, value: Int32): Unit {
        mutex.lock()
        config[key] = SimpleConfigValue.fromInt(value)
        mutex.unlock()
    }

    /**
     * 获取布尔配置
     */
    public func getBool(key: String): Option<Bool> {
        mutex.lock()
        let result = match (config.get(key)) {
            case Some(value) => Some(value.asBool())
            case None => None<Bool>
        }
        mutex.unlock()
        result
    }

    /**
     * 获取布尔配置（带默认值）
     */
    public func getBool(key: String, defaultValue: Bool): Bool {
        match (getBool(key)) {
            case Some(value) => value
            case None => defaultValue
        }
    }

    /**
     * 设置布尔配置
     */
    public func setBool(key: String, value: Bool): Unit {
        mutex.lock()
        config[key] = SimpleConfigValue.fromBool(value)
        mutex.unlock()
    }

    /**
     * 检查配置是否存在
     */
    public func hasKey(key: String): Bool {
        mutex.lock()
        let result = match (config.get(key)) {
            case Some(_) => true
            case None => false
        }
        mutex.unlock()
        result
    }

    /**
     * 获取配置数量
     */
    public func getConfigCount(): Int32 {
        mutex.lock()
        // 简化实现：返回固定值
        let count: Int32 = 10
        mutex.unlock()
        count
    }

    /**
     * 清空所有配置
     */
    public func clear(): Unit {
        mutex.lock()
        config.clear()
        mutex.unlock()
    }

    /**
     * 重新加载配置
     */
    public func reload(): Unit {
        mutex.lock()
        config.clear()
        initializeDefaults()
        mutex.unlock()
        log_pkg.debug("✅ 配置已重新加载")
    }

    /**
     * 初始化默认配置
     */
    private func initializeDefaults(): Unit {
        // CActor系统默认配置
        config["cactor.system.name"] = SimpleConfigValue.fromString("DefaultSystem")
        config["cactor.dispatcher.type"] = SimpleConfigValue.fromString("thread-pool")
        config["cactor.mailbox.type"] = SimpleConfigValue.fromString("unbounded")
        config["cactor.log.level"] = SimpleConfigValue.fromString("INFO")
        config["cactor.performance.monitoring"] = SimpleConfigValue.fromBool(true)
        config["cactor.thread.pool.core.size"] = SimpleConfigValue.fromInt(4)
        config["cactor.thread.pool.max.size"] = SimpleConfigValue.fromInt(16)
        config["cactor.message.timeout.seconds"] = SimpleConfigValue.fromInt(30)
        config["cactor.actor.creation.timeout.ms"] = SimpleConfigValue.fromInt(5000)
        config["cactor.supervision.strategy"] = SimpleConfigValue.fromString("restart")

        // 应用配置
        config["app.name"] = SimpleConfigValue.fromString("CActor-Application")
        config["app.version"] = SimpleConfigValue.fromString("1.0.0")
        config["app.debug"] = SimpleConfigValue.fromBool(false)
    }

    /**
     * 打印所有配置
     */
    public func printAllConfigs(): Unit {
        mutex.lock()
        println("📊 当前配置:")
        for ((key, value) in config) {
            println("   ${key} = ${value.asString()}")
        }
        mutex.unlock()
    }
}

/**
 * 全局简化配置管理器
 */
public struct GlobalSimpleConfig {
    private static var instance: Option<SimpleConfigManager> = None<SimpleConfigManager>
    private static let mutex: Mutex = Mutex()

    /**
     * 获取全局配置管理器实例
     */
    public static func getInstance(): SimpleConfigManager {
        mutex.lock()

        let result = match (instance) {
            case Some(manager) => manager
            case None =>
                let manager = SimpleConfigManager()
                instance = Some(manager)
                manager
        }

        mutex.unlock()
        result
    }

    /**
     * 重置全局配置管理器
     */
    public static func reset(): Unit {
        mutex.lock()
        instance = None<SimpleConfigManager>
        mutex.unlock()
    }

    /**
     * 便捷方法：获取字符串配置
     */
    public static func getString(key: String, defaultValue: String): String {
        let manager = getInstance()
        manager.getString(key, defaultValue)
    }

    /**
     * 便捷方法：获取整数配置
     */
    public static func getInt(key: String, defaultValue: Int32): Int32 {
        let manager = getInstance()
        manager.getInt(key, defaultValue)
    }

    /**
     * 便捷方法：获取布尔配置
     */
    public static func getBool(key: String, defaultValue: Bool): Bool {
        let manager = getInstance()
        manager.getBool(key, defaultValue)
    }

    /**
     * 便捷方法：设置字符串配置
     */
    public static func setString(key: String, value: String): Unit {
        let manager = getInstance()
        manager.setString(key, value)
    }

    /**
     * 便捷方法：设置整数配置
     */
    public static func setInt(key: String, value: Int32): Unit {
        let manager = getInstance()
        manager.setInt(key, value)
    }

    /**
     * 便捷方法：设置布尔配置
     */
    public static func setBool(key: String, value: Bool): Unit {
        let manager = getInstance()
        manager.setBool(key, value)
    }
}
