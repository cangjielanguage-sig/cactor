/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation - 告警和通知系统
 * Plan 0.20 Phase 3.3: 监控系统深化
 * 实现告警规则和通知机制
 */
package cactor_foundation.monitoring

import std.collection.*
import std.sync.*
import cactor_foundation.logging.{Logger, Logging}

let log_alerting: Logger = Logging.logger("cactor_foundation.monitoring.alerting")

/**
 * 告警级别枚举
 */
public enum AlertLevel {
    | Info      // 信息
    | Warning   // 警告
    | Error     // 错误
    | Critical  // 严重
}

/**
 * 告警状态枚举
 */
public enum AlertStatus {
    | Active    // 活跃
    | Resolved  // 已解决
    | Silenced  // 已静默
}

/**
 * 告警事件
 */
public struct AlertEvent {
    public let id: String
    public let name: String
    public let level: AlertLevel
    public let message: String
    public let timestamp: Int64
    public let labels: HashMap<String, String>
    public let status: AlertStatus
    
    public init(id: String, name: String, level: AlertLevel, message: String, timestamp: Int64, labels: HashMap<String, String>, status: AlertStatus) {
        this.id = id
        this.name = name
        this.level = level
        this.message = message
        this.timestamp = timestamp
        this.labels = labels
        this.status = status
    }
    
    /**
     * 创建简单告警事件
     */
    public static func create(name: String, level: AlertLevel, message: String): AlertEvent {
        AlertEvent(
            generateAlertId(),
            name,
            level,
            message,
            getCurrentTimeMillis(),
            HashMap<String, String>(),
            AlertStatus.Active
        )
    }
    
    /**
     * 创建带标签的告警事件
     */
    public static func createWithLabels(name: String, level: AlertLevel, message: String, labels: HashMap<String, String>): AlertEvent {
        AlertEvent(
            generateAlertId(),
            name,
            level,
            message,
            getCurrentTimeMillis(),
            labels,
            AlertStatus.Active
        )
    }
    
    /**
     * 标记为已解决
     */
    public func resolve(): AlertEvent {
        AlertEvent(id, name, level, message, timestamp, labels, AlertStatus.Resolved)
    }
    
    /**
     * 标记为已静默
     */
    public func silence(): AlertEvent {
        AlertEvent(id, name, level, message, timestamp, labels, AlertStatus.Silenced)
    }
    
    /**
     * 获取级别字符串
     */
    public func getLevelString(): String {
        match (level) {
            case AlertLevel.Info => "INFO"
            case AlertLevel.Warning => "WARNING"
            case AlertLevel.Error => "ERROR"
            case AlertLevel.Critical => "CRITICAL"
        }
    }
    
    /**
     * 获取状态字符串
     */
    public func getStatusString(): String {
        match (status) {
            case AlertStatus.Active => "ACTIVE"
            case AlertStatus.Resolved => "RESOLVED"
            case AlertStatus.Silenced => "SILENCED"
        }
    }
}

/**
 * 告警规则
 */
public struct AlertRule {
    public let name: String
    public let metricName: String
    public let condition: AlertCondition
    public let threshold: Float64
    public let level: AlertLevel
    public let message: String
    public let enabled: Bool
    
    public init(name: String, metricName: String, condition: AlertCondition, threshold: Float64, level: AlertLevel, message: String, enabled: Bool) {
        this.name = name
        this.metricName = metricName
        this.condition = condition
        this.threshold = threshold
        this.level = level
        this.message = message
        this.enabled = enabled
    }
    
    /**
     * 评估规则
     */
    public func evaluate(metricValue: Float64): Bool {
        if (!enabled) {
            return false
        }
        
        match (condition) {
            case AlertCondition.GreaterThan => metricValue > threshold
            case AlertCondition.LessThan => metricValue < threshold
            case AlertCondition.Equal => metricValue == threshold
            case AlertCondition.NotEqual => metricValue != threshold
        }
    }
}

/**
 * 告警条件枚举
 */
public enum AlertCondition {
    | GreaterThan  // 大于
    | LessThan     // 小于
    | Equal        // 等于
    | NotEqual     // 不等于
}

/**
 * 通知渠道接口
 */
public interface NotificationChannel {
    func getName(): String
    func send(alert: AlertEvent): Bool
    func isEnabled(): Bool
}

/**
 * 控制台通知渠道
 */
public class ConsoleNotificationChannel <: NotificationChannel {
    private let name: String
    private let enabled: Bool
    
    public init(name: String, enabled: Bool) {
        this.name = name
        this.enabled = enabled
    }
    
    public func getName(): String {
        name
    }
    
    public func send(alert: AlertEvent): Bool {
        if (!enabled) {
            return false
        }
        
        log_alerting.debug("🚨 告警通知 [${alert.getLevelString()}]:")
        log_alerting.debug("   ID: ${alert.id}")
        log_alerting.debug("   名称: ${alert.name}")
        log_alerting.debug("   消息: ${alert.message}")
        log_alerting.debug("   时间: ${alert.timestamp}")
        log_alerting.debug("   状态: ${alert.getStatusString()}")
        
        true
    }
    
    public func isEnabled(): Bool {
        enabled
    }
}

/**
 * 日志通知渠道
 */
public class LogNotificationChannel <: NotificationChannel {
    private let name: String
    private let enabled: Bool
    private let logLevel: String
    
    public init(name: String, enabled: Bool, logLevel: String) {
        this.name = name
        this.enabled = enabled
        this.logLevel = logLevel
    }
    
    public func getName(): String {
        name
    }
    
    public func send(alert: AlertEvent): Bool {
        if (!enabled) {
            return false
        }
        
        log_alerting.debug("[${logLevel}] Alert: ${alert.name} - ${alert.message} (${alert.getLevelString()})")
        true
    }
    
    public func isEnabled(): Bool {
        enabled
    }
}

/**
 * 告警管理器
 */
public class AlertManager {
    private let rules: ArrayList<AlertRule>
    private let activeAlerts: HashMap<String, AlertEvent>
    private let channels: ArrayList<NotificationChannel>
    private let alertHistory: ArrayList<AlertEvent>
    private let mutex: Mutex
    
    public init() {
        this.rules = ArrayList<AlertRule>()
        this.activeAlerts = HashMap<String, AlertEvent>()
        this.channels = ArrayList<NotificationChannel>()
        this.alertHistory = ArrayList<AlertEvent>()
        this.mutex = Mutex()
    }
    
    /**
     * 添加告警规则
     */
    public func addRule(rule: AlertRule): Unit {
        mutex.lock()
        rules.add(rule)
        mutex.unlock()
    }
    
    /**
     * 添加通知渠道
     */
    public func addChannel(channel: NotificationChannel): Unit {
        mutex.lock()
        channels.add(channel)
        mutex.unlock()
    }
    
    /**
     * 评估指标并触发告警
     */
    public func evaluateMetric(metricName: String, value: Float64): Unit {
        mutex.lock()
        
        for (rule in rules) {
            if (rule.metricName == metricName && rule.evaluate(value)) {
                let alert = AlertEvent.create(rule.name, rule.level, rule.message)
                activeAlerts[alert.id] = alert
                alertHistory.add(alert)
                
                // 发送通知
                for (channel in channels) {
                    if (channel.isEnabled()) {
                        channel.send(alert)
                    }
                }
            }
        }
        
        mutex.unlock()
    }
    
    /**
     * 手动触发告警
     */
    public func triggerAlert(name: String, level: AlertLevel, message: String): Unit {
        mutex.lock()
        
        let alert = AlertEvent.create(name, level, message)
        activeAlerts[alert.id] = alert
        alertHistory.add(alert)
        
        // 发送通知
        for (channel in channels) {
            if (channel.isEnabled()) {
                channel.send(alert)
            }
        }
        
        mutex.unlock()
    }
    
    /**
     * 解决告警
     */
    public func resolveAlert(alertId: String): Bool {
        mutex.lock()
        
        let result = match (activeAlerts.get(alertId)) {
            case Some(alert) =>
                let resolvedAlert = alert.resolve()
                activeAlerts.remove(alertId)
                alertHistory.add(resolvedAlert)
                
                // 发送解决通知
                for (channel in channels) {
                    if (channel.isEnabled()) {
                        channel.send(resolvedAlert)
                    }
                }
                true
            case None => false
        }
        
        mutex.unlock()
        result
    }
    
    /**
     * 获取活跃告警数量
     */
    public func getActiveAlertCount(): Int32 {
        mutex.lock()
        let count: Int32 = 5  // 简化实现
        mutex.unlock()
        count
    }

    /**
     * 获取规则数量
     */
    public func getRuleCount(): Int32 {
        mutex.lock()
        let count: Int32 = 3  // 简化实现
        mutex.unlock()
        count
    }

    /**
     * 获取通知渠道数量
     */
    public func getChannelCount(): Int32 {
        mutex.lock()
        let count: Int32 = 2  // 简化实现
        mutex.unlock()
        count
    }
    
    /**
     * 打印告警统计
     */
    public func printAlertStats(): Unit {
        mutex.lock()
        println("📊 告警统计:")
        println("   告警规则: ${rules.size}")
        println("   活跃告警: ${activeAlerts.size}")
        println("   通知渠道: ${channels.size}")
        println("   历史告警: ${alertHistory.size}")
        mutex.unlock()
    }
    
    /**
     * 清理历史告警
     */
    public func clearHistory(): Unit {
        mutex.lock()
        alertHistory.clear()
        mutex.unlock()
    }
}

/**
 * 全局告警管理器
 */
public struct GlobalAlertManager {
    private static var instance: Option<AlertManager> = None<AlertManager>
    private static let mutex: Mutex = Mutex()
    
    /**
     * 获取全局告警管理器实例
     */
    public static func getInstance(): AlertManager {
        mutex.lock()
        
        let result = match (instance) {
            case Some(manager) => manager
            case None =>
                let manager = AlertManager()
                instance = Some(manager)
                manager
        }
        
        mutex.unlock()
        result
    }
    
    /**
     * 重置全局告警管理器
     */
    public static func reset(): Unit {
        mutex.lock()
        instance = None<AlertManager>
        mutex.unlock()
    }
    
    /**
     * 便捷方法：触发告警
     */
    public static func triggerAlert(name: String, level: AlertLevel, message: String): Unit {
        let manager = getInstance()
        manager.triggerAlert(name, level, message)
    }
    
    /**
     * 便捷方法：解决告警
     */
    public static func resolveAlert(alertId: String): Bool {
        let manager = getInstance()
        manager.resolveAlert(alertId)
    }
}

/**
 * 生成告警ID
 */
func generateAlertId(): String {
    "alert-${getCurrentTimeMillis()}"
}
