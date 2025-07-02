/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 升级监督策略
 * 实现失败升级和层次化监督
 */
package cactor.core.supervision

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*
import cactor.core.actor.{ActorRef, ActorLifecycleState}

/**
 * 升级监督策略
 * 实现失败升级到父级监督者
 */
public class EscalationSupervisionStrategy <: SupervisionStrategy {
    private let baseStrategy: SupervisionStrategy
    private let escalationConfig: EscalationConfig
    private let escalationHistory: ConcurrentHashMap<String, EscalationInfo>
    
    public init(baseStrategy: SupervisionStrategy, escalationConfig: EscalationConfig) {
        this.baseStrategy = baseStrategy
        this.escalationConfig = escalationConfig
        this.escalationHistory = ConcurrentHashMap<String, EscalationInfo>()
    }
    
    public func decide(failure: ActorFailure): SupervisionDirective {
        let actorKey = failure.actor.path.toString()
        let escalationInfo = getOrCreateEscalationInfo(actorKey, failure.actor)
        
        // 检查是否应该升级
        if (shouldEscalate(escalationInfo, failure)) {
            // 记录升级
            escalationInfo.recordEscalation()
            
            println("[ESCALATION] Actor ${actorKey} 失败升级到父级监督者")
            println("[ESCALATION] 升级原因: ${failure.cause.toString()}")
            println("[ESCALATION] 升级次数: ${escalationInfo.getEscalationCount()}")
            
            SupervisionDirective.Escalate
        } else {
            // 使用基础策略
            let directive = baseStrategy.decide(failure)
            
            // 记录失败
            escalationInfo.recordFailure(failure)
            
            directive
        }
    }
    
    private func shouldEscalate(escalationInfo: EscalationInfo, failure: ActorFailure): Bool {
        // 检查升级条件
        let failureCount = escalationInfo.getFailureCount()
        let escalationCount = escalationInfo.getEscalationCount()
        let timeSinceLastEscalation = escalationInfo.getTimeSinceLastEscalation()
        
        // 条件1: 失败次数超过阈值
        if (failureCount >= escalationConfig.failureThreshold) {
            return true
        }
        
        // 条件2: 连续升级次数未超过限制
        if (escalationCount >= escalationConfig.maxEscalations) {
            return false
        }
        
        // 条件3: 距离上次升级时间足够长
        if (timeSinceLastEscalation < escalationConfig.escalationCooldown.toMilliseconds()) {
            return false
        }
        
        // 条件4: 检查异常类型是否需要升级
        if (escalationConfig.shouldEscalateException(failure.cause)) {
            return true
        }
        
        false
    }
    
    private func getOrCreateEscalationInfo(actorKey: String, actor: ActorRef): EscalationInfo {
        match (escalationHistory.get(actorKey)) {
            case Some(info) => info
            case None =>
                let newInfo = EscalationInfo(actor)
                escalationHistory.add(actorKey, newInfo)
                newInfo
        }
    }
    
    /**
     * 重置Actor的升级信息
     */
    public func resetEscalation(actor: ActorRef): Unit {
        let actorKey = actor.path.toString()
        match (escalationHistory.get(actorKey)) {
            case Some(info) => info.reset()
            case None => ()
        }
    }
    
    /**
     * 获取Actor的升级统计
     */
    public func getEscalationStats(actor: ActorRef): Option<EscalationStats> {
        let actorKey = actor.path.toString()
        match (escalationHistory.get(actorKey)) {
            case Some(info) => Some(info.getStats())
            case None => None
        }
    }
}

/**
 * 升级配置
 */
public struct EscalationConfig {
    public let failureThreshold: Int64          // 失败阈值
    public let maxEscalations: Int64            // 最大升级次数
    public let escalationCooldown: Duration     // 升级冷却时间
    public let escalationDecider: (Exception) -> Bool  // 升级决策函数
    
    public init(failureThreshold: Int64, maxEscalations: Int64, 
                escalationCooldown: Duration, escalationDecider: (Exception) -> Bool) {
        this.failureThreshold = failureThreshold
        this.maxEscalations = maxEscalations
        this.escalationCooldown = escalationCooldown
        this.escalationDecider = escalationDecider
    }
    
    public func shouldEscalateException(exception: Exception): Bool {
        escalationDecider(exception)
    }
    
    /**
     * 创建默认配置
     */
    public static func createDefault(): EscalationConfig {
        EscalationConfig(
            5,                          // 5次失败后升级
            3,                          // 最多升级3次
            Duration.minute,            // 1分钟冷却时间
            defaultEscalationDecider
        )
    }
    
    /**
     * 创建严格配置
     */
    public static func createStrict(): EscalationConfig {
        EscalationConfig(
            3,                          // 3次失败后升级
            2,                          // 最多升级2次
            Duration.second * 30,       // 30秒冷却时间
            strictEscalationDecider
        )
    }
    
    /**
     * 默认升级决策函数
     */
    public static func defaultEscalationDecider(exception: Exception): Bool {
        let exceptionType = exception.toString()
        match (exceptionType) {
            case "OutOfMemoryError" => true
            case "StackOverflowError" => true
            case "SystemException" => true
            case _ => false
        }
    }
    
    /**
     * 严格升级决策函数
     */
    public static func strictEscalationDecider(exception: Exception): Bool {
        let exceptionType = exception.toString()
        match (exceptionType) {
            case "OutOfMemoryError" => true
            case "StackOverflowError" => true
            case "SystemException" => true
            case "SecurityException" => true
            case "IllegalStateException" => true
            case _ => false
        }
    }
}

/**
 * 升级信息
 */
public class EscalationInfo {
    private let actor: ActorRef
    private let failureCount: AtomicInt64
    private let escalationCount: AtomicInt64
    private let lastEscalationTime: AtomicInt64
    private let firstFailureTime: AtomicInt64
    private let failures: ArrayList<ActorFailure>
    
    public init(actor: ActorRef) {
        this.actor = actor
        this.failureCount = AtomicInt64(0)
        this.escalationCount = AtomicInt64(0)
        this.lastEscalationTime = AtomicInt64(0)
        this.firstFailureTime = AtomicInt64(0)
        this.failures = ArrayList<ActorFailure>()
    }
    
    public func recordFailure(failure: ActorFailure): Unit {
        failureCount.fetchAdd(1)
        
        if (firstFailureTime.load() == 0) {
            firstFailureTime.store(DateTime.now().toUnixTimeStamp().toMilliseconds())
        }
        
        failures.add(failure)
        
        // 限制历史记录大小
        if (failures.size > 100) {
            failures.remove(0..1)  // 移除第一个元素，使用Range
        }
    }
    
    public func recordEscalation(): Unit {
        escalationCount.fetchAdd(1)
        lastEscalationTime.store(DateTime.now().toUnixTimeStamp().toMilliseconds())
    }
    
    public func getFailureCount(): Int64 {
        failureCount.load()
    }
    
    public func getEscalationCount(): Int64 {
        escalationCount.load()
    }
    
    public func getTimeSinceLastEscalation(): Int64 {
        let now = DateTime.now().toUnixTimeStamp().toMilliseconds()
        let lastEscalation = lastEscalationTime.load()
        if (lastEscalation == 0) {
            9223372036854775807  // Int64最大值，从未升级过
        } else {
            now - lastEscalation
        }
    }
    
    public func reset(): Unit {
        failureCount.store(0)
        escalationCount.store(0)
        lastEscalationTime.store(0)
        firstFailureTime.store(0)
        failures.clear()
    }
    
    public func getStats(): EscalationStats {
        EscalationStats(
            actor,
            failureCount.load(),
            escalationCount.load(),
            lastEscalationTime.load(),
            firstFailureTime.load()
        )
    }
}

/**
 * 升级统计信息
 */
public struct EscalationStats {
    public let actor: ActorRef
    public let failureCount: Int64
    public let escalationCount: Int64
    public let lastEscalationTime: Int64
    public let firstFailureTime: Int64
    
    public init(actor: ActorRef, failureCount: Int64, escalationCount: Int64,
                lastEscalationTime: Int64, firstFailureTime: Int64) {
        this.actor = actor
        this.failureCount = failureCount
        this.escalationCount = escalationCount
        this.lastEscalationTime = lastEscalationTime
        this.firstFailureTime = firstFailureTime
    }
}

/**
 * 升级监督策略工厂
 */
public struct EscalationSupervisionFactory {
    /**
     * 创建带升级的一对一策略
     */
    public static func createOneForOneWithEscalation(
        maxRetries: Int32,
        withinTimeRange: Duration,
        decider: (Exception) -> SupervisionDirective,
        escalationConfig: EscalationConfig
    ): EscalationSupervisionStrategy {
        let baseStrategy = OneForOneStrategy(maxRetries, withinTimeRange, decider)
        EscalationSupervisionStrategy(baseStrategy, escalationConfig)
    }
    
    /**
     * 创建默认的升级监督策略
     */
    public static func createDefault(): EscalationSupervisionStrategy {
        let baseStrategy = DefaultSupervisionStrategy.oneForOne()
        let config = EscalationConfig.createDefault()
        EscalationSupervisionStrategy(baseStrategy, config)
    }
}
