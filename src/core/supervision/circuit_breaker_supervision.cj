/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 断路器监督策略
 * 集成断路器模式到监督策略中
 */
package cactor.core.supervision

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*
import cactor.core.actor.{ActorRef, ActorLifecycleState}
import cactor.patterns.circuit_breaker.{BasicCircuitBreaker, CircuitBreakerConfig, CircuitBreakerState}

/**
 * 断路器监督策略
 * 结合断路器模式和传统监督策略
 */
public class CircuitBreakerSupervisionStrategy <: SupervisionStrategy {
    private let baseStrategy: SupervisionStrategy
    private let circuitBreakers: ConcurrentHashMap<String, BasicCircuitBreaker<Unit>>
    private let config: CircuitBreakerSupervisionConfig
    
    public init(baseStrategy: SupervisionStrategy, config: CircuitBreakerSupervisionConfig) {
        this.baseStrategy = baseStrategy
        this.circuitBreakers = ConcurrentHashMap<String, BasicCircuitBreaker<Unit>>()
        this.config = config
    }
    
    public func decide(failure: ActorFailure): SupervisionDirective {
        let actorKey = failure.actor.path.toString()
        let circuitBreaker = getOrCreateCircuitBreaker(actorKey)
        
        // 检查断路器状态
        match (circuitBreaker.getState()) {
            case CircuitBreakerState.Open =>
                // 断路器开启，直接停止Actor
                println("[CIRCUIT-BREAKER] Actor ${actorKey} 断路器开启，停止Actor")
                SupervisionDirective.Stop
                
            case CircuitBreakerState.HalfOpen =>
                // 半开状态，谨慎处理
                let directive = baseStrategy.decide(failure)
                match (directive) {
                    case SupervisionDirective.Restart =>
                        // 记录失败到断路器
                        recordFailureToCircuitBreaker(circuitBreaker)
                        SupervisionDirective.Stop  // 半开状态下失败直接停止
                    case _ => directive
                }
                
            case CircuitBreakerState.Closed =>
                // 正常状态，使用基础策略
                let directive = baseStrategy.decide(failure)
                
                // 记录失败到断路器
                recordFailureToCircuitBreaker(circuitBreaker)
                
                directive
        }
    }
    
    private func getOrCreateCircuitBreaker(actorKey: String): BasicCircuitBreaker<Unit> {
        match (circuitBreakers.get(actorKey)) {
            case Some(breaker) => breaker
            case None =>
                let breakerConfig = CircuitBreakerConfig(
                    config.failureThreshold,
                    config.successThreshold,
                    config.callTimeout,
                    config.resetTimeout
                )
                let newBreaker = BasicCircuitBreaker<Unit>(breakerConfig)
                circuitBreakers.add(actorKey, newBreaker)
                newBreaker
        }
    }
    
    private func recordFailureToCircuitBreaker(circuitBreaker: BasicCircuitBreaker<Unit>): Unit {
        try {
            circuitBreaker.execute({ =>
                throw Exception("Actor failure")
            })
        } catch (e: Exception) {
            // 预期的异常，用于记录失败
        }
    }
    
    /**
     * 记录Actor成功恢复
     */
    public func recordSuccess(actor: ActorRef): Unit {
        let actorKey = actor.path.toString()
        match (circuitBreakers.get(actorKey)) {
            case Some(breaker) =>
                try {
                    breaker.execute({ => () })  // 成功操作
                } catch (e: Exception) {
                    // 忽略
                }
            case None => ()
        }
    }
    
    /**
     * 重置Actor的断路器
     */
    public func resetCircuitBreaker(actor: ActorRef): Unit {
        let actorKey = actor.path.toString()
        match (circuitBreakers.get(actorKey)) {
            case Some(breaker) => breaker.reset()
            case None => ()
        }
    }
    
    /**
     * 获取Actor的断路器状态
     */
    public func getCircuitBreakerState(actor: ActorRef): Option<CircuitBreakerState> {
        let actorKey = actor.path.toString()
        match (circuitBreakers.get(actorKey)) {
            case Some(breaker) => Some(breaker.getState())
            case None => None
        }
    }
}

/**
 * 断路器监督策略配置
 */
public struct CircuitBreakerSupervisionConfig {
    public let failureThreshold: Int32      // 失败阈值
    public let successThreshold: Int32      // 成功阈值  
    public let callTimeout: Duration        // 调用超时
    public let resetTimeout: Duration       // 重置超时
    
    public init(failureThreshold: Int32, successThreshold: Int32, 
                callTimeout: Duration, resetTimeout: Duration) {
        this.failureThreshold = failureThreshold
        this.successThreshold = successThreshold
        this.callTimeout = callTimeout
        this.resetTimeout = resetTimeout
    }
    
    /**
     * 创建默认配置
     */
    public static func createDefault(): CircuitBreakerSupervisionConfig {
        CircuitBreakerSupervisionConfig(
            5,                          // 5次失败后开启断路器
            3,                          // 3次成功后关闭断路器
            Duration.second * 5,        // 5秒调用超时
            Duration.second * 30        // 30秒重置超时
        )
    }
    
    /**
     * 创建快速失败配置
     */
    public static func createFastFail(): CircuitBreakerSupervisionConfig {
        CircuitBreakerSupervisionConfig(
            3,                          // 3次失败后开启断路器
            2,                          // 2次成功后关闭断路器
            Duration.second * 2,        // 2秒调用超时
            Duration.second * 10        // 10秒重置超时
        )
    }
}

/**
 * 断路器监督策略工厂
 */
public struct CircuitBreakerSupervisionFactory {
    /**
     * 创建带断路器的一对一策略
     */
    public static func createOneForOneWithCircuitBreaker(
        maxRetries: Int32,
        withinTimeRange: Duration,
        decider: (Exception) -> SupervisionDirective,
        circuitBreakerConfig: CircuitBreakerSupervisionConfig
    ): CircuitBreakerSupervisionStrategy {
        let baseStrategy = OneForOneStrategy(maxRetries, withinTimeRange, decider)
        CircuitBreakerSupervisionStrategy(baseStrategy, circuitBreakerConfig)
    }
    
    /**
     * 创建带断路器的一对全策略
     */
    public static func createOneForAllWithCircuitBreaker(
        maxRetries: Int32,
        withinTimeRange: Duration,
        decider: (Exception) -> SupervisionDirective,
        circuitBreakerConfig: CircuitBreakerSupervisionConfig
    ): CircuitBreakerSupervisionStrategy {
        let baseStrategy = OneForAllStrategy(maxRetries, withinTimeRange, decider)
        CircuitBreakerSupervisionStrategy(baseStrategy, circuitBreakerConfig)
    }
    
    /**
     * 创建默认的断路器监督策略
     */
    public static func createDefault(): CircuitBreakerSupervisionStrategy {
        let baseStrategy = DefaultSupervisionStrategy.oneForOne()
        let config = CircuitBreakerSupervisionConfig.createDefault()
        CircuitBreakerSupervisionStrategy(baseStrategy, config)
    }
}
