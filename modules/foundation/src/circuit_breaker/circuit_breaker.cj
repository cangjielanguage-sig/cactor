/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 断路器模式
 * 提供故障隔离和快速失败机制
 */
package cactor_foundation.circuit_breaker

import std.sync.*
import std.time.*
import cactor_foundation.logging.{Logger, Logging}

let log_circuit_breaker: Logger = Logging.logger("cactor_foundation.circuit_breaker.circuit_breaker")

/**
 * 断路器状态
 */
public enum CircuitBreakerState {
    | Closed      // 关闭状态，正常工作
    | Open        // 开启状态，快速失败
    | HalfOpen    // 半开状态，尝试恢复
}

/**
 * 断路器配置
 */
public struct CircuitBreakerConfig {
    public let failureThreshold: Int32        // 失败阈值
    public let successThreshold: Int32        // 成功阈值（半开状态）
    public let timeout: Duration              // 超时时间
    public let resetTimeout: Duration         // 重置超时时间

    public init(failureThreshold: Int32, successThreshold: Int32, 
                timeout: Duration, resetTimeout: Duration) {
        this.failureThreshold = failureThreshold
        this.successThreshold = successThreshold
        this.timeout = timeout
        this.resetTimeout = resetTimeout
    }
}

/**
 * 断路器统计信息
 */
public struct CircuitBreakerStats {
    public let state: CircuitBreakerState
    public let failureCount: Int64
    public let successCount: Int64
    public let totalRequests: Int64
    public let lastFailureTime: Int64
    public let lastSuccessTime: Int64

    public init(state: CircuitBreakerState, failureCount: Int64, successCount: Int64,
                totalRequests: Int64, lastFailureTime: Int64, lastSuccessTime: Int64) {
        this.state = state
        this.failureCount = failureCount
        this.successCount = successCount
        this.totalRequests = totalRequests
        this.lastFailureTime = lastFailureTime
        this.lastSuccessTime = lastSuccessTime
    }

    public func getFailureRate(): Float64 {
        if (totalRequests == 0) {
            return 0.0
        }
        Float64(failureCount) / Float64(totalRequests)
    }

    public func getSuccessRate(): Float64 {
        if (totalRequests == 0) {
            return 0.0
        }
        Float64(successCount) / Float64(totalRequests)
    }
}

/**
 * 断路器异常
 */
public class CircuitBreakerOpenException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 断路器调用超时：调用本身完成了，但耗时超过 callTimeout，按失败计（与 Akka withSyncCircuitBreaker 相同）
 */
public class CircuitBreakerTimeoutException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 断路器接口
 */
public interface CircuitBreaker<T> {
    /**
     * 执行操作
     */
    func execute(operation: () -> T): T

    /**
     * 获取当前状态
     */
    func getState(): CircuitBreakerState

    /**
     * 获取统计信息
     */
    func getStats(): CircuitBreakerStats

    /**
     * 手动重置断路器
     */
    func reset(): Unit

    /**
     * 手动触发断路器
     */
    func trip(): Unit
}

/** 单调时钟（毫秒）：墙钟会被 NTP 调整，开启时长不能用它计算 */
let breakerEpoch: MonoTime = MonoTime.now()

func monotonicMillis(): Int64 {
    (MonoTime.now() - breakerEpoch).toMilliseconds()
}

/**
 * 基础断路器实现，语义对齐 Akka CircuitBreaker（P7）：
 * - Closed：失败计**连续**次数，任何一次成功清零；达到 failureThreshold 转 Open。
 * - Open：快速失败，开启满 resetTimeout 后转 HalfOpen（查询状态或下一次调用时生效，效果同 Akka 的定时转换）。
 * - HalfOpen：**同一时刻只放行一个试探调用**，其余快速失败；试探成功累计到 successThreshold 转 Closed，
 *   试探失败立即回到 Open 并重新计时。
 * - callTimeout（配置中的 timeout，为 0 时不检查）：调用耗时超过它按失败计，并抛 CircuitBreakerTimeoutException。
 *
 * 状态机字段只在 lock 内读写；操作本身在锁外执行。统计计数为原子量，锁外可读。
 */
public class BasicCircuitBreaker<T> <: CircuitBreaker<T> {
    private let config: CircuitBreakerConfig
    private let clock: () -> Int64
    private let lock = Mutex()
    // ---- 只在 lock 内访问 ----
    private var state = CircuitBreakerState.Closed
    private var consecutiveFailures: Int64 = 0
    private var halfOpenSuccesses: Int64 = 0
    private var probeInFlight = false
    private var openedAt: Int64 = 0
    // ---- 统计（自创建或上次 reset 起的累计值） ----
    private let failureCount = AtomicInt64(0)
    private let successCount = AtomicInt64(0)
    private let totalRequests = AtomicInt64(0)
    private let lastFailureTime = AtomicInt64(0)
    private let lastSuccessTime = AtomicInt64(0)

    public init(config: CircuitBreakerConfig) {
        this(config, monotonicMillis)
    }

    /** clock 返回单调毫秒数；测试注入虚拟时钟即可确定性地驱动开启 → 半开的转换 */
    public init(config: CircuitBreakerConfig, clock: () -> Int64) {
        this.config = config
        this.clock = clock
    }

    public func execute(operation: () -> T): T {
        totalRequests.fetchAdd(1)
        let probe = admit()
        let start = clock()
        var settled = false
        try {
            let result = operation()
            let elapsed = clock() - start
            let limit = config.timeout.toMilliseconds()
            settled = true
            if (limit > 0 && elapsed > limit) {
                onFailure(probe)
                throw CircuitBreakerTimeoutException("调用耗时 ${elapsed} ms，超过 callTimeout ${limit} ms")
            }
            onSuccess(probe)
            return result
        } finally {
            // 操作抛出（包括 Error）时按失败计；否则半开试探位永远不释放，断路器卡死在 HalfOpen
            if (!settled) {
                onFailure(probe)
            }
        }
    }

    /**
     * 准入：返回本次调用是否是半开状态的试探调用；不放行时抛 CircuitBreakerOpenException
     */
    private func admit(): Bool {
        synchronized(lock) {
            advance()
            match (state) {
                case CircuitBreakerState.Closed => false
                case CircuitBreakerState.Open =>
                    let remaining = config.resetTimeout.toMilliseconds() - (clock() - openedAt)
                    throw CircuitBreakerOpenException("断路器处于开启状态，约 ${remaining} ms 后进入半开")
                case CircuitBreakerState.HalfOpen =>
                    if (probeInFlight) {
                        throw CircuitBreakerOpenException("断路器处于半开状态，已有试探调用在执行")
                    }
                    probeInFlight = true
                    true
            }
        }
    }

    /** Open 满 resetTimeout 后转 HalfOpen（调用方持有 lock） */
    private func advance(): Unit {
        match (state) {
            case CircuitBreakerState.Open where clock() - openedAt >= config.resetTimeout.toMilliseconds() =>
                state = CircuitBreakerState.HalfOpen
                halfOpenSuccesses = 0
                probeInFlight = false
                log_circuit_breaker.debug("断路器转换到半开状态")
            case _ => ()
        }
    }

    private func onSuccess(probe: Bool): Unit {
        successCount.fetchAdd(1)
        lastSuccessTime.store(DateTime.now().toUnixTimeStamp().toMilliseconds())
        synchronized(lock) {
            match (state) {
                case CircuitBreakerState.Closed => consecutiveFailures = 0
                case CircuitBreakerState.HalfOpen where probe =>
                    probeInFlight = false
                    halfOpenSuccesses++
                    if (halfOpenSuccesses >= Int64(config.successThreshold)) {
                        toClosed()
                    }
                // 调用期间状态已被别的调用或 trip/reset 改变：以当前状态为准，不再计入
                case _ => ()
            }
        }
    }

    private func onFailure(probe: Bool): Unit {
        failureCount.fetchAdd(1)
        lastFailureTime.store(DateTime.now().toUnixTimeStamp().toMilliseconds())
        synchronized(lock) {
            match (state) {
                case CircuitBreakerState.Closed =>
                    consecutiveFailures++
                    if (consecutiveFailures >= Int64(config.failureThreshold)) {
                        toOpen()
                    }
                case CircuitBreakerState.HalfOpen where probe => toOpen()
                case _ => ()
            }
        }
    }

    private func toOpen(): Unit {
        state = CircuitBreakerState.Open
        openedAt = clock()
        consecutiveFailures = 0
        halfOpenSuccesses = 0
        probeInFlight = false
        log_circuit_breaker.debug("断路器转换到开启状态")
    }

    private func toClosed(): Unit {
        state = CircuitBreakerState.Closed
        consecutiveFailures = 0
        halfOpenSuccesses = 0
        probeInFlight = false
        log_circuit_breaker.debug("断路器转换到关闭状态")
    }

    /**
     * 手动记一次成功（对标 Akka succeed()）：用于结果不经 execute 的场景，例如监督策略记录子 Actor 的恢复
     */
    public func succeed(): Unit {
        synchronized(lock) {
            advance()
        }
        onSuccess(true)
    }

    /**
     * 手动记一次失败（对标 Akka fail()）：Closed 时计入连续失败，HalfOpen 时立即回到 Open
     */
    public func fail(): Unit {
        synchronized(lock) {
            advance()
        }
        onFailure(true)
    }

    /** 当前状态；开启满 resetTimeout 后返回 HalfOpen */
    public func getState(): CircuitBreakerState {
        synchronized(lock) {
            advance()
            state
        }
    }

    /** 统计：failureCount / successCount 为累计次数（含超时），totalRequests 含被快速失败拒绝的调用 */
    public func getStats(): CircuitBreakerStats {
        let s = getState()
        CircuitBreakerStats(s, failureCount.load(), successCount.load(), totalRequests.load(), lastFailureTime.load(),
            lastSuccessTime.load())
    }

    public func reset(): Unit {
        synchronized(lock) {
            toClosed()
        }
        failureCount.store(0)
        successCount.store(0)
        totalRequests.store(0)
        log_circuit_breaker.debug("断路器手动重置")
    }

    public func trip(): Unit {
        synchronized(lock) {
            toOpen()
        }
        log_circuit_breaker.debug("断路器手动触发")
    }
}

/**
 * 断路器工厂
 */
public struct CircuitBreakerFactory {
    /**
     * 创建默认配置的断路器
     */
    public static func createDefault<T>(): BasicCircuitBreaker<T> {
        let config = CircuitBreakerConfig(
            5,                          // 失败阈值
            3,                          // 成功阈值
            Duration.second * 5,        // 超时时间
            Duration.second * 30        // 重置超时时间
        )
        BasicCircuitBreaker<T>(config)
    }

    /**
     * 创建快速失败的断路器
     */
    public static func createFastFail<T>(): BasicCircuitBreaker<T> {
        let config = CircuitBreakerConfig(
            3,                          // 失败阈值
            2,                          // 成功阈值
            Duration.second * 2,        // 超时时间
            Duration.second * 10        // 重置超时时间
        )
        BasicCircuitBreaker<T>(config)
    }

    /**
     * 创建容错的断路器
     */
    public static func createTolerant<T>(): BasicCircuitBreaker<T> {
        let config = CircuitBreakerConfig(
            10,                         // 失败阈值
            5,                          // 成功阈值
            Duration.second * 10,       // 超时时间
            Duration.minute             // 重置超时时间
        )
        BasicCircuitBreaker<T>(config)
    }
}

/**
 * 断路器装饰器
 * 为现有函数添加断路器功能
 */
public class CircuitBreakerDecorator<T> {
    private let circuitBreaker: CircuitBreaker<T>
    private let fallback: Option<() -> T>

    public init(circuitBreaker: CircuitBreaker<T>, fallback: Option<() -> T>) {
        this.circuitBreaker = circuitBreaker
        this.fallback = fallback
    }

    public func call(operation: () -> T): T {
        try {
            circuitBreaker.execute(operation)
        } catch (e: CircuitBreakerOpenException) {
            match (fallback) {
                case Some(fb) => fb()
                case None => throw e
            }
        }
    }

    public func getCircuitBreaker(): CircuitBreaker<T> {
        circuitBreaker
    }
}
