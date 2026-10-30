/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 监督策略
 * 实现Actor监督和容错机制
 */
package cactor_core.supervision

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*
import cactor_core.actor.{ActorRef}
// 监督的核心类型定义在 cactor_core.actor（供 Actor / Props 引用而不成环），这里重导出以保持兼容
public import cactor_core.actor.{SupervisionDirective, ActorFailure, SupervisionStrategy}
import cactor_foundation.logging.{Logger, Logging}

let log_supervision_strategy: Logger = Logging.logger("cactor_core.supervision.supervision_strategy")

/**
 * 重试信息
 */
public class RetryInfo {
    private let retryCount: AtomicInt64
    private let firstFailureTime: AtomicInt64
    private let lastFailureTime: AtomicInt64

    public init() {
        this.retryCount = AtomicInt64(0)
        this.firstFailureTime = AtomicInt64(0)
        this.lastFailureTime = AtomicInt64(0)
    }

    public func recordRetry(): Unit {
        let now = DateTime.now().toUnixTimeStamp().toMilliseconds()
        retryCount.fetchAdd(1)
        lastFailureTime.store(now)
        
        if (firstFailureTime.load() == 0) {
            firstFailureTime.store(now)
        }
    }

    public func shouldRetry(maxRetries: Int32, withinTimeRange: Duration): Bool {
        // 负数表示不限次数（与 Akka 的 maxNrOfRetries = -1 相同）
        if (maxRetries < 0) {
            return true
        }
        let now = DateTime.now().toUnixTimeStamp().toMilliseconds()
        let firstFailure = firstFailureTime.load()

        // 如果是第一次失败，允许重试
        if (firstFailure == 0) {
            return true
        }

        let timeElapsed = now - firstFailure

        // 如果超出时间窗口，重置计数器并允许重试
        if (timeElapsed > withinTimeRange.toMilliseconds()) {
            reset()
            return true
        }

        // 在时间窗口内，检查重试次数
        let currentRetries = retryCount.load()
        return currentRetries < Int64(maxRetries)
    }

    public func reset(): Unit {
        retryCount.store(0)
        firstFailureTime.store(0)
        lastFailureTime.store(0)
    }

    public func getRetryCount(): Int64 {
        retryCount.load()
    }
}

/**
 * 一对一监督策略
 * 每个子Actor的失败只影响自己
 */
public class OneForOneStrategy <: SupervisionStrategy {
    private let maxRetries: Int32
    private let withinTimeRange: Duration
    private let retryHistory: ConcurrentHashMap<String, RetryInfo>
    private let decider: (Exception) -> SupervisionDirective

    public init(maxRetries: Int32, withinTimeRange: Duration, 
                decider: (Exception) -> SupervisionDirective) {
        this.maxRetries = maxRetries
        this.withinTimeRange = withinTimeRange
        this.retryHistory = ConcurrentHashMap<String, RetryInfo>()
        this.decider = decider
    }

    public func decide(failure: ActorFailure): SupervisionDirective {
        // 首先使用决策函数判断
        let directive = decider(failure.cause)
        
        match (directive) {
            case SupervisionDirective.Restart =>
                // 检查重试限制
                let retryInfo = getOrCreateRetryInfo(failure.actor)

                if (retryInfo.shouldRetry(maxRetries, withinTimeRange)) {
                    retryInfo.recordRetry()
                    SupervisionDirective.Restart
                } else {
                    // 超出重试限制，停止Actor
                    SupervisionDirective.Stop
                }
            case _ => directive
        }
    }

    private func getOrCreateRetryInfo(actor: ActorRef): RetryInfo {
        let actorKey = actor.path.toString()
        match (retryHistory.get(actorKey)) {
            case Some(info) => info
            case None =>
                let newInfo = RetryInfo()
                retryHistory.add(actorKey, newInfo)
                newInfo
        }
    }

    public func resetRetryInfo(actor: ActorRef): Unit {
        let actorKey = actor.path.toString()
        match (retryHistory.get(actorKey)) {
            case Some(info) => info.reset()
            case None => ()
        }
    }
}

/**
 * 一对全监督策略
 * 一个子Actor的失败会影响所有兄弟Actor
 */
public class OneForAllStrategy <: SupervisionStrategy {
    private let maxRetries: Int32
    private let withinTimeRange: Duration
    private let globalRetryInfo: RetryInfo
    private let decider: (Exception) -> SupervisionDirective

    public init(maxRetries: Int32, withinTimeRange: Duration,
                decider: (Exception) -> SupervisionDirective) {
        this.maxRetries = maxRetries
        this.withinTimeRange = withinTimeRange
        this.globalRetryInfo = RetryInfo()
        this.decider = decider
    }

    public func decide(failure: ActorFailure): SupervisionDirective {
        let directive = decider(failure.cause)
        
        match (directive) {
            case SupervisionDirective.Restart =>
                if (globalRetryInfo.shouldRetry(maxRetries, withinTimeRange)) {
                    globalRetryInfo.recordRetry()
                    SupervisionDirective.Restart
                } else {
                    SupervisionDirective.Stop
                }
            case _ => directive
        }
    }

    public func resetRetryInfo(): Unit {
        globalRetryInfo.reset()
    }

    public func appliesToAllChildren(): Bool {
        true
    }
}

/**
 * Escalate监督策略
 * 将失败向上级监督者传播
 * 实现plan3.md优先级3：监督策略完善
 */
public class EscalateStrategy <: SupervisionStrategy {
    private let escalateThreshold: Int64
    private let escalateTimeWindow: Duration
    private let escalateHistory: ConcurrentHashMap<String, RetryInfo>

    public init() {
        this.escalateThreshold = 3  // 默认3次失败后向上传播
        this.escalateTimeWindow = Duration.minute * 5  // 5分钟时间窗口
        this.escalateHistory = ConcurrentHashMap<String, RetryInfo>()
    }

    public init(threshold: Int64, timeWindow: Duration) {
        this.escalateThreshold = threshold
        this.escalateTimeWindow = timeWindow
        this.escalateHistory = ConcurrentHashMap<String, RetryInfo>()
    }

    public func decide(failure: ActorFailure): SupervisionDirective {
        let actorKey = failure.actor.path.toString()
        let escalateInfo = getOrCreateEscalateInfo(actorKey)

        log_supervision_strategy.error("[ESCALATE-STRATEGY] 评估失败: " + actorKey)
        log_supervision_strategy.error("[ESCALATE-STRATEGY] 失败次数: " + escalateInfo.getRetryCount().toString())
        log_supervision_strategy.debug("[ESCALATE-STRATEGY] 阈值: " + escalateThreshold.toString())

        if (escalateInfo.shouldRetry(Int32(escalateThreshold), escalateTimeWindow)) {
            escalateInfo.recordRetry()
            log_supervision_strategy.error("[ESCALATE-STRATEGY] 失败次数未达到阈值，继续重启")
            SupervisionDirective.Restart
        } else {
            log_supervision_strategy.error("[ESCALATE-STRATEGY] 失败次数达到阈值，向上级传播")
            SupervisionDirective.Escalate
        }
    }

    private func getOrCreateEscalateInfo(actorKey: String): RetryInfo {
        match (escalateHistory.get(actorKey)) {
            case Some(info) => info
            case None =>
                let newInfo = RetryInfo()
                escalateHistory.add(actorKey, newInfo)
                newInfo
        }
    }

    public func resetEscalateHistory(actorKey: String): Unit {
        escalateHistory.remove(actorKey)
    }

    public func getEscalateStats(): String {
        "EscalateStrategy - 阈值: " + escalateThreshold.toString() +
        ", 时间窗口: " + escalateTimeWindow.toString() +
        ", 跟踪Actor数: " + escalateHistory.size.toString()
    }
}

/**
 * 指数退避监督策略
 * 实现指数退避重启机制
 */
public class BackoffSupervisorStrategy <: SupervisionStrategy {
    private let minBackoff: Duration
    private let maxBackoff: Duration
    private let randomFactor: Float64
    private let maxRetries: Int64
    private let backoffHistory: ConcurrentHashMap<String, BackoffInfo>

    public init(minBackoff: Duration, maxBackoff: Duration) {
        this.minBackoff = minBackoff
        this.maxBackoff = maxBackoff
        this.randomFactor = 0.2  // 20%的随机因子
        this.maxRetries = 10
        this.backoffHistory = ConcurrentHashMap<String, BackoffInfo>()
    }

    public init(minBackoff: Duration, maxBackoff: Duration, randomFactor: Float64, maxRetries: Int64) {
        this.minBackoff = minBackoff
        this.maxBackoff = maxBackoff
        this.randomFactor = randomFactor
        this.maxRetries = maxRetries
        this.backoffHistory = ConcurrentHashMap<String, BackoffInfo>()
    }

    public func decide(failure: ActorFailure): SupervisionDirective {
        let actorKey = failure.actor.path.toString()
        let backoffInfo = getOrCreateBackoffInfo(actorKey)

        log_supervision_strategy.error("[BACKOFF-STRATEGY] 评估失败: " + actorKey)
        log_supervision_strategy.debug("[BACKOFF-STRATEGY] 重试次数: " + backoffInfo.retryCount.toString())

        if (backoffInfo.retryCount >= maxRetries) {
            log_supervision_strategy.debug("[BACKOFF-STRATEGY] 达到最大重试次数，停止Actor")
            return SupervisionDirective.Stop
        }

        // 计算退避时间
        let backoffTime = calculateBackoffTime(backoffInfo.retryCount)
        backoffInfo.retryCount += 1
        backoffInfo.lastBackoffTime = backoffTime
        backoffInfo.nextRetryTime = DateTime.now().toUnixTimeStamp().toMilliseconds() + backoffTime.toMilliseconds()

        log_supervision_strategy.debug("[BACKOFF-STRATEGY] 计算退避时间: " + backoffTime.toString())
        log_supervision_strategy.debug("[BACKOFF-STRATEGY] 下次重试时间: " + backoffInfo.nextRetryTime.toString())

        // 检查是否到了重试时间
        let currentTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        if (currentTime >= backoffInfo.nextRetryTime) {
            log_supervision_strategy.debug("[BACKOFF-STRATEGY] 可以重试，执行重启")
            SupervisionDirective.Restart
        } else {
            log_supervision_strategy.debug("[BACKOFF-STRATEGY] 还未到重试时间，暂时停止")
            SupervisionDirective.Stop  // 实际实现中应该延迟重启
        }
    }

    public func calculateBackoffTime(retryCount: Int64): Duration {
        // 指数退避：minBackoff * 2^retryCount
        let exponentialBackoff = minBackoff.toMilliseconds() * (1 << retryCount)
        let cappedBackoff = if (exponentialBackoff > maxBackoff.toMilliseconds()) {
            maxBackoff.toMilliseconds()
        } else {
            exponentialBackoff
        }

        // 添加随机因子
        let randomMultiplier = 1.0 + (randomFactor * (2.0 * Math.random() - 1.0))
        let finalBackoff = Int64(Float64(cappedBackoff) * randomMultiplier)

        Duration.millisecond * finalBackoff
    }

    /** 返回最大重试次数 */
    public func getMaxRetries(): Int64 { maxRetries }

    private func getOrCreateBackoffInfo(actorKey: String): BackoffInfo {
        match (backoffHistory.get(actorKey)) {
            case Some(info) => info
            case None =>
                let newInfo = BackoffInfo()
                backoffHistory.add(actorKey, newInfo)
                newInfo
        }
    }

    public func resetBackoffInfo(actorKey: String): Unit {
        backoffHistory.remove(actorKey)
    }

    public func getBackoffStats(): String {
        "BackoffSupervisorStrategy - 最小退避: " + minBackoff.toString() +
        ", 最大退避: " + maxBackoff.toString() +
        ", 最大重试: " + maxRetries.toString() +
        ", 跟踪Actor数: " + backoffHistory.size.toString()
    }
}

/**
 * 退避信息
 */
public class BackoffInfo {
    public var retryCount: Int64 = 0
    public var lastBackoffTime: Duration = Duration.millisecond * 0
    public var nextRetryTime: Int64 = 0

    public init() {}

    public func reset(): Unit {
        retryCount = 0
        lastBackoffTime = Duration.millisecond * 0
        nextRetryTime = 0
    }
}

/**
 * 简化的Math类
 */
public struct Math {
    public static func random(): Float64 {
        // 简化实现，返回0.5
        0.5
    }
}

/**
 * 默认监督策略
 * 提供常用的异常处理决策
 */
public struct DefaultSupervisionStrategy {
    /**
     * 默认的异常决策函数
     */
    public static func defaultDecider(exception: Exception): SupervisionDirective {
        // 简化实现：根据异常类型决定策略
        // 实际实现中可以根据具体的异常类型进行更细粒度的控制
        SupervisionDirective.Restart
    }

    /**
     * 未指定监督策略时使用的策略：任何异常都重启，不限次数（与 Akka 经典 API 的默认策略相同）。
     * 需要“短时间内失败过多就停止”时，显式使用 oneForOne() 或自定义 OneForOneStrategy。
     */
    public static func restartAlways(): OneForOneStrategy {
        OneForOneStrategy(-1, Duration.minute, defaultDecider)
    }

    /**
     * 创建一对一策略：一分钟内最多重启 3 次，超过后停止
     */
    public static func oneForOne(): OneForOneStrategy {
        OneForOneStrategy(
            3,
            Duration.minute,
            defaultDecider
        )
    }

    /**
     * 创建默认的一对全策略
     */
    public static func oneForAll(): OneForAllStrategy {
        OneForAllStrategy(
            3,
            Duration.minute,
            defaultDecider
        )
    }

    /**
     * 创建停止策略（遇到任何异常都停止）
     */
    public static func stoppingStrategy(): OneForOneStrategy {
        OneForOneStrategy(
            0,
            Duration.second,
            { exception => SupervisionDirective.Stop }
        )
    }

    /**
     * 创建恢复策略（遇到任何异常都恢复）
     */
    public static func resumingStrategy(): OneForOneStrategy {
        OneForOneStrategy(
            2147483647,
            Duration.hour,
            { exception => SupervisionDirective.Resume }
        )
    }
}

/**
 * 监督者接口
 */
public interface Supervisor {
    /**
     * 处理子Actor的失败
     */
    func handleFailure(failure: ActorFailure): Unit

    /**
     * 获取监督策略
     */
    func getSupervisionStrategy(): SupervisionStrategy

    /**
     * 添加子Actor
     */
    func addChild(child: ActorRef): Unit

    /**
     * 移除子Actor
     */
    func removeChild(child: ActorRef): Unit

    /**
     * 获取所有子Actor
     */
    func getChildren(): Array<ActorRef>
}

/**
 * 基础监督者实现
 */
public class BasicSupervisor <: Supervisor {
    private let strategy: SupervisionStrategy
    private let children: ConcurrentHashMap<String, ActorRef>
    private let failureCount: AtomicInt64

    public init(strategy: SupervisionStrategy) {
        this.strategy = strategy
        this.children = ConcurrentHashMap<String, ActorRef>()
        this.failureCount = AtomicInt64(0)
    }

    public func handleFailure(failure: ActorFailure): Unit {
        failureCount.fetchAdd(1)
        
        let directive = strategy.decide(failure)
        
        match (directive) {
            case SupervisionDirective.Resume =>
                // 恢复Actor - 简化实现
                log_supervision_strategy.error("恢复Actor: ${failure.actor.path.toString()}")
            case SupervisionDirective.Restart =>
                // 重启Actor - 简化实现
                log_supervision_strategy.error("重启Actor: ${failure.actor.path.toString()}")
            case SupervisionDirective.Stop =>
                // 停止Actor
                log_supervision_strategy.error("停止Actor: ${failure.actor.path.toString()}")
                removeChild(failure.actor)
            case SupervisionDirective.Escalate =>
                // 上报失败 - 简化实现
                log_supervision_strategy.error("上报失败: ${failure.actor.path.toString()}")
        }
    }

    public func getSupervisionStrategy(): SupervisionStrategy {
        strategy
    }

    public func addChild(child: ActorRef): Unit {
        children.add(child.path.toString(), child)
    }

    public func removeChild(child: ActorRef): Unit {
        children.remove(child.path.toString())
    }

    public func getChildren(): Array<ActorRef> {
        let childList = ArrayList<ActorRef>()
        for ((_, actor) in children) {
            childList.add(actor)
        }
        childList.toArray()
    }

    public func getFailureCount(): Int64 {
        failureCount.load()
    }
}
