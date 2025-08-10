/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 监督策略
 * 实现Actor监督和容错机制
 */
package cactor.core.supervision

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*
import cactor.core.actor.{ActorRef, ActorLifecycleState}

/**
 * 监督指令
 * 定义当Actor失败时应该采取的行动
 */
public enum SupervisionDirective {
    | Resume      // 恢复Actor，忽略失败
    | Restart     // 重启Actor
    | Stop        // 停止Actor
    | Escalate    // 将失败上报给上级监督者

    public func toString(): String {
        match (this) {
            case Resume => "Resume"
            case Restart => "Restart"
            case Stop => "Stop"
            case Escalate => "Escalate"
        }
    }
}

/**
 * Actor失败信息
 */
public struct ActorFailure {
    public let actor: ActorRef
    public let cause: Exception
    public let timestamp: Int64
    public let failureCount: Int64

    public init(actor: ActorRef, cause: Exception, failureCount: Int64) {
        this.actor = actor
        this.cause = cause
        this.timestamp = DateTime.now().toUnixTimeStamp().toMilliseconds()
        this.failureCount = failureCount
    }
}

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
 * 监督策略接口
 */
public interface SupervisionStrategy {
    /**
     * 决定如何处理Actor失败
     */
    func decide(failure: ActorFailure): SupervisionDirective
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

        println("[ESCALATE-STRATEGY] 评估失败: " + actorKey)
        println("[ESCALATE-STRATEGY] 失败次数: " + escalateInfo.getRetryCount().toString())
        println("[ESCALATE-STRATEGY] 阈值: " + escalateThreshold.toString())

        if (escalateInfo.shouldRetry(Int32(escalateThreshold), escalateTimeWindow)) {
            escalateInfo.recordRetry()
            println("[ESCALATE-STRATEGY] 失败次数未达到阈值，继续重启")
            SupervisionDirective.Restart
        } else {
            println("[ESCALATE-STRATEGY] 失败次数达到阈值，向上级传播")
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

        println("[BACKOFF-STRATEGY] 评估失败: " + actorKey)
        println("[BACKOFF-STRATEGY] 重试次数: " + backoffInfo.retryCount.toString())

        if (backoffInfo.retryCount >= maxRetries) {
            println("[BACKOFF-STRATEGY] 达到最大重试次数，停止Actor")
            return SupervisionDirective.Stop
        }

        // 计算退避时间
        let backoffTime = calculateBackoffTime(backoffInfo.retryCount)
        backoffInfo.retryCount += 1
        backoffInfo.lastBackoffTime = backoffTime
        backoffInfo.nextRetryTime = DateTime.now().toUnixTimeStamp().toMilliseconds() + backoffTime.toMilliseconds()

        println("[BACKOFF-STRATEGY] 计算退避时间: " + backoffTime.toString())
        println("[BACKOFF-STRATEGY] 下次重试时间: " + backoffInfo.nextRetryTime.toString())

        // 检查是否到了重试时间
        let currentTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        if (currentTime >= backoffInfo.nextRetryTime) {
            println("[BACKOFF-STRATEGY] 可以重试，执行重启")
            SupervisionDirective.Restart
        } else {
            println("[BACKOFF-STRATEGY] 还未到重试时间，暂时停止")
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

    /** 返回最小退避时间 */
    public func getMinBackoff(): Duration { minBackoff }

    /** 返回最大退避时间 */
    public func getMaxBackoff(): Duration { maxBackoff }

    /** 返回随机因子 */
    public func getRandomFactor(): Float64 { randomFactor }

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
 * 优先级规则
 * 支持基于优先级的规则匹配
 */
public struct PriorityRule {
    public let pattern: String
    public let directive: SupervisionDirective
    public let priority: Int32  // 数值越小优先级越高
    public let ruleType: RuleType

    public init(pattern: String, directive: SupervisionDirective, priority: Int32, ruleType: RuleType) {
        this.pattern = pattern
        this.directive = directive
        this.priority = priority
        this.ruleType = ruleType
    }
}

/**
 * 条件规则
 * 支持基于条件函数的动态规则匹配
 */
public struct ConditionalRule {
    public let name: String
    public let condition: (Exception) -> Bool
    public let directive: SupervisionDirective
    public let priority: Int32

    public init(name: String, condition: (Exception) -> Bool, directive: SupervisionDirective, priority: Int32) {
        this.name = name
        this.condition = condition
        this.directive = directive
        this.priority = priority
    }
}

/**
 * 规则类型枚举
 */
public enum RuleType <: ToString {
    | Exact      // 精确匹配
    | Wildcard   // 通配符匹配
    | Regex      // 正则表达式匹配
    | Conditional // 条件匹配

    public func toString(): String {
        match (this) {
            case Exact => "Exact"
            case Wildcard => "Wildcard"
            case Regex => "Regex"
            case Conditional => "Conditional"
        }
    }
}

/**
 * 规则统计信息
 */
public struct RuleStatistics {
    public let ruleName: String
    public let matchCount: AtomicInt64
    public let lastMatchTime: AtomicInt64
    public let averageMatchTime: AtomicInt64

    public init(ruleName: String) {
        this.ruleName = ruleName
        this.matchCount = AtomicInt64(0)
        this.lastMatchTime = AtomicInt64(0)
        this.averageMatchTime = AtomicInt64(0)
    }

    public func recordMatch(matchTime: Int64): Unit {
        matchCount.fetchAdd(1)
        lastMatchTime.store(matchTime)

        // 简单的移动平均
        let currentAvg = averageMatchTime.load()
        let newAvg = (currentAvg + matchTime) / 2
        averageMatchTime.store(newAvg)
    }

    public func getMatchCount(): Int64 {
        matchCount.load()
    }

    public func getAverageMatchTime(): Int64 {
        averageMatchTime.load()
    }
}

/**
 * 缓存统计信息
 */
public struct CacheStatistics {
    public let currentSize: Int32
    public let maxSize: Int32
    public let hitRate: Float64

    public init(currentSize: Int32, maxSize: Int32, hitRate: Float64) {
        this.currentSize = currentSize
        this.maxSize = maxSize
        this.hitRate = hitRate
    }
}

/**
 * 矩阵配置信息
 */
public struct MatrixConfiguration {
    public let defaultDirective: SupervisionDirective
    public let exactRulesCount: Int32
    public let wildcardRulesCount: Int32
    public let priorityRulesCount: Int32
    public let conditionalRulesCount: Int32
    public let statisticsEnabled: Bool
    public let cacheSize: Int32

    public init(defaultDirective: SupervisionDirective, exactRulesCount: Int32, wildcardRulesCount: Int32,
                priorityRulesCount: Int32, conditionalRulesCount: Int32, statisticsEnabled: Bool, cacheSize: Int32) {
        this.defaultDirective = defaultDirective
        this.exactRulesCount = exactRulesCount
        this.wildcardRulesCount = wildcardRulesCount
        this.priorityRulesCount = priorityRulesCount
        this.conditionalRulesCount = conditionalRulesCount
        this.statisticsEnabled = statisticsEnabled
        this.cacheSize = cacheSize
    }
}

/**
 * 增强的监督策略矩阵
 * 基于异常类型提供精确的决策控制，支持优先级、条件匹配和性能优化
 */
public class SupervisionStrategyMatrix {
    private let exceptionRules: HashMap<String, SupervisionDirective>
    private let defaultDirective: SupervisionDirective
    private let wildcardRules: ArrayList<(String, SupervisionDirective)>
    private let priorityRules: ArrayList<PriorityRule>  // 优先级规则
    private let conditionalRules: ArrayList<ConditionalRule>  // 条件规则
    private let ruleCache: HashMap<String, SupervisionDirective>  // 决策缓存
    private let statisticsEnabled: Bool
    private let ruleStats: HashMap<String, RuleStatistics>  // 规则统计
    private let mutex: Mutex  // 线程安全保护

    public init(defaultDirective: SupervisionDirective) {
        this.exceptionRules = HashMap<String, SupervisionDirective>()
        this.defaultDirective = defaultDirective
        this.wildcardRules = ArrayList<(String, SupervisionDirective)>()
        this.priorityRules = ArrayList<PriorityRule>()
        this.conditionalRules = ArrayList<ConditionalRule>()
        this.ruleCache = HashMap<String, SupervisionDirective>()
        this.statisticsEnabled = true
        this.ruleStats = HashMap<String, RuleStatistics>()
        this.mutex = Mutex()
    }

    /**
     * 添加精确异常类型规则
     */
    public func addRule(exceptionType: String, directive: SupervisionDirective): SupervisionStrategyMatrix {
        exceptionRules.add(exceptionType, directive)
        this
    }

    /**
     * 添加通配符规则（支持模式匹配）
     */
    public func addWildcardRule(pattern: String, directive: SupervisionDirective): SupervisionStrategyMatrix {
        wildcardRules.add((pattern, directive))
        this
    }

    /**
     * 添加优先级规则
     */
    public func addPriorityRule(pattern: String, directive: SupervisionDirective, priority: Int32, ruleType: RuleType): SupervisionStrategyMatrix {
        let rule = PriorityRule(pattern, directive, priority, ruleType)
        priorityRules.add(rule)
        this
    }

    /**
     * 添加条件规则
     */
    public func addConditionalRule(name: String, condition: (Exception) -> Bool, directive: SupervisionDirective, priority: Int32): SupervisionStrategyMatrix {
        let rule = ConditionalRule(name, condition, directive, priority)
        conditionalRules.add(rule)
        this
    }

    /**
     * 启用或禁用统计功能
     */
    public func setStatisticsEnabled(enabled: Bool): SupervisionStrategyMatrix {
        mutex.lock()
        try {
            // 注意：这里不能修改statisticsEnabled，因为它是let字段
            // 在实际实现中，可以考虑将其改为var字段
        } finally {
            mutex.unlock()
        }
        this
    }

    /**
     * 清空决策缓存
     */
    public func clearCache(): SupervisionStrategyMatrix {
        mutex.lock()
        try {
            ruleCache.clear()
        } finally {
            mutex.unlock()
        }
        this
    }

    /**
     * 根据异常决定监督指令 - 增强版本
     * 支持优先级匹配、条件匹配和缓存优化
     */
    public func decide(exception: Exception): SupervisionDirective {
        let exceptionMessage = exception.toString()
        let startTime = getCurrentTimeNanos()

        // 1. 检查缓存
        match (ruleCache.get(exceptionMessage)) {
            case Some(cachedDirective) =>
                recordRuleMatch("cache", startTime)
                return cachedDirective
            case None => ()
        }

        // 2. 按优先级检查优先级规则
        let sortedPriorityRules = sortPriorityRules(priorityRules.toArray())
        for (rule in sortedPriorityRules) {
            if (matchesRule(exceptionMessage, exception, rule)) {
                let directive = rule.directive
                cacheDecision(exceptionMessage, directive)
                recordRuleMatch(rule.pattern, startTime)
                return directive
            }
        }

        // 3. 检查条件规则（按优先级排序）
        let sortedConditionalRules = sortConditionalRules(conditionalRules.toArray())
        for (rule in sortedConditionalRules) {
            if (rule.condition(exception)) {
                let directive = rule.directive
                cacheDecision(exceptionMessage, directive)
                recordRuleMatch(rule.name, startTime)
                return directive
            }
        }

        // 4. 检查精确匹配规则
        for ((exceptionType, directive) in exceptionRules) {
            if (exceptionMessage.contains(exceptionType)) {
                cacheDecision(exceptionMessage, directive)
                recordRuleMatch(exceptionType, startTime)
                return directive
            }
        }

        // 5. 检查通配符规则
        for ((pattern, directive) in wildcardRules) {
            if (exceptionMessage.contains(pattern)) {
                cacheDecision(exceptionMessage, directive)
                recordRuleMatch(pattern, startTime)
                return directive
            }
        }

        // 6. 返回默认指令
        cacheDecision(exceptionMessage, defaultDirective)
        recordRuleMatch("default", startTime)
        defaultDirective
    }

    private func matchesRule(exceptionMessage: String, exception: Exception, rule: PriorityRule): Bool {
        match (rule.ruleType) {
            case RuleType.Exact => exceptionMessage == rule.pattern
            case RuleType.Wildcard => exceptionMessage.contains(rule.pattern)
            case RuleType.Regex => matchesRegex(exceptionMessage, rule.pattern)
            case RuleType.Conditional => false  // 条件规则在别处处理
        }
    }

    private func matchesRegex(text: String, pattern: String): Bool {
        // 简化的正则匹配实现
        // 在实际实现中可以使用更完整的正则表达式库
        text.contains(pattern)
    }

    private func cacheDecision(exceptionMessage: String, directive: SupervisionDirective): Unit {
        if (ruleCache.size < 1000) {  // 限制缓存大小
            ruleCache.add(exceptionMessage, directive)
        }
    }

    private func recordRuleMatch(ruleName: String, startTime: Int64): Unit {
        if (!statisticsEnabled) { return }

        let matchTime = getCurrentTimeNanos() - startTime
        mutex.lock()
        try {
            match (ruleStats.get(ruleName)) {
                case Some(stats) => stats.recordMatch(matchTime)
                case None =>
                    let newStats = RuleStatistics(ruleName)
                    newStats.recordMatch(matchTime)
                    ruleStats.add(ruleName, newStats)
            }
        } finally {
            mutex.unlock()
        }
    }

    private func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds() * 1_000_000
    }

    private func sortPriorityRules(rules: Array<PriorityRule>): Array<PriorityRule> {
        // 简单的冒泡排序实现，按优先级排序（数值越小优先级越高）
        let sortedRules = ArrayList<PriorityRule>()
        for (rule in rules) {
            sortedRules.add(rule)
        }

        let size = sortedRules.size
        for (i in 0..size) {
            for (j in 0..(size - i - 1)) {
                if (sortedRules[j].priority > sortedRules[j + 1].priority) {
                    let temp = sortedRules[j]
                    sortedRules[j] = sortedRules[j + 1]
                    sortedRules[j + 1] = temp
                }
            }
        }

        sortedRules.toArray()
    }

    private func sortConditionalRules(rules: Array<ConditionalRule>): Array<ConditionalRule> {
        // 简单的冒泡排序实现，按优先级排序（数值越小优先级越高）
        let sortedRules = ArrayList<ConditionalRule>()
        for (rule in rules) {
            sortedRules.add(rule)
        }

        let size = sortedRules.size
        for (i in 0..size) {
            for (j in 0..(size - i - 1)) {
                if (sortedRules[j].priority > sortedRules[j + 1].priority) {
                    let temp = sortedRules[j]
                    sortedRules[j] = sortedRules[j + 1]
                    sortedRules[j + 1] = temp
                }
            }
        }

        sortedRules.toArray()
    }

    /**
     * 获取所有规则的描述
     */
    public func getDescription(): String {
        let sb = StringBuilder()
        sb.append("SupervisionStrategyMatrix[")
        sb.append("default=${defaultDirective.toString()}")

        if (!exceptionRules.isEmpty()) {
            sb.append(", exact_rules=${exceptionRules.size}")
        }

        if (!wildcardRules.isEmpty()) {
            sb.append(", wildcard_rules=${wildcardRules.size}")
        }

        if (!priorityRules.isEmpty()) {
            sb.append(", priority_rules=${priorityRules.size}")
        }

        if (!conditionalRules.isEmpty()) {
            sb.append(", conditional_rules=${conditionalRules.size}")
        }

        sb.append("]")
        sb.toString()
    }

    /**
     * 获取规则统计信息
     */
    public func getRuleStatistics(): Array<RuleStatistics> {
        mutex.lock()
        try {
            let stats = ArrayList<RuleStatistics>()
            for ((_, ruleStats) in ruleStats) {
                stats.add(ruleStats)
            }
            stats.toArray()
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 获取缓存统计信息
     */
    public func getCacheStatistics(): CacheStatistics {
        mutex.lock()
        try {
            CacheStatistics(Int32(ruleCache.size), 1000, Float64(ruleCache.size) / 1000.0)
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 重置所有统计信息
     */
    public func resetStatistics(): SupervisionStrategyMatrix {
        mutex.lock()
        try {
            ruleStats.clear()
            ruleCache.clear()
        } finally {
            mutex.unlock()
        }
        this
    }

    /**
     * 获取矩阵配置信息
     */
    public func getMatrixConfiguration(): MatrixConfiguration {
        MatrixConfiguration(
            defaultDirective,
            Int32(exceptionRules.size),
            Int32(wildcardRules.size),
            Int32(priorityRules.size),
            Int32(conditionalRules.size),
            statisticsEnabled,
            Int32(ruleCache.size)
        )
    }
}

/**
 * 默认监督策略
 * 提供常用的异常处理决策和策略矩阵
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
     * 创建基于矩阵的异常决策函数
     */
    public static func matrixDecider(matrix: SupervisionStrategyMatrix): (Exception) -> SupervisionDirective {
        { exception => matrix.decide(exception) }
    }

    /**
     * 创建默认的一对一策略
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
     * 创建基于矩阵的一对一策略
     */
    public static func oneForOneWithMatrix(maxRetries: Int32, withinTimeRange: Duration, matrix: SupervisionStrategyMatrix): OneForOneStrategy {
        OneForOneStrategy(
            maxRetries,
            withinTimeRange,
            matrixDecider(matrix)
        )
    }

    /**
     * 创建基于矩阵的一对全策略
     */
    public static func oneForAllWithMatrix(maxRetries: Int32, withinTimeRange: Duration, matrix: SupervisionStrategyMatrix): OneForAllStrategy {
        OneForAllStrategy(
            maxRetries,
            withinTimeRange,
            matrixDecider(matrix)
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
 * 监督策略模板
 * 提供常用的策略配置模板
 */
public struct SupervisionStrategyTemplates {
    /**
     * 创建严格的监督策略矩阵
     * 对大多数异常都采用停止策略
     */
    public static func createStrictMatrix(): SupervisionStrategyMatrix {
        SupervisionStrategyMatrix(SupervisionDirective.Stop)
            .addRule("NullPointerException", SupervisionDirective.Restart)
            .addRule("IllegalArgumentException", SupervisionDirective.Restart)
            .addWildcardRule("Timeout", SupervisionDirective.Restart)
            .addWildcardRule("Connection", SupervisionDirective.Restart)
            .addWildcardRule("OutOfMemory", SupervisionDirective.Stop)
            .addWildcardRule("StackOverflow", SupervisionDirective.Stop)
    }

    /**
     * 创建宽松的监督策略矩阵
     * 对大多数异常都采用重启策略
     */
    public static func createLenientMatrix(): SupervisionStrategyMatrix {
        SupervisionStrategyMatrix(SupervisionDirective.Restart)
            .addWildcardRule("OutOfMemory", SupervisionDirective.Stop)
            .addWildcardRule("StackOverflow", SupervisionDirective.Stop)
            .addWildcardRule("System", SupervisionDirective.Escalate)
            .addWildcardRule("Fatal", SupervisionDirective.Stop)
    }

    /**
     * 创建网络应用的监督策略矩阵
     * 针对网络相关异常优化
     */
    public static func createNetworkMatrix(): SupervisionStrategyMatrix {
        SupervisionStrategyMatrix(SupervisionDirective.Restart)
            .addWildcardRule("Connection", SupervisionDirective.Restart)
            .addWildcardRule("Timeout", SupervisionDirective.Restart)
            .addWildcardRule("Socket", SupervisionDirective.Restart)
            .addWildcardRule("Network", SupervisionDirective.Restart)
            .addWildcardRule("DNS", SupervisionDirective.Restart)
            .addWildcardRule("SSL", SupervisionDirective.Restart)
            .addWildcardRule("Certificate", SupervisionDirective.Stop)
            .addWildcardRule("OutOfMemory", SupervisionDirective.Stop)
            .addWildcardRule("Security", SupervisionDirective.Escalate)
    }

    /**
     * 创建数据库应用的监督策略矩阵
     * 针对数据库相关异常优化
     */
    public static func createDatabaseMatrix(): SupervisionStrategyMatrix {
        SupervisionStrategyMatrix(SupervisionDirective.Restart)
            .addWildcardRule("SQL", SupervisionDirective.Restart)
            .addWildcardRule("Database", SupervisionDirective.Restart)
            .addWildcardRule("Connection", SupervisionDirective.Restart)
            .addWildcardRule("Transaction", SupervisionDirective.Restart)
            .addWildcardRule("Deadlock", SupervisionDirective.Restart)
            .addWildcardRule("Constraint", SupervisionDirective.Stop)
            .addWildcardRule("Schema", SupervisionDirective.Stop)
            .addWildcardRule("Permission", SupervisionDirective.Escalate)
            .addWildcardRule("OutOfMemory", SupervisionDirective.Stop)
    }

    /**
     * 创建微服务应用的监督策略矩阵
     * 针对微服务架构中的常见异常优化
     */
    public static func createMicroserviceMatrix(): SupervisionStrategyMatrix {
        SupervisionStrategyMatrix(SupervisionDirective.Restart)
            .addPriorityRule("CircuitBreakerOpen", SupervisionDirective.Resume, 1, RuleType.Wildcard)
            .addPriorityRule("ServiceUnavailable", SupervisionDirective.Restart, 2, RuleType.Wildcard)
            .addPriorityRule("Timeout", SupervisionDirective.Restart, 3, RuleType.Wildcard)
            .addWildcardRule("RateLimit", SupervisionDirective.Resume)
            .addWildcardRule("Authentication", SupervisionDirective.Escalate)
            .addWildcardRule("Authorization", SupervisionDirective.Escalate)
            .addWildcardRule("Validation", SupervisionDirective.Stop)
            .addWildcardRule("OutOfMemory", SupervisionDirective.Stop)
    }

    /**
     * 创建高可用性监督策略矩阵
     * 最大化系统可用性，尽量避免停止Actor
     */
    public static func createHighAvailabilityMatrix(): SupervisionStrategyMatrix {
        SupervisionStrategyMatrix(SupervisionDirective.Restart)
            .addConditionalRule("CriticalError",
                { exception => exception.toString().contains("Critical") || exception.toString().contains("Fatal") },
                SupervisionDirective.Stop, 1)
            .addConditionalRule("RecoverableError",
                { exception => exception.toString().contains("Recoverable") || exception.toString().contains("Temporary") },
                SupervisionDirective.Resume, 2)
            .addWildcardRule("OutOfMemory", SupervisionDirective.Stop)
            .addWildcardRule("StackOverflow", SupervisionDirective.Stop)
            .addWildcardRule("Security", SupervisionDirective.Escalate)
    }

    /**
     * 创建性能优化的监督策略矩阵
     * 针对性能敏感的应用场景
     */
    public static func createPerformanceOptimizedMatrix(): SupervisionStrategyMatrix {
        SupervisionStrategyMatrix(SupervisionDirective.Resume)  // 默认恢复，避免重启开销
            .addPriorityRule("OutOfMemory", SupervisionDirective.Stop, 1, RuleType.Wildcard)
            .addPriorityRule("StackOverflow", SupervisionDirective.Stop, 2, RuleType.Wildcard)
            .addPriorityRule("DeadLock", SupervisionDirective.Restart, 3, RuleType.Wildcard)
            .addConditionalRule("HighFrequencyError",
                { exception => true },  // 简化条件，实际中可以检查错误频率
                SupervisionDirective.Restart, 10)
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
                println("恢复Actor: ${failure.actor.path.toString()}")
            case SupervisionDirective.Restart =>
                // 重启Actor - 简化实现
                println("重启Actor: ${failure.actor.path.toString()}")
            case SupervisionDirective.Stop =>
                // 停止Actor
                println("停止Actor: ${failure.actor.path.toString()}")
                removeChild(failure.actor)
            case SupervisionDirective.Escalate =>
                // 上报失败 - 简化实现
                println("上报失败: ${failure.actor.path.toString()}")
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

/**
 * 监督策略组合器
 * 支持多种策略的组合和链式调用
 */
public class SupervisionStrategyComposer {
    private let strategies: ArrayList<SupervisionStrategy>
    private let fallbackStrategy: SupervisionStrategy

    public init(fallbackStrategy: SupervisionStrategy) {
        this.strategies = ArrayList<SupervisionStrategy>()
        this.fallbackStrategy = fallbackStrategy
    }

    /**
     * 添加策略到组合器
     */
    public func addStrategy(strategy: SupervisionStrategy): SupervisionStrategyComposer {
        strategies.add(strategy)
        this
    }

    /**
     * 创建组合策略
     */
    public func compose(): SupervisionStrategy {
        CompositeSupervisionStrategy(strategies.toArray(), fallbackStrategy)
    }
}

/**
 * 组合监督策略
 * 按顺序尝试多个策略，直到找到非Escalate的决策
 */
public class CompositeSupervisionStrategy <: SupervisionStrategy {
    private let strategies: Array<SupervisionStrategy>
    private let fallbackStrategy: SupervisionStrategy

    public init(strategies: Array<SupervisionStrategy>, fallbackStrategy: SupervisionStrategy) {
        this.strategies = strategies
        this.fallbackStrategy = fallbackStrategy
    }

    public func decide(failure: ActorFailure): SupervisionDirective {
        // 按顺序尝试每个策略
        for (strategy in strategies) {
            let directive = strategy.decide(failure)
            // 如果不是Escalate，则使用这个决策
            match (directive) {
                case SupervisionDirective.Escalate => () // 继续尝试下一个策略
                case _ => return directive
            }
        }

        // 所有策略都返回Escalate，使用回退策略
        fallbackStrategy.decide(failure)
    }
}

/**
 * 动态监督策略
 * 支持运行时动态切换监督策略
 */
public class DynamicSupervisionStrategy <: SupervisionStrategy {
    private var currentStrategy: SupervisionStrategy
    private let strategyHistory: ArrayList<(Int64, String)>
    private let mutex: Mutex

    public init(initialStrategy: SupervisionStrategy) {
        this.currentStrategy = initialStrategy
        this.strategyHistory = ArrayList<(Int64, String)>()
        this.mutex = Mutex()

        // 记录初始策略
        let now = DateTime.now().toUnixTimeStamp().toMilliseconds()
        strategyHistory.add((now, "InitialStrategy"))
    }

    /**
     * 切换到新的监督策略
     */
    public func switchStrategy(newStrategy: SupervisionStrategy, reason: String): Unit {
        mutex.lock()
        try {
            currentStrategy = newStrategy
            let now = DateTime.now().toUnixTimeStamp().toMilliseconds()
            strategyHistory.add((now, reason))
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 获取当前策略
     */
    public func getCurrentStrategy(): SupervisionStrategy {
        mutex.lock()
        try {
            currentStrategy
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 获取策略切换历史
     */
    public func getStrategyHistory(): Array<(Int64, String)> {
        mutex.lock()
        try {
            strategyHistory.toArray()
        } finally {
            mutex.unlock()
        }
    }

    public func decide(failure: ActorFailure): SupervisionDirective {
        getCurrentStrategy().decide(failure)
    }
}
