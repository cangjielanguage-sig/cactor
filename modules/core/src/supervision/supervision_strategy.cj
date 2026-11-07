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
import cactor_core.actor.{ActorRef, ActorInitializationException, ActorKilledException, DeathPactException}
// 监督的核心类型定义在 cactor_core.actor（供 Actor / Props 引用而不成环），这里重导出以保持兼容
public import cactor_core.actor.{SupervisionDirective, ActorFailure, SupervisionStrategy}
import cactor_foundation.logging.{Logger, Logging}

let log_supervision_strategy: Logger = Logging.logger("cactor_core.supervision.supervision_strategy")

/** 单调时钟毫秒数，+1 使"未设置"可以用 0 表示（墙钟会被 NTP 回拨，时间窗口不能用它算） */
let retryEpoch: MonoTime = MonoTime.now()

func retryNowMillis(): Int64 {
    (MonoTime.now() - retryEpoch).toMilliseconds() + 1
}

/**
 * 重试信息
 *
 * 同一个策略实例可能被多个父 Actor 共享，decide 会在不同线程并发调用（v1 A3）：
 * 窗口起点用 CAS 只设置一次，窗口过期也只由 CAS 成功的一方清零，不会"一方刚记的失败被另一方的复位抹掉"。
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
        let now = retryNowMillis()
        retryCount.fetchAdd(1)
        lastFailureTime.store(now)
        firstFailureTime.compareAndSwap(0, now) |> ignoreSwap
    }

    public func shouldRetry(maxRetries: Int32, withinTimeRange: Duration): Bool {
        // 负数表示不限次数（与 Akka 的 maxNrOfRetries = -1 相同）
        if (maxRetries < 0) {
            return true
        }
        let firstFailure = firstFailureTime.load()
        // 时间窗口已过：计数从头开始。只有把窗口起点从旧值换成 0 的那一方清零计数
        if (firstFailure != 0 && retryNowMillis() - firstFailure > withinTimeRange.toMilliseconds()) {
            if (firstFailureTime.compareAndSwap(firstFailure, 0)) {
                retryCount.store(0)
            }
        }
        // 与 Akka 一致：maxRetries = 0 表示不重启（此前第一次失败总被放行，0 实际等于 1，R13）
        retryCount.load() < Int64(maxRetries)
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

func ignoreSwap(_: Bool): Unit {}

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
            // RestartAfter 与 Restart 一样计入重启上限（此前 case _ 原样放行，绕过了上限，R14）
            case SupervisionDirective.Restart | SupervisionDirective.RestartAfter(_) =>
                let retryInfo = getOrCreateRetryInfo(failure.actor)

                if (retryInfo.shouldRetry(maxRetries, withinTimeRange)) {
                    retryInfo.recordRetry()
                    directive
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
            case SupervisionDirective.Restart | SupervisionDirective.RestartAfter(_) =>
                if (globalRetryInfo.shouldRetry(maxRetries, withinTimeRange)) {
                    globalRetryInfo.recordRetry()
                    directive
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
 * 旧版退避策略，保留为 BackoffSupervisionStrategy 的薄封装（E6′）。
 *
 * 此前是假实现：先设"下次重试时间 = 现在 + 退避"再判断"现在 >= 下次重试时间"，退避 ≥1 ms 时一律返回 Stop；
 * 抖动用的 Math.random() 恒为 0.5（R9）。现在委托统一实现，返回 RestartAfter(退避延迟)，并带 Akka 式抖动与自动复位。
 */
@Deprecated["请改用 BackoffSupervisionStrategy(BackoffPolicy(...))", since: "1.2.0"]
public class BackoffSupervisorStrategy <: SupervisionStrategy {
    private let inner: BackoffSupervisionStrategy

    /** randomFactor 0.2、最多重启 10 次（与旧版缺省值相同） */
    public init(minBackoff: Duration, maxBackoff: Duration) {
        this(minBackoff, maxBackoff, 0.2, 10)
    }

    public init(minBackoff: Duration, maxBackoff: Duration, randomFactor: Float64, maxRetries: Int64) {
        this.inner = BackoffSupervisionStrategy(
            BackoffPolicy(minBackoff, maxBackoff, randomFactor: randomFactor, maxRestarts: maxRetries))
    }

    public func decide(failure: ActorFailure): SupervisionDirective {
        inner.decide(failure)
    }

    /** 第 retryCount 次重启的延迟（含随机抖动） */
    public func calculateBackoffTime(retryCount: Int64): Duration {
        let p = inner.policy
        p.delayFor(retryCount, if (p.randomFactor > 0.0) { nextJitter() } else { 0.0 })
    }

    public func getMaxRetries(): Int64 {
        inner.policy.maxRestarts
    }

    public func resetBackoffInfo(actorKey: String): Unit {
        inner.resetBackoff(actorKey)
    }

    public func getBackoffStats(): String {
        "BackoffSupervisorStrategy - ${inner.policy}, 跟踪Actor数: ${inner.trackedActors()}"
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
        // 与 Akka 经典 API 的默认 decider 一致（E27）：
        // - 启动失败（ActorInitializationException）：重启只会再次失败，停止；
        // - Kill（ActorKilledException）：Kill 的语义就是停止该 Actor；
        // - 未处理 Terminated（DeathPactException）：被监视者已死，监视者随之停止；
        // - 其他异常：重启。
        match (exception) {
            case _: ActorInitializationException => SupervisionDirective.Stop
            case _: ActorKilledException => SupervisionDirective.Stop
            case _: DeathPactException => SupervisionDirective.Stop
            case _ => SupervisionDirective.Restart
        }
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
            case SupervisionDirective.RestartAfter(delay) =>
                log_supervision_strategy.error("${delay.toMilliseconds()}ms 后重启Actor: ${failure.actor.path.toString()}")
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
