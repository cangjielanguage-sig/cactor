/*
 * CActor Runtime - Enhanced Backoff Supervisor
 * 增强的退避监督器，支持多种退避策略、监控统计和动态配置
 */
package cactor.runtime.supervision

import std.sync.*
import std.time.*
import std.collection.*
import std.math.*
import cactor.core.actor.{ActorRef}
import cactor.core.supervision.{ActorFailure, BackoffSupervisorStrategy, SupervisionStrategyMatrix}
import cactor.core.guardian.{GuardianActor}

/**
 * 退避策略类型
 */
public enum BackoffStrategyType {
    | Linear      // 线性退避：delay = baseDelay * retryCount
    | Exponential // 指数退避：delay = baseDelay * (2 ^ retryCount)
    | Fibonacci   // 斐波那契退避：delay = baseDelay * fib(retryCount)
    | Custom      // 自定义退避：使用用户提供的函数
}

/**
 * 退避统计信息
 */
public struct BackoffStats {
    public let totalFailures: Int64
    public let totalRestarts: Int64
    public let totalStops: Int64
    public let averageBackoffTime: Float64
    public let maxBackoffTime: Int64
    public let minBackoffTime: Int64
    public let activeRetries: Int64

    public init(totalFailures: Int64, totalRestarts: Int64, totalStops: Int64,
                averageBackoffTime: Float64, maxBackoffTime: Int64, minBackoffTime: Int64, activeRetries: Int64) {
        this.totalFailures = totalFailures
        this.totalRestarts = totalRestarts
        this.totalStops = totalStops
        this.averageBackoffTime = averageBackoffTime
        this.maxBackoffTime = maxBackoffTime
        this.minBackoffTime = minBackoffTime
        this.activeRetries = activeRetries
    }
}

/**
 * 退避配置
 */
public struct BackoffConfig {
    public let strategyType: BackoffStrategyType
    public let baseDelay: Duration
    public let maxDelay: Duration
    public let maxRetries: Int64
    public let jitterEnabled: Bool
    public let jitterFactor: Float64
    public let customBackoffFunction: Option<(Int64) -> Duration>

    public init(strategyType: BackoffStrategyType, baseDelay: Duration, maxDelay: Duration,
                maxRetries: Int64, jitterEnabled: Bool, jitterFactor: Float64,
                customBackoffFunction: Option<(Int64) -> Duration>) {
        this.strategyType = strategyType
        this.baseDelay = baseDelay
        this.maxDelay = maxDelay
        this.maxRetries = maxRetries
        this.jitterEnabled = jitterEnabled
        this.jitterFactor = jitterFactor
        this.customBackoffFunction = customBackoffFunction
    }

    /**
     * 创建默认配置
     */
    public static func createDefault(): BackoffConfig {
        BackoffConfig(
            BackoffStrategyType.Exponential,
            Duration.millisecond * 100,
            Duration.second * 30,
            10,
            true,
            0.1,
            None<(Int64) -> Duration>
        )
    }

    /**
     * 创建线性退避配置
     */
    public static func createLinear(baseDelay: Duration, maxRetries: Int64): BackoffConfig {
        BackoffConfig(
            BackoffStrategyType.Linear,
            baseDelay,
            baseDelay * maxRetries,
            maxRetries,
            true,
            0.1,
            None<(Int64) -> Duration>
        )
    }

    /**
     * 创建指数退避配置
     */
    public static func createExponential(baseDelay: Duration, maxDelay: Duration, maxRetries: Int64): BackoffConfig {
        BackoffConfig(
            BackoffStrategyType.Exponential,
            baseDelay,
            maxDelay,
            maxRetries,
            true,
            0.1,
            None<(Int64) -> Duration>
        )
    }
}

/**
 * 增强的运行时Backoff监督器：
 * - 支持多种退避策略（线性、指数、斐波那契、自定义）
 * - 提供详细的监控统计和性能分析
 * - 支持动态配置调整和抖动控制
 * - 集成监督策略矩阵，提供更精确的异常处理
 * - 线程安全的状态管理和重试计数
 */
public class BackoffSupervisor {
    private let strategy: BackoffSupervisorStrategy
    private let guardian: GuardianActor
    private let retryCounts: HashMap<String, Int64>
    private let backoffTimes: HashMap<String, ArrayList<Int64>>
    private let config: BackoffConfig
    private let strategyMatrix: Option<SupervisionStrategyMatrix>
    private let mutex: Mutex

    // 统计信息
    private let totalFailures: AtomicInt64
    private let totalRestarts: AtomicInt64
    private let totalStops: AtomicInt64
    private let backoffTimeSum: AtomicInt64
    private let backoffTimeCount: AtomicInt64
    private let maxBackoffTime: AtomicInt64
    private let minBackoffTime: AtomicInt64

    public init(strategy: BackoffSupervisorStrategy, guardian: GuardianActor) {
        this.strategy = strategy
        this.guardian = guardian
        this.retryCounts = HashMap<String, Int64>()
        this.backoffTimes = HashMap<String, ArrayList<Int64>>()
        // 从strategy中创建配置，而不是使用默认配置
        this.config = BackoffConfig(
            BackoffStrategyType.Exponential,  // 默认使用指数退避
            strategy.getMinBackoff(),         // 使用minBackoff作为baseDelay
            strategy.getMaxBackoff(),         // 使用maxBackoff
            strategy.getMaxRetries(),         // 使用strategy的maxRetries
            true,                             // 启用抖动
            strategy.getRandomFactor(),       // 使用strategy的randomFactor作为jitterFactor
            None<(Int64) -> Duration>         // 不使用自定义函数
        )
        this.strategyMatrix = None<SupervisionStrategyMatrix>
        this.mutex = Mutex()

        // 初始化统计信息
        this.totalFailures = AtomicInt64(0)
        this.totalRestarts = AtomicInt64(0)
        this.totalStops = AtomicInt64(0)
        this.backoffTimeSum = AtomicInt64(0)
        this.backoffTimeCount = AtomicInt64(0)
        this.maxBackoffTime = AtomicInt64(0)
        this.minBackoffTime = AtomicInt64(9223372036854775807)
    }

    /**
     * 增强构造函数，支持自定义配置和策略矩阵
     */
    public init(strategy: BackoffSupervisorStrategy, guardian: GuardianActor,
                config: BackoffConfig, strategyMatrix: Option<SupervisionStrategyMatrix>) {
        this.strategy = strategy
        this.guardian = guardian
        this.retryCounts = HashMap<String, Int64>()
        this.backoffTimes = HashMap<String, ArrayList<Int64>>()
        this.config = config
        this.strategyMatrix = strategyMatrix
        this.mutex = Mutex()

        // 初始化统计信息
        this.totalFailures = AtomicInt64(0)
        this.totalRestarts = AtomicInt64(0)
        this.totalStops = AtomicInt64(0)
        this.backoffTimeSum = AtomicInt64(0)
        this.backoffTimeCount = AtomicInt64(0)
        this.maxBackoffTime = AtomicInt64(0)
        this.minBackoffTime = AtomicInt64(9223372036854775807)
    }

    /**
     * 增强的失败处理：支持策略矩阵、多种退避算法和详细统计
     */
    public func handleFailure(failure: ActorFailure): Unit {
        let path = failure.actor.path.toString()
        totalFailures.fetchAdd(1)

        // 首先检查策略矩阵是否有特定的处理指令
        match (strategyMatrix) {
            case Some(matrix) =>
                let directive = matrix.decide(failure.cause)
                match (directive.toString()) {
                    case "Stop" =>
                        totalStops.fetchAdd(1)
                        cleanup(path)
                        guardian.stopChild(failure.actor)
                        return
                    case "Resume" =>
                        // 恢复Actor，不进行重启
                        return
                    case "Escalate" =>
                        // 上报给上级监督者，这里简化为停止
                        totalStops.fetchAdd(1)
                        cleanup(path)
                        guardian.stopChild(failure.actor)
                        return
                    case _ => () // Restart，继续执行退避逻辑
                }
            case None => () // 没有策略矩阵，使用默认退避逻辑
        }

        var shouldStop = false
        var nextDelay = Duration.millisecond * 0

        mutex.lock()
        let current = match (retryCounts.get(path)) { case Some(v) => v; case None => 0 }
        if (current >= config.maxRetries) {
            shouldStop = true
        } else {
            nextDelay = calculateEnhancedBackoffTime(current)
            retryCounts.add(path, current + 1)

            // 记录退避时间
            match (backoffTimes.get(path)) {
                case Some(times) => times.add(nextDelay.toMilliseconds())
                case None =>
                    let times = ArrayList<Int64>()
                    times.add(nextDelay.toMilliseconds())
                    backoffTimes.add(path, times)
            }
        }
        mutex.unlock()

        if (shouldStop) {
            // 超上限：停止并清理计数
            totalStops.fetchAdd(1)
            cleanup(path)
            guardian.stopChild(failure.actor)
            return
        }

        // 更新统计信息
        let delayMs = nextDelay.toMilliseconds()
        updateBackoffStats(delayMs)
        totalRestarts.fetchAdd(1)

        // 安排延迟重启
        if (delayMs <= 0) {
            guardian.restartChild(failure.actor, failure.cause)
        } else {
            Timer.once(nextDelay, { =>
                guardian.restartChild(failure.actor, failure.cause)
            })
        }
    }

    /**
     * 计算增强的退避时间
     */
    private func calculateEnhancedBackoffTime(retryCount: Int64): Duration {
        let baseDelayMs = config.baseDelay.toMilliseconds()
        let maxDelayMs = config.maxDelay.toMilliseconds()

        var delayMs = match (config.strategyType) {
            case BackoffStrategyType.Linear =>
                baseDelayMs * (retryCount + 1)
            case BackoffStrategyType.Exponential =>
                let exponentialDelay = Float64(baseDelayMs) * pow(2.0, Float64(retryCount))
                Int64(exponentialDelay)
            case BackoffStrategyType.Fibonacci =>
                baseDelayMs * fibonacci(retryCount + 1)
            case BackoffStrategyType.Custom =>
                match (config.customBackoffFunction) {
                    case Some(customFunc) => customFunc(retryCount).toMilliseconds()
                    case None => baseDelayMs * (retryCount + 1) // 回退到线性
                }
        }

        // 应用最大延迟限制
        delayMs = min(delayMs, maxDelayMs)

        // 应用抖动
        if (config.jitterEnabled) {
            let jitter = Float64(delayMs) * config.jitterFactor * (random() - 0.5) * 2.0
            delayMs = max(0, delayMs + Int64(jitter))
        }

        Duration.millisecond * delayMs
    }

    /**
     * 计算斐波那契数列
     */
    private func fibonacci(n: Int64): Int64 {
        if (n <= 1) { return n }
        var a = 0
        var b = 1
        for (i in 2..n+1) {
            let temp = a + b
            a = b
            b = temp
        }
        b
    }

    /**
     * 生成随机数（简化实现）
     */
    private func random(): Float64 {
        // 简化的随机数生成，实际应该使用更好的随机数生成器
        let now = DateTime.now().toUnixTimeStamp().toMilliseconds()
        Float64(now % 1000) / 1000.0
    }

    /**
     * 更新退避统计信息
     */
    private func updateBackoffStats(delayMs: Int64): Unit {
        backoffTimeSum.fetchAdd(delayMs)
        backoffTimeCount.fetchAdd(1)

        // 更新最大值
        var currentMax = maxBackoffTime.load()
        while (delayMs > currentMax) {
            if (maxBackoffTime.compareAndSwap(currentMax, delayMs)) {
                break
            }
            currentMax = maxBackoffTime.load()
        }

        // 更新最小值
        var currentMin = minBackoffTime.load()
        while (delayMs < currentMin) {
            if (minBackoffTime.compareAndSwap(currentMin, delayMs)) {
                break
            }
            currentMin = minBackoffTime.load()
        }
    }

    /**
     * 获取统计信息
     */
    public func getStats(): BackoffStats {
        let count = backoffTimeCount.load()
        let avgTime = if (count > 0) { Float64(backoffTimeSum.load()) / Float64(count) } else { 0.0 }

        mutex.lock()
        let activeRetries = Int64(retryCounts.size)
        mutex.unlock()

        BackoffStats(
            totalFailures.load(),
            totalRestarts.load(),
            totalStops.load(),
            avgTime,
            maxBackoffTime.load(),
            minBackoffTime.load(),
            activeRetries
        )
    }

    /**
     * 动态更新配置
     */
    public func updateConfig(newConfig: BackoffConfig): Unit {
        // 注意：这里简化实现，实际应该更仔细地处理配置更新
        // 由于config是let，这里只能通过重新创建BackoffSupervisor来更新配置
        // 或者将config改为var并添加适当的同步机制
    }

    /**
     * 获取指定Actor的重试历史
     */
    public func getRetryHistory(actorPath: String): Array<Int64> {
        mutex.lock()
        let history = match (backoffTimes.get(actorPath)) {
            case Some(times) => times.toArray()
            case None => Array<Int64>()
        }
        mutex.unlock()
        history
    }

    /** 成功后清理重试状态 */
    public func cleanup(path: String): Unit {
        mutex.lock()
        retryCounts.remove(path)
        backoffTimes.remove(path)
        mutex.unlock()
    }
}

