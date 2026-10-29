/*
 * CActor Cluster - Phi Accrual 故障检测器（Hayashibara et al., 2004；与 Akka 的实现一致）
 *
 * 不给出“活 / 死”的二值判断，而是根据心跳间隔的历史分布计算怀疑度 φ：
 *   φ = -log10(P(下一次心跳在 now 之后才到达))
 * 间隔分布用正态分布近似（均值、标准差取滑动窗口），并用逻辑函数近似正态分布的累积分布函数。
 * φ 超过阈值（Akka 默认 8）即认为节点不可用。网络抖动越大，同样的停顿产生的 φ 越小，误判更少。
 *
 * 时间通过 clock 注入（毫秒），测试中可以使用手动时钟。
 */
package cactor_cluster.detector

import std.math.*
import std.sync.*
import std.time.*
import std.collection.*

let monoBase: MonoTime = MonoTime.now()

/** 单调时钟（毫秒） */
public func monotonicMillis(): Int64 {
    (MonoTime.now() - monoBase).toMilliseconds()
}

/** 心跳间隔的滑动窗口统计 */
public class HeartbeatHistory {
    private let maxSampleSize: Int64
    private let intervals = ArrayList<Int64>()
    private var intervalSum: Int64 = 0
    private var squaredIntervalSum: Float64 = 0.0

    public init(maxSampleSize: Int64) {
        if (maxSampleSize <= 0) {
            throw IllegalArgumentException("maxSampleSize 必须为正数")
        }
        this.maxSampleSize = maxSampleSize
    }

    public func add(interval: Int64): Unit {
        if (intervals.size >= maxSampleSize) {
            let dropped = intervals.remove(at: 0)
            intervalSum -= dropped
            squaredIntervalSum -= Float64(dropped) * Float64(dropped)
        }
        intervals.add(interval)
        intervalSum += interval
        squaredIntervalSum += Float64(interval) * Float64(interval)
    }

    public func size(): Int64 {
        intervals.size
    }

    public func mean(): Float64 {
        if (intervals.size == 0) { 0.0 } else { Float64(intervalSum) / Float64(intervals.size) }
    }

    public func variance(): Float64 {
        if (intervals.size == 0) {
            return 0.0
        }
        let m = mean()
        let v = squaredIntervalSum / Float64(intervals.size) - m * m
        if (v < 0.0) { 0.0 } else { v }
    }

    public func stdDeviation(): Float64 {
        sqrt(variance())
    }
}

/**
 * Phi Accrual 故障检测器（单个被监测节点）
 */
public class PhiAccrualFailureDetector {
    public let threshold: Float64
    private let minStdDeviationMillis: Float64
    private let acceptableHeartbeatPauseMillis: Int64
    private let clock: () -> Int64
    private let lock = Mutex()
    private let history: HeartbeatHistory
    private var lastTimestamp: Option<Int64> = None<Int64>

    /**
     * @param threshold 判定不可用的 φ 阈值（Akka 默认 8.0）
     * @param maxSampleSize 统计窗口大小（默认 1000）
     * @param minStdDeviation 标准差下限，避免心跳非常规律时 φ 过于敏感（默认 100ms）
     * @param acceptableHeartbeatPause 可以容忍的停顿（例如 GC），计入均值（默认 3s）
     * @param firstHeartbeatEstimate 只有一次心跳时对间隔的估计（默认 1s）
     * @param clock 当前时间（毫秒）
     */
    public init(threshold: Float64, maxSampleSize: Int64, minStdDeviation: Duration, acceptableHeartbeatPause: Duration,
        firstHeartbeatEstimate: Duration, clock: () -> Int64) {
        if (threshold <= 0.0) {
            throw IllegalArgumentException("threshold 必须为正数")
        }
        this.threshold = threshold
        this.minStdDeviationMillis = Float64(minStdDeviation.toMilliseconds())
        this.acceptableHeartbeatPauseMillis = acceptableHeartbeatPause.toMilliseconds()
        this.clock = clock
        this.history = HeartbeatHistory(maxSampleSize)
        // 用首次估计初始化窗口：均值为估计值，标准差为其 1/4
        let est = firstHeartbeatEstimate.toMilliseconds()
        let dev = est / 4
        history.add(est - dev)
        history.add(est + dev)
    }

    /** Akka 默认参数，使用单调时钟 */
    public init() {
        this(8.0, 1000, Duration.millisecond * 100, Duration.second * 3, Duration.second, monotonicMillis)
    }

    /** 记录一次心跳 */
    public func heartbeat(): Unit {
        let now = clock()
        synchronized(lock) {
            if (let Some(last) <- lastTimestamp) {
                let interval = now - last
                // 只在节点被认为可用时记录间隔，避免长时间中断后的巨大间隔污染统计
                if (phiAt(now, last) < threshold) {
                    history.add(interval)
                }
            }
            lastTimestamp = Some(now)
        }
    }

    /** 当前的怀疑度 φ；还没有收到心跳时为 0 */
    public func phi(): Float64 {
        let now = clock()
        synchronized(lock) {
            match (lastTimestamp) {
                case Some(last) => phiAt(now, last)
                case None => 0.0
            }
        }
    }

    /** φ 低于阈值时认为可用；还没有收到心跳时认为可用 */
    public func isAvailable(): Bool {
        phi() < threshold
    }

    /** 是否收到过心跳 */
    public func isMonitoring(): Bool {
        synchronized(lock) {
            lastTimestamp.isSome()
        }
    }

    private func phiAt(now: Int64, last: Int64): Float64 {
        let elapsed = Float64(now - last)
        let mean = history.mean() + Float64(acceptableHeartbeatPauseMillis)
        let sd = max(history.stdDeviation(), minStdDeviationMillis)
        PhiAccrualFailureDetector.phiOf(elapsed, mean, sd)
    }

    /**
     * φ = -log10(1 - F(elapsed))，F 用逻辑函数近似正态分布的累积分布函数（误差 < 0.0002）
     */
    public static func phiOf(elapsed: Float64, mean: Float64, stdDeviation: Float64): Float64 {
        let y = (elapsed - mean) / stdDeviation
        let e = exp(-y * (1.5976 + 0.070566 * y * y))
        if (elapsed > mean) {
            -log10(e / (1.0 + e))
        } else {
            -log10(1.0 - 1.0 / (1.0 + e))
        }
    }
}

/**
 * 多个节点的故障检测器注册表
 */
public class FailureDetectorRegistry {
    private let lock = Mutex()
    private let detectors = HashMap<String, PhiAccrualFailureDetector>()
    private let factory: () -> PhiAccrualFailureDetector

    public init(factory: () -> PhiAccrualFailureDetector) {
        this.factory = factory
    }

    public func heartbeat(node: String): Unit {
        let d = synchronized(lock) {
            match (detectors.get(node)) {
                case Some(d) => d
                case None =>
                    let d = factory()
                    detectors.add(node, d)
                    d
            }
        }
        d.heartbeat()
    }

    /** 未被监测的节点视为可用 */
    public func isAvailable(node: String): Bool {
        match (synchronized(lock) { detectors.get(node) }) {
            case Some(d) => d.isAvailable()
            case None => true
        }
    }

    public func phi(node: String): Float64 {
        match (synchronized(lock) { detectors.get(node) }) {
            case Some(d) => d.phi()
            case None => 0.0
        }
    }

    public func remove(node: String): Unit {
        synchronized(lock) {
            detectors.remove(node)
        }
    }

    /** 当前不可用的节点 */
    public func unavailableNodes(): Array<String> {
        let all = synchronized(lock) { detectors.toArray() }
        let result = ArrayList<String>()
        for ((node, d) in all) {
            if (!d.isAvailable()) {
                result.add(node)
            }
        }
        result.toArray()
    }
}
