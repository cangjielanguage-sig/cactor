/*
 * CActor Core.Supervision - 指数退避重启（计划 E6′，对标 Akka BackoffOpts / SupervisorStrategy.restartWithBackoff）
 *
 * 经典 BackoffSupervisionStrategy 与类型化 TypedSupervisorStrategy.restartWithBackoff 共用 BackoffPolicy.next，
 * 两套 API 的退避行为因此完全一致：
 *   - 第 n 次重启（n 从 0 计）的延迟 = min(maxBackoff, minBackoff × 2^n) × (1 + rnd × randomFactor)，与 Akka 相同；
 *   - 重启后稳定运行 resetBackoffAfter 仍未失败，计数清零（Akka 的 resetBackoffAfter / AutoReset）；
 *   - 计数达到 maxRestarts（<0 不限）后放弃，由调用方停止 Actor。
 * 退避进度是不可变值（BackoffState）；next 的时钟与随机数都由参数传入，因此算法可以零 sleep 地确定性测试。
 *
 * 延迟本身由运行时执行：策略返回 SupervisionDirective.RestartAfter(delay)，Cell 挂起（延迟期间不处理用户消息），
 * 到期后在自己的线程上重建实例；延迟期间被停止的 Actor 不会复活（见 runtime actor_cell.restartInCell）。
 */
package cactor_core.supervision

import std.time.*
import std.sync.*
import std.random.*
import std.collection.*
import cactor_core.actor.{ActorRef, ActorKilledException, DeathPactException}

/** 一个 Actor 的退避进度（不可变；持有者在自己的锁内替换） */
public struct BackoffState {
    /** 自上次复位以来已安排的重启次数 */
    public let restarts: Int64
    /** 最近一次重启的预计完成时刻（失败时刻 + 延迟），稳定运行时长从此刻算起 */
    public let restartAt: MonoTime

    public init(restarts: Int64, restartAt: MonoTime) {
        this.restarts = restarts
        this.restartAt = restartAt
    }
}

/** 一次失败之后的退避决定 */
public enum BackoffDecision {
    | RestartIn(Duration, BackoffState)
    | GiveUp
}

// 抖动随机数：只在失败路径（冷路径）上取，一把全局锁足够，也避免每个 Actor 各建一个 Random
let jitterLock = Mutex()
let jitterRandom = Random()

func nextJitter(): Float64 {
    synchronized(jitterLock) {
        jitterRandom.nextFloat64()
    }
}

/**
 * 退避参数（不可变值类型）。
 *
 * resetBackoffAfter 缺省等于 minBackoff（与 Akka 相同）；传 Duration.Max 表示永不自动复位。
 */
public struct BackoffPolicy <: ToString {
    public let minBackoff: Duration
    public let maxBackoff: Duration
    public let randomFactor: Float64
    public let maxRestarts: Int64
    public let resetBackoffAfter: Duration

    public init(minBackoff: Duration, maxBackoff: Duration, randomFactor!: Float64 = 0.2, maxRestarts!: Int64 = -1,
        resetBackoffAfter!: Option<Duration> = None) {
        if (minBackoff <= Duration.Zero) {
            throw IllegalArgumentException("minBackoff 必须大于 0，实际 ${minBackoff}")
        }
        if (maxBackoff < minBackoff) {
            throw IllegalArgumentException("maxBackoff（${maxBackoff}）不能小于 minBackoff（${minBackoff}）")
        }
        if (randomFactor < 0.0 || randomFactor > 1.0) {
            throw IllegalArgumentException("randomFactor 必须在 [0, 1] 内，实际 ${randomFactor}")
        }
        this.minBackoff = minBackoff
        this.maxBackoff = maxBackoff
        this.randomFactor = randomFactor
        this.maxRestarts = maxRestarts
        this.resetBackoffAfter = resetBackoffAfter ?? minBackoff
    }

    /** 达到 n 次重启后放弃（n < 0 不限） */
    public func withMaxRestarts(n: Int64): BackoffPolicy {
        BackoffPolicy(minBackoff, maxBackoff, randomFactor: randomFactor, maxRestarts: n,
            resetBackoffAfter: Some(resetBackoffAfter))
    }

    /** 重启后稳定运行 d 未再失败，退避计数清零 */
    public func withResetBackoffAfter(d: Duration): BackoffPolicy {
        BackoffPolicy(minBackoff, maxBackoff, randomFactor: randomFactor, maxRestarts: maxRestarts,
            resetBackoffAfter: Some(d))
    }

    /** 第 restartCount 次（从 0 计）重启的延迟；rnd ∈ [0, 1) 是抖动用的随机数 */
    public func delayFor(restartCount: Int64, rnd: Float64): Duration {
        let maxNs = maxBackoff.toNanoseconds()
        var ns = minBackoff.toNanoseconds()
        var i: Int64 = 0
        // 逐次翻倍并封顶，而不是算 2^n：n 很大时不会溢出
        while (i < restartCount && ns < maxNs) {
            ns = if (ns > maxNs / 2) { maxNs } else { ns * 2 }
            i++
        }
        if (randomFactor <= 0.0) {
            return Duration.nanosecond * ns
        }
        // 与 Akka 相同：抖动只向上加，可以略超 maxBackoff（错开同时失败的一批 Actor）
        let jittered = Float64(ns) * (1.0 + rnd * randomFactor)
        if (jittered >= 9.0e18) {
            maxBackoff
        } else {
            Duration.nanosecond * Int64(jittered)
        }
    }

    /**
     * 记录一次发生在 now 的失败，给出下一步（纯函数：不读时钟、不取随机数）。
     *
     * restartedAt 是运行时报告的"上次重启实际完成的时刻"（`ActorFailure.lastRestartAt`）。稳定运行时长从
     * 它与预计时刻（失败时刻 + 延迟）中**较晚**的一个算起：定时器或调度晚到时实际重启更晚，若按预计时刻算，
     * 运行时长会虚增、计数被误清零，负载下 maxRestarts 永远到不了；实际时刻比预计还早（来自更早的重启）则不可信，用预计时刻
     */
    public func next(prev: Option<BackoffState>, now: MonoTime, rnd: Float64,
        restartedAt!: Option<MonoTime> = None): BackoffDecision {
        let count = match (prev) {
            // 上次重启后已稳定运行 resetBackoffAfter：这是一次新故障，从 minBackoff 重新开始
            case Some(s) where now - stableSince(s, restartedAt) >= resetBackoffAfter => 0
            case Some(s) => s.restarts
            case None => 0
        }
        if (maxRestarts >= 0 && count >= maxRestarts) {
            return BackoffDecision.GiveUp
        }
        let delay = delayFor(count, rnd)
        BackoffDecision.RestartIn(delay, BackoffState(count + 1, now + delay))
    }

    /** 以当前时刻和随机抖动记录一次失败；restartedAt 见上 */
    public func next(prev: Option<BackoffState>, restartedAt!: Option<MonoTime> = None): BackoffDecision {
        let rnd = if (randomFactor > 0.0) { nextJitter() } else { 0.0 }
        next(prev, MonoTime.now(), rnd, restartedAt: restartedAt)
    }

    public func toString(): String {
        "backoff(min=${minBackoff}, max=${maxBackoff}, randomFactor=${randomFactor}, maxRestarts=${maxRestarts}, resetAfter=${resetBackoffAfter})"
    }
}

/** 稳定运行的起点：实际重启时刻与预计时刻中较晚的一个 */
func stableSince(s: BackoffState, restartedAt: Option<MonoTime>): MonoTime {
    match (restartedAt) {
        case Some(t) where t > s.restartAt => t
        case _ => s.restartAt
    }
}

/**
 * 经典 API 的退避监督策略：决策为重启时返回 RestartAfter(退避延迟)，计数超限时返回 Stop。
 *
 * 一个策略实例可以被多个父 Actor 共享，每个子 Actor 按路径各自计数；状态只在锁内读写，锁内不调用用户代码。
 */
public class BackoffSupervisionStrategy <: SupervisionStrategy {
    public let policy: BackoffPolicy
    private let decider: (Exception) -> SupervisionDirective
    private let lock = Mutex()
    private let states = HashMap<String, BackoffState>()

    /**
     * 兼容构造（E6 版本的语义）：不加抖动、不自动复位，maxRetries 是总重启次数上限。
     * 新代码请用 BackoffSupervisionStrategy(BackoffPolicy(...))，获得 Akka 的抖动与自动复位。
     */
    public init(initialDelay: Duration, maxDelay: Duration, maxRetries: Int32) {
        this(BackoffPolicy(initialDelay, maxDelay, randomFactor: 0.0, maxRestarts: Int64(maxRetries),
            resetBackoffAfter: Some(Duration.Max)))
    }

    public init(policy: BackoffPolicy) {
        this(policy, BackoffSupervisionStrategy.defaultDecider)
    }

    /** decider 决定哪些异常走退避重启：它返回 Restart / RestartAfter 时按退避延迟重启，其余指令原样返回 */
    public init(policy: BackoffPolicy, decider: (Exception) -> SupervisionDirective) {
        this.policy = policy
        this.decider = decider
    }

    /**
     * 默认决策：Kill 与 DeathPact 停止，其余异常（包括启动失败）都退避重启。
     * 与 Akka 一致：退避重启正是为"启动时依赖还没就绪"这类失败设计的，因此启动失败也重试。
     */
    public static func defaultDecider(e: Exception): SupervisionDirective {
        match (e) {
            case _: ActorKilledException => SupervisionDirective.Stop
            case _: DeathPactException => SupervisionDirective.Stop
            case _ => SupervisionDirective.Restart
        }
    }

    public func decide(failure: ActorFailure): SupervisionDirective {
        let key = failure.actor.path.toString()
        match (decider(failure.cause)) {
            case SupervisionDirective.Restart | SupervisionDirective.RestartAfter(_) => ()
            case SupervisionDirective.Stop =>
                forget(key)
                return SupervisionDirective.Stop
            case other => return other
        }
        let decision = synchronized(lock) {
            let d = policy.next(states.get(key), restartedAt: failure.lastRestartAt)
            match (d) {
                case BackoffDecision.RestartIn(_, s) => states[key] = s
                // 放弃后清理：之后再失败（例如被重新创建的同名 Actor）从 minBackoff 重新开始
                case BackoffDecision.GiveUp => states.remove(key)
            }
            d
        }
        match (decision) {
            case BackoffDecision.RestartIn(delay, _) => SupervisionDirective.RestartAfter(delay)
            case BackoffDecision.GiveUp =>
                log_supervision_strategy.warn("[BACKOFF] Actor ${key} 已达最大重启次数 ${policy.maxRestarts}，停止")
                SupervisionDirective.Stop
        }
    }

    /** 手动清零某个 Actor 的退避进度（通常不需要：稳定运行 resetBackoffAfter 后会自动清零） */
    public func resetBackoff(actor: ActorRef): Unit {
        forget(actor.path.toString())
    }

    /** 按路径清零（路径格式与 ActorRef.path.toString() 相同） */
    public func resetBackoff(actorPath: String): Unit {
        forget(actorPath)
    }

    /** 当前有退避进度的 Actor 数 */
    public func trackedActors(): Int64 {
        synchronized(lock) {
            states.size
        }
    }

    private func forget(key: String): Unit {
        synchronized(lock) {
            states.remove(key)
        }
    }
}
