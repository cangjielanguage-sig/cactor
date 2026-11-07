/*
 * CActor Core.Supervision - 退避算法单元测试（E6′）
 *
 * BackoffPolicy.next 是纯函数（时钟、随机数都由参数传入），这里用固定的 now / rnd 确定性地验证：
 * 指数增长与封顶、大次数不溢出、抖动范围、次数上限、稳定运行后复位（按实际重启时刻计）、参数校验，以及经典策略的决策分流。
 */
package cactor_core.supervision

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{ActorRef, ActorPath, ActorKilledException, DeathPactException, ActorInitializationException}
import cactor_core.message.{Message}
import cactor_foundation.circuit_breaker.{CircuitBreakerState}

func ms(n: Int64): Duration {
    Duration.millisecond * n
}

/** 依次喂 n 次失败（时刻都取 t0，不触发复位），返回每次的延迟（毫秒）；放弃记为 -1 */
func delaysOf(p: BackoffPolicy, n: Int64, t0: MonoTime): Array<Int64> {
    var state: Option<BackoffState> = None
    let out = Array<Int64>(n, repeat: 0)
    for (i in 0..n) {
        match (p.next(state, t0, 0.0)) {
            case BackoffDecision.RestartIn(d, s) =>
                out[i] = d.toMilliseconds()
                state = Some(s)
            case BackoffDecision.GiveUp =>
                out[i] = -1
                state = None
        }
    }
    out
}

class PathRef <: ActorRef {
    private let p: ActorPath

    init(name: String) {
        this.p = ActorPath("sys", name)
    }

    public prop path: ActorPath {
        get() { p }
    }

    public func tell(message: Message): Unit {}

    public func tell(message: Message, sender: Option<String>): Unit {}

    public func equals(other: ActorRef): Bool {
        other.path.toString() == p.toString()
    }

    public func hashCode(): Int32 {
        0
    }
}

class DbDown <: Exception {
    init() {
        super("db down")
    }
}

func directiveText(d: SupervisionDirective): String {
    match (d) {
        case SupervisionDirective.RestartAfter(x) => "after:${x.toMilliseconds()}"
        case SupervisionDirective.Restart => "restart"
        case SupervisionDirective.Stop => "stop"
        case SupervisionDirective.Resume => "resume"
        case SupervisionDirective.Escalate => "escalate"
    }
}

@Test
public class BackoffPolicyTest {
    @TestCase
    public func delaysDoubleUntilCapped(): Unit {
        let p = BackoffPolicy(ms(10), ms(100), randomFactor: 0.0)
        @Expect(delaysOf(p, 7, MonoTime.now()), [10, 20, 40, 80, 100, 100, 100])
    }

    @TestCase
    public func hugeRestartCountDoesNotOverflow(): Unit {
        // 旧实现 minBackoff * (1 << n)：n ≥ 63 溢出，延迟变成负数或 0
        let p = BackoffPolicy(Duration.nanosecond, Duration.hour, randomFactor: 0.0)
        @Expect(p.delayFor(10_000, 0.0), Duration.hour)
        @Expect(p.delayFor(63, 0.0), Duration.hour)
    }

    @TestCase
    public func jitterOnlyAddsUpToRandomFactor(): Unit {
        let p = BackoffPolicy(ms(100), ms(1000), randomFactor: 0.5)
        @Expect(p.delayFor(0, 0.0), ms(100))
        @Expect(p.delayFor(0, 0.5), ms(125))
        let top = p.delayFor(3, 0.999999)
        // 封顶后的 800ms 再乘 (1 + 0.5 × rnd)：与 Akka 相同，可略超 maxBackoff
        @Expect(top > ms(1199) && top < ms(1200), true)
    }

    @TestCase
    public func givesUpAfterMaxRestarts(): Unit {
        let p = BackoffPolicy(ms(10), ms(100), randomFactor: 0.0, maxRestarts: 3)
        // 3 次退避重启，第 4 次失败放弃；放弃后状态清空，再失败从头开始
        @Expect(delaysOf(p, 5, MonoTime.now()), [10, 20, 40, -1, 10])
        @Expect(delaysOf(BackoffPolicy(ms(10), ms(100), maxRestarts: 0), 1, MonoTime.now()), [-1])
    }

    @TestCase
    public func stableRunResetsCount(): Unit {
        let p = BackoffPolicy(ms(10), ms(1000), randomFactor: 0.0, resetBackoffAfter: Some(ms(50)))
        let t0 = MonoTime.now()
        var s = BackoffState(0, t0)
        // 连续两次失败：10ms、20ms
        if (let BackoffDecision.RestartIn(_, s1) <- p.next(None, t0, 0.0)) {
            s = s1
        }
        if (let BackoffDecision.RestartIn(d, s2) <- p.next(Some(s), t0, 0.0)) {
            @Expect(d, ms(20))
            s = s2
        }
        @Expect(s.restarts, 2)
        // 重启完成后只运行了 49ms 又失败：属于同一轮故障，继续翻倍
        match (p.next(Some(s), s.restartAt + ms(49), 0.0)) {
            case BackoffDecision.RestartIn(d, n) =>
                @Expect(d, ms(40))
                @Expect(n.restarts, 3)
            case _ => @Expect(false, true)
        }
        // 稳定运行满 50ms 后才失败：新一轮故障，从 minBackoff 重新开始
        match (p.next(Some(s), s.restartAt + ms(50), 0.0)) {
            case BackoffDecision.RestartIn(d, n) =>
                @Expect(d, ms(10))
                @Expect(n.restarts, 1)
            case _ => @Expect(false, true)
        }
        // 重启还没完成（延迟期间）就再次失败：不复位
        match (p.next(Some(s), s.restartAt - ms(5), 0.0)) {
            case BackoffDecision.RestartIn(d, _) => @Expect(d, ms(40))
            case _ => @Expect(false, true)
        }
    }

    @TestCase
    public func lateRestartDoesNotLookLikeStableRun(): Unit {
        // 实测（第 5 批 CI）：负载下 40ms 的重启定时器约 100ms 才生效，按"失败时刻 + 延迟"估算的运行时长虚增 60ms，
        // 超过 resetBackoffAfter（缺省 = minBackoff）而被误清零，maxRestarts=3 的 Actor 启动了 6 次（类型化 15 次）
        let p = BackoffPolicy(ms(40), ms(160), randomFactor: 0.0, maxRestarts: 3)
        let t0 = MonoTime.now()
        var s = BackoffState(0, t0)
        if (let BackoffDecision.RestartIn(_, s1) <- p.next(None, t0, 0.0)) {
            s = s1
        }
        // 预计 t0+40ms 重启，实际 t0+100ms 才完成，t0+105ms 又失败：只稳定运行了 5ms，继续翻倍
        let actual = t0 + ms(100)
        match (p.next(Some(s), t0 + ms(105), 0.0, restartedAt: Some(actual))) {
            case BackoffDecision.RestartIn(d, n) =>
                @Expect(d, ms(80))
                @Expect(n.restarts, 2)
            case _ => @Expect(false, true)
        }
        // 不提供实际时刻时按预计时刻算（旧行为）：65ms ≥ 40ms，被清零
        match (p.next(Some(s), t0 + ms(105), 0.0)) {
            case BackoffDecision.RestartIn(d, _) => @Expect(d, ms(40))
            case _ => @Expect(false, true)
        }
        // 实际时刻早于预计（来自更早的一次重启，不可信）：按预计时刻
        match (p.next(Some(s), t0 + ms(105), 0.0, restartedAt: Some(t0 - ms(500)))) {
            case BackoffDecision.RestartIn(d, _) => @Expect(d, ms(40))
            case _ => @Expect(false, true)
        }
        // 经典策略把 ActorFailure.lastRestartAt 交给算法：实际重启时刻晚于预计时，按实际时刻算（端到端见
        // failure_semantics/backoff_semantics_test 的 slowRestartDoesNotResetBackoffCount）
        let strat = BackoffSupervisionStrategy(p)
        let a = PathRef("/user/late")
        @Expect(directiveText(strat.decide(ActorFailure(a, DbDown(), 1))), "after:40")
        @Expect(directiveText(strat.decide(ActorFailure(a, DbDown(), 1, lastRestartAt: Some(MonoTime.now())))), "after:80")
    }

    @TestCase
    public func resetDefaultsToMinBackoff(): Unit {
        @Expect(BackoffPolicy(ms(30), ms(100)).resetBackoffAfter, ms(30))
        @Expect(BackoffPolicy(ms(30), ms(100)).withResetBackoffAfter(ms(7)).resetBackoffAfter, ms(7))
        @Expect(BackoffPolicy(ms(30), ms(100)).withMaxRestarts(4).maxRestarts, 4)
    }

    @TestCase
    public func invalidArgumentsAreRejected(): Unit {
        @ExpectThrows[IllegalArgumentException](BackoffPolicy(Duration.Zero, ms(10)))
        @ExpectThrows[IllegalArgumentException](BackoffPolicy(ms(10), ms(5)))
        @ExpectThrows[IllegalArgumentException](BackoffPolicy(ms(10), ms(50), randomFactor: 1.5))
        @ExpectThrows[IllegalArgumentException](BackoffPolicy(ms(10), ms(50), randomFactor: -0.1))
    }

    @TestCase
    public func classicStrategyRoutesByDecider(): Unit {
        let s = BackoffSupervisionStrategy(BackoffPolicy(ms(10), ms(100), randomFactor: 0.0, maxRestarts: 2))
        let a = PathRef("/user/a")
        // 启动失败也退避重启（退避正是为"依赖未就绪"设计的）
        @Expect(directiveText(s.decide(ActorFailure(a, ActorInitializationException("/user/a", DbDown()), 1))), "after:10")
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "after:20")
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "stop")
        @Expect(s.trackedActors(), 0)
        // Kill 与 DeathPact：停止，且清掉已有进度
        let b = PathRef("/user/b")
        @Expect(directiveText(s.decide(ActorFailure(b, DbDown(), 1))), "after:10")
        @Expect(directiveText(s.decide(ActorFailure(b, ActorKilledException("/user/b"), 1))), "stop")
        @Expect(directiveText(s.decide(ActorFailure(b, DeathPactException("/user/x"), 1))), "stop")
        @Expect(s.trackedActors(), 0)
        // 自定义 decider：Resume 原样返回，不计入退避
        let r = BackoffSupervisionStrategy(BackoffPolicy(ms(10), ms(100)), { _ => SupervisionDirective.Resume })
        @Expect(directiveText(r.decide(ActorFailure(a, DbDown(), 1))), "resume")
        @Expect(r.trackedActors(), 0)
    }

    @TestCase
    public func restartAfterCountsTowardsOneForOneLimit(): Unit {
        // R14：此前 OneForOneStrategy 对 RestartAfter 走 case _ 原样放行，绕过了重启上限
        let s = OneForOneStrategy(2, Duration.minute, { _ => SupervisionDirective.RestartAfter(ms(5)) })
        let a = PathRef("/user/limited")
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "after:5")
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "after:5")
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "stop")
    }

    @TestCase
    public func halfOpenCircuitBreakerStopsOnRestartAfter(): Unit {
        // R14：断路器半开时，基础策略给出的 RestartAfter（退避）同样按重启处理——再失败直接停止并重新打开断路器，
        // 而不是走 case _ 原样放行
        let base = BackoffSupervisionStrategy(BackoffPolicy(ms(5), ms(50), randomFactor: 0.0))
        let s = CircuitBreakerSupervisionStrategy(base, CircuitBreakerSupervisionConfig(1, 1, Duration.second, ms(20)))
        let a = PathRef("/user/breaker")
        // 关闭：照基础策略退避重启，并记一次失败（阈值 1 → 打开）
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "after:5")
        @Expect(s.getCircuitBreakerState(a).isSome(), true)
        // 打开：直接停止
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "stop")
        // 重置超时后进入半开
        let deadline = MonoTime.now() + Duration.second * 5
        while (MonoTime.now() < deadline) {
            if (let Some(CircuitBreakerState.HalfOpen) <- s.getCircuitBreakerState(a)) {
                break
            }
            sleep(ms(5))
        }
        var halfOpen = false
        if (let Some(CircuitBreakerState.HalfOpen) <- s.getCircuitBreakerState(a)) {
            halfOpen = true
        }
        @Expect(halfOpen, true)
        // 半开：基础策略给 RestartAfter，仍然停止，断路器重新打开
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "stop")
        var reopened = false
        if (let Some(CircuitBreakerState.Open) <- s.getCircuitBreakerState(a)) {
            reopened = true
        }
        @Expect(reopened, true)
    }

    @TestCase
    public func zeroMaxRetriesNeverRestarts(): Unit {
        // R13：此前第一次失败总被放行，maxRetries = 0 实际允许重启一次
        let s = OneForOneStrategy(0, Duration.minute, { _ => SupervisionDirective.Restart })
        @Expect(directiveText(s.decide(ActorFailure(PathRef("/user/z"), DbDown(), 1))), "stop")
        let all = OneForAllStrategy(0, Duration.minute, { _ => SupervisionDirective.Restart })
        @Expect(directiveText(all.decide(ActorFailure(PathRef("/user/z"), DbDown(), 1))), "stop")
    }

    @TestCase
    public func legacyBackoffSupervisorStrategyReallyBacksOff(): Unit {
        // R9：旧实现在退避 ≥1ms 时一律返回 Stop
        let s = BackoffSupervisorStrategy(ms(10), ms(80), 0.0, 3)
        let a = PathRef("/user/legacy")
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "after:10")
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "after:20")
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "after:40")
        @Expect(directiveText(s.decide(ActorFailure(a, DbDown(), 1))), "stop")
        @Expect(s.calculateBackoffTime(5), ms(80))
        @Expect(s.getMaxRetries(), 3)
    }
}
