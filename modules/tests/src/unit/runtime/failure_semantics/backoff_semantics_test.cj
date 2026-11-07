/*
 * CActor - 退避重启的端到端语义（E6′，对照 Akka restartWithBackoff）
 *
 *   - 经典 BackoffSupervisionStrategy 与类型化 restartWithBackoff 在真实运行时上的重启间隔一致，按指数增长，
 *     达到上限后停止（此前经典旧策略恒返回 Stop（R9），类型化 API 没有 backoff）
 *   - 退避等待期间 Actor 挂起：等待期间到达的消息由重启后的新实例处理，不丢失、也不被旧实例处理
 *   - 类型化 setup 失败：不限次数的 restart 不重试（Akka 语义，避免无限重启），限次 restart 照常重试；
 *     此前监督规则在 setup 之后才登记，首次 setup 失败时任何策略都不生效
 */
package cactor_tests.unit.runtime.failure_semantics

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}
import cactor_core.supervision.{BackoffSupervisionStrategy, BackoffPolicy}
import cactor_core.typed.{TypedBehavior, TypedBehaviors, TypedSupervisorStrategy, TypedActorRef, TypedAsk}
import cactor_testkit.*

/** 线程安全的时间戳记录 */
class Stamps {
    let lock = Mutex()
    let items = ArrayList<MonoTime>()

    func add(): Unit {
        synchronized(lock) { items.add(MonoTime.now()) }
    }

    func size(): Int64 {
        synchronized(lock) { items.size }
    }

    /** 相邻两次的间隔（毫秒） */
    func gapsMs(): Array<Int64> {
        synchronized(lock) {
            let out = ArrayList<Int64>()
            for (i in 1..items.size) {
                out.add((items[i] - items[i - 1]).toMilliseconds())
            }
            out.toArray()
        }
    }
}

func millis(n: Int64): Duration {
    Duration.millisecond * n
}

/** postStop 要花 stopMs 毫秒（模拟释放连接）：父 Actor 重启时要等它终止 */
class SlowStopChild <: Actor {
    let stopMs: Int64

    init(stopMs: Int64) {
        this.stopMs = stopMs
    }

    public prop name: String {
        get() { "slow-stop" }
    }

    public prop description: String {
        get() { "slow-stop" }
    }

    public func postStop(): Unit {
        let _ = awaitCondition(millis(stopMs), { => false })
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

func boIgnore(_: ActorRef): Unit {}

/** 对照用：把失败信息里的实际重启时刻去掉再交给内层策略 */
class WithoutRestartTime <: SupervisionStrategy {
    let inner: SupervisionStrategy

    init(inner: SupervisionStrategy) {
        this.inner = inner
    }

    public func decide(failure: ActorFailure): SupervisionDirective {
        inner.decide(ActorFailure(failure.actor, failure.cause, failure.failureCount))
    }
}

/**
 * 每个间隔都不短于对应的退避延迟（定时器只会晚、不会早，留 2ms 时钟粒度余量），
 * 也不会长到离谱（说明重启没有卡住）；并且确实在增长。
 */
func expectBackoffGaps(gaps: Array<Int64>, expected: Array<Int64>): Unit {
    @Expect(gaps.size, expected.size)
    if (gaps.size != expected.size) {
        return
    }
    for (i in 0..gaps.size) {
        @Expect(gaps[i] >= expected[i] - 2, true)
        @Expect(gaps[i] < expected[i] + 2000, true)
    }
    @Expect(gaps[gaps.size - 1] > gaps[0], true)
}

enum BoCmd {
    | Add(Int64)
    | Get(TypedActorRef<Int64>)
    | Crash
}

func boCounter(n: Int64): TypedBehavior<BoCmd> {
    TypedBehaviors.receive<BoCmd>({ _, cmd =>
        match (cmd) {
            case BoCmd.Add(k) => boCounter(n + k)
            case BoCmd.Get(replyTo) =>
                replyTo.tell(n)
                TypedBehaviors.same<BoCmd>()
            case BoCmd.Crash => throw Boom("crash")
        }
    })
}

func boCount(ref: TypedActorRef<BoCmd>): Int64 {
    TypedAsk.ask<BoCmd, Int64>(ref, { r => BoCmd.Get(r) }, T).get(T)
}

@Test
public class BackoffSemanticsTest {
    @TestCase
    public func classicBackoffRestartsWithGrowingDelaysThenStops(): Unit {
        let kit = ActorTestKit("bo-classic")
        let rec = EventRecorder()
        let starts = Stamps()
        let props = PropsFactory.create<Actor>({ => Subject("db", rec, { _, _, _ => handled() }, { _, _ =>
            starts.add()
            throw Boom("数据库未就绪")
        }) }).withSupervisorStrategy(BackoffSupervisionStrategy(
            BackoffPolicy(millis(40), millis(160), randomFactor: 0.0, maxRestarts: 3)))
        kit.actorOf(props, "db")
        @Expect(rec.await("db:postStop", T), true)
        // 首次启动 + 3 次退避重启（40、80、160ms），第 4 次失败放弃并停止
        @Expect(starts.size(), 4)
        expectBackoffGaps(starts.gapsMs(), [40, 80, 160])
        kit.shutdown()
    }

    @TestCase
    public func typedBackoffMatchesClassic(): Unit {
        let kit = ActorTestKit("bo-typed")
        let sys = kit.system.typed()
        let starts = Stamps()
        let b = TypedBehaviors.supervise(TypedBehaviors.setup<String>({ _ =>
            starts.add()
            throw Boom("数据库未就绪")
        })).onFailure(TypedSupervisorStrategy.restartWithBackoff(millis(40), millis(160), 0.0).withMaxRestarts(3))
        sys.actorOf(b, "db")
        @Expect(awaitCondition(T, { => starts.size() >= 4 && kit.system.lookup("/user/db").isNone() }), true)
        @Expect(starts.size(), 4)
        expectBackoffGaps(starts.gapsMs(), [40, 80, 160])
        kit.shutdown()
    }

    @TestCase
    public func slowRestartDoesNotResetBackoffCount(): Unit {
        // 每次重启都要等子 Actor 慢慢停止（约 100ms，E33 的 Recreating），实际重启比"失败时刻 + 40ms"晚得多。
        // 此前按预计时刻计算稳定运行时长，每次都误判为"已稳定运行 ≥ resetBackoffAfter（= 40ms）"而清零：
        // 永远从 40ms 重来、maxRestarts 永远到不了，Actor 无限重启（第 5 批 CI 在负载下观察到同一问题：定时器晚到）
        let kit = ActorTestKit("bo-slow-restart")
        let rec = EventRecorder()
        let starts = Stamps()
        let props = PropsFactory.create<Actor>({ => Subject("db", rec, { _, _, _ => handled() }, { _, ctx =>
            starts.add()
            ctx.actorOf({ => SlowStopChild(100) }, "conn") |> boIgnore
            throw Boom("数据库未就绪")
        }) }).withSupervisorStrategy(BackoffSupervisionStrategy(
            BackoffPolicy(millis(40), millis(160), randomFactor: 0.0, maxRestarts: 3)))
        kit.actorOf(props, "db")
        @Expect(rec.await("db:postStop", T), true)
        @Expect(starts.size(), 4)

        // 对照：同样的场景去掉运行时提供的实际重启时刻（等价于修复前的算法），计数每次被清零，一直重启
        let controlStarts = Stamps()
        let control = PropsFactory.create<Actor>({ => Subject("ctl", rec, { _, _, _ => handled() }, { _, ctx =>
            controlStarts.add()
            ctx.actorOf({ => SlowStopChild(100) }, "conn") |> boIgnore
            throw Boom("数据库未就绪")
        }) }).withSupervisorStrategy(WithoutRestartTime(BackoffSupervisionStrategy(
            BackoffPolicy(millis(40), millis(160), randomFactor: 0.0, maxRestarts: 3))))
        kit.actorOf(control, "ctl")
        @Expect(awaitCondition(T, { => controlStarts.size() >= 8 }), true)
        @Expect(rec.contains("ctl:postStop"), false)
        kit.shutdown()
    }

    @TestCase
    public func typedBackoffSuspendsMailboxDuringDelay(): Unit {
        let kit = ActorTestKit("bo-typed-suspend")
        let sys = kit.system.typed()
        let c = sys.actorOf(TypedBehaviors.supervise(boCounter(0))
            .onFailure(TypedSupervisorStrategy.restartWithBackoff(millis(300), Duration.second, 0.0)), "c")
        c << BoCmd.Add(5)
        @Expect(boCount(c), 5)
        let t0 = MonoTime.now()
        // Crash 之后的 Add(1) 在退避期间到达：旧实例不处理它，重启后的新实例（状态归零）处理
        c << BoCmd.Crash << BoCmd.Add(1)
        @Expect(boCount(c), 1)
        @Expect((MonoTime.now() - t0).toMilliseconds() >= 298, true)
        kit.shutdown()
    }

    @TestCase
    public func typedSetupFailureRetriesOnlyWhenBounded(): Unit {
        let kit = ActorTestKit("bo-typed-setup")
        let sys = kit.system.typed()
        // 不限次数的 restart：setup 失败直接停止（与 Akka Typed 一致），否则就是无限重启
        let unbounded = Stamps()
        sys.actorOf(TypedBehaviors.supervise(TypedBehaviors.setup<String>({ _ =>
            unbounded.add()
            throw Boom("配置错误")
        })).onFailure(TypedSupervisorStrategy.restart()), "cfg")
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/cfg").isNone() }), true)
        @Expect(unbounded.size(), 1)
        // 限次 restart：照常重试，首次启动 + 2 次重启后停止
        let bounded = Stamps()
        sys.actorOf(TypedBehaviors.supervise(TypedBehaviors.setup<String>({ _ =>
            bounded.add()
            throw Boom("配置错误")
        })).onFailure(TypedSupervisorStrategy.restart().withLimit(2, Duration.minute)), "cfg2")
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/cfg2").isNone() }), true)
        @Expect(bounded.size(), 3)
        kit.shutdown()
    }
}
