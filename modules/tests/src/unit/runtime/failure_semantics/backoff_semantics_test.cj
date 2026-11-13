/*
 * CActor - 退避重启的端到端语义（E6′，对照 Akka restartWithBackoff）
 *
 *   - 经典 BackoffSupervisionStrategy 与类型化 restartWithBackoff 在真实运行时上的重启间隔一致，按指数增长，
 *     达到上限后停止（E52 起用 ManualTime 驱动，间隔按虚拟时间精确断言：差 1ms 不重启、到点重启）（此前经典旧策略恒返回 Stop（R9），类型化 API 没有 backoff）
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

/** 线程安全的虚拟时间戳记录（E52：退避延迟由 ManualTime 驱动） */
class VirtualStamps {
    let lock = Mutex()
    let time: ManualTime
    let items = ArrayList<Int64>()

    init(time: ManualTime) {
        this.time = time
    }

    func add(): Unit {
        let now = time.now().toMilliseconds()
        synchronized(lock) { items.add(now) }
    }

    func size(): Int64 {
        synchronized(lock) { items.size }
    }

    /** 每次启动时的虚拟时刻（毫秒） */
    func millisAt(): Array<Int64> {
        synchronized(lock) { items.toArray() }
    }
}

/**
 * 第 before 次启动失败后，退避延迟恰好是 delayMs（虚拟时间）：差 1ms 不重启，到点重启。
 * 任务未到期时不会投递 SysRecreate，"不重启"是确定的，无需等待
 */
func expectRestartAfter(time: ManualTime, starts: VirtualStamps, before: Int64, delayMs: Int64): Unit {
    @Expect(awaitCondition(T, { => starts.size() == before }), true)
    // 退避延迟登记之后再推进
    @Expect(time.awaitPending(1), true)
    time.timePasses(millis(delayMs - 1))
    @Expect(starts.size(), before)
    time.timePasses(millis(1))
    @Expect(awaitCondition(T, { => starts.size() == before + 1 }), true)
}

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
        // 虚拟时钟（E52）：退避延迟精确到毫秒，不受定时器精度与机器负载影响
        let time = ManualTime()
        let kit = ActorTestKit("bo-classic", time)
        let rec = EventRecorder()
        let starts = VirtualStamps(time)
        let props = PropsFactory.create<Actor>({ => Subject("db", rec, { _, _, _ => handled() }, { _, _ =>
            starts.add()
            throw Boom("数据库未就绪")
        }) }).withSupervisorStrategy(BackoffSupervisionStrategy(
            BackoffPolicy(millis(40), millis(160), randomFactor: 0.0, maxRestarts: 3)))
        kit.actorOf(props, "db")
        // 首次启动 + 3 次退避重启（40、80、160ms），第 4 次失败放弃并停止
        expectRestartAfter(time, starts, 1, 40)
        expectRestartAfter(time, starts, 2, 80)
        expectRestartAfter(time, starts, 3, 160)
        @Expect(rec.await("db:postStop", T), true)
        @Expect(starts.size(), 4)
        @Expect(starts.millisAt(), [0, 40, 120, 280])
        @Expect(time.pendingCount(), 0)
        kit.shutdown()
    }

    @TestCase
    public func typedBackoffMatchesClassic(): Unit {
        let time = ManualTime()
        let kit = ActorTestKit("bo-typed", time)
        let sys = kit.system.typed()
        let starts = VirtualStamps(time)
        let b = TypedBehaviors.supervise(TypedBehaviors.setup<String>({ _ =>
            starts.add()
            throw Boom("数据库未就绪")
        })).onFailure(TypedSupervisorStrategy.restartWithBackoff(millis(40), millis(160), 0.0).withMaxRestarts(3))
        sys.actorOf(b, "db")
        expectRestartAfter(time, starts, 1, 40)
        expectRestartAfter(time, starts, 2, 80)
        expectRestartAfter(time, starts, 3, 160)
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/db").isNone() }), true)
        @Expect(starts.size(), 4)
        @Expect(starts.millisAt(), [0, 40, 120, 280])
        @Expect(time.pendingCount(), 0)
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
        let time = ManualTime()
        let kit = ActorTestKit("bo-typed-suspend", time)
        let sys = kit.system.typed()
        let c = sys.actorOf(TypedBehaviors.supervise(boCounter(0))
            .onFailure(TypedSupervisorStrategy.restartWithBackoff(millis(300), Duration.second, 0.0)), "c")
        c << BoCmd.Add(5)
        @Expect(boCount(c), 5)
        // Crash 之后的 Add(1) 与 Get 在退避期间到达：旧实例不处理它们，重启后的新实例（状态归零）处理
        c << BoCmd.Crash << BoCmd.Add(1)
        @Expect(time.awaitPending(1), true)
        // Actor 外的 ask 用真实时间超时，不受虚拟时钟影响
        let reply = TypedAsk.ask<BoCmd, Int64>(c, { r => BoCmd.Get(r) }, T)
        time.timePasses(millis(299))
        // 退避未到期：挂起中的 Actor 不会回复（给调度线程 100ms 真实时间，确认没有被旧实例处理）
        @Expect(awaitCondition(millis(100), { => reply.isCompleted() }), false)
        time.timePasses(millis(1))
        @Expect(reply.get(T), 1)
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
