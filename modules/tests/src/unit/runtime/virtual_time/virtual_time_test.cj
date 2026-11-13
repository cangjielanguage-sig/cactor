/*
 * CActor - 可注入调度器 / 虚拟时钟驱动真实 ActorSystem（E52，对标 Akka ManualTime + ExplicitlyTriggeredScheduler）
 *
 * 覆盖：
 *   - ManualTime 作为调度器的基本语义：按到期时刻（同刻按登记先后）执行、任务内登记的后续任务、周期任务、
 *     取消、按绝对时刻登记、负时长拒绝
 *   - 默认系统仍用真实时间调度器（零行为变化）；注入后系统 / Actor 上下文拿到的是同一个虚拟时钟
 *   - 类型化 ctx.scheduleOnce、ctx.scheduler()、Actor 内 ctx.ask 的超时都随虚拟时间触发
 *   - 监督 withLimit 的时间窗口按调度器时钟计算：虚拟时间推进超过窗口后，旧的重启记录过期
 * 全部用例不 sleep：时间只随 timePasses 前进，等待的是消息处理（awaitCondition / 探针），不是定时器
 */
package cactor_tests.unit.runtime.virtual_time

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{ActorScheduler, RealTimeScheduler, ScheduledTask}
import cactor_core.typed.*
import cactor_testkit.*

let T = Duration.second * 3

func ms(n: Int64): Duration {
    Duration.millisecond * n
}

/** 线程安全的字符串记录 */
class Log {
    let lock = Mutex()
    let items = ArrayList<String>()

    func add(s: String): Unit {
        synchronized(lock) { items.add(s) }
    }

    func snapshot(): Array<String> {
        synchronized(lock) { items.toArray() }
    }

    func size(): Int64 {
        synchronized(lock) { items.size }
    }
}

class VtBoom <: Exception {
    init(msg: String) { super(msg) }
}

enum PingReq {
    | Ping(TypedActorRef<String>)
}

enum AskerCmd {
    | Go(Duration)
    | Outcome(String)
}

enum CrashCmd {
    | Crash
    | Hello
}

@Test
public class VirtualTimeTest {
    // ---------- ManualTime 本身 ----------

    @TestCase
    public func manualTimeRunsDueTasksInTimeOrder(): Unit {
        let time = ManualTime()
        let sched: ActorScheduler = time
        let log = Log()
        sched.scheduleOnce(ms(30), { => log.add("c30") }) |> ignoreTask
        sched.scheduleOnce(ms(10), { => log.add("a10") }) |> ignoreTask
        // 同一到期时刻按登记先后
        sched.scheduleOnce(ms(10), { => log.add("b10") }) |> ignoreTask
        // 任务里登记的后续任务从任务的到期时刻起算：10 + 5 = 15，落在本次推进范围内
        sched.scheduleOnce(ms(10), { =>
            sched.scheduleOnce(ms(5), { => log.add("nested15@${time.now().toMilliseconds()}") }) |> ignoreTask
        }) |> ignoreTask
        let cancelled = sched.scheduleOnce(ms(20), { => log.add("cancelled") })
        cancelled.cancel()
        time.timePasses(ms(29))
        @Expect(log.snapshot(), ["a10", "b10", "nested15@15"])
        time.timePasses(ms(1))
        @Expect(log.snapshot(), ["a10", "b10", "nested15@15", "c30"])
        @Expect(time.now(), ms(30))
        @Expect(time.pendingCount(), 0)
    }

    @TestCase
    public func manualTimeRepeatsAndSchedulesAtAbsoluteDeadline(): Unit {
        let time = ManualTime()
        let sched: ActorScheduler = time
        let count = AtomicInt64(0)
        let periodic = sched.scheduleAtFixedRate(ms(5), ms(10), { => count.fetchAdd(1) |> ignoreInt })
        // 5、15、25、35
        time.timePasses(ms(35))
        @Expect(count.load(), 4)
        periodic.cancel()
        time.timePasses(ms(100))
        @Expect(count.load(), 4)
        // 绝对时刻：起点 + 200ms；已过期的绝对时刻在下一次推进（哪怕 0）时立即执行
        let log = Log()
        sched.scheduleOnceAt(sched.monotonicNow() + ms(65), { => log.add("at@${time.now().toMilliseconds()}") }) |> ignoreTask
        sched.scheduleOnceAt(sched.monotonicNow() - ms(1000), { => log.add("past@${time.now().toMilliseconds()}") }) |> ignoreTask
        time.timePasses(Duration.Zero)
        @Expect(log.snapshot(), ["past@135"])
        time.timePasses(ms(65))
        @Expect(log.snapshot(), ["past@135", "at@200"])
        // 虚拟单调时钟与 now() 同步前进
        let before = sched.monotonicNow()
        time.timePasses(ms(7))
        @Expect(sched.monotonicNow() - before, ms(7))
        var threw = false
        try {
            time.timePasses(ms(-1))
        } catch (_: IllegalArgumentException) {
            threw = true
        }
        @Expect(threw, true)
    }

    // ---------- 注入 ----------

    @TestCase
    public func defaultSystemKeepsRealTimeSchedulerAndInjectionIsVisible(): Unit {
        let plain = ActorTestKit("vt-plain")
        @Expect((plain.system.scheduler() as RealTimeScheduler).isSome(), true)
        plain.shutdown()

        let time = ManualTime()
        let kit = ActorTestKit("vt-injected", time)
        @Expect((kit.system.scheduler() as ManualTime).isSome(), true)
        @Expect(refEq(kit.manualTime(), time), true)
        // Actor 上下文拿到的也是同一个调度器
        let seen = Log()
        kit.system.typed().actorOf(TypedBehaviors.setup<String>({ ctx =>
            let classic = ctx.toClassic().getOrThrow()
            seen.add(if ((classic.scheduler() as ManualTime).isSome()) { "manual" } else { "other" })
            TypedBehaviors.same<String>()
        }), "probe")
        @Expect(awaitCondition(T, { => seen.size() == 1 }), true)
        @Expect(seen.snapshot(), ["manual"])
        kit.shutdown()
    }

    // ---------- 类型化上下文的调度 ----------

    @TestCase
    public func typedScheduleOnceAndSchedulerFollowVirtualTime(): Unit {
        let time = ManualTime()
        let kit = ActorTestKit("vt-typed-schedule", time)
        let sys = kit.system.typed()
        let log = Log()
        let sink = sys.actorOf(TypedBehaviors.receiveMessage<String>({ m =>
            log.add(m)
            TypedBehaviors.same<String>()
        }), "sink")
        sys.actorOf(TypedBehaviors.setup<String>({ ctx =>
            ctx.scheduleOnce<String>(ms(50), sink, "once") |> ignoreCancellable
            let anySink = TypedAdapters.messageAdapter<Any, String>(sink, { a =>
                match (a) {
                    case s: String => s
                    case _ => "?"
                }
            })
            ctx.scheduler().schedule(ms(20), ms(20), anySink, "tick") |> ignoreCancellable
            TypedBehaviors.same<String>()
        }), "scheduler")
        @Expect(time.awaitPending(2), true)
        // 20、40 两次 tick，once 在 50
        time.timePasses(ms(49))
        @Expect(awaitCondition(T, { => log.size() == 2 }), true)
        time.timePasses(ms(1))
        @Expect(awaitCondition(T, { => log.size() == 3 }), true)
        @Expect(log.snapshot(), ["tick", "tick", "once"])
        kit.shutdown()
    }

    @TestCase
    public func actorAskTimeoutFollowsVirtualTime(): Unit {
        let time = ManualTime()
        let kit = ActorTestKit("vt-ask", time)
        let sys = kit.system.typed()
        let log = Log()
        // 永不回复的目标
        let silent = sys.actorOf(TypedBehaviors.receiveMessage<PingReq>({ _ => TypedBehaviors.same<PingReq>() }), "silent")
        let echo = sys.actorOf(TypedBehaviors.receiveMessage<PingReq>({ m =>
            match (m) {
                case PingReq.Ping(r) => r.tell("pong")
            }
            TypedBehaviors.same<PingReq>()
        }), "echo")
        let asker = sys.actorOf(TypedBehaviors.receive<AskerCmd>({ ctx, cmd =>
            match (cmd) {
                case AskerCmd.Go(timeout) =>
                    ctx.ask<PingReq, String>(silent, { r => PingReq.Ping(r) }, timeout, { o =>
                        match (o) {
                            case AskOutcome.Replied(v) => AskerCmd.Outcome("reply:${v}")
                            case AskOutcome.Failed(e) => AskerCmd.Outcome("failed:${(e as TypedAskTimeoutException).isSome()}")
                        }
                    })
                    // 有回复的 ask 不依赖时间推进
                    ctx.ask<PingReq, String>(echo, { r => PingReq.Ping(r) }, timeout, { o =>
                        match (o) {
                            case AskOutcome.Replied(v) => AskerCmd.Outcome("reply:${v}")
                            case AskOutcome.Failed(_) => AskerCmd.Outcome("echo-failed")
                        }
                    })
                case AskerCmd.Outcome(s) => log.add(s)
            }
            TypedBehaviors.same<AskerCmd>()
        }), "asker")
        // 超时设为 10 分钟（虚拟）：真实时间里永远等不到，只能由虚拟时钟触发
        asker << AskerCmd.Go(Duration.minute * 10)
        @Expect(awaitCondition(T, { => log.size() == 1 }), true)
        @Expect(log.snapshot(), ["reply:pong"])
        @Expect(time.awaitPending(1), true)
        time.timePasses(Duration.minute * 10 - ms(1))
        @Expect(log.size(), 1)
        time.timePasses(ms(1))
        @Expect(awaitCondition(T, { => log.size() == 2 }), true)
        @Expect(log.snapshot()[1], "failed:true")
        kit.shutdown()
    }

    // ---------- 监督时间窗口 ----------

    @TestCase
    public func restartLimitWindowUsesSchedulerClock(): Unit {
        // withLimit(2, 1s)：1 秒内第 3 次失败才停止。窗口按调度器时钟（失败时刻 ActorFailure.failedAt）计算：
        // 两次失败后虚拟时间推进 2s，旧记录过期，第 3 次失败照常重启；若仍按真实时钟，三次失败在几毫秒内发生，会停止
        let time = ManualTime()
        let kit = ActorTestKit("vt-limit", time)
        let sys = kit.system.typed()
        let starts = AtomicInt64(0)
        let log = Log()
        let behavior = TypedBehaviors.supervise(TypedBehaviors.setup<CrashCmd>({ _ =>
            starts.fetchAdd(1) |> ignoreInt
            TypedBehaviors.receiveMessage<CrashCmd>({ m =>
                match (m) {
                    case CrashCmd.Crash => throw VtBoom("boom")
                    case CrashCmd.Hello => log.add("hello")
                }
                TypedBehaviors.same<CrashCmd>()
            })
        })).onFailure(TypedSupervisorStrategy.restart().withLimit(2, Duration.second))
        let ref = sys.actorOf(behavior, "limited")
        ref << CrashCmd.Crash << CrashCmd.Crash
        @Expect(awaitCondition(T, { => starts.load() == 3 }), true)
        time.timePasses(Duration.second * 2)
        ref << CrashCmd.Crash << CrashCmd.Hello
        @Expect(awaitCondition(T, { => log.size() == 1 }), true)
        @Expect(starts.load(), 4)
        // 窗口内连续 3 次失败（虚拟时间不动）：超过上限，停止
        ref << CrashCmd.Crash << CrashCmd.Crash << CrashCmd.Crash
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/limited").isNone() }), true)
        kit.shutdown()
    }
}

func ignoreTask(_: ScheduledTask): Unit {}

func ignoreCancellable(_: TypedCancellable): Unit {}

func ignoreInt(_: Int64): Unit {}
