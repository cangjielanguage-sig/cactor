/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R3 BehaviorTestKit / TestInbox / ManualTime 与 R2 行为拦截器测试
 */
package cactor_tests.unit.runtime.typed_testkit

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{DeathPactException, InvalidActorNameException}
import cactor_core.typed.*
import cactor_runtime.typed.{TypedActorSystemImpl}
import cactor_testkit.*

enum Cmd {
    | Inc
    | Get(TypedActorRef<Int64>)
    | Spawn(String)
    | StopChild(String)
    | Tick
    | Idle
    | Ping(TypedActorRef<String>)
    | Answer(String)
    | Halt
}

class Counter {
    var v: Int64 = 0
}

func counter(n: Int64): TypedBehavior<Cmd> {
    TypedBehaviors.receive<Cmd>({ ctx, cmd =>
        match (cmd) {
            case Cmd.Inc => counter(n + 1)
            case Cmd.Get(r) =>
                r.tell(n)
                TypedBehaviors.same<Cmd>()
            case Cmd.Halt => TypedBehaviors.stopped<Cmd>()
            case _ => TypedBehaviors.unhandled<Cmd>()
        }
    })
}

let MS100 = Duration.millisecond * 100

@Test
public class BehaviorTestKitTest {
    @TestCase
    public func testRunAndReplyThroughInbox(): Unit {
        let kit = BehaviorTestKit<Cmd>(counter(0))
        let reply = TestInbox<Int64>("reply")
        kit.run(Cmd.Inc)
        kit.run(Cmd.Inc)
        kit.run(Cmd.Get(reply))
        reply.expectMessage(2)
        reply.expectNoMessage()
        @Expect(TypedBehaviors.isSame(kit.returnedBehavior()), true)
        kit.run(Cmd.Tick)
        @Expect(kit.unhandledCount(), 1)
        @Expect(kit.hasEffects(), false)
        @Expect(kit.selfInbox().pathString(), "/user/testkit")
    }

    @TestCase
    public func testStoppedRunsPostStopAndRejectsFurtherMessages(): Unit {
        let rec = EventRecorder()
        let kit = BehaviorTestKit<Cmd>(TypedBehaviors.receive<Cmd>({ ctx, cmd =>
            match (cmd) {
                case Cmd.Halt => TypedBehaviors.stopped<Cmd>({ => rec.record("callback") })
                case _ => TypedBehaviors.same<Cmd>()
            }
        }))
        kit.run(Cmd.Halt)
        @Expect(kit.isAlive(), false)
        @Expect(rec.snapshot(), ["callback"])
        var rejected = false
        try {
            kit.run(Cmd.Inc)
        } catch (_: IllegalStateException) {
            rejected = true
        }
        @Expect(rejected, true)

        // 没有回调时 PostStop 信号交给停止前的行为
        let kit2 = BehaviorTestKit<Cmd>(TypedBehaviors.receive<Cmd>({ ctx, cmd =>
            ctx.stopSelf()
            TypedBehaviors.same<Cmd>()
        }).receiveSignal({ _, sig =>
            rec.record(sig.signalName())
            TypedBehaviors.same<Cmd>()
        }))
        kit2.run(Cmd.Inc)
        @Expect(kit2.isAlive(), false)
        @Expect(rec.contains("PostStop"), true)
    }

    @TestCase
    public func testSpawnEffectsAndChildKits(): Unit {
        let parent = TypedBehaviors.setup<Cmd>({ ctx =>
            let worker = ctx.actorOf<String>(TypedBehaviors.receiveMessage<String>({ m =>
                if (m == "stop") {
                    TypedBehaviors.stopped<String>()
                } else {
                    TypedBehaviors.same<String>()
                }
            }), "worker")
            worker.tell("job-1")
            TypedBehaviors.receive<Cmd>({ c, cmd =>
                match (cmd) {
                    case Cmd.Spawn(name) =>
                        if (name.isEmpty()) {
                            c.actorOf<Cmd>(counter(0))
                        } else {
                            c.actorOf<Cmd>(counter(0), name)
                        }
                        TypedBehaviors.same<Cmd>()
                    case Cmd.StopChild(name) =>
                        match (c.child(name)) {
                            case Some(r) => c.stop<Any>(r)
                            case None => ()
                        }
                        TypedBehaviors.same<Cmd>()
                    case _ => TypedBehaviors.same<Cmd>()
                }
            })
        })
        let kit = BehaviorTestKit<Cmd>(parent, "parent")
        kit.expectEffect(Effect.Spawned("worker"))
        let inbox = kit.childInbox<String>("worker")
        @Expect(inbox.pathString(), "/user/parent/worker")
        inbox.expectMessage("job-1")

        kit.run(Cmd.Spawn("c1"))
        kit.run(Cmd.Spawn(""))
        @Expect(kit.retrieveAllEffects(), [Effect.Spawned("c1"), Effect.SpawnedAnonymous("$1")])

        // 子 Actor 的同步测试
        let childKit = kit.childTestKit<Cmd>("c1")
        let reply = TestInbox<Int64>("r")
        childKit.run(Cmd.Inc)
        childKit.run(Cmd.Get(reply))
        reply.expectMessage(1)
        @Expect(refEq(kit.childTestKit<Cmd>("c1"), childKit), true)

        // 重名
        var dup = false
        try {
            kit.run(Cmd.Spawn("c1"))
        } catch (_: InvalidActorNameException) {
            dup = true
        }
        @Expect(dup, true)

        kit.run(Cmd.StopChild("c1"))
        kit.expectEffect(Effect.Stopped("c1"))
        @Expect(kit.hasEffects(), false)

        // 协议类型不符
        var mismatch = false
        try {
            kit.childInbox<Int64>("worker")
        } catch (_: TestKitAssertionException) {
            mismatch = true
        }
        @Expect(mismatch, true)
    }

    @TestCase
    public func testTimersWithManualTime(): Unit {
        let ticks = Counter()
        let b = TypedBehaviors.withTimers<Cmd>({ timers =>
            timers.startTimerAtFixedRate("tick", Cmd.Tick, MS100)
            TypedBehaviors.receiveMessage<Cmd>({ cmd =>
                match (cmd) {
                    case Cmd.Tick => ticks.v++
                    case Cmd.Halt => timers.cancel("tick")
                    case _ => ()
                }
                TypedBehaviors.same<Cmd>()
            })
        })
        let kit = BehaviorTestKit<Cmd>(b)
        kit.expectEffect(Effect.TimerScheduled("tick", MS100, true))
        kit.timePasses(Duration.millisecond * 99)
        @Expect(ticks.v, 0)
        kit.timePasses(Duration.millisecond * 251)
        @Expect(ticks.v, 3)
        @Expect(kit.manualTime().now(), Duration.millisecond * 350)
        kit.run(Cmd.Halt)
        kit.expectEffect(Effect.TimerCancelled("tick"))
        kit.timePasses(Duration.second * 10)
        @Expect(ticks.v, 3)
        @Expect(kit.manualTime().pendingCount(), 0)
    }

    @TestCase
    public func testSingleTimerReplacedBySameKey(): Unit {
        let fired = Counter()
        let b = TypedBehaviors.setup<Cmd>({ ctx =>
            ctx.timers.startSingleTimer("t", Cmd.Tick, Duration.second)
            ctx.timers.startSingleTimer("t", Cmd.Tick, Duration.second * 2)
            TypedBehaviors.receiveMessage<Cmd>({ cmd =>
                fired.v++
                TypedBehaviors.same<Cmd>()
            })
        })
        let kit = BehaviorTestKit<Cmd>(b)
        kit.timePasses(Duration.millisecond * 1500)
        @Expect(fired.v, 0)
        kit.timePasses(Duration.second)
        @Expect(fired.v, 1)
        kit.timePasses(Duration.second * 5)
        @Expect(fired.v, 1)
    }

    @TestCase
    public func testReceiveTimeoutIsResetByMessages(): Unit {
        let b = TypedBehaviors.setup<Cmd>({ ctx =>
            ctx.setReceiveTimeout(Duration.second, Cmd.Idle)
            TypedBehaviors.receiveMessage<Cmd>({ cmd =>
                match (cmd) {
                    case Cmd.Idle => TypedBehaviors.stopped<Cmd>()
                    case _ => TypedBehaviors.same<Cmd>()
                }
            })
        })
        let kit = BehaviorTestKit<Cmd>(b)
        kit.expectEffect(Effect.ReceiveTimeoutSet(Duration.second))
        kit.timePasses(Duration.millisecond * 900)
        kit.run(Cmd.Inc)
        kit.timePasses(Duration.millisecond * 900)
        @Expect(kit.isAlive(), true)
        kit.timePasses(Duration.millisecond * 100)
        @Expect(kit.isAlive(), false)
    }

    @TestCase
    public func testAskReplyAndTimeout(): Unit {
        let server = TestInbox<Cmd>("server")
        let answers = ArrayList<String>()
        let b = TypedBehaviors.receive<Cmd>({ ctx, cmd =>
            match (cmd) {
                case Cmd.Inc =>
                    ctx.ask<Cmd, String>(server, { r => Cmd.Ping(r) }, Duration.second, { o =>
                        match (o) {
                            case AskOutcome.Replied(s) => Cmd.Answer(s)
                            case AskOutcome.Failed(e) => Cmd.Answer("timeout")
                        }
                    })
                case Cmd.Answer(s) => answers.add(s)
                case _ => ()
            }
            TypedBehaviors.same<Cmd>()
        })
        let kit = BehaviorTestKit<Cmd>(b)
        kit.run(Cmd.Inc)
        kit.expectEffect(Effect.AskInitiated("/user/server", Duration.second))
        match (server.receiveMessage()) {
            case Cmd.Ping(replyTo) =>
                replyTo.tell("pong")
                replyTo.tell("late duplicate is ignored")
            case _ => @Fail("expected Ping")
        }
        @Expect(kit.runAll(), 1)
        @Expect(answers.toArray(), ["pong"])
        // 已回复的 ask 超时不再触发
        kit.timePasses(Duration.second * 2)
        @Expect(kit.selfInbox().hasMessages(), false)

        // 不回复：虚拟时间推进到超时
        kit.run(Cmd.Inc)
        kit.timePasses(Duration.millisecond * 999)
        @Expect(kit.selfInbox().hasMessages(), false)
        kit.timePasses(Duration.millisecond)
        kit.runOne()
        @Expect(answers.toArray(), ["pong", "timeout"])
    }

    @TestCase
    public func testWatchEffectsAndDeathPact(): Unit {
        let other = TestInbox<String>("other")
        let b = TypedBehaviors.setup<Cmd>({ ctx =>
            ctx.watch<String>(other)
            TypedBehaviors.receiveMessage<Cmd>({ _ => TypedBehaviors.same<Cmd>() })
        })
        let kit = BehaviorTestKit<Cmd>(b)
        kit.expectEffect(Effect.Watched("/user/other"))
        // 未处理的 Terminated 信号 → DeathPactException（与真实运行时一致）
        let sysKit = ActorTestKit("deathpact")
        let probe = sysKit.createTestProbe("other")
        var pact = false
        try {
            kit.signal(TypedTerminated(probe.ref))
        } catch (_: DeathPactException) {
            pact = true
        }
        @Expect(pact, true)
        sysKit.shutdown()
    }

    @TestCase
    public func testMessageAdapterAndLogCapture(): Unit {
        let b = TypedBehaviors.setup<Cmd>({ ctx =>
            let adapter = ctx.messageAdapter<Int64>({ n => Cmd.Answer("n=${n}") })
            adapter.tell(42)
            TypedBehaviors.receiveMessage<Cmd>({ cmd =>
                match (cmd) {
                    case Cmd.Answer(s) => ctx.log().info("answer ${s}")
                    case _ => ()
                }
                TypedBehaviors.same<Cmd>()
            })
        })
        let kit = BehaviorTestKit<Cmd>(b)
        kit.expectEffect(Effect.MessageAdapterCreated)
        kit.runOne()
        let logs = kit.logEntries()
        @Expect(logs.size, 1)
        @Expect(logs[0].toString(), "[INFO] answer n=42")
    }

    @TestCase
    public func testScheduleOnceDeliversToTarget(): Unit {
        let target = TestInbox<String>("target")
        let b = TypedBehaviors.setup<Cmd>({ ctx =>
            ctx.scheduleOnce<String>(Duration.second * 5, target, "wake up")
            TypedBehaviors.receiveMessage<Cmd>({ _ => TypedBehaviors.same<Cmd>() })
        })
        let kit = BehaviorTestKit<Cmd>(b)
        kit.expectEffect(Effect.Scheduled(Duration.second * 5, "/user/target"))
        kit.timePasses(Duration.second * 4)
        target.expectNoMessage()
        kit.timePasses(Duration.second)
        target.expectMessage("wake up")
    }

    @TestCase
    public func testManualTimeOrdersTasksByDueTime(): Unit {
        let t = ManualTime()
        let order = ArrayList<String>()
        t.schedule(Duration.second * 3, { => order.add("c") })
        t.schedule(Duration.second, { => order.add("a") })
        t.schedule(Duration.second, { => order.add("b") })
        let r = t.scheduleRepeated(Duration.second * 2, { => order.add("r@${t.now().toSeconds()}") })
        t.timePasses(Duration.second * 5)
        @Expect(order.toArray(), ["a", "b", "r@2", "c", "r@4"])
        @Expect(r.cancel(), true)
        @Expect(r.cancel(), false)
        t.timePasses(Duration.second * 10)
        @Expect(order.size, 5)
    }
}

// ======================= 拦截器 =======================

/** 只放行偶数的拦截器，同时统计调用次数 */
class EvenOnly <: BehaviorInterceptor<Int64> {
    var calls: Int64 = 0
    var signals: Int64 = 0

    public func aroundReceive(ctx: TypedActorContext<Int64>, message: Int64, target: (Int64) -> TypedBehavior<Int64>): TypedBehavior<Int64> {
        calls++
        if (message % 2 == 0) {
            target(message)
        } else {
            TypedBehaviors.unhandled<Int64>()
        }
    }

    public func aroundSignal(ctx: TypedActorContext<Int64>, signal: TypedSignal, target: (TypedSignal) -> TypedBehavior<Int64>): TypedBehavior<Int64> {
        signals++
        target(signal)
    }
}

func summing(total: Int64, out: ArrayList<Int64>): TypedBehavior<Int64> {
    TypedBehaviors.receiveMessage<Int64>({ n =>
        out.add(total + n)
        if (total + n > 100) {
            TypedBehaviors.stopped<Int64>()
        } else {
            summing(total + n, out)
        }
    })
}

@Test
public class InterceptorTest {
    @TestCase
    public func testInterceptorFiltersAndSurvivesBehaviorChange(): Unit {
        let ic = EvenOnly()
        let out = ArrayList<Int64>()
        let kit = BehaviorTestKit<Int64>(TypedBehaviors.intercept<Int64>(ic, summing(0, out)))
        for (n in [2, 3, 4, 5, 6]) {
            kit.run(n)
        }
        // 奇数被拦截；内层每次返回新行为后拦截器仍然生效
        @Expect(out.toArray(), [2, 6, 12])
        @Expect(ic.calls, 5)
        @Expect(kit.unhandledCount(), 2)
        kit.run(100)
        @Expect(kit.isAlive(), false)
        // PostStop 也经过拦截器
        @Expect(ic.signals, 1)
    }

    @TestCase
    public func testSameInterceptorIsNotStacked(): Unit {
        // 递归构造时每次都用同一个拦截器包裹，消息只应被拦截一次
        let ic = EvenOnly()
        let seen = ArrayList<Int64>()
        let kit = BehaviorTestKit<Int64>(stacked(ic, seen))
        for (_ in 0..10) {
            kit.run(2)
        }
        @Expect(ic.calls, 10)
        @Expect(seen.size, 10)
    }

    @TestCase
    public func testInterceptorCanTransformMessages(): Unit {
        let out = ArrayList<Int64>()
        let doubled = TypedBehaviors.intercept<Int64>({ ctx, m, target => target(m * 2) }, summing(0, out))
        let kit = BehaviorTestKit<Int64>(doubled)
        kit.run(1)
        kit.run(2)
        @Expect(out.toArray(), [2, 6])
    }

    @TestCase
    public func testInterceptorAroundSetupKeepsStateOnSame(): Unit {
        let setups = Counter()
        let ic = EvenOnly()
        let b = TypedBehaviors.setup<Int64>({ _ =>
            setups.v++
            let state = Counter()
            TypedBehaviors.receiveMessage<Int64>({ n =>
                state.v += n
                if (n == 0) {
                    throw Exception("state=${state.v}")
                }
                TypedBehaviors.same<Int64>()
            })
        })
        let kit = BehaviorTestKit<Int64>(TypedBehaviors.intercept<Int64>(ic, b))
        kit.run(2)
        kit.run(4)
        var msg = ""
        try {
            kit.run(0)
        } catch (e: Exception) {
            msg = e.message
        }
        // setup 只执行一次，状态没有因为拦截而丢失
        @Expect(setups.v, 1)
        @Expect(msg, "state=6")
    }

    @TestCase
    public func testMonitorAndLogMessagesOnRealRuntime(): Unit {
        let kit = ActorTestKit("typed-intercept")
        let sys: TypedActorSystemImpl = kit.system.typed()
        let monitorLock = Mutex()
        let copies = ArrayList<Int64>()
        let monitor = sys.actorOf<Int64>(TypedBehaviors.receiveMessage<Int64>({ n =>
            synchronized(monitorLock) {
                copies.add(n)
            }
            TypedBehaviors.same<Int64>()
        }), "monitor")
        let out = ArrayList<Int64>()
        let outLock = Mutex()
        let target = sys.actorOf<Int64>(TypedBehaviors.monitor<Int64>(monitor,
            TypedBehaviors.logMessages<Int64>(lockedSumming(0, out, outLock), { n => "${n}" })), "target")
        for (i in 1..=10) {
            target.tell(i)
        }
        @Expect(awaitCondition(Duration.second * 3, { => synchronized(monitorLock) { copies.size == 10 } }), true)
        @Expect(awaitCondition(Duration.second * 3, { => synchronized(outLock) { out.size == 10 } }), true)
        synchronized(outLock) {
            @Expect(out[9], 55)
        }
        synchronized(monitorLock) {
            @Expect(copies.toArray(), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        }
        kit.shutdown()
    }
}

func stacked(ic: EvenOnly, seen: ArrayList<Int64>): TypedBehavior<Int64> {
    TypedBehaviors.intercept<Int64>(ic, TypedBehaviors.receiveMessage<Int64>({ n =>
        seen.add(n)
        stacked(ic, seen)
    }))
}

func lockedSumming(total: Int64, out: ArrayList<Int64>, lock: Mutex): TypedBehavior<Int64> {
    TypedBehaviors.receiveMessage<Int64>({ n =>
        synchronized(lock) {
            out.add(total + n)
        }
        lockedSumming(total + n, out, lock)
    })
}
