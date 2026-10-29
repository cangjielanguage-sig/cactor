/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 类型化 Actor 语义测试（R2）
 * 覆盖：函数式行为切换、setup、stopped / PostStop、子 Actor、监视与 watchWith、DeathPact、
 * 监督（默认停止 / restart / resume / 次数限制 / 按异常类型）、PreRestart、定时器、Stash、
 * ask（外部与 Actor 内）、超时、messageAdapter、pipeToSelf、接收超时、Receptionist、
 * transformMessages、查找、与经典 Actor 互操作、TypedProps、并发计数
 */
package cactor_tests.unit.runtime.typed_semantics

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorRef, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.typed.*
import cactor_runtime.typed.{TypedActorSystemImpl}
import cactor_testkit.*

// ---------------- 协议 ----------------

enum CounterCmd {
    | Increment(Int64)
    | GetCount(TypedActorRef<Int64>)
    | Fail(String)
    | FailState(String)
    | StopIt
}

class TBoom <: Exception {
    init(msg: String) { super(msg) }
}

class TOther <: Exception {
    init(msg: String) { super(msg) }
}

/** 可变状态盒子（仓颉闭包不能捕获可变局部变量后逃逸） */
class IntBox {
    var v: Int64 = 0
}

let T3S: Duration = Duration.second * 3

/** 函数式计数器：状态通过返回新行为保存 */
func counter(n: Int64): TypedBehavior<CounterCmd> {
    TypedBehaviors.receive<CounterCmd>({ ctx, cmd =>
        match (cmd) {
            case CounterCmd.Increment(k) => counter(n + k)
            case CounterCmd.GetCount(replyTo) =>
                replyTo.tell(n)
                TypedBehaviors.same<CounterCmd>()
            case CounterCmd.Fail(m) => throw TBoom(m)
            case CounterCmd.FailState(m) => throw TOther(m)
            case CounterCmd.StopIt => TypedBehaviors.stopped<CounterCmd>()
        }
    })
}

func countOf(ref: TypedActorRef<CounterCmd>): Int64 {
    TypedAsk.ask<CounterCmd, Int64>(ref, { r => CounterCmd.GetCount(r) }, T3S).get(T3S)
}

/** 收集消息的类型化探针 */
class Collector<T> {
    let lock = Mutex()
    let items = ArrayList<T>()

    func add(t: T): Unit {
        synchronized(lock) { items.add(t) }
    }

    func size(): Int64 {
        synchronized(lock) { items.size }
    }

    func snapshot(): Array<T> {
        synchronized(lock) { items.toArray() }
    }

    func awaitSize(n: Int64): Bool {
        awaitCondition(T3S, { => this.size() >= n })
    }

    func behavior(): TypedBehavior<T> {
        TypedBehaviors.receive<T>({ _, m =>
            this.add(m)
            TypedBehaviors.same<T>()
        })
    }
}

func newSystem(name: String): (ActorTestKit, TypedActorSystemImpl) {
    let kit = ActorTestKit(name)
    (kit, kit.system.typed())
}

@Test
public class TypedSemanticsTest {
    @TestCase
    public func testFunctionalStateAndAsk(): Unit {
        let (kit, sys) = newSystem("typed-basic")
        let c = sys.actorOf(counter(0), "counter")
        c << CounterCmd.Increment(2) << CounterCmd.Increment(3)
        c.tell(CounterCmd.Increment(5))
        @Expect(countOf(c), 10)
        @Expect(c.pathString(), "/user/counter")
        kit.shutdown()
    }

    @TestCase
    public func testSetupRunsOnceBeforeFirstMessage(): Unit {
        let (kit, sys) = newSystem("typed-setup")
        let rec = EventRecorder()
        let b = TypedBehaviors.setup<String>({ ctx =>
            rec.record("setup:${ctx.self.pathString()}")
            TypedBehaviors.receiveMessage<String>({ m =>
                rec.record(m)
                TypedBehaviors.same<String>()
            })
        })
        let r = sys.actorOf(b, "s1")
        r << "a" << "b" << "c"
        @Expect(rec.awaitSize(4, T3S), true)
        @Expect(rec.snapshot()[0], "setup:/user/s1")
        @Expect(rec.count("setup:/user/s1"), 1)
        @Expect(rec.indexOf("c") > rec.indexOf("a"), true)
        kit.shutdown()
    }

    @TestCase
    public func testSetupStartsTimersWithoutMessages(): Unit {
        // setup 在 Actor 启动时执行，即使没有任何消息到达
        let (kit, sys) = newSystem("typed-setup-eager")
        let rec = EventRecorder()
        sys.actorOf(TypedBehaviors.withTimers<String>({ timers =>
            timers.startSingleTimer("t", "tick", Duration.millisecond * 20)
            TypedBehaviors.receiveMessage<String>({ m =>
                rec.record(m)
                TypedBehaviors.same<String>()
            })
        }), "eager")
        @Expect(rec.await("tick", T3S), true)
        kit.shutdown()
    }

    @TestCase
    public func testStoppedDeliversPostStop(): Unit {
        let (kit, sys) = newSystem("typed-stop")
        let rec = EventRecorder()
        let b = TypedBehaviors.receive<String>({ _, m =>
            if (m == "stop") {
                TypedBehaviors.stopped<String>()
            } else {
                rec.record(m)
                TypedBehaviors.same<String>()
            }
        }).receiveSignal({ _, sig =>
            rec.record("signal:${sig.signalName()}")
            TypedBehaviors.same<String>()
        })
        let r = sys.actorOf(b, "stopper")
        r << "x" << "stop" << "late"
        @Expect(rec.await("signal:PostStop", T3S), true)
        @Expect(rec.contains("late"), false)
        @Expect(awaitCondition(T3S, { => kit.system.lookup("/user/stopper").isNone() }), true)
        kit.shutdown()
    }

    @TestCase
    public func testStoppedCallback(): Unit {
        let (kit, sys) = newSystem("typed-stop-cb")
        let rec = EventRecorder()
        let r = sys.actorOf(TypedBehaviors.receiveMessage<String>({ _ =>
            TypedBehaviors.stopped<String>({ => rec.record("cleanup") })
        }), "cb")
        r << "go"
        @Expect(rec.await("cleanup", T3S), true)
        kit.shutdown()
    }

    @TestCase
    public func testChildrenWatchAndTerminatedSignal(): Unit {
        let (kit, sys) = newSystem("typed-children")
        let rec = EventRecorder()
        let parent = TypedBehaviors.setup<String>({ ctx =>
            let child = ctx.actorOf(counter(0), "child")
            ctx.watch(child)
            rec.record("child:${child.pathString()}")
            rec.record("children:${ctx.children().size}")
            TypedBehaviors.receive<String>({ c, m =>
                if (m == "kill-child") {
                    child.tell(CounterCmd.StopIt)
                }
                TypedBehaviors.same<String>()
            }, { _, sig =>
                match (sig) {
                    case t: TypedTerminated =>
                        rec.record("terminated:${t.pathString()}:${t.isFor(child)}")
                        TypedBehaviors.same<String>()
                    case _ => TypedBehaviors.unhandled<String>()
                }
            })
        })
        let p = sys.actorOf(parent, "parent")
        @Expect(rec.await("child:/user/parent/child", T3S), true)
        @Expect(rec.contains("children:1"), true)
        p << "kill-child"
        @Expect(rec.await("terminated:/user/parent/child:true", T3S), true)
        kit.shutdown()
    }

    @TestCase
    public func testWatchWithCustomMessage(): Unit {
        let (kit, sys) = newSystem("typed-watchwith")
        let rec = EventRecorder()
        let target = sys.actorOf(counter(0), "target")
        sys.actorOf(TypedBehaviors.setup<String>({ ctx =>
            ctx.watchWith(target, "target-gone")
            TypedBehaviors.receiveMessage<String>({ m =>
                rec.record(m)
                TypedBehaviors.same<String>()
            })
        }), "watcher")
        // 等 watcher 完成 setup 再停止目标
        sleep(Duration.millisecond * 50)
        target << CounterCmd.StopIt
        @Expect(rec.await("target-gone", T3S), true)
        kit.shutdown()
    }

    @TestCase
    public func testUnhandledTerminatedIsDeathPact(): Unit {
        let (kit, sys) = newSystem("typed-deathpact")
        let target = sys.actorOf(counter(0), "target")
        let rec = EventRecorder()
        let watcher = sys.actorOf(TypedBehaviors.setup<String>({ ctx =>
            ctx.watch(target)
            TypedBehaviors.receiveMessage<String>({ _ => TypedBehaviors.same<String>() })
                .receiveSignal({ _, sig =>
                    if (sig is PostStop) {
                        rec.record("watcher-stopped")
                    }
                    TypedBehaviors.unhandled<String>()
                })
        }), "watcher")
        sleep(Duration.millisecond * 50)
        target << CounterCmd.StopIt
        // 未处理 Terminated -> DeathPactException -> 默认策略停止
        @Expect(rec.await("watcher-stopped", T3S), true)
        @Expect(awaitCondition(T3S, { => kit.system.lookup(watcher.pathString()).isNone() }), true)
        kit.shutdown()
    }

    @TestCase
    public func testDefaultSupervisionStops(): Unit {
        let (kit, sys) = newSystem("typed-sup-default")
        let c = sys.actorOf(counter(0), "c")
        c << CounterCmd.Increment(1) << CounterCmd.Fail("x")
        @Expect(awaitCondition(T3S, { => kit.system.lookup("/user/c").isNone() }), true)
        kit.shutdown()
    }

    @TestCase
    public func testRestartResetsStateAndRerunsSetup(): Unit {
        let (kit, sys) = newSystem("typed-sup-restart")
        let rec = EventRecorder()
        // 状态保存在 setup 内（面向对象风格），行为始终是同一个，因此信号处理函数一直有效
        let b = TypedBehaviors.supervise(TypedBehaviors.setup<CounterCmd>({ _ =>
            rec.record("setup")
            let n = IntBox()
            TypedBehaviors.receive<CounterCmd>({ _, cmd =>
                match (cmd) {
                    case CounterCmd.Increment(k) => n.v += k
                    case CounterCmd.GetCount(r) => r.tell(n.v)
                    case CounterCmd.Fail(m) => throw TBoom(m)
                    case _ => ()
                }
                TypedBehaviors.same<CounterCmd>()
            }, { _, sig =>
                if (sig is PreRestart) {
                    rec.record("preRestart")
                }
                TypedBehaviors.same<CounterCmd>()
            })
        })).onFailure(TypedSupervisorStrategy.restart())
        let c = sys.actorOf(b, "c")
        c << CounterCmd.Increment(5)
        @Expect(countOf(c), 5)
        c << CounterCmd.Fail("boom")
        @Expect(rec.awaitSize(3, T3S), true)
        @Expect(countOf(c), 0)
        @Expect(rec.count("setup"), 2)
        @Expect(rec.contains("preRestart"), true)
        kit.shutdown()
    }

    @TestCase
    public func testResumeKeepsState(): Unit {
        let (kit, sys) = newSystem("typed-sup-resume")
        let c = sys.actorOf(TypedBehaviors.supervise(counter(0)).onFailure(TypedSupervisorStrategy.resume()), "c")
        c << CounterCmd.Increment(7) << CounterCmd.Fail("ignored") << CounterCmd.Increment(1)
        @Expect(countOf(c), 8)
        kit.shutdown()
    }

    @TestCase
    public func testRestartLimit(): Unit {
        let (kit, sys) = newSystem("typed-sup-limit")
        let rec = EventRecorder()
        let b = TypedBehaviors.supervise(TypedBehaviors.setup<CounterCmd>({ _ =>
            rec.record("setup")
            counter(0)
        })).onFailure(TypedSupervisorStrategy.restart().withLimit(2, Duration.second * 10))
        let c = sys.actorOf(b, "c")
        for (_ in 0..5) {
            c << CounterCmd.Fail("x")
        }
        @Expect(awaitCondition(T3S, { => kit.system.lookup("/user/c").isNone() }), true)
        // 初次启动 + 2 次重启
        @Expect(rec.count("setup"), 3)
        kit.shutdown()
    }

    @TestCase
    public func testSupervisionByExceptionType(): Unit {
        let (kit, sys) = newSystem("typed-sup-type")
        let b = TypedBehaviors.supervise(counter(0))
            .onFailureOf<TBoom>(TypedSupervisorStrategy.resume())
            .onFailure(TypedSupervisorStrategy.restart())
        let c = sys.actorOf(b, "c")
        c << CounterCmd.Increment(3) << CounterCmd.Fail("resume me")
        @Expect(countOf(c), 3)
        c << CounterCmd.FailState("restart me")
        @Expect(awaitCondition(T3S, { => countOf(c) == 0 }), true)
        kit.shutdown()
    }

    @TestCase
    public func testChildSupervisionIsolatedFromParent(): Unit {
        let (kit, sys) = newSystem("typed-sup-child")
        let rec = EventRecorder()
        sys.actorOf(TypedBehaviors.setup<String>({ ctx =>
            let child = ctx.actorOf(TypedBehaviors.supervise(counter(0)).onFailure(TypedSupervisorStrategy.restart()), "child")
            TypedBehaviors.receiveMessage<String>({ m =>
                match (m) {
                    case "fail" => child.tell(CounterCmd.Fail("x"))
                    case "inc" => child.tell(CounterCmd.Increment(1))
                    case _ => ()
                }
                rec.record(m)
                TypedBehaviors.same<String>()
            })
        }), "parent") << "inc" << "fail" << "inc" << "alive"
        @Expect(rec.await("alive", T3S), true)
        let child = sys.findActor<CounterCmd>("/user/parent/child").getOrThrow()
        @Expect(awaitCondition(T3S, { => countOf(child) == 1 }), true)
        kit.shutdown()
    }

    @TestCase
    public func testTimersPeriodicAndCancel(): Unit {
        let (kit, sys) = newSystem("typed-timers")
        let col = Collector<String>()
        let r = sys.actorOf(TypedBehaviors.withTimers<String>({ timers =>
            timers.startTimerWithFixedDelay("tick", "tick", Duration.millisecond * 10)
            TypedBehaviors.receiveMessage<String>({ m =>
                if (m == "tick") {
                    col.add(m)
                    if (col.size() >= 3) {
                        timers.cancel("tick")
                    }
                }
                TypedBehaviors.same<String>()
            })
        }), "timers")
        @Expect(col.awaitSize(3), true)
        sleep(Duration.millisecond * 100)
        @Expect(col.size(), 3)
        kit.shutdown()
    }

    @TestCase
    public func testStashUntilReady(): Unit {
        let (kit, sys) = newSystem("typed-stash")
        let col = Collector<String>()
        let ready = TypedBehaviors.receiveMessage<String>({ m =>
            col.add(m)
            TypedBehaviors.same<String>()
        })
        let r = sys.actorOf(TypedBehaviors.withStash<String>(100, { stash =>
            TypedBehaviors.receiveMessage<String>({ m =>
                if (m == "ready") {
                    stash.unstashAll(ready)
                } else {
                    stash.stash(m)
                    TypedBehaviors.same<String>()
                }
            })
        }), "stash")
        r << "a" << "b" << "c" << "ready" << "d"
        @Expect(col.awaitSize(4), true)
        let got = col.snapshot()
        @Expect(got[0], "a")
        @Expect(got[1], "b")
        @Expect(got[2], "c")
        @Expect(got[3], "d")
        kit.shutdown()
    }

    @TestCase
    public func testStashOverflowFailsActor(): Unit {
        let (kit, sys) = newSystem("typed-stash-overflow")
        let r = sys.actorOf(TypedBehaviors.withStash<String>(2, { stash =>
            TypedBehaviors.receiveMessage<String>({ m =>
                stash.stash(m)
                TypedBehaviors.same<String>()
            })
        }), "stash")
        r << "1" << "2" << "3"
        @Expect(awaitCondition(T3S, { => kit.system.lookup("/user/stash").isNone() }), true)
        kit.shutdown()
    }

    @TestCase
    public func testAskFromActorAndTimeout(): Unit {
        let (kit, sys) = newSystem("typed-ask-ctx")
        let rec = EventRecorder()
        let c = sys.actorOf(counter(41), "c")
        let silent = sys.actorOf(TypedBehaviors.ignore<CounterCmd>(), "silent")
        sys.actorOf(TypedBehaviors.setup<String>({ ctx =>
            ctx.ask<CounterCmd, Int64>(c, { r => CounterCmd.GetCount(r) }, T3S, { o =>
                match (o) {
                    case AskOutcome.Replied(n) => "got:${n + 1}"
                    case AskOutcome.Failed(_) => "failed"
                }
            })
            ctx.ask<CounterCmd, Int64>(silent, { r => CounterCmd.GetCount(r) }, Duration.millisecond * 50, { o =>
                match (o) {
                    case AskOutcome.Replied(_) => "unexpected"
                    case AskOutcome.Failed(e) => "timeout:${e is TypedAskTimeoutException}"
                }
            })
            TypedBehaviors.receiveMessage<String>({ m =>
                rec.record(m)
                TypedBehaviors.same<String>()
            })
        }), "asker")
        @Expect(rec.await("got:42", T3S), true)
        @Expect(rec.await("timeout:true", T3S), true)
        kit.shutdown()
    }

    @TestCase
    public func testExternalAskTimeout(): Unit {
        let (kit, sys) = newSystem("typed-ask-timeout")
        let silent = sys.actorOf(TypedBehaviors.ignore<CounterCmd>(), "silent")
        let p = sys.ask<CounterCmd, Int64>(silent, { r => CounterCmd.GetCount(r) }, Duration.millisecond * 30)
        var timedOut = false
        try {
            p.get(T3S)
        } catch (e: TypedAskTimeoutException) {
            timedOut = true
        }
        @Expect(timedOut, true)
        @Expect(p.isCompleted(), true)
        kit.shutdown()
    }

    @TestCase
    public func testMessageAdapterAndPipeToSelf(): Unit {
        let (kit, sys) = newSystem("typed-adapter")
        let rec = EventRecorder()
        let c = sys.actorOf(counter(9), "c")
        sys.actorOf(TypedBehaviors.setup<String>({ ctx =>
            // 计数器回复 Int64，适配成本 Actor 的 String 协议
            let adapter = ctx.messageAdapter<Int64>({ n => "count=${n}" })
            c << CounterCmd.GetCount(adapter)
            let promise = TypedPromise<Int64>()
            ctx.pipeToSelf<Int64>(promise, { o => "piped=${o.toOption() ?? -1}" })
            promise.trySuccess(7)
            TypedBehaviors.receiveMessage<String>({ m =>
                rec.record(m)
                TypedBehaviors.same<String>()
            })
        }), "adapting")
        @Expect(rec.await("count=9", T3S), true)
        @Expect(rec.await("piped=7", T3S), true)
        kit.shutdown()
    }

    @TestCase
    public func testReceiveTimeout(): Unit {
        let (kit, sys) = newSystem("typed-rt")
        let rec = EventRecorder()
        sys.actorOf(TypedBehaviors.setup<String>({ ctx =>
            ctx.setReceiveTimeout(Duration.millisecond * 30, "idle")
            TypedBehaviors.receiveMessage<String>({ m =>
                rec.record(m)
                if (m == "idle") {
                    ctx.cancelReceiveTimeout()
                }
                TypedBehaviors.same<String>()
            })
        }), "rt")
        @Expect(rec.await("idle", T3S), true)
        sleep(Duration.millisecond * 120)
        @Expect(rec.count("idle"), 1)
        kit.shutdown()
    }

    @TestCase
    public func testReceptionist(): Unit {
        let (kit, sys) = newSystem("typed-receptionist")
        let key = ServiceKey<CounterCmd>("counter")
        let rec = sys.receptionist()
        let listings = Collector<Int64>()
        rec.subscribe(key, { l: Listing<CounterCmd> => listings.add(l.size()) })
        let a = sys.actorOf(counter(1), "a")
        let b = sys.actorOf(counter(2), "b")
        rec.register(key, a)
        rec.register(key, b)
        rec.register(key, b)
        @Expect(rec.find(key).size(), 2)
        var sum: Int64 = 0
        for (r in rec.find(key).services) {
            sum += countOf(r)
        }
        @Expect(sum, 3)
        // 实例停止后自动注销
        a << CounterCmd.StopIt
        @Expect(awaitCondition(T3S, { => rec.find(key).size() == 1 }), true)
        rec.deregister(key, b)
        @Expect(rec.find(key).isEmpty(), true)
        // 初始 0，+a，+b，-a，-b
        @Expect(listings.awaitSize(5), true)
        @Expect(listings.snapshot()[0], 0)
        @Expect(listings.snapshot()[4], 0)
        kit.shutdown()
    }

    @TestCase
    public func testTransformMessagesWithSetupRunsOnce(): Unit {
        let (kit, sys) = newSystem("typed-transform")
        let rec = EventRecorder()
        let inner = TypedBehaviors.setup<Int64>({ _ =>
            rec.record("setup")
            TypedBehaviors.receiveMessage<Int64>({ n =>
                rec.record("n=${n}")
                TypedBehaviors.same<Int64>()
            })
        })
        let r = sys.actorOf(TypedBehaviors.transformMessages<String, Int64>(inner, { s => Int64(s.size) }), "t")
        r << "a" << "bb" << "ccc"
        @Expect(rec.await("n=3", T3S), true)
        @Expect(rec.count("setup"), 1)
        kit.shutdown()
    }

    @TestCase
    public func testFilterAndPartialCountUnhandled(): Unit {
        let (kit, sys) = newSystem("typed-filter")
        let col = Collector<Int64>()
        let r = sys.actorOf(TypedBehaviors.filter<Int64>(col.behavior(), { n => n % 2 == 0 }), "f")
        for (i in 1..=10) {
            r << i
        }
        @Expect(col.awaitSize(5), true)
        sleep(Duration.millisecond * 30)
        @Expect(col.size(), 5)
        kit.shutdown()
    }

    @TestCase
    public func testLookupAndSelection(): Unit {
        let (kit, sys) = newSystem("typed-lookup")
        sys.actorOf(counter(5), "worker")
        let found = sys.findActor<CounterCmd>("worker")
        @Expect(found.isSome(), true)
        @Expect(countOf(found.getOrThrow()), 5)
        @Expect(sys.findActor<CounterCmd>("nobody").isNone(), true)
        let sel = kit.system.typedActorSelection<CounterCmd>("/user/worker")
        sel.tell(CounterCmd.Increment(1))
        @Expect(countOf(sel.resolveOne(T3S).getOrThrow()), 6)
        kit.shutdown()
    }

    @TestCase
    public func testClassicInterop(): Unit {
        let (kit, sys) = newSystem("typed-interop")
        let probe = kit.createTestProbe()
        // 类型化 Actor 直接处理经典消息（协议类型是 Message）
        let typedRef = sys.actorOf(TypedBehaviors.receiveMessage<Message>({ m =>
            probe.ref.tell(StringMessage("typed got ${m.messageType()}"))
            TypedBehaviors.same<Message>()
        }), "typed")
        let classic = kit.system.unwrapTypedActor(typedRef)
        classic.tell(StringMessage("hello"))
        let reply = probe.expectMsgType<StringMessage>()
        @Expect(reply.getContent().contains("typed got"), true)
        // 经典引用包装为类型化引用
        let wrapped = kit.system.wrapAsTypedActor<Message>(probe.ref, "probe")
        wrapped.tell(StringMessage("via typed"))
        let viaTyped = probe.expectMsg()
        @Expect(viaTyped.messageType() == "TypedEnvelope" || viaTyped is StringMessage, true)
        kit.shutdown()
    }

    @TestCase
    public func testTypedPropsBoundedMailbox(): Unit {
        let (kit, sys) = newSystem("typed-props")
        let gate = AtomicBool(false)
        let col = Collector<Int64>()
        let r = sys.actorOf(TypedBehaviors.receiveMessage<Int64>({ n =>
            while (!gate.load()) {
                sleep(Duration.millisecond)
            }
            col.add(n)
            TypedBehaviors.same<Int64>()
        }), "bounded", TypedProps().withBoundedMailbox(5).withDispatcher("blocking"))
        for (i in 0..50) {
            r << i
        }
        let dropped = kit.system.deadLetterCount()
        gate.store(true)
        @Expect(dropped >= 40, true)
        sleep(Duration.millisecond * 100)
        @Expect(col.size() <= 6, true)
        kit.shutdown()
    }

    @TestCase
    public func testConcurrentSendersExactCount(): Unit {
        let (kit, sys) = newSystem("typed-concurrent")
        let c = sys.actorOf(counter(0), "c")
        let futures = ArrayList<Future<Unit>>()
        for (_ in 0..8) {
            futures.add(spawn {
                for (_ in 0..5000) {
                    c << CounterCmd.Increment(1)
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        @Expect(awaitCondition(Duration.second * 10, { => countOf(c) == 40000 }), true)
        kit.shutdown()
    }
}
