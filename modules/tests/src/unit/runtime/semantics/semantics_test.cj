/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - Actor 语义测试（R1）
 * 覆盖：self/reply、子 Actor 与路径、停止协议、死亡监视、行为切换、Stash、
 * 接收超时、定时器、PoisonPill / Kill、层级监督（Resume / Restart / Stop / Escalate / OneForAll）、
 * 有界与优先级邮箱、阻塞调度器隔离、死信订阅
 */
package cactor_tests.unit.runtime.semantics

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem}
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.supervision.{OneForOneStrategy, OneForAllStrategy}
import cactor_testkit.*

// ---------------- 测试消息 ----------------

class Cmd <: Message {
    let text: String
    init(text: String) { this.text = text }
    public func messageType(): String { "Cmd(${text})" }
}

class Num <: Message {
    let value: Int64
    init(value: Int64) { this.value = value }
    public func messageType(): String { "Num(${value})" }
}

class Prio <: Message {
    let value: Int64
    let p: Int32
    init(value: Int64, p: Int32) {
        this.value = value
        this.p = p
    }
    public func messageType(): String { "Prio(${value})" }
    public func priority(): Int32 { p }
}

class Boom <: Exception {
    init(msg: String) { super(msg) }
}

func text(m: Message): String {
    match (m) {
        case c: Cmd => c.text
        case n: Num => "n${n.value}"
        case s: StringMessage => s.getContent()
        case _ => m.messageType()
    }
}

/** 通用测试 Actor：handler 决定行为，所有生命周期回调写入 recorder */
class Probeable <: Actor {
    let label: String
    let rec: EventRecorder
    let handler: (Probeable, Message, ActorContext) -> MessageResult
    var state: Int64 = 0
    var strategy: Option<SupervisionStrategy> = None<SupervisionStrategy>

    init(label: String, rec: EventRecorder, handler: (Probeable, Message, ActorContext) -> MessageResult) {
        this.label = label
        this.rec = rec
        this.handler = handler
    }

    public prop name: String { get() { label } }
    public prop description: String { get() { "test actor ${label}" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        handler(this, message, context)
    }

    public func preStart(): Unit { rec.record("${label}:preStart") }
    public func postStop(): Unit { rec.record("${label}:postStop") }
    public func preRestart(reason: Exception): Unit { rec.record("${label}:preRestart") }
    public func postRestart(reason: Exception): Unit { rec.record("${label}:postRestart") }

    public func supervisorStrategy(): Option<SupervisionStrategy> { strategy }
}

func handled(): MessageResult { MessageResult.Handled }

let T = Duration.second * 3

@Test
public class ActorSemanticsTest {

    // ---------- self / reply / sender ----------

    @TestCase
    public func selfAndReply(): Unit {
        let kit = ActorTestKit("sem-reply")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let echo = kit.actorOf({ => Probeable("echo", rec, { a, m, ctx =>
            ctx.reply(Cmd("echo:${text(m)}:${ctx.self.path.getPath()}"))
            handled()
        }) }, "echo")
        probe.send(echo, Cmd("hi"))
        let r = probe.expectMsgType<Cmd>()
        @Expect(r.text, "echo:hi:/user/echo")
        @Expect(probe.lastSender().getOrThrow().path.getPath(), "/user/echo")
        kit.shutdown()
    }

    @TestCase
    public func sendWithSelfAsSender(): Unit {
        // A 用 ctx.send 发给 B，B 回复，A 收到后转给探针
        let kit = ActorTestKit("sem-send")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let b = kit.actorOf({ => Probeable("b", rec, { a, m, ctx => ctx.reply(Cmd("pong")); handled() }) }, "b")
        let pr = probe.ref
        let a = kit.actorOf({ => Probeable("a", rec, { self, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "go" => ctx.send(b, Cmd("ping"))
                case c: Cmd => pr.tell(Cmd("${c.text} from ${ctx.senderRef().getOrThrow().path.getPath()}"))
                case _ => ()
            }
            handled()
        }) }, "a")
        a << Cmd("go")
        @Expect(probe.expectMsgType<Cmd>().text, "pong from /user/b")
        kit.shutdown()
    }

    // ---------- 子 Actor、路径与查找 ----------

    @TestCase
    public func childrenPathsAndLookup(): Unit {
        let kit = ActorTestKit("sem-children")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let parent = kit.actorOf({ => Probeable("p", rec, { a, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "spawn" =>
                    let c1 = ctx.actorOf({ => Probeable("c1", rec, { _, _, _ => handled() }) }, "c1")
                    let c2 = ctx.actorOf({ => Probeable("anon", rec, { _, _, _ => handled() }) })
                    var dupRejected = false
                    try {
                        ctx.actorOf({ => Probeable("dup", rec, { _, _, _ => handled() }) }, "c1")
                    } catch (e: InvalidActorNameException) {
                        dupRejected = true
                    }
                    pr.tell(Cmd("${c1.path.getPath()}|${c2.path.getPath().startsWith("/user/p/$")}|${ctx.children().size}|${dupRejected}|${ctx.child("c1").isSome()}|${ctx.parent().isNone()}"))
                case _ => ()
            }
            handled()
        }) }, "p")
        parent << Cmd("spawn")
        @Expect(probe.expectMsgType<Cmd>().text, "/user/p/c1|true|2|true|true|true")
        @Expect(kit.system.lookup("/user/p/c1").isSome(), true)
        @Expect(kit.system.lookup("/user/p/nope").isNone(), true)
        kit.shutdown()
    }

    // ---------- 停止协议 ----------

    @TestCase
    public func stopStopsChildrenFirst(): Unit {
        let kit = ActorTestKit("sem-stop-order")
        let rec = EventRecorder()
        let parent = kit.actorOf({ => Probeable("p", rec, { a, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "spawn" =>
                    let c1 = ctx.actorOf({ => Probeable("c1", rec, { _, _, _ => handled() }) }, "c1")
                    c1 << Cmd("nest")
                    ctx.actorOf({ => Probeable("c2", rec, { s, mm, cc =>
                        match (mm) {
                            case x: Cmd where x.text == "nest" =>
                                cc.actorOf({ => Probeable("g1", rec, { _, _, _ => handled() }) }, "g1")
                                ()
                            case _ => ()
                        }
                        handled()
                    }) }, "c2") << Cmd("nest")
                case _ => ()
            }
            handled()
        }) }, "p")
        parent << Cmd("spawn")
        @Expect(rec.await("g1:preStart", T), true)
        kit.system.stop(parent)
        @Expect(rec.await("p:postStop", T), true)
        // 子孙先于父 Actor 结束
        @Expect(rec.indexOf("g1:postStop") < rec.indexOf("c2:postStop"), true)
        @Expect(rec.indexOf("c1:postStop") < rec.indexOf("p:postStop"), true)
        @Expect(rec.indexOf("c2:postStop") < rec.indexOf("p:postStop"), true)
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/p/c2/g1").isNone() && kit.system.lookup("/user/p").isNone() }), true)
        kit.shutdown()
    }

    @TestCase
    public func contextStopSelfAndChild(): Unit {
        let kit = ActorTestKit("sem-ctx-stop")
        let rec = EventRecorder()
        let parent = kit.actorOf({ => Probeable("p", rec, { a, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "spawn" =>
                    ctx.actorOf({ => Probeable("c1", rec, { _, _, _ => handled() }) }, "c1")
                    ()
                case c: Cmd where c.text == "stopChild" => ctx.stop(ctx.child("c1").getOrThrow())
                case c: Cmd where c.text == "stopSelf" =>
                    ctx.stop(ctx.self)
                    rec.record("after-stop-call")
                case c: Cmd => rec.record("p:got:${c.text}")
                case _ => ()
            }
            handled()
        }) }, "p")
        parent << Cmd("spawn") << Cmd("stopChild")
        @Expect(rec.await("c1:postStop", T), true)
        @Expect(rec.count("p:postStop"), 0)
        parent << Cmd("stopSelf") << Cmd("late")
        @Expect(rec.await("p:postStop", T), true)
        // 当前消息处理完才停止；之后的消息不再处理
        @Expect(rec.contains("after-stop-call"), true)
        @Expect(rec.contains("p:got:late"), false)
        kit.shutdown()
    }

    @TestCase
    public func messageResultStopCleansUp(): Unit {
        let kit = ActorTestKit("sem-result-stop")
        let rec = EventRecorder()
        let a = kit.actorOf({ => Probeable("a", rec, { _, m, _ =>
            if (text(m) == "bye") { MessageResult.Stop } else { handled() }
        }) }, "a")
        a << Cmd("x") << Cmd("bye") << Cmd("after")
        @Expect(rec.await("a:postStop", T), true)
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/a").isNone() }), true)
        @Expect(kit.system.getRuntime().getUserGuardian().getUserActor("a").isNone(), true)
        // 名称释放后可以重新创建
        @Expect(awaitCondition(T, { =>
            try {
                kit.actorOf({ => Probeable("a2", rec, { _, _, _ => handled() }) }, "a")
                true
            } catch (_: Exception) { false }
        }), true)
        kit.shutdown()
    }

    // ---------- PoisonPill / Kill ----------

    @TestCase
    public func poisonPillAfterQueuedMessages(): Unit {
        let kit = ActorTestKit("sem-poison")
        let rec = EventRecorder()
        let a = kit.actorOf({ => Probeable("a", rec, { _, m, _ => rec.record("got:${text(m)}"); handled() }) }, "a")
        a << Cmd("1") << Cmd("2") << PoisonPill() << Cmd("3")
        @Expect(rec.await("a:postStop", T), true)
        @Expect(rec.contains("got:1") && rec.contains("got:2"), true)
        @Expect(rec.contains("got:3"), false)
        kit.shutdown()
    }

    @TestCase
    public func killTriggersSupervisionRestart(): Unit {
        let kit = ActorTestKit("sem-kill")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let a = kit.actorOf({ => Probeable("a", rec, { self, m, _ =>
            self.state++
            pr.tell(Num(self.state))
            handled()
        }) }, "a")
        a << Cmd("x") << Cmd("y")
        @Expect(probe.receiveN(2, T).size, 2)
        a << Kill() << Cmd("z")
        // 重启后状态清零
        @Expect(probe.expectMsgType<Num>().value, 1)
        @Expect(rec.count("a:preRestart"), 1)
        @Expect(rec.count("a:postRestart"), 1)
        kit.shutdown()
    }

    // ---------- 死亡监视 ----------

    @TestCase
    public func watchDeliversTerminatedOnce(): Unit {
        let kit = ActorTestKit("sem-watch")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let target = kit.actorOf({ => Probeable("t", rec, { _, _, _ => handled() }) }, "t")
        let watcher = kit.actorOf({ => Probeable("w", rec, { _, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "watch" =>
                    ctx.watch(target)
                    ctx.watch(target) // 重复 watch 只投递一次
                    pr.tell(Cmd("watching"))
                case t: Terminated => pr.tell(Cmd("terminated:${t.actor.path.getPath()}"))
                case _ => ()
            }
            handled()
        }) }, "w")
        watcher << Cmd("watch")
        @Expect(probe.expectMsgType<Cmd>().text, "watching")
        target << PoisonPill()
        @Expect(probe.expectMsgType<Cmd>().text, "terminated:/user/t")
        probe.expectNoMsg(Duration.millisecond * 200)
        kit.shutdown()
    }

    @TestCase
    public func watchAlreadyTerminatedActor(): Unit {
        let kit = ActorTestKit("sem-watch-dead")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let target = kit.actorOf({ => Probeable("t", rec, { _, _, _ => handled() }) }, "t")
        target << PoisonPill()
        @Expect(rec.await("t:postStop", T), true)
        let watcher = kit.actorOf({ => Probeable("w", rec, { _, m, ctx =>
            match (m) {
                case c: Cmd => ctx.watch(target)
                case t: Terminated => pr.tell(Cmd("terminated"))
                case _ => ()
            }
            handled()
        }) }, "w")
        watcher << Cmd("watch")
        @Expect(probe.expectMsgType<Cmd>().text, "terminated")
        kit.shutdown()
    }

    @TestCase
    public func unwatchSuppressesTerminated(): Unit {
        let kit = ActorTestKit("sem-unwatch")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let target = kit.actorOf({ => Probeable("t", rec, { _, _, _ => handled() }) }, "t")
        let watcher = kit.actorOf({ => Probeable("w", rec, { _, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "watch" => ctx.watch(target)
                case c: Cmd where c.text == "unwatch" =>
                    ctx.unwatch(target)
                    pr.tell(Cmd("unwatched"))
                case t: Terminated => pr.tell(Cmd("terminated"))
                case _ => ()
            }
            handled()
        }) }, "w")
        watcher << Cmd("watch") << Cmd("unwatch")
        @Expect(probe.expectMsgType<Cmd>().text, "unwatched")
        target << PoisonPill()
        @Expect(rec.await("t:postStop", T), true)
        probe.expectNoMsg(Duration.millisecond * 200)
        kit.shutdown()
    }

    @TestCase
    public func unhandledTerminatedIsDeathPact(): Unit {
        // 监视者不处理 Terminated（返回 Unhandled）→ DeathPactException → 默认策略重启监视者
        let kit = ActorTestKit("sem-deathpact")
        let rec = EventRecorder()
        let target = kit.actorOf({ => Probeable("t", rec, { _, _, _ => handled() }) }, "t")
        let watcher = kit.actorOf({ => Probeable("w", rec, { _, m, ctx =>
            match (m) {
                case c: Cmd =>
                    ctx.watch(target)
                    handled()
                case _ => MessageResult.Unhandled
            }
        }) }, "w")
        watcher << Cmd("watch")
        sleep(Duration.millisecond * 50)
        target << PoisonPill()
        @Expect(rec.await("w:postRestart", T), true)
        kit.shutdown()
    }

    // ---------- 行为切换 ----------

    @TestCase
    public func becomeAndUnbecome(): Unit {
        let kit = ActorTestKit("sem-become")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let angry: Receive = { m, ctx =>
            match (m) {
                case c: Cmd where c.text == "calm" => ctx.unbecome()
                case _ => pr.tell(Cmd("angry:${text(m)}"))
            }
            MessageResult.Handled
        }
        let a = kit.actorOf({ => Probeable("a", rec, { _, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "anger" => ctx.become(angry, false)
                case _ => pr.tell(Cmd("happy:${text(m)}"))
            }
            handled()
        }) }, "a")
        a << Cmd("x") << Cmd("anger") << Cmd("y") << Cmd("calm") << Cmd("z")
        let got = probe.receiveN(3, T)
        @Expect(text(got[0]), "happy:x")
        @Expect(text(got[1]), "angry:y")
        @Expect(text(got[2]), "happy:z")
        kit.shutdown()
    }

    // ---------- Stash ----------

    @TestCase
    public func stashAndUnstashKeepOrder(): Unit {
        let kit = ActorTestKit("sem-stash")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let a = kit.actorOf({ => Probeable("a", rec, { self, m, ctx =>
            if (self.state == 0) {
                match (m) {
                    case c: Cmd where c.text == "open" =>
                        self.state = 1
                        ctx.unstashAll()
                    case _ => ctx.stash()
                }
            } else {
                pr.tell(m)
            }
            handled()
        }) }, "a")
        a << Num(1) << Num(2) << Num(3) << Cmd("open") << Num(4)
        let got = probe.receiveN(4, T)
        @Expect("${text(got[0])},${text(got[1])},${text(got[2])},${text(got[3])}", "n1,n2,n3,n4")
        kit.shutdown()
    }

    // ---------- 接收超时 ----------

    @TestCase
    public func receiveTimeoutFiresWhenIdle(): Unit {
        let kit = ActorTestKit("sem-rt")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let start = MonoTime.now()
        let a = kit.actorOf({ => Probeable("a", rec, { self, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "arm" => ctx.setReceiveTimeout(Duration.millisecond * 100)
                case c: Cmd where c.text == "disarm" => ctx.cancelReceiveTimeout()
                case _: ReceiveTimeout => pr.tell(Cmd("timeout"))
                case _ => ()
            }
            handled()
        }) }, "a")
        a << Cmd("arm")
        @Expect(probe.expectMsgType<Cmd>().text, "timeout")
        @Expect(MonoTime.now() - start >= Duration.millisecond * 100, true)
        // 空闲期间重复投递
        @Expect(probe.expectMsgType<Cmd>().text, "timeout")
        a << Cmd("disarm")
        probe.expectNoMsg(Duration.millisecond * 300)
        kit.shutdown()
    }

    @TestCase
    public func receiveTimeoutResetByActivity(): Unit {
        let kit = ActorTestKit("sem-rt-reset")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let a = kit.actorOf({ => Probeable("a", rec, { self, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "arm" => ctx.setReceiveTimeout(Duration.millisecond * 200)
                case _: ReceiveTimeout => pr.tell(Cmd("timeout"))
                case _ => ()
            }
            handled()
        }) }, "a")
        a << Cmd("arm")
        // 每 50ms 一条消息，持续 500ms：不应超时
        for (_ in 0..10) {
            sleep(Duration.millisecond * 50)
            a << Cmd("tick")
        }
        probe.expectNoMsg(Duration.millisecond * 50)
        @Expect(probe.expectMsgType<Cmd>().text, "timeout")
        kit.shutdown()
    }

    // ---------- 定时器 ----------

    @TestCase
    public func singleAndPeriodicTimers(): Unit {
        let kit = ActorTestKit("sem-timers")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let a = kit.actorOf({ => Probeable("a", rec, { self, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "start" =>
                    ctx.timers.startSingleTimer("once", Cmd("once"), Duration.millisecond * 30)
                    ctx.timers.startTimerWithFixedDelay("tick", Cmd("tick"), Duration.millisecond * 20)
                case c: Cmd where c.text == "tick" =>
                    self.state++
                    if (self.state == 5) {
                        ctx.timers.cancel("tick")
                        pr.tell(Cmd("ticks-done:${ctx.timers.isTimerActive("tick")}"))
                    }
                case c: Cmd where c.text == "once" => pr.tell(Cmd("once:${ctx.timers.isTimerActive("once")}"))
                case _ => ()
            }
            handled()
        }) }, "a")
        a << Cmd("start")
        let got = probe.receiveN(2, T)
        let joined = "${text(got[0])}|${text(got[1])}"
        @Expect(joined.contains("once:false"), true)
        @Expect(joined.contains("ticks-done:false"), true)
        // 取消之后不再有 tick
        probe.expectNoMsg(Duration.millisecond * 150)
        kit.shutdown()
    }

    @TestCase
    public func restartingTimerWithSameKeyReplacesOld(): Unit {
        let kit = ActorTestKit("sem-timer-replace")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let a = kit.actorOf({ => Probeable("a", rec, { self, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "start" =>
                    ctx.timers.startSingleTimer("k", Cmd("old"), Duration.millisecond * 40)
                    ctx.timers.startSingleTimer("k", Cmd("new"), Duration.millisecond * 60)
                case c: Cmd => pr.tell(c)
                case _ => ()
            }
            handled()
        }) }, "a")
        a << Cmd("start")
        @Expect(probe.expectMsgType<Cmd>().text, "new")
        probe.expectNoMsg(Duration.millisecond * 150)
        kit.shutdown()
    }

    @TestCase
    public func timersCancelledOnStop(): Unit {
        let kit = ActorTestKit("sem-timer-stop")
        let rec = EventRecorder()
        let count = AtomicInt64(0)
        let a = kit.actorOf({ => Probeable("a", rec, { self, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "start" =>
                    ctx.timers.startTimerAtFixedRate("t", Cmd("t"), Duration.millisecond * 10)
                case c: Cmd where c.text == "t" => count.fetchAdd(1)
                case _ => ()
            }
            handled()
        }) }, "a")
        a << Cmd("start")
        @Expect(awaitCondition(T, { => count.load() >= 3 }), true)
        a << PoisonPill()
        @Expect(rec.await("a:postStop", T), true)
        let snap = count.load()
        sleep(Duration.millisecond * 100)
        @Expect(count.load(), snap)
        kit.shutdown()
    }

    // ---------- 层级监督 ----------

    @TestCase
    public func parentStrategyResumeKeepsState(): Unit {
        let kit = ActorTestKit("sem-sup-resume")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let parent = kit.actorOf({ =>
            let p = Probeable("p", rec, { self, m, ctx =>
                match (m) {
                    case c: Cmd where c.text == "spawn" =>
                        ctx.actorOf({ => Probeable("c", rec, { cs, cm, _ =>
                            match (cm) {
                                case x: Cmd where x.text == "fail" => throw Boom("boom")
                                case _ =>
                                    cs.state++
                                    pr.tell(Num(cs.state))
                            }
                            handled()
                        }) }, "c")
                        ()
                    case _ => ()
                }
                handled()
            })
            p.strategy = Some(OneForOneStrategy(10, Duration.minute, { _ => SupervisionDirective.Resume }))
            p
        }, "p")
        parent << Cmd("spawn")
        let child = { => kit.system.lookup("/user/p/c") }
        @Expect(awaitCondition(T, { => child().isSome() }), true)
        child().getOrThrow() << Cmd("a") << Cmd("fail") << Cmd("b")
        @Expect(probe.expectMsgType<Num>().value, 1)
        @Expect(probe.expectMsgType<Num>().value, 2)
        @Expect(rec.count("c:preRestart"), 0)
        kit.shutdown()
    }

    @TestCase
    public func parentStrategyRestartResetsState(): Unit {
        let kit = ActorTestKit("sem-sup-restart")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let parent = kit.actorOf({ => Probeable("p", rec, { self, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "spawn" =>
                    ctx.actorOf({ => Probeable("c", rec, { cs, cm, _ =>
                        match (cm) {
                            case x: Cmd where x.text == "fail" => throw Boom("boom")
                            case _ =>
                                cs.state++
                                pr.tell(Num(cs.state))
                        }
                        handled()
                    }) }, "c")
                    ()
                case _ => ()
            }
            handled()
        }) }, "p")
        parent << Cmd("spawn")
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/p/c").isSome() }), true)
        let c = kit.system.lookup("/user/p/c").getOrThrow()
        c << Cmd("a") << Cmd("b") << Cmd("fail") << Cmd("c")
        @Expect(probe.expectMsgType<Num>().value, 1)
        @Expect(probe.expectMsgType<Num>().value, 2)
        @Expect(probe.expectMsgType<Num>().value, 1) // 重启后状态清零
        @Expect(rec.count("c:preRestart"), 1)
        @Expect(rec.count("p:preRestart"), 0)
        kit.shutdown()
    }

    @TestCase
    public func parentStrategyStopStopsChild(): Unit {
        let kit = ActorTestKit("sem-sup-stop")
        let rec = EventRecorder()
        let parent = kit.actorOf({ =>
            let p = Probeable("p", rec, { self, m, ctx =>
                match (m) {
                    case c: Cmd where c.text == "spawn" =>
                        let child = ctx.actorOf({ => Probeable("c", rec, { _, _, _ => throw Boom("boom") }) }, "c")
                        ctx.watch(child)
                    case t: Terminated => rec.record("p:saw-terminated")
                    case _ => ()
                }
                handled()
            })
            p.strategy = Some(OneForOneStrategy(10, Duration.minute, { _ => SupervisionDirective.Stop }))
            p
        }, "p")
        parent << Cmd("spawn")
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/p/c").isSome() }), true)
        kit.system.lookup("/user/p/c").getOrThrow() << Cmd("x")
        @Expect(rec.await("p:saw-terminated", T), true)
        @Expect(rec.contains("c:postStop"), true)
        @Expect(kit.system.lookup("/user/p").isSome(), true)
        kit.shutdown()
    }

    @TestCase
    public func escalateRestartsParentAndItsChildren(): Unit {
        let kit = ActorTestKit("sem-sup-escalate")
        let rec = EventRecorder()
        let parent = kit.actorOf({ =>
            let p = Probeable("p", rec, { self, m, ctx =>
                match (m) {
                    case c: Cmd where c.text == "spawn" =>
                        ctx.actorOf({ => Probeable("c", rec, { _, _, _ => throw Boom("boom") }) }, "c")
                        ()
                    case _ => ()
                }
                handled()
            })
            p.strategy = Some(OneForOneStrategy(10, Duration.minute, { _ => SupervisionDirective.Escalate }))
            p
        }, "p")
        parent << Cmd("spawn")
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/p/c").isSome() }), true)
        kit.system.lookup("/user/p/c").getOrThrow() << Cmd("x")
        // 子 Actor 上报 → 父 Actor 失败 → 顶层默认策略重启父 Actor → 重启时停止旧的子 Actor
        @Expect(rec.await("p:postRestart", T), true)
        @Expect(rec.await("c:postStop", T), true)
        @Expect(rec.count("c:preRestart"), 0)
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/p/c").isNone() }), true)
        kit.shutdown()
    }

    @TestCase
    public func oneForAllRestartsSiblings(): Unit {
        let kit = ActorTestKit("sem-sup-all")
        let rec = EventRecorder()
        let parent = kit.actorOf({ =>
            let p = Probeable("p", rec, { self, m, ctx =>
                match (m) {
                    case c: Cmd where c.text == "spawn" =>
                        for (i in 1..=3) {
                            ctx.actorOf({ => Probeable("c${i}", rec, { _, cm, _ =>
                                if (text(cm) == "fail") { throw Boom("boom") }
                                handled()
                            }) }, "c${i}")
                        }
                    case _ => ()
                }
                handled()
            })
            p.strategy = Some(OneForAllStrategy(10, Duration.minute, { _ => SupervisionDirective.Restart }))
            p
        }, "p")
        parent << Cmd("spawn")
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/p/c3").isSome() }), true)
        kit.system.lookup("/user/p/c2").getOrThrow() << Cmd("fail")
        @Expect(awaitCondition(T, { => rec.count("c1:postRestart") == 1 && rec.count("c2:postRestart") == 1 && rec.count("c3:postRestart") == 1 }), true)
        kit.shutdown()
    }

    @TestCase
    public func propsStrategyOverridesParent(): Unit {
        let kit = ActorTestKit("sem-sup-props")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let parent = kit.actorOf({ => Probeable("p", rec, { self, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "spawn" =>
                    let props = PropsFactory.create<Actor>({ => Probeable("c", rec, { cs, cm, _ =>
                        if (text(cm) == "fail") { throw Boom("boom") }
                        cs.state++
                        pr.tell(Num(cs.state))
                        handled()
                    }) }).withSupervisorStrategy(OneForOneStrategy(10, Duration.minute, { _ => SupervisionDirective.Resume }))
                    ctx.actorOf(props, "c")
                    ()
                case _ => ()
            }
            handled()
        }) }, "p")
        parent << Cmd("spawn")
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/p/c").isSome() }), true)
        kit.system.lookup("/user/p/c").getOrThrow() << Cmd("a") << Cmd("fail") << Cmd("b")
        @Expect(probe.expectMsgType<Num>().value, 1)
        @Expect(probe.expectMsgType<Num>().value, 2) // Resume 保留状态（默认会 Restart）
        kit.shutdown()
    }

    @TestCase
    public func retryLimitStopsChild(): Unit {
        let kit = ActorTestKit("sem-sup-limit")
        let rec = EventRecorder()
        let parent = kit.actorOf({ =>
            let p = Probeable("p", rec, { self, m, ctx =>
                match (m) {
                    case c: Cmd where c.text == "spawn" =>
                        let child = ctx.actorOf({ => Probeable("c", rec, { _, _, _ => throw Boom("boom") }) }, "c")
                        ctx.watch(child)
                    case _: Terminated => rec.record("child-gone")
                    case _ => ()
                }
                handled()
            })
            p.strategy = Some(OneForOneStrategy(2, Duration.minute, { _ => SupervisionDirective.Restart }))
            p
        }, "p")
        parent << Cmd("spawn")
        @Expect(awaitCondition(T, { => kit.system.lookup("/user/p/c").isSome() }), true)
        let c = kit.system.lookup("/user/p/c").getOrThrow()
        for (_ in 0..5) {
            c << Cmd("x")
        }
        @Expect(rec.await("child-gone", T), true)
        @Expect(rec.count("c:postRestart"), 2)
        kit.shutdown()
    }

    // ---------- 邮箱与调度器 ----------

    @TestCase
    public func boundedMailboxDropsOverflow(): Unit {
        let kit = ActorTestKit("sem-bounded")
        let rec = EventRecorder()
        let gate = AtomicBool(false)
        let processed = AtomicInt64(0)
        let dead = AtomicInt64(0)
        kit.system.subscribeDeadLetters({ m, path => if (path == "/user/b") { dead.fetchAdd(1) } })
        let props = PropsFactory.create<Actor>({ => Probeable("b", rec, { _, _, _ =>
            while (!gate.load()) { sleep(Duration.millisecond) }
            processed.fetchAdd(1)
            handled()
        }) }).withBoundedMailbox(10).withDispatcher("blocking")
        let b = kit.actorOf(props, "b")
        b << Cmd("first")
        sleep(Duration.millisecond * 50) // 第一条已被取出并阻塞在 receive 中
        for (i in 0..20) {
            b << Num(i)
        }
        gate.store(true)
        @Expect(awaitCondition(T, { => processed.load() == 11 }), true)
        @Expect(dead.load(), 10)
        sleep(Duration.millisecond * 50)
        @Expect(processed.load(), 11)
        kit.shutdown()
    }

    @TestCase
    public func priorityMailboxOrdersByPriority(): Unit {
        let kit = ActorTestKit("sem-priority")
        let rec = EventRecorder()
        let gate = AtomicBool(false)
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let props = PropsFactory.create<Actor>({ => Probeable("q", rec, { _, m, _ =>
            match (m) {
                case c: Cmd where c.text == "block" =>
                    while (!gate.load()) { sleep(Duration.millisecond) }
                case p: Prio => pr.tell(Num(p.value))
                case _ => ()
            }
            handled()
        }) }).withMailbox("priority").withDispatcher("blocking")
        let q = kit.actorOf(props, "q")
        q << Cmd("block")
        sleep(Duration.millisecond * 50)
        q << Prio(1, 0) << Prio(2, 5) << Prio(3, 0) << Prio(4, 9) << Prio(5, 5)
        gate.store(true)
        let got = probe.receiveN(5, T)
        let order = "${text(got[0])},${text(got[1])},${text(got[2])},${text(got[3])},${text(got[4])}"
        // 优先级高的先处理；同优先级先进先出
        @Expect(order, "n4,n2,n5,n1,n3")
        kit.shutdown()
    }

    @TestCase
    public func blockingDispatcherIsolatesSlowActors(): Unit {
        let kit = ActorTestKit("sem-blocking")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        // 32 个会阻塞 200ms 的 Actor 放在阻塞调度器上
        for (i in 0..32) {
            let props = PropsFactory.create<Actor>({ => Probeable("slow${i}", rec, { _, _, _ =>
                sleep(Duration.millisecond * 200)
                handled()
            }) }).withDispatcher("blocking")
            kit.actorOf(props, "slow${i}") << Cmd("work")
        }
        // 默认调度器上的 Actor 仍然能及时响应
        let fast = kit.actorOf({ => Probeable("fast", rec, { _, m, _ => pr.tell(m); handled() }) }, "fast")
        let start = MonoTime.now()
        fast << Cmd("ping")
        @Expect(probe.expectMsgType<Cmd>().text, "ping")
        @Expect(MonoTime.now() - start < Duration.millisecond * 100, true)
        kit.shutdown()
    }

    // ---------- 死信 ----------

    @TestCase
    public func deadLettersAfterStop(): Unit {
        let kit = ActorTestKit("sem-deadletters")
        let rec = EventRecorder()
        let seen = EventRecorder()
        kit.system.subscribeDeadLetters({ m, path => seen.record("${path}:${text(m)}") })
        let a = kit.actorOf({ => Probeable("a", rec, { _, _, _ => handled() }) }, "a")
        a << PoisonPill()
        @Expect(rec.await("a:postStop", T), true)
        a << Cmd("lost")
        @Expect(seen.await("/user/a:lost", T), true)
        @Expect(kit.system.deadLetterCount() >= 1, true)
        kit.shutdown()
    }

    @TestCase
    public func replyWithoutSenderIsDeadLetter(): Unit {
        let kit = ActorTestKit("sem-reply-dead")
        let rec = EventRecorder()
        let seen = EventRecorder()
        kit.system.subscribeDeadLetters({ m, path => seen.record(text(m)) })
        let a = kit.actorOf({ => Probeable("a", rec, { _, _, ctx => ctx.reply(Cmd("nobody")); handled() }) }, "a")
        a << Cmd("x")
        @Expect(seen.await("nobody", T), true)
        kit.shutdown()
    }

    @TestCase
    public func stopNonChildIsRejected(): Unit {
        let kit = ActorTestKit("sem-stop-reject")
        let rec = EventRecorder()
        let other = kit.actorOf({ => Probeable("o", rec, { _, _, _ => handled() }) }, "o")
        let a = kit.actorOf({ => Probeable("a", rec, { _, _, ctx =>
            try {
                ctx.stop(other)
            } catch (e: IllegalArgumentException) {
                rec.record("rejected")
            }
            handled()
        }) }, "a")
        a << Cmd("x")
        @Expect(rec.await("rejected", T), true)
        @Expect(rec.contains("o:postStop"), false)
        kit.shutdown()
    }

    // ---------- 并发：树形结构上的并发收发与停止 ----------

    @TestCase
    public func concurrentTreeTraffic(): Unit {
        let kit = ActorTestKit("sem-tree")
        let rec = EventRecorder()
        let total = AtomicInt64(0)
        let parent = kit.actorOf({ => Probeable("p", rec, { self, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "spawn" =>
                    for (i in 0..8) {
                        ctx.actorOf({ => Probeable("w${i}", rec, { ws, wm, _ =>
                            ws.state++
                            total.fetchAdd(1)
                            handled()
                        }) }, "w${i}")
                    }
                case n: Num =>
                    let kids = ctx.children()
                    kids[n.value % kids.size] << n
                case _ => ()
            }
            handled()
        }) }, "p")
        parent << Cmd("spawn")
        let senders = ArrayList<Future<Unit>>()
        for (t in 0..8) {
            senders.add(spawn {
                for (i in 0..2000) {
                    parent << Num(t * 2000 + i)
                }
            })
        }
        for (f in senders) {
            f.get()
        }
        @Expect(awaitCondition(Duration.second * 10, { => total.load() == 16000 }), true)
        kit.system.stop(parent)
        @Expect(rec.await("p:postStop", T), true)
        for (i in 0..8) {
            @Expect(rec.contains("w${i}:postStop"), true)
        }
        @Expect(awaitCondition(T, { => kit.system.getRuntime().getUserGuardian().liveCellCount() <= 1 }), true)
        kit.shutdown()
    }
}
