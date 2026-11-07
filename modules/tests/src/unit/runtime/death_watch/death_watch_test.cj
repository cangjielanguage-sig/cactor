/*
 * CActor - DeathWatch 语义回归（E32，对照 Akka DeathWatch）
 *
 *   - 目标死前发来的消息先于 Terminated 被处理：Terminated 入用户邮箱队尾（R6）
 *   - Terminated 入队后、处理前 unwatch：该 Terminated 被丢弃（Akka 的 terminatedQueued）
 *   - 重启时先 unwatch 再停子 Actor：新实例不会收到旧子的 Terminated（Akka 默认 preRestart，T3）
 *   - 重启后同名重建并 watch 的子 Actor 被正确监视（此前按路径判重被跳过，R18）
 *   - watchWith 映射保存在 Cell：类型化 Actor 重启后仍收到自定义消息，而不是 DeathPact（T3）
 *   - 有界邮箱已满时 Terminated 不丢失
 */
package cactor_tests.unit.runtime.death_watch

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}
import cactor_core.typed.{TypedBehavior, TypedBehaviors, TypedSupervisorStrategy, TypedActorRef}
import cactor_testkit.*

class Cmd <: Message {
    let text: String
    let ref: Option<ActorRef>

    init(text: String) {
        this(text, None<ActorRef>)
    }

    init(text: String, ref: Option<ActorRef>) {
        this.text = text
        this.ref = ref
    }

    public func messageType(): String {
        "Cmd(${text})"
    }
}

class DwBoom <: Exception {
    init(msg: String) {
        super(msg)
    }
}

/** 行为由闭包决定的测试 Actor；生命周期写入 recorder */
class Scripted <: Actor {
    let label: String
    let rec: EventRecorder
    let handler: (Scripted, Message, ActorContext) -> Unit
    var child: Option<ActorRef> = None<ActorRef>

    init(label: String, rec: EventRecorder, handler: (Scripted, Message, ActorContext) -> Unit) {
        this.label = label
        this.rec = rec
        this.handler = handler
    }

    public prop name: String {
        get() { label }
    }

    public prop description: String {
        get() { "scripted ${label}" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            // 未处理的 Terminated 交给运行时（DeathPact）
            case _: Terminated where !handlesTerminated => MessageResult.Unhandled
            case _ =>
                handler(this, message, context)
                if (stopNow) {
                    MessageResult.Stop
                } else {
                    MessageResult.Handled
                }
        }
    }

    var handlesTerminated: Bool = true
    var stopNow: Bool = false

    public func postStop(): Unit {
        rec.record("${label}:postStop")
    }

    public func postRestart(reason: Exception): Unit {
        rec.record("${label}:postRestart")
    }
}

func textOf(m: Message): String {
    match (m) {
        case c: Cmd => c.text
        case _ => m.messageType()
    }
}

let T = Duration.second * 5

/** 引用同一性（同路径的新旧两代 Actor 用 equals 比较会相等） */
func sameRef(a: ActorRef, b: ActorRef): Bool {
    match ((a as Object, b as Object)) {
        case (Some(x), Some(y)) => refEq(x, y)
        case _ => false
    }
}

/** 子 Actor：收到 "stop" 即停止 */
func stoppable(label: String, rec: EventRecorder): Scripted {
    Scripted(label, rec, { self, m, _ =>
        if (textOf(m) == "stop") {
            self.stopNow = true
        }
    })
}

@Test
public class DeathWatchTest {
    @TestCase
    public func messagesSentBeforeDeathArriveBeforeTerminated(): Unit {
        let kit = ActorTestKit("dw-order")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let x = kit.actorOf({ => Scripted("x", rec, { self, m, _ =>
            match (m) {
                case c: Cmd where c.text == "replyAndStop" =>
                    c.ref.getOrThrow().tell(Cmd("result"))
                    self.stopNow = true
                case _ => ()
            }
        }) }, "x")
        let w = kit.actorOf({ => Scripted("w", rec, { _, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "watch" =>
                    ctx.watch(c.ref.getOrThrow())
                    pr.tell(Cmd("watched"))
                case c: Cmd where c.text == "block" =>
                    rec.record("w:blocking")
                    // 等目标停止，再留一点时间让死亡通知进入本 Actor 的系统队列：
                    // 这样 Result 与死亡通知在本 Actor 下次处理时同时待处理（旧实现先处理系统消息，Terminated 抢在前面）
                    let _ = awaitCondition(T, { => rec.contains("x:postStop") })
                    sleep(Duration.millisecond * 50)
                case c: Cmd where c.text == "result" => rec.record("w:result")
                case _: Terminated => rec.record("w:terminated")
                case _ => ()
            }
        }) }, "w")
        w << Cmd("watch", Some(x))
        @Expect(probe.expectMsgType<Cmd>().text, "watched")
        w << Cmd("block")
        @Expect(rec.await("w:blocking", T), true)
        x << Cmd("replyAndStop", Some(w))
        @Expect(rec.await("w:terminated", T), true)
        let r = rec.indexOf("w:result")
        @Expect(r >= 0 && r < rec.indexOf("w:terminated"), true)
        kit.shutdown()
    }

    @TestCase
    public func unwatchAfterTerminatedQueuedDropsIt(): Unit {
        let kit = ActorTestKit("dw-unwatch-queued")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let x = kit.actorOf({ => stoppable("x", rec) }, "x")
        let w = kit.actorOf({ => Scripted("w", rec, { _, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "watch" =>
                    ctx.watch(c.ref.getOrThrow())
                    pr.tell(Cmd("watched"))
                case c: Cmd where c.text == "block" =>
                    rec.record("w:blocking")
                    let _ = awaitCondition(T, { => rec.contains("x:postStop") })
                    sleep(Duration.millisecond * 50)
                case c: Cmd where c.text == "unwatch" => ctx.unwatch(c.ref.getOrThrow())
                case c: Cmd where c.text == "ping" => pr.tell(Cmd("pong"))
                case _: Terminated => rec.record("w:terminated")
                case _ => ()
            }
        }) }, "w")
        w << Cmd("watch", Some(x))
        @Expect(probe.expectMsgType<Cmd>().text, "watched")
        // 邮箱：[block, unwatch, ping]；x 在 block 期间停止，Terminated 入队排在 unwatch 之后
        w << Cmd("block") << Cmd("unwatch", Some(x)) << Cmd("ping")
        @Expect(rec.await("w:blocking", T), true)
        x << Cmd("stop")
        @Expect(probe.expectMsgType<Cmd>().text, "pong")
        // Terminated 排在 ping 之前已被丢弃；再往返一次确认它也不在 ping 之后
        w << Cmd("ping")
        @Expect(probe.expectMsgType<Cmd>().text, "pong")
        @Expect(rec.contains("w:terminated"), false)
        kit.shutdown()
    }

    @TestCase
    public func restartUnwatchesChildrenBeforeStoppingThem(): Unit {
        // 父 Actor 不处理 Terminated：若重启后新实例收到旧子的 Terminated，就是 DeathPact，默认策略将其停止
        let kit = ActorTestKit("dw-restart-unwatch")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let p = kit.actorOf(PropsFactory.create<Actor>({ =>
            let s = Scripted("p", rec, { self, m, ctx =>
                match (textOf(m)) {
                    case "spawn" =>
                        let c = ctx.actorOf({ => stoppable("c", rec) }, "c")
                        ctx.watch(c)
                        pr.tell(Cmd("spawned"))
                    case "crash" => throw DwBoom("crash")
                    case "ping" => pr.tell(Cmd("pong"))
                    case _ => ()
                }
            })
            s.handlesTerminated = false
            s
        }), "p")
        p << Cmd("spawn")
        @Expect(probe.expectMsgType<Cmd>().text, "spawned")
        p << Cmd("crash")
        @Expect(rec.await("p:postRestart", T), true)
        @Expect(rec.await("c:postStop", T), true)
        // 旧子已停止；新实例仍然存活（没有因为旧子的 Terminated 触发 DeathPact）
        p << Cmd("ping")
        @Expect(probe.expectMsgType<Cmd>().text, "pong")
        // 恰好 1 次：旧实例由默认 preRestart 调用（E33 起与 Akka 相同）。新实例没有因 DeathPact 停止——
        // 重启要等旧子终止后才建新实例，旧子若有 Terminated 漏给新实例，也一定排在 ping 之前，pong 回来时已经处理过了
        @Expect(rec.count("p:postStop"), 1)
        kit.shutdown()
    }

    @TestCase
    public func recreatedChildWithSameNameIsWatched(): Unit {
        let kit = ActorTestKit("dw-rewatch")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let p = kit.actorOf({ =>
            Scripted("p", rec, { self, m, ctx =>
                match (m) {
                    case c: Cmd where c.text == "spawn" =>
                        let child = ctx.actorOf({ => stoppable("c", rec) }, "c")
                        self.child = Some(child)
                        ctx.watch(child)
                        pr.tell(Cmd("spawned"))
                    case c: Cmd where c.text == "crash" => throw DwBoom("crash")
                    case c: Cmd where c.text == "stopChild" => self.child.getOrThrow() << Cmd("stop")
                    case t: Terminated =>
                        // 只应收到当前这一代子 Actor 的 Terminated
                        let current = match (self.child) {
                            case Some(c) => sameRef(c, t.actor)
                            case None => false
                        }
                        rec.record("p:terminated:${current}")
                    case _ => ()
                }
            })
        }, "p")
        p << Cmd("spawn")
        @Expect(probe.expectMsgType<Cmd>().text, "spawned")
        // 重启：旧子被停止；新实例立即以同名重建并 watch
        p << Cmd("crash") << Cmd("spawn")
        @Expect(probe.expectMsgType<Cmd>().text, "spawned")
        p << Cmd("stopChild")
        @Expect(rec.await("p:terminated:true", T), true)
        @Expect(rec.contains("p:terminated:false"), false)
        kit.shutdown()
    }

    @TestCase
    public func terminatedIsNotDroppedWhenBoundedMailboxIsFull(): Unit {
        let kit = ActorTestKit("dw-bounded")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let x = kit.actorOf({ => stoppable("x", rec) }, "x")
        let w = kit.actorOf(PropsFactory.create<Actor>({ => Scripted("w", rec, { _, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "watch" =>
                    ctx.watch(c.ref.getOrThrow())
                    pr.tell(Cmd("watched"))
                case c: Cmd where c.text == "block" =>
                    rec.record("w:blocking")
                    let _ = awaitCondition(T, { => rec.contains("x:postStop") })
                    sleep(Duration.millisecond * 50)
                case _: Terminated => rec.record("w:terminated")
                case _ => ()
            }
        }) }).withBoundedMailbox(2), "w")
        w << Cmd("watch", Some(x))
        @Expect(probe.expectMsgType<Cmd>().text, "watched")
        w << Cmd("block")
        @Expect(rec.await("w:blocking", T), true)
        // 填满容量为 2 的邮箱，再让目标停止
        w << Cmd("fill-1") << Cmd("fill-2")
        x << Cmd("stop")
        @Expect(rec.await("w:terminated", T), true)
        kit.shutdown()
    }

    @TestCase
    public func classicWatchWithDeliversCustomMessage(): Unit {
        let kit = ActorTestKit("dw-watchwith")
        let rec = EventRecorder()
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let x = kit.actorOf({ => stoppable("x", rec) }, "x")
        let w = kit.actorOf({ => Scripted("w", rec, { _, m, ctx =>
            match (m) {
                case c: Cmd where c.text == "watch" =>
                    ctx.watchWith(c.ref.getOrThrow(), Cmd("x-gone"))
                    pr.tell(Cmd("watched"))
                case c: Cmd where c.text == "x-gone" => rec.record("w:x-gone")
                case _: Terminated => rec.record("w:terminated")
                case _ => ()
            }
        }) }, "w")
        w << Cmd("watch", Some(x))
        @Expect(probe.expectMsgType<Cmd>().text, "watched")
        x << Cmd("stop")
        @Expect(rec.await("w:x-gone", T), true)
        @Expect(rec.contains("w:terminated"), false)
        kit.shutdown()
    }
}

// ---------------- 类型化：watchWith 跨重启 ----------------

enum WCmd {
    | WatchIt(TypedActorRef<String>)
    | Crash
    | TargetGone
}

@Test
public class TypedDeathWatchTest {
    @TestCase
    public func watchWithSurvivesRestart(): Unit {
        let kit = ActorTestKit("dw-typed-watchwith")
        let sys = kit.system.typed()
        let rec = EventRecorder()
        let target = sys.actorOf(TypedBehaviors.receiveMessage<String>({ m =>
            if (m == "stop") {
                TypedBehaviors.stopped<String>()
            } else {
                TypedBehaviors.same<String>()
            }
        }), "target")
        let a = sys.actorOf(TypedBehaviors.supervise(TypedBehaviors.setup<WCmd>({ _ =>
            rec.record("a:setup")
            TypedBehaviors.receive<WCmd>({ ctx, cmd =>
                match (cmd) {
                    case WCmd.WatchIt(ref) =>
                        ctx.watchWith(ref, WCmd.TargetGone)
                        rec.record("a:watching")
                    case WCmd.Crash => throw DwBoom("crash")
                    case WCmd.TargetGone => rec.record("a:target-gone")
                }
                TypedBehaviors.same<WCmd>()
            })
        })).onFailure(TypedSupervisorStrategy.restart()), "a")
        a << WCmd.WatchIt(target)
        @Expect(rec.await("a:watching", T), true)
        a << WCmd.Crash
        @Expect(awaitCondition(T, { => rec.count("a:setup") == 2 }), true)
        // 重启后的新实例：目标停止时收到 TargetGone（此前映射随旧上下文丢失，收到未处理的 Terminated → DeathPact）
        target << "stop"
        @Expect(rec.await("a:target-gone", T), true)
        @Expect(rec.count("a:setup"), 2)
        kit.shutdown()
    }
}
