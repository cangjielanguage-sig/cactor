/*
 * CActor - 重启协议与生命周期钩子（E33，对标 Akka）
 *
 *   - 默认钩子与 Akka 一致：preRestart 默认调用 postStop，postRestart 默认调用 preStart
 *   - 重启等旧子 Actor 全部终止后才创建新实例（Recreating）：此前不等，新旧同名子 Actor 并存
 *   - preStart 在 Actor 自己的线程执行；失败按启动失败交给监督（默认停止），不再从 actorOf 抛给创建方
 */
package cactor_tests.unit.runtime.lifecycle

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_testkit.*

class RpBoom <: Exception {
    init(msg: String) {
        super(msg)
    }
}

/** 实例编号：每创建一个新实例加 1 */
class Gen {
    let n = AtomicInt64(0)
}

/** 子 Actor：postStop 等待 release 后才返回，用来拖住"旧子 Actor 终止" */
class SlowChild <: Actor {
    let rec: EventRecorder
    let release: AtomicBool
    let id: Int64

    init(rec: EventRecorder, release: AtomicBool, gen: Gen) {
        this.rec = rec
        this.release = release
        this.id = gen.n.fetchAdd(1) + 1
    }

    public prop name: String {
        get() { "slow-child" }
    }

    public prop description: String {
        get() { "slow-child" }
    }

    public func preStart(): Unit {
        rec.record("C${id}:preStart")
    }

    public func postStop(): Unit {
        let _ = awaitCondition(Duration.second * 10, { => release.load() })
        rec.record("C${id}:postStop")
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

/** 只覆盖 preStart / postStop / onStarted：preRestart / postRestart 用默认实现 */
class RpParent <: Actor {
    let rec: EventRecorder
    let release: AtomicBool
    let childGen: Gen
    let id: Int64

    init(rec: EventRecorder, release: AtomicBool, gen: Gen, childGen: Gen) {
        this.rec = rec
        this.release = release
        this.childGen = childGen
        this.id = gen.n.fetchAdd(1) + 1
    }

    public prop name: String {
        get() { "rp-parent" }
    }

    public prop description: String {
        get() { "rp-parent" }
    }

    public func preStart(): Unit {
        rec.record("P${id}:preStart")
    }

    public func onStarted(context: ActorContext): Unit {
        rec.record("P${id}:onStarted")
        // 新旧实例都创建同名子 Actor：旧子 Actor 未终止时同名创建会失败
        context.actorOf({ => SlowChild(rec, release, childGen) }, "kid") |> ignoreRef
    }

    public func postStop(): Unit {
        rec.record("P${id}:postStop")
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            if (s.getContent() == "fail") {
                throw RpBoom("fail")
            }
            rec.record("P${id}:got:${s.getContent()}")
        }
        MessageResult.Handled
    }
}

class PreStartProbe <: Actor {
    let rec: EventRecorder
    let fail: Bool

    init(rec: EventRecorder, fail: Bool) {
        this.rec = rec
        this.fail = fail
    }

    public prop name: String {
        get() { "prestart-probe" }
    }

    public prop description: String {
        get() { "prestart-probe" }
    }

    public func preStart(): Unit {
        rec.record("thread:${Thread.currentThread.id}")
        if (fail) {
            throw RpBoom("preStart 失败")
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

@Test
public class RestartProtocolTest {
    @TestCase
    public func restartWaitsForOldChildrenAndUsesAkkaDefaultHooks(): Unit {
        let kit = ActorTestKit("restart-protocol")
        let rec = EventRecorder()
        let release = AtomicBool(false)
        // 编号器放在工厂闭包外：每次重启由同一个工厂创建新实例，编号递增
        let parentGen = Gen()
        let childGen = Gen()
        let p = kit.actorOf({ => RpParent(rec, release, parentGen, childGen) }, "p")
        @Expect(rec.await("C1:preStart", Duration.second * 5), true)
        p << StringMessage("fail")
        // 默认 preRestart 调用 postStop（此前调用 preStop，旧实例的 postStop 永远不会执行）
        @Expect(rec.await("P1:postStop", Duration.second * 5), true)
        // 旧子 Actor 还没终止：新实例不能创建（此前立即创建，新旧 "kid" 并存）
        p << StringMessage("during-recreate")
        // 用 onStarted 判断（新旧代码都会调用它），不只看 preStart（旧代码的新实例根本不调 preStart）
        @Expect(awaitCondition(Duration.millisecond * 300, { =>
            rec.contains("P2:preStart") || rec.contains("P2:onStarted") || rec.contains("C2:preStart")
        }), false)
        release.store(true)
        // 旧子 Actor 确实被停止了（postStop 执行完），之后才轮到新实例
        @Expect(rec.await("C1:postStop", Duration.second * 5), true)
        @Expect(rec.await("P2:onStarted", Duration.second * 5), true)
        @Expect(rec.await("C2:preStart", Duration.second * 5), true)
        // 顺序：旧子终止 → 新实例 preStart（默认 postRestart 调用 preStart）→ onStarted → 新子
        @Expect(rec.indexOf("C1:postStop") < rec.indexOf("P2:onStarted"), true)
        @Expect(rec.indexOf("C1:postStop") < rec.indexOf("P2:preStart"), true)
        @Expect(rec.indexOf("P2:preStart") < rec.indexOf("P2:onStarted"), true)
        // 等待期间到达的消息由新实例处理
        @Expect(rec.await("P2:got:during-recreate", Duration.second * 5), true)
        @Expect(rec.count("P1:got:during-recreate"), 0)
        kit.shutdown()
    }

    @TestCase
    public func preStartRunsOnActorThreadAndFailureStopsActor(): Unit {
        let kit = ActorTestKit("prestart-thread")
        let rec = EventRecorder()
        let ok = kit.actorOf({ => PreStartProbe(rec, false) }, "ok")
        @Expect(rec.awaitSize(1, Duration.second * 5), true)
        // 不在创建方线程上执行
        @Expect(rec.snapshot()[0] != "thread:${Thread.currentThread.id}", true)
        ok << StringMessage("x")
        // preStart 失败：actorOf 不抛异常（启动失败交给监督，默认停止）
        var threw = false
        let bad = try {
            Some(kit.actorOf({ => PreStartProbe(rec, true) }, "bad"))
        } catch (e: Exception) {
            threw = true
            None<ActorRef>
        }
        @Expect(threw, false)
        match (bad) {
            case Some(b) => @Expect(awaitCondition(Duration.second * 5, { => b.isTerminated() }), true)
            case None => ()
        }
        kit.shutdown()
    }
}

func ignoreRef(_: ActorRef): Unit {}
