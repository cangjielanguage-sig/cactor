/*
 * CActor - system.stop 对齐 Akka（E33 收尾）
 *
 *   - 立即停止：处理完当前消息即停，邮箱中剩余的消息计为死信
 *     （此前在已入队的消息之后才停，失败后被监督停止的 Actor 也要先把积压处理完）
 *   - 名字保留到真正终止：终止前同名 actorOf 报"名称已存在"
 *     （此前 stop 时立即释放名字，停止中的旧实例与同名新实例短暂共存）
 *   - system.stop 对子 Actor 同样生效（此前静默无效）
 *   - gracefulStop：发 PoisonPill，处理完已入队的消息后停止，返回时名字已释放（对标 Akka Patterns.gracefulStop）
 */
package cactor_tests.unit.runtime.stop_protocol

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}
import cactor_testkit.*

/** postStop 阻塞到 release：把"停止中"的窗口拉长 */
class SlowStop <: Actor {
    let rec: EventRecorder
    let release: AtomicBool

    init(rec: EventRecorder, release: AtomicBool) {
        this.rec = rec
        this.release = release
    }

    public prop name: String {
        get() { "slow-stop" }
    }

    public prop description: String {
        get() { "slow-stop" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }

    public func postStop(): Unit {
        rec.record("slow:postStop:begin")
        let _ = awaitCondition(Duration.second * 10, { => release.load() })
        rec.record("slow:postStop")
    }
}

/** 启动时建一个子 Actor "kid"（Gate） */
class SpParent <: Actor {
    let rec: EventRecorder
    let release: AtomicBool

    init(rec: EventRecorder, release: AtomicBool) {
        this.rec = rec
        this.release = release
    }

    public prop name: String {
        get() { "sp-parent" }
    }

    public prop description: String {
        get() { "sp-parent" }
    }

    public func onStarted(context: ActorContext): Unit {
        let r = rec
        let g = release
        context.actorOf(PropsFactory.create<Actor>({ => Gate(r, g) }), "kid") |> ignoreRef
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }

    public func postStop(): Unit {
        rec.record("parent:postStop")
    }
}

func ignoreRef(_: ActorRef): Unit {}

func gotCount(rec: EventRecorder, prefix: String, n: Int64): Int64 {
    var c = 0
    for (i in 0..n) {
        if (rec.contains("got:${prefix}${i}")) {
            c++
        }
    }
    c
}

@Test
public class StopSemanticsTest {
    @TestCase
    public func systemStopDropsQueuedMessagesToDeadLetters(): Unit {
        let kit = ActorTestKit("ss-drop")
        let rec = EventRecorder()
        let release = AtomicBool(false)
        let ref = kit.actorOf({ => Gate(rec, release) }, "gate")
        ref << SpMsg("block")
        @Expect(rec.await("blocking", Duration.second * 5), true)
        for (i in 0..5) {
            ref << SpMsg("q${i}")
        }
        let before = kit.system.deadLetterCount()
        kit.system.stop(ref)
        release.store(true)
        @Expect(rec.await("postStop", Duration.second * 5), true)
        // 正在处理的消息照常处理完；排在后面的 5 条不再处理，计为死信
        @Expect(rec.contains("got:block"), true)
        @Expect(gotCount(rec, "q", 5), 0)
        @Expect(awaitCondition(Duration.second * 5, { => kit.system.deadLetterCount() - before >= 5 }), true)
        kit.shutdown()
    }

    @TestCase
    public func nameIsReservedUntilTerminated(): Unit {
        let kit = ActorTestKit("ss-name")
        let rec = EventRecorder()
        let release = AtomicBool(false)
        let ref = kit.actorOf({ => SlowStop(rec, release) }, "slow")
        kit.system.stop(ref)
        @Expect(rec.await("slow:postStop:begin", Duration.second * 5), true)
        // 旧实例还在停止：同名创建被拒绝，按路径仍能查到它
        var clash = false
        try {
            kit.actorOf({ => SlowStop(rec, AtomicBool(true)) }, "slow") |> ignoreRef
        } catch (e: Exception) {
            clash = true
        }
        @Expect(clash, true)
        @Expect(kit.system.lookup("/user/slow").isSome(), true)
        release.store(true)
        @Expect(awaitCondition(Duration.second * 5, { => kit.system.lookup("/user/slow").isNone() }), true)
        // 终止后名字可以复用
        var reused = true
        try {
            kit.actorOf({ => SlowStop(rec, AtomicBool(true)) }, "slow") |> ignoreRef
        } catch (e: Exception) {
            reused = false
        }
        @Expect(reused, true)
        kit.shutdown()
    }

    @TestCase
    public func systemStopWorksOnChildActors(): Unit {
        let kit = ActorTestKit("ss-child")
        let rec = EventRecorder()
        let release = AtomicBool(true)
        kit.actorOf({ => SpParent(rec, release) }, "parent") |> ignoreRef
        @Expect(awaitCondition(Duration.second * 5, { => kit.system.lookup("/user/parent/kid").isSome() }), true)
        let kid = kit.system.lookup("/user/parent/kid").getOrThrow()
        kit.system.stop(kid)
        @Expect(rec.await("postStop", Duration.second * 5), true)
        @Expect(awaitCondition(Duration.second * 5, { => kit.system.lookup("/user/parent/kid").isNone() }), true)
        // 父 Actor 不受影响
        @Expect(rec.contains("parent:postStop"), false)
        @Expect(kit.system.lookup("/user/parent").isSome(), true)
        kit.shutdown()
    }

    @TestCase
    public func gracefulStopProcessesQueuedMessagesThenReleasesName(): Unit {
        let kit = ActorTestKit("ss-graceful")
        let rec = EventRecorder()
        let release = AtomicBool(false)
        let ref = kit.actorOf({ => Gate(rec, release) }, "graceful")
        ref << SpMsg("block")
        @Expect(rec.await("blocking", Duration.second * 5), true)
        for (i in 0..3) {
            ref << SpMsg("q${i}")
        }
        // 超时：目标还卡着，返回 false
        @Expect(kit.system.gracefulStop(ref, Duration.millisecond * 100), false)
        let result = AtomicBool(false)
        let done = AtomicBool(false)
        let waiter = spawn {
            result.store(kit.system.gracefulStop(ref, Duration.second * 5))
            done.store(true)
        }
        // 已入队的消息还没处理完：不返回
        @Expect(awaitCondition(Duration.millisecond * 200, { => done.load() }), false)
        release.store(true)
        waiter.get()
        @Expect(result.load(), true)
        @Expect(gotCount(rec, "q", 3), 3)
        @Expect(rec.contains("postStop"), true)
        // 返回时名字已经释放
        var reused = true
        try {
            kit.actorOf({ => Gate(rec, AtomicBool(true)) }, "graceful") |> ignoreRef
        } catch (e: Exception) {
            reused = false
        }
        @Expect(reused, true)
        kit.shutdown()
    }
}
