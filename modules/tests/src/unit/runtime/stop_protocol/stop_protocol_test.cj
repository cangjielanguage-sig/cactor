/*
 * CActor - 停止协议（E33 部分）
 *
 *   - system.stop 不受有界邮箱容量影响：邮箱满时仍然生效，Fail 策略下也不抛异常（R7）
 *   - 启动即失败的顶层 Actor 反复同名创建：注销与登记串行化，不会留下"名称已存在"的死条目（R11）
 */
package cactor_tests.unit.runtime.stop_protocol

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}
import cactor_testkit.*

class SpMsg <: Message {
    let text: String

    init(text: String) {
        this.text = text
    }

    public func messageType(): String {
        "SpMsg(${text})"
    }
}

class SpBoom <: Exception {
    init(msg: String) {
        super(msg)
    }
}

/** 收到 block 后占住线程直到 release；记录处理过的消息 */
class Gate <: Actor {
    let rec: EventRecorder
    let release: AtomicBool

    init(rec: EventRecorder, release: AtomicBool) {
        this.rec = rec
        this.release = release
    }

    public prop name: String {
        get() { "gate" }
    }

    public prop description: String {
        get() { "gate" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(m) <- (message as SpMsg)) {
            if (m.text == "block") {
                rec.record("blocking")
                let _ = awaitCondition(Duration.second * 10, { => release.load() })
            }
            rec.record("got:${m.text}")
        }
        MessageResult.Handled
    }

    public func postStop(): Unit {
        rec.record("postStop")
    }
}

class FailsOnStart <: Actor {
    public prop name: String {
        get() { "fails-on-start" }
    }

    public prop description: String {
        get() { "fails-on-start" }
    }

    public func onStarted(context: ActorContext): Unit {
        throw SpBoom("依赖不可用")
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

@Test
public class StopProtocolTest {
    @TestCase
    public func systemStopWorksWhenBoundedMailboxIsFull(): Unit {
        for (overflow in [MailboxOverflow.DropNew, MailboxOverflow.Fail]) {
            let kit = ActorTestKit("sp-full")
            let rec = EventRecorder()
            let release = AtomicBool(false)
            let ref = kit.actorOf(PropsFactory.create<Actor>({ => Gate(rec, release) }).withBoundedMailbox(2, overflow),
                "gate")
            ref << SpMsg("block")
            @Expect(rec.await("blocking", Duration.second * 5), true)
            ref << SpMsg("q1") << SpMsg("q2")
            // 邮箱已满：此前 StopMessage 被丢弃（DropNew）或 stop 本身抛异常（Fail），Actor 继续运行
            var threw = false
            try {
                kit.system.stop(ref)
            } catch (e: Exception) {
                threw = true
            }
            release.store(true)
            @Expect(threw, false)
            @Expect(rec.await("postStop", Duration.second * 5), true)
            // 与 Akka 一致（E33）：处理完当前消息即停，已入队的 q1、q2 不再处理（计为死信）
            @Expect(rec.contains("got:block"), true)
            @Expect(rec.contains("got:q1"), false)
            @Expect(rec.contains("got:q2"), false)
            kit.shutdown()
        }
    }

    @TestCase
    public func failingTopLevelActorCanBeRecreatedWithSameName(): Unit {
        let kit = ActorTestKit("sp-recreate")
        var failures: Int64 = 0
        for (i in 0..200) {
            try {
                kit.actorOf({ => FailsOnStart() }, "flaky")
            } catch (e: Exception) {
                failures++
            }
            if (!awaitCondition(Duration.second * 5, { => kit.system.lookup("/user/flaky").isNone() })) {
                failures++
            }
        }
        @Expect(failures, 0)
        kit.shutdown()
    }
}
