/*
 * CActor Patterns.Backpressure - C2 组合接口 askWithBackpressure 验证
 *
 * 1. 下游积压达到水位时快速失败（AskBackpressuredException），而不是让请求排队到 ask 超时；
 *    抛出的异常带 inFlight/limit，便于上游按容量降速；
 * 2. 可写时退化为普通 AskInbox.ask：目标 context.reply 即可，调用方 get() 拿回复。
 */
package cactor_tests.unit.patterns

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_patterns.backpressure.{askWithBackpressure, AskBackpressuredException}
import cactor_testkit.*

/** 立即用 context.reply 回复 "echo:<内容>" */
class EchoReply <: Actor {
    public prop name: String { get() { "echo-reply" } }
    public prop description: String { get() { "echo reply" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage =>
                context.reply(StringMessage("echo:${s.getContent()}"))
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

@Test
public class AskBackpressureTest {
    @TestCase
    public func askWithBackpressureFailsFastWhenSaturated(): Unit {
        let system = CActorSystemFactory.create("ask-bp-sat-${MonoTime.now().hashCode()}")
        let started = AtomicInt64(0)
        let release = AtomicBool(false)
        let processed = AtomicInt64(0)
        let ref = system.actorOf({ => GatedActor(started, release, processed) }, "gated")
        try {
            ref.tell(StringMessage("busy"))
            @Expect(awaitCondition(Duration.second * 10, { => started.load() == 1 }), true)
            ref.tell(StringMessage("q1"))
            ref.tell(StringMessage("q2"))
            @Expect(awaitCondition(Duration.second * 10, { => ref.pendingCount() >= 2 }), true)

            var thrown = false
            try {
                let _ = askWithBackpressure(ref, StringMessage("q3"),
                    limit: 2, writableTimeout: Duration.millisecond * 100)
            } catch (e: AskBackpressuredException) {
                thrown = true
                @Expect(e.limit, 2)
                @Expect(e.inFlight >= 2, true)
            }
            @Expect(thrown, true)

            // 被拒的 ask 没有入队：释放后只处理 busy + q1 + q2 三条
            release.store(true)
            @Expect(awaitCondition(Duration.second * 10, { => processed.load() == 3 }), true)
        } finally {
            release.store(true)
            system.terminate()
        }
    }

    @TestCase
    public func askWithBackpressureDeliversWhenWritable(): Unit {
        let system = CActorSystemFactory.create("ask-bp-ok-${MonoTime.now().hashCode()}")
        try {
            let ref = system.actorOf({ => EchoReply() }, "echo")
            let reply = askWithBackpressure(ref, StringMessage("ping"), limit: 16,
                writableTimeout: Duration.second).get(Duration.second * 3)
            let s = (reply as StringMessage).getOrThrow()
            @Expect(s.getContent(), "echo:ping")
        } finally {
            system.terminate()
        }
    }
}
