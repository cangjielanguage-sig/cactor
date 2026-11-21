/*
 * CActor - trace 在本地 Actor 消息边界的自动传播（4.10 剩余部分）
 *
 * 远程投递的 traceparent 早已随帧入邮箱（RemoteTraceTest）；这里补的是**本地** actor→actor：
 * 发送线程处于追踪作用域时，runtime 自动把消息包一层 TracedMessage，接收方在 receive 前
 * 续接出本地子 span。断言：
 *   - A 收到的 traceId 等于发起处；
 *   - A 在 receive 里告诉 B，B 的 traceId 仍是同一个（span 不同）；
 *   - 没有追踪作用域时不产生 traceId（不回退、不误传播）。
 */
package cactor_tests.unit.runtime.trace

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.logging.{Mdc}
import cactor_foundation.monitoring.{TraceContext, generateSpanId, generateTraceId, withTraceMdc}
import cactor_testkit.*

/** 记录本次 receive 期间 MDC 里的 traceId|spanId；可选地把消息再转发给下一个 Actor */
class LocalTraceActor <: Actor {
    let seen: EventRecorder
    let next: Option<ActorRef>

    init(seen: EventRecorder, next: Option<ActorRef>) {
        this.seen = seen
        this.next = next
    }

    public prop name: String { get() { "local-trace" } }
    public prop description: String { get() { "local trace probe" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        seen.record("${Mdc.get("traceId") ?? ""}|${Mdc.get("spanId") ?? ""}")
        match (next) {
            case Some(n) => n.tell(StringMessage("hop"))
            case None => ()
        }
        MessageResult.Handled
    }
}

func traceIdOf(line: String): String {
    line.split("|")[0]
}

@Test
public class LocalTraceTest {
    @TestCase
    public func localTraceIdPropagatesBetweenActors(): Unit {
        let kit = ActorTestKit("local-trace")
        try {
            let seen = EventRecorder()
            let b = kit.system.actorOf({ => LocalTraceActor(seen, None<ActorRef>) }, "trace-b")
            let a = kit.system.actorOf({ => LocalTraceActor(seen, Some<ActorRef>(b)) }, "trace-a")
            let ctx = TraceContext(generateTraceId(), generateSpanId(), None<String>, HashMap<String, String>())
            withTraceMdc(ctx, { => a.tell(StringMessage("start")) })
            @Expect(awaitCondition(Duration.second * 5, { => seen.size() == 2 }), true)
            let lines = seen.snapshot()
            @Expect(traceIdOf(lines[0]), ctx.traceId)
            // 转发到本地另一个 Actor：traceId 延续，span 是接收方续接出的新子 span
            @Expect(traceIdOf(lines[1]), ctx.traceId)
            @Expect(lines[1] != lines[0], true)
        } finally {
            kit.shutdown()
        }
    }

    @TestCase
    public func noTraceScopeLeavesMdcEmpty(): Unit {
        let kit = ActorTestKit("local-trace-plain")
        try {
            let seen = EventRecorder()
            let ref = kit.system.actorOf({ => LocalTraceActor(seen, None<ActorRef>) }, "plain")
            ref.tell(StringMessage("plain"))
            @Expect(awaitCondition(Duration.second * 5, { => seen.size() == 1 }), true)
            @Expect(traceIdOf(seen.snapshot()[0]), "")
        } finally {
            kit.shutdown()
        }
    }
}
