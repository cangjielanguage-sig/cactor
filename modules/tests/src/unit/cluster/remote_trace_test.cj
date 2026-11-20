/*
 * CActor - trace 上下文跨节点传播（P7 / G2）
 *
 * 发送侧在 MDC 里带 traceId/spanId 时，远程帧尾附带 W3C traceparent；
 * 接收侧拆出 traceparent 并随消息进邮箱，ActorCell 在 receive 期间续接为本地子 span，
 * 因此 B 侧 Actor 日志里读到的 traceId 与 A 侧发起处一致。
 *
 * 帧尾扩展是向后兼容的：没有 trace 时不写块，老读者读完消息体即停止，不需要升线协议版本。
 */
package cactor_tests.unit.cluster

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.logging.{Mdc}
import cactor_foundation.monitoring.{TraceContext, generateSpanId, generateTraceId, withTraceMdc}
import cactor_testkit.*

class TraceProbeActor <: Actor {
    let seen = EventRecorder()

    public prop name: String {
        get() { "trace-probe" }
    }

    public prop description: String {
        get() { "trace propagation probe" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage =>
                seen.record(Mdc.get("traceId") ?? "")
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }

    func traceIds(): Array<String> {
        seen.snapshot()
    }
}

@Test
public class RemoteTraceTest {
    @TestCase
    public func traceIdPropagatesAcrossNodes(): Unit {
        let nodes = startCluster(2, "trace", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let probe = TraceProbeActor()
        let ref = nodes[1].kit.system.actorOf({ => probe }, "trace-probe")
        nodes[1].remote.exposeActor("trace-probe", ref)
        let remoteRef = nodes[0].remote.remoteActorOf(nodes[1].addr(), "trace-probe")

        let traceId = generateTraceId()
        let spanId = generateSpanId()
        let ctx = TraceContext(traceId, spanId, None<String>, HashMap<String, String>())
        // 发送线程带 MDC → 帧尾自动附 traceparent
        withTraceMdc(ctx, { =>
            remoteRef.tell(StringMessage("ping"))
        })
        @Expect(awaitCondition(CLUSTER_WAIT, { => probe.seen.size() == 1 }), true)
        @Expect(probe.traceIds()[0], traceId)

        // 没有 MDC 时不产生追踪包装：接收侧 traceId 为空
        remoteRef.tell(StringMessage("plain"))
        @Expect(awaitCondition(CLUSTER_WAIT, { => probe.seen.size() == 2 }), true)
        @Expect(probe.traceIds()[1], "")
        closeAll(nodes)
    }
}
