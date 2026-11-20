/*
 * CActor Platform.Edge - 接入层（P3 / F4）
 *
 * 覆盖三件事：
 *   1. 连接在 node0、业务 Actor 在 node1：入站按 Receptionist 解析到远程 Actor，
 *      出站经 sender 回复到 Edge 节点的 sink，再落到该节点的流；
 *   2. 断线重连：带恢复令牌从 lastAck + 1 续读，不重放已确认消息，也不丢未确认消息；
 *   3. Edge 无状态：换一个 router 实例（等价于 Edge 节点重启）凭令牌和同一份流存储即可续传，
 *      会话计数由 Actor 自己保存（证明业务状态不在 Edge）。
 */
package cactor_tests.unit.cluster

import std.collection.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_persistence.{MemoryDurableStateStore}
import cactor_platform.durable.{StreamRecord}
import cactor_platform.edge.*
import cactor_testkit.*

/** 有状态的会话 Actor：计数保存在 Actor 内，回复带序号（用来证明 Edge 重启不丢业务状态） */
class EdgeStatefulActor <: Actor {
    private let lock = Mutex()
    private var count: Int64 = 0
    private let received = EventRecorder()

    public prop name: String {
        get() { "edge-stateful" }
    }

    public prop description: String {
        get() { "edge test stateful actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage =>
                let n = synchronized(lock) {
                    count += 1
                    count
                }
                received.record(s.getContent())
                context.reply(StringMessage("${n}|${s.getContent()}"))
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }

    func saw(text: String): Bool {
        received.contains(text)
    }
}

func expectDelivered(result: EdgeDeliverResult): Unit {
    match (result) {
        case Delivered => ()
        case NoRoute(sid) => @Fail("会话 ${sid} 应当可路由")
        case Forbidden(path) => @Fail("会话 ${path} 不应被拒绝")
    }
}

@Test
public class EdgeRouterTest {
    @TestCase
    public func edgeWebSocketRoutesToClusterActor(): Unit {
        let nodes = receptionistCluster("edge-route", FAST)
        let actor = EdgeStatefulActor()
        let ref = nodes[1].node.kit.system.actorOf({ => actor }, "edge-session-room-1")
        nodes[1].receptionist.register("edge-session-room-1", ref)
        // 等登记经 Replicator 传播到 node0，之后 node0 上的 Edge 才能解析到 node1 的 Actor
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[0].receptionist.find("edge-session-room-1").size == 1 }), true)

        let store = MemoryDurableStateStore()
        let router = EdgeRouter(nodes[0].node.kit.system, store,
            ReceptionistEdgeResolver(nodes[0].receptionist, servicePrefix: "edge-session"), "edge-secret")
        match (router.connect("room-1")) {
            case Connected(conn) =>
                expectDelivered(conn.deliver("hello"))
                // 远程 Actor 收到（node1）
                @Expect(awaitCondition(CLUSTER_WAIT, { => actor.saw("hello") }), true)
                // 回复经 sender 回到 node0 的 sink，再写入 node0 的流
                let stream = router.streamFor("room-1")
                @Expect(awaitCondition(CLUSTER_WAIT, { => stream.nextOffset() == 2 }), true)
                let r = conn.poll(timeout: Duration.millisecond * 200)
                @Expect(r.records.size, 1)
                @Expect(r.records[0].payload, "1|hello")
                conn.ack(r.records[0].offset)
                @Expect(conn.lastAckedOffset(), 1)
            case Unauthorized(_, reason) => @Fail("连接被拒绝: ${reason}")
        }
        let cns = ArrayList<CNode>()
        for (r in nodes) {
            cns.add(r.node)
        }
        closeAll(cns.toArray())
    }

    @TestCase
    public func edgeReconnectResumesStreamFromLastAck(): Unit {
        let kit = ActorTestKit("edge-resume")
        let store = MemoryDurableStateStore()
        let actor = EdgeStatefulActor()
        let ref = kit.actorOf({ => actor }, "edge-resume-actor")
        let resolver = LocalEdgeResolver()
        resolver.bind("s1", ref)
        let router = EdgeRouter(kit.system, store, resolver, "edge-secret")
        let stream = router.streamFor("s1")

        var first: Option<EdgeConnection> = None
        match (router.connect("s1")) {
            case Connected(c) => first = Some(c)
            case Unauthorized(_, reason) => @Fail("连接被拒绝: ${reason}")
        }
        let c1 = first.getOrThrow()
        expectDelivered(c1.deliver("a"))
        expectDelivered(c1.deliver("b"))
        @Expect(awaitCondition(Duration.second * 5, { => stream.nextOffset() == 3 }), true)

        let batch = c1.poll(timeout: Duration.millisecond * 200)
        @Expect(batch.records.size, 2)
        @Expect(batch.records[0].payload, "1|a")
        @Expect(batch.records[1].payload, "2|b")
        // 只确认第一条：第二条仍属"未确认"，重连后必须重发
        c1.ack(batch.records[0].offset)
        @Expect(c1.lastAckedOffset(), 1)
        let token = c1.recoveryToken()

        // 等价于 Edge 节点重启：新 router 不共享任何连接状态，只共享持久流存储
        let router2 = EdgeRouter(kit.system, store, resolver, "edge-secret")
        var second: Option<EdgeConnection> = None
        match (router2.connect("s1", resumeToken: Some(token))) {
            case Connected(c) => second = Some(c)
            case Unauthorized(_, reason) => @Fail("重连被拒绝: ${reason}")
        }
        let c2 = second.getOrThrow()
        @Expect(c2.lastAckedOffset(), 1)
        let resumed = c2.poll(timeout: Duration.millisecond * 200)
        @Expect(resumed.records.size, 1)
        @Expect(resumed.records[0].offset, 2)
        @Expect(resumed.records[0].payload, "2|b")
        c2.ack(resumed.records[0].offset)

        // 业务状态在 Actor：重连后计数继续为 3（Edge 没有保存 count）
        expectDelivered(c2.deliver("c"))
        @Expect(awaitCondition(Duration.second * 5, { => stream.nextOffset() == 4 }), true)
        let tail = c2.poll(timeout: Duration.millisecond * 200)
        @Expect(tail.records.size, 1)
        @Expect(tail.records[0].offset, 3)
        @Expect(tail.records[0].payload, "3|c")
        kit.shutdown()
    }

    @TestCase
    public func edgeRejectsTamperedExpiredAndForeignTokens(): Unit {
        let kit = ActorTestKit("edge-token")
        let store = MemoryDurableStateStore()
        let actor = EdgeStatefulActor()
        let ref = kit.actorOf({ => actor }, "edge-token-actor")
        let resolver = LocalEdgeResolver()
        resolver.bind("s1", ref)
        resolver.bind("s2", ref)
        let router = EdgeRouter(kit.system, store, resolver, "edge-secret")

        // 过期：签发时就已过期
        let expired = EdgeRecoveryToken.issue("edge-secret", "s1", 1, ttlMillis: -1, nowMillis: 1000)
        match (router.connect("s1", resumeToken: Some(expired))) {
            case Unauthorized(status, _) => @Expect(status, 401)
            case Connected(_) => @Fail("过期令牌不应建立连接")
        }
        // 会话不匹配：s2 的令牌不能续读 s1
        let otherToken = router.issueRecoveryToken("s2", 1)
        match (router.connect("s1", resumeToken: Some(otherToken))) {
            case Unauthorized(status, _) => @Expect(status, 403)
            case Connected(_) => @Fail("跨会话令牌不应建立连接")
        }
        // 篡改 lastAck
        let good = router.issueRecoveryToken("s1", 1)
        let parts = good.split("|")
        let tampered = "${parts[0]}|${parts[1]}|2|${parts[3]}|${parts[4]}"
        match (router.connect("s1", resumeToken: Some(tampered))) {
            case Unauthorized(status, _) => @Expect(status, 403)
            case Connected(_) => @Fail("签名不符的令牌不应建立连接")
        }
        // 正确令牌可用
        match (router.connect("s1", resumeToken: Some(good))) {
            case Connected(c) => @Expect(c.lastAckedOffset(), 1)
            case Unauthorized(_, reason) => @Fail("合法令牌被拒绝: ${reason}")
        }
        kit.shutdown()
    }

    @TestCase
    public func edgeGatewayMapsHandshakeAndFrames(): Unit {
        let kit = ActorTestKit("edge-gateway")
        let store = MemoryDurableStateStore()
        let resolver = LocalEdgeResolver()
        let router = EdgeRouter(kit.system, store, resolver, "edge-secret")
        let gateway = EdgeGateway(router)

        match (gateway.upgrade(EdgeRequest("GET", "/v1/edge/room-9"))) {
            case Upgraded(up) =>
                @Expect(up.headers[HEADER_SESSION_ID], "room-9")
                @Expect(up.headers[HEADER_LAST_ACK], "0")
                @Expect(up.headers[HEADER_EDGE_NEXT_OFFSET], "1")
                @Expect(up.headers.contains(HEADER_RESUME_TOKEN), true)
            case Rejected(status, reason) => @Fail("握手被拒绝(${status}): ${reason}")
        }
        // 非 GET / 缺会话 id / 过期令牌的状态码
        match (gateway.upgrade(EdgeRequest("POST", "/v1/edge/room-9"))) {
            case Rejected(status, _) => @Expect(status, 405)
            case Upgraded(_) => @Fail("POST 不应升级")
        }
        match (gateway.upgrade(EdgeRequest("GET", "/other/room-9"))) {
            case Rejected(status, _) => @Expect(status, 404)
            case Upgraded(_) => @Fail("未知路径不应升级")
        }
        let h = HashMap<String, String>()
        h[HEADER_RESUME_TOKEN] = EdgeRecoveryToken.issue("edge-secret", "room-9", 1, ttlMillis: -1, nowMillis: 1)
        match (gateway.upgrade(EdgeRequest("GET", "/v1/edge/room-9", headers: h))) {
            case Rejected(status, _) => @Expect(status, 401)
            case Upgraded(_) => @Fail("过期令牌不应升级")
        }

        // SSE：id 是 offset，多行 payload 拆成多个 data 字段
        let records = [StreamRecord(1, "edge", "line1\nline2")]
        let sse = EdgeSse.encode(records)
        @Expect(sse.contains("id: 1\n"), true)
        @Expect(sse.contains("data: line1\n"), true)
        @Expect(sse.contains("data: line2\n"), true)
        // WebSocket：offset 与载荷可往返
        let frame = EdgeWs.encodeOne(StreamRecord(7, "edge", "hello\nworld"))
        match (EdgeWs.decode(frame)) {
            case Some(f) =>
                @Expect(f.offset, 7)
                @Expect(f.payload, "hello\nworld")
            case None => @Fail("WebSocket 帧应可解码")
        }
        kit.shutdown()
    }
}
