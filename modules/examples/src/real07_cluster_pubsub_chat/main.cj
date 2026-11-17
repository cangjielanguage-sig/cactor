/*
 * CActor Real 07 - 集群发布订阅聊天 + SSE（对标 protoactor-go examples/chat、
 *                     Akka akka-sample-distributed-pub-sub）
 *
 * 真实约束：
 * - 3 个真实 TCP 节点，用户连到任意节点都能收到同一房间的消息（跨节点广播）；
 * - 每个订阅者恰好收到一次，且同一发布者的消息保持顺序；
 * - 退订后不再收到；
 * - HTTP 侧是 SSE 长连接（Lou），聊天事件从 DistributedPubSub 流到浏览器。
 *
 * 组件：
 * - Cluster + Replicator + DistributedPubSub（每个节点一个）
 * - ChatClient（经典 Actor）：把主题消息收进本地记录 / SSE
 * - Lou：GET /chat/:room/events?user=&limit=（SSE）、POST /broadcast?room=&user=&text=
 *
 * 自检（退出码 0/1）：
 * 1. 节点 1、2 订阅同一主题，节点 0 发布 3 条 → 两边按序各收到 3 条（恰好一次）；
 * 2. 退订节点 2 后再发布 → 节点 2 不再增加，节点 1 继续收到；
 * 3. Lou SSE 客户端（带 limit=3）在收到 3 条后正常结束，内容与发布一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.real07_cluster_pubsub_chat
 *   ./target/release/bin/cactor_examples.real07_cluster_pubsub_chat serve 18091   # 供 curl -N
 */
package cactor_examples.real07_cluster_pubsub_chat

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings, DistributedPubSub}
import cactor_cluster.ddata.{Replicator}

let TOPIC = "chat-general"
let CLUSTER_WAIT = Duration.second * 20
let HTTP_TIMEOUT = Duration.second * 15

/** 收到消息后写入记录；count 用于断言"恰好一次" */
class ChatClient <: Actor {
    let user: String
    let rec = ArrayList<String>()
    let lock = Mutex()
    let onMessage: Option<(String) -> Unit>

    init(user: String) {
        this.user = user
        this.onMessage = None<(String) -> Unit>
    }

    init(user: String, onMessage: (String) -> Unit) {
        this.user = user
        this.onMessage = Some(onMessage)
    }

    public prop name: String { get() { "chat-${user}" } }
    public prop description: String { get() { "chat client ${user}" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let text = s.getContent()
            synchronized(lock) {
                rec.add(text)
            }
            if (let Some(f) <- onMessage) {
                f(text)
            }
        }
        MessageResult.Handled
    }

    func lines(): Array<String> {
        synchronized(lock) {
            rec.toArray()
        }
    }

    func count(): Int64 {
        synchronized(lock) {
            Int64(rec.size)
        }
    }
}

/** SSE 生产者与订阅者共享的计数（放在 class 里，避免闭包捕获可变的局部变量） */
class SseState {
    let gate: Gate
    let limit: Int64
    var seen: Int64 = 0

    init(gate: Gate, limit: Int64) {
        this.gate = gate
        this.limit = limit
    }
}

func eqLines(a: Array<String>, b: Array<String>): Bool {
    if (a.size != b.size) {
        return false
    }
    for (i in 0..a.size) {
        if (a[i] != b[i]) {
            return false
        }
    }
    true
}

/** 生产者协程与 SSE 客户端之间的闸门：收到 limit 条后放行 */
class Gate {
    let lock = Mutex()
    let cond: Condition
    var open = false

    init() {
        this.cond = synchronized(this.lock) {
            this.lock.condition()
        }
    }

    func release(): Unit {
        synchronized(lock) {
            open = true
            cond.notifyAll()
        }
    }

    func await(timeout: Duration): Unit {
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (!open) {
                let left = deadline - MonoTime.now()
                if (left <= Duration.Zero) {
                    return
                }
                cond.wait(timeout: left)
            }
        }
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let pubsub: DistributedPubSub

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.index = index
        this.system = CActorSystemFactory.create("real07-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        // 种子为 None 时用自己的地址（单节点自成一圈，与 tools/cluster_demo.sh 的播种语义一致）
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        let rep = Replicator(system, remote, Duration.millisecond * 50)
        this.pubsub = DistributedPubSub(cluster, system, remote, rep)
    }

    func shutdown(): Unit {
        cluster.shutdown()
        remote.stop()
        system.terminate()
    }
}

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

func sseText(text: String): String {
    // SSE 的 data 行不能直接包含换行；消息里只有 "user|text" 且这里再压平一次
    text.replace("\n", " ")
}

func buildApp(node: Node, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    // SSE：limit 条后主动结束，便于客户端与自检都能确定收尾
    app.get("/chat/:room/events", { ctx =>
        let room = ctx.param("room")
        let user = ctx.query("user") ?? "anonymous"
        let limit = ctx.queryInt("limit") ?? 1000
        ctx.setSingleHeader("Cache-Control", "no-cache")
        ctx.sse({ stream =>
            let gate = Gate()
            let state = SseState(gate, limit)
            let sink = { text: String =>
                stream.sendEvent("chat", sseText(text))
                state.seen++
                if (state.seen >= state.limit) {
                    gate.release()
                }
            }
            let ref = node.system.actorOf({ => ChatClient("sse-${user}", sink) },
                "sse-${user}-${MonoTime.now().hashCode()}")
            let ack = node.pubsub.subscribeAsync("chat-${room}", ref)
            ack.get(Duration.second * 10)
            gate.await(Duration.second * 20)
            node.pubsub.unsubscribe("chat-${room}", ref)
            stream.close()
        })
    })
    // POST 用查询参数而不是路径参数：Lou 1.2.0 的路由树把 `/chat/:room` 这类
    // "参数父路径 + 子路径" 组合在 POST 上判成 405，改成静态叶子路径最稳。
    app.post("/broadcast", { ctx =>
        let room = ctx.query("room") ?? "general"
        let user = ctx.query("user") ?? "anonymous"
        let text = ctx.query("text") ?? String.fromUtf8(ctx.bodyRaw()).trimAscii()
        if (text.isEmpty()) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(400u16, "{\"error\":\"empty_text\"}")
        } else {
            let nodes = node.pubsub.publish("chat-${room}", StringMessage("${user}|${text}"))
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString("{\"deliveredNodes\":${nodes}}")
        }
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
    app
}

func startServer(app: Lou): UInt16 {
    spawn { app.listen() }
    for (_ in 0..500) {
        if (let Some(s) <- app.getStdServer()) {
            if (s.port != 0u16) {
                return s.port
            }
        }
        sleep(Duration.millisecond * 20)
    }
    0u16
}

func readBody(response: HttpResponse): String {
    let out = ArrayList<UInt8>()
    let buf = Array<UInt8>(4096, repeat: 0)
    var n = response.body.read(buf)
    while (n > 0) {
        for (i in 0..n) {
            out.add(buf[i])
        }
        n = response.body.read(buf)
    }
    String.fromUtf8(out.toArray())
}

func sseDataLines(body: String): Array<String> {
    let out = ArrayList<String>()
    for (line in body.split("\n")) {
        let t = line.trimAscii()
        if (t.startsWith("data:")) {
            out.add(t[5..].trimAscii())
        }
    }
    out.toArray()
}

class Checker {
    var failures = 0

    func check(cond: Bool, what: String): Unit {
        if (cond) {
            println("  ✓ ${what}")
        } else {
            failures++
            println("  ✗ ${what}")
        }
    }
}

func startCluster(settings: ClusterSettings): Array<Node> {
    let first = Node(0, None<NetworkAddress>, settings)
    let seed = first.remote.getLocalAddress()
    let n1 = Node(1, Some(seed), settings)
    let n2 = Node(2, Some(seed), settings)
    [first, n1, n2]
}

main(args: Array<String>): Int64 {
    let settings = ClusterSettings(Duration.millisecond * 80, Duration.millisecond * 80, 8.0,
        Duration.second * 2, Some(Duration.second * 3))
    let nodes = startCluster(settings)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(nodes[0], port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("real07：端口绑定失败")
            return 1
        }
        println("real07 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let app = buildApp(nodes[0], 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        let formed = waitUntil(CLUSTER_WAIT, { =>
            var ok = true
            for (n in nodes) {
                ok = ok && n.cluster.state().upCount() == 3 && n.cluster.state().converged
            }
            ok
        })
        c.check(formed, "三节点集群形成并收敛")

        // ---- 1. 跨节点广播 ----
        println("\n[1] 节点 1/2 订阅，节点 0 发布 3 条")
        let alice = ChatClient("alice")
        let bob = ChatClient("bob")
        let aliceRef = nodes[1].system.actorOf({ => alice }, "alice")
        let bobRef = nodes[2].system.actorOf({ => bob }, "bob")
        nodes[1].pubsub.subscribeAsync(TOPIC, aliceRef).get(Duration.second * 10)
        nodes[2].pubsub.subscribeAsync(TOPIC, bobRef).get(Duration.second * 10)
        let propagated = waitUntil(CLUSTER_WAIT, { => nodes[0].pubsub.subscriberNodes(TOPIC).size == 2 })
        c.check(propagated, "订阅信息同步到发布者节点（2 个节点）")

        let texts = ["hello", "from", "node-zero"]
        for (t in texts) {
            nodes[0].pubsub.publish(TOPIC, StringMessage("carol|${t}"))
        }
        let bothGot = waitUntil(Duration.second * 10, { => alice.count() == 3 && bob.count() == 3 })
        c.check(bothGot, "两个订阅者各收到 3 条（alice=${alice.count()}, bob=${bob.count()}）")
        c.check(eqLines(alice.lines(), ["carol|hello", "carol|from", "carol|node-zero"]), "alice 按序且恰好一次")
        c.check(eqLines(bob.lines(), alice.lines()), "bob 与 alice 内容一致")

        // ---- 2. 退订 ----
        println("\n[2] 退订节点 2 后不再收到")
        nodes[2].pubsub.unsubscribe(TOPIC, bobRef)
        // 等退订在本地生效
        let unsub = waitUntil(Duration.second * 10, { => nodes[2].pubsub.localSubscriberCount(TOPIC) == 0 })
        c.check(unsub, "节点 2 本地订阅数归零")
        nodes[0].pubsub.publish(TOPIC, StringMessage("carol|after-unsub"))
        let aliceGot = waitUntil(Duration.second * 10, { => alice.count() == 4 })
        c.check(aliceGot, "节点 1 继续收到（4 条）")
        c.check(bob.count() == 3, "节点 2 保持 3 条（没收到新消息）")

        // ---- 3. SSE 长连接 ----
        println("\n[3] Lou SSE：HTTP 客户端收到 3 条后正常结束")
        let client = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).build()
        let sseUrl = "http://127.0.0.1:${port}/chat/sse-room/events?user=http&limit=3"
        let pending = spawn {
            readBody(client.get(sseUrl))
        }
        // 等 SSE 订阅在本节点登记，再发布
        let subscribed = waitUntil(Duration.second * 10, { => nodes[0].pubsub.localSubscriberCount("chat-sse-room") >= 1 })
        c.check(subscribed, "SSE 订阅已在节点 0 登记")
        for (t in ["s1", "s2", "s3"]) {
            nodes[0].pubsub.publish("chat-sse-room", StringMessage("http|${t}"))
        }
        let body = pending.get()
        client.close()
        let data = sseDataLines(body)
        c.check(data.size == 3, "SSE 收到 3 条（实际 ${data.size}）")
        if (data.size == 3) {
            c.check(data[0] == "http|s1" && data[1] == "http|s2" && data[2] == "http|s3", "SSE 内容与顺序正确")
        } else {
            c.check(false, "SSE 内容因条数不符跳过")
        }
    } finally {
        app.stop()
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nreal07：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
