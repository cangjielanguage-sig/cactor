/*
 * CActor Ex 51 - 持久发布订阅（差距 4.8 剩余部分：DistributedPubSub 自动接入持久日志）
 *
 * 真实约束：
 * - 在线扇出至多一次：订阅者掉线期间发布的事件直接丢失；
 * - 业务希望"事件流 + 补齐"一步到位：发布时自动落日志，重连时按游标回放；
 * - 持久日志必须跨进程/跨系统存在（存储提供者负责一致性），而在线扇出仍走集群。
 *
 * 组件：
 * - 框架：`DistributedPubSub.withDurableLog(store)` / `replayDurable(...)` / `durableHighestSeq`
 *   + `DurableTopic.replay`（本轮新增）
 * - Lou：POST /publish、GET /replay、GET /state、GET /healthz
 *
 * 自检（退出码 0/1）：
 * 1. 2 节点收敛；
 * 2. 订阅者离线期间发布 3 条 → 另一节点的持久日志序号为 3；
 * 3. 在 node1 回放 → 收到 3 条且顺序正确；再次回放不重复；
 * 4. 订阅后在线发布 → 跨节点实时到达，序号继续增长；
 * 5. Lou /publish、/replay、/state 与内存一致。
 *
 * 说明：持久日志只承载文本消息（`StringMessage`）；其他类型仍只做在线扇出。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex51_durable_pubsub
 *   ./target/release/bin/cactor_examples.ex51_durable_pubsub serve 18196
 */
package cactor_examples.ex51_durable_pubsub

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
import cactor_persistence.{DurableStateStore, MemoryDurableStateStore}

let WAIT = Duration.second * 20
let TOPIC = "events"

/** 订阅者收到的事件（线程安全，供 HTTP 与自检共用） */
class Feed {
    private let lock = Mutex()
    private let lines = ArrayList<String>()

    func add(line: String): Unit {
        synchronized(lock) {
            lines.add(line)
        }
    }

    func size(): Int64 {
        synchronized(lock) {
            Int64(lines.size)
        }
    }

    func snapshot(): Array<String> {
        synchronized(lock) {
            lines.toArray()
        }
    }

    func json(): String {
        let sb = StringBuilder()
        sb.append("[")
        var first = true
        for (line in snapshot()) {
            if (!first) {
                sb.append(",")
            }
            first = false
            sb.append("\"${line}\"")
        }
        sb.append("]")
        sb.toString()
    }
}

class FeedActor <: Actor {
    let feed: Feed

    init(feed: Feed) {
        this.feed = feed
    }

    public prop name: String { get() { "feed" } }
    public prop description: String { get() { "durable pubsub feed" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            feed.add(s.getContent())
        }
        MessageResult.Handled
    }
}

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let pubsub: DistributedPubSub
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, store: DurableStateStore) {
        this.system = CActorSystemFactory.create("ex51-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        let rep = Replicator(system, remote, Duration.millisecond * 50)
        this.pubsub = DistributedPubSub(cluster, system, remote, rep).withDurableLog(store)
    }

    func address(): String {
        remote.getLocalAddress().toString()
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            cluster.shutdown()
            remote.stop()
            system.terminate()
        }
    }
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

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

/** 回放并把事件收集成 JSON（HTTP 用；直接读持久日志，不经过订阅者 Actor） */
func replayJson(pubsub: DistributedPubSub, name: String, offsets: DurableStateStore, from: Int64): String {
    let events = ArrayList<String>()
    let last = pubsub.replayDurable(TOPIC, name, offsets, { _, payload => events.add(payload) }, fromSeq: from)
    let sb = StringBuilder()
    sb.append("{\"last\":${last},\"events\":[")
    var first = true
    for (e in events) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${e}\"")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(publisher: Node, reader: Node, offsets: DurableStateStore, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/publish", { ctx =>
        let message = ctx.query("message") ?? ""
        let _ = publisher.pubsub.publish(TOPIC, StringMessage(message))
        let seq = reader.pubsub.durableHighestSeq(TOPIC)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"published\":true,\"seq\":${seq}}")
    })
    app.get("/replay", { ctx =>
        let from = ctx.queryInt("from") ?? 1
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(replayJson(reader.pubsub, "http", offsets, from))
    })
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"seq\":${reader.pubsub.durableHighestSeq(TOPIC)}," +
            "\"subscriber_nodes\":${publisher.pubsub.subscriberNodes(TOPIC).size}}")
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

main(args: Array<String>): Int64 {
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.millisecond * 300, Duration.second * 2).withSeedNodeTimeout(Duration.second * 2)
    // 两个节点共用同一持久存储（真实部署由共享数据库/目录提供一致性）
    let store = MemoryDurableStateStore()
    let nodes = ArrayList<Node>()
    nodes.add(Node(0, None<NetworkAddress>, settings, store))
    nodes.add(Node(1, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings, store))

    let feed = Feed()
    let offsets = MemoryDurableStateStore()
    let sub = nodes[1].system.actorOf({ => FeedActor(feed) }, "feed")

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let _ = waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 2 })
        let server = buildApp(nodes[0], nodes[1], offsets, port)
        let actual = startServer(server)
        if (actual == 0u16) {
            println("ex51：端口绑定失败")
            return 1
        }
        println("ex51 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    try {
        println("\n[1] 集群收敛")
        c.check(waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 2 }), "2 节点 Up")

        println("\n[2] 订阅者离线：只进持久日志")
        let _ = nodes[0].pubsub.publish(TOPIC, StringMessage("a"))
        let _ = nodes[0].pubsub.publish(TOPIC, StringMessage("b"))
        let _ = nodes[0].pubsub.publish(TOPIC, StringMessage("c"))
        c.check(waitUntil(WAIT, { => nodes[1].pubsub.durableHighestSeq(TOPIC) == 3 }), "node1 读到序号 3")
        c.check(feed.size() == 0, "离线期间订阅者没收到消息")

        println("\n[3] 回放补齐")
        let last = nodes[1].pubsub.replayDurable(TOPIC, "svc", offsets,
            { _, payload => sub.tell(StringMessage(payload)) })
        c.check(last == 3, "回放最后序号 3")
        c.check(waitUntil(WAIT, { => feed.size() == 3 }), "订阅者收到 3 条")
        c.check(feed.snapshot().toString().contains("a") && feed.snapshot()[1] == "b", "顺序正确：${feed.json()}")
        let _ = nodes[1].pubsub.replayDurable(TOPIC, "svc", offsets,
            { _, payload => sub.tell(StringMessage(payload)) })
        c.check(!waitUntil(Duration.millisecond * 300, { => feed.size() > 3 }), "游标生效：再次回放不重复")

        println("\n[4] 在线扇出 + 继续落日志")
        nodes[1].pubsub.subscribe(TOPIC, sub)
        c.check(waitUntil(WAIT, { => nodes[0].pubsub.subscriberNodes(TOPIC).size == 1 }), "订阅信息同步到发布节点")
        let _ = nodes[0].pubsub.publish(TOPIC, StringMessage("d"))
        c.check(waitUntil(WAIT, { => feed.size() == 4 }), "跨节点实时到达")
        c.check(nodes[1].pubsub.durableHighestSeq(TOPIC) == 4, "实时消息同样入日志")

        println("\n[5] Lou 端点")
        let server = buildApp(nodes[0], nodes[1], MemoryDurableStateStore(), 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let published = readBody(client.post("http://127.0.0.1:${port}/publish?message=e", ""))
            c.check(published.contains("\"seq\":5"), "POST /publish：${published}")
            let replay = readBody(client.get("http://127.0.0.1:${port}/replay?from=1"))
            c.check(replay.contains("\"last\":5") && replay.contains("\"c\"") && replay.contains("\"e\""),
                "GET /replay：${replay}")
            let state = readBody(client.get("http://127.0.0.1:${port}/state"))
            c.check(state.contains("\"seq\":5"), "GET /state：${state}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }
    } finally {
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex51：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
