/*
 * CActor Ex 36 - 集群客户端（ClusterClient，差距 4.3 / C2c：外部进程不加入集群访问服务）
 *
 * 真实约束：
 * - 网关/BFF/运维脚本往往不属于业务集群（不能也不该加入 gossip），但要按逻辑名调用集群内服务；
 * - 集群成员会变化：客户端不能缓存死地址，每次调用前向联系点取最新 Up 成员；
 * - 联系点全部不可用时必须明确失败，调用方才能降级，而不是把消息投进黑洞。
 *
 * 组件：
 * - 框架：`ClusterClient`（联系点查询 + 轮询投递），集群侧由 "cluster" 守护响应查询
 * - 拓扑：3 节点业务集群（每节点一个 "quotes" 服务）+ 1 个**未加入集群**的客户端节点
 * - Lou：GET /members、GET /state、POST /quote、POST /down
 *
 * 自检（退出码 0/1）：
 * 1. 3 节点收敛，客户端查到 3 个成员；
 * 2. 轮询投递 6 次：三个节点各收到 2 次；
 * 3. down 节点 2 后成员降为 2，再投递 4 次由剩余两节点各收到 2 次；
 * 4. 全部联系点不可达时 `members()` 明确失败；
 * 5. Lou /members /state 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex36_cluster_client
 *   ./target/release/bin/cactor_examples.ex36_cluster_client serve 18121
 */
package cactor_examples.ex36_cluster_client

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterClient, ClusterSettings}

let WAIT = Duration.second * 20

class Counts {
    private let lock = Mutex()
    private let perNode = HashMap<Int64, Int64>()

    func hit(index: Int64): Unit {
        synchronized(lock) {
            perNode[index] = (perNode.get(index) ?? 0) + 1
        }
    }

    func count(index: Int64): Int64 {
        synchronized(lock) {
            perNode.get(index) ?? 0
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{")
            var first = true
            for ((i, c) in perNode) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${i}\":${c}")
            }
            sb.append("}")
            sb.toString()
        }
    }
}

class QuoteService <: Actor {
    let index: Int64
    let counts: Counts

    init(index: Int64, counts: Counts) {
        this.index = index
        this.counts = counts
    }

    public prop name: String { get() { "quote-service" } }
    public prop description: String { get() { "quote service" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(_) <- (message as StringMessage)) {
            counts.hit(index)
        }
        MessageResult.Handled
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, counts: Counts) {
        this.index = index
        this.system = CActorSystemFactory.create("ex36-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        let service = system.actorOf({ => QuoteService(index, counts) }, "quotes")
        remote.exposeActor("quotes", service)
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

func membersJson(members: Array<String>): String {
    let sb = StringBuilder()
    sb.append("{\"members\":[")
    var first = true
    for (m in members) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${m}\"")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(nodes: ArrayList<Node>, client: ClusterClient, counts: Counts, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/members", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        try {
            ctx.writeString(membersJson(client.members()))
        } catch (e: IllegalStateException) {
            ctx.writeStringWithCode(503u16, "{\"error\":\"no_contact\"}")
        }
    })
    app.post("/quote", { ctx =>
        let n = ctx.queryInt("n") ?? 1
        let symbol = ctx.query("symbol") ?? "BTC"
        ctx.setSingleHeader("Content-Type", "application/json")
        try {
            for (i in 0..n) {
                client.tell("quotes", StringMessage("${symbol}-${i}"))
            }
            ctx.writeString("{\"sent\":${n},\"members\":${client.members().size}}")
        } catch (e: IllegalStateException) {
            ctx.writeStringWithCode(503u16, "{\"error\":\"no_contact\"}")
        }
    })
    app.get("/state", { ctx =>
        let sb = StringBuilder()
        sb.append("{\"nodes\":[")
        var first = true
        for (n in nodes) {
            if (!first) {
                sb.append(",")
            }
            first = false
            sb.append("{\"index\":${n.index},\"up\":${n.cluster.state().upCount()},\"received\":${counts.count(n.index)}}")
        }
        sb.append("],\"counts\":${counts.json()}}")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(sb.toString())
    })
    app.post("/down", { ctx =>
        let idx = ctx.queryInt("node") ?? 2
        ctx.setSingleHeader("Content-Type", "application/json")
        if (idx <= 0 || idx >= Int64(nodes.size)) {
            ctx.writeStringWithCode(400u16, "{\"error\":\"bad_node\"}")
        } else {
            nodes[0].cluster.down(nodes[idx].address())
            ctx.writeString("{\"downed\":${idx}}")
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

main(args: Array<String>): Int64 {
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.millisecond * 300, Duration.second * 2).withSeedNodeTimeout(Duration.second * 2)
    let counts = Counts()

    let nodes = ArrayList<Node>()
    var seed: Option<NetworkAddress> = None<NetworkAddress>
    for (i in 0..3) {
        let n = Node(i, seed, settings, counts)
        if (i == 0) {
            seed = Some(n.remote.getLocalAddress())
        }
        nodes.add(n)
    }

    // 客户端：独立的远程管理器，**不加入集群**
    let clientRemote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    clientRemote.start()
    let contacts = Array<NetworkAddress>(nodes.size, { i => nodes[i].remote.getLocalAddress() })
    let client = ClusterClient(clientRemote, contacts, timeout: Duration.second)
    let app = buildApp(nodes, client, counts, 0u16)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let real = buildApp(nodes, client, counts, port)
        let actual = startServer(real)
        if (actual == 0u16) {
            println("ex36：端口绑定失败")
            return 1
        }
        println("ex36 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        println("\n[1] 3 节点收敛，客户端查到全部成员")
        var formed = true
        for (n in nodes) {
            formed = formed && waitUntil(WAIT, { =>
                n.cluster.state().upCount() == 3 && n.cluster.state().converged
            })
        }
        c.check(formed, "3 节点全部 Up")
        c.check(waitUntil(WAIT, { => client.members().size == 3 }), "客户端查到 3 个成员")

        println("\n[2] 轮询投递：三节点各 2 次")
        for (i in 0..6) {
            client.tell("quotes", StringMessage("BTC-${i}"))
        }
        c.check(waitUntil(WAIT, { =>
            counts.count(0) == 2 && counts.count(1) == 2 && counts.count(2) == 2
        }), "轮询均匀：每节点 2 次")

        println("\n[3] down 节点 2 后成员降为 2，继续投递")
        nodes[0].cluster.down(nodes[2].address())
        c.check(waitUntil(WAIT, { =>
            let m = client.members()
            m.size == 2 && !m.contains(nodes[2].address())
        }), "成员降为 2，且不含已 down 的节点")
        for (i in 0..4) {
            client.tell("quotes", StringMessage("ETH-${i}"))
        }
        c.check(waitUntil(WAIT, { => counts.count(0) == 4 && counts.count(1) == 4 }),
            "剩余两节点各再收到 2 次")
        c.check(counts.count(2) == 2, "已 down 节点不再收到消息")

        println("\n[4] 联系点全部不可达时明确失败")
        let dead = ClusterClient(clientRemote, [NetworkAddress("127.0.0.1", 1)],
            timeout: Duration.millisecond * 300)
        var failed = false
        try {
            let _ = dead.members()
        } catch (_: IllegalStateException) {
            failed = true
        }
        c.check(failed, "无可用联系点时抛 IllegalStateException")

        println("\n[5] Lou 端点")
        let http = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let members = readBody(http.get("http://127.0.0.1:${port}/members"))
        c.check(members.contains("\"members\":[") && !members.contains(nodes[2].address()),
            "GET /members 不含已 down 节点")
        let resp = readBody(http.post("http://127.0.0.1:${port}/quote?n=2&symbol=SOL", ""))
        c.check(resp.contains("\"sent\":2"), "POST /quote 受理投递")
        c.check(waitUntil(WAIT, { => counts.count(0) + counts.count(1) == 10 }),
            "新投递由剩余两节点接收")
        let st = readBody(http.get("http://127.0.0.1:${port}/state"))
        c.check(st.contains("\"received\":") && st.contains("\"counts\":"), "GET /state 与计数一致")
        let bad = http.post("http://127.0.0.1:${port}/down?node=0", "")
        c.check(bad.status == 400, "拒绝 down 承载 HTTP 的节点 0")
        http.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        clientRemote.stop()
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex36：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
