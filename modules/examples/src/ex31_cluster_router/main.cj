/*
 * CActor Ex 31 - 集群感知路由器（ClusterRouter，对标 Akka cluster-aware routers，差距 4.5）
 *
 * 真实约束：
 * - 跨节点分发任务时，不能把"选哪个节点"写死在应用里：成员会下线、负载会倾斜；
 * - 成员被 down / 移除后必须立刻停止向它投递，否则消息持续落进死信；
 * - 没有可用成员时应当明确失败（调用方能降级/重试），而不是悄悄丢消息。
 *
 * 组件：
 * - 框架：`ClusterRouter`（RoundRobin / LeastLoaded + 应用负载回调）
 * - 拓扑：3 节点集群，每个节点 expose 同名 "worker"
 * - Lou：GET /candidates、GET /loads、POST /load、POST /dispatch、POST /down、GET /stats
 *
 * 自检（退出码 0/1）：
 * 1. 3 节点收敛，候选 3 个；
 * 2. LeastLoaded：负载 (5,1,3) → 5 条全部到节点 1；负载反转后改选节点 2；
 * 3. RoundRobin：6 条 → 每节点 2 条；
 * 4. down 节点 2 后候选变 2，后续 20 条没有任何一条到节点 2；
 * 5. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex31_cluster_router
 *   ./target/release/bin/cactor_examples.ex31_cluster_router serve 18116
 */
package cactor_examples.ex31_cluster_router

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
import cactor_cluster.membership.{Cluster, ClusterSettings}
import cactor_cluster.routing.{ClusterRouter, ClusterRouterStrategy}

let WAIT = Duration.second * 20

class Stats {
    let lock = Mutex()
    let counts = HashMap<Int64, Int64>()

    func onMessage(node: Int64): Unit {
        synchronized(lock) {
            counts[node] = (counts.get(node) ?? 0) + 1
        }
    }

    func count(node: Int64): Int64 {
        synchronized(lock) {
            counts.get(node) ?? 0
        }
    }

    func total(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for ((_, c) in counts) {
                n += c
            }
            n
        }
    }
}

class Worker <: Actor {
    let index: Int64
    let stats: Stats

    init(index: Int64, stats: Stats) {
        this.index = index
        this.stats = stats
    }

    public prop name: String { get() { "router-worker" } }
    public prop description: String { get() { "router worker" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(_) <- (message as StringMessage)) {
            stats.onMessage(index)
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

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, stats: Stats) {
        this.index = index
        this.system = CActorSystemFactory.create("ex31-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        let worker = system.actorOf({ => Worker(index, stats) }, "worker")
        remote.exposeActor("worker", worker)
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

func candidatesJson(nodes: ArrayList<Node>, router: ClusterRouter): String {
    let sb = StringBuilder()
    sb.append("{\"candidates\":[")
    var first = true
    for (a in router.candidates()) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${a}\"")
    }
    sb.append("],\"nodes\":[")
    first = true
    for (n in nodes) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("{\"index\":${n.index},\"up\":${n.cluster.state().upCount()}}")
    }
    sb.append("]}")
    sb.toString()
}

func loadsJson(nodes: ArrayList<Node>, loads: HashMap<String, Int64>): String {
    let sb = StringBuilder()
    sb.append("{\"loads\":{")
    var first = true
    for (n in nodes) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${n.index}\":${loads.get(n.address()) ?? 0}")
    }
    sb.append("}}")
    sb.toString()
}

func statsJson(nodes: ArrayList<Node>, stats: Stats): String {
    let sb = StringBuilder()
    sb.append("{\"counts\":{")
    var first = true
    for (n in nodes) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${n.index}\":${stats.count(n.index)}")
    }
    sb.append("},\"total\":${stats.total()}}")
    sb.toString()
}

func buildApp(nodes: ArrayList<Node>, loads: HashMap<String, Int64>, least: ClusterRouter,
    roundRobin: ClusterRouter, stats: Stats, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/candidates", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(candidatesJson(nodes, least))
    })
    app.get("/loads", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(loadsJson(nodes, loads))
    })
    app.post("/load", { ctx =>
        let idx = ctx.queryInt("node") ?? 0
        let n = ctx.queryInt("n") ?? 0
        ctx.setSingleHeader("Content-Type", "application/json")
        if (idx < 0 || idx >= Int64(nodes.size)) {
            ctx.writeStringWithCode(400u16, "{\"error\":\"bad_node\"}")
        } else {
            loads[nodes[idx].address()] = n
            ctx.writeString("{\"node\":${idx},\"load\":${n}}")
        }
    })
    app.post("/dispatch", { ctx =>
        let n = ctx.queryInt("n") ?? 5
        let strategy = ctx.query("strategy") ?? "least"
        let router = if (strategy == "rr") { roundRobin } else { least }
        ctx.setSingleHeader("Content-Type", "application/json")
        try {
            for (i in 0..n) {
                router.tell(StringMessage("job-${i}"))
            }
            ctx.writeString("{\"dispatched\":${n},\"strategy\":\"${strategy}\",\"candidates\":${router.candidates().size}}")
        } catch (e: IllegalStateException) {
            ctx.writeStringWithCode(503u16, "{\"error\":\"no_candidates\"}")
        }
    })
    app.post("/down", { ctx =>
        let idx = ctx.queryInt("node") ?? 0
        ctx.setSingleHeader("Content-Type", "application/json")
        if (idx <= 0 || idx >= Int64(nodes.size)) {
            ctx.writeStringWithCode(400u16, "{\"error\":\"bad_node\"}")
        } else {
            nodes[0].cluster.down(nodes[idx].address())
            ctx.writeString("{\"downed\":${idx}}")
        }
    })
    app.get("/stats", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(statsJson(nodes, stats))
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
    let stats = Stats()
    let loads = HashMap<String, Int64>()

    let nodes = ArrayList<Node>()
    var seed: Option<NetworkAddress> = None<NetworkAddress>
    for (i in 0..3) {
        let n = Node(i, seed, settings, stats)
        if (i == 0) {
            seed = Some(n.remote.getLocalAddress())
        }
        nodes.add(n)
    }

    let least = ClusterRouter(nodes[0].cluster, nodes[0].remote, "worker",
        strategy: ClusterRouterStrategy.LeastLoaded, loadOf: { a: String => loads.get(a) ?? 0 })
    let roundRobin = ClusterRouter(nodes[0].cluster, nodes[0].remote, "worker",
        strategy: ClusterRouterStrategy.RoundRobin)

    let app = buildApp(nodes, loads, least, roundRobin, stats, 0u16)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let real = buildApp(nodes, loads, least, roundRobin, stats, port)
        let actual = startServer(real)
        if (actual == 0u16) {
            println("ex31：端口绑定失败")
            return 1
        }
        println("ex31 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        println("\n[1] 3 节点收敛")
        var formed = true
        for (n in nodes) {
            formed = formed && waitUntil(WAIT, { =>
                n.cluster.state().upCount() == 3 && n.cluster.state().converged
            })
        }
        c.check(formed, "3 节点全部 Up，候选 ${least.candidates().size} 个")

        println("\n[2] LeastLoaded：按负载选目标")
        loads[nodes[0].address()] = 5
        loads[nodes[1].address()] = 1
        loads[nodes[2].address()] = 3
        c.check(least.route() == nodes[1].address(), "负载最低的节点 1 被选中")
        for (i in 0..5) {
            least.tell(StringMessage("job-${i}"))
        }
        c.check(waitUntil(WAIT, { => stats.count(1) == 5 }), "5 条全部到节点 1")
        c.check(stats.count(0) == 0 && stats.count(2) == 0, "节点 0/2 未收到")
        loads[nodes[1].address()] = 100
        loads[nodes[2].address()] = 0
        c.check(least.route() == nodes[2].address(), "负载反转后改选节点 2")

        println("\n[3] RoundRobin：均匀分发")
        for (i in 0..6) {
            roundRobin.tell(StringMessage("rr-${i}"))
        }
        c.check(waitUntil(WAIT, { => stats.count(0) == 2 && stats.count(2) == 2 }), "节点 0/2 各收到 2 条")
        c.check(waitUntil(WAIT, { => stats.count(1) == 7 }), "节点 1 累计 5+2 条")

        println("\n[4] down 节点 2 后自动剔除")
        nodes[0].cluster.down(nodes[2].address())
        c.check(waitUntil(WAIT, { =>
            let cs = roundRobin.candidates()
            !cs.contains(nodes[2].address()) && cs.size == 2
        }), "候选降为 2，节点 2 被剔除")
        let before2 = stats.count(2)
        for (i in 0..20) {
            roundRobin.tell(StringMessage("after-${i}"))
        }
        c.check(waitUntil(WAIT, { => stats.count(0) + stats.count(1) >= 20 + 7 + 2 }),
            "20 条由剩余节点接收")
        c.check(stats.count(2) == before2, "节点 2 未再收到消息")

        println("\n[5] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let cand = readBody(client.get("http://127.0.0.1:${port}/candidates"))
        c.check(cand.contains("\"candidates\":") && !cand.contains(nodes[2].address()),
            "GET /candidates 不含已 down 的节点")
        let loadsBody = readBody(client.get("http://127.0.0.1:${port}/loads"))
        c.check(loadsBody.contains("\"1\":100"), "GET /loads 反映负载")
        let disp = readBody(client.post("http://127.0.0.1:${port}/dispatch?n=4&strategy=rr", ""))
        c.check(disp.contains("\"dispatched\":4") && disp.contains("\"candidates\":2"),
            "POST /dispatch 只发给 2 个候选：${disp}")
        let bad = client.post("http://127.0.0.1:${port}/down?node=0", "")
        c.check(bad.status == 400, "拒绝 down 承载 HTTP 的节点 0")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex31：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
