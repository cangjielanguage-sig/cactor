/*
 * CActor Ex 56 - 跨数据中心就近路由（差距 4.3 的部分实现：dataCenter 属性 + DC 感知路由）
 *
 * 真实约束：
 * - 多机房部署时，接入层应优先把请求发给**同机房**服务，跨机房只做兜底；
 * - DC 归属要随成员视图传播，新节点加入后自动进入对应机房的候选；
 * - 没有同机房候选时应明确失败（运维改路由/扩容），而不是悄悄跨机房放大延迟。
 *
 * 范围说明（与 Akka Multi-DC 的差异）：本示例只做 dataCenter 属性的传播与路由过滤；
 * **跨 DC 成员仍在同一 gossip 圈**（C2a 的"DC 独立 gossip + DC 目录"尚未实现），
 * 因此两个 DC 之间仍会互相探测心跳；生产上跨机房部署还需补这一层。
 *
 * 组件：`Cluster(..., dataCenter: "dc1")`、`ClusterSnapshot.dataCenters/membersInDataCenter`、
 *       `ClusterRouter.withDataCenter`（本轮新增）、Lou
 *
 * 自检（退出码 0/1）：
 * 1. 4 节点（dc1×2、dc2×2）收敛，全部视图看到 2 个 DC，各 2 成员；
 * 2. 不加过滤时候选 4 个；withDataCenter("dc1") 候选 2 个且不含 dc2 地址；
 * 3. 6 个任务全部由 dc1 处理，dc2 0 次；
 * 4. Lou /members、/route 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex56_data_center
 *   ./target/release/bin/cactor_examples.ex56_data_center serve 18201
 */
package cactor_examples.ex56_data_center

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
import cactor_cluster.membership.{Cluster, ClusterSettings, MemberStatus}
import cactor_cluster.routing.{ClusterRouter, ClusterRouterStrategy}

let WAIT = Duration.second * 25

class DcTally {
    private let lock = Mutex()
    private let hits = HashMap<String, Int64>()

    func hit(node: String): Unit {
        synchronized(lock) {
            hits[node] = (hits.get(node) ?? 0) + 1
        }
    }

    func total(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for ((_, v) in hits) {
                n += v
            }
            n
        }
    }

    func at(node: String): Int64 {
        synchronized(lock) {
            hits.get(node) ?? 0
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{")
            var first = true
            for ((k, v) in hits) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${k}\":${v}")
            }
            sb.append("}")
            sb.toString()
        }
    }
}

class DcWorker <: Actor {
    let node: String

    init(node: String) {
        this.node = node
    }

    public prop name: String { get() { "dc-worker" } }
    public prop description: String { get() { "dc worker" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            context.reply(StringMessage("${node}|${s.getContent()}"))
        }
        MessageResult.Handled
    }
}

class DcCollector <: Actor {
    let tally: DcTally

    init(tally: DcTally) {
        this.tally = tally
    }

    public prop name: String { get() { "dc-collector" } }
    public prop description: String { get() { "dc collector" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let parts = s.getContent().split("|")
            if (parts.size >= 2) {
                tally.hit(parts[0])
            }
        }
        MessageResult.Handled
    }
}

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, dc: String) {
        this.system = CActorSystemFactory.create("ex56-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings, dataCenter: dc)
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

func membersJson(cluster: Cluster): String {
    let sb = StringBuilder()
    sb.append("[")
    var first = true
    for (m in cluster.state().members) {
        if (m.status != MemberStatus.Up) {
            continue
        }
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("{\"address\":\"${m.address}\",\"dc\":\"${m.dataCenter}\"}")
    }
    sb.append("]")
    sb.toString()
}

func buildApp(router: ClusterRouter, collector: ActorRef, tally: DcTally, cluster: Cluster, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/route", { ctx =>
        let jobs = ctx.queryInt("jobs") ?? 6
        let before = tally.total()
        for (i in 0..jobs) {
            router.tell(StringMessage("job-${i}"), collector)
        }
        let ok = waitUntil(WAIT, { => tally.total() >= before + jobs })
        ctx.setSingleHeader("Content-Type", "application/json")
        if (ok) {
            ctx.writeString("{\"done\":${tally.total()},\"by_node\":${tally.json()}}")
        } else {
            ctx.writeString("{\"error\":\"timeout\",\"done\":${tally.total()}}")
        }
    })
    app.get("/members", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(membersJson(cluster))
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

func clusterSettings(): ClusterSettings {
    ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.millisecond * 300, Duration.second * 2).withSeedNodeTimeout(Duration.second * 2)
}

main(args: Array<String>): Int64 {
    let settings = clusterSettings()
    let nodes = ArrayList<Node>()
    nodes.add(Node(0, None<NetworkAddress>, settings, "dc1"))
    nodes.add(Node(1, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings, "dc1"))
    nodes.add(Node(2, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings, "dc2"))
    nodes.add(Node(3, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings, "dc2"))

    for (i in 0..nodes.size) {
        let addr = nodes[i].address()
        let ref = nodes[i].system.actorOf({ => DcWorker(addr) }, "dc-worker")
        nodes[i].remote.exposeActor("worker", ref)
    }
    let tally = DcTally()
    let collector = nodes[0].system.actorOf({ => DcCollector(tally) }, "dc-collector")
    let router = ClusterRouter(nodes[0].cluster, nodes[0].remote, "worker",
        strategy: ClusterRouterStrategy.RoundRobin).withDataCenter("dc1")

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let _ = waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 4 })
        let server = buildApp(router, collector, tally, nodes[0].cluster, port)
        let actual = startServer(server)
        if (actual == 0u16) {
            println("ex56：端口绑定失败")
            return 1
        }
        println("ex56 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    try {
        println("\n[1] 数据中心传播")
        c.check(waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 4 }), "4 节点 Up")
        c.check(waitUntil(WAIT, { => nodes[0].cluster.state().dataCenters().size == 2 }), "两个 DC")
        c.check(nodes[0].cluster.state().membersInDataCenter("dc1").size == 2, "dc1 有 2 个成员")
        c.check(nodes[0].cluster.state().membersInDataCenter("dc2").size == 2, "dc2 有 2 个成员")
        c.check(nodes[3].cluster.dataCenter() == "dc2", "节点自述 DC 正确")

        println("\n[2] DC 感知路由")
        let unfiltered = ClusterRouter(nodes[0].cluster, nodes[0].remote, "worker")
        c.check(unfiltered.candidates().size == 4, "不过滤时 4 个候选")
        let dcCandidates = router.candidates()
        c.check(dcCandidates.size == 2, "dc1 只有 2 个候选")
        c.check(!dcCandidates.contains(nodes[2].address()) && !dcCandidates.contains(nodes[3].address()),
            "dc2 地址被排除")
        for (i in 0..6) {
            router.tell(StringMessage("job-${i}"), collector)
        }
        c.check(waitUntil(WAIT, { => tally.total() == 6 }), "6 个任务完成")
        c.check(tally.at(nodes[2].address()) == 0 && tally.at(nodes[3].address()) == 0,
            "dc2 处理 0 个：${tally.json()}")
        c.check(tally.at(nodes[0].address()) > 0 && tally.at(nodes[1].address()) > 0,
            "dc1 两台都参与：${tally.json()}")
        unfiltered.stop()

        println("\n[3] Lou 端点")
        let server = buildApp(router, collector, tally, nodes[0].cluster, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let members = readBody(client.get("http://127.0.0.1:${port}/members"))
            c.check(members.contains("\"dc1\"") && members.contains("\"dc2\""), "GET /members：${members}")
            let body = readBody(client.post("http://127.0.0.1:${port}/route?jobs=4", ""))
            c.check(body.contains("\"done\":10"), "POST /route：${body}")
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

    println("\nex56：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
