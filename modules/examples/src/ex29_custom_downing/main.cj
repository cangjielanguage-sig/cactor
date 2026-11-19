/*
 * CActor Ex 29 - 自定义脑裂决策（DowningProvider，对标 Akka DowningProvider）
 *
 * 真实约束：
 * - "多数派存活"不是所有业务的正确规则：金融/工业场景常见"某个业务区必须始终可用"，
 *   宁可让多数派停下，也不能让指定区域整体消失后继续运行；
 * - 决策必须可插拔，且保持纯函数：分区两侧各自在本地视图上计算，规则相同才能保证最多一侧存活；
 * - 规则不能削弱安全边界：本侧不满足条件时必须自行停止，而不是只把对方 down 掉。
 *
 * 组件：
 * - 框架：`DowningProvider` 接口 + `DowningStrategy.Custom(provider)` + `ClusterSettings.withDowningProvider`
 * - 拓扑：5 节点、东区 2 个（少数派）/西区 3 个（多数派）；黑洞传输做真实双向断网
 * - 规则：东区至少有 1 个可达成员才存活（东区不可达的一侧停止，哪怕它是多数派）
 * - Lou：GET /state、GET /zones、POST /partition
 *
 * 自检（退出码 0/1）：
 * 1. 5 节点收敛；
 * 2. 分区后：西区 3 个全部 DownedBySbr（多数派被判不存活）；
 * 3. 东区 2 个存活且视图 upCount == 2（少数派被保留）；
 * 4. Lou /state /zones 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex29_custom_downing
 *   ./target/release/bin/cactor_examples.ex29_custom_downing serve 18114
 */
package cactor_examples.ex29_custom_downing

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_foundation.network.{BlackholeTransport, NetworkAddress, TcpTransport}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterSettings, DowningProvider, DowningStrategy, Member, SbrDecision}

let CLUSTER_WAIT = Duration.second * 25

/**
 * 业务规则：指定区域（requiredZone）必须至少有一个可达成员，否则本侧停止。
 * 纯函数：只读 members/unreachable/indirect 与构造时给定的 zone 映射。
 */
class ZoneQuorumProvider <: DowningProvider {
    private let zones: HashMap<String, String>
    private let requiredZone: String

    init(zones: HashMap<String, String>, requiredZone: String) {
        this.zones = zones
        this.requiredZone = requiredZone
    }

    public func stableAfter(): Option<Duration> {
        Some(Duration.millisecond * 500)
    }

    public func decide(members: Array<Member>, unreachable: HashSet<String>,
        indirect: HashSet<String>): SbrDecision {
        let downList = ArrayList<String>()
        var requiredReachable = false
        for (m in members) {
            if (unreachable.contains(m.address) || indirect.contains(m.address)) {
                downList.add(m.address)
            } else if ((zones.get(m.address) ?? "") == requiredZone) {
                requiredReachable = true
            }
        }
        if (downList.isEmpty()) {
            return SbrDecision.NoAction
        }
        if (!requiredReachable) {
            return SbrDecision.DownSelf
        }
        SbrDecision.DownUnreachable(downList.toArray())
    }
}

class Node {
    let index: Int64
    let zone: String
    let transport: BlackholeTransport
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    var stopped = false

    init(index: Int64, zone: String, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.index = index
        this.zone = zone
        this.system = CActorSystemFactory.create("ex29-node-${index}")
        this.transport = BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", 0)))
        this.remote = SimpleRemoteManager(transport)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
    }

    func address(): String {
        remote.getLocalAddress().toString()
    }

    /** 单向断开：丢掉所有发往该地址的帧 */
    func blackhole(targets: Array<String>): Unit {
        for (t in targets) {
            transport.blackhole(t)
        }
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

func zonesJson(nodes: ArrayList<Node>): String {
    let sb = StringBuilder()
    sb.append("{\"zones\":{")
    var first = true
    for (n in nodes) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${n.address()}\":\"${n.zone}\"")
    }
    sb.append("},\"required\":\"east\"}")
    sb.toString()
}

func stateJson(nodes: ArrayList<Node>): String {
    let sb = StringBuilder()
    sb.append("{\"nodes\":[")
    var first = true
    for (n in nodes) {
        if (!first) {
            sb.append(",")
        }
        first = false
        let s = n.cluster.state()
        sb.append("{\"index\":${n.index},\"zone\":\"${n.zone}\",\"up\":${s.upCount()}," +
            "\"converged\":${s.converged},\"downed\":${n.cluster.isDownedBySbr()}," +
            "\"terminated\":${n.cluster.isTerminated()}}")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(nodes: ArrayList<Node>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(stateJson(nodes))
    })
    app.get("/zones", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(zonesJson(nodes))
    })
    app.post("/partition", { ctx =>
        let east = ArrayList<String>()
        let west = ArrayList<String>()
        for (n in nodes) {
            if (n.zone == "east") {
                east.add(n.address())
            } else {
                west.add(n.address())
            }
        }
        applyPartition(nodes)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"partitioned\":true,\"east\":${east.size},\"west\":${west.size}}")
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

func applyPartition(nodes: ArrayList<Node>): Unit {
    let east = ArrayList<String>()
    let west = ArrayList<String>()
    for (n in nodes) {
        if (n.zone == "east") {
            east.add(n.address())
        } else {
            west.add(n.address())
        }
    }
    for (n in nodes) {
        if (n.zone == "east") {
            n.blackhole(west.toArray())
        } else {
            n.blackhole(east.toArray())
        }
    }
}

main(args: Array<String>): Int64 {
    let zones = HashMap<String, String>()
    let provider = ZoneQuorumProvider(zones, "east")
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.millisecond * 300, DowningStrategy.Custom(provider)).withSeedNodeTimeout(Duration.second * 2)

    let nodes = ArrayList<Node>()
    var seed: Option<NetworkAddress> = None<NetworkAddress>
    for (i in 0..5) {
        let zone = if (i < 2) { "east" } else { "west" }
        let n = Node(i, zone, seed, settings)
        if (i == 0) {
            seed = Some(n.remote.getLocalAddress())
        }
        nodes.add(n)
        zones[n.address()] = zone
    }

    // 先建节点再建 app：Lou 端点在运行时读节点状态
    let app = buildApp(nodes, 0u16)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let real = buildApp(nodes, port)
        let actual = startServer(real)
        if (actual == 0u16) {
            println("ex29：端口绑定失败")
            return 1
        }
        println("ex29 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. 收敛 ----
        println("\n[1] 5 节点（东区 2 / 西区 3）收敛")
        var formed = true
        for (n in nodes) {
            formed = formed && waitUntil(CLUSTER_WAIT, { =>
                n.cluster.state().upCount() == 5 && n.cluster.state().converged
            })
        }
        c.check(formed, "5 节点全部 Up 且收敛")

        // ---- 2. 分区 ----
        println("\n[2] 双向断网（东区 ↔ 西区）")
        applyPartition(nodes)
        let east = ArrayList<Node>()
        let west = ArrayList<Node>()
        for (n in nodes) {
            if (n.zone == "east") {
                east.add(n)
            } else {
                west.add(n)
            }
        }
        var westDowned = true
        for (n in west) {
            westDowned = westDowned && waitUntil(CLUSTER_WAIT, { => n.cluster.isDownedBySbr() })
        }
        c.check(westDowned, "西区 3 个节点被自定义规则判为不存活（多数派也停止）")

        // ---- 3. 东区存活 ----
        println("\n[3] 东区（少数派）按规则存活")
        var eastAlive = true
        for (n in east) {
            eastAlive = eastAlive && waitUntil(CLUSTER_WAIT, { =>
                !n.cluster.isTerminated() && n.cluster.state().converged && n.cluster.state().upCount() == 2
            })
        }
        c.check(eastAlive, "东区 2 个节点收敛，upCount == 2")
        var noDownSelf = true
        for (n in east) {
            noDownSelf = noDownSelf && !n.cluster.isDownedBySbr()
        }
        c.check(noDownSelf, "东区未被判定 DownSelf")

        // ---- 4. Lou ----
        println("\n[4] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let st = readBody(client.get("http://127.0.0.1:${port}/state"))
        c.check(st.contains("\"zone\":\"east\"") && st.contains("\"downed\":true"),
            "GET /state 同时反映东区存活与西区 downed")
        let zj = readBody(client.get("http://127.0.0.1:${port}/zones"))
        c.check(zj.contains("\"required\":\"east\"") && zj.contains("\"east\""), "GET /zones 含区域映射")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex29：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
