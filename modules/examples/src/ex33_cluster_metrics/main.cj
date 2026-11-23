/*
 * CActor Ex 33 - 集群指标导出（ClusterMetrics，差距 4.10 的指标部分）
 *
 * 真实约束：
 * - 集群容量与健康必须能被监控系统抓取：成员数、不可达数、收敛状态、分片/实体分布、
 *   gossip 量、隔离表大小、可靠投递未确认量，而不是只能翻日志；
 * - 指标不能靠业务代码打点：应从现有原子计数与成员快照读，抓取频率由 Prometheus 决定；
 * - 唯一指标源：同一份值既要能渲染 Prometheus 文本，也要能被程序化读取。
 *
 * 组件：
 * - 框架：`ClusterMetrics`（Cluster + SimpleRemoteManager，可选挂接 ClusterSharding / Replicator）
 * - 拓扑：3 节点集群，每个节点一个 8 分片的区域
 * - Lou：GET /metrics（节点 0）、GET /metrics/all（三节点聚合）、POST /work、POST /down
 *
 * 自检（退出码 0/1）：
 * 1. 3 节点收敛，/metrics 含 cactor_cluster_members_up 3、converged 1；
 * 2. 派发 12 个实体后，三节点 shards_owned 合计 > 0、entities_local 合计 == 12；
 * 3. 指标与 MetricRegistry 程序化读取一致；
 * 4. down 节点 2 后 members_up 降为 2；
 * 5. Lou 端点在 serve 模式下可 curl。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex33_cluster_metrics
 *   ./target/release/bin/cactor_examples.ex33_cluster_metrics serve 18118
 */
package cactor_examples.ex33_cluster_metrics

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.monitoring.{GaugeMetric}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterMetrics, ClusterSettings, ClusterSharding, RebalancePolicy,
    ShardingSettings}

let WAIT = Duration.second * 25

class Entity <: Actor {
    let id: String

    init(id: String) {
        this.id = id
    }

    public prop name: String { get() { "metrics-entity" } }
    public prop description: String { get() { "cluster metrics entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let region: ClusterSharding
    let metrics: ClusterMetrics
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.index = index
        this.system = CActorSystemFactory.create("ex33-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        // 关闭按负载自动再平衡：本示例统计"已创建实体数"，迁移会在新宿主的首条消息前不重建实体
        this.region = ClusterSharding(cluster, system, remote, "metrics-entity", 8,
            { id => Entity(id) }, ShardingSettings.defaults().withRebalance(RebalancePolicy.disabled()))
        this.metrics = ClusterMetrics(cluster, remote).attachSharding(region)
    }

    func address(): String {
        remote.getLocalAddress().toString()
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            region.stop()
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

/** 从 Prometheus 文本里取一条无标签指标的值（找不到返回 -1） */
func metricValue(text: String, name: String): Int64 {
    for (line in text.split("\n")) {
        if (line.startsWith("${name} ")) {
            match (Int64.tryParse(line[(name.size + 1)..].trimAscii())) {
                case Some(v) => return v
                case None => return -1
            }
        }
    }
    -1
}

func textOf(node: Node): String {
    node.metrics.renderPrometheus()
}

func allMetricsText(nodes: ArrayList<Node>): String {
    let sb = StringBuilder()
    for (n in nodes) {
        sb.append("# node ${n.index} ${n.address()}\n")
        sb.append(textOf(n))
    }
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
        let text = textOf(n)
        sb.append("{\"index\":${n.index},\"up\":${metricValue(text, "cactor_cluster_members_up")}," +
            "\"shards\":${metricValue(text, "cactor_cluster_shards_owned")}," +
            "\"entities\":${metricValue(text, "cactor_cluster_entities_local")}}")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(nodes: ArrayList<Node>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/metrics", { ctx =>
        ctx.setSingleHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        ctx.writeString(textOf(nodes[0]))
    })
    app.get("/metrics/all", { ctx =>
        ctx.setSingleHeader("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        ctx.writeString(allMetricsText(nodes))
    })
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(stateJson(nodes))
    })
    app.post("/work", { ctx =>
        let n = ctx.queryInt("n") ?? 6
        for (i in 0..n) {
            nodes[0].region.tell("entity-${i}", StringMessage("work-${i}"))
        }
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"dispatched\":${n}}")
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

    let nodes = ArrayList<Node>()
    var seed: Option<NetworkAddress> = None<NetworkAddress>
    for (i in 0..3) {
        let n = Node(i, seed, settings)
        if (i == 0) {
            seed = Some(n.remote.getLocalAddress())
        }
        nodes.add(n)
    }

    let app = buildApp(nodes, 0u16)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let real = buildApp(nodes, port)
        let actual = startServer(real)
        if (actual == 0u16) {
            println("ex33：端口绑定失败")
            return 1
        }
        println("ex33 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        println("\n[1] 3 节点收敛，指标反映成员与收敛状态")
        var formed = true
        for (n in nodes) {
            formed = formed && waitUntil(WAIT, { =>
                metricValue(textOf(n), "cactor_cluster_members_up") == 3 &&
                metricValue(textOf(n), "cactor_cluster_converged") == 1
            })
        }
        c.check(formed, "/metrics 各节点 members_up=3、converged=1")

        println("\n[2] 派发实体后分片/实体指标")
        for (i in 0..12) {
            nodes[0].region.tell("entity-${i}", StringMessage("work-${i}"))
        }
        c.check(waitUntil(WAIT, { =>
            var shards: Int64 = 0
            var entities: Int64 = 0
            for (n in nodes) {
                let t = textOf(n)
                shards += metricValue(t, "cactor_cluster_shards_owned")
                entities += metricValue(t, "cactor_cluster_entities_local")
            }
            shards > 0 && entities == 12
        }), "三节点 shards_owned 合计 >0，entities_local 合计 12")

        // 唯一指标源：程序化读取与文本一致
        let text0 = textOf(nodes[0])
        let registryView = nodes[0].metrics.registryValue()
        var registryOk = true
        match (registryView.getMetric("cactor_cluster_members_up")) {
            case Some(g: GaugeMetric) =>
                registryOk = Int64(g.getCurrentValue()) == metricValue(text0, "cactor_cluster_members_up")
            case _ => registryOk = false
        }
        c.check(registryOk, "MetricRegistry 与 Prometheus 文本一致")

        println("\n[3] down 节点 2 后 members_up 降为 2")
        nodes[0].cluster.down(nodes[2].address())
        c.check(waitUntil(WAIT, { => metricValue(textOf(nodes[0]), "cactor_cluster_members_up") == 2 }),
            "节点 0 视图 members_up=2")

        println("\n[4] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let m = readBody(client.get("http://127.0.0.1:${port}/metrics"))
        c.check(m.contains("# TYPE cactor_cluster_members_up gauge") &&
            m.contains("cactor_cluster_members_up 2") &&
            m.contains("cactor_cluster_shards_owned"), "GET /metrics 文本格式与数值正确")
        let all = readBody(client.get("http://127.0.0.1:${port}/metrics/all"))
        c.check(all.contains("# node 0") && all.contains("# node 1") && all.contains("# node 2"),
            "GET /metrics/all 含三节点")
        let st = readBody(client.get("http://127.0.0.1:${port}/state"))
        c.check(st.contains("\"up\":2") && st.contains("\"shards\":"), "GET /state 与指标一致")
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

    println("\nex33：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
