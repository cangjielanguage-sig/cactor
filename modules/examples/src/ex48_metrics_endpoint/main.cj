/*
 * CActor Ex 48 - 统一指标端点（差距 4.10：运行时 + 集群 + 应用一次渲染）
 *
 * 真实约束：
 * - 运维只愿意抓一个 `/metrics`：运行时（Actor 数/死信）、集群（成员/收敛/隔离）、
 *   应用（请求/拒绝）必须在同一份文本里，且格式合法、段落不重复；
 * - 抓取不应打乱业务：所有指标都是读现有原子计数；
 * - 指标名要能分辨来源（cactor_actors_* / cactor_cluster_* / 应用前缀）。
 *
 * 组件：
 * - `MetricsExporter`（cluster/monitoring，本次新增）：统一渲染入口
 * - `ClusterMetrics`（ex33）作为集群来源；应用来源由示例提供
 * - Lou：GET /metrics、POST /work、GET /healthz
 *
 * 自检（退出码 0/1）：
 * 1. 2 节点收敛后，统一文本同时含运行时、集群与应用指标；
 * 2. 应用计数随请求增长；
 * 3. 段落之间不出现连续空行；
 * 4. Lou /metrics 与直接渲染一致（同一来源）。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex48_metrics_endpoint
 *   ./target/release/bin/cactor_examples.ex48_metrics_endpoint serve 18193
 */
package cactor_examples.ex48_metrics_endpoint

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterMetrics, ClusterSettings}
import cactor_cluster.monitoring.{MetricsExporter}

let WAIT = Duration.second * 25

/** 应用侧计数：HTTP /work 的调用次数 */
class AppMetrics {
    let requests = AtomicInt64(0)
    let rejected = AtomicInt64(0)

    func hit(): Unit {
        requests.fetchAdd(1)
    }

    func reject(): Unit {
        rejected.fetchAdd(1)
    }

    func render(): String {
        "# HELP cactor_app_requests_total Application requests\n" +
            "# TYPE cactor_app_requests_total counter\n" +
            "cactor_app_requests_total ${requests.load()}\n" +
            "# TYPE cactor_app_rejected_total counter\n" +
            "cactor_app_rejected_total ${rejected.load()}"
    }
}

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.system = CActorSystemFactory.create("ex48-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
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

func buildApp(exporter: MetricsExporter, app: AppMetrics, port: UInt16): Lou {
    let app0 = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app0.post("/work", { ctx =>
        app.hit()
        if ((ctx.query("fail") ?? "0") == "1") {
            app.reject()
        }
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"ok\":true,\"requests\":${app.requests.load()}}")
    })
    app0.get("/metrics", { ctx =>
        ctx.setSingleHeader("Content-Type", "text/plain; version=0.0.4")
        ctx.writeString(exporter.renderPrometheus())
    })
    app0.get("/healthz", { ctx => ctx.writeString("ok") })
    app0
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
    let app = AppMetrics()

    let nodes = ArrayList<Node>()
    nodes.add(Node(0, None<NetworkAddress>, settings))
    nodes.add(Node(1, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings))
    let exporter = MetricsExporter(nodes[0].system)
        .addCluster(ClusterMetrics(nodes[0].cluster, nodes[0].remote))
        .addProvider("app", { => app.render() })

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let _ = waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 2 })
        let server = buildApp(exporter, app, port)
        let actual = startServer(server)
        if (actual == 0u16) {
            println("ex48：端口绑定失败")
            return 1
        }
        println("ex48 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    try {
        println("\n[1] 集群收敛")
        c.check(waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 2 }), "2 节点 Up")

        println("\n[2] 统一渲染")
        app.hit()
        app.hit()
        app.reject()
        c.check(waitUntil(WAIT, { => exporter.renderPrometheus().contains("cactor_cluster_converged 1") }),
            "同一次渲染出现收敛标志")
        let text = exporter.renderPrometheus()
        c.check(text.contains("cactor_actors_live"), "含运行时指标")
        c.check(text.contains("cactor_cluster_members_up 2"), "含集群成员指标")
        c.check(text.contains("# metrics from app") && text.contains("cactor_app_requests_total 2"),
            "含应用指标")
        c.check(text.contains("cactor_app_rejected_total 1"), "含应用拒绝计数")
        c.check(!text.contains("\n\n\n"), "段落之间无多余空行")

        println("\n[3] Lou /metrics 与直接渲染同源")
        let server = buildApp(exporter, app, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let work = readBody(client.post("http://127.0.0.1:${port}/work?x=1", ""))
            c.check(work.contains("\"requests\":3"), "POST /work 计数：${work}")
            let metrics = readBody(client.get("http://127.0.0.1:${port}/metrics"))
            c.check(metrics.contains("cactor_actors_live") && metrics.contains("cactor_cluster_members_up 2") &&
                metrics.contains("cactor_app_requests_total 3"), "GET /metrics 三段齐全")
            c.check(metrics.contains("# TYPE cactor_cluster_members_up gauge"), "Prometheus TYPE 行存在")
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

    println("\nex48：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
