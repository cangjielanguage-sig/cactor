/*
 * CActor Ex 26 - 集群优雅离开接入协调关闭（差距 4.1 的框架修复）
 *
 * 真实约束：
 * - 节点正常下线（滚动升级、缩容）时，其余节点应该看到 Leaving/Exiting，
 *   而不是把它当成"突然不可达"；否则会触发不必要的故障检测、脑裂判定与重新分配；
 * - 关闭流程必须有界：离开等不到就继续关闭，不能把进程卡死；
 * - 优雅关闭必须是显式开启的（默认行为不变）。
 *
 * 框架能力：`Cluster.installCoordinatedShutdown()`（本次新增）
 * - 在 `CoordinatedShutdown.PhaseClusterLeave` 注册任务：发送 leave 并等待本地离开 Up；
 * - 后续 `actor-system-terminate` 阶段才停止集群守护 Actor，其余成员因此收到
 *   MemberLeft / MemberExited。
 *
 * 组件：
 * - 3 节点 Cluster + installCoordinatedShutdown
 * - Lou：GET /state、GET /events、POST /shutdown?node=2
 *
 * 自检（退出码 0/1）：
 * 1. 三节点收敛；
 * 2. 触发一个节点正常关闭；
 * 3. 幸存节点收到 MemberLeft / MemberExited（不是仅不可达）；
 * 4. 幸存视图 upCount 变为 2；
 * 5. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex26_graceful_shutdown
 *   ./target/release/bin/cactor_examples.ex26_graceful_shutdown serve 18111
 */
package cactor_examples.ex26_graceful_shutdown

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterSettings}

let CLUSTER_WAIT = Duration.second * 25

class Events {
    let lock = Mutex()
    let items = ArrayList<String>()

    func add(e: String): Unit {
        synchronized(lock) {
            items.add(e)
        }
    }

    func leaves(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for (e in items) {
                if (e.contains("MemberLeft") || e.contains("MemberExited")) {
                    n++
                }
            }
            n
        }
    }

    func snapshot(): Array<String> {
        synchronized(lock) {
            items.toArray()
        }
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, events: Events) {
        this.index = index
        this.system = CActorSystemFactory.create("ex26-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        // 记录成员事件（用于断言"优雅离开"而不是"不可达"）
        cluster.subscribe({ e => events.add(e.toString()) })
        cluster.installCoordinatedShutdown()
    }

    /** 正常关闭：协调关闭先 leave，再停止守护 Actor */
    func gracefulStop(): Unit {
        if (!stopped) {
            stopped = true
            system.terminate()
            remote.stop()
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

func jsonEscape(s: String): String {
    let sb = StringBuilder()
    for (b in s.toArray()) {
        if (b == UInt8(UInt32(r'"'))) {
            sb.append("\\\"")
        } else if (b == UInt8(10)) {
            sb.append(" ")
        } else {
            sb.append(String.fromUtf8([b]))
        }
    }
    sb.toString()
}

func buildApp(nodes: ArrayList<Node>, events: Events, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"up\":${nodes[0].cluster.state().upCount()},\"leaves\":${events.leaves()}}")
    })
    app.get("/events", { ctx =>
        let sb = StringBuilder()
        sb.append("{\"events\":[")
        var first = true
        for (e in events.snapshot()) {
            if (!first) {
                sb.append(",")
            }
            first = false
            sb.append("\"${jsonEscape(e)}\"")
        }
        sb.append("]}")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(sb.toString())
    })
    app.post("/shutdown", { ctx =>
        let idx = ctx.queryInt("node") ?? 2
        if (idx <= 0 || idx >= Int64(nodes.size)) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(400u16, "{\"error\":\"bad_node\"}")
        } else {
            nodes[idx].gracefulStop()
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString("{\"stopped\":${idx}}")
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
    let nodeCount = 3
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.second * 2, Some(Duration.second * 3))
    let events = Events()
    let nodes = ArrayList<Node>()
    var seed: Option<NetworkAddress> = None<NetworkAddress>
    for (i in 0..nodeCount) {
        let n = Node(i, seed, settings, events)
        seed = Some(n.remote.getLocalAddress())
        nodes.add(n)
    }

    let app = buildApp(nodes, events, 0u16)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let real = buildApp(nodes, events, port)
        let actual = startServer(real)
        if (actual == 0u16) {
            println("ex26：端口绑定失败")
            return 1
        }
        println("ex26 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    var formed = true
    for (n in nodes) {
        formed = formed && waitUntil(CLUSTER_WAIT, { =>
            n.cluster.state().upCount() == nodeCount && n.cluster.state().converged
        })
    }
    c.check(formed, "三节点集群形成并收敛")
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. 正常关闭一个节点 ----
        println("\n[1] 触发节点 2 正常关闭（协调关闭 → leave）")
        nodes[2].gracefulStop()

        // ---- 2. 幸存者看到 Leaving/Exited ----
        println("\n[2] 幸存者收到 MemberLeft / MemberExited")
        let graceful = waitUntil(CLUSTER_WAIT, { => events.leaves() > 0 })
        c.check(graceful, "收到优雅离开事件 ${events.leaves()} 条")
        let up2 = waitUntil(CLUSTER_WAIT, { =>
            nodes[0].cluster.state().upCount() == 2 && nodes[1].cluster.state().upCount() == 2
        })
        c.check(up2, "幸存视图 upCount == 2")

        // ---- 3. Lou ----
        println("\n[3] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let state = readBody(client.get("http://127.0.0.1:${port}/state"))
        c.check(state.contains("\"up\":2") && state.contains("\"leaves\":"), "GET /state 反映 up=2：${state}")
        let ev = readBody(client.get("http://127.0.0.1:${port}/events"))
        c.check(ev.contains("MemberLeft") || ev.contains("MemberExited"), "GET /events 含优雅离开事件")
        let bad = client.post("http://127.0.0.1:${port}/shutdown?node=0", "")
        c.check(bad.status == 400, "拒绝关闭节点 0（承载 HTTP）")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        nodes[0].shutdown()
        nodes[1].shutdown()
    }

    println("\nex26：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
