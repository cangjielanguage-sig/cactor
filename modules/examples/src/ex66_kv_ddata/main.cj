/*
 * CActor Ex 66 - 基于 DData 的共享 KV（平台 kv + Lou，对标 Akka Distributed Data 的 LWWMap/PNCounter）
 *
 * 真实约束：
 * - 任意节点写入，其他节点通过反熵最终都能读到；
 * - 计数器跨节点累加是合并值，不是各算各的；
 * - 删除要能传播，读多数失败必须明确报错而不是返回旧值。
 *
 * 组件：
 * - `cactor_cluster.ddata.Replicator`（三节点两两 join）
 * - `cactor_platform.kv.DDataKvStore`：get/put/delete/keys/list/incr/getMajority
 * - Lou：GET/PUT/DELETE /kv、POST /incr、GET /keys、GET /remote
 *
 * 自检（退出码 0/1）：
 * 1. node0 put、node2 读到；node2 put、node0 读到（双向反熵）；
 * 2. node0 +2、node2 +3 后，node1 读计数 == 5；
 * 3. node1 删除后 node0 读到 None；list 前缀正确；getMajority 读回 user:1。
 * 4. Lou 端点与本地一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex66_kv_ddata
 *   ./target/release/bin/cactor_examples.ex66_kv_ddata serve 18210
 */
package cactor_examples.ex66_kv_ddata

import std.collection.*
import std.convert.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings}
import cactor_cluster.ddata.{Replicator}
import cactor_platform.kv.{DDataKvStore}

let WAIT = Duration.second * 25
let NAMESPACE = "demo-kv"

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let rep: Replicator
    let kv: DDataKvStore
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.system = CActorSystemFactory.create("ex66-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        this.rep = Replicator(system, remote, Duration.millisecond * 20)
        this.kv = DDataKvStore(rep, namespace: NAMESPACE)
    }

    func addr(): NetworkAddress {
        remote.getLocalAddress()
    }

    func address(): String {
        addr().toString()
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

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

func optJson(o: Option<String>): String {
    match (o) {
        case Some(v) => "\"${v}\""
        case None => "null"
    }
}

func keysJson(keys: Array<String>): String {
    let sb = StringBuilder()
    sb.append("[")
    var first = true
    for (k in keys) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${k}\"")
    }
    sb.append("]")
    sb.toString()
}

func buildApp(nodes: Array<Node>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/kv", { ctx =>
        let key = ctx.query("key") ?? ""
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"key\":\"${key}\",\"value\":${optJson(nodes[0].kv.get(key))}}")
    })
    // 从指定节点读（默认 node0），用来观察反熵是否到达
    app.get("/remote", { ctx =>
        let key = ctx.query("key") ?? ""
        let idx = ctx.queryInt("node") ?? 0
        let i = if (idx >= 0 && idx < Int64(nodes.size)) { idx } else { 0 }
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"node\":${i},\"key\":\"${key}\",\"value\":${optJson(nodes[i].kv.get(key))}}")
    })
    app.put("/kv", { ctx =>
        let key = ctx.query("key") ?? ""
        let value = ctx.query("value") ?? ""
        nodes[0].kv.put(key, value)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"ok\":true}")
    })
    app.post("/incr", { ctx =>
        let key = ctx.query("key") ?? "counter"
        let delta = ctx.queryInt("delta") ?? 1
        let v = nodes[0].kv.incr(key, delta)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"key\":\"${key}\",\"value\":${v}}")
    })
    app.delete("/kv", { ctx =>
        let key = ctx.query("key") ?? ""
        let existed = nodes[0].kv.delete(key)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"existed\":${existed}}")
    })
    app.get("/keys", { ctx =>
        let prefix = ctx.query("prefix") ?? ""
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"keys\":${keysJson(nodes[0].kv.list(prefix))}}")
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

func clusterSettings(): ClusterSettings {
    ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.millisecond * 300, Duration.second * 2).withSeedNodeTimeout(Duration.second * 2)
}

main(args: Array<String>): Int64 {
    let settings = clusterSettings()
    let nodes = ArrayList<Node>()
    nodes.add(Node(0, None<NetworkAddress>, settings))
    nodes.add(Node(1, Some<NetworkAddress>(nodes[0].addr()), settings))
    nodes.add(Node(2, Some<NetworkAddress>(nodes[0].addr()), settings))
    let all = nodes.toArray()
    // Replicator 需要两两 join（只单向 join 时数据只向一侧推送）
    for (i in 0..all.size) {
        for (k in 0..all.size) {
            if (i != k) {
                all[i].rep.join(all[k].addr())
            }
        }
    }
    try {
        let _ = waitUntil(WAIT, { => all[0].cluster.state().upCount() == 3 })

        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            let server = buildApp(all, port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex66：端口绑定失败")
                return 1
            }
            println("ex66 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        println("\n[1] 双向反熵")
        all[0].kv.put("user:1", "alice")
        c.check(waitUntil(WAIT, { => all[2].kv.get("user:1") == Some("alice") }), "node0 写入，node2 读到 alice")
        all[2].kv.put("user:2", "bob")
        c.check(waitUntil(WAIT, { => all[0].kv.get("user:2") == Some("bob") }), "node2 写入，node0 读到 bob")

        println("\n[2] 计数器跨节点合并")
        all[0].kv.incr("visits", 2)
        all[2].kv.incr("visits", 3)
        c.check(waitUntil(WAIT, { => all[1].kv.incr("visits", 0) == 5 }), "node1 读到合并计数 5")

        println("\n[3] 删除、前缀与读多数")
        all[1].kv.delete("user:2")
        c.check(waitUntil(WAIT, { => all[0].kv.get("user:2").isNone() }), "node1 删除后 node0 读到 None")
        c.check(all[0].kv.list("user:").size == 1, "list(user:) 只剩 1 个")
        c.check(all[0].kv.getMajority("user:1", timeout: Duration.second * 5) == Some("alice"), "getMajority 读回 alice")

        println("\n[4] Lou 端点")
        let server = buildApp(all, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            c.check(readBody(client.put("http://127.0.0.1:${port}/kv?key=cfg:mode&value=fast", "")).contains("\"ok\":true"),
                "PUT /kv")
            let got = readBody(client.get("http://127.0.0.1:${port}/kv?key=cfg:mode"))
            c.check(got.contains("\"value\":\"fast\""), "GET /kv：${got}")
            // 反熵是异步的：先等 node2 的副本真的收到，再读 /remote，避免把传播延迟当失败
            c.check(waitUntil(WAIT, { => all[2].kv.get("cfg:mode") == Some("fast") }), "node2 收到 cfg:mode")
            let remote = readBody(client.get("http://127.0.0.1:${port}/remote?key=cfg:mode&node=2"))
            c.check(remote.contains("\"value\":\"fast\""), "GET /remote node=2 也可见：${remote}")
            let inc = readBody(client.post("http://127.0.0.1:${port}/incr?key=hits&delta=4", ""))
            c.check(inc.contains("\"value\":4"), "POST /incr：${inc}")
            let keys = readBody(client.get("http://127.0.0.1:${port}/keys?prefix=cfg:"))
            c.check(keys.contains("cfg:mode"), "GET /keys：${keys}")
            let del = readBody(client.delete("http://127.0.0.1:${port}/kv?key=cfg:mode"))
            c.check(del.contains("\"existed\":true"), "DELETE /kv：${del}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex66：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        for (n in all) {
            n.shutdown()
        }
    }
}
