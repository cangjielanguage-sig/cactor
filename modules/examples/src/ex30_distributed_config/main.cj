/*
 * CActor Ex 30 - 分布式配置：Map 系 CRDT（ORMap / LWWMap，对标 Akka Distributed Data 的 ORMap/LWWMap）
 *
 * 真实约束：
 * - 分布式配置/标签是"多写者、按 key 改"的：把整个 Map 塞进一个 LWWRegister 会让并发改不同 key 的写入互相覆盖；
 * - 删除必须能收敛：旧副本上的旧值不能把已删除的 key 复活（观察-删除）；
 * - 某些字段需要"最后写入胜出"（如功能开关），另一些需要"并发添加都保留"（如标签集合）——两种语义并存。
 *
 * 组件：
 * - 框架：`ORMap`（键值对集合的观察-删除）、`LWWMap`（每键一个 LWW 寄存器，墓碑删除）
 *   + `Replicator.putInMap/removeMapKey/removeFromMap`、`putInLWWMap/removeFromLWWMap`
 * - 拓扑：3 节点集群 + ClusterReplicatorBinding
 * - Lou：POST/DELETE /labels、POST/DELETE /flags、GET /state
 *
 * 自检（退出码 0/1）：
 * 1. 3 节点收敛；
 * 2. 两个节点并发写不同 label key → 合并后两个 key 都在；
 * 3. 同一 key 并发写不同值 → add-wins，两个值都保留；
 * 4. removeMapKey 后所有节点都看不到该 key（旧副本不复活）；
 * 5. LWWMap：更晚时间戳覆盖；墓碑删除在所有节点生效；
 * 6. Lou /state 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex30_distributed_config
 *   ./target/release/bin/cactor_examples.ex30_distributed_config serve 18115
 */
package cactor_examples.ex30_distributed_config

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterReplicatorBinding, ClusterSettings}
import cactor_cluster.ddata.{ReplicatedValue, Replicator}

let WAIT = Duration.second * 20
let GOSSIP = Duration.millisecond * 50

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let rep: Replicator
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.index = index
        this.system = CActorSystemFactory.create("ex30-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        this.rep = Replicator(system, remote, GOSSIP)
        ClusterReplicatorBinding.bind(cluster, rep)
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            rep.stop()
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

/** 某节点上 labels 键的可见内容（key -> 值数组） */
func labelsOf(node: Node): HashMap<String, Array<String>> {
    let out = HashMap<String, Array<String>>()
    match (node.rep.get("labels")) {
        case Some(ReplicatedValue.ORMapValue(m)) =>
            for (k in m.keys()) {
                out[k] = m.values(k)
            }
        case _ => ()
    }
    out
}

func flagsJson(node: Node): String {
    let sb = StringBuilder()
    sb.append("{")
    let flagKeys = ["mode", "rollout"]
    var first = true
    for (k in flagKeys) {
        if (!first) {
            sb.append(",")
        }
        first = false
        let v = node.rep.lwwMapGet("flags", k)
        sb.append("\"${k}\":")
        match (v) {
            case Some(x) => sb.append("\"${x}\"")
            case None => sb.append("null")
        }
    }
    sb.append("}")
    sb.toString()
}

func stateJson(nodes: ArrayList<Node>): String {
    let sb = StringBuilder()
    sb.append("{\"nodes\":[")
    var firstNode = true
    for (n in nodes) {
        if (!firstNode) {
            sb.append(",")
        }
        firstNode = false
        let labels = labelsOf(n)
        sb.append("{\"index\":${n.index},\"up\":${n.cluster.state().upCount()},\"labels\":{")
        var first = true
        for ((k, values) in labels) {
            if (!first) {
                sb.append(",")
            }
            first = false
            sb.append("\"${k}\":[")
            var fv = true
            for (v in values) {
                if (!fv) {
                    sb.append(",")
                }
                fv = false
                sb.append("\"${v}\"")
            }
            sb.append("]")
        }
        sb.append("},\"flags\":${flagsJson(n)}}")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(nodes: ArrayList<Node>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    // 写请求打到非 HTTP 节点上，证明确实经集群 gossip 收敛，而不是本地自读自写
    app.post("/labels", { ctx =>
        let key = ctx.query("key") ?? "region"
        let value = ctx.query("value") ?? "east"
        nodes[1].rep.putInMap("labels", key, value)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"set\":\"${key}\",\"value\":\"${value}\"}")
    })
    app.delete("/labels", { ctx =>
        let key = ctx.query("key") ?? "region"
        nodes[2].rep.removeMapKey("labels", key)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"removed\":\"${key}\"}")
    })
    app.post("/flags", { ctx =>
        let key = ctx.query("key") ?? "mode"
        let value = ctx.query("value") ?? "fast"
        let ts = ctx.queryInt("ts") ?? 1
        nodes[2].rep.putInLWWMap("flags", key, value, ts)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"set\":\"${key}\",\"value\":\"${value}\",\"ts\":${ts}}")
    })
    app.delete("/flags", { ctx =>
        let key = ctx.query("key") ?? "mode"
        let ts = ctx.queryInt("ts") ?? 1
        nodes[2].rep.removeFromLWWMap("flags", key, ts)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"removed\":\"${key}\"}")
    })
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(stateJson(nodes))
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

/** 三个节点上 labels 的某 key 都满足 cond */
func allLabel(nodes: ArrayList<Node>, key: String, cond: (Array<String>) -> Bool): Bool {
    for (n in nodes) {
        if (!cond(labelsOf(n).get(key) ?? Array<String>())) {
            return false
        }
    }
    true
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
            println("ex30：端口绑定失败")
            return 1
        }
        println("ex30 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
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
        c.check(formed, "3 节点全部 Up 且收敛")

        println("\n[2] 并发写不同 key（ORMap 按 key 独立合并）")
        nodes[0].rep.putInMap("labels", "region", "east")
        nodes[1].rep.putInMap("labels", "tier", "gold")
        c.check(waitUntil(WAIT, { =>
            allLabel(nodes, "region", { v => v == ["east"] }) &&
            allLabel(nodes, "tier", { v => v == ["gold"] })
        }), "两个 key 在 3 个节点都可见")

        println("\n[3] 同一 key 并发写不同值（add-wins）")
        nodes[0].rep.putInMap("labels", "zone", "a")
        nodes[1].rep.putInMap("labels", "zone", "b")
        c.check(waitUntil(WAIT, { => allLabel(nodes, "zone", { v => v == ["a", "b"] }) }),
            "zone 同时保留 a、b")

        println("\n[4] 删除 key（观察-删除，旧副本不复活）")
        nodes[2].rep.removeMapKey("labels", "region")
        c.check(waitUntil(WAIT, { =>
            allLabel(nodes, "region", { v => v.isEmpty() }) && allLabel(nodes, "tier", { v => v == ["gold"] })
        }), "region 在 3 个节点都消失，tier 不受影响")

        println("\n[5] LWWMap：时间戳决胜与墓碑删除")
        nodes[0].rep.putInLWWMap("flags", "mode", "fast", 10)
        nodes[1].rep.putInLWWMap("flags", "mode", "safe", 20)
        c.check(waitUntil(WAIT, { =>
            nodes[0].rep.lwwMapGet("flags", "mode") == Some("safe") &&
            nodes[1].rep.lwwMapGet("flags", "mode") == Some("safe") &&
            nodes[2].rep.lwwMapGet("flags", "mode") == Some("safe")
        }), "更晚时间戳的 safe 覆盖 fast")
        nodes[2].rep.removeFromLWWMap("flags", "mode", 30)
        c.check(waitUntil(WAIT, { =>
            nodes[0].rep.lwwMapGet("flags", "mode").isNone() &&
            nodes[1].rep.lwwMapGet("flags", "mode").isNone() &&
            nodes[2].rep.lwwMapGet("flags", "mode").isNone()
        }), "墓碑删除在 3 个节点生效")

        println("\n[6] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let set = readBody(client.post("http://127.0.0.1:${port}/labels?key=owner&value=alice", ""))
        c.check(set.contains("\"set\":\"owner\""), "POST /labels 写入")
        c.check(waitUntil(WAIT, { => allLabel(nodes, "owner", { v => v == ["alice"] }) }),
            "写入经 gossip 出现在所有节点")
        let flag = readBody(client.post("http://127.0.0.1:${port}/flags?key=rollout&value=on&ts=1", ""))
        c.check(flag.contains("\"set\":\"rollout\""), "POST /flags 写入")
        c.check(waitUntil(WAIT, { => nodes[0].rep.lwwMapGet("flags", "rollout") == Some("on") }),
            "flag 收敛到 HTTP 节点")
        let st = readBody(client.get("http://127.0.0.1:${port}/state"))
        c.check(st.contains("\"owner\":[\"alice\"]") && st.contains("\"rollout\":\"on\""),
            "GET /state 与内存一致")
        let del = readBody(client.delete("http://127.0.0.1:${port}/labels?key=owner"))
        c.check(del.contains("\"removed\":\"owner\""), "DELETE /labels 删除")
        c.check(waitUntil(WAIT, { => allLabel(nodes, "owner", { v => v.isEmpty() }) }),
            "删除同样收敛到所有节点")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex30：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
