/*
 * CActor Ex 49 - 分片协调状态持久化（差距 4.4 剩余部分：对标 Akka CoordinatorState store）
 *
 * 真实约束：
 * - 运维手动做过的再平衡（把某分片钉到某节点）不能被一次滚动重启抹掉，
 *   否则每次发布后热点又回到原处；
 * - 协调状态由区域持有、可多副本；重启后每个区域都要从同一存储恢复同样的覆盖；
 * - 恢复必须容错：目标节点已不在集群时，覆盖本身仍然保留，分配时按成员视图回退。
 *
 * 组件：
 * - 框架：`ShardingSettings.withStateStore(DurableStateStore)`（cluster/membership/sharding.cj）
 *   + `ClusterSharding.rebalanceShard` / `loadedOverrideOf` / `loadedOverrideCount`
 * - 存储：FileDurableStateStore（两个节点共享同一目录）
 * - Lou：POST /rebalance、GET /state、GET /healthz
 *
 * 自检（退出码 0/1）：
 * 1. 2 节点收敛后，手动覆盖分片 3 → 两节点区域都加载到该覆盖；
 * 2. Lou POST /rebalance 生效，GET /state 反映覆盖；
 * 3. 整个集群关停后，用同一存储全新启动，覆盖仍在（loadedOverrideOf 一致）；
 * 4. 没有配置存储时（对照）不会凭空出现覆盖。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex49_shard_state
 *   ./target/release/bin/cactor_examples.ex49_shard_state serve 18194
 */
package cactor_examples.ex49_shard_state

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterSharding, RebalancePolicy, ShardingSettings}
import cactor_persistence.{FileDurableStateStore}

let WAIT = Duration.second * 25
let SHARDS: Int64 = 16

/** 订单实体：只为分散到分片上，消息本身不做事 */
class OrderEntity <: Actor {
    let id: String

    init(id: String) {
        this.id = id
    }

    public prop name: String { get() { "order-entity" } }
    public prop description: String { get() { "order entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

class ShardNode {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let sharding: ClusterSharding
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, store: Option<FileDurableStateStore>) {
        this.system = CActorSystemFactory.create("ex49-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        var shardSettings = ShardingSettings.defaults().withRebalance(RebalancePolicy.disabled())
        match (store) {
            case Some(s) => shardSettings = shardSettings.withStateStore(s)
            case None => ()
        }
        this.sharding = ClusterSharding(cluster, system, remote, "orders", SHARDS,
            { id => OrderEntity(id) }, shardSettings)
    }

    func address(): String {
        remote.getLocalAddress().toString()
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            sharding.stop()
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

func stateJson(node: ShardNode): String {
    let sb = StringBuilder()
    sb.append("{\"up\":${node.cluster.state().upCount()},\"overrides\":{")
    var first = true
    for (i in 0..SHARDS) {
        match (node.sharding.loadedOverrideOf(i)) {
            case Some(a) =>
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${i}\":\"${a}\"")
            case None => ()
        }
    }
    sb.append("}}")
    sb.toString()
}

func buildApp(node: ShardNode, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/rebalance", { ctx =>
        let shard = ctx.queryInt("shard") ?? 0
        let target = ctx.query("target") ?? node.address()
        try {
            node.sharding.rebalanceShard(shard, target)
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString("{\"ok\":true,\"shard\":${shard},\"target\":\"${target}\"}")
        } catch (e: IllegalArgumentException) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString("{\"ok\":false,\"error\":\"${e.message}\"}")
        }
    })
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(stateJson(node))
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
    let dir = "/tmp/cactor-ex49-${MonoTime.now().hashCode()}"
    let store = FileDurableStateStore("${dir}/coord")
    let settings = clusterSettings()

    let nodes = ArrayList<ShardNode>()
    nodes.add(ShardNode(0, None<NetworkAddress>, settings, Some<FileDurableStateStore>(store)))
    nodes.add(ShardNode(1, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings,
        Some<FileDurableStateStore>(store)))

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let _ = waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 2 })
        let server = buildApp(nodes[0], port)
        let actual = startServer(server)
        if (actual == 0u16) {
            println("ex49：端口绑定失败")
            return 1
        }
        println("ex49 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    try {
        println("\n[1] 集群收敛与两区域加载")
        c.check(waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 2 }), "2 节点 Up")
        let target = nodes[1].address()
        nodes[0].sharding.rebalanceShard(3, target)
        c.check(waitUntil(WAIT, { =>
            nodes[0].sharding.loadedOverrideOf(3) == Some(target) &&
                nodes[1].sharding.loadedOverrideOf(3) == Some(target)
        }), "两节点区域都加载到分片 3 → ${target}")

        println("\n[2] Lou 端点")
        let server = buildApp(nodes[0], 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let resp = readBody(client.post(
                "http://127.0.0.1:${port}/rebalance?shard=7&target=${nodes[1].address()}", ""))
            c.check(resp.contains("\"ok\":true") && resp.contains("\"shard\":7"), "POST /rebalance：${resp}")
            c.check(waitUntil(WAIT, { => nodes[0].sharding.loadedOverrideOf(7) == Some(nodes[1].address()) }),
                "HTTP 覆盖分片 7 生效")
            let state = readBody(client.get("http://127.0.0.1:${port}/state"))
            c.check(state.contains("\"7\"") && state.contains("\"3\""), "GET /state 反映覆盖：${state}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\n[3] 整集群重启后覆盖仍在")
        for (n in nodes) {
            n.shutdown()
        }
        let store2 = FileDurableStateStore("${dir}/coord")
        let revived = ArrayList<ShardNode>()
        revived.add(ShardNode(10, None<NetworkAddress>, settings, Some<FileDurableStateStore>(store2)))
        revived.add(ShardNode(11, Some<NetworkAddress>(revived[0].remote.getLocalAddress()), settings,
            Some<FileDurableStateStore>(store2)))
        try {
            c.check(waitUntil(WAIT, { => revived[0].cluster.state().upCount() == 2 }), "重启后 2 节点 Up")
            c.check(waitUntil(WAIT, { => revived[0].sharding.loadedOverrideCount() >= 2 }), "重启后加载到覆盖")
            c.check(revived[0].sharding.loadedOverrideOf(3) == Some(target), "分片 3 覆盖保持：${target}")
            c.check(revived[0].sharding.loadedOverrideOf(7) == Some(target), "分片 7 覆盖保持：${target}")
        } finally {
            for (n in revived) {
                n.shutdown()
            }
        }

        println("\n[4] 无存储对照")
        let plain = ShardNode(20, None<NetworkAddress>, settings, None<FileDurableStateStore>)
        try {
            c.check(waitUntil(WAIT, { => plain.cluster.state().upCount() == 1 }), "对照节点就绪")
            c.check(plain.sharding.loadedOverrideCount() == 0, "无存储时不加载任何覆盖")
        } finally {
            plain.shutdown()
        }
    } finally {
        for (n in nodes) {
            try {
                n.shutdown()
            } catch (_: Exception) {}
        }
    }

    println("\nex49：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
