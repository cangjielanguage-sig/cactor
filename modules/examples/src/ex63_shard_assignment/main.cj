/*
 * CActor Ex 63 - 分片协调状态（全量归属）恢复（差距 4.4 收尾）
 *
 * 真实约束：
 * - 运维/上一轮运行已经决定"分片 -> 宿主"，重启后应沿用，而不是重新洗牌；
 * - 存储里的宿主若已不在集群，该分片必须回退到分配策略，不能卡死；
 * - 每个节点都能读到同一份归属，并只获取属于自己的分片。
 *
 * 组件：
 * - `ShardingSettings.withStateStore`、`ClusterSharding.assignmentKey`（新增）
 * - `ClusterSharding.loadedAssignmentOf / loadedAssignmentCount`（新增）
 * - 区域启动时加载全量归属；领导者按当前视图写回（内容不变则不写）
 * - Lou：GET /state、GET /where?shard=
 *
 * 自检（退出码 0/1）：
 * 1. 预置 10 个分片：0..8 给 node0、9 给 node1；两个区域都加载到 10 条；
 * 2. 实际托管严格按存储：node0 = 9、node1 = 1（与 rendezvous 分布刻意不同）；
 * 3. Lou /state、/where 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex63_shard_assignment
 *   ./target/release/bin/cactor_examples.ex63_shard_assignment serve 18207
 */
package cactor_examples.ex63_shard_assignment

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
let SHARDS: Int64 = 10
let TYPE_NAME = "orders"

class IdleEntity <: Actor {
    public prop name: String { get() { "idle-entity" } }
    public prop description: String { get() { "idle entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    var region: Option<ClusterSharding> = None<ClusterSharding>
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.system = CActorSystemFactory.create("ex63-node-${index}")
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

    func sharding(): ClusterSharding {
        region.getOrThrow()
    }

    func startSharding(store: FileDurableStateStore): Unit {
        let settings = ShardingSettings.defaults().withRebalance(RebalancePolicy.disabled()).withStateStore(store)
        region = Some<ClusterSharding>(ClusterSharding(cluster, system, remote, TYPE_NAME, SHARDS,
            { _ => IdleEntity() }, settings))
    }

    func hosted(): Int64 {
        let (h, _, _) = sharding().shardStats()
        h
    }

    func acquiring(): Int64 {
        let (_, a, _) = sharding().shardStats()
        a
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            match (region) {
                case Some(r) => r.stop()
                case None => ()
            }
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

func upCount(cluster: Cluster): Int64 {
    cluster.state().upCount()
}

/** 预置归属：0..8 -> a，9 -> b */
func seedAssignment(store: FileDurableStateStore, a: String, b: String): Unit {
    let sb = StringBuilder()
    for (sh in 0..9) {
        sb.append("${sh}|${a}\n")
    }
    sb.append("9|${b}\n")
    let key = ClusterSharding.assignmentKey(TYPE_NAME)
    store.upsert(key, store.revisionOf(key) + 1, sb.toString())
}

func buildApp(n0: Node, n1: Node, store: FileDurableStateStore, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"up\":${upCount(n0.cluster)},\"hosted\":{\"${n0.address()}\":${n0.hosted()}," +
            "\"${n1.address()}\":${n1.hosted()}},\"loaded\":${n0.sharding().loadedAssignmentCount()}," +
            "\"acquiring\":${n0.acquiring() + n1.acquiring()}}")
    })
    app.get("/where", { ctx =>
        let sh = ctx.queryInt("shard") ?? 0
        let o0 = n0.sharding().loadedAssignmentOf(sh)
        let o1 = n1.sharding().loadedAssignmentOf(sh)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"shard\":${sh},\"from_node0\":${optJson(o0)},\"from_node1\":${optJson(o1)}}")
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
    app
}

func optJson(o: Option<String>): String {
    match (o) {
        case Some(v) => "\"${v}\""
        case None => "null"
    }
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
    nodes.add(Node(0, None<NetworkAddress>, settings))
    nodes.add(Node(1, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings))
    let n0 = nodes[0]
    let n1 = nodes[1]
    // 每次运行用独立目录，避免上一轮的旧地址影响断言
    let dir = "/tmp/cactor-ex63-${n0.remote.getLocalAddress().port}"
    let store = FileDurableStateStore(dir)
    try {
        let _ = waitUntil(WAIT, { => upCount(n0.cluster) == 2 })
        seedAssignment(store, n0.address(), n1.address())
        n0.startSharding(store)
        n1.startSharding(store)

        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            let _ = waitUntil(WAIT, { => n0.hosted() == 9 && n1.hosted() == 1 })
            let server = buildApp(n0, n1, store, port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex63：端口绑定失败")
                return 1
            }
            println("ex63 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        println("\n[1] 两个区域都恢复全量归属")
        c.check(waitUntil(WAIT, { => n0.sharding().loadedAssignmentCount() == SHARDS &&
            n1.sharding().loadedAssignmentCount() == SHARDS }), "各加载 ${SHARDS} 条")
        c.check(n0.sharding().loadedAssignmentOf(0) == Some(n0.address()), "分片 0 -> node0")
        c.check(n1.sharding().loadedAssignmentOf(9) == Some(n1.address()), "分片 9 -> node1")

        println("\n[2] 实际托管严格按存储")
        c.check(waitUntil(WAIT, { => n0.hosted() == 9 && n1.hosted() == 1 &&
            n0.acquiring() == 0 && n1.acquiring() == 0 }), "node0=9、node1=1、无获取中")

        println("\n[3] Lou 端点")
        let server = buildApp(n0, n1, store, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let state = readBody(client.get("http://127.0.0.1:${port}/state"))
            c.check(state.contains("\"loaded\":10") && state.contains("\"up\":2") &&
                state.contains(n0.address()) && state.contains(n1.address()), "GET /state：${state}")
            let whereBody = readBody(client.get("http://127.0.0.1:${port}/where?shard=9"))
            c.check(whereBody.contains(n1.address()), "GET /where?shard=9：${whereBody}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex63：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        n0.shutdown()
        n1.shutdown()
    }
}
