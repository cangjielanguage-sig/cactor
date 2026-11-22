/*
 * CActor Ex 58 - 分片宿主角色（对标 Akka ClusterShardingSettings.withRole，差距 4.3）
 *
 * 真实约束：
 * - 网关节点只做接入，不托管任何分片；业务实体只能落在 worker 角色节点上；
 * - 角色随成员视图传播：区域在计算归属与移交确认时都只看带该角色的成员；
 * - 网关仍可正常收发：消息由它转发到 worker 上的唯一实体，不会因为自身不是 worker 而卡住。
 *
 * 组件：
 * - `ShardingSettings.withRole("worker")`（本轮新增）
 * - `Cluster(..., roles: [...])`、`ClusterSnapshot.membersWithRole`
 * - `ClusterSharding.tell / shardStats / localEntityCount`
 * - Lou：POST /order、GET /state、GET /where
 *
 * 自检（退出码 0/1）：
 * 1. 3 节点收敛，2 worker + 1 gateway；
 * 2. 64 个分片全部托管在 worker 上，网关托管 0、获取中 0；
 * 3. 从三个节点各发 8 条，实体只建在 worker 上（网关 localEntityCount == 0）；
 * 4. 每个实体只出现在 worker 地址上，消息总数与内存一致；
 * 5. Lou /state、/where 与内存一致，POST /order 生效。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex58_shard_roles
 *   ./target/release/bin/cactor_examples.ex58_shard_roles serve 18202
 */
package cactor_examples.ex58_shard_roles

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
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterSharding, RebalancePolicy,
    ShardingSettings}

let WAIT = Duration.second * 25
let SHARDS: Int64 = 64

/** 实体侧统计：每个实体被哪些节点处理过、累计条数、最近一次内容 */
class Ledger {
    private let lock = Mutex()
    private let hosts = HashMap<String, HashSet<String>>()
    private let counts = HashMap<String, Int64>()
    private let last = HashMap<String, String>()
    private let byNode = HashMap<String, Int64>()
    private var totalCount: Int64 = 0

    func record(id: String, node: String, content: String): Unit {
        synchronized(lock) {
            match (hosts.get(id)) {
                case Some(s) => s.add(node)
                case None =>
                    let s = HashSet<String>()
                    s.add(node)
                    hosts[id] = s
            }
            counts[id] = (counts.get(id) ?? 0) + 1
            last[id] = content
            byNode[node] = (byNode.get(node) ?? 0) + 1
            totalCount++
        }
    }

    func total(): Int64 {
        synchronized(lock) { totalCount }
    }

    func countOf(id: String): Int64 {
        synchronized(lock) { counts.get(id) ?? 0 }
    }

    func hostsOf(id: String): Array<String> {
        synchronized(lock) {
            match (hosts.get(id)) {
                case Some(s) => s.toArray()
                case None => Array<String>()
            }
        }
    }

    func at(node: String): Int64 {
        synchronized(lock) { byNode.get(node) ?? 0 }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{")
            var first = true
            for ((k, v) in byNode) {
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

class OrderEntity <: Actor {
    let id: String
    let node: String
    let ledger: Ledger

    init(id: String, node: String, ledger: Ledger) {
        this.id = id
        this.node = node
        this.ledger = ledger
    }

    public prop name: String { get() { "order-entity" } }
    public prop description: String { get() { "order entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            ledger.record(id, node, s.getContent())
        }
        MessageResult.Handled
    }
}

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    var region: Option<ClusterSharding> = None<ClusterSharding>
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, roles: Array<String>) {
        this.system = CActorSystemFactory.create("ex58-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings, roles: roles)
    }

    func address(): String {
        remote.getLocalAddress().toString()
    }

    func sharding(): ClusterSharding {
        region.getOrThrow()
    }

    func startSharding(ledger: Ledger): Unit {
        let addr = address()
        let settings = ShardingSettings.defaults().withRebalance(RebalancePolicy.disabled()).withRole("worker")
        region = Some<ClusterSharding>(ClusterSharding(cluster, system, remote, "orders", SHARDS,
            { id => OrderEntity(id, addr, ledger) }, settings))
    }

    func hostedShards(): Int64 {
        let (h, _, _) = sharding().shardStats()
        h
    }

    func acquiringShards(): Int64 {
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

func allAcquired(nodes: Array<Node>): Bool {
    var hosted: Int64 = 0
    var acquiring: Int64 = 0
    for (n in nodes) {
        hosted += n.hostedShards()
        acquiring += n.acquiringShards()
    }
    acquiring == 0 && hosted == SHARDS
}

func hostsJson(nodes: Array<Node>): String {
    let sb = StringBuilder()
    sb.append("{")
    var first = true
    for (n in nodes) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${n.address()}\":${n.hostedShards()}")
    }
    sb.append("}")
    sb.toString()
}

func buildApp(nodes: Array<Node>, ledger: Ledger, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/order", { ctx =>
        let id = ctx.query("id") ?? "e0"
        let v = ctx.query("v") ?? "x"
        let before = ledger.total()
        nodes[0].sharding().tell(id, StringMessage(v))
        let ok = waitUntil(WAIT, { => ledger.total() >= before + 1 })
        ctx.setSingleHeader("Content-Type", "application/json")
        if (ok) {
            ctx.writeString("{\"ok\":true,\"id\":\"${id}\",\"hosts\":${arrayJson(ledger.hostsOf(id))}}")
        } else {
            ctx.writeString("{\"ok\":false,\"id\":\"${id}\",\"error\":\"timeout\"}")
        }
    })
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        let workers = nodes[0].cluster.state().membersWithRole("worker").size
        ctx.writeString("{\"total\":${ledger.total()},\"workers\":${workers},\"hosted\":${hostsJson(nodes)},\"by_node\":${ledger.json()}}")
    })
    app.get("/where", { ctx =>
        let id = ctx.query("id") ?? "e0"
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"id\":\"${id}\",\"count\":${ledger.countOf(id)},\"hosts\":${arrayJson(ledger.hostsOf(id))}}")
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
    app
}

func arrayJson(items: Array<String>): String {
    let sb = StringBuilder()
    sb.append("[")
    var first = true
    for (x in items) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${x}\"")
    }
    sb.append("]")
    sb.toString()
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
    nodes.add(Node(0, None<NetworkAddress>, settings, ["gateway"]))
    nodes.add(Node(1, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings, ["worker"]))
    nodes.add(Node(2, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings, ["worker"]))
    let all = nodes.toArray()
    let ledger = Ledger()

    try {
        // 每个节点都要用相同 typeName 建区域；网关也建，但它不是 worker，不会托管分片
        for (n in all) {
            n.startSharding(ledger)
        }

        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            let _ = waitUntil(WAIT, { => upCount(all[0].cluster) == 3 && allAcquired(all) })
            let server = buildApp(all, ledger, port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex58：端口绑定失败")
                return 1
            }
            println("ex58 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        println("\n[1] 角色与收敛")
        c.check(waitUntil(WAIT, { => upCount(all[0].cluster) == 3 }), "3 节点 Up")
        c.check(waitUntil(WAIT, { =>
            all[0].cluster.state().membersWithRole("worker").size == 2 &&
                all[0].cluster.state().membersWithRole("gateway").size == 1
        }), "视图：2 worker + 1 gateway")

        println("\n[2] 分片只落在 worker")
        c.check(waitUntil(WAIT, { => allAcquired(all) }), "64 个分片全部获取完成")
        c.check(all[1].hostedShards() + all[2].hostedShards() == SHARDS,
            "worker 托管 ${all[1].hostedShards() + all[2].hostedShards()} 个分片")
        c.check(all[0].hostedShards() == 0, "网关托管 0 个分片")
        c.check(all[0].acquiringShards() == 0, "网关没有卡在获取中")

        println("\n[3] 网关转发、实体只在 worker 上创建")
        for (i in 0..3) {
            for (k in 0..8) {
                all[i].sharding().tell("e${k}", StringMessage("from-${i}"))
            }
        }
        c.check(waitUntil(WAIT, { => ledger.total() == 24 }), "24 条全部处理：${ledger.total()}")
        c.check(all[0].sharding().localEntityCount() == 0, "网关 localEntityCount == 0")
        c.check(all[1].sharding().localEntityCount() + all[2].sharding().localEntityCount() == 8,
            "8 个实体全在 worker 上")
        c.check(all[1].hostedShards() + all[2].hostedShards() == SHARDS, "转发后分片归属不变")
        var onlyWorkers = true
        for (k in 0..8) {
            for (h in ledger.hostsOf("e${k}")) {
                if (h != all[1].address() && h != all[2].address()) {
                    onlyWorkers = false
                }
            }
        }
        c.check(onlyWorkers, "实体宿主只出现 worker 地址")
        c.check(ledger.at(all[0].address()) == 0, "网关处理 0 条：${ledger.json()}")

        println("\n[4] Lou 端点")
        let server = buildApp(all, ledger, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let state = readBody(client.get("http://127.0.0.1:${port}/state"))
            c.check(state.contains("\"total\":24") && state.contains("\"workers\":2"), "GET /state：${state}")
            let whereBody = readBody(client.get("http://127.0.0.1:${port}/where?id=e3"))
            c.check(whereBody.contains("\"count\":3"), "GET /where：${whereBody}")
            let put = readBody(client.post("http://127.0.0.1:${port}/order?id=e3&v=apple", ""))
            c.check(put.contains("\"ok\":true"), "POST /order：${put}")
            c.check(waitUntil(WAIT, { => ledger.countOf("e3") == 4 }), "e3 计数 4")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex58：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        for (n in all) {
            n.shutdown()
        }
    }
}

