/*
 * CActor Ex 62 - 分片角色的动态可用性（差距 4.3：withRole + 无宿主时不静默卡死）
 *
 * 真实约束：
 * - 集群先只有网关（gateway），业务分片要求 worker 角色；此时消息不能丢，应明确缓冲；
 * - worker 加入后，缓冲消息自动排空并落到新 worker 上，网关始终不托管分片；
 * - 运维要能看见"当前有多少可托管成员 / 缓冲了多少消息"，而不是只看到超时。
 *
 * 组件：
 * - `ShardingSettings.withRole("worker")`、`ClusterSharding.stats()` / `ownerCount()`
 * - 区域在"角色没有可托管成员"时告警一次（本轮新增），恢复后重置
 * - Lou：GET /state、POST /send
 *
 * 自检（退出码 0/1）：
 * 1. 只有网关时：ownerCount=0、3 条消息进入缓冲、网关实体数 0；
 * 2. worker 加入并收敛后：缓冲清空、3 条全部在 worker 上处理、网关实体数仍为 0；
 * 3. Lou /state 与 /send 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex62_shard_role_join
 *   ./target/release/bin/cactor_examples.ex62_shard_role_join serve 18206
 */
package cactor_examples.ex62_shard_role_join

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
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterSharding, RebalancePolicy, ShardingSettings}

let WAIT = Duration.second * 25
let SHARDS: Int64 = 16

class Ledger {
    private let lock = Mutex()
    private let hosts = HashMap<String, Int64>()
    private var totalCount: Int64 = 0

    func record(node: String): Unit {
        synchronized(lock) {
            hosts[node] = (hosts.get(node) ?? 0) + 1
            totalCount++
        }
    }

    func total(): Int64 {
        synchronized(lock) { totalCount }
    }

    func at(node: String): Int64 {
        synchronized(lock) { hosts.get(node) ?? 0 }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{")
            var first = true
            for ((k, v) in hosts) {
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

class JobEntity <: Actor {
    let node: String
    let ledger: Ledger

    init(node: String, ledger: Ledger) {
        this.node = node
        this.ledger = ledger
    }

    public prop name: String { get() { "job-entity" } }
    public prop description: String { get() { "job entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(_) <- (message as StringMessage)) {
            ledger.record(node)
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
        this.system = CActorSystemFactory.create("ex62-node-${index}")
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
        region = Some<ClusterSharding>(ClusterSharding(cluster, system, remote, "jobs", SHARDS,
            { _ => JobEntity(addr, ledger) }, settings))
    }

    func buffered(): Int64 {
        let (_, _, _, _, b) = sharding().stats()
        b
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

func buildApp(gateway: Node, worker: Node, ledger: Ledger, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/state", { ctx =>
        let g = gateway.sharding()
        let (delivered, forwarded, handedOff, dropped, buffered) = g.stats()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"up\":${upCount(gateway.cluster)},\"owners\":${g.ownerCount()}," +
            "\"gateway_entities\":${g.localEntityCount()},\"worker_entities\":${worker.sharding().localEntityCount()}," +
            "\"buffered\":${buffered},\"delivered\":${delivered},\"forwarded\":${forwarded}," +
            "\"dropped\":${dropped},\"handed_off\":${handedOff},\"processed\":${ledger.total()}," +
            "\"by_node\":${ledger.json()}}")
    })
    app.post("/send", { ctx =>
        let id = ctx.query("id") ?? "job-1"
        let before = ledger.total()
        gateway.sharding().tell(id, StringMessage("run"))
        let ok = waitUntil(WAIT, { => ledger.total() >= before + 1 })
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"ok\":${ok},\"processed\":${ledger.total()}}")
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
    let ledger = Ledger()
    let gateway = Node(0, None<NetworkAddress>, settings, ["gateway"])
    var worker = Node(1, Some<NetworkAddress>(gateway.remote.getLocalAddress()), settings, ["worker"])
    var workerStarted = false
    try {
        gateway.startSharding(ledger)

        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            // serve 模式下先把 worker 拉起来，便于外部直接观察稳态
            worker.startSharding(ledger)
            workerStarted = true
            let _ = waitUntil(WAIT, { => upCount(gateway.cluster) == 2 })
            let server = buildApp(gateway, worker, ledger, port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex62：端口绑定失败")
                return 1
            }
            println("ex62 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        println("\n[1] 只有网关：消息进入缓冲，不静默丢弃")
        let _ = waitUntil(WAIT, { => upCount(gateway.cluster) == 1 })
        for (i in 0..3) {
            gateway.sharding().tell("job-${i}", StringMessage("run"))
        }
        c.check(waitUntil(WAIT, { => gateway.buffered() >= 3 }), "缓冲 3 条：${gateway.buffered()}")
        c.check(gateway.sharding().ownerCount() == 0, "没有可托管成员（ownerCount=0）")
        c.check(gateway.sharding().localEntityCount() == 0, "网关没有创建实体")

        println("\n[2] worker 加入后缓冲自动排空")
        worker.startSharding(ledger)
        workerStarted = true
        c.check(waitUntil(WAIT, { => upCount(gateway.cluster) == 2 }), "2 节点 Up")
        c.check(waitUntil(WAIT, { => ledger.total() == 3 }), "3 条全部在 worker 上处理")
        c.check(waitUntil(WAIT, { => gateway.buffered() == 0 }), "网关缓冲清空")
        c.check(ledger.at(worker.address()) == 3, "worker 处理 3 条")
        c.check(gateway.sharding().localEntityCount() == 0, "网关仍不托管实体")

        println("\n[3] Lou 端点")
        let server = buildApp(gateway, worker, ledger, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let state = readBody(client.get("http://127.0.0.1:${port}/state"))
            c.check(state.contains("\"processed\":3") && state.contains("\"buffered\":0") &&
                state.contains("\"gateway_entities\":0"), "GET /state：${state}")
            let send = readBody(client.post("http://127.0.0.1:${port}/send?id=job-9", ""))
            c.check(send.contains("\"ok\":true") && send.contains("\"processed\":4"), "POST /send：${send}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex62：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        if (workerStarted) {
            worker.shutdown()
        }
        gateway.shutdown()
    }
}
