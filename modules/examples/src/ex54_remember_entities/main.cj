/*
 * CActor Ex 54 - 记住实体（对标 Akka Cluster Sharding remember-entities）
 *
 * 真实约束：
 * - 有状态的业务实体（订单/会话）在分片迁移或节点宕机后必须被重新激活，
 *   而不是等下一次用户请求才"想起来"；
 * - 恢复不能重复启动同一个实体，也不能把已删除的实体复活；
 * - 运维要能看到"这次故障恢复了多少个实体、分别在哪个节点"。
 *
 * 组件：
 * - `ShardingSettings.remembering(replicator)` + `ClusterSharding.restartedCount()`（cluster/membership）
 * - 经典 Actor 的 `preStart` 记录激活节点；Lou 提供触发与观测端点
 *
 * 自检（退出码 0/1）：
 * 1. 3 节点收敛，20 个订单分布到各节点；
 * 2. 发消息创建实体后，实体 ID 已复制到所有节点；
 * 3. 宕掉持有实体的节点 → 幸存节点**不收到新消息**也把实体重新激活，恢复数等于受害者实体数；
 * 4. 每个受害实体恰好被启动两次，第二次在别的节点；
 * 5. Lou /state 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex54_remember_entities
 *   ./target/release/bin/cactor_examples.ex54_remember_entities serve 18199
 */
package cactor_examples.ex54_remember_entities

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
import cactor_cluster.ddata.{Replicator}

let WAIT = Duration.second * 25
let ORDERS: Int64 = 20

/** 每个实体在哪些节点被激活过（按顺序） */
class StartLog {
    private let lock = Mutex()
    private let starts = HashMap<String, ArrayList<String>>()

    func record(id: String, node: String): Unit {
        synchronized(lock) {
            match (starts.get(id)) {
                case Some(list) => list.add(node)
                case None =>
                    let list = ArrayList<String>()
                    list.add(node)
                    starts[id] = list
            }
        }
    }

    func countOf(id: String): Int64 {
        synchronized(lock) {
            match (starts.get(id)) {
                case Some(list) => Int64(list.size)
                case None => 0
            }
        }
    }

    func snapshotOf(id: String): Array<String> {
        synchronized(lock) {
            match (starts.get(id)) {
                case Some(list) => list.toArray()
                case None => Array<String>()
            }
        }
    }

    func totalStarts(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for ((_, list) in starts) {
                n += list.size
            }
            n
        }
    }
}

class OrderEntity <: Actor {
    let id: String
    let node: String
    let log: StartLog
    var paid: Int64 = 0

    init(id: String, node: String, log: StartLog) {
        this.id = id
        this.node = node
        this.log = log
    }

    public prop name: String { get() { "order-entity" } }
    public prop description: String { get() { "order ${id}" } }

    public func preStart(): Unit {
        // 实体被"想起来"时立刻记录，不等业务消息
        log.record(id, node)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            if (s.getContent() == "stop") {
                context.stop(context.self)
                return MessageResult.Handled
            }
            paid++
        }
        MessageResult.Handled
    }
}

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let replicator: Replicator
    let sharding: ClusterSharding
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, log: StartLog) {
        this.system = CActorSystemFactory.create("ex54-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        this.replicator = Replicator(system, remote, Duration.millisecond * 50)
        let addr = remote.getLocalAddress().toString()
        let shardSettings = ShardingSettings.remembering(replicator).withRebalance(RebalancePolicy.disabled())
        this.sharding = ClusterSharding(cluster, system, remote, "orders", 32,
            { id => OrderEntity(id, addr, log) }, shardSettings)
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

    /** 模拟宕机：不停止分片区域、不通知任何成员，直接断开 */
    func crash(): Unit {
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

func totalLocal(nodes: ArrayList<Node>, skip: Int64): Int64 {
    var n: Int64 = 0
    for (i in 0..nodes.size) {
        if (i != skip) {
            n += nodes[i].sharding.localEntityCount()
        }
    }
    n
}

func buildApp(nodes: ArrayList<Node>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/pay", { ctx =>
        let id = ctx.query("id") ?? "o1"
        nodes[0].sharding.tell(id, StringMessage("pay"))
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"accepted\":true,\"id\":\"${id}\"}")
    })
    app.get("/state", { ctx =>
        var local = 0
        var restarts: Int64 = 0
        for (n in nodes) {
            local += n.sharding.localEntityCount()
            restarts += n.sharding.restartedCount()
        }
        let l = local
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"up\":${nodes[0].cluster.state().upCount()},\"local_entities\":${l}," +
            "\"restarts\":${restarts}}")
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
    let log = StartLog()
    let nodes = ArrayList<Node>()
    nodes.add(Node(0, None<NetworkAddress>, settings, log))
    nodes.add(Node(1, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings, log))
    nodes.add(Node(2, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings, log))

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let _ = waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 3 })
        let server = buildApp(nodes, port)
        let actual = startServer(server)
        if (actual == 0u16) {
            println("ex54：端口绑定失败")
            return 1
        }
        println("ex54 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    var crashed = false
    try {
        println("\n[1] 集群收敛与实体创建")
        c.check(waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 3 }), "3 节点 Up")
        for (e in 0..ORDERS) {
            nodes[0].sharding.tell("o${e}", StringMessage("pay"))
        }
        c.check(waitUntil(WAIT, { => log.totalStarts() == ORDERS }), "创建 ${ORDERS} 个订单实体")

        println("\n[2] Lou 端点")
        let server = buildApp(nodes, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let paid = readBody(client.post("http://127.0.0.1:${port}/pay?id=o99", ""))
            c.check(paid.contains("o99"), "POST /pay：${paid}")
            c.check(waitUntil(WAIT, { => log.countOf("o99") == 1 }), "HTTP 触发实体创建")
            let state = readBody(client.get("http://127.0.0.1:${port}/state"))
            c.check(state.contains("\"up\":3"), "GET /state：${state}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\n[3] 宕机后主动恢复")
        let victimIds = nodes[2].sharding.localEntityIds()
        c.check(victimIds.size > 0, "受害者节点持有 ${victimIds.size} 个实体")
        nodes[2].crash()
        crashed = true
        c.check(waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 2 }), "幸存 2 节点")
        // 不再发任何业务消息：幸存节点凭 remember-entities 主动启动
        c.check(waitUntil(WAIT, { => totalLocal(nodes, 2) >= ORDERS + 1 }), "幸存节点本地实体数补齐")
        let restarted = nodes[0].sharding.restartedCount() + nodes[1].sharding.restartedCount()
        c.check(restarted == Int64(victimIds.size), "恢复数等于受害者实体数（${restarted}）")

        println("\n[4] 每个受害实体恰好两次激活且换了节点")
        var twice = true
        var moved = true
        for (id in victimIds) {
            twice = twice && log.countOf(id) == 2
            let starts = log.snapshotOf(id)
            moved = moved && starts.size == 2 && starts[0] != starts[1]
        }
        c.check(twice, "激活次数 = 2")
        c.check(moved, "第二次激活在别的节点")
    } finally {
        if (!crashed) {
            nodes[2].shutdown()
        }
        nodes[1].shutdown()
        nodes[0].shutdown()
    }

    println("\nex54：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
