/*
 * CActor Ex 34 - 分片分配策略 SPI（差距 4.4：ShardAllocationStrategy 可插拔）
 *
 * 真实约束：
 * - 业务常常要把分片"钉"在特定位置：同机房亲和、容量隔离、灰度期间只让新节点接管一部分分片；
 * - 这些规则不该改框架源码，也不该靠改哈希函数（会引发全量迁移）；
 * - 策略必须是纯函数：各节点在各自视图上算出同样结果，否则同一分片会被多个节点托管。
 *
 * 框架能力（本轮新增）：
 * - `ShardAllocationStrategy { allocate(shard, members): Option<String> }`
 * - `RendezvousAllocation`（默认，行为不变）、`FirstMemberAllocation`
 * - `ShardingSettings.withAllocationStrategy(...)`
 *
 * 示意图：两个不同类型的区域共存
 * - `parity-entity` 用 `ParityAllocation`：偶数分片给地址最小的节点、奇数分片给地址最大的节点
 *   （确定性使用 2 个节点，第三节点不托管）；
 * - `pinned-entity` 用 `FirstMemberAllocation`：全部实体落在地址最小的节点。
 *
 * Lou：GET /state、POST /work?type=&n=、GET /healthz
 *
 * 自检（退出码 0/1）：
 * 1. 3 节点收敛；
 * 2. parity 类型 16 个实体：恰好 2 个节点各自托管一部分，第三个为 0；
 * 3. pinned 类型 12 个实体：全部在地址最小的节点，另外两个为 0；
 * 4. Lou /state 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex34_shard_allocation
 *   ./target/release/bin/cactor_examples.ex34_shard_allocation serve 18119
 */
package cactor_examples.ex34_shard_allocation

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
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterSharding, FirstMemberAllocation,
    ShardAllocationStrategy, ShardingSettings}

let WAIT = Duration.second * 25
// HTTP 端点生成的实体 ID 序号（从 10000 起，避开自检里手工使用的 ID）
let workSeq = AtomicInt64(10000)

/** 偶数分片 → 地址最小的节点；奇数分片 → 地址最大的节点（确定性使用两个节点） */
class ParityAllocation <: ShardAllocationStrategy {
    public init() {}

    public func allocate(shard: Int64, members: Array<String>): Option<String> {
        if (members.isEmpty()) {
            return None<String>
        }
        var lowest = members[0]
        var highest = members[0]
        for (i in 1..members.size) {
            if (members[i] < lowest) {
                lowest = members[i]
            }
            if (members[i] > highest) {
                highest = members[i]
            }
        }
        if (shard % 2 == 0) {
            Some(lowest)
        } else {
            Some(highest)
        }
    }
}

class Entity <: Actor {
    let id: String

    init(id: String) {
        this.id = id
    }

    public prop name: String { get() { "allocation-entity" } }
    public prop description: String { get() { "allocation entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let parity: ClusterSharding
    let pinned: ClusterSharding
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.index = index
        this.system = CActorSystemFactory.create("ex34-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        this.parity = ClusterSharding(cluster, system, remote, "parity-entity", 16,
            { id => Entity(id) }, ShardingSettings.defaults().withAllocationStrategy(ParityAllocation()))
        this.pinned = ClusterSharding(cluster, system, remote, "pinned-entity", 16,
            { id => Entity(id) }, ShardingSettings.defaults().withAllocationStrategy(FirstMemberAllocation()))
    }

    func address(): String {
        remote.getLocalAddress().toString()
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            parity.stop()
            pinned.stop()
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

func stateJson(nodes: ArrayList<Node>): String {
    let sb = StringBuilder()
    sb.append("{\"nodes\":[")
    var first = true
    for (n in nodes) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("{\"index\":${n.index},\"address\":\"${n.address()}\"," +
            "\"parityEntities\":${n.parity.localEntityCount()}," +
            "\"pinnedEntities\":${n.pinned.localEntityCount()}," +
            "\"parityShards\":${n.parity.shardStats()[0]}," +
            "\"pinnedShards\":${n.pinned.shardStats()[0]}}")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(nodes: ArrayList<Node>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(stateJson(nodes))
    })
    app.post("/work", { ctx =>
        let kind = ctx.query("type") ?? "parity"
        let n = ctx.queryInt("n") ?? 8
        let region = if (kind == "pinned") { nodes[0].pinned } else { nodes[0].parity }
        let base = workSeq.fetchAdd(n)
        for (i in 0..n) {
            region.tell("${kind}-e-${base + i}", StringMessage("work-${i}"))
        }
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"type\":\"${kind}\",\"dispatched\":${n}}")
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

func lowestAddressIndex(nodes: ArrayList<Node>): Int64 {
    var best = 0
    for (i in 1..nodes.size) {
        if (nodes[i].address() < nodes[best].address()) {
            best = i
        }
    }
    best
}

func highestAddressIndex(nodes: ArrayList<Node>): Int64 {
    var best = 0
    for (i in 1..nodes.size) {
        if (nodes[i].address() > nodes[best].address()) {
            best = i
        }
    }
    best
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
            println("ex34：端口绑定失败")
            return 1
        }
        println("ex34 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
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
        c.check(formed, "3 节点全部 Up")
        let lowest = lowestAddressIndex(nodes)
        let highest = highestAddressIndex(nodes)

        println("\n[2] parity 策略：偶数分片 → 最小地址，奇数分片 → 最大地址")
        for (i in 0..16) {
            nodes[0].parity.tell("parity-e-${i}", StringMessage("work-${i}"))
        }
        c.check(waitUntil(WAIT, { =>
            nodes[lowest].parity.localEntityCount() > 0 && nodes[highest].parity.localEntityCount() > 0
        }), "最小与最大地址节点各自托管部分 parity 实体")
        var third = -1
        for (i in 0..3) {
            if (i != lowest && i != highest) {
                third = i
            }
        }
        c.check(nodes[third].parity.localEntityCount() == 0, "第三节点不托管 parity 实体")

        println("\n[3] FirstMember 策略：全部实体落在最小地址节点")
        for (i in 0..12) {
            nodes[0].pinned.tell("pinned-e-${i}", StringMessage("work-${i}"))
        }
        c.check(waitUntil(WAIT, { => nodes[lowest].pinned.localEntityCount() == 12 }),
            "最小地址节点托管 12 个 pinned 实体")
        c.check(nodes[highest].pinned.localEntityCount() == 0 && nodes[third].pinned.localEntityCount() == 0,
            "其余两个节点不托管 pinned 实体")

        println("\n[4] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let st = readBody(client.get("http://127.0.0.1:${port}/state"))
        c.check(st.contains("\"pinnedEntities\":12") || st.contains("\"pinnedEntities\": 12"),
            "GET /state 反映 pinned 实体数")
        let work = readBody(client.post("http://127.0.0.1:${port}/work?type=pinned&n=2", ""))
        c.check(work.contains("\"type\":\"pinned\"") && work.contains("\"dispatched\":2"),
            "POST /work 受理自定义类型")
        c.check(waitUntil(WAIT, { => nodes[lowest].pinned.localEntityCount() == 14 }),
            "新的 2 个 pinned 实体同样落在最小地址节点")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex34：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
