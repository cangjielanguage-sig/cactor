/*
 * CActor Ex 20 - 租约保护的单例（对标 Akka cluster-singleton + lease）
 *
 * 真实约束：
 * - 单例实例全集群最多一个：靠成员视图选"最老"还不够，分区/错误 down 后可能双主；
 * - 因此实例启动前必须先拿到租约（分布式互斥），旧实例的租约未过期时新实例不能启动；
 * - 持有者宕机后，租约到期、新的最老节点接管，服务自动恢复；
 * - 任何时刻本节点 `isRunningHere()` 为真的节点数不超过 1。
 *
 * 组件：
 * - ClusterSingleton + ClusterSingletonSettings.withLease(FileLease)
 * - FileLease：与 ex02 相同的文件租约（生产可替换为 etcd/Redis 实现）
 * - Lou：GET /status 返回实例所在地址与计数
 *
 * 自检（退出码 0/1）：
 * 1. 三节点收敛后，恰好一个节点运行实例，命令都能打到同一个实例；
 * 2. down 掉持有者并关闭它 → 租约到期后另一个节点接管；
 * 3. 接管期间与之后，任意时刻运行实例的节点数 ≤ 1；
 * 4. 接管后命令仍然可用（计数从新实例重新开始，说明是新实例）；
 * 5. Lou /status 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex20_singleton_lease
 *   ./target/release/bin/cactor_examples.ex20_singleton_lease serve 18105
 */
package cactor_examples.ex20_singleton_lease

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.dsl.AskInbox
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_foundation.coordination.LeaseSettings
import cactor_persistence.FileLease
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterSingleton, ClusterSingletonSettings}

let NAME = "ex20-ledger"
let ASK = Duration.second * 10
let CLUSTER_WAIT = Duration.second * 30

/** 单例实例：计数；回复 "addr|count" */
class Ledger <: Actor {
    let node: String
    var count: Int64 = 0

    init(node: String) {
        this.node = node
    }

    public prop name: String { get() { "ledger" } }
    public prop description: String { get() { "lease protected singleton" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            match (s.getContent()) {
                case "inc" =>
                    count++
                    context.reply(StringMessage("${node}|${count}"))
                case "status" => context.reply(StringMessage("${node}|${count}"))
                case _ => context.reply(StringMessage("${node}|0"))
            }
        }
        MessageResult.Handled
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let singleton: ClusterSingleton
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, leaseDir: String) {
        this.index = index
        this.system = CActorSystemFactory.create("ex20-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        let self = cluster.selfAddress
        // 同 leaseName、不同 ownerName；ttl 600ms 让故障接管在数秒内可观测
        let lease = FileLease(leaseDir,
            LeaseSettings(NAME, self, Duration.millisecond * 600, Duration.millisecond * 100,
                Duration.millisecond * 100))
        let singletonSettings = ClusterSingletonSettings()
            .withLease(lease)
            .withLeaseRetry(Duration.millisecond * 100, Duration.millisecond * 300)
        this.singleton = ClusterSingleton(cluster, system, remote, NAME, { => Ledger(self) }, singletonSettings)
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

func textOf(m: Message): String {
    match (m as StringMessage) {
        case Some(s) => s.getContent()
        case None => "?"
    }
}

func ask(n: Node, cmd: String): String {
    textOf(n.singleton.ask(StringMessage(cmd), ASK).get(ASK))
}

func aliveInstances(nodes: ArrayList<Node>, skip: Int64): Int64 {
    var n: Int64 = 0
    for (i in 0..nodes.size) {
        if (i != skip && nodes[i].singleton.isRunningHere()) {
            n++
        }
    }
    n
}

func survivors(nodes: ArrayList<Node>, owner: Int64): String {
    var addr = ""
    for (i in 0..nodes.size) {
        if (i != owner && nodes[i].singleton.isRunningHere()) {
            addr = nodes[i].cluster.selfAddress
        }
    }
    addr
}

func buildApp(node: Node, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/status", { ctx =>
        let owner = match (node.singleton.location()) {
            case Some(a) => a
            case None => ""
        }
        let report = ask(node, "status")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"owner\":\"${owner}\",\"report\":\"${report}\"}")
    })
    app.post("/inc", { ctx =>
        let report = ask(node, "inc")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"report\":\"${report}\"}")
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
    let dir = "/tmp/cactor-ex20-${MonoTime.now().hashCode()}"
    let leaseDir = "${dir}/lease"
    let nodeCount = 3
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.millisecond * 800, Some(Duration.second * 2))
    let nodes = ArrayList<Node>()
    var seed: Option<NetworkAddress> = None<NetworkAddress>
    for (i in 0..nodeCount) {
        let n = Node(i, seed, settings, leaseDir)
        seed = Some(n.remote.getLocalAddress())
        nodes.add(n)
    }

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(nodes[0], port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex20：端口绑定失败")
            return 1
        }
        println("ex20 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
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

    // ---- 1. 只有一个实例 ----
    println("\n[1] 三节点中恰好一个实例")
    let one = waitUntil(CLUSTER_WAIT, { => aliveInstances(nodes, -1) == 1 })
    c.check(one, "运行实例数 == 1（实际 ${aliveInstances(nodes, -1)}）")
    let first = ask(nodes[0], "inc")
    let second = ask(nodes[1], "inc")
    let third = ask(nodes[2], "inc")
    c.check(first.split("|")[0] == second.split("|")[0] && second.split("|")[0] == third.split("|")[0],
        "三个节点都打到同一个实例：${first} / ${second} / ${third}")
    c.check(third.split("|")[1] == "3", "计数连续递增到 3（${third}）")
    var found: Int64 = -1
    for (i in 0..nodeCount) {
        if (nodes[i].singleton.isRunningHere()) {
            found = i
        }
    }
    let ownerIdx = found
    c.check(ownerIdx >= 0, "定位到持有者节点 ${ownerIdx}")

    // ---- 2. 持有者宕机 → 接管 ----
    println("\n[2] 持有者被 down 并关闭 → 另一个节点接管")
    for (i in 0..nodeCount) {
        if (i != ownerIdx) {
            nodes[i].cluster.down(nodes[ownerIdx].cluster.selfAddress)
        }
    }
    nodes[ownerIdx].shutdown()
    let takenOver = waitUntil(CLUSTER_WAIT, { => aliveInstances(nodes, ownerIdx) == 1 })
    c.check(takenOver, "幸存节点接管（存活实例数 ${aliveInstances(nodes, ownerIdx)}）")
    let newOwner = survivors(nodes, ownerIdx)
    c.check(!newOwner.isEmpty(), "新持有者地址：${newOwner}")

    // ---- 3. 任意时刻至多一个 ----
    println("\n[3] 接管后持续采样：至多一个实例")
    var peak: Int64 = 0
    for (_ in 0..50) {
        peak = max(peak, aliveInstances(nodes, ownerIdx))
        sleep(Duration.millisecond * 20)
    }
    c.check(peak == 1, "采样期间实例数峰值为 ${peak}（应为 1）")

    // ---- 4. 接管后可用 ----
    println("\n[4] 接管后命令仍可用")
    let after = ask(nodes[(ownerIdx + 1) % nodeCount], "inc")
    c.check(after.endsWith("|1"), "新实例从 1 开始计数（${after}）")
    let status = ask(nodes[(ownerIdx + 1) % nodeCount], "status")
    c.check(status.endsWith("|1"), "状态查询一致（${status}）")

    // ---- 5. Lou ----
    println("\n[5] Lou /status")
    let httpIdx = if (ownerIdx == 0) { 1 } else { 0 }
    let app = buildApp(nodes[httpIdx], 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
    let body = readBody(client.get("http://127.0.0.1:${port}/status"))
    c.check(body.contains(newOwner), "状态里的 owner 是新持有者（${body}）")
    let inc = client.post("http://127.0.0.1:${port}/inc", "")
    c.check(inc.status == 200 && readBody(inc).contains("|2"), "POST /inc 打到同一实例（计数 2）")
    client.close()
    app.stop()

    for (n in nodes) {
        n.shutdown()
    }

    println("\nex20：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
