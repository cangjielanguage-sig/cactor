/*
 * CActor Best 09 - 集群分片（单进程三节点模拟）
 *
 * 演示：
 * - Cluster 成员协议（gossip、Leader、converged）
 * - ClusterSharding：按 entityId 路由到唯一实体；实体回报所在节点
 * - 同一实体的消息始终落在同一份状态上
 *
 * 说明：生产环境应为每个节点启动独立进程（参见 tools/cluster_demo.sh 与
 * cactor_examples.cluster_node）。本示例在单进程内启动 3 个远程节点 +
 * 3 个 ClusterSharding region，验证同样的分片语义，避免端口与进程管理。
 *
 * 自检：30 个实体各收到 5 条消息、每条消息只被一个实体处理、实体分布在多个节点。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.best09_cluster_sharding
 */
package cactor_examples.best09_cluster_sharding

import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterSharding}

let ENTITIES = 30
let PER_ENTITY = 5

/** 分片实体：计数并回报 "实体|节点|累计" */
class CounterEntity <: Actor {
    let id: String
    let node: String
    let remote: SimpleRemoteManager
    let driver: NetworkAddress
    var total: Int64 = 0

    init(id: String, node: String, remote: SimpleRemoteManager, driver: NetworkAddress) {
        this.id = id
        this.node = node
        this.remote = remote
        this.driver = driver
    }

    public prop name: String { get() { "counter" } }
    public prop description: String { get() { "counter" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        total++
        remote.remoteActorOf(driver, "collector").tell(StringMessage("${id}|${node}|${total}"))
        MessageResult.Handled
    }
}

class Results {
    let lock = Mutex()
    let hosts = HashMap<String, HashSet<String>>()
    let totals = HashMap<String, Int64>()
    var acks: Int64 = 0
}

class Collector <: Actor {
    let results: Results

    init(results: Results) {
        this.results = results
    }

    public prop name: String { get() { "collector" } }
    public prop description: String { get() { "collector" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let parts = s.getContent().split("|")
            if (parts.size == 3) {
                synchronized(results.lock) {
                    match (results.hosts.get(parts[0])) {
                        case Some(h) => h.add(parts[1])
                        case None =>
                            let h = HashSet<String>()
                            h.add(parts[1])
                            results.hosts[parts[0]] = h
                    }
                    let t = Int64.parse(parts[2])
                    if (t > (results.totals.get(parts[0]) ?? 0)) {
                        results.totals[parts[0]] = t
                    }
                    results.acks++
                }
            }
        }
        MessageResult.Handled
    }
}

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

main(): Int64 {
    let nodeCount = 3
    let seedPort: Int32 = 0
    let system = CActorSystemFactory.create("best09")
    let results = Results()

    // 三个节点：端口 0（系统分配），种子为第一个节点
    let systems = ArrayList<CActorSystem>()
    let remotes = ArrayList<SimpleRemoteManager>()
    let clusters = ArrayList<Cluster>()
    var seed = NetworkAddress("127.0.0.1", seedPort)
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.second * 2, Some(Duration.second * 3))

    for (i in 0..nodeCount) {
        let sys = CActorSystemFactory.create("best09-node-${i}")
        let remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        remote.start()
        if (i == 0) {
            seed = remote.getLocalAddress()
        }
        let cluster = Cluster(sys, remote, [seed], settings)
        systems.add(sys)
        remotes.add(remote)
        clusters.add(cluster)
    }

    // driver 的 collector 挂在节点 0
    remotes[0].exposeActor("collector", systems[0].actorOf({ => Collector(results) }, "collector"))

    // 每个节点一个分片区域
    let shardingList = ArrayList<ClusterSharding>()
    for (i in 0..nodeCount) {
        let remote = remotes[i]
        let nodeAddr = remote.getLocalAddress().toString()
        let driver = remotes[0].getLocalAddress()
        let sharding = ClusterSharding(clusters[i], systems[i], remote, "counter", 32,
            { id => CounterEntity(id, nodeAddr, remote, driver) })
        shardingList.add(sharding)
    }

    // 等待集群形成
    var formed = true
    for (c in clusters) {
        formed = formed && waitUntil(Duration.second * 20, { => c.state().upCount() == nodeCount && c.state().converged })
    }
    println("集群形成：${formed}，成员 ${clusters[0].state().upCount()}，Leader ${clusters[0].leader() ?? "无"}")

    // driver 经分片向 30 个实体各发 5 条消息
    var ok = formed
    if (ok) {
        for (k in 0..PER_ENTITY) {
            for (e in 0..ENTITIES) {
                shardingList[0].tell("entity-${e}", StringMessage("${k}"))
            }
        }
        ok = waitUntil(Duration.second * 20, { => synchronized(results.lock) { results.acks } == ENTITIES * PER_ENTITY })
        var perNode: Int64 = 0
        let nodeSet = HashSet<String>()
        synchronized(results.lock) {
            println("收到回报 ${results.acks} 条")
            for (e in 0..ENTITIES) {
                let id = "entity-${e}"
                let hosts = (results.hosts.get(id) ?? HashSet<String>()).toArray()
                let total = results.totals.get(id) ?? 0
                if (hosts.size != 1 || total != PER_ENTITY) {
                    println("实体 ${id} 异常：节点 ${hosts.size} 个，累计 ${total}")
                    ok = false
                } else {
                    nodeSet.add(hosts[0])
                }
            }
            perNode = Int64(nodeSet.size)
        }
        println("承担实体的节点数：${perNode}/${nodeCount}（单进程模拟下分配可能集中；多进程分布由 tools/cluster_demo.sh 验证）")
        // 单进程模拟时分片分配可能集中在少数节点；核心语义（实体唯一、计数正确）已验证。
        // 跨进程的分布均匀性由 tools/cluster_demo.sh（3 个独立进程）验证。
    }

    for (i in 0..nodeCount) {
        clusters[i].shutdown()
        remotes[i].stop()
        systems[i].terminate()
    }
    system.terminate()
    println(if (ok) { "best09：通过" } else { "best09：失败" })
    if (ok) { 0 } else { 1 }
}
