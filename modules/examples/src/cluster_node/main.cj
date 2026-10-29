/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 多进程集群验证节点（配合 tools/cluster_demo.sh 使用）
 *
 *   cactor_examples.cluster_node <端口> <种子 host:port> <节点总数> [driver]
 *
 * 每个进程是一个独立的集群节点：加入集群、启动分片区域 "counter"。
 * driver 节点（种子）在全部节点 Up 后，经分片向 30 个实体各发 10 条消息；
 * 实体每收到一条就把 "实体|节点|累计" 回报给 driver 的 collector。driver 校验：
 *   - 每个实体只在一个进程上运行，累计值恰好为 10；
 *   - 每个进程都承担了一部分实体。
 * 校验结束后 driver 离开集群；其他节点看到 driver 被移除后也离开并退出。
 * 退出码：0 通过，1 失败或超时。
 */
package cactor_examples.cluster_node

import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.*

let ENTITIES = 30
let PER_ENTITY = 10

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
    while (!cond()) {
        if (MonoTime.now() > deadline) {
            return false
        }
        sleep(Duration.millisecond * 20)
    }
    true
}

main(args: Array<String>): Int64 {
    if (args.size < 3) {
        eprintln("用法: cluster_node <端口> <种子 host:port> <节点总数> [driver]")
        return 2
    }
    let port = Int32.parse(args[0])
    let seed = NetworkAddress.parse(args[1])
    let expected = Int64.parse(args[2])
    let isDriver = args.size > 3 && args[3] == "driver"

    let system = CActorSystemFactory.create("node-${port}")
    let remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", port), TransportProtocol.TCP)
    remote.start()
    let settings = ClusterSettings(Duration.millisecond * 200, Duration.millisecond * 200, 8.0, Duration.second * 2,
        Some(Duration.second * 3))
    let cluster = Cluster(system, remote, [seed], settings)
    let me = cluster.selfAddress
    let results = Results()
    if (isDriver) {
        remote.exposeActor("collector", system.actorOf({ => Collector(results) }, "collector"))
    }
    let sharding = ClusterSharding(cluster, system, remote, "counter", 32, { id => CounterEntity(id, me, remote, seed) })

    var ok = waitUntil(Duration.second * 30, { => cluster.state().upCount() == expected && cluster.state().converged })
    println("[${me}] 集群形成：${ok}，成员 ${cluster.state().upCount()}，Leader ${cluster.leader() ?? "无"}")

    if (isDriver && ok) {
        for (k in 0..PER_ENTITY) {
            for (e in 0..ENTITIES) {
                sharding.tell("entity-${e}", StringMessage("${k}"))
            }
        }
        ok = waitUntil(Duration.second * 30, { => synchronized(results.lock) { results.acks } == ENTITIES * PER_ENTITY })
        let perNode = HashMap<String, Int64>()
        synchronized(results.lock) {
            println("[${me}] 收到回报 ${results.acks} 条")
            for (e in 0..ENTITIES) {
                let id = "entity-${e}"
                let hosts = (results.hosts.get(id) ?? HashSet<String>()).toArray()
                let total = results.totals.get(id) ?? 0
                if (hosts.size != 1 || total != PER_ENTITY) {
                    println("[${me}] 实体 ${id} 异常：节点 ${hosts.size} 个，累计 ${total}")
                    ok = false
                } else {
                    perNode[hosts[0]] = (perNode.get(hosts[0]) ?? 0) + 1
                }
            }
        }
        for ((n, c) in perNode) {
            println("[${me}] 进程 ${n} 承担 ${c} 个实体")
        }
        if (perNode.size != expected) {
            println("[${me}] 只有 ${perNode.size} 个进程承担了实体")
            ok = false
        }
        println(if (ok) { "多进程集群验证：通过" } else { "多进程集群验证：失败" })
        cluster.leave()
        waitUntil(Duration.second * 10, { => cluster.isTerminated() })
    } else if (ok) {
        // 等 driver 离开后自己也离开
        ok = waitUntil(Duration.second * 60, { => cluster.state().member(seed.toString()).isNone() })
        cluster.leave()
        ok = ok && waitUntil(Duration.second * 10, { => cluster.isTerminated() })
        println("[${me}] 实体数 ${sharding.localEntityCount()}，已离开集群：${cluster.isTerminated()}")
    }
    sharding.stop()
    remote.stop()
    system.terminate()
    if (ok) { 0 } else { 1 }
}
