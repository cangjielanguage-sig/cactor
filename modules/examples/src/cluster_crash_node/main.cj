/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 多进程宕机验证节点（配合 tools/cluster_crash_demo.sh 使用）
 *
 *   cactor_examples.cluster_crash_node <端口> <种子 host:port> <节点总数> <driver|worker>
 *
 * 集群使用 KeepMajority 脑裂处理，分片开启 remember-entities（实体 ID 经 Replicator 复制）。
 * 实体启动（preStart）时把 "started|实体|节点" 报给 driver 的 collector。
 * driver（种子）：
 *   1. 全部节点 Up 后向 30 个实体各发一条消息，等全部实体启动并且实体 ID 已复制到本节点；
 *   2. 打印 READY_TO_KILL，由脚本 kill -9 最后一个进程；
 *   3. 校验：SBR 把宕机进程 down 并移出集群（剩 2 个成员）；宕机进程上的实体
 *      在没有任何新消息的情况下，在存活进程上重新启动，且每个实体只在一个存活进程上；
 *   4. 离开集群；另一个存活节点看到 driver 被移除后也离开。
 * 退出码：0 通过，1 失败或超时。
 */
package cactor_examples.cluster_crash_node

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
import cactor_cluster.ddata.{Replicator}

let ENTITIES = 30
let SHARDS = 32

class TrackedEntity <: Actor {
    let id: String
    let node: String
    let remote: SimpleRemoteManager
    let driver: NetworkAddress

    init(id: String, node: String, remote: SimpleRemoteManager, driver: NetworkAddress) {
        this.id = id
        this.node = node
        this.remote = remote
        this.driver = driver
    }

    public prop name: String { get() { "tracked" } }
    public prop description: String { get() { "tracked" } }

    public func preStart(): Unit {
        try {
            remote.remoteActorOf(driver, "collector").tell(StringMessage("started|${id}|${node}"))
        } catch (_: Exception) {}
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

class Starts {
    let lock = Mutex()
    // 实体 -> 启动过的节点（按顺序）
    let hosts = HashMap<String, ArrayList<String>>()
}

class Collector <: Actor {
    let starts: Starts

    init(starts: Starts) {
        this.starts = starts
    }

    public prop name: String { get() { "collector" } }
    public prop description: String { get() { "collector" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let parts = s.getContent().split("|")
            if (parts.size == 3 && parts[0] == "started") {
                synchronized(starts.lock) {
                    match (starts.hosts.get(parts[1])) {
                        case Some(l) => l.add(parts[2])
                        case None =>
                            let l = ArrayList<String>()
                            l.add(parts[2])
                            starts.hosts[parts[1]] = l
                    }
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

func rememberedCount(rep: Replicator): Int64 {
    var n: Int64 = 0
    for (s in 0..SHARDS) {
        n += rep.setElements(rememberedEntitiesKey("tracked", s)).size
    }
    n
}

main(args: Array<String>): Int64 {
    if (args.size < 4) {
        eprintln("用法: cluster_crash_node <端口> <种子 host:port> <节点总数> <driver|worker>")
        return 2
    }
    let port = Int32.parse(args[0])
    let seed = NetworkAddress.parse(args[1])
    let expected = Int64.parse(args[2])
    let isDriver = args[3] == "driver"

    let system = CActorSystemFactory.create("crash-${port}")
    let remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", port), TransportProtocol.TCP)
    remote.start()
    let settings = ClusterSettings(Duration.millisecond * 200, Duration.millisecond * 200, 8.0, Duration.second * 2,
        DowningStrategy.KeepMajority(Duration.second * 2))
    let cluster = Cluster(system, remote, [seed], settings)
    let me = cluster.selfAddress
    let starts = Starts()
    if (isDriver) {
        remote.exposeActor("collector", system.actorOf({ => Collector(starts) }, "collector"))
    }
    let rep = Replicator(system, remote, Duration.millisecond * 200)
    let sharding = ClusterSharding(cluster, system, remote, "tracked", SHARDS, { id => TrackedEntity(id, me, remote, seed) },
        ShardingSettings.remembering(rep))

    var ok = waitUntil(Duration.second * 30, { => cluster.state().upCount() == expected && cluster.state().converged })
    println("[${me}] 集群形成：${ok}，成员 ${cluster.state().upCount()}")

    if (isDriver && ok) {
        for (e in 0..ENTITIES) {
            sharding.tell("t-${e}", StringMessage("hello"))
        }
        ok = waitUntil(Duration.second * 30, { => synchronized(starts.lock) { starts.hosts.size } == ENTITIES }) &&
            waitUntil(Duration.second * 30, { => rememberedCount(rep) == ENTITIES })
        // 多等几个 gossip 周期，让实体 ID 也复制到另一个存活节点
        sleep(Duration.second)
        let victim = "127.0.0.1:${Int64(port) + expected - 1}"
        let onVictim = ArrayList<String>()
        synchronized(starts.lock) {
            for ((id, l) in starts.hosts) {
                if (l[0] == victim) {
                    onVictim.add(id)
                }
            }
        }
        println("[${me}] 实体全部启动并已复制：${ok}；宕机进程 ${victim} 上有 ${onVictim.size} 个实体")
        ok = ok && onVictim.size > 0
        // stderr 不缓冲：脚本据此立即 kill -9（stdout 重定向到文件时会缓冲到退出）
        eprintln("READY_TO_KILL")
        // SBR：宕机进程被 down 并移出，剩 2 个成员且收敛
        let downed = waitUntil(Duration.second * 60, { =>
            let s = cluster.state()
            s.member(victim).isNone() && s.upCount() == expected - 1 && s.converged
        })
        println("[${me}] 宕机进程被脑裂处理移出集群：${downed}")
        // remember-entities：宕机进程上的实体在存活进程上自动重启（没有发送任何新消息）
        let restarted = waitUntil(Duration.second * 30, { =>
            synchronized(starts.lock) {
                var all = true
                for (id in onVictim) {
                    let l = starts.hosts[id]
                    all = all && l.size == 2 && l[1] != victim
                }
                all
            }
        })
        println("[${me}] 宕机进程上的 ${onVictim.size} 个实体在存活进程上自动重启：${restarted}")
        // 其他实体没有被重复启动
        var duplicates = 0
        synchronized(starts.lock) {
            for ((id, l) in starts.hosts) {
                if (l.size > 1 && l[0] != victim) {
                    duplicates++
                }
            }
        }
        println("[${me}] 存活进程上的实体被重复启动：${duplicates} 个")
        ok = ok && downed && restarted && duplicates == 0
        println(if (ok) { "多进程宕机验证：通过" } else { "多进程宕机验证：失败" })
        cluster.leave()
        waitUntil(Duration.second * 10, { => cluster.isTerminated() })
    } else if (ok) {
        // 被 kill -9 的进程不会走到这里之后；存活的 worker 等 driver 离开后自己也离开
        ok = waitUntil(Duration.second * 120, { => cluster.state().member(seed.toString()).isNone() })
        cluster.leave()
        ok = ok && waitUntil(Duration.second * 10, { => cluster.isTerminated() })
        println("[${me}] 实体数 ${sharding.localEntityCount()}，已离开集群：${cluster.isTerminated()}")
    }
    sharding.stop()
    rep.stop()
    remote.stop()
    system.terminate()
    if (ok) { 0 } else { 1 }
}
