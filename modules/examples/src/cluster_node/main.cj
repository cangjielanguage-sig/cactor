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
import std.env.{getVariable}
import cactor.api.{CActorSystemFactory, ManagementServer}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol, TcpTransport, BlackholeTransport}
import cactor_remote.{SimpleRemoteManager, RemotingSettings, RemoteTls}
import cactor_cluster.membership.*
import cactor_cluster.ops.*

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
    let mode = if (args.size > 3) { args[3] } else { "" }
    let isDriver = mode == "driver"
    // hold：只起节点等外部驱动（tools/cluster_chaos.sh 用它做进程级故障注入），校验由脚本轮询端点完成
    let isHold = mode == "hold"
    // CACTOR_MGMT_PORT：管理端点端口，设置后挂 /cluster/status、/cluster/shards 与故障注入端点
    let mgmtPort = match (getVariable("CACTOR_MGMT_PORT")) {
        case Some(v) => UInt16.parse(v)
        case None => UInt16(0)
    }

    let system = CActorSystemFactory.create("node-${port}")
    // 传输包一层黑洞注入：管理端点可以在运行期切换"丢掉发往某地址的全部出站帧"
    let transport = BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", port)))
    let remote = SimpleRemoteManager(transport)
    // CACTOR_TLS_DIR：含 node.pem / node-key.pem / ca.pem 的目录，设置后全部集群流量走 TLS 1.3 双向认证（E65）
    let tlsDir = getVariable("CACTOR_TLS_DIR")
    if (let Some(dir) <- tlsDir) {
        remote.configure(RemotingSettings.defaults().withTls(
            RemoteTls.fromPemFiles("${dir}/node.pem", "${dir}/node-key.pem", "${dir}/ca.pem")))
    }
    remote.start()
    // CACTOR_NO_AUTODOWN：只观察可达性、不自动 Down（故障注入验证要用——Down 掉就再也回不来）
    let autoDown = if (getVariable("CACTOR_NO_AUTODOWN").isSome()) {
        None<Duration>
    } else {
        Some(Duration.second * 3)
    }
    let settings = ClusterSettings(Duration.millisecond * 200, Duration.millisecond * 200, 8.0, Duration.second * 2,
        autoDown)
    let cluster = Cluster(system, remote, [seed], settings)
    let me = cluster.selfAddress
    let results = Results()
    if (isDriver) {
        remote.exposeActor("collector", system.actorOf({ => Collector(results) }, "collector"))
    }
    let sharding = ClusterSharding(cluster, system, remote, "counter", 32, { id => CounterEntity(id, me, remote, seed) })
    if (mgmtPort > 0) {
        let mgmt = ManagementServer(system, "127.0.0.1", mgmtPort)
        attachClusterRoutes(mgmt, cluster, sharding: Some(sharding))
        attachFaultInjectionRoutes(mgmt, transport)
        mgmt.start()
        println("[${me}] 管理端点 127.0.0.1:${mgmt.port()}（/cluster/status、/cluster/shards、/cluster/blackhole、/cluster/heal）")
    }

    // "集群已形成" = 曾看到预期数量的成员都 Up。不要求轮询的那一刻本地视图恰好收敛：分片路由不等收敛（E13′），
    // driver 可能在本节点自己观察到收敛之前就完成校验并离开
    let ups = HashSet<String>()
    let upsLock = Mutex()
    let formed = AtomicBool(false)
    cluster.subscribe({ e =>
        match (e) {
            case MemberUp(m) =>
                synchronized(upsLock) {
                    ups.add(m.address)
                    if (ups.size >= expected) {
                        formed.store(true)
                    }
                }
            case _ => ()
        }
    })
    var ok = waitUntil(Duration.second * 30, { => formed.load() || cluster.state().upCount() == expected })
    println("[${me}] 集群形成：${ok}，成员 ${cluster.state().upCount()}，Leader ${cluster.leader() ?? "无"}")

    if (isHold) {
        // 脚本通过端点驱动；进程只负责保持存活，收到 SIGTERM 退出
        println("[${me}] hold 模式：等待外部驱动")
        while (true) {
            sleep(Duration.millisecond * 200)
        }
    }
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
        if (tlsDir.isSome()) {
            // 全部连接都经 TLS 建立、没有被拒绝的连接
            let secured = remote.securedConnections()
            let rejected = remote.rejectedConnections()
            println("[${me}] TLS 连接 ${secured} 条，被拒绝 ${rejected} 条")
            ok = ok && secured > 0 && rejected == 0
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
