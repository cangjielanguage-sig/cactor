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
 *   4. 打印 READY_TO_RESTART，由脚本在原端口重启被 kill 的进程（新进程 = 新启动标识）；校验新实例 10 s 内
 *      重新 Up、不继承旧实例的身份（uid 不同），remember-entities 记录的实体 ID 一个不少（E7′ / N4）；
 *   5. 离开集群；其他节点看到 driver 被移除后也离开。
 * 退出码：0 通过，1 失败或超时。
 */
package cactor_examples.cluster_crash_node

import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import std.env.{getVariable}
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager, WireProtocolSettings}
import cactor_cluster.membership.*
import cactor_cluster.ddata.{Replicator}
import cactor_foundation.coordination.{LeaseSettings}
import cactor_persistence.{FileLease, FileFencingTokenStore, FileDurableStateStore, FencedDurableStateStore}

let ENTITIES = 30
let SHARDS = 32
// 分片类型名（LEASE_TYPE）：启用租约时每分片一个 FileLease（名字 shard-tracked-<n>）
let LEASE_TYPE = "tracked"

class TrackedEntity <: Actor {
    let id: String
    let node: String
    let remote: SimpleRemoteManager
    let driver: NetworkAddress
    // CACTOR_LEASE_DIR 设置时（DA8 混合版本 soak），实体写入经分片 fencing token 保护：
    // 分片租约（DA4）+ FencedDurableStateStore，kill -9 后新实例拿更大 token，旧 token 写入被拒绝
    let store: Option<FencedDurableStateStore>

    init(id: String, node: String, remote: SimpleRemoteManager, driver: NetworkAddress,
        store: Option<FencedDurableStateStore>) {
        this.id = id
        this.node = node
        this.remote = remote
        this.driver = driver
        this.store = store
    }

    public prop name: String { get() { "tracked" } }
    public prop description: String { get() { "tracked" } }

    public func preStart(): Unit {
        // 启动即写一条（首次 rev=1，此后每次启动 rev+1）：脚本据此断言重启后计数完整、fencing 生效
        match (store) {
            case Some(s) =>
                try {
                    s.upsert(id, s.revisionOf(id) + 1, "started@${node}")
                } catch (e: Exception) {
                    // 旧 token 的写入被拒（StaleFencingTokenException）：记到 stderr，让脚本知道 fencing 挡住了谁
                    eprintln("FENCE_REJECT ${id} ${e}")
                }
            case None => ()
        }
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

func totalStarts(starts: Starts): Int64 {
    synchronized(starts.lock) {
        var n: Int64 = 0
        for ((_, l) in starts.hosts) {
            n += l.size
        }
        n
    }
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
    // 混合版本演练（DA6 / DA8）：CACTOR_WIRE_LEGACY=1 时本节点按引入协商之前的 v1.2.x 行为过线
    // （握手不带能力扩展、收发都按旧编码）。只用于 tools/mixed_version_soak.sh 的升级演练，生产节点不要打开。
    if (getVariable("CACTOR_WIRE_LEGACY").isSome()) {
        remote.configureWire(WireProtocolSettings.legacyEmulation())
    }
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
    // CACTOR_LEASE_DIR：DA8 混合版本 soak 使用的共享租约目录。设置后分片托管经 DA4 租约许可、
    // 实体写入经 fencing token 保护——这是对"kill -9 + 原端口重启 + 混合版本"组合场景的验证。
    // 每个实体一个 fencing 存储（内层 FileDurableStateStore / FileFencingTokenStore 按实体分目录，
    // 避免多个实体竞争同一 resource 的修订号）；token 来自守护该实体的分片租约。
    let leaseDir = getVariable("CACTOR_LEASE_DIR")
    let shardLeases = match (leaseDir) {
        case Some(dir) =>
            let addr = "${me}#${cluster.selfUniqueAddress()}"
            Some(ShardLeases({ name => FileLease(dir, LeaseSettings(name, addr)) }))
        case None => None<ShardLeases>
    }
    let leasesRef = shardLeases
    var shardingSettings = ShardingSettings.remembering(rep).withRebalance(RebalancePolicy.disabled())
    match (shardLeases) {
        case Some(leases) => shardingSettings = shardingSettings.withLease(leases)
        case None => ()
    }
    let sharding = ClusterSharding(cluster, system, remote, LEASE_TYPE, SHARDS,
        { id =>
            var store: Option<FencedDurableStateStore> = None
            match (leasesRef) {
                case Some(leases) =>
                    let dir = leaseDir.getOrThrow()
                    store = Some(FencedDurableStateStore(FileDurableStateStore("${dir}/state/${id}"),
                        FileFencingTokenStore("${dir}/fence/${id}"),
                        leases.leaseForEntity(LEASE_TYPE, SHARDS, id)))
                case None => ()
            }
            TrackedEntity(id, me, remote, seed, store)
        },
        shardingSettings)

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
    println("[${me}] 集群形成：${ok}，成员 ${cluster.state().upCount()}")

    if (isDriver && ok) {
        for (e in 0..ENTITIES) {
            sharding.tell("t-${e}", StringMessage("hello"))
        }
        ok = waitUntil(Duration.second * 30, { => synchronized(starts.lock) { starts.hosts.size } == ENTITIES }) &&
            waitUntil(Duration.second * 30, { => rememberedCount(rep) == ENTITIES })
        // 多等几个 gossip 周期，让实体 ID 也复制到另一个存活节点
        sleep(Duration.second)
        // 形成期间各节点的成员视图可能还没一致（driver 看到全部 Up 就开始发消息），分片会按收敛后的归属移交一次——
        // 这是正常迁移（先停后启），不是宕机引起的重启。等启动事件静止 2 s 再记下 kill 前的基线
        let quiet = waitUntil(Duration.second * 30, { =>
            let n0 = totalStarts(starts)
            sleep(Duration.second * 2)
            totalStarts(starts) == n0
        })
        ok = ok && quiet
        let victim = "127.0.0.1:${Int64(port) + expected - 1}"
        let onVictim = ArrayList<String>()
        let baseline = HashMap<String, Int64>()
        var movedBeforeKill = 0
        synchronized(starts.lock) {
            for ((id, l) in starts.hosts) {
                baseline[id] = l.size
                if (l.size > 1) {
                    movedBeforeKill++
                }
                if (l[l.size - 1] == victim) {
                    onVictim.add(id)
                }
            }
        }
        println("[${me}] 实体全部启动并已复制：${ok}；宕机进程 ${victim} 上有 ${onVictim.size} 个实体（形成期间迁移 ${movedBeforeKill} 个）")
        ok = ok && onVictim.size > 0
        let victimUid = match (cluster.state().member(victim)) {
            case Some(m) => m.uid
            case None => 0u64
        }
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
                    // 恰好在存活进程上重启一次
                    all = all && l.size == baseline[id] + 1 && l[l.size - 1] != victim
                }
                all
            }
        })
        println("[${me}] 宕机进程上的 ${onVictim.size} 个实体在存活进程上自动重启：${restarted}")
        // 存活进程上的实体没有因为宕机被重复启动（只看 kill 之后新增的启动）
        var duplicates = 0
        let victimSet = HashSet<String>(onVictim)
        synchronized(starts.lock) {
            for ((id, l) in starts.hosts) {
                if (!victimSet.contains(id) && l.size > baseline.get(id).getOrDefault({ => 0 })) {
                    duplicates++
                    println("[${me}]   重复启动 ${id}：${String.join(l.toArray(), delimiter: " -> ")}")
                }
            }
        }
        println("[${me}] 存活进程上的实体被重复启动：${duplicates} 个")
        ok = ok && downed && restarted && duplicates == 0
        // 原端口重启：新实例重新 Up，且是新成员（uid 不同于被 kill 的旧实例）
        eprintln("READY_TO_RESTART")
        let restartBegin = MonoTime.now()
        let rejoined = waitUntil(Duration.second * 10, { =>
            let s = cluster.state()
            match (s.member(victim)) {
                case Some(m) => m.status == MemberStatus.Up && m.uid != victimUid && s.upCount() == expected && s.converged
                case None => false
            }
        })
        println("[${me}] 原端口重启的进程以新实例重新加入：${rejoined}（${if (rejoined) { "用时 ${(MonoTime.now() - restartBegin).toMilliseconds()} ms" } else { "10 s 内未完成" }}）")
        // 新实例加入后分片重新分配，实体 ID 的复制记录不能丢（ORSet 的节点键是"地址#uid"，新进程的 add 不会被旧进程的点吞掉）
        let kept = waitUntil(Duration.second * 30, { => rememberedCount(rep) == ENTITIES })
        println("[${me}] 重启后 remember-entities 记录完整：${kept}（${rememberedCount(rep)}/${ENTITIES}）")
        ok = ok && rejoined && kept
        println(if (ok) { "多进程宕机与重启验证：通过" } else { "多进程宕机与重启验证：失败" })
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
