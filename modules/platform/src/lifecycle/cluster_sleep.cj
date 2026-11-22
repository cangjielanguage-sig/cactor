/*
 * CActor Platform.Lifecycle - 集群内的会话睡/醒与迁移（F1 的节点迁移部分 + P8）
 *
 * 与同目录 sleepable.cj 的区别：
 *   - SleepRegistry 是"单节点"的睡醒注册表：句柄属于某个节点，会话实例也只在该节点创建；
 *   - ClusterSleepRegistry 在此基础上补两件事（对标计划 §2.10 的 F1 扩展）：
 *       ① 归属：一个 key（sessionId）由哪个节点托管，用与分片一致的
 *          `shardOf` + `shardOwner`（rendezvous）在"当前 Up 成员"上计算；
 *          因此归属是**从成员视图推导**的，不落一条会过期的"归属记录"。
 *       ② 迁移：节点 down 后成员视图收敛，同一个 key 的归属自动落到幸存节点；
 *          发往幸存节点的消息在本地按存储里的状态唤醒会话——状态从未丢，只是换了宿主。
 *
 * 状态存储：睡着会话的状态写在 `DurableStateStore`。跨节点迁移要求该存储在节点间可见
 * （测试里同进程共享一个 MemoryDurableStateStore 实例；生产用共享文件系统/数据库实现）。
 * 注册表的工厂 `(key, restoredState) -> Actor` 必须每个节点都相同——否则幸存节点无法重建会话
 * （这与 ClusterSharding 要求各节点提供同一实体工厂是同一条约束）。
 *
 * 投递：`tell` 先算归属；不在本节点则经远程把载荷转发给拥有者的管理器，由它本地唤醒/投递。
 * 跨节点转发只携带载荷（不带发送方 ActorRef）；需要回复时由调用方改用带 sender 的本地路径。
 */
package cactor_platform.lifecycle

import std.collection.*
import std.sync.*
import std.time.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef, PropsFactory, DispatcherMode}
import cactor_core.message.{Message, MessageResult, MessageSerializer, MessageCodec, MessageCodecRegistry}
import cactor_foundation.network.{NetworkAddress}
import cactor_remote.{SimpleRemoteManager}
import cactor_persistence.{DurableStateStore, MemoryDurableStateStore}
import cactor_cluster.membership.{Cluster, MemberStatus, shardOf, shardOwner}

/** 句柄 → 管理器：把消息投递给 key 对应的会话（本地计算归属，必要时转发/唤醒） */
class ClusterDeliver <: Message {
    let key: String
    let payload: Message
    let sender: Option<ActorRef>

    init(key: String, payload: Message, sender: Option<ActorRef>) {
        this.key = key
        this.payload = payload
        this.sender = sender
    }

    public func messageType(): String {
        "ClusterSleepDeliver"
    }
}

/** 跨节点转发：只带 key 与载荷（发送方不跨进程） */
class ClusterForwarded <: Message {
    let key: String
    let payload: Message

    init(key: String, payload: Message) {
        this.key = key
        this.payload = payload
    }

    public func messageType(): String {
        "cactor.ClusterSleepForwarded"
    }
}

/** 跨节点转发帧的编解码：`<key 长度>:<key><内层消息>`，内层复用 MessageSerializer */
class ClusterSleepForwardedCodec <: MessageCodec {
    private let inner = MessageSerializer()

    public func encode(message: Message): Array<UInt8> {
        let f = (message as ClusterForwarded).getOrThrow()
        let keyBytes = f.key.toArray()
        let header = "${keyBytes.size}:".toArray()
        let body = inner.serialize(f.payload)
        let out = Array<UInt8>(header.size + keyBytes.size + body.size, repeat: 0)
        header.copyTo(out, 0, 0, header.size)
        keyBytes.copyTo(out, 0, header.size, keyBytes.size)
        body.copyTo(out, 0, header.size + keyBytes.size, body.size)
        out
    }

    public func decode(payload: Array<UInt8>): Message {
        var pos: Int64 = 0
        let len = readNumber(payload, pos)
        pos = len[1]
        if (len[0] < 0 || pos + len[0] > payload.size) {
            throw IllegalArgumentException("会话转发帧损坏")
        }
        let key = String.fromUtf8(payload[pos..(pos + len[0])])
        pos += len[0]
        ClusterForwarded(key, inner.deserialize(payload[pos..]))
    }

    private static func readNumber(data: Array<UInt8>, start: Int64): Array<Int64> {
        var n: Int64 = 0
        var i = start
        while (i < data.size && data[i] != UInt8(UInt32(r':'))) {
            let d = data[i]
            if (d < UInt8(UInt32(r'0')) || d > UInt8(UInt32(r'9'))) {
                throw IllegalArgumentException("会话转发帧损坏")
            }
            n = n * 10 + Int64(d - UInt8(UInt32(r'0')))
            i++
        }
        if (i >= data.size) {
            throw IllegalArgumentException("会话转发帧损坏")
        }
        [n, i + 1]
    }
}

/** 每个 key 的管理条目：只保留"当前是否有活实例"、实例代数与空闲定时器 key */
class ClusterSessionEntry {
    var active: Option<ActorRef> = None<ActorRef>
    var generation: Int64 = 0
    var pendingSleepGeneration: Int64 = -1
    var timerKey: String
    // 该实例当前宿主节点（诊断/迁移日志用）
    var host: String

    init(timerKey: String, host: String) {
        this.timerKey = timerKey
        this.host = host
    }
}

/** 注册表与管理器共享的诊断计数（管理器是 Actor，注册表读不到它的私有字段） */
class ClusterSleepShared {
    private let lock = Mutex()
    private let activeKeys = HashSet<String>()
    private var wakes: Int64 = 0
    private var forwards: Int64 = 0

    func started(key: String): Unit {
        synchronized(lock) {
            activeKeys.add(key)
            wakes++
        }
    }

    func stopped(key: String): Unit {
        synchronized(lock) {
            activeKeys.remove(key)
        }
    }

    func forwarded(): Unit {
        synchronized(lock) {
            forwards++
        }
    }

    func activeCount(): Int64 {
        synchronized(lock) { activeKeys.size }
    }

    func activeKeysSnapshot(): Array<String> {
        synchronized(lock) { activeKeys.toArray() }
    }

    func stats(): (Int64, Int64, Int64) {
        synchronized(lock) { (activeKeys.size, wakes, forwards) }
    }
}

/** 当前 Up 成员地址（与分片区域一致：只把这些成员作为托管候选） */
func clusterUpMembers(cluster: Cluster): Array<String> {
    let ups = ArrayList<String>()
    for (m in cluster.state().members) {
        if (m.status == MemberStatus.Up) {
            ups.add(m.address)
        }
    }
    ups.toArray()
}

/** key → 归属节点：与 ClusterSharding 同算法（先按 key 分片，再在 Up 成员上 rendezvous） */
func clusterSessionOwner(cluster: Cluster, numberOfShards: Int64, key: String): Option<String> {
    let ups = clusterUpMembers(cluster)
    if (ups.isEmpty()) {
        return None<String>
    }
    shardOwner(shardOf(key, numberOfShards), ups)
}

/**
 * 集群睡醒管理器：每个 ClusterSleepRegistry 一个，每节点一个实例，暴露为 "cluster-sleep-<name>"。
 *
 * 收到投递请求时先算归属：本节点 -> 本地唤醒并投递；其他节点 -> 远程转发给其管理器。
 * 睡眠（空闲/显式）与唤醒语义与单节点 SleepRegistry 完全一致（复用 SleepableActor 的
 * CaptureSleep/SleepStateMsg 协议），因此会话实现不需要为集群另写一套。
 */
class ClusterSleepManagerActor <: Actor {
    private let registryName: String
    private let numberOfShards: Int64
    private let selfAddr: String
    private let managerName: String
    private let cluster: Cluster
    private let remote: SimpleRemoteManager
    private let store: DurableStateStore
    private let policy: SleepPolicy
    private let factory: (String, Option<String>) -> Actor
    private let shared: ClusterSleepShared
    private let sessions = HashMap<String, ClusterSessionEntry>()
    private var entrySeq: Int64 = 0
    private var childSeq: Int64 = 0

    init(registryName: String, numberOfShards: Int64, selfAddr: String, managerName: String,
        cluster: Cluster, remote: SimpleRemoteManager, store: DurableStateStore, policy: SleepPolicy,
        factory: (String, Option<String>) -> Actor, shared: ClusterSleepShared) {
        this.registryName = registryName
        this.numberOfShards = numberOfShards
        this.selfAddr = selfAddr
        this.managerName = managerName
        this.cluster = cluster
        this.remote = remote
        this.store = store
        this.policy = policy
        this.factory = factory
        this.shared = shared
    }

    public prop name: String {
        get() { "cluster-sleep-manager-${registryName}" }
    }

    public prop description: String {
        get() { "Cluster sleepable session manager" }
    }

    public func onStarted(context: ActorContext): Unit {
        // 暴露给其他节点的同名管理器：跨节点转发按 owner 地址发到这个名字
        remote.exposeActor(managerName, context.self)
    }

    public func postStop(): Unit {
        remote.unexposeActor(managerName)
    }

    private func persistenceId(key: String): String {
        "cluster-sleepable/${registryName}/${key}"
    }

    private func entryOf(key: String): ClusterSessionEntry {
        match (sessions.get(key)) {
            case Some(e) => e
            case None =>
                entrySeq++
                let e = ClusterSessionEntry("cluster-sleep-idle-${entrySeq}", selfAddr)
                sessions[key] = e
                e
        }
    }

    private func loadState(key: String): Option<String> {
        match (store.get(persistenceId(key))) {
            case Some(r) => Some(r.state)
            case None => None<String>
        }
    }

    private func saveState(key: String, state: String): Unit {
        let pid = persistenceId(key)
        let revision = store.revisionOf(pid) + 1
        store.upsert(pid, revision, state)
    }

    private func armIdleTimer(context: ActorContext, entry: ClusterSessionEntry, key: String): Unit {
        match (policy) {
            case Idle(d) => context.timers.startSingleTimer(entry.timerKey, IdleTick(key), d)
            case Explicit => ()
        }
    }

    private func cancelIdleTimer(context: ActorContext, entry: ClusterSessionEntry): Unit {
        context.timers.cancel(entry.timerKey)
    }

    /** 按归属决定本地投递还是跨节点转发 */
    private func route(context: ActorContext, key: String, payload: Message, sender: Option<ActorRef>): Unit {
        match (clusterSessionOwner(cluster, numberOfShards, key)) {
            case Some(addr) =>
                if (addr == selfAddr) {
                    deliverLocal(context, key, payload, sender)
                } else {
                    forward(key, addr, payload)
                }
            case None =>
                // 没有可用视图（单节点尚未收敛）：本节点托管
                deliverLocal(context, key, payload, sender)
        }
    }

    private func forward(key: String, addr: String, payload: Message): Unit {
        try {
            let ref = remote.remoteActorOf(NetworkAddress.parse(addr), managerName)
            ref.tell(ClusterForwarded(key, payload))
            shared.forwarded()
        } catch (e: Exception) {
            // 地址解析/发送失败：视图分歧期间可能发生。这里不本地兜底，避免暂态双活；
            // 与分片一致，交给上层重试或死信（消息至多一次）。
            shared.forwarded()
        }
    }

    private func deliverLocal(context: ActorContext, key: String, payload: Message, sender: Option<ActorRef>): Unit {
        let entry = entryOf(key)
        entry.pendingSleepGeneration = -1
        entry.host = selfAddr
        match (entry.active) {
            case Some(child) =>
                match (sender) {
                    case Some(s) => child.tell(payload, s)
                    case None => child.tell(payload)
                }
            case None =>
                let restored = loadState(key)
                childSeq++
                let gen = childSeq
                let name = "cluster-sleep-${gen}"
                let props = PropsFactory.create<Actor>({ => factory(key, restored) })
                    .withDispatcherMode(DispatcherMode.ThreadPerActor)
                let child = context.actorOf(props, name)
                context.watchWith(child, ChildGone(key, gen))
                entry.active = Some(child)
                entry.generation = gen
                shared.started(key)
                match (sender) {
                    case Some(s) => child.tell(payload, s)
                    case None => child.tell(payload)
                }
        }
        armIdleTimer(context, entry, key)
    }

    private func beginSleep(context: ActorContext, key: String): Unit {
        match (sessions.get(key)) {
            case Some(entry) =>
                match (entry.active) {
                    case Some(child) =>
                        if (entry.pendingSleepGeneration < 0) {
                            entry.pendingSleepGeneration = entry.generation
                            child.tell(CaptureSleep(context.self, key, entry.generation))
                        }
                    case None => ()
                }
            case None => ()
        }
    }

    private func finishSleep(context: ActorContext, msg: SleepStateMsg): Unit {
        match (sessions.get(msg.key)) {
            case Some(entry) =>
                if (entry.pendingSleepGeneration != msg.generation) {
                    return
                }
                entry.pendingSleepGeneration = -1
                saveState(msg.key, msg.state)
                cancelIdleTimer(context, entry)
                match (entry.active) {
                    case Some(child) =>
                        entry.active = None<ActorRef>
                        shared.stopped(msg.key)
                        context.stop(child)
                    case None => ()
                }
            case None => ()
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case d: ClusterDeliver =>
                route(context, d.key, d.payload, d.sender)
                MessageResult.Handled
            case f: ClusterForwarded =>
                // 已由拥有者判定归属：直接本地唤醒/投递，避免视图短暂不一致时来回转发
                deliverLocal(context, f.key, f.payload, None<ActorRef>)
                MessageResult.Handled
            case t: IdleTick =>
                beginSleep(context, t.key)
                MessageResult.Handled
            case s: SleepNow =>
                beginSleep(context, s.key)
                MessageResult.Handled
            case r: SleepStateMsg =>
                finishSleep(context, r)
                MessageResult.Handled
            case g: ChildGone =>
                match (sessions.get(g.key)) {
                    case Some(entry) =>
                        match (entry.active) {
                            case Some(_) =>
                                if (entry.generation == g.generation) {
                                    entry.active = None<ActorRef>
                                    entry.pendingSleepGeneration = -1
                                    shared.stopped(g.key)
                                }
                            case None => ()
                        }
                    case None => ()
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

/**
 * 集群睡醒注册表（F1 扩展 + P8 的集群侧）。
 *
 * ```
 * let sleep = ClusterSleepRegistry(system, cluster, remote, "session", { key, restored => MySession(key, restored) })
 * let handle = sleep.handle("session-42")
 * handle.tell(msg)      // 归属节点负责唤醒并投递；节点宕机后由幸存节点接手
 * handle.sleep()
 * ```
 */
public class ClusterSleepRegistry {
    private let system: CActorSystem
    private let cluster: Cluster
    private let remote: SimpleRemoteManager
    private let manager: ActorRef
    private let name: String
    private let numberOfShards: Int64
    private let shared: ClusterSleepShared

    public init(system: CActorSystem, cluster: Cluster, remote: SimpleRemoteManager, name: String,
        factory: (String, Option<String>) -> Actor,
        numberOfShards!: Int64 = 64,
        store!: DurableStateStore = MemoryDurableStateStore(),
        policy!: SleepPolicy = SleepPolicy.Explicit) {
        MessageCodecRegistry.register("cactor.ClusterSleepForwarded", ClusterSleepForwardedCodec())
        let shared = ClusterSleepShared()
        let managerName = "cluster-sleep-${name}"
        let selfAddr = cluster.selfAddress
        let props = PropsFactory.create<Actor>({ =>
            ClusterSleepManagerActor(name, numberOfShards, selfAddr, managerName, cluster, remote, store, policy,
                factory, shared)
        }).withDispatcherMode(DispatcherMode.ThreadPerActor)
        this.system = system
        this.cluster = cluster
        this.remote = remote
        this.name = name
        this.numberOfShards = numberOfShards
        this.shared = shared
        this.manager = system.actorOf(props, managerName)
    }

    /** 取得某个 key（sessionId）的会话句柄；唤醒时用注册表工厂重建会话 */
    public func handle(key: String): ClusterSleepHandle {
        ClusterSleepHandle(this, key, pathOf(key))
    }

    /** 投递（归属节点负责唤醒并投递；跨节点时只携带载荷） */
    public func deliver(key: String, message: Message, sender: Option<ActorRef>): Unit {
        manager.tell(ClusterDeliver(key, message, sender))
    }

    /** 显式睡眠 */
    func requestSleep(key: String): Unit {
        manager.tell(SleepNow(key))
    }

    /** 当前 Up 成员数（0 = 集群视图未就绪，此时全部本地托管） */
    public func ownerCount(): Int64 {
        clusterUpMembers(cluster).size
    }

    /** key 的归属节点（与投递使用的算法一致；诊断与断言用） */
    public func ownerOf(key: String): Option<String> {
        clusterSessionOwner(cluster, numberOfShards, key)
    }

    /** 本节点当前活跃（已唤醒）的会话数 */
    public func localActiveCount(): Int64 {
        shared.activeCount()
    }

    /** 本节点当前活跃会话的 key（诊断用） */
    public func localActiveKeys(): Array<String> {
        shared.activeKeysSnapshot()
    }

    /** (活跃会话数, 累计唤醒数, 累计转发数) */
    public func stats(): (Int64, Int64, Int64) {
        shared.stats()
    }

    public func pathOf(key: String): String {
        "/cluster-sleepable/${name}/${key}"
    }

    public func stop(): Unit {
        system.stop(manager)
    }
}

/** 集群会话句柄：不是 ActorRef，不占本地 Actor；睡着的会话只有句柄 + 存储里的状态 */
public class ClusterSleepHandle {
    private let registry: ClusterSleepRegistry
    private let key: String
    private let path: String

    init(registry: ClusterSleepRegistry, key: String, path: String) {
        this.registry = registry
        this.key = key
        this.path = path
    }

    /** 投递消息（归属节点负责唤醒；跨节点时只携带载荷） */
    public func tell(message: Message): Unit {
        registry.deliver(key, message, None<ActorRef>)
    }

    /** 带发送方投递（仅本地归属时保留 sender；跨节点转发不携带） */
    public func tell(message: Message, sender: ActorRef): Unit {
        registry.deliver(key, message, Some<ActorRef>(sender))
    }

    /** 显式睡眠 */
    public func sleep(): Unit {
        registry.requestSleep(key)
    }

    /** 会话的稳定标识（诊断用；不是 Actor 路径） */
    public func pathString(): String {
        path
    }

    public func toString(): String {
        "ClusterSleepHandle(${path})"
    }
}
