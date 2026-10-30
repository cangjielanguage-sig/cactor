/*
 * CActor Cluster - 集群分片（对标 Akka Cluster Sharding 的消息路由、按需创建实体与再平衡）
 *
 * - 实体 ID → 分片：FNV-1a(实体 ID) mod 分片数（与进程无关的稳定哈希，不能用 String.hashCode）。
 * - 分片 → 节点：在全部 Up 成员上做最高随机权重（rendezvous）哈希。只有在本节点看到的成员视图已收敛时才计算：
 *   收敛意味着全部成员看到同一份成员表，因此各节点算出的归属一致，同一实体不会同时在两个节点上创建。
 *   视图未收敛（有成员加入、离开、不可达）期间，消息先缓冲在本节点的 Region 中，收敛后再投递。
 * - 每个节点一个 Region（暴露为 "sharding-<类型名>"）：本节点拥有的分片中的实体按需创建为子 Actor；
 *   其他分片的消息转发给拥有者的 Region。成员变化后不再拥有的分片，其实体被停止（移交），
 *   之后的消息由新的拥有者重新创建实体。实体状态如需在移交后保留，请结合持久化（事件溯源）使用。
 * - 投递是至多一次；同一发送方发给同一实体的消息在没有移交时保持顺序。
 * - 钝化（ShardingSettings.passivateAfter）：空闲超过给定时长的实体被停止，之后的消息重新创建它。
 *   实体停止期间到达的消息先缓冲，收到 Terminated 后再投递（子 Actor 名称释放后才能重建）。
 * - remember-entities（ShardingSettings.rememberEntities）：实体 ID 保存在 Replicator 的 ORSet
 *   （键 "sharding/<类型名>/<分片>"）中，拥有者变化（移交、宕机）后新拥有者主动启动这些实体，不需要等新消息；
 *   实体自己停止时从集合中删除。与钝化互斥（与 Akka 相同：记住的实体不会被自动钝化）。
 *   ORSet 经 gossip 复制，新创建的实体在一个 gossip 间隔内复制到其他节点；在此之前宕机则不会被自动重启。
 */
package cactor_cluster.membership

import std.sync.*
import std.time.*
import std.collection.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef, Terminated, PoisonPill}
import cactor_core.message.{Message, MessageResult, MessageSerializer, MessageCodec, MessageCodecRegistry}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.logging.{Logging}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.ddata.{Replicator}
import cactor_foundation.detector.{monotonicMillis}

// ======================= 哈希 =======================

/** 64 位 FNV-1a：跨进程、跨版本稳定（乘法按 2^64 取模，仓颉默认检查溢出，因此显式声明回绕） */
@OverflowWrapping
public func stableHash(s: String): UInt64 {
    var h: UInt64 = 0xcbf29ce484222325
    for (b in s) {
        h = h ^ UInt64(b)
        h = h * 0x100000001b3
    }
    h
}

/** 实体所在的分片 */
public func shardOf(entityId: String, numberOfShards: Int64): Int64 {
    Int64(stableHash(entityId) % UInt64(numberOfShards))
}

/** 分片的拥有者：在候选成员上做 rendezvous 哈希（成员变化时只有约 1/n 的分片移动） */
public func shardOwner(shard: Int64, members: Array<String>): Option<String> {
    var best: Option<String> = None
    var bestScore: UInt64 = 0
    for (m in members) {
        let score = stableHash("${shard}|${m}")
        match (best) {
            case Some(b) where score < bestScore || (score == bestScore && m > b) => ()
            case _ =>
                best = Some(m)
                bestScore = score
        }
    }
    best
}

// ======================= 信封 =======================

/** 发给分片实体的消息（远程传输时使用） */
public class ShardingEnvelope <: Message {
    public let entityId: String
    public let payload: Message
    /** 已经被转发过的次数：只转发一次，避免成员视图短暂不一致时来回转发 */
    let hops: Int64

    public init(entityId: String, payload: Message) {
        this(entityId, payload, 0)
    }

    init(entityId: String, payload: Message, hops: Int64) {
        this.entityId = entityId
        this.payload = payload
        this.hops = hops
    }

    public func messageType(): String {
        "cactor.ShardingEnvelope"
    }
}

class ShardingEnvelopeCodec <: MessageCodec {
    private let inner = MessageSerializer()

    public func encode(message: Message): Array<UInt8> {
        let e = (message as ShardingEnvelope).getOrThrow()
        let header = "${e.hops}:${e.entityId.size}:${e.entityId}".toArray()
        let body = inner.serialize(e.payload)
        let out = Array<UInt8>(header.size + body.size, repeat: 0)
        header.copyTo(out, 0, 0, header.size)
        body.copyTo(out, 0, header.size, body.size)
        out
    }

    public func decode(payload: Array<UInt8>): Message {
        var pos: Int64 = 0
        let hops = readNumber(payload, pos)
        pos = hops[1]
        let len = readNumber(payload, pos)
        pos = len[1]
        if (len[0] < 0 || pos + len[0] > payload.size) {
            throw IllegalArgumentException("分片信封损坏")
        }
        let id = String.fromUtf8(payload[pos..(pos + len[0])])
        pos += len[0]
        ShardingEnvelope(id, inner.deserialize(payload[pos..]), hops[0])
    }

    /** 读取 "<数字>:"，返回 (数字, 冒号之后的位置) */
    private static func readNumber(data: Array<UInt8>, start: Int64): Array<Int64> {
        var n: Int64 = 0
        var i = start
        while (i < data.size && data[i] != UInt8(UInt32(r':'))) {
            let d = data[i]
            if (d < UInt8(UInt32(r'0')) || d > UInt8(UInt32(r'9'))) {
                throw IllegalArgumentException("分片信封损坏")
            }
            n = n * 10 + Int64(d - UInt8(UInt32(r'0')))
            i++
        }
        if (i >= data.size) {
            throw IllegalArgumentException("分片信封损坏")
        }
        [n, i + 1]
    }
}

// ======================= 设置 =======================

/** 分片的可选行为 */
public class ShardingSettings {
    /** Region 中等待路由的消息上限，超出丢弃最旧的 */
    public let bufferSize: Int64
    /** 空闲超过该时长的实体被停止（钝化）；None 表示不钝化 */
    public let passivateAfter: Option<Duration>
    /** 记住实体 ID 的复制器；Some 表示开启 remember-entities */
    public let rememberEntities: Option<Replicator>

    public init(bufferSize: Int64, passivateAfter: Option<Duration>, rememberEntities: Option<Replicator>) {
        if (bufferSize <= 0) {
            throw IllegalArgumentException("缓冲区大小必须为正数: ${bufferSize}")
        }
        if (passivateAfter.isSome() && rememberEntities.isSome()) {
            throw IllegalArgumentException("remember-entities 与自动钝化不能同时开启")
        }
        this.bufferSize = bufferSize
        this.passivateAfter = passivateAfter
        this.rememberEntities = rememberEntities
    }

    public static func defaults(): ShardingSettings {
        ShardingSettings(10000, None<Duration>, None<Replicator>)
    }

    public static func passivating(idle: Duration): ShardingSettings {
        ShardingSettings(10000, Some(idle), None<Replicator>)
    }

    public static func remembering(replicator: Replicator): ShardingSettings {
        ShardingSettings(10000, None<Duration>, Some(replicator))
    }
}

/** remember-entities 在 Replicator 中使用的键 */
public func rememberedEntitiesKey(typeName: String, shard: Int64): String {
    "sharding/${typeName}/${shard}"
}

// ======================= Region =======================

class RegionTick <: Message {
    public func messageType(): String {
        "ShardRegionTick"
    }
}

class RegionShared {
    let lock = Mutex()
    let entityIds = HashSet<String>()
    var deliveredLocal: Int64 = 0
    var forwarded: Int64 = 0
    var handedOff: Int64 = 0
    var dropped: Int64 = 0
    var buffered: Int64 = 0
    var passivated: Int64 = 0
    var restarted: Int64 = 0
}

class ShardRegionActor <: Actor {
    let typeName: String
    let numberOfShards: Int64
    let cluster: Cluster
    let remote: SimpleRemoteManager
    let entityFactory: (String) -> Actor
    let settings: ShardingSettings
    let bufferSize: Int64
    let shared: RegionShared
    let entities = HashMap<String, ActorRef>()
    // 实体 Actor 路径 -> 实体 ID（运行中与停止中的都在）
    let idByPath = HashMap<String, String>()
    // 由 Region 停止、尚未收到 Terminated 的实体 -> 期间到达的消息
    let stopping = HashMap<String, ArrayList<ShardingEnvelope>>()
    // 实体最近一次收到消息的时间（毫秒，仅钝化时使用）
    let lastActive = HashMap<String, Int64>()
    let buffer = ArrayList<ShardingEnvelope>()
    // 当前（已收敛的）Up 成员，None 表示视图未收敛
    var owners: Option<Array<String>> = None
    var ticks: Int64 = 0
    var ownersChanged = false
    let log = Logging.logger("cactor.cluster.sharding")

    init(typeName: String, numberOfShards: Int64, cluster: Cluster, remote: SimpleRemoteManager,
        entityFactory: (String) -> Actor, settings: ShardingSettings, shared: RegionShared) {
        this.typeName = typeName
        this.numberOfShards = numberOfShards
        this.cluster = cluster
        this.remote = remote
        this.entityFactory = entityFactory
        this.settings = settings
        this.bufferSize = settings.bufferSize
        this.shared = shared
    }

    public prop name: String {
        get() { "shard-region" }
    }

    public prop description: String {
        get() { "shard region ${typeName}" }
    }

    public func onStarted(context: ActorContext): Unit {
        context.timers.startTimerWithFixedDelay("tick", RegionTick(), Duration.millisecond * 50)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: RegionTick =>
                ticks++
                refreshOwners(context)
                flush(context)
                passivateIdle(context)
                // 移交或成员变化后立即检查，平时每 500ms 检查一次需要恢复的实体
                if (ownersChanged || ticks % 10 == 0) {
                    ownersChanged = false
                    restoreRemembered(context)
                }
            case e: ShardingEnvelope =>
                if (buffer.isEmpty()) {
                    route(e, context)
                } else {
                    // 已有缓冲：排在后面，保持顺序
                    enqueue(e)
                }
            case t: Terminated => onEntityTerminated(t.actor.path.toString(), context)
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }

    private func refreshOwners(context: ActorContext): Unit {
        let s = cluster.state()
        if (cluster.isTerminated()) {
            // 本节点已离开、被 down 或被脑裂处理停止：其他节点会接管这些分片，本地实例必须停止
            owners = None
            for ((_, ref) in entities) {
                context.unwatch(ref)
                context.stop(ref)
            }
            entities.clear()
            idByPath.clear()
            stopping.clear()
            lastActive.clear()
            synchronized(shared.lock) {
                shared.entityIds.clear()
            }
            return
        }
        if (!s.converged || !cluster.isUp()) {
            owners = None
            return
        }
        let ups = ArrayList<String>()
        for (m in s.members) {
            if (m.status == MemberStatus.Up) {
                ups.add(m.address)
            }
        }
        let next = ups.toArray()
        if (!sameMembers(owners, next)) {
            ownersChanged = true
        }
        owners = Some(next)
        // 移交：停止不再属于本节点的实体（remember-entities 时 ID 留在集合中，由新拥有者启动）
        let moved = ArrayList<String>()
        for ((id, _) in entities) {
            if (ownerOf(id) != Some(cluster.selfAddress)) {
                moved.add(id)
            }
        }
        for (id in moved) {
            stopEntity(id, context)
        }
        if (!moved.isEmpty()) {
            log.info("${cluster.selfAddress} 移交 ${moved.size} 个 ${typeName} 实体")
            synchronized(shared.lock) {
                shared.handedOff += moved.size
            }
        }
    }

    private static func sameMembers(cur: Option<Array<String>>, next: Array<String>): Bool {
        match (cur) {
            case Some(c) where c.size == next.size =>
                let set = HashSet<String>(c)
                for (x in next) {
                    if (!set.contains(x)) {
                        return false
                    }
                }
                true
            case _ => false
        }
    }

    /**
     * Region 主动停止实体（钝化、移交）：经邮箱发送 PoisonPill，已投递的消息先处理完再停止
     * （系统级 stop 会丢弃邮箱中尚未处理的消息）；期间新到达的消息缓冲到 Terminated 之后
     */
    private func stopEntity(id: String, context: ActorContext): Unit {
        if (let Some(ref) <- entities.remove(id)) {
            stopping[id] = ArrayList<ShardingEnvelope>(0)
            lastActive.remove(id)
            ref.tell(PoisonPill())
            synchronized(shared.lock) {
                shared.entityIds.remove(id)
            }
        }
    }

    private func onEntityTerminated(path: String, context: ActorContext): Unit {
        let id = match (idByPath.remove(path)) {
            case Some(i) => i
            case None => return
        }
        match (stopping.remove(id)) {
            case Some(pending) =>
                // Region 停止的（钝化或移交）：重新路由期间到达的消息
                for (e in pending) {
                    route(e, context)
                }
            case None =>
                // 实体自己停止：不再需要记住它
                entities.remove(id)
                lastActive.remove(id)
                synchronized(shared.lock) {
                    shared.entityIds.remove(id)
                }
                if (let Some(rep) <- settings.rememberEntities) {
                    rep.removeFromSet(rememberedEntitiesKey(typeName, shardOf(id, numberOfShards)), id)
                }
        }
    }

    private func passivateIdle(context: ActorContext): Unit {
        let idle = match (settings.passivateAfter) {
            case Some(d) => d.toMilliseconds()
            case None => return
        }
        let now = monotonicMillis()
        let expired = ArrayList<String>()
        for ((id, at) in lastActive) {
            if (now - at >= idle) {
                expired.add(id)
            }
        }
        for (id in expired) {
            stopEntity(id, context)
        }
        if (!expired.isEmpty()) {
            synchronized(shared.lock) {
                shared.passivated += expired.size
            }
        }
    }

    /** remember-entities：启动本节点拥有的分片中被记住但未运行的实体 */
    private func restoreRemembered(context: ActorContext): Unit {
        let rep = match (settings.rememberEntities) {
            case Some(r) => r
            case None => return
        }
        if (owners.isNone()) {
            return
        }
        let prefix = "sharding/${typeName}/"
        var started: Int64 = 0
        for (key in rep.keys()) {
            if (!key.startsWith(prefix)) {
                continue
            }
            for (id in rep.setElements(key)) {
                if (entities.contains(id) || stopping.contains(id) || ownerOf(id) != Some(cluster.selfAddress)) {
                    continue
                }
                startEntity(id, context)
                started++
            }
        }
        if (started > 0) {
            log.info("${cluster.selfAddress} 恢复 ${started} 个记住的 ${typeName} 实体")
            synchronized(shared.lock) {
                shared.restarted += started
            }
        }
    }

    private func startEntity(id: String, context: ActorContext): ActorRef {
        let factory = entityFactory
        let r = context.actorOf({ => factory(id) }, entityName(id))
        context.watch(r)
        entities[id] = r
        idByPath[r.path.toString()] = id
        synchronized(shared.lock) {
            shared.entityIds.add(id)
        }
        r
    }

    private func ownerOf(entityId: String): Option<String> {
        match (owners) {
            case Some(ms) => shardOwner(shardOf(entityId, numberOfShards), ms)
            case None => None<String>
        }
    }

    private func enqueue(e: ShardingEnvelope): Unit {
        buffer.add(e)
        if (buffer.size > bufferSize) {
            buffer.remove(at: 0)
            synchronized(shared.lock) {
                shared.dropped++
            }
        }
        synchronized(shared.lock) {
            shared.buffered = buffer.size
        }
    }

    private func flush(context: ActorContext): Unit {
        if (buffer.isEmpty() || owners.isNone()) {
            return
        }
        let pending = buffer.toArray()
        buffer.clear()
        for (e in pending) {
            route(e, context)
        }
        synchronized(shared.lock) {
            shared.buffered = buffer.size
        }
    }

    private func route(e: ShardingEnvelope, context: ActorContext): Unit {
        match (ownerOf(e.entityId)) {
            case None => enqueue(e)
            case Some(o) where o == cluster.selfAddress => deliver(e, context)
            case Some(o) =>
                if (e.hops >= 1) {
                    // 已转发过一次仍不在拥有者上：说明视图刚变化，等下一轮再判断
                    enqueue(e)
                    return
                }
                try {
                    remote.remoteActorOf(NetworkAddress.parse(o), "sharding-${typeName}").tell(
                        ShardingEnvelope(e.entityId, e.payload, e.hops + 1))
                    synchronized(shared.lock) {
                        shared.forwarded++
                    }
                } catch (ex: Exception) {
                    log.warn("转发 ${typeName}/${e.entityId} 到 ${o} 失败: ${ex}")
                    enqueue(e)
                }
        }
    }

    private func deliver(e: ShardingEnvelope, context: ActorContext): Unit {
        if (let Some(pending) <- stopping.get(e.entityId)) {
            // 实体正在停止：同名子 Actor 释放后再重建
            pending.add(e)
            return
        }
        let ref = match (entities.get(e.entityId)) {
            case Some(r) => r
            case None =>
                let r = startEntity(e.entityId, context)
                if (let Some(rep) <- settings.rememberEntities) {
                    rep.addToSet(rememberedEntitiesKey(typeName, shardOf(e.entityId, numberOfShards)), e.entityId)
                }
                r
        }
        if (settings.passivateAfter.isSome()) {
            lastActive[e.entityId] = monotonicMillis()
        }
        ref.tell(e.payload)
        synchronized(shared.lock) {
            shared.deliveredLocal++
        }
    }

    /** 实体 ID 可以是任意字符串；子 Actor 名称不能含 '/' */
    private static func entityName(id: String): String {
        "e-" + id.replace("%", "%25").replace("/", "%2F")
    }
}

/**
 * 一种实体类型在本节点上的分片区域。集群中每个节点都要用相同的 typeName、numberOfShards 和工厂创建。
 *
 * ```
 * let carts = ClusterSharding(cluster, system, remote, "cart", 100, { id => CartActor(id) })
 * carts.tell("user-42", StringMessage("add:apple"))   // 无论在哪个节点调用，都送到 user-42 唯一的实体
 * ```
 */
public class ClusterSharding {
    private let region: ActorRef
    private let shared = RegionShared()
    private let system: CActorSystem
    private let remote: SimpleRemoteManager
    public let typeName: String
    public let numberOfShards: Int64

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, typeName: String,
        numberOfShards: Int64, entityFactory: (String) -> Actor) {
        this(cluster, system, remote, typeName, numberOfShards, entityFactory, 10000)
    }

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, typeName: String,
        numberOfShards: Int64, entityFactory: (String) -> Actor, bufferSize: Int64) {
        this(cluster, system, remote, typeName, numberOfShards, entityFactory,
            ShardingSettings(bufferSize, None<Duration>, None<Replicator>))
    }

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, typeName: String,
        numberOfShards: Int64, entityFactory: (String) -> Actor, settings: ShardingSettings) {
        if (numberOfShards <= 0) {
            throw IllegalArgumentException("分片数必须为正数: ${numberOfShards}")
        }
        MessageCodecRegistry.register("cactor.ShardingEnvelope", ShardingEnvelopeCodec())
        this.system = system
        this.remote = remote
        this.typeName = typeName
        this.numberOfShards = numberOfShards
        if (let Some(rep) <- settings.rememberEntities) {
            ClusterReplicatorBinding.bind(cluster, rep)
        }
        let sh = shared
        this.region = system.actorOf({ => ShardRegionActor(typeName, numberOfShards, cluster, remote, entityFactory, settings, sh) },
            "sharding-${typeName}-${cluster.selfAddress.replace(":", "_").replace(".", "_")}")
        remote.exposeActor("sharding-${typeName}", region)
    }

    /** 发给实体（消息需要能被远程序列化） */
    public func tell(entityId: String, message: Message): Unit {
        region.tell(ShardingEnvelope(entityId, message))
    }

    /** 本节点当前运行的实体 */
    public func localEntityIds(): Array<String> {
        synchronized(shared.lock) {
            shared.entityIds.toArray()
        }
    }

    public func localEntityCount(): Int64 {
        synchronized(shared.lock) {
            shared.entityIds.size
        }
    }

    /** (本地投递数, 转发数, 移交的实体数, 因缓冲区满丢弃数, 当前缓冲数) */
    public func stats(): (Int64, Int64, Int64, Int64, Int64) {
        synchronized(shared.lock) {
            (shared.deliveredLocal, shared.forwarded, shared.handedOff, shared.dropped, shared.buffered)
        }
    }

    /** 因空闲被钝化的实体数 */
    public func passivatedCount(): Int64 {
        synchronized(shared.lock) {
            shared.passivated
        }
    }

    /** remember-entities 主动恢复（不经新消息）启动的实体数 */
    public func restartedCount(): Int64 {
        synchronized(shared.lock) {
            shared.restarted
        }
    }

    public func stop(): Unit {
        remote.unexposeActor("sharding-${typeName}")
        system.stop(region)
    }
}
