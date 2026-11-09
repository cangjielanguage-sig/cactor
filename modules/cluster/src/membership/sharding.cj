/*
 * CActor Cluster - 集群分片（对标 Akka Cluster Sharding 的消息路由、按需创建实体、移交与再平衡）
 *
 * - 实体 ID → 分片：FNV-1a(实体 ID) 经 fmix64 混合后 mod 分片数（与进程无关的稳定哈希，不能用 String.hashCode）。
 * - 分片 → 节点：在本节点看到的全部 Up 成员上做最高随机权重（rendezvous）哈希，成员变化时只有约 1/n 的分片移动。
 *   算法版本（SHARDING_HASH_VERSION）在加入集群时比对，版本不同的节点被拒绝加入（E42，见 membership.cj）。
 * - 单活（E13′）：节点只在征得其他每个可能托管该分片的成员（Up / Leaving / Exiting）确认"已停止该分片"之后
 *   才托管它（BEGIN → 对方停止实体、全部终止 → STOPPED → 本节点创建实体）。视图不一致只影响可用性，不会双活；
 *   因此路由不需要等成员视图收敛：只有"拥有者不可达"或"正在获取"的分片缓冲消息，其他分片照常投递。
 * - 每个节点一个 Region（暴露为 "sharding-<类型名>"）：本节点托管的分片中的实体按需创建为子 Actor；
 *   其他分片的消息转发给拥有者的 Region。实体状态如需在移交后保留，请结合持久化（事件溯源）使用。
 * - 投递是至多一次；同一发送方发给同一实体的消息在没有移交时保持顺序（缓冲按分片保持顺序）。
 *   节点离开集群、缓冲区满时丢弃的消息交给死信（CActorSystem.publishDeadLetter）。
 * - 钝化（ShardingSettings.passivateAfter）：空闲超过给定时长的实体被停止，之后的消息重新创建它。
 *   实体停止期间到达的消息先缓冲，收到 Terminated 后再投递（子 Actor 名称释放后才能重建）。
 * - remember-entities（ShardingSettings.rememberEntities）：实体 ID 保存在 Replicator 的 ORSet
 *   （键 "sharding/<类型名>/<分片>"）中，新拥有者获取分片后主动启动这些实体，不需要等新消息；
 *   实体自己停止时从集合中删除。与钝化互斥（与 Akka 相同：记住的实体不会被自动钝化）。
 *   ORSet 经 gossip 复制，新创建的实体在一个 gossip 间隔内复制到其他节点；在此之前宕机则不会被自动重启。
 * - 前提：集群中每个节点都用相同的 typeName、numberOfShards 创建 Region（获取分片要等每个成员确认）。
 */
package cactor_cluster.membership

import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef, Terminated, PoisonPill}
import cactor_core.message.{Message, MessageResult, MessageSerializer, MessageCodec, MessageCodecRegistry}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.logging.{Logging}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.ddata.{Replicator, FieldWriter, FieldReader}
import cactor_foundation.detector.{monotonicMillis}
import cactor_core.typed.{TypedPromise}

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

/**
 * 64 位最终混合（MurmurHash3 的 fmix64）：把输入的所有位雪崩到输出。
 *
 * 为什么必须加：FNV-1a 对"共享前缀、仅末位不同"的字符串（例如本机测试里顺序分配端口的
 * `127.0.0.1:4916{8,9,0}`）就近平移关系；直接比较 FNV 值取最大时，末位字符会系统性地
 * 决定大小关系——实测 64 个分片全部落在 .168/.170 两个节点上，.169 一个都分不到。
 * 先过一遍 fmix64 后分布变为 24/23/17，问题消失。
 */
@OverflowWrapping
public func mixHash(h: UInt64): UInt64 {
    var x = h
    x = x ^ (x >> 33)
    x = x * 0xff51afd7ed558ccd
    x = x ^ (x >> 33)
    x = x * 0xc4ceb9fe1a85ec53
    x = x ^ (x >> 33)
    x
}

/** 实体所在的分片（先混合再取模，避免短 id/共享前缀造成的低位聚集） */
public func shardOf(entityId: String, numberOfShards: Int64): Int64 {
    Int64(mixHash(stableHash(entityId)) % UInt64(numberOfShards))
}

/**
 * 分片的拥有者：在候选成员上做 rendezvous 哈希（成员变化时只有约 1/n 的分片移动）。
 * 取"混合后分数最大"的成员；分数相同按地址字典序兜底，保证各节点独立计算结果一致。
 */
public func shardOwner(shard: Int64, members: Array<String>): Option<String> {
    var best: Option<String> = None
    var bestScore: UInt64 = 0
    for (m in members) {
        let score = mixHash(stableHash("${shard}|${m}"))
        if (best.isNone() || score > bestScore || (score == bestScore && m > best.getOrThrow())) {
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
    /**
     * 原始发送方（E12′，N16）：随信封缓冲与转发，投递时作为实体收到消息的 sender，实体 context.reply 即可回复。
     * 不进编码：跨节点时由远程层的发送方字段携带（远程发送方原样转交，回复直接回到发起方）
     */
    let sender: Option<ActorRef>

    public init(entityId: String, payload: Message) {
        this(entityId, payload, 0, None<ActorRef>)
    }

    init(entityId: String, payload: Message, hops: Int64) {
        this(entityId, payload, hops, None<ActorRef>)
    }

    init(entityId: String, payload: Message, hops: Int64, sender: Option<ActorRef>) {
        this.entityId = entityId
        this.payload = payload
        this.hops = hops
        this.sender = sender
    }

    func withHops(n: Int64): ShardingEnvelope {
        ShardingEnvelope(entityId, payload, n, sender)
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

// ======================= 移交协议（E13′） =======================

/**
 * 分片区域之间的移交控制消息：
 * - BEGIN：发送方（按它的视图）是这些分片的拥有者，请接收方停止这些分片的实体；items 为 (分片, 轮次)；
 * - STOPPED：接收方这些分片的实体已全部终止，并且放弃了自己对它们的托管与获取（回应对应轮次）。
 * 带协议版本：版本不同的区域互不理睬（E42 已在加入时拒绝这种节点，这里是第二道防线）
 */
public class ShardControl <: Message {
    public let kind: String
    public let from: String
    public let protocolVersion: Int64
    public let items: Array<(Int64, Int64)>

    public init(kind: String, from: String, protocolVersion: Int64, items: Array<(Int64, Int64)>) {
        this.kind = kind
        this.from = from
        this.protocolVersion = protocolVersion
        this.items = items
    }

    public func messageType(): String {
        "cactor.ShardControl"
    }
}

class ShardControlCodec <: MessageCodec {
    public func encode(message: Message): Array<UInt8> {
        let c = (message as ShardControl).getOrThrow()
        let w = FieldWriter().put(c.kind).put(c.from).put(c.protocolVersion).put(c.items.size)
        for ((sh, round) in c.items) {
            w.put(sh).put(round)
        }
        w.done().toArray()
    }

    public func decode(payload: Array<UInt8>): Message {
        let r = FieldReader(String.fromUtf8(payload))
        let kind = r.next()
        let from = r.next()
        let version = Int64.parse(r.next())
        let n = Int64.parse(r.next())
        if (n < 0 || n > 1_000_000) {
            throw IllegalArgumentException("分片控制消息损坏：条目数 ${n}")
        }
        let items = ArrayList<(Int64, Int64)>(n)
        for (_ in 0..n) {
            let sh = Int64.parse(r.next())
            items.add((sh, Int64.parse(r.next())))
        }
        ShardControl(kind, from, version, items.toArray())
    }
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
    // 当前路由使用的 Up 成员数（0 表示还没有可用视图）；供测试/诊断等待"分片区域已看到全部节点"
    var ownerCount: Int64 = 0
    var hostedShards: Int64 = 0
    var acquiringShards: Int64 = 0
    var acquisitionsCompleted: Int64 = 0
    var yieldedShards: Int64 = 0
    // 每个分片上当前运行的实体数（供负载报告 / 再平衡决策，E62/E13′ 后续）
    let shardCounts = HashMap<Int64, Int64>()
}

/** 本节点正在获取的分片：等其他成员确认已停止它 */
class Acquisition {
    let round: Int64
    let pending = HashSet<String>()
    let acked = HashSet<String>()
    let startedMs: Int64
    var lastSentMs: Int64 = -1
    var warned = false

    init(round: Int64, startedMs: Int64) {
        this.round = round
        this.startedMs = startedMs
    }
}

/** 区域使用的成员视图（来自本节点最新的集群快照，不要求已收敛） */
class RegionView {
    // 计算归属用的 Up 成员
    let ups: Array<String>
    // 获取分片时必须征得同意的成员：Up / Leaving / Exiting（可能托管着分片），不含自己
    let askSet: Array<String>
    let unreachable: HashSet<String>

    init(ups: Array<String>, askSet: Array<String>, unreachable: HashSet<String>) {
        this.ups = ups
        this.askSet = askSet
        this.unreachable = unreachable
    }
}

// 转发次数上限：超过后挂起，视图变化（或每秒）重新路由；拥有者让出后消息转回来超过 2 跳即重新获取
let MAX_HOPS: Int64 = 3
let HANDOFF_RESEND_MS: Int64 = 200
let SLOW_ACQUIRE_WARN_MS: Int64 = 10000
let REROUTE_PARKED_TICKS: Int64 = 20

/**
 * 分片区域（每个节点、每种实体类型一个）。
 *
 * 单活的保证（E13′）不再依赖"各节点同时看到同一份已收敛视图"，而是许可式移交：
 * 节点只在征得其他每个可能托管该分片的成员（Up / Leaving / Exiting）的确认之后才托管分片——
 * 确认意味着对方的该分片实体已全部终止，并且放弃了它自己对该分片的托管与获取。
 * 两个节点因为视图不一致都认为自己是拥有者时，后请求的一方让先托管的一方停止，最多来回几次，
 * 视图一致后只剩真正的拥有者在请求；任何时刻都不会有两个节点同时托管同一分片。
 * 被 Down / Removed 的成员不参与（与 Akka 相同的前提：被 down 的节点会自行停止，见 sbr.cj）。
 *
 * 可用性：只有"拥有者不可达"或"正在获取"的分片缓冲消息，其他分片照常投递（N6）；
 * 节点在视图建立后立即获取自己的全部分片，之后即使有成员不可达，已托管的分片不受影响。
 */
class ShardRegionActor <: Actor {
    let typeName: String
    let numberOfShards: Int64
    let cluster: Cluster
    let remote: SimpleRemoteManager
    let system: CActorSystem
    let entityFactory: (String) -> Actor
    let settings: ShardingSettings
    let bufferSize: Int64
    let shared: RegionShared
    let selfAddress: String
    let entities = HashMap<String, ActorRef>()
    // 实体 Actor 路径 -> 实体 ID（运行中与停止中的都在）
    let idByPath = HashMap<String, String>()
    // 由 Region 停止、尚未收到 Terminated 的实体 -> 期间到达的消息
    let stopping = HashMap<String, ArrayList<ShardingEnvelope>>()
    // 实体最近一次收到消息的时间（毫秒，仅钝化时使用）
    let lastActive = HashMap<String, Int64>()
    // 分片 -> 本节点上属于它的实体（运行中与停止中），移交时据此停止、据此判断"已全部终止"
    let shardEntities = HashMap<Int64, HashSet<String>>()
    // ---- 移交状态 ----
    let hosting = HashSet<Int64>()
    let acquiring = HashMap<Int64, Acquisition>()
    // 本节点按自己的视图是拥有者、但已让给了请求方的分片 -> 请求方
    let yielded = HashMap<Int64, String>()
    // 实体还没全部终止、尚未回复的移交请求：分片 -> (请求方, 轮次)
    let pendingStopAcks = HashMap<Int64, ArrayList<(String, Int64)>>()
    // ---- 缓冲 ----
    let shardBuffers = HashMap<Int64, ArrayList<ShardingEnvelope>>()
    // 还没有可用视图（本节点尚未看到任何 Up 成员）时的消息
    let regionBuffer = ArrayList<ShardingEnvelope>()
    var bufferedTotal: Int64 = 0
    var view: Option<RegionView> = None
    // 上次计算视图时对应的集群快照：快照对象按不可变整体替换，用引用相等判断"成员视图是否变了"
    var cachedSnapshot: Option<ClusterSnapshot> = None
    var nextRound: Int64 = 0
    var ticks: Int64 = 0
    var restoreDue = false
    // 本节点已离开集群（离开、被 down、被脑裂处理停止）
    var left = false
    let log = Logging.logger("cactor.cluster.sharding")

    init(typeName: String, numberOfShards: Int64, cluster: Cluster, remote: SimpleRemoteManager, system: CActorSystem,
        entityFactory: (String) -> Actor, settings: ShardingSettings, shared: RegionShared) {
        this.typeName = typeName
        this.numberOfShards = numberOfShards
        this.cluster = cluster
        this.remote = remote
        this.system = system
        this.entityFactory = entityFactory
        this.settings = settings
        this.bufferSize = settings.bufferSize
        this.shared = shared
        this.selfAddress = cluster.selfAddress
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
                refreshView(context)
                sendAcquireRequests()
                passivateIdle(context)
                if (ticks % REROUTE_PARKED_TICKS == 0) {
                    rerouteParked(context)
                }
                // 获取完成或成员变化后立即检查，平时每 500ms 检查一次需要恢复的实体
                if (restoreDue || ticks % 10 == 0) {
                    restoreDue = false
                    restoreRemembered(context)
                }
            case e: ShardingEnvelope =>
                // 信封本身没带 sender 时取这条消息的发送方（远程转发来的信封，发送方是发起方的远程引用）
                let withSender = match (e.sender) {
                    case Some(_) => e
                    case None => ShardingEnvelope(e.entityId, e.payload, e.hops, context.senderRef())
                }
                route(withSender, context)
            case c: ShardControl => onControl(c, context)
            case t: Terminated => onEntityTerminated(t.actor.path.toString(), context)
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }

    // ---------- 视图 ----------

    private func refreshView(context: ActorContext): Unit {
        if (left) {
            return
        }
        let (s, terminated) = cluster.stateAndTerminated()
        if (terminated) {
            leaveCluster(context)
            return
        }
        // 成员视图未变（同一个快照对象）：每次 route 前的快速路径，只有一次无争用锁 + 一次引用比较
        match (cachedSnapshot) {
            case Some(c) where refEq(c, s) => return
            case _ => ()
        }
        cachedSnapshot = Some(s)
        let ups = ArrayList<String>()
        let ask = ArrayList<String>()
        for (m in s.members) {
            match (m.status) {
                case Up =>
                    ups.add(m.address)
                    if (m.address != selfAddress) {
                        ask.add(m.address)
                    }
                case Leaving | Exiting =>
                    if (m.address != selfAddress) {
                        ask.add(m.address)
                    }
                case _ => ()
            }
        }
        let next = RegionView(ups.toArray(), ask.toArray(), HashSet<String>(s.unreachable))
        let changed = match (view) {
            case Some(v) => !sameMembers(v.ups, next.ups)
            case None => true
        }
        view = Some(next)
        synchronized(shared.lock) {
            shared.ownerCount = next.ups.size
        }
        if (changed) {
            // 归属变了：之前的让出记录作废，按新视图重新判断
            yielded.clear()
            onOwnershipChanged(context)
            restoreDue = true
        } else {
            // Up 集合不变，但可能有成员 Down / Leaving：调整还在等待的确认
            updatePending(context)
        }
        // 可达性或归属变化：挂起的消息重新路由；之前没有视图时缓冲的消息现在可以路由了
        rerouteParked(context)
        flushRegionBuffer(context)
        updateShardStats()
    }

    private static func sameMembers(cur: Array<String>, next: Array<String>): Bool {
        if (cur.size != next.size) {
            return false
        }
        let set = HashSet<String>(cur)
        for (x in next) {
            if (!set.contains(x)) {
                return false
            }
        }
        true
    }

    private func ownerOfShard(sh: Int64): Option<String> {
        match (view) {
            case Some(v) => shardOwner(sh, v.ups)
            case None => None<String>
        }
    }

    private func onOwnershipChanged(context: ActorContext): Unit {
        let me = Some(selfAddress)
        // 不再属于本节点的分片：停止实体（移交出去）。新拥有者的请求到达时，全部终止后回复 STOPPED
        var movedEntities: Int64 = 0
        for (sh in hosting.toArray()) {
            if (ownerOfShard(sh) != me) {
                hosting.remove(sh)
                movedEntities += stopShard(sh)
            }
        }
        if (movedEntities > 0) {
            log.info("${selfAddress} 移交 ${movedEntities} 个 ${typeName} 实体")
        }
        // 不再属于本节点的获取：放弃（缓冲的消息随后重新路由）
        let abandoned = ArrayList<Int64>()
        for ((sh, _) in acquiring) {
            if (ownerOfShard(sh) != me) {
                abandoned.add(sh)
            }
        }
        for (sh in abandoned) {
            acquiring.remove(sh)
        }
        // 本节点拥有、还没托管的分片：立即获取，不等第一条消息——
        // 视图建立时成员都可达，之后哪怕有成员不可达，已托管的分片照常工作
        for (sh in 0..numberOfShards) {
            if (ownerOfShard(sh) == me && !hosting.contains(sh) && !acquiring.contains(sh)) {
                startAcquire(sh, context)
            }
        }
        updatePending(context)
        sendAcquireRequests()
    }

    // ---------- 获取（本节点成为拥有者） ----------

    private func startAcquire(sh: Int64, context: ActorContext): Unit {
        nextRound++
        yielded.remove(sh)
        let a = Acquisition(nextRound, monotonicMillis())
        if (let Some(v) <- view) {
            for (m in v.askSet) {
                a.pending.add(m)
            }
        }
        acquiring[sh] = a
        if (a.pending.isEmpty()) {
            completeAcquire(sh, context)
        }
    }

    /** 等待中的确认随成员变化调整：Down / Removed 的不再等，新出现的 Up / Leaving 成员也要确认 */
    private func updatePending(context: ActorContext): Unit {
        let v = match (view) {
            case Some(v) => v
            case None => return
        }
        let ask = HashSet<String>(v.askSet)
        let done = ArrayList<Int64>()
        for ((sh, a) in acquiring) {
            for (m in a.pending.toArray()) {
                if (!ask.contains(m)) {
                    a.pending.remove(m)
                }
            }
            for (m in ask) {
                if (!a.acked.contains(m) && !a.pending.contains(m)) {
                    a.pending.add(m)
                    a.lastSentMs = -1
                }
            }
            if (a.pending.isEmpty()) {
                done.add(sh)
            }
        }
        for (sh in done) {
            completeAcquire(sh, context)
        }
    }

    /** 向还没确认的成员（重）发移交请求：按成员合并成一条消息，每 200ms 重发一次直到确认 */
    private func sendAcquireRequests(): Unit {
        if (acquiring.isEmpty()) {
            return
        }
        let now = monotonicMillis()
        let perMember = HashMap<String, ArrayList<(Int64, Int64)>>()
        for ((sh, a) in acquiring) {
            if (a.pending.isEmpty() || (a.lastSentMs >= 0 && now - a.lastSentMs < HANDOFF_RESEND_MS)) {
                continue
            }
            a.lastSentMs = now
            for (m in a.pending) {
                match (perMember.get(m)) {
                    case Some(l) => l.add((sh, a.round))
                    case None =>
                        let l = ArrayList<(Int64, Int64)>()
                        l.add((sh, a.round))
                        perMember[m] = l
                }
            }
            if (!a.warned && now - a.startedMs > SLOW_ACQUIRE_WARN_MS) {
                a.warned = true
                log.warn("${selfAddress} 获取 ${typeName} 分片 ${sh} 已等待 ${now - a.startedMs} ms，仍未确认的成员：${a.pending.toArray()}（这些节点需要运行同类型的 ClusterSharding）")
            }
        }
        for ((m, items) in perMember) {
            sendControl(m, "BEGIN", items.toArray())
        }
    }

    private func completeAcquire(sh: Int64, context: ActorContext): Unit {
        if (acquiring.remove(sh).isNone()) {
            return
        }
        hosting.add(sh)
        synchronized(shared.lock) {
            shared.acquisitionsCompleted++
        }
        restoreRememberedShard(sh, context)
        flushShard(sh, context)
        updateShardStats()
    }

    // ---------- 控制消息 ----------

    private func sendControl(to: String, kind: String, items: Array<(Int64, Int64)>): Unit {
        try {
            remote.remoteActorOf(NetworkAddress.parse(to), "sharding-${typeName}").tell(
                ShardControl(kind, selfAddress, SHARDING_PROTOCOL_VERSION, items))
        } catch (e: Exception) {
            // 发送失败（地址非法、传输已停止）：获取方每 200ms 重发
            log.debug("发送分片 ${kind} 到 ${to} 失败: ${e}")
        }
    }

    private func onControl(c: ShardControl, context: ActorContext): Unit {
        if (c.protocolVersion != SHARDING_PROTOCOL_VERSION) {
            log.warn("忽略 ${c.from} 的分片控制消息：协议版本 ${c.protocolVersion}，本节点 ${SHARDING_PROTOCOL_VERSION}")
            return
        }
        refreshView(context)
        match (c.kind) {
            case "BEGIN" => onBegin(c.from, c.items, context)
            case "STOPPED" => onStopped(c.from, c.items, context)
            case _ => ()
        }
    }

    /**
     * 对方要托管这些分片：本节点放弃对它们的托管与获取，停止其中的实体，全部终止后回复 STOPPED。
     * 无论本节点的视图怎么看都照办——让出总是安全的；本节点若确实是拥有者，消息转回来时会再获取
     */
    private func onBegin(from: String, items: Array<(Int64, Int64)>, context: ActorContext): Unit {
        let ready = ArrayList<(Int64, Int64)>()
        let touched = ArrayList<Int64>()
        var stoppedEntities: Int64 = 0
        for ((sh, round) in items) {
            if (sh < 0 || sh >= numberOfShards) {
                continue
            }
            acquiring.remove(sh)
            if (hosting.remove(sh)) {
                stoppedEntities += stopShard(sh)
            }
            yielded[sh] = from
            touched.add(sh)
            if (shardHasEntities(sh)) {
                let acks = match (pendingStopAcks.get(sh)) {
                    case Some(l) => l
                    case None =>
                        let l = ArrayList<(String, Int64)>()
                        pendingStopAcks[sh] = l
                        l
                }
                if (!acks.iterator().any({ p => p[0] == from && p[1] == round })) {
                    acks.add((from, round))
                }
            } else {
                ready.add((sh, round))
            }
        }
        if (stoppedEntities > 0) {
            log.info("${selfAddress} 应 ${from} 的请求移交 ${stoppedEntities} 个 ${typeName} 实体")
        }
        if (!ready.isEmpty()) {
            sendControl(from, "STOPPED", ready.toArray())
        }
        // 本节点缓冲的这些分片的消息：转给请求方
        for (sh in touched) {
            flushShard(sh, context)
        }
        synchronized(shared.lock) {
            shared.yieldedShards = yielded.size
        }
        updateShardStats()
    }

    private func onStopped(from: String, items: Array<(Int64, Int64)>, context: ActorContext): Unit {
        for ((sh, round) in items) {
            match (acquiring.get(sh)) {
                case Some(a) where a.round == round =>
                    a.pending.remove(from)
                    a.acked.add(from)
                    if (a.pending.isEmpty()) {
                        completeAcquire(sh, context)
                    }
                case _ => () // 旧轮次（已放弃或已完成）的确认
            }
        }
    }

    // ---------- 实体 ----------

    private func shardHasEntities(sh: Int64): Bool {
        match (shardEntities.get(sh)) {
            case Some(ids) => !ids.isEmpty()
            case None => false
        }
    }

    /** 停止分片中仍在运行的实体，返回停止的个数（停止中的实体收到 Terminated 后才算终止） */
    private func stopShard(sh: Int64): Int64 {
        var n: Int64 = 0
        if (let Some(ids) <- shardEntities.get(sh)) {
            for (id in ids.toArray()) {
                if (entities.contains(id)) {
                    stopEntity(id)
                    n++
                }
            }
        }
        if (n > 0) {
            synchronized(shared.lock) {
                shared.handedOff += n
            }
        }
        n
    }

    /**
     * Region 主动停止实体（钝化、移交）：经邮箱发送 PoisonPill，已投递的消息先处理完再停止
     * （系统级 stop 会丢弃邮箱中尚未处理的消息）；期间新到达的消息缓冲到 Terminated 之后
     */
    private func stopEntity(id: String): Unit {
        if (let Some(ref) <- entities.remove(id)) {
            stopping[id] = ArrayList<ShardingEnvelope>(0)
            lastActive.remove(id)
            ref.tell(PoisonPill())
            synchronized(shared.lock) {
                shared.entityIds.remove(id)
                let sh = shardOf(id, numberOfShards)
                shared.shardCounts[sh] = max(0, (shared.shardCounts.get(sh) ?? 1) - 1)
            }
        }
    }

    private func onEntityTerminated(path: String, context: ActorContext): Unit {
        let id = match (idByPath.remove(path)) {
            case Some(i) => i
            case None => return
        }
        let sh = shardOf(id, numberOfShards)
        if (let Some(ids) <- shardEntities.get(sh)) {
            ids.remove(id)
            if (ids.isEmpty()) {
                shardEntities.remove(sh)
            }
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
                    shared.shardCounts[sh] = max(0, (shared.shardCounts.get(sh) ?? 1) - 1)
                }
                if (let Some(rep) <- settings.rememberEntities) {
                    // 在 receive 里：不同步等待 Replicator（见 Replicator.updateAsync）
                    rep.removeFromSetAsync(rememberedEntitiesKey(typeName, sh), id)
                }
        }
        // 分片的实体全部终止：回复等待中的移交请求
        if (!shardHasEntities(sh)) {
            if (let Some(acks) <- pendingStopAcks.remove(sh)) {
                for ((requester, round) in acks) {
                    sendControl(requester, "STOPPED", [(sh, round)])
                }
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
            stopEntity(id)
        }
        if (!expired.isEmpty()) {
            synchronized(shared.lock) {
                shared.passivated += expired.size
            }
        }
    }

    /** remember-entities：启动本节点托管的分片中被记住但未运行的实体 */
    private func restoreRemembered(context: ActorContext): Unit {
        if (settings.rememberEntities.isNone()) {
            return
        }
        for (sh in hosting.toArray()) {
            restoreRememberedShard(sh, context)
        }
    }

    private func restoreRememberedShard(sh: Int64, context: ActorContext): Unit {
        let rep = match (settings.rememberEntities) {
            case Some(r) => r
            case None => return
        }
        var started: Int64 = 0
        for (id in rep.setElements(rememberedEntitiesKey(typeName, sh))) {
            if (entities.contains(id) || stopping.contains(id) || shardOf(id, numberOfShards) != sh) {
                continue
            }
            startEntity(id, context)
            started++
        }
        if (started > 0) {
            log.info("${selfAddress} 恢复 ${started} 个记住的 ${typeName} 实体（分片 ${sh}）")
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
        let sh = shardOf(id, numberOfShards)
        match (shardEntities.get(sh)) {
            case Some(ids) => ids.add(id)
            case None =>
                let ids = HashSet<String>()
                ids.add(id)
                shardEntities[sh] = ids
        }
        synchronized(shared.lock) {
            shared.entityIds.add(id)
            shared.shardCounts[sh] = (shared.shardCounts.get(sh) ?? 0) + 1
        }
        r
    }

    // ---------- 路由 ----------

    private func route(e: ShardingEnvelope, context: ActorContext): Unit {
        // 即时刷新成员视图：50ms 轮询间隙内成员可能已变化。快照未变时这里只多一次加锁 + 引用比较
        refreshView(context)
        if (left) {
            dropMessage(e, "本节点已离开集群")
            return
        }
        let sh = shardOf(e.entityId, numberOfShards)
        // 该分片已有缓冲的消息：排在后面，保持同一分片内的顺序
        if (let Some(buf) <- shardBuffers.get(sh)) {
            if (!buf.isEmpty()) {
                bufferShard(sh, e)
                return
            }
        }
        routeNow(sh, e, context)
    }

    private func routeNow(sh: Int64, e: ShardingEnvelope, context: ActorContext): Unit {
        let v = match (view) {
            case Some(v) where v.ups.size > 0 => v
            case _ =>
                bufferRegion(e)
                return
        }
        let owner = shardOwner(sh, v.ups).getOrThrow()
        if (owner == selfAddress) {
            if (hosting.contains(sh)) {
                deliver(e, context)
                return
            }
            if (acquiring.contains(sh)) {
                bufferShard(sh, e)
                return
            }
            if (let Some(to) <- yielded.get(sh)) {
                if (e.hops < 2) {
                    forward(sh, to, e)
                    return
                }
                // 让出之后消息又被转回来：请求方（视图已更新）不再认为自己是拥有者，由本节点重新获取
                yielded.remove(sh)
            }
            startAcquire(sh, context)
            if (hosting.contains(sh)) {
                deliver(e, context)
            } else {
                bufferShard(sh, e)
                sendAcquireRequests()
            }
            return
        }
        // 拥有者不可达：只缓冲这个分片（其他分片不受影响，N6）；转发次数超限：挂起等视图变化（N8）
        if (v.unreachable.contains(owner) || e.hops >= MAX_HOPS) {
            bufferShard(sh, e)
            return
        }
        forward(sh, owner, e)
    }

    private func forward(sh: Int64, to: String, e: ShardingEnvelope): Unit {
        try {
            let target = remote.remoteActorOf(NetworkAddress.parse(to), "sharding-${typeName}")
            let next = e.withHops(e.hops + 1)
            match (e.sender) {
                case Some(s) => target.tell(next, s)
                case None => target.tell(next)
            }
            synchronized(shared.lock) {
                shared.forwarded++
            }
        } catch (ex: Exception) {
            log.warn("转发 ${typeName}/${e.entityId} 到 ${to} 失败: ${ex}")
            bufferShard(sh, e)
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
                    // 在 receive 里：不同步等待 Replicator。同步等待会占住当前工作线程，
                    // Replicator 若排在同一线程上（亲和线程相同，或在本线程的 runNext 里）就要等到 10 秒超时
                    rep.addToSetAsync(rememberedEntitiesKey(typeName, shardOf(e.entityId, numberOfShards)), e.entityId)
                }
                r
        }
        if (settings.passivateAfter.isSome()) {
            lastActive[e.entityId] = monotonicMillis()
        }
        match (e.sender) {
            case Some(s) => ref.tell(e.payload, s)
            case None => ref.tell(e.payload)
        }
        synchronized(shared.lock) {
            shared.deliveredLocal++
        }
    }

    // ---------- 缓冲 ----------

    private func bufferShard(sh: Int64, e: ShardingEnvelope): Unit {
        if (bufferedTotal >= bufferSize) {
            dropMessage(e, "缓冲区已满（${bufferSize}）")
            return
        }
        match (shardBuffers.get(sh)) {
            case Some(l) => l.add(e)
            case None =>
                let l = ArrayList<ShardingEnvelope>()
                l.add(e)
                shardBuffers[sh] = l
        }
        bufferedTotal++
        updateBuffered()
    }

    private func bufferRegion(e: ShardingEnvelope): Unit {
        if (bufferedTotal >= bufferSize) {
            dropMessage(e, "缓冲区已满（${bufferSize}）")
            return
        }
        regionBuffer.add(e)
        bufferedTotal++
        updateBuffered()
    }

    /** 重新路由某个分片缓冲的消息（转发次数清零：这是一次新的路由尝试），保持顺序 */
    private func flushShard(sh: Int64, context: ActorContext): Unit {
        let buf = match (shardBuffers.remove(sh)) {
            case Some(b) => b
            case None => return
        }
        bufferedTotal -= buf.size
        for (e in buf) {
            let fresh = e.withHops(0)
            match (shardBuffers.get(sh)) {
                // 前面的消息又被缓冲了：后面的排在它后面
                case Some(b) where !b.isEmpty() => bufferShard(sh, fresh)
                case _ => routeNow(sh, fresh, context)
            }
        }
        updateBuffered()
    }

    /** 挂起的消息（拥有者不可达、转发超限）重新路由；正在获取的分片继续等待 */
    private func rerouteParked(context: ActorContext): Unit {
        if (shardBuffers.isEmpty()) {
            return
        }
        let shards = ArrayList<Int64>()
        for ((sh, _) in shardBuffers) {
            if (!acquiring.contains(sh)) {
                shards.add(sh)
            }
        }
        for (sh in shards) {
            flushShard(sh, context)
        }
    }

    private func flushRegionBuffer(context: ActorContext): Unit {
        if (regionBuffer.isEmpty()) {
            return
        }
        match (view) {
            case Some(v) where v.ups.size > 0 => ()
            case _ => return
        }
        let pending = regionBuffer.toArray()
        regionBuffer.clear()
        bufferedTotal -= pending.size
        for (e in pending) {
            route(e, context)
        }
        updateBuffered()
    }

    private func dropMessage(e: ShardingEnvelope, reason: String): Unit {
        let n = synchronized(shared.lock) {
            shared.dropped++
            shared.dropped
        }
        system.publishDeadLetter(e.payload, "sharding/${typeName}/${e.entityId}")
        if (n % 1000 == 1) {
            log.warn("${selfAddress} 丢弃发给 ${typeName}/${e.entityId} 的消息：${reason}（累计 ${n} 条，已交给死信）")
        }
    }

    /**
     * 本节点已离开集群（离开、被 down 或被脑裂处理停止）：其他节点会接管全部分片。
     * 停止全部实体，缓冲的消息交给死信（此前永远滞留在缓冲区里，N8）；之后到达的消息直接进死信
     */
    private func leaveCluster(context: ActorContext): Unit {
        left = true
        view = None
        cachedSnapshot = None
        for (id in entities.keys().toArray()) {
            stopEntity(id)
        }
        hosting.clear()
        acquiring.clear()
        yielded.clear()
        for (e in regionBuffer) {
            dropMessage(e, "本节点已离开集群")
        }
        regionBuffer.clear()
        for ((_, buf) in shardBuffers) {
            for (e in buf) {
                dropMessage(e, "本节点已离开集群")
            }
        }
        shardBuffers.clear()
        bufferedTotal = 0
        synchronized(shared.lock) {
            shared.ownerCount = 0
        }
        updateBuffered()
        updateShardStats()
    }

    private func updateBuffered(): Unit {
        synchronized(shared.lock) {
            shared.buffered = bufferedTotal
        }
    }

    private func updateShardStats(): Unit {
        synchronized(shared.lock) {
            shared.hostedShards = hosting.size
            shared.acquiringShards = acquiring.size
            shared.yieldedShards = yielded.size
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
        MessageCodecRegistry.register("cactor.ShardControl", ShardControlCodec())
        this.system = system
        this.remote = remote
        this.typeName = typeName
        this.numberOfShards = numberOfShards
        if (let Some(rep) <- settings.rememberEntities) {
            ClusterReplicatorBinding.bind(cluster, rep)
        }
        let sh = shared
        this.region = system.actorOf({ => ShardRegionActor(typeName, numberOfShards, cluster, remote, system, entityFactory, settings, sh) },
            "sharding-${typeName}-${cluster.selfAddress.replace(":", "_").replace(".", "_")}")
        remote.exposeActor("sharding-${typeName}", region)
    }

    /** 发给实体（消息需要能被远程序列化） */
    public func tell(entityId: String, message: Message): Unit {
        region.tell(ShardingEnvelope(entityId, message))
    }

    /**
     * 带发送方发给实体（E12′）：实体用 context.reply / senderRef 回复到 sender，
     * 实体在其他节点时回复经远程直接送回 sender（sender 须是本地 Actor 或远程引用）
     */
    public func tell(entityId: String, message: Message, sender: ActorRef): Unit {
        region.tell(ShardingEnvelope(entityId, message, 0, Some(sender)))
    }

    /**
     * 请求-响应（E12′，对标 Akka `EntityRef.ask`）：实体用 context.reply 回复，timeout 内没有回复时
     * Promise 以 TypedAskTimeoutException 失败。不阻塞：Actor 内用 onComplete / pipeToSelf 接收结果
     *
     * ```
     * let total = carts.ask("user-42", StringMessage("total"), 2.seconds).get(3.seconds)
     * ```
     */
    public func ask(entityId: String, message: Message, timeout: Duration): TypedPromise<Message> {
        askVia("${typeName}/${entityId}", timeout, { replyTo => tell(entityId, message, replyTo) })
    }

    /** 本节点当前运行的实体 */
    public func localEntityIds(): Array<String> {
        synchronized(shared.lock) {
            shared.entityIds.toArray()
        }
    }

    /** 分片区域当前用于路由的 Up 成员数（0 = 还没有可用视图）。用于诊断/测试等待成员视图就绪。 */
    public func ownerCount(): Int64 {
        synchronized(shared.lock) {
            shared.ownerCount
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

    /**
     * 分片的移交状态：(本节点托管的分片数, 正在获取的分片数, 已完成的获取次数)。
     * 获取 = 征得其他成员"已停止该分片"的确认（E13′），完成后才在本节点创建实体
     */
    public func shardStats(): (Int64, Int64, Int64) {
        synchronized(shared.lock) {
            (shared.hostedShards, shared.acquiringShards, shared.acquisitionsCompleted)
        }
    }

    /** remember-entities 主动恢复（不经新消息）启动的实体数 */
    public func restartedCount(): Int64 {
        synchronized(shared.lock) {
            shared.restarted
        }
    }

    /**
     * 本节点各分片当前运行的实体数（负载报告，E13′/再平衡基础）。
     * 返回的映射为分片号 -> 实体数的快照；未托管的分片计数为 0。
     * 跨节点的负载均衡决策应结合所有成员的报告；本方法只报告本节点。
     */
    public func shardLoad(): Map<Int64, Int64> {
        let out = HashMap<Int64, Int64>()
        synchronized(shared.lock) {
            for (sh in 0..numberOfShards) {
                out[sh] = shared.shardCounts.get(sh) ?? 0
            }
        }
        out
    }

    /**
     * 负载最重的 n 个分片号（按本节点实体数降序），供再平衡决策使用。
     * n 大于分片数时返回全部分片。
     */
    public func busiestShards(n: Int64): Array<Int64> {
        let load = shardLoad()
        let entries = ArrayList<(Int64, Int64)>()
        for ((sh, c) in load) {
            entries.add((sh, c))
        }
        // 简单选择排序（分片数通常 ≤ 几百，性能可接受）
        let size = entries.size
        let limit = if (n < size) { n } else { size }
        let result = ArrayList<Int64>()
        let used = HashSet<Int64>()
        for (_ in 0..limit) {
            var best: Int64 = -1
            var bestLoad: Int64 = -1
            for (i in 0..size) {
                let (sh, c) = entries[i]
                if (!used.contains(sh) && c > bestLoad) {
                    bestLoad = c
                    best = sh
                }
            }
            if (best >= 0) {
                result.add(best)
                used.add(best)
            }
        }
        result.toArray()
    }

    public func stop(): Unit {
        remote.unexposeActor("sharding-${typeName}")
        system.stop(region)
    }
}
