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
 * - 按分片租约（DA4，对标 Akka sharding use-lease，默认关闭）：许可式移交只防得住"双方视图都包含对方"的情形；
 *   非对称分区 / 单侧 down / 长停顿下旧宿主仍在托管时，新宿主的 askSet 里已没有它，移交会立即"完成"。
 *   ShardingSettings.withLease 后，移交确认收齐还必须取得该分片的租约（后台线程获取，失败指数退避，绝不超时强行托管）
 *   才创建实体；持有期间失去租约立即停止该分片的实体并放弃托管；移交 / 放弃获取 / 区域停止时，实体全部终止后交还租约，
 *   移交时先交还再回 STOPPED。实体用 ShardLeases.leaseForEntity 取得守护自己的租约，包装成
 *   FencedEventStore / FencedDurableStateStore 后写入自动携带 fencing token。
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
import cactor_foundation.coordination.{Lease}

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

/** 分片租约的名字（每种实体类型的每个分片一个；ShardLeases 的工厂收到的就是它） */
public func shardLeaseName(typeName: String, shardId: Int64): String {
    "shard-${typeName}-${shardId}"
}

/**
 * 按分片的租约来源（DA4，对标 Akka sharding use-lease）：每个节点一个，工厂按租约名（shardLeaseName）
 * 创建本节点在该分片上的 Lease（同一个 leaseName、本节点唯一的 ownerName）。
 *
 * 为什么是一个对象而不是裸闭包：同一个分片的 Lease 必须在区域与实体之间共享——区域用它判断能否托管，
 * 实体用它的 fencing token 包装持久化存储（FencedEventStore / FencedDurableStateStore 直接接受 Lease）。
 * 同名的两个 Lease 对象互相续约会让对方失去租约，所以这里按名字记忆，保证一个名字只有一个对象。
 *
 * ```
 * let leases = ShardLeases({ name => FileLease("/shared/leases", LeaseSettings(name, "${addr}#${uid}")) })
 * let carts = ClusterSharding(cluster, system, remote, "cart", 64, { id =>
 *     let store = FencedDurableStateStore(inner, fence, leases.leaseForEntity("cart", 64, id))
 *     CartActor(id, store)
 * }, ShardingSettings.defaults().withLease(leases))
 * ```
 */
public class ShardLeases {
    private let factory: (String) -> Lease
    private let lock = Mutex()
    private let leases = HashMap<String, Lease>()

    public init(factory: (String) -> Lease) {
        this.factory = factory
    }

    /** 名为 leaseName 的租约（首次调用时创建，之后返回同一个对象）；工厂返回的租约名必须与之一致 */
    public func leaseFor(leaseName: String): Lease {
        synchronized(lock) {
            if (let Some(l) <- leases.get(leaseName)) {
                return l
            }
            let l = factory(leaseName)
            if (l.settings.leaseName != leaseName) {
                // 名字不一致会让多个分片共用一个后端记录：同一持有者重复获取会互相顶掉对方的 token
                throw IllegalArgumentException("分片租约工厂返回的租约名 ${l.settings.leaseName} 与请求的 ${leaseName} 不一致")
            }
            leases[leaseName] = l
            l
        }
    }

    /** 保护 typeName 第 shardId 个分片的租约 */
    public func leaseForShard(typeName: String, shardId: Int64): Lease {
        leaseFor(shardLeaseName(typeName, shardId))
    }

    /** 保护实体 entityId 所在分片的租约（实体用它包装持久化存储，写入自动携带 fencing token） */
    public func leaseForEntity(typeName: String, numberOfShards: Int64, entityId: String): Lease {
        leaseForShard(typeName, shardOf(entityId, numberOfShards))
    }

    /** 实体所在分片的当前 fencing token（本节点未持有该分片的租约时为 None） */
    public func fencingToken(typeName: String, numberOfShards: Int64, entityId: String): Option<Int64> {
        leaseForEntity(typeName, numberOfShards, entityId).fencingToken()
    }

    /** 已创建过的租约（诊断用） */
    func created(): Array<Lease> {
        synchronized(lock) {
            leases.values().toArray()
        }
    }
}

/** 分片的可选行为 */
public class ShardingSettings {
    /** Region 中等待路由的消息上限，超出丢弃最旧的 */
    public let bufferSize: Int64
    /** 空闲超过该时长的实体被停止（钝化）；None 表示不钝化 */
    public let passivateAfter: Option<Duration>
    /** 记住实体 ID 的复制器；Some 表示开启 remember-entities */
    public let rememberEntities: Option<Replicator>
    /** 自动再平衡策略；决策逻辑见 rebalance.cj 的 decideRebalance（纯函数） */
    public let rebalance: RebalancePolicy
    /**
     * 获取分片等待该时长后开始告警、并计入 acquisitionHealth 的 slow 计数（默认 10 s）。
     * 只影响观测，不影响正确性：获取仍会一直重发直到对方确认（否则可能两个节点同时托管同一分片）。
     */
    public let acquireWarnAfter: Duration
    /**
     * 按分片的租约（DA4，默认 None = 不使用，行为与之前完全相同）。启用后，许可式移交完成还不够，
     * 必须再取得该分片的租约才托管、才创建实体；获取失败按 leaseRetryMin ~ leaseRetryMax 指数退避，绝不超时强行托管。
     */
    public let lease: Option<ShardLeases>
    public let leaseRetryMin: Duration
    public let leaseRetryMax: Duration

    public init(bufferSize: Int64, passivateAfter: Option<Duration>, rememberEntities: Option<Replicator>,
        rebalance!: RebalancePolicy = RebalancePolicy(),
        acquireWarnAfter!: Duration = Duration.second * 10,
        lease!: Option<ShardLeases> = None<ShardLeases>,
        leaseRetryMin!: Duration = Duration.millisecond * 200,
        leaseRetryMax!: Duration = Duration.second * 5) {
        if (bufferSize <= 0) {
            throw IllegalArgumentException("缓冲区大小必须为正数: ${bufferSize}")
        }
        if (passivateAfter.isSome() && rememberEntities.isSome()) {
            throw IllegalArgumentException("remember-entities 与自动钝化不能同时开启")
        }
        this.bufferSize = bufferSize
        this.passivateAfter = passivateAfter
        this.rememberEntities = rememberEntities
        this.rebalance = rebalance
        if (acquireWarnAfter <= Duration.Zero) {
            throw IllegalArgumentException("acquireWarnAfter 必须为正数: ${acquireWarnAfter}")
        }
        this.acquireWarnAfter = acquireWarnAfter
        if (leaseRetryMin <= Duration.Zero || leaseRetryMax < leaseRetryMin) {
            throw IllegalArgumentException("租约重试间隔必须为正且 max >= min")
        }
        this.lease = lease
        this.leaseRetryMin = leaseRetryMin
        this.leaseRetryMax = leaseRetryMax
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

    // 所有 withXxx 经这一处构造：新增字段时只需改这里与各自的参数
    private func make(bufferSize: Int64, passivateAfter: Option<Duration>, rememberEntities: Option<Replicator>,
        rebalance: RebalancePolicy, acquireWarnAfter: Duration, lease: Option<ShardLeases>, leaseRetryMin: Duration,
        leaseRetryMax: Duration): ShardingSettings {
        ShardingSettings(bufferSize, passivateAfter, rememberEntities, rebalance: rebalance,
            acquireWarnAfter: acquireWarnAfter, lease: lease, leaseRetryMin: leaseRetryMin, leaseRetryMax: leaseRetryMax)
    }

    /** 换再平衡策略（链式：ShardingSettings.passivating(1.minutes).withRebalance(...)） */
    public func withRebalance(policy: RebalancePolicy): ShardingSettings {
        make(bufferSize, passivateAfter, rememberEntities, policy, acquireWarnAfter, lease, leaseRetryMin, leaseRetryMax)
    }

    public func withBufferSize(size: Int64): ShardingSettings {
        make(size, passivateAfter, rememberEntities, rebalance, acquireWarnAfter, lease, leaseRetryMin, leaseRetryMax)
    }

    public func withPassivation(idle: Duration): ShardingSettings {
        make(bufferSize, Some(idle), None<Replicator>, rebalance, acquireWarnAfter, lease, leaseRetryMin, leaseRetryMax)
    }

    /** 换获取等待的告警阈值（只影响观测；链式与 withRebalance 一致） */
    public func withAcquireWarnAfter(after: Duration): ShardingSettings {
        make(bufferSize, passivateAfter, rememberEntities, rebalance, after, lease, leaseRetryMin, leaseRetryMax)
    }

    /** 启用按分片租约（DA4）；leases 在本节点的区域与实体之间共享 */
    public func withLease(leases: ShardLeases): ShardingSettings {
        make(bufferSize, passivateAfter, rememberEntities, rebalance, acquireWarnAfter, Some(leases), leaseRetryMin,
            leaseRetryMax)
    }

    /** 启用按分片租约：factory 收到租约名（shardLeaseName），返回本节点在该分片上的 Lease */
    public func withLease(factory: (String) -> Lease): ShardingSettings {
        withLease(ShardLeases(factory))
    }

    public func withoutLease(): ShardingSettings {
        make(bufferSize, passivateAfter, rememberEntities, rebalance, acquireWarnAfter, None<ShardLeases>,
            leaseRetryMin, leaseRetryMax)
    }

    /** 租约获取失败后的指数退避区间（默认 200 ms ~ 5 s，对标 Akka lease-retry-interval） */
    public func withLeaseRetry(min: Duration, max: Duration): ShardingSettings {
        make(bufferSize, passivateAfter, rememberEntities, rebalance, acquireWarnAfter, lease, min, max)
    }
}

/**
 * 按分片租约的状态快照（运维观测，DA4）。enabled = false 时其余字段为空 / 0。
 *   - held：本节点持有租约的分片及 token；
 *   - waiting：移交确认已收齐、只差租约的分片及已等待毫秒数（"waiting for lease"）——
 *     长时间不为空通常意味着旧宿主仍在续约（分区 / 停顿的旧宿主），或租约存储不可达；
 *   - attempts / failures / lost：累计发起获取、获取失败（含被他人占用与后端异常）、持有期间失去的次数。
 */
public struct ShardLeaseStatus <: ToString {
    public let enabled: Bool
    public let owner: String
    public let held: Array<(Int64, Int64)>
    public let waiting: Array<(Int64, Int64)>
    public let attempts: Int64
    public let failures: Int64
    public let lost: Int64

    public init(enabled: Bool, owner: String, held: Array<(Int64, Int64)>, waiting: Array<(Int64, Int64)>,
        attempts: Int64, failures: Int64, lost: Int64) {
        this.enabled = enabled
        this.owner = owner
        this.held = held
        this.waiting = waiting
        this.attempts = attempts
        this.failures = failures
        this.lost = lost
    }

    /** 某个分片当前是否由本节点持有租约 */
    public func holds(shardId: Int64): Bool {
        held.iterator().any({ p => p[0] == shardId })
    }

    /** 某个分片是否正在等待租约 */
    public func isWaiting(shardId: Int64): Bool {
        waiting.iterator().any({ p => p[0] == shardId })
    }

    public func toString(): String {
        "ShardLeaseStatus(启用 ${enabled}, 持有 ${held.size}, 等待 ${waiting.size}, 获取 ${attempts}, 失败 ${failures}, 失去 ${lost})"
    }
}

/**
 * 分片获取的健康度快照（运维观测，E13′ 补充）。
 *
 * 获取是两阶段移交：本方必须等到其他每个可能托管者回复"已停止"，否则两个节点可能同时托管同一分片。
 * 因此这里**不设超时强推**——只把"卡了多久、在等谁"变成可查询、可告警的数据：
 *   - `slow`：等待超过 `ShardingSettings.acquireWarnAfter` 的分片数；
 *   - `oldestMs`：最久的一个已经等了多久；
 *   - `pendingMembers`：这些分片还在等的成员地址（通常是没跑同类型 ClusterSharding 的节点）；
 *   - `slowWarnings`：累计告警次数（每过一个阈值窗口 +1）。
 */
public struct AcquisitionHealth <: ToString {
    public let hosted: Int64
    public let acquiring: Int64
    public let slow: Int64
    public let oldestMs: Int64
    public let pendingMembers: Array<String>
    public let slowWarnings: Int64
    /** 移交确认已收齐、只差租约的分片数（DA4；未启用租约时恒为 0），计入 acquiring */
    public let awaitingLease: Int64

    public init(hosted: Int64, acquiring: Int64, slow: Int64, oldestMs: Int64, pendingMembers: Array<String>, slowWarnings: Int64) {
        this(hosted, acquiring, slow, oldestMs, pendingMembers, slowWarnings, 0)
    }

    public init(hosted: Int64, acquiring: Int64, slow: Int64, oldestMs: Int64, pendingMembers: Array<String>,
        slowWarnings: Int64, awaitingLease: Int64) {
        this.hosted = hosted
        this.acquiring = acquiring
        this.slow = slow
        this.oldestMs = oldestMs
        this.pendingMembers = pendingMembers
        this.slowWarnings = slowWarnings
        this.awaitingLease = awaitingLease
    }

    public func toString(): String {
        "AcquisitionHealth(托管 ${hosted}, 获取中 ${acquiring}, 等租约 ${awaitingLease}, 慢 ${slow}, 最久 ${oldestMs} ms, 等待 ${String.join(pendingMembers, delimiter: ",")})"
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
/** 手动/自动再平衡：把指定分片的拥有者改为 targetAddress（E13′ 之后的再平衡基础） */
public class ShardRebalance <: Message {
    public let shardId: Int64
    public let targetAddress: String
    public init(shardId: Int64, targetAddress: String) {
        this.shardId = shardId
        this.targetAddress = targetAddress
    }
    public func messageType(): String { "cactor.ShardRebalance" }
}

class ShardRebalanceCodec <: MessageCodec {
    public func encode(message: Message): Array<UInt8> {
        let c = (message as ShardRebalance).getOrThrow()
        FieldWriter().put(c.shardId).put(c.targetAddress).done().toArray()
    }
    public func decode(payload: Array<UInt8>): Message {
        let r = FieldReader(String.fromUtf8(payload))
        let sh = Int64.parse(r.next())
        let addr = r.next()
        ShardRebalance(sh, addr)
    }
}

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


/** 负载报告：某节点各分片的实体数，供领导者做再平衡决策 */
public class ShardLoadReport <: Message {
    public let nodeAddress: String
    public let loads: Array<(Int64, Int64)>

    public init(nodeAddress: String, loads: Array<(Int64, Int64)>) {
        this.nodeAddress = nodeAddress
        this.loads = loads
    }
    public func messageType(): String { "cactor.ShardLoadReport" }
}

class ShardLoadReportCodec <: MessageCodec {
    public func encode(message: Message): Array<UInt8> {
        let c = (message as ShardLoadReport).getOrThrow()
        let w = FieldWriter().put(c.nodeAddress).put(c.loads.size)
        for ((sh, cnt) in c.loads) {
            w.put(sh).put(cnt)
        }
        w.done().toArray()
    }
    public func decode(payload: Array<UInt8>): Message {
        let r = FieldReader(String.fromUtf8(payload))
        let addr = r.next()
        let n = Int64.parse(r.next())
        if (n < 0 || n > 1_000_000) {
            throw IllegalArgumentException("负载报告损坏：条目数 ${n}")
        }
        let loads = ArrayList<(Int64, Int64)>(n)
        for (_ in 0..n) {
            let sh = Int64.parse(r.next())
            let cnt = Int64.parse(r.next())
            loads.add((sh, cnt))
        }
        ShardLoadReport(addr, loads.toArray())
    }
}

// ======================= Region =======================

class RegionTick <: Message {
    public func messageType(): String {
        "ShardRegionTick"
    }
}

/** 请求 Region 立即做一次再平衡评估（运维 / 测试入口；领导者本来就会周期评估，见 evaluateRebalance） */
class RebalanceEvaluate <: Message {
    public func messageType(): String {
        "cactor.ShardRebalanceEvaluate"
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
    // 获取健康度快照（sendAcquireRequests 每次重发时更新，供 /cluster/shards 与 acquisitionHealth）
    var slowAcquires: Int64 = 0
    var oldestAcquireMs: Int64 = 0
    var acquireSlowWarnings: Int64 = 0
    let pendingAcquireMembers = HashSet<String>()
    // 每个分片上当前运行的实体数（供负载报告 / 再平衡决策，E62/E13′ 后续）
    let shardCounts = HashMap<Int64, Int64>()
    // ---- 按分片租约（DA4）：区域线程维护、管理端点读取 ----
    let leaseHeldTokens = HashMap<Int64, Int64>()
    // 等租约的分片 -> 开始等待的单调时刻
    let leaseWaitingSince = HashMap<Int64, Int64>()
    var leaseAttempts: Int64 = 0
    var leaseFailures: Int64 = 0
    var leaseLost: Int64 = 0
    // 区域已停止：后台线程上刚拿到的租约没人处理，由该线程自己交还
    var regionStopped = false
    // 后台已获取成功、区域还没处理结果的租约（区域在这期间停止时由 postStop 交还）
    let unconfirmedLeases = HashMap<Int64, Lease>()
}

/** 分片租约获取结果（获取在独立线程上执行，结果回送区域） */
class ShardLeaseResult <: Message {
    let shard: Int64
    let attempt: Int64
    let acquired: Bool
    let failure: Option<Exception>

    init(shard: Int64, attempt: Int64, acquired: Bool, failure: Option<Exception>) {
        this.shard = shard
        this.attempt = attempt
        this.acquired = acquired
        this.failure = failure
    }

    public func messageType(): String {
        "ShardLeaseResult"
    }
}

/** 分片租约的释放已完成（释放同样不在区域线程上执行） */
class ShardLeaseReleased <: Message {
    let shard: Int64

    init(shard: Int64) {
        this.shard = shard
    }

    public func messageType(): String {
        "ShardLeaseReleased"
    }
}

/** 持有期间失去分片租约（租约内部线程回调后发给区域） */
class ShardLeaseLost <: Message {
    let shard: Int64
    let attempt: Int64
    let reason: Option<Exception>

    init(shard: Int64, attempt: Int64, reason: Option<Exception>) {
        this.shard = shard
        this.attempt = attempt
        this.reason = reason
    }

    public func messageType(): String {
        "ShardLeaseLost"
    }
}

/**
 * 区域对一个分片租约的视角。所有字段只在区域线程上读写。
 * inFlight：后台线程上有一个 acquire / release 在执行——Lease 约定同一对象不并发调用 acquire / release，
 * 所以在它完成（结果消息到达）之前不再发起新的操作。attempt 用来识别作废的获取结果与旧的 lost 信号。
 */
class ShardLeaseSlot {
    let lease: Lease
    var held = false
    var inFlight = false
    var attempt: Int64 = 0
    var nextAttemptMs: Int64 = 0
    var backoffMs: Int64

    init(lease: Lease, backoffMs: Int64) {
        this.lease = lease
        this.backoffMs = backoffMs
    }
}

/** 本节点正在获取的分片：等其他成员确认已停止它 */
class Acquisition {
    let round: Int64
    let pending = HashSet<String>()
    let acked = HashSet<String>()
    let startedMs: Int64
    var lastSentMs: Int64 = -1
    var lastWarnMs: Int64 = -1
    // 移交确认已收齐、正在等租约（DA4）：此时仍算"获取中"，消息继续缓冲，不创建实体
    var awaitingLease = false

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
let REROUTE_PARKED_TICKS: Int64 = 20
// 负载报告与迁移间隔的默认值在 RebalancePolicy 里（见 rebalance.cj），区域只读 settings.rebalance

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
    // 手动/自动再平衡的归属覆盖：分片号 -> 拥有者地址（优先于 rendezvous 哈希）
    let placementOverrides = HashMap<Int64, String>()
    // 其他成员上报的负载：节点地址 -> (分片号 -> 实体数)，以及各报告的时刻（过期即丢弃）
    let peerLoads = HashMap<String, HashMap<Int64, Int64>>()
    let peerLoadAt = HashMap<String, Int64>()
    // 上次自动迁移的时刻：两次迁移之间至少间隔 settings.rebalance.cooldown，避免分片来回搬
    var lastAutoRebalanceMs: Int64 = Int64.Min / 2
    // 上次负载上报的签名与时刻：负载不变时只在 TTL 的一半后刷新，避免每 tick 都推报告
    var lastReportSignature: String = ""
    var lastReportMs: Int64 = Int64.Min / 2
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
    // ---- 按分片租约（DA4）：未启用时 leaseSlots 始终为空 ----
    let leaseSlots = HashMap<Int64, ShardLeaseSlot>()
    var leaseAttemptSeq: Int64 = 0
    var selfRef: Option<ActorRef> = None
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
        selfRef = Some(context.self)
        context.timers.startTimerWithFixedDelay("tick", RegionTick(), Duration.millisecond * 50)
    }

    /**
     * 区域停止（ClusterSharding.stop / 系统关闭）：子 Actor（实体）此时已全部终止，交还持有的全部分片租约，
     * 新宿主不必等 ttl 过期。这里同步释放——区域已经不再处理消息，没有可阻塞的后续工作；
     * 后台还在执行的获取若随后成功，由它自己的线程看到区域已停止后立即交还（见 requestShardLease）
     */
    public func postStop(): Unit {
        let orphans = synchronized(shared.lock) {
            shared.regionStopped = true
            let o = shared.unconfirmedLeases.values().toArray()
            shared.unconfirmedLeases.clear()
            shared.leaseHeldTokens.clear()
            shared.leaseWaitingSince.clear()
            o
        }
        let toRelease = ArrayList<Lease>(orphans)
        for ((_, slot) in leaseSlots) {
            slot.attempt = -1
            if (slot.held) {
                slot.held = false
                toRelease.add(slot.lease)
            }
        }
        for (l in toRelease) {
            try {
                l.release()
            } catch (e: Exception) {
                log.warn("${typeName} 区域停止时释放分片租约失败（将在 ttl 后过期）：${e}")
            }
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: RegionTick =>
                ticks++
                refreshView(context)
                sendLoadReport(context)
                sendAcquireRequests()
                passivateIdle(context)
                tickLeases(context)
                if (ticks % REROUTE_PARKED_TICKS == 0) {
                    rerouteParked(context)
                }
                // 领导者每 2s 无条件评估一次（报告的到达顺序不该决定是否评估），
                // 报告不全或视图不稳定时 evaluateRebalance 自己会放弃
                if (ticks % 40 == 0 && isLeader(cluster.state())) {
                    evaluateRebalance(context)
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
            case rb: ShardRebalance => onRebalance(rb, context)
            case rl: ShardLoadReport => onLoadReport(rl, context)
            case _: RebalanceEvaluate => evaluateRebalance(context)
            case t: Terminated => onEntityTerminated(t.actor.path.toString(), context)
            case r: ShardLeaseResult => onShardLeaseResult(r, context)
            case r: ShardLeaseReleased => onShardLeaseReleased(r.shard, context)
            case l: ShardLeaseLost => onShardLeaseLost(l.shard, l.attempt, l.reason, context)
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
        match (placementOverrides.get(sh)) {
            case Some(addr) =>
                // 覆盖目标必须是当前视图中的成员，否则回退到 rendezvous 哈希
                match (view) {
                    case Some(v) =>
                        let set = HashSet<String>(v.ups)
                        if (set.contains(addr)) { Some(addr) } else { shardOwner(sh, v.ups) }
                    case None => Some(addr)
                }
            case None =>
                match (view) {
                    case Some(v) => shardOwner(sh, v.ups)
                    case None => None<String>
                }
        }
    }

    private func onRebalance(rb: ShardRebalance, context: ActorContext): Unit {
        // 校验分片号范围
        if (rb.shardId < 0 || rb.shardId >= numberOfShards) {
            return
        }
        // 已经是相同覆盖：避免重复传播
        match (placementOverrides.get(rb.shardId)) {
            case Some(a) where a == rb.targetAddress => return
            case _ => ()
        }
        placementOverrides[rb.shardId] = rb.targetAddress
        // 让出记录作废，按新归属重新判断
        yielded.remove(rb.shardId)
        onOwnershipChanged(context)
        log.info("${selfAddress} 再平衡：分片 ${rb.shardId} -> ${rb.targetAddress}")
        // 传播给其他成员（发起者也发给自己已处理；这里发给非自身的成员）。
        // 用可靠通道（E41）：放置覆盖必须每个成员都收到，否则各节点对同一分片的归属判断不一致，
        // 分片会在两边反复获取/让出（单活由许可式移交保证，但会白搬）。
        let s = cluster.state()
        for (m in s.members) {
            if (m.address != selfAddress && m.status.toString() != "Removed") {
                let ref = remote.remoteActorOf(NetworkAddress.parse(m.address), "sharding-${typeName}")
                remote.tellReliable(ref, ShardRebalance(rb.shardId, rb.targetAddress))
            }
        }
    }

    private func sendLoadReport(context: ActorContext): Unit {
        // 策略关闭时连报告都不发：既省跨节点流量，也避免"看起来在监控负载却从不决策"的误解
        if (!settings.rebalance.enabled) {
            return
        }
        let s = cluster.state()
        if (s.members.size <= 1) {
            return
        }
        match (leaderAddress(s)) {
            case None => return
            case Some(a) where a == selfAddress => return
            case Some(leader) =>
                // 负载没变就不发：每个 tick 都推报告在大集群里是纯噪声；但必须在 TTL 内刷新一次，
                // 否则领导者会把本节点当成没有报告、干脆不做再平衡。
                let now = monotonicMillis()
                let ttlMs = settings.rebalance.reportTtl.toMilliseconds()
                let refreshMs = if (ttlMs > 2) { ttlMs / 2 } else { 1 }
                let loads = ArrayList<(Int64, Int64)>()
                let parts = ArrayList<String>()
                synchronized(shared.lock) {
                    for (sh in 0..numberOfShards) {
                        let c = shared.shardCounts.get(sh) ?? 0
                        loads.add((sh, c))
                        parts.add("${sh}=${c}")
                    }
                }
                let signature = String.join(parts.toArray(), delimiter: ",")
                if (signature == lastReportSignature && now - lastReportMs < refreshMs) {
                    return
                }
                let ref = remote.remoteActorOf(NetworkAddress.parse(leader), "sharding-${typeName}")
                ref.tell(ShardLoadReport(selfAddress, loads.toArray()))
                lastReportSignature = signature
                lastReportMs = now
        }
    }

    /** 当前视图里地址最小的 Up 成员（与 Cluster 的 Leader 判定一致）；没有 Up 成员时为 None */
    private func leaderAddress(s: ClusterSnapshot): Option<String> {
        var leader = ""
        for (m in s.members) {
            if (m.status.toString() == "Up" && (leader.isEmpty() || m.address < leader)) {
                leader = m.address
            }
        }
        if (leader.isEmpty()) {
            return None<String>
        }
        Some(leader)
    }

    private func isLeader(s: ClusterSnapshot): Bool {
        match (leaderAddress(s)) {
            case Some(a) => a == selfAddress
            case None => false
        }
    }

    /** 丢弃已不是 Up 成员的报告，以及超过 reportTtl 没有更新的报告 */
    private func prunePeerLoads(now: Int64, ups: HashSet<String>): Unit {
        let ttlMs = settings.rebalance.reportTtl.toMilliseconds()
        let stale = ArrayList<String>()
        for ((addr, at) in peerLoadAt) {
            if (!ups.contains(addr) || now - at > ttlMs) {
                stale.add(addr)
            }
        }
        for (addr in stale) {
            peerLoadAt.remove(addr)
            peerLoads.remove(addr)
        }
    }

    private func onLoadReport(rl: ShardLoadReport, context: ActorContext): Unit {
        // 只接受当前 Up 成员的报告：伪造地址、已离开节点的陈旧报告不得影响分片放置
        let s = cluster.state()
        var isMember = false
        for (m in s.members) {
            if (m.address == rl.nodeAddress && m.status.toString() == "Up") {
                isMember = true
                break
            }
        }
        if (!isMember || rl.nodeAddress == selfAddress) {
            return
        }
        let m = HashMap<Int64, Int64>()
        for ((sh, c) in rl.loads) {
            if (sh >= 0 && sh < numberOfShards && c >= 0) {
                m[sh] = c
            }
        }
        peerLoads[rl.nodeAddress] = m
        peerLoadAt[rl.nodeAddress] = monotonicMillis()
        if (isLeader(s)) {
            evaluateRebalance(context)
        }
    }

    /**
     * 领导者按各成员上报的负载评估一次再平衡：把**最忙节点**上最重的分片迁到最空闲节点。
     *
     * 决策本身在 `decideRebalance`（rebalance.cj，纯函数）：这里只负责把集群状态翻译成视图快照，
     * 再按结果执行。这样阈值/冷却/放弃理由都能单测，也允许任意成员作为源（旧实现只搬领导者自己的分片）。
     */
    private func evaluateRebalance(context: ActorContext): Unit {
        let s = cluster.state()
        // 聚合各节点的（分片 -> 实体数）：本节点直接读，其他成员用报告（不可达为 0，报告由 prune 处理过期）
        let ups = ArrayList<String>()
        for (m in s.members) {
            if (m.status.toString() == "Up") {
                ups.add(m.address)
            }
        }
        let now = monotonicMillis()
        let upSet = HashSet<String>(ups)
        prunePeerLoads(now, upSet)
        let loads = HashMap<String, Map<Int64, Int64>>()
        synchronized(shared.lock) {
            let selfLoad = HashMap<Int64, Int64>()
            for (sh in 0..numberOfShards) {
                selfLoad[sh] = shared.shardCounts.get(sh) ?? 0
            }
            loads[selfAddress] = selfLoad
        }
        for ((addr, l) in peerLoads) {
            if (addr != selfAddress) {
                loads[addr] = l
            }
        }
        let view = RebalanceView(ups.toArray(), loads, unreachable: Int64(s.unreachable.size),
            lastMoveAtMs: lastAutoRebalanceMs, nowMs: now)
        match (decideRebalance(view, settings.rebalance)) {
            case RebalanceDecision.Skip(reason) =>
                log.debug("跳过再平衡（${rebalanceSkipText(reason)}）：成员 ${ups.size}，报告 ${peerLoads.size}，不可达 ${s.unreachable.size}")
            case RebalanceDecision.Move(mv) =>
                lastAutoRebalanceMs = now
                log.info("自动再平衡：分片 ${mv.shardId}（${mv.entityCount} 个实体）从 ${mv.source} -> ${mv.target}；节点实体数 ${mv.sourceTotal} / ${mv.targetTotal}")
                // 走与手动再平衡相同的路径：设覆盖、让出、广播给所有成员；源节点收到后停止该分片
                onRebalance(ShardRebalance(mv.shardId, mv.target), context)
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
        // 放弃的获取、移出且实体已全部终止的分片：交还租约（还有实体的，等 Terminated 后再交还）
        if (!leaseSlots.isEmpty()) {
            for (sh in leaseSlots.keys().toArray()) {
                reconcileShardLease(sh, context)
            }
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
            publishAcquireHealth(0, 0, HashSet<String>())
            return
        }
        let now = monotonicMillis()
        let warnMs = settings.acquireWarnAfter.toMilliseconds()
        var slow: Int64 = 0
        var oldest: Int64 = 0
        let waiting = HashSet<String>()
        let perMember = HashMap<String, ArrayList<(Int64, Int64)>>()
        for ((sh, a) in acquiring) {
            if (a.pending.isEmpty()) {
                continue
            }
            let waited = now - a.startedMs
            if (waited >= warnMs) {
                slow++
                if (waited > oldest) {
                    oldest = waited
                }
                for (m in a.pending) {
                    waiting.add(m)
                }
            }
            if (a.lastSentMs >= 0 && now - a.lastSentMs < HANDOFF_RESEND_MS) {
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
            // 每经过一个阈值窗口再告警一次：一次性的 warn 在长时间故障里会被后续日志淹没
            if (a.lastWarnMs < 0 || waited - a.lastWarnMs >= warnMs) {
                a.lastWarnMs = waited
                synchronized(shared.lock) {
                    shared.acquireSlowWarnings++
                }
                log.warn("${selfAddress} 获取 ${typeName} 分片 ${sh} 已等待 ${waited} ms，仍未确认的成员：${a.pending.toArray()}（这些节点需要运行同类型的 ClusterSharding 且网络可达）")
            }
        }
        publishAcquireHealth(slow, oldest, waiting)
        for ((m, items) in perMember) {
            sendControl(m, "BEGIN", items.toArray())
        }
    }

    /** 把获取健康度写进共享区：告警之外，运维还能从 /cluster/shards 看到卡在等谁 */
    private func publishAcquireHealth(slow: Int64, oldest: Int64, waiting: HashSet<String>): Unit {
        synchronized(shared.lock) {
            shared.slowAcquires = slow
            shared.oldestAcquireMs = oldest
            shared.pendingAcquireMembers.clear()
            for (m in waiting) {
                shared.pendingAcquireMembers.add(m)
            }
        }
    }

    /**
     * 移交确认已收齐。未启用租约：立即托管。启用租约（DA4）：还必须持有该分片的租约——没有就发起获取
     * （后台线程执行，结果以消息回送），分片保持"获取中"、消息继续缓冲；获取失败只退避重试，
     * 不会因为等了太久就强行托管：拿不到租约说明旧宿主可能还活着（分区 / 停顿），宁可不可用也不双活。
     */
    private func completeAcquire(sh: Int64, context: ActorContext): Unit {
        let a = match (acquiring.get(sh)) {
            case Some(x) => x
            case None => return
        }
        if (!a.pending.isEmpty()) {
            return
        }
        if (settings.lease.isSome()) {
            let slot = match (leaseSlot(sh)) {
                case Some(x) => x
                case None =>
                    // 租约对象创建失败（工厂异常）：不托管，下个 tick 再试
                    a.awaitingLease = true
                    markLeaseWaiting(sh)
                    return
            }
            if (slot.held && !slot.lease.checkLease()) {
                // 本地有效期已过而 lost 回调还没到（回调线程被饿死）：按失去处理，重新获取
                markLeaseLost(sh, slot, None<Exception>, context)
            }
            if (!slot.held) {
                a.awaitingLease = true
                markLeaseWaiting(sh)
                requestShardLease(sh, slot)
                return
            }
        }
        acquiring.remove(sh)
        clearLeaseWaiting(sh)
        hosting.add(sh)
        synchronized(shared.lock) {
            shared.acquisitionsCompleted++
        }
        restoreRememberedShard(sh, context)
        flushShard(sh, context)
        updateShardStats()
    }

    // ---------- 按分片租约（DA4，对标 Akka sharding use-lease） ----------

    /** 分片的租约槽（首次需要时经 ShardLeases 创建）；未启用租约或工厂失败时为 None */
    private func leaseSlot(sh: Int64): Option<ShardLeaseSlot> {
        let leases = match (settings.lease) {
            case Some(l) => l
            case None => return None<ShardLeaseSlot>
        }
        if (let Some(slot) <- leaseSlots.get(sh)) {
            return Some(slot)
        }
        try {
            let slot = ShardLeaseSlot(leases.leaseForShard(typeName, sh), settings.leaseRetryMin.toMilliseconds())
            leaseSlots[sh] = slot
            Some(slot)
        } catch (e: Exception) {
            log.error("${selfAddress} 创建 ${typeName} 分片 ${sh} 的租约失败（分片不会被托管）：${e}")
            None<ShardLeaseSlot>
        }
    }

    private func markLeaseWaiting(sh: Int64): Unit {
        synchronized(shared.lock) {
            if (!shared.leaseWaitingSince.contains(sh)) {
                shared.leaseWaitingSince[sh] = monotonicMillis()
            }
        }
    }

    private func clearLeaseWaiting(sh: Int64): Unit {
        synchronized(shared.lock) {
            shared.leaseWaitingSince.remove(sh)
        }
    }

    /**
     * 发起一次获取（后台线程）。同一槽同时最多一个 acquire / release 在执行（inFlight），退避期内不发起。
     * 后台线程拿到租约时区域可能已经停止：此时没人处理结果，由该线程自己交还，避免租约一直被心跳续着。
     */
    private func requestShardLease(sh: Int64, slot: ShardLeaseSlot): Unit {
        if (slot.inFlight || slot.held || monotonicMillis() < slot.nextAttemptMs) {
            return
        }
        let me = match (selfRef) {
            case Some(r) => r
            case None => return
        }
        leaseAttemptSeq++
        let attempt = leaseAttemptSeq
        slot.inFlight = true
        let l = slot.lease
        let sharedRef = shared
        synchronized(shared.lock) {
            shared.leaseAttempts++
        }
        spawn {
            var acquired = false
            var failure: Option<Exception> = None
            try {
                acquired = l.acquire({ reason => me.tell(ShardLeaseLost(sh, attempt, reason)) })
            } catch (e: Exception) {
                failure = Some(e)
            }
            let orphan = synchronized(sharedRef.lock) {
                if (sharedRef.regionStopped) {
                    true
                } else {
                    if (acquired) {
                        sharedRef.unconfirmedLeases[sh] = l
                    }
                    false
                }
            }
            if (orphan) {
                if (acquired) {
                    try {
                        l.release()
                    } catch (_: Exception) {}
                }
            } else {
                me.tell(ShardLeaseResult(sh, attempt, acquired, failure))
            }
        }
    }

    private func onShardLeaseResult(r: ShardLeaseResult, context: ActorContext): Unit {
        synchronized(shared.lock) {
            shared.unconfirmedLeases.remove(r.shard)
        }
        let slot = match (leaseSlots.get(r.shard)) {
            case Some(x) => x
            case None => return
        }
        slot.inFlight = false
        if (r.acquired) {
            slot.held = true
            // lost 回调带的是这次获取的编号：以它为准识别之后的失去信号
            slot.attempt = r.attempt
            slot.backoffMs = settings.leaseRetryMin.toMilliseconds()
            let token = slot.lease.fencingToken() ?? -1
            synchronized(shared.lock) {
                shared.leaseHeldTokens[r.shard] = token
            }
            log.info("${selfAddress} 获得 ${typeName} 分片 ${r.shard} 的租约（token ${token}）")
        } else {
            let first = synchronized(shared.lock) {
                shared.leaseFailures++
                shared.leaseFailures == 1
            }
            let why = match (r.failure) {
                case Some(e) => "：${e}"
                case None => "（被其他持有者占用）"
            }
            // 只退避、不超时强行托管
            slot.nextAttemptMs = monotonicMillis() + slot.backoffMs
            let msg = "${selfAddress} 获取 ${typeName} 分片 ${r.shard} 的租约失败${why}，${slot.backoffMs} ms 后重试"
            if (first) {
                log.info(msg)
            } else {
                log.debug(msg)
            }
            slot.backoffMs = min(slot.backoffMs * 2, settings.leaseRetryMax.toMilliseconds())
        }
        reconcileShardLease(r.shard, context)
        trySendStopAcks(r.shard)
        updateShardStats()
    }

    private func onShardLeaseReleased(sh: Int64, context: ActorContext): Unit {
        if (let Some(slot) <- leaseSlots.get(sh)) {
            slot.inFlight = false
        }
        reconcileShardLease(sh, context)
        trySendStopAcks(sh)
    }

    private func onShardLeaseLost(sh: Int64, attempt: Int64, reason: Option<Exception>, context: ActorContext): Unit {
        let slot = match (leaseSlots.get(sh)) {
            case Some(x) => x
            case None => return
        }
        if (!slot.held || attempt != slot.attempt) {
            return
        }
        markLeaseLost(sh, slot, reason, context)
    }

    /**
     * 失去分片租约：立即停止该分片的全部实体（系统级 stop，不等邮箱处理完——新持有者可能已经在托管），
     * 放弃托管；仍是拥有者时走正常的获取流程（移交确认 + 退避后重新取租约）。
     */
    private func markLeaseLost(sh: Int64, slot: ShardLeaseSlot, reason: Option<Exception>, context: ActorContext): Unit {
        slot.held = false
        slot.nextAttemptMs = monotonicMillis() + slot.backoffMs
        synchronized(shared.lock) {
            shared.leaseHeldTokens.remove(sh)
            shared.leaseLost++
        }
        let why = match (reason) {
            case Some(e) => "${e}"
            case None => "本地有效期已过"
        }
        if (hosting.remove(sh)) {
            let n = stopShardNow(sh)
            log.warn("${selfAddress} 失去 ${typeName} 分片 ${sh} 的租约（${why}），立即停止 ${n} 个实体")
        } else {
            log.info("${selfAddress} 失去 ${typeName} 分片 ${sh} 的租约（${why}）")
        }
        if (!left && ownerOfShard(sh) == Some(selfAddress) && !acquiring.contains(sh)) {
            startAcquire(sh, context)
        }
        updateShardStats()
    }

    /**
     * 让分片的租约状态与托管状态一致：
     *   - 正在获取且移交确认已收齐：持有租约就完成托管，否则（按退避）发起获取；
     *   - 不再托管也不在获取、实体已全部终止：交还租约（异步，完成后再回复等待中的 STOPPED）。
     */
    private func reconcileShardLease(sh: Int64, context: ActorContext): Unit {
        if (settings.lease.isNone()) {
            return
        }
        match (acquiring.get(sh)) {
            case Some(a) =>
                if (a.pending.isEmpty() && !left) {
                    completeAcquire(sh, context)
                }
                return
            case None => ()
        }
        // 不在获取中就不算"等租约"（包括租约对象创建失败、随后放弃的获取）
        clearLeaseWaiting(sh)
        let slot = match (leaseSlots.get(sh)) {
            case Some(x) => x
            case None => return
        }
        if (hosting.contains(sh)) {
            return
        }
        if (slot.held && !slot.inFlight && !shardHasEntities(sh)) {
            releaseShardLease(sh, slot)
        }
    }

    private func releaseShardLease(sh: Int64, slot: ShardLeaseSlot): Unit {
        let me = match (selfRef) {
            case Some(r) => r
            case None => return
        }
        slot.held = false
        slot.inFlight = true
        synchronized(shared.lock) {
            shared.leaseHeldTokens.remove(sh)
        }
        let l = slot.lease
        let lg = log
        let tn = typeName
        spawn {
            try {
                l.release()
            } catch (e: Exception) {
                lg.warn("释放 ${tn} 分片 ${sh} 的租约失败（将在 ttl 后过期）：${e}")
            }
            me.tell(ShardLeaseReleased(sh))
        }
    }

    /** 每个 tick：等租约的分片按退避重试；托管中的分片本地有效期已过而 lost 未到时按失去处理；多余的租约交还 */
    private func tickLeases(context: ActorContext): Unit {
        if (settings.lease.isNone() || left) {
            return
        }
        let waiting = ArrayList<Int64>()
        for ((sh, a) in acquiring) {
            if (a.pending.isEmpty()) {
                waiting.add(sh)
            }
        }
        for (sh in waiting) {
            completeAcquire(sh, context)
        }
        for (sh in hosting.toArray()) {
            if (let Some(slot) <- leaseSlots.get(sh)) {
                if (slot.held && !slot.lease.checkLease()) {
                    markLeaseLost(sh, slot, None<Exception>, context)
                }
            }
        }
        for ((sh, slot) in leaseSlots) {
            if (slot.held && !hosting.contains(sh) && !acquiring.contains(sh)) {
                waiting.add(sh)
            }
        }
        for (sh in waiting) {
            if (!acquiring.contains(sh)) {
                reconcileShardLease(sh, context)
                trySendStopAcks(sh)
            }
        }
        // 已不在获取中的"等租约"标记（例如租约对象创建失败后获取被放弃）：清掉，避免运维看到假的等待
        let staleWaiting = ArrayList<Int64>()
        synchronized(shared.lock) {
            for ((sh, _) in shared.leaseWaitingSince) {
                if (!acquiring.contains(sh)) {
                    staleWaiting.add(sh)
                }
            }
            for (sh in staleWaiting) {
                shared.leaseWaitingSince.remove(sh)
            }
        }
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
            // 持有的租约要先交还（异步）再回 STOPPED：请求方收到 STOPPED 后马上获取租约，不必撞上本节点的租约
            reconcileShardLease(sh, context)
            if (shardBusy(sh)) {
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

    /** 失去租约时立即停止分片的实体（系统级 stop：邮箱里剩下的消息不再处理），返回停止的个数 */
    private func stopShardNow(sh: Int64): Int64 {
        var n: Int64 = 0
        if (let Some(ids) <- shardEntities.get(sh)) {
            for (id in ids.toArray()) {
                if (let Some(ref) <- entities.remove(id)) {
                    stopping[id] = ArrayList<ShardingEnvelope>(0)
                    lastActive.remove(id)
                    system.stop(ref)
                    synchronized(shared.lock) {
                        shared.entityIds.remove(id)
                        shared.shardCounts[sh] = max(0, (shared.shardCounts.get(sh) ?? 1) - 1)
                    }
                    n++
                }
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
        // 分片的实体全部终止：不再托管的分片交还租约；全部完成后回复等待中的移交请求
        if (!shardHasEntities(sh)) {
            reconcileShardLease(sh, context)
        }
        trySendStopAcks(sh)
    }

    /** 分片还不能回 STOPPED：实体未全部终止，或租约还没交还（DA4） */
    private func shardBusy(sh: Int64): Bool {
        if (shardHasEntities(sh)) {
            return true
        }
        match (leaseSlots.get(sh)) {
            case Some(slot) => slot.inFlight || (slot.held && !hosting.contains(sh) && !acquiring.contains(sh))
            case None => false
        }
    }

    private func trySendStopAcks(sh: Int64): Unit {
        if (shardBusy(sh)) {
            return
        }
        if (let Some(acks) <- pendingStopAcks.remove(sh)) {
            for ((requester, round) in acks) {
                sendControl(requester, "STOPPED", [(sh, round)])
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
        for (sh in leaseSlots.keys().toArray()) {
            reconcileShardLease(sh, context)
        }
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
    private let cluster: Cluster
    private let region: ActorRef
    private let shared = RegionShared()
    private let system: CActorSystem
    private let remote: SimpleRemoteManager
    public let typeName: String
    public let numberOfShards: Int64
    private let leases: Option<ShardLeases>

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
        MessageCodecRegistry.register("cactor.ShardRebalance", ShardRebalanceCodec())
        MessageCodecRegistry.register("cactor.ShardLoadReport", ShardLoadReportCodec())
        this.cluster = cluster
        this.system = system
        this.remote = remote
        this.typeName = typeName
        this.numberOfShards = numberOfShards
        this.leases = settings.lease
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

    /**
     * 获取健康度（见 AcquisitionHealth）：托管/获取中的分片数、等待超过阈值的分片数与成员。
     * 快照由区域的重发循环维护，最多滞后一个 tick；数量小、无需额外调度。
     */
    public func acquisitionHealth(): AcquisitionHealth {
        synchronized(shared.lock) {
            let pending = ArrayList<String>(shared.pendingAcquireMembers)
            pending.sort()
            AcquisitionHealth(shared.hostedShards, shared.acquiringShards, shared.slowAcquires,
                shared.oldestAcquireMs, pending.toArray(), shared.acquireSlowWarnings, shared.leaseWaitingSince.size)
        }
    }

    /**
     * 按分片租约的状态（DA4）：本节点持有租约的分片与 token、只差租约的分片与已等待时长、累计计数。
     * 未启用租约时 enabled = false。与 acquisitionHealth 一样读共享快照，可在任何线程调用。
     */
    public func leaseStatus(): ShardLeaseStatus {
        let ls = match (leases) {
            case Some(l) => l
            case None => return ShardLeaseStatus(false, "", [], [], 0, 0, 0)
        }
        let created = ls.created()
        let owner = if (created.isEmpty()) { "" } else { created[0].settings.ownerName }
        let now = monotonicMillis()
        synchronized(shared.lock) {
            let held = ArrayList<(Int64, Int64)>()
            for ((sh, t) in shared.leaseHeldTokens) {
                held.add((sh, t))
            }
            let waiting = ArrayList<(Int64, Int64)>()
            for ((sh, since) in shared.leaseWaitingSince) {
                waiting.add((sh, now - since))
            }
            // 按分片号排序：HashMap 迭代顺序不稳定，运维比对与测试都需要确定的顺序
            held.sortBy(comparator: { a, b => a[0].compare(b[0]) })
            waiting.sortBy(comparator: { a, b => a[0].compare(b[0]) })
            ShardLeaseStatus(true, owner, held.toArray(), waiting.toArray(), shared.leaseAttempts,
                shared.leaseFailures, shared.leaseLost)
        }
    }

    /**
     * 保护实体 entityId 所在分片的租约（未启用租约时为 None）。实体用它包装持久化存储，写入自动携带 fencing token：
     * `FencedDurableStateStore(inner, fence, lease)` / `FencedEventStore(inner, fence, lease)`（cactor_persistence）。
     * 实体工厂里拿不到 ClusterSharding 实例时，直接用 ShardLeases.leaseForEntity（同一个对象）。
     */
    public func entityLease(entityId: String): Option<Lease> {
        match (leases) {
            case Some(l) => Some(l.leaseForEntity(typeName, numberOfShards, entityId))
            case None => None<Lease>
        }
    }

    /** 实体所在分片的当前 fencing token（未启用租约或本节点未持有时为 None） */
    public func fencingToken(entityId: String): Option<Int64> {
        match (entityLease(entityId)) {
            case Some(l) => l.fencingToken()
            case None => None<Int64>
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

    /**
     * 把指定分片的拥有者改为 targetAddress（手动再平衡，E13′ 之后的再平衡基础）。
     * 本节点与集群中所有成员的区域都会收到覆盖指令并重新评估归属；targetAddress 必须是当前 Up 成员之一。
     */
    public func rebalanceShard(shardId: Int64, targetAddress: String): Unit {
        if (shardId < 0 || shardId >= numberOfShards) {
            throw IllegalArgumentException("分片号越界: ${shardId}")
        }
        region.tell(ShardRebalance(shardId, targetAddress))
    }

    /**
     * 自动再平衡（基础版）：本节点作为领导者（Up 成员中地址最小者）时，
     * 根据本节点的负载报告，把负载最重的分片迁移到负载最轻的成员。
     * 跨节点负载需要各节点报告；本版仅在单节点视图下做"把本节点最忙的分片迁出到地址最小的其他成员"
     * 的保守决策，完整跨节点聚合归后续版本。
     */
    /**
     * 触发一次自动再平衡评估（运维 / 测试入口）。
     *
     * 常规情况下领导者每 2s 自己评估一次（并在收到负载报告时评估），本方法只是让 Region
     * 立即评估一遍；是否迁移仍由 evaluateRebalance 的条件（全部成员报告、视图稳定、阈值、冷却）决定。
     */
    public func rebalance(): Unit {
        region.tell(RebalanceEvaluate())
    }

    public func stop(): Unit {
        remote.unexposeActor("sharding-${typeName}")
        system.stop(region)
    }
}
