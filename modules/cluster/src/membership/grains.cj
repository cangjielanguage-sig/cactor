/*
 * CActor Cluster - 虚拟 Actor（对标 Orleans 的 Grain）
 *
 * 调用方只需要类型名和标识：`grains.call("cart-42", AddItem(...), 2.seconds)`，不需要创建、查找或管理生命周期。
 * - 按需激活：第一次调用时在分片拥有者节点上创建；
 * - 单一激活：同一标识在集群中同时最多一个实例（由集群分片保证，成员视图收敛时才计算归属）；
 * - 回合制：同一个 Grain 的调用逐个执行，不需要锁；
 * - 空闲停用：空闲超过 idleTimeout 后停用（onDeactivate），下次调用重新激活（onActivate）；
 * - 故障后自动重新激活：所在节点宕机并被移出集群后，下一次调用在其他节点上激活；
 *   需要保留的状态由 Grain 自己保存（例如 DurableStateStore），在 onActivate 中读取；
 * - 异常传回调用方（GrainCallException），Grain 本身继续可用（与 Orleans 相同）；
 * - 每次调用带超时（GrainTimeoutException）；超时后到达的应答被丢弃。
 * 请求与应答经远程传输，消息需要能被序列化（内置消息、注册了编解码器的消息或 @Message 宏生成的消息）。
 */
package cactor_cluster.membership

import std.sync.*
import std.time.*
import std.collection.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, MessageSerializer, MessageCodec, MessageCodecRegistry,
    MessageFieldWriter, MessageFieldReader}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.logging.{Logging}
import cactor_remote.{SimpleRemoteManager}

/** Grain 的激活上下文 */
public class GrainContext {
    public let kind: String
    public let id: String
    /** 所在节点地址 */
    public let node: String

    init(kind: String, id: String, node: String) {
        this.kind = kind
        this.id = id
        this.node = node
    }
}

/** 用户实现的 Grain */
public interface Grain {
    /** 处理一次调用并返回应答；抛出的异常传回调用方 */
    func onCall(request: Message, ctx: GrainContext): Message

    /** 激活（创建后、处理第一个调用前） */
    func onActivate(ctx: GrainContext): Unit {}

    /** 停用（空闲超时、移交或节点离开） */
    func onDeactivate(ctx: GrainContext): Unit {}
}

public class GrainCallException <: Exception {
    public init(message: String) {
        super(message)
    }
}

public class GrainTimeoutException <: Exception {
    public init(message: String) {
        super(message)
    }
}

// ---------------- 线上消息 ----------------

class GrainCall <: Message {
    let correlationId: Int64
    let replyTo: String
    let payload: Message

    init(correlationId: Int64, replyTo: String, payload: Message) {
        this.correlationId = correlationId
        this.replyTo = replyTo
        this.payload = payload
    }

    public func messageType(): String {
        "cactor.GrainCall"
    }
}

class GrainReply <: Message {
    let correlationId: Int64
    let ok: Bool
    let error: String
    let payload: Option<Message>

    init(correlationId: Int64, ok: Bool, error: String, payload: Option<Message>) {
        this.correlationId = correlationId
        this.ok = ok
        this.error = error
        this.payload = payload
    }

    public func messageType(): String {
        "cactor.GrainReply"
    }
}

class GrainCallCodec <: MessageCodec {
    private let inner = MessageSerializer()

    public func encode(message: Message): Array<UInt8> {
        let c = (message as GrainCall).getOrThrow()
        let w = MessageFieldWriter()
        w.writeInt64(c.correlationId)
        w.writeString(c.replyTo)
        let body = inner.serialize(c.payload)
        w.writeInt64(body.size)
        let head = w.toArray()
        let out = Array<UInt8>(head.size + body.size, repeat: 0)
        head.copyTo(out, 0, 0, head.size)
        body.copyTo(out, 0, head.size, body.size)
        out
    }

    public func decode(payload: Array<UInt8>): Message {
        let r = MessageFieldReader(payload)
        let id = r.readInt64()
        let replyTo = r.readString()
        let n = r.readInt64()
        let start = payload.size - n
        if (n < 0 || start < 0) {
            throw IllegalArgumentException("Grain 调用数据损坏")
        }
        GrainCall(id, replyTo, inner.deserialize(payload[start..]))
    }
}

class GrainReplyCodec <: MessageCodec {
    private let inner = MessageSerializer()

    public func encode(message: Message): Array<UInt8> {
        let r = (message as GrainReply).getOrThrow()
        let w = MessageFieldWriter()
        w.writeInt64(r.correlationId)
        w.writeBool(r.ok)
        w.writeString(r.error)
        let body = match (r.payload) {
            case Some(p) => inner.serialize(p)
            case None => Array<UInt8>()
        }
        w.writeInt64(body.size)
        let head = w.toArray()
        let out = Array<UInt8>(head.size + body.size, repeat: 0)
        head.copyTo(out, 0, 0, head.size)
        body.copyTo(out, 0, head.size, body.size)
        out
    }

    public func decode(payload: Array<UInt8>): Message {
        let rd = MessageFieldReader(payload)
        let id = rd.readInt64()
        let ok = rd.readBool()
        let error = rd.readString()
        let n = rd.readInt64()
        let start = payload.size - n
        if (n < 0 || start < 0) {
            throw IllegalArgumentException("Grain 应答数据损坏")
        }
        let body = if (n == 0) { None<Message> } else { Some(inner.deserialize(payload[start..])) }
        GrainReply(id, ok, error, body)
    }
}

// ---------------- 激活 ----------------

class GrainStats {
    let lock = Mutex()
    var activations: Int64 = 0
    var deactivations: Int64 = 0
    var calls: Int64 = 0
}

/** 分片实体：承载一个 Grain 的激活，把应答送回调用方节点 */
class GrainHost <: Actor {
    let ctx: GrainContext
    let grain: Grain
    let remote: SimpleRemoteManager
    let stats: GrainStats
    let local: (GrainReply) -> Unit

    init(ctx: GrainContext, grain: Grain, remote: SimpleRemoteManager, stats: GrainStats, local: (GrainReply) -> Unit) {
        this.ctx = ctx
        this.grain = grain
        this.remote = remote
        this.stats = stats
        this.local = local
    }

    public prop name: String { get() { "grain" } }
    public prop description: String { get() { "grain ${ctx.kind}/${ctx.id}" } }

    public func preStart(): Unit {
        grain.onActivate(ctx)
        synchronized(stats.lock) {
            stats.activations++
        }
    }

    public func postStop(): Unit {
        try {
            grain.onDeactivate(ctx)
        } finally {
            synchronized(stats.lock) {
                stats.deactivations++
            }
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(c) <- (message as GrainCall)) {
            let reply = try {
                GrainReply(c.correlationId, true, "", Some(grain.onCall(c.payload, ctx)))
            } catch (e: Exception) {
                GrainReply(c.correlationId, false, "${e}", None<Message>)
            }
            synchronized(stats.lock) {
                stats.calls++
            }
            if (c.correlationId < 0) {
                // 单向发送：不回应答
                return MessageResult.Handled
            }
            if (c.replyTo == ctx.node) {
                local(reply)
            } else {
                try {
                    remote.sendMessage(NetworkAddress.parse(c.replyTo), "grain-replies-${ctx.kind}", reply)
                } catch (_: Exception) {
                    // 调用方节点不可达：调用方会超时
                }
            }
        }
        MessageResult.Handled
    }
}

/** 一次等待中的调用 */
class PendingCall {
    let lock = Mutex()
    let cond: Condition
    var reply: Option<GrainReply> = None

    init() {
        cond = synchronized(lock) { lock.condition() }
    }

    func complete(r: GrainReply): Unit {
        synchronized(lock) {
            reply = Some(r)
            cond.notifyAll()
        }
    }

    func await(timeout: Duration): Option<GrainReply> {
        synchronized(lock) {
            let deadline = MonoTime.now() + timeout
            while (reply.isNone()) {
                let left = deadline - MonoTime.now()
                if (left <= Duration.Zero) {
                    break
                }
                cond.wait(timeout: left)
            }
            reply
        }
    }
}

/** 等待中的调用表：应答到达（本地或远程）时完成对应的调用 */
class ReplyRouter {
    let lock = Mutex()
    let pending = HashMap<Int64, PendingCall>()
    let lateReplies = AtomicInt64(0)

    func onReply(r: GrainReply): Unit {
        let p = synchronized(lock) { pending.remove(r.correlationId) }
        match (p) {
            case Some(call) => call.complete(r)
            case None => lateReplies.fetchAdd(1)
        }
    }
}

/**
 * 一种 Grain 在本节点上的入口。集群中每个节点都要用相同的 kind 与工厂创建。
 *
 * ```
 * let carts = Grains(cluster, system, remote, "cart", { id => CartGrain(store) }, 5.minutes)
 * let total = carts.call("user-42", StringMessage("add:apple"), 2.seconds)
 * ```
 */
public class Grains {
    public let kind: String
    private let sharding: ClusterSharding
    private let remote: SimpleRemoteManager
    private let selfNode: String
    private let router = ReplyRouter()
    private let nextId = AtomicInt64(0)
    private let grainStats = GrainStats()
    private let log = Logging.logger("cactor.cluster.grains")

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, kind: String,
        factory: (String) -> Grain, idleTimeout: Duration) {
        this(cluster, system, remote, kind, factory, idleTimeout, 64)
    }

    public init(cluster: Cluster, system: CActorSystem, remote: SimpleRemoteManager, kind: String,
        factory: (String) -> Grain, idleTimeout: Duration, numberOfShards: Int64) {
        MessageCodecRegistry.register("cactor.GrainCall", GrainCallCodec())
        MessageCodecRegistry.register("cactor.GrainReply", GrainReplyCodec())
        this.kind = kind
        this.remote = remote
        this.selfNode = cluster.selfAddress
        let node = cluster.selfAddress
        let st = grainStats
        let rt = router
        let deliver = { r: GrainReply => rt.onReply(r) }
        this.sharding = ClusterSharding(cluster, system, remote, "grain-${kind}", numberOfShards,
            { id => GrainHost(GrainContext(kind, id, node), factory(id), remote, st, deliver) },
            ShardingSettings.passivating(idleTimeout))
        remote.registerHandler("grain-replies-${kind}", { m: Message =>
            if (let Some(r) <- (m as GrainReply)) {
                rt.onReply(r)
            }
        })
    }

    /** 调用 Grain 并等待应答；超时抛 GrainTimeoutException，Grain 抛出的异常以 GrainCallException 传回 */
    public func call(id: String, request: Message, timeout: Duration): Message {
        let cid = nextId.fetchAdd(1) + 1
        let p = PendingCall()
        synchronized(router.lock) {
            router.pending[cid] = p
        }
        sharding.tell(id, GrainCall(cid, selfNode, request))
        match (p.await(timeout)) {
            case Some(r) =>
                if (r.ok) {
                    r.payload.getOrThrow()
                } else {
                    throw GrainCallException("${kind}/${id}: ${r.error}")
                }
            case None =>
                synchronized(router.lock) {
                    router.pending.remove(cid)
                }
                throw GrainTimeoutException("调用 ${kind}/${id} 超过 ${timeout.toMilliseconds()} ms 没有应答")
        }
    }

    /** 单向发送（不等待应答，Grain 的返回值被丢弃） */
    public func tell(id: String, message: Message): Unit {
        sharding.tell(id, GrainCall(-1, "", message))
    }

    /** 本节点当前激活的 Grain 标识 */
    public func activeIds(): Array<String> {
        sharding.localEntityIds()
    }

    /** (本节点激活次数, 停用次数, 处理的调用数, 超时后才到达的应答数) */
    public func stats(): (Int64, Int64, Int64, Int64) {
        synchronized(grainStats.lock) {
            (grainStats.activations, grainStats.deactivations, grainStats.calls, router.lateReplies.load())
        }
    }

    public func stop(): Unit {
        sharding.stop()
    }
}
