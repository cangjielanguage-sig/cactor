/*
 * CActor Cluster - 虚拟 Actor（对标 Orleans 的 Grain）
 *
 * 调用方只需要类型名和标识：`grains.call("cart-42", AddItem(...), 2.seconds)`，不需要创建、查找或管理生命周期。
 * - 按需激活：第一次调用时在分片拥有者节点上创建；
 * - 单一激活：同一标识在集群中同时最多一个实例（由集群分片的许可式移交保证：节点征得其他成员确认已停止该分片后才托管它）；
 * - 回合制：同一个 Grain 的调用逐个执行，不需要锁；
 * - 空闲停用：空闲超过 idleTimeout 后停用（onDeactivate），下次调用重新激活（onActivate）；
 * - 故障后自动重新激活：所在节点宕机并被移出集群后，下一次调用在其他节点上激活；
 *   需要保留的状态由 Grain 自己保存（例如 DurableStateStore），在 onActivate 中读取；
 * - 异常传回调用方（GrainCallException），Grain 本身继续可用（与 Orleans 相同）；
 * - 每次调用带超时（GrainTimeoutException）；超时后到达的应答被丢弃。
 * - 异步（E12′，N13）：callAsync 返回 Promise、不占线程；call 在它上面阻塞等待，只能在 Actor 之外调用
 *   （在 Actor 处理方法里阻塞会占住亲和工作线程，被等的 Grain 恰好在同一线程上就是死锁，因此立即报错）。
 *   Grain 之间互相调用请实现 onCallAsync 并用 callAsync 组合：结果未完成期间本激活暂存后续调用（仍是回合制），
 *   reentrant() 为 true 时等待期间照常处理其他调用，A→B→A 的调用链因此能完成（对标 Orleans [Reentrant]）。
 * - 应答按调用方进程的启动标识（uid）寻址：节点重启后，发给旧进程的迟到应答找不到入口，不会与新进程的调用串号。
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
import cactor_core.typed.{TypedPromise, TypedPromises, AskOutcome}

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

/** 用户实现的 Grain（实现 onCall 与 onCallAsync 之一） */
public interface Grain {
    /** 处理一次调用并返回应答；抛出的异常传回调用方 */
    func onCall(request: Message, ctx: GrainContext): Message {
        throw GrainCallException("${ctx.kind} 没有实现 onCall / onCallAsync")
    }

    /**
     * 异步处理一次调用（E12′）：返回的 Promise 成功时作为应答，失败时异常传回调用方。默认调用 onCall。
     * 需要调用其他 Grain 或其他异步操作时实现它（用 callAsync / map / flatMap 组合，不要阻塞）
     */
    func onCallAsync(request: Message, ctx: GrainContext): TypedPromise<Message> {
        try {
            TypedPromises.successful<Message>(onCall(request, ctx))
        } catch (e: Exception) {
            TypedPromises.failed<Message>(e)
        }
    }

    /**
     * 是否可重入（E12′，对标 Orleans [Reentrant]）：默认 false，onCallAsync 的结果未完成期间后续调用排队（回合制）；
     * true 时等待期间照常处理其他调用（状态可能在两次回调之间被改变），A→B→A 这类调用链才能完成
     */
    func reentrant(): Bool {
        false
    }

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

/** 异步调用完成：回送给 GrainHost 自己，在 Actor 线程里发出应答（只在本节点内传递） */
class GrainTurnDone <: Message {
    let call: GrainCall
    let outcome: AskOutcome<Message>

    init(call: GrainCall, outcome: AskOutcome<Message>) {
        this.call = call
        this.outcome = outcome
    }

    public func messageType(): String {
        "cactor.GrainTurnDone"
    }
}

/** 应答地址："地址#uid"；应答入口名带 uid，重启后的新进程不会收到旧进程调用的应答 */
func grainReplyEndpoint(node: String, uid: UInt64): String {
    "${node}#${uid}"
}

func grainReplyHandler(kind: String, uidText: String): String {
    "grain-replies-${kind}-${uidText}"
}

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
    // 本节点的应答地址（"地址#uid"）：调用方就是本进程时直接交给本地的等待表
    let localEndpoint: String
    // 结果未完成的异步调用数；不可重入时 > 0 期间后续调用进 waiting
    var inFlight: Int64 = 0
    let waiting = ArrayDeque<GrainCall>()

    init(ctx: GrainContext, grain: Grain, remote: SimpleRemoteManager, stats: GrainStats, local: (GrainReply) -> Unit,
        localEndpoint: String) {
        this.ctx = ctx
        this.grain = grain
        this.remote = remote
        this.stats = stats
        this.local = local
        this.localEndpoint = localEndpoint
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
        match (message) {
            case c: GrainCall =>
                if (inFlight > 0 && !grain.reentrant()) {
                    // 回合制：上一个调用的异步结果还没出来，排队
                    waiting.addLast(c)
                } else {
                    startCall(c, context)
                }
            case d: GrainTurnDone =>
                inFlight--
                finishCall(d.call, d.outcome)
                while (inFlight == 0 || grain.reentrant()) {
                    match (waiting.removeFirst()) {
                        case Some(next) => startCall(next, context)
                        case None => break
                    }
                }
            case _ => ()
        }
        MessageResult.Handled
    }

    private func startCall(c: GrainCall, context: ActorContext): Unit {
        let p = try {
            grain.onCallAsync(c.payload, ctx)
        } catch (e: Exception) {
            TypedPromises.failed<Message>(e)
        }
        match (p.outcome()) {
            // 同步完成（onCall 或已完成的 Promise）：本回合内直接应答
            case Some(o) => finishCall(c, o)
            case None =>
                // 不阻塞工作线程：完成时回送给自己，在 Actor 线程里应答并处理排队的调用
                inFlight++
                let me = context.self
                p.onComplete({ o => me.tell(GrainTurnDone(c, o)) })
        }
    }

    private func finishCall(c: GrainCall, o: AskOutcome<Message>): Unit {
        let reply = match (o) {
            case Replied(m) => GrainReply(c.correlationId, true, "", Some(m))
            case Failed(e) => GrainReply(c.correlationId, false, "${e}", None<Message>)
        }
        synchronized(stats.lock) {
            stats.calls++
        }
        if (c.correlationId < 0) {
            // 单向发送：不回应答
            return
        }
        if (c.replyTo == localEndpoint) {
            local(reply)
            return
        }
        // "地址#uid"：应答发往调用方进程自己的入口
        let (addr, handler) = match (c.replyTo.lastIndexOf("#")) {
            case Some(i) => (c.replyTo[0..i], grainReplyHandler(ctx.kind, c.replyTo[(i + 1)..]))
            case None => (c.replyTo, "grain-replies-${ctx.kind}")
        }
        try {
            remote.sendMessage(NetworkAddress.parse(addr), handler, reply)
        } catch (_: Exception) {
            // 调用方节点不可达：调用方会超时
        }
    }
}

/** 等待中的调用表：应答到达（本地或远程）时完成对应的 Promise */
class ReplyRouter {
    let lock = Mutex()
    // correlationId -> (Promise, "kind/id"，用于错误信息)
    let pending = HashMap<Int64, (TypedPromise<Message>, String)>()
    let lateReplies = AtomicInt64(0)

    func onReply(r: GrainReply): Unit {
        let p = synchronized(lock) { pending.remove(r.correlationId) }
        match (p) {
            case Some((promise, what)) =>
                if (r.ok) {
                    promise.trySuccess(r.payload.getOrThrow()) |> discardCompleted
                } else {
                    promise.tryFailure(GrainCallException("${what}: ${r.error}")) |> discardCompleted
                }
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
    // 本进程的应答地址 "地址#uid"
    private let endpoint: String
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
        let ep = grainReplyEndpoint(node, remote.selfUid())
        this.endpoint = ep
        let st = grainStats
        let rt = router
        let deliver = { r: GrainReply => rt.onReply(r) }
        this.sharding = ClusterSharding(cluster, system, remote, "grain-${kind}", numberOfShards,
            { id => GrainHost(GrainContext(kind, id, node), factory(id), remote, st, deliver, ep) },
            ShardingSettings.passivating(idleTimeout))
        remote.registerHandler(grainReplyHandler(kind, remote.selfUid().toString()), { m: Message =>
            if (let Some(r) <- (m as GrainReply)) {
                rt.onReply(r)
            }
        })
    }

    /**
     * 异步调用 Grain（E12′）：返回应答的 Promise，不占用调用线程，Actor 与 Grain 内部都可以用。
     * 超时以 GrainTimeoutException 失败，Grain 抛出的异常以 GrainCallException 失败
     *
     * ```
     * carts.callAsync("user-42", AddItem("apple"), 2.seconds).map<Int64>({ r => ... })
     * ```
     */
    public func callAsync(id: String, request: Message, timeout: Duration): TypedPromise<Message> {
        let cid = nextId.fetchAdd(1) + 1
        let p = TypedPromise<Message>()
        let what = "${kind}/${id}"
        let rt = router
        synchronized(rt.lock) {
            rt.pending[cid] = (p, what)
        }
        let timer = Timer.once(timeout, { =>
            let removed = synchronized(rt.lock) { rt.pending.remove(cid) }
            if (removed.isSome()) {
                p.tryFailure(GrainTimeoutException("调用 ${what} 超过 ${timeout.toMilliseconds()} ms 没有应答")) |> discardCompleted
            }
        })
        p.onComplete({ _ => timer.cancel() })
        try {
            sharding.tell(id, GrainCall(cid, endpoint, request))
        } catch (e: Exception) {
            synchronized(rt.lock) {
                rt.pending.remove(cid)
            }
            p.tryFailure(e) |> discardCompleted
        }
        p
    }

    /**
     * 调用 Grain 并等待应答；超时抛 GrainTimeoutException，Grain 抛出的异常以 GrainCallException 传回。
     * 只能在 Actor 之外调用：在 Actor（包括 Grain）的处理方法里阻塞等待会占住亲和工作线程，
     * 立即抛 IllegalStateException——Grain 之间互相调用请用 callAsync（见 Grain.onCallAsync）
     */
    public func call(id: String, request: Message, timeout: Duration): Message {
        // 超时由 callAsync 的定时器给出（GrainTimeoutException）；这里多等一点只是兜底
        callAsync(id, request, timeout).get(timeout + Duration.second * 5)
    }

    /** 单向发送（不等待应答，Grain 的返回值被丢弃） */
    public func tell(id: String, message: Message): Unit {
        sharding.tell(id, GrainCall(-1, "", message))
    }

    /** 本节点当前激活的 Grain 标识 */
    public func activeIds(): Array<String> {
        sharding.localEntityIds()
    }

    /** 底层分片区域当前看到的 Up 成员数（0 = 视图未收敛；用于测试等待成员视图就绪） */
    public func ownerCount(): Int64 {
        sharding.ownerCount()
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
