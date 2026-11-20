/*
 * CActor Platform.Edge - 无状态接入层：连接 ↔ 集群 Actor（P3 / F4）
 *
 * 分层：Edge 只持有"连接"，不持有会话状态。
 *   - 入站：客户端消息经 resolver（默认 ClusterReceptionist）找到会话 Actor，直接 tell；
 *   - 出站：入站消息携带一个本节点的回复 sink 作为 sender，会话 Actor 的 reply 回到
 *     sink，sink 把内容追加到本节点的 DurableStream（于是断线期间的消息不会丢）；
 *   - 重连：客户端带 `EdgeRecoveryToken`，从 lastAck + 1 继续读同一条流。
 *
 * 连接本身不缓存业务状态，也没有"连接表"要清理：换了 Edge 节点（只要能看到同一份流存储）
 * 一样能续传。跨节点寻址复用既有能力（ClusterReceptionist 的 ORSet 登记 + RemoteActorRef），
 * 这里不新建发现协议。
 *
 * 与具体 HTTP/WS 框架解耦：本文件只做语义；帧格式与握手头映射见 `edge_protocol.cj`。
 */
package cactor_platform.edge

import std.collection.*
import std.sync.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.hashing.{sha256, toHex}
import cactor_foundation.logging.{Logging}
import cactor_persistence.{DurableStateStore}
import cactor_platform.durable.*
import cactor_cluster.membership.{ClusterReceptionist}

/** 入站载荷 → 会话消息。使用方可换成自己的协议编解码，Edge 不规定业务消息类型 */
public interface EdgeMessageFactory {
    func inbound(sessionId: String, payload: String): Message
}

/** 回复消息 → 出站载荷；返回 None 表示这条回复不属于流（例如内部确认消息） */
public interface EdgeReplyEncoder {
    func encode(message: Message): Option<String>
}

/** 默认入站映射：把载荷装进 StringMessage（适合回显/文本聊天） */
public class StringEdgeMessages <: EdgeMessageFactory {
    public init() {}

    public func inbound(sessionId: String, payload: String): Message {
        StringMessage(payload)
    }
}

/** 默认出站映射：只编码 StringMessage；业务协议请实现 EdgeReplyEncoder */
public class StringEdgeReplies <: EdgeReplyEncoder {
    public init() {}

    public func encode(message: Message): Option<String> {
        match (message) {
            case s: StringMessage => Some<String>(s.getContent())
            case _ => None<String>
        }
    }
}

/** 按 sessionId 找会话 Actor；找不到返回 None（Edge 报 404/NoRoute，而不是丢消息） */
public interface EdgeActorResolver {
    func resolve(sessionId: String): Option<ActorRef>
}

/**
 * 集群解析：经本节点的 Receptionist 找全集群的会话实例。
 *
 * 服务名不能包含 '/'（Receptionist 的约束），所以用 `${servicePrefix}-${sessionId}`；
 * 登记侧必须用同一个名字。找不到（登记尚未传播或实例已下线）时返回 None。
 */
public class ReceptionistEdgeResolver <: EdgeActorResolver {
    private let receptionist: ClusterReceptionist
    private let servicePrefix: String

    public init(receptionist: ClusterReceptionist, servicePrefix!: String = "edge-session") {
        this.receptionist = receptionist
        this.servicePrefix = servicePrefix
    }

    /** 会话对应的服务名（登记与解析共用） */
    public func serviceName(sessionId: String): String {
        "${servicePrefix}-${sessionId}"
    }

    public func resolve(sessionId: String): Option<ActorRef> {
        let refs = receptionist.find(serviceName(sessionId))
        if (refs.size > 0) {
            Some<ActorRef>(refs[0])
        } else {
            None<ActorRef>
        }
    }
}

/** 单节点解析：显式绑定 sessionId → Actor（测试与单机部署） */
public class LocalEdgeResolver <: EdgeActorResolver {
    private let lock = Mutex()
    private let refs = HashMap<String, ActorRef>()

    public init() {}

    public func bind(sessionId: String, ref: ActorRef): Unit {
        synchronized(lock) {
            refs[sessionId] = ref
        }
    }

    public func unbind(sessionId: String): Unit {
        synchronized(lock) {
            refs.remove(sessionId)
        }
    }

    public func resolve(sessionId: String): Option<ActorRef> {
        synchronized(lock) {
            refs.get(sessionId)
        }
    }
}

/** 连接建立结果：令牌问题必须是可区分的拒绝，而不是 500 */
public enum EdgeConnectResult {
    | Connected(EdgeConnection)
    | Unauthorized(Int64, String)
}

/** 入站投递结果：找不到会话是明确的 NoRoute，调用方据此回 404（而不是静默吞掉） */
public enum EdgeDeliverResult {
    | Delivered
    | NoRoute(String)
}

/**
 * 一条 Edge 连接（无状态视图）：绑定 sessionId、一条持久流、一个已确认游标。
 *
 * `poll` 只读不推进游标；客户端确认后调用 `ack`——这样断线重发最多重复未确认的消息，
 * 不会漏。`recoveryToken` 把游标封进令牌，换连接/换 Edge 节点都能续读。
 */
public class EdgeConnection {
    private let router: EdgeRouter
    private let sessionIdValue: String
    private let stream: DurableStream
    private let lock = Mutex()
    private var lastAckValue: Int64

    init(router: EdgeRouter, sessionId: String, stream: DurableStream, lastAck: Int64) {
        this.router = router
        this.sessionIdValue = sessionId
        this.stream = stream
        this.lastAckValue = lastAck
    }

    public prop sessionId: String {
        get() { sessionIdValue }
    }

    /** 已确认的最大 offset（0 = 还没有确认任何消息） */
    public func lastAckedOffset(): Int64 {
        synchronized(lock) {
            lastAckValue
        }
    }

    /** 入站：解析会话 Actor 并投递；解析失败返回 NoRoute */
    public func deliver(payload: String): EdgeDeliverResult {
        router.deliver(sessionIdValue, payload)
    }

    /**
     * 出站读取：从 lastAck + 1 开始，无新消息时最多等 timeout（默认 2s）。
     * 读到不等于确认——调用方处理完再 ack，否则断线会重复投递。
     */
    public func poll(timeout!: Duration = DEFAULT_LONG_POLL, maxRows!: Int64 = MAX_READ_ROWS): StreamRead {
        let from = lastAckedOffset() + 1
        stream.readLongPoll(fromOffset: from, timeout: timeout, maxRows: maxRows)
    }

    /** 确认到 offset（只增不减）；重复或回退的 ack 被忽略 */
    public func ack(offset: Int64): Unit {
        synchronized(lock) {
            if (offset > lastAckValue) {
                lastAckValue = offset
            }
        }
    }

    /** 签发重连令牌（默认 24h）；到期后客户端应重新建立会话而不是无限续期 */
    public func recoveryToken(ttl!: Duration = DEFAULT_EDGE_TOKEN_TTL): String {
        router.issueRecoveryToken(sessionIdValue, lastAckedOffset(), ttl: ttl)
    }

    public func streamPath(): String {
        stream.path
    }
}

/** 回复 sink 的全局序号：同一系统里多个 router 实例（例如 Edge 重启后重建）不能重名 */
let EDGE_SINK_SEQ = AtomicInt64(0)

/** 回复 sink：会话 Actor 的 reply 落在这里，再由它追加到 Edge 节点的持久流 */
class EdgeReplySink <: Actor {
    private let router: EdgeRouter
    private let sessionId: String
    private let log = Logging.logger("cactor.platform.edge")

    init(router: EdgeRouter, sessionId: String) {
        this.router = router
        this.sessionId = sessionId
    }

    public prop name: String {
        get() { "edge-reply-sink" }
    }

    public prop description: String {
        get() { "edge reply sink" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (router.encodeReply(message)) {
            case Some(text) =>
                try {
                    router.publish(sessionId, text)
                } catch (e: Exception) {
                    log.warn("Edge 回复写入流失败（${sessionId}）：${e}")
                }
            case None => ()
        }
        MessageResult.Handled
    }
}

/**
 * Edge 路由器：每节点一个，持有本节点的流注册表、解析器与回复 sink。
 *
 * @param store 流存储；必须是 Edge 节点可访问的同一份存储（进程内、文件或外部提供者）
 * @param resolver 会话寻址（跨节点用 ReceptionistEdgeResolver）
 * @param secret 恢复令牌的 HMAC 密钥
 */
public class EdgeRouter {
    private let system: CActorSystem
    private let store: DurableStateStore
    private let registry: DurableStreamRegistry
    private let resolver: EdgeActorResolver
    private let secret: String
    private let messages: EdgeMessageFactory
    private let replies: EdgeReplyEncoder
    private let streamTtl: Int64
    private let lock = Mutex()
    private let streams = HashMap<String, DurableStream>()
    private let sinks = HashMap<String, ActorRef>()

    public init(system: CActorSystem, store: DurableStateStore, resolver: EdgeActorResolver, secret: String,
        messages!: EdgeMessageFactory = StringEdgeMessages(),
        replies!: EdgeReplyEncoder = StringEdgeReplies(), streamTtlSeconds!: Int64 = 0) {
        if (secret.isEmpty()) {
            throw IllegalArgumentException("Edge 恢复令牌密钥不能为空")
        }
        this.system = system
        this.store = store
        this.registry = DurableStreamRegistry(store)
        this.resolver = resolver
        this.secret = secret
        this.messages = messages
        this.replies = replies
        this.streamTtl = streamTtlSeconds
    }

    /** 会话的出站流；同一条流在本路由器内共享一个实例（append 串行化） */
    public func streamFor(sessionId: String): DurableStream {
        validateSessionId(sessionId)
        let path = edgeStreamPath(sessionId)
        synchronized(lock) {
            match (streams.get(path)) {
                case Some(s) => s
                case None =>
                    let s = registry.stream(path, ttlSeconds: streamTtl)
                    streams[path] = s
                    s
            }
        }
    }

    /** 建立连接；带令牌时校验会话、签名与有效期，并从 lastAck + 1 续读 */
    public func connect(sessionId: String, resumeToken!: Option<String> = None): EdgeConnectResult {
        validateSessionId(sessionId)
        var lastAck: Int64 = 0
        match (resumeToken) {
            case Some(tok) =>
                match (EdgeRecoveryToken.verify(secret, tok, nowMillis: edgeNowMillis())) {
                    case Valid(sid, ack) =>
                        if (sid != sessionId) {
                            return EdgeConnectResult.Unauthorized(403, "恢复令牌与请求的会话不匹配")
                        }
                        lastAck = ack
                    case Expired(_) =>
                        return EdgeConnectResult.Unauthorized(401, "恢复令牌已过期，请重新建立会话")
                    case Invalid(reason) =>
                        return EdgeConnectResult.Unauthorized(403, reason)
                }
            case None => ()
        }
        EdgeConnectResult.Connected(EdgeConnection(this, sessionId, streamFor(sessionId), lastAck))
    }

    /** 解析当前会话 Actor（每次投递都重新解析：分片/实例变化后连接自动跟随） */
    public func resolveActor(sessionId: String): Option<ActorRef> {
        resolver.resolve(sessionId)
    }

    /** 追加一条出站消息（会话 Actor 或其工具代码可直接调用） */
    public func publish(sessionId: String, payload: String, producerId!: String = "edge"): Int64 {
        streamFor(sessionId).append(payload, producerId: producerId)
    }

    /** 为连接的已确认游标签发令牌 */
    public func issueRecoveryToken(sessionId: String, lastAck: Int64,
        ttl!: Duration = DEFAULT_EDGE_TOKEN_TTL): String {
        EdgeRecoveryToken.issue(secret, sessionId, lastAck, ttlMillis: ttl.toMilliseconds(),
            nowMillis: edgeNowMillis())
    }

    /** 该会话是否已被本路由器创建过流（诊断） */
    public func hasStream(sessionId: String): Bool {
        registry.exists(edgeStreamPath(sessionId))
    }

    func deliver(sessionId: String, payload: String): EdgeDeliverResult {
        match (resolveActor(sessionId)) {
            case Some(ref) =>
                ref.tell(messages.inbound(sessionId, payload), sinkFor(sessionId))
                EdgeDeliverResult.Delivered
            case None => EdgeDeliverResult.NoRoute(sessionId)
        }
    }

    func encodeReply(message: Message): Option<String> {
        replies.encode(message)
    }

    private func sinkFor(sessionId: String): ActorRef {
        synchronized(lock) {
            match (sinks.get(sessionId)) {
                case Some(r) => r
                case None =>
                    // 会话 id 可能含任意字符：用摘要生成合法的 Actor 名；再加全局序号，
                    // 使同一系统内重建的 router 不会与上一代的 sink 重名
                    let name = "edge-sink-${toHex(sha256(sessionId.toArray()))}-${EDGE_SINK_SEQ.fetchAdd(1) + 1}"
                    let ref = system.actorOf({ => EdgeReplySink(this, sessionId) }, name)
                    sinks[sessionId] = ref
                    ref
            }
        }
    }
}

func validateSessionId(sessionId: String): Unit {
    if (sessionId.isEmpty()) {
        throw IllegalArgumentException("Edge 会话 id 不能为空")
    }
    if (sessionId.contains("\n")) {
        throw IllegalArgumentException("Edge 会话 id 不能包含换行")
    }
}

/** 每条会话一条流：用摘要做键，避免 sessionId 中的特殊字符进入路径 */
public func edgeStreamPath(sessionId: String): String {
    "edge-${toHex(sha256(sessionId.toArray()))}"
}
