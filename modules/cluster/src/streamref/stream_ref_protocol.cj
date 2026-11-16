/*
 * CActor Cluster.StreamRef - 类型化流式回复的线上协议（E61 / DA7，对标 Akka StreamRefs）
 *
 * 一条流 = 生产方阶段 Actor（SourceRef 指向它）+ 消费方阶段 Actor，两者只交换下面这个枚举：
 *
 *   消费方                                   生产方
 *     | -- Subscribe(流, 消费方位置, 版本) -->  |   单次挂接：第二个订阅者收到 Rejected
 *     | <-------- OnSubscribe(流) ------------ |
 *     | ------ Demand(流, 累计上限序号) ------> |   信用：只允许发送序号 ≤ 上限的元素
 *     | <---- Element(流, 序号, 元素) -------- |   序号从 1 连续递增
 *     | <-- Completed(流, 最后序号) / Failed -- |   恰好一个终止信号
 *     | ---------- Cancel(流, 原因) ---------> |   消费方取消，生产方据此中止（例如中止 LLM 调用）
 *
 * 为什么需求用"累计上限序号"而不是增量 n：累计值是幂等、单调的——重复或乱序到达的 Demand
 * 只取最大值，不会把信用算多；生产方不变量 sentSeq ≤ grantedUpTo 一眼可查。
 *
 * 为什么元素带序号：远程 tell 可能因断连/出站队列满而丢帧。消费方按序号检查，
 * 缺号、重复、超出已授予需求都**大声失败**（onError + 通知生产方取消），从不静默跳过。
 *
 * 线上编码：整个枚举以命名载荷（TypedPayloadCodecs，类型名 STREAM_REF_WIRE_TYPE）过线，
 * 走 TypedPayloadBox 的 kind 2，不需要 codec v3 能力（DA6），新旧节点都能互读外层信封。
 * 元素本身复用已有机制：实现 Message 的走嵌套消息帧；注册了 TypedPayloadCodecs 的走命名载荷；
 * String 内置支持（LLM token 最常见的形态）。元素编码失败在**发送侧**抛出，生产方阶段据此
 * 向消费方发 Failed；接收侧解码失败不丢帧，而是变成 StreamRefUndecodable 元素，消费方据此失败并取消。
 */
package cactor_cluster.streamref

import std.collection.*
import std.sync.*
import std.time.*
import cactor_core.message.{Message, MessageSerializer, SerializationException, DeserializationException}
import cactor_core.typed.*

/** 协议版本：Subscribe 携带，生产方拒绝不一致的版本（而不是猜测对方的帧格式） */
public let STREAM_REF_PROTOCOL_VERSION: Int64 = 1

/**
 * 可靠模式（reliable = true）的协议版本。
 *
 * 版本协商复用既有的 Subscribe/Rejected 帧，不引入新的握手格式：
 * - 可靠消费方跨节点挂接时发送 SubscribeReliable(version = 2)；旧生产方只认版本 1，
 *   会回 Rejected("协议版本不符")——旧节点对新模式是**明确拒绝**，不是静默半挂接；
 * - 可靠生产方收到旧消费方的 Subscribe(version = 1) 同样回 Rejected("可靠模式需要协议版本 2")；
 * - 两端都关闭可靠模式时版本恒为 1，现有帧与线格式一个字节都不变（DA7 兼容性要求）。
 */
public let STREAM_REF_PROTOCOL_VERSION_RELIABLE: Int64 = 2

/** 线上类型名（TypedPayloadCodecs 命名空间，两端须一致） */
public let STREAM_REF_WIRE_TYPE: String = "cactor.streamref.StreamRefMsg.v1"

/**
 * 流阶段 Actor 的协议。前 8 个是线上消息；其余是本节点内的控制与定时消息，
 * 编码时直接抛 SerializationException——它们带闭包语义或本地引用，本来就不该过线。
 */
public enum StreamRefMsg {
    // ---------- 线上协议 ----------
    /** 消费方 -> 生产方：挂接（流标识、消费方的远程位置、协议版本） */
    | Subscribe(String, String, Int64)
    /** 生产方 -> 消费方：挂接成功 */
    | OnSubscribe(String)
    /** 生产方 -> 消费方：拒绝挂接（已有订阅者、版本不符、流已结束） */
    | Rejected(String, String)
    /** 消费方 -> 生产方：累计需求，允许发送序号 ≤ upTo 的元素 */
    | Demand(String, Int64)
    /** 生产方 -> 消费方：第 seq 个元素 */
    | Element(String, Int64, Any)
    /** 生产方 -> 消费方：正常结束，lastSeq = 已发送的最后序号 */
    | Completed(String, Int64)
    /** 生产方 -> 消费方：失败结束 */
    | Failed(String, String)
    /** 消费方 -> 生产方：取消 */
    | Cancel(String, String)
    // ---------- 可靠模式（版本 2）线上协议，旧对端不会走到这里 ----------
    /**
     * 消费方 -> 生产方：可靠挂接 / 恢复挂接。
     * 首次挂接 lastAck = 0；恢复时带同一个 token 与已连续收到的序号，生产方从 lastAck+1 重发。
     */
    | SubscribeReliable(String, String, Int64, String, Int64)
    /** 消费方 -> 生产方：累计确认（已连续收到的最大序号，生产方可丢弃 ≤ seq 的保留元素） */
    | Ack(String, Int64)
    /** 消费方 -> 生产方：已收到 Completed；生产方据此真正终止（完成后不再无限保留元素） */
    | AckCompleted(String)
    /**
     * 生产方 -> 消费方：版本 2 探测被接受，请用 SubscribeReliable 带恢复令牌正式挂接。
     *
     * 可靠消费方的首次挂接先发**既有** Subscribe 帧并带版本 2：旧生产方认得这个帧、会以
     * Rejected("协议版本不符") 明确拒绝；新可靠生产方回本帧，消费方再发 SubscribeReliable。
     * 于是"与旧节点互通"不依赖旧节点理解新帧，而是复用旧节点的版本检查。
     */
    | ReliableProbeOk(String)
    // ---------- 本节点内（不过线） ----------
    /** 同进程挂接：直接带消费方引用，免去暴露与网络回环 */
    | SubscribeLocal(String, TypedActorRef<StreamRefMsg>, Int64)
    /** StreamProducer.offer 投进来的元素（容量已在调用方预留） */
    | Offer(Any)
    | ProducerComplete
    | ProducerFail(String)
    /** StreamRefSubscription.request(n) / cancel */
    | SubscriberRequest(Int64)
    | SubscriberCancel(String)
    /**
     * SinkRef 本地消费句柄的一次 pull（本地控制消息，不过线）。
     * 与 SubscriberRequest(1) 分开命名：SinkRef 的 sink 是消费方，语义是"本地缓冲空出一格"，
     * 而不是 SourceRef 消费方向上游授予的预取窗口，混用会让两端的统计与超时判断难以自证。
     */
    | ConsumerPull
    /** 定时器 */
    | SubscriptionTimeout
    | IdleTimeout
    | TerminationDeadline
    /** 可靠模式：恢复挂接的重试（消费方）或重连窗口检查（生产方） */
    | ReconnectTick
    /** 可靠模式：完成确认的截止时间（生产方） */
    | CompletionAckTimeout
    /** 可靠模式：有未满足需求却长时间没有任何进展（消费方），与空闲超时分开，触发恢复而不是失败 */
    | ProgressTick

    public func streamIdOrEmpty(): String {
        match (this) {
            case StreamRefMsg.Subscribe(s, _, _) => s
            case StreamRefMsg.OnSubscribe(s) => s
            case StreamRefMsg.Rejected(s, _) => s
            case StreamRefMsg.Demand(s, _) => s
            case StreamRefMsg.Element(s, _, _) => s
            case StreamRefMsg.Completed(s, _) => s
            case StreamRefMsg.Failed(s, _) => s
            case StreamRefMsg.Cancel(s, _) => s
            case StreamRefMsg.SubscribeReliable(s, _, _, _, _) => s
            case StreamRefMsg.Ack(s, _) => s
            case StreamRefMsg.AckCompleted(s) => s
            case StreamRefMsg.ReliableProbeOk(s) => s
            case StreamRefMsg.SubscribeLocal(s, _, _) => s
            case _ => ""
        }
    }
}

/** 接收侧解码不了的元素：不丢帧（丢了只会变成一个难查的"缺号"），由消费方明确失败并取消 */
public class StreamRefUndecodable {
    public let reason: String

    public init(reason: String) {
        this.reason = reason
    }
}

// ================= 异常 =================

/** 流式回复失败的公共父类：消费方 onError 只会收到它的子类 */
public open class StreamRefException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** 在 subscriptionTimeout 内没有完成挂接（生产方不存在、对端读不懂协议、或 Subscribe 丢失） */
public class StreamRefSubscriptionTimeoutException <: StreamRefException {
    public init(message: String) {
        super(message)
    }
}

/** 有未满足的需求，但 idleTimeout 内生产方一条消息都没发来 */
public class StreamRefIdleTimeoutException <: StreamRefException {
    public init(message: String) {
        super(message)
    }
}

/** 协议违例：缺号、重复、超出需求、完成序号不符、元素类型/解码不符 */
public class StreamRefProtocolException <: StreamRefException {
    public init(message: String) {
        super(message)
    }
}

/** 生产方以 Failed 结束（原因原样带回） */
public class StreamRefRemoteFailureException <: StreamRefException {
    public let reason: String

    public init(reason: String) {
        super("生产方失败：${reason}")
        this.reason = reason
    }
}

/** 挂接被拒绝（单次挂接、版本不符、流已结束） */
public class StreamRefRejectedException <: StreamRefException {
    public init(message: String) {
        super(message)
    }
}

/** 生产方阶段（或其节点）终止，且在宽限期内没有收到终止信号 */
public class StreamRefPeerTerminatedException <: StreamRefException {
    public init(message: String) {
        super(message)
    }
}

/** 可靠模式：断连后在重连窗口内没能重新挂接（生产方真的没了，或网络一直不可达） */
public class StreamRefReconnectTimeoutException <: StreamRefException {
    public init(message: String) {
        super(message)
    }
}

/** 订阅者违反 Reactive Streams 规则（request(n ≤ 0)）或自身回调抛出异常 */
public class StreamRefSubscriberException <: StreamRefException {
    public init(message: String) {
        super(message)
    }
}

// ================= 结果类型 =================

/** 生产方视角的流结局（恰好完成一次） */
public enum StreamTermination <: ToString {
    /** 全部元素已发出并发出了 Completed */
    | Completed
    /** 消费方取消或消费方阶段/节点终止：生产方应中止生成（例如中止 LLM 调用） */
    | Cancelled(String)
    /** 生产方失败、订阅超时、元素编码失败、属主终止 */
    | Failed(String)

    public func isCompleted(): Bool {
        match (this) {
            case StreamTermination.Completed => true
            case _ => false
        }
    }

    public func isCancelled(): Bool {
        match (this) {
            case StreamTermination.Cancelled(_) => true
            case _ => false
        }
    }

    public func isFailed(): Bool {
        match (this) {
            case StreamTermination.Failed(_) => true
            case _ => false
        }
    }

    public func reason(): String {
        match (this) {
            case StreamTermination.Completed => ""
            case StreamTermination.Cancelled(r) => r
            case StreamTermination.Failed(r) => r
        }
    }

    public func toString(): String {
        match (this) {
            case StreamTermination.Completed => "Completed"
            case StreamTermination.Cancelled(r) => "Cancelled(${r})"
            case StreamTermination.Failed(r) => "Failed(${r})"
        }
    }
}

/** StreamProducer.offer 的结果：非阻塞，满了就告诉调用方，由调用方决定等待（whenReady）还是丢弃 */
public enum OfferResult <: ToString {
    | Enqueued
    /** 生产方缓冲已满（消费方没有给足需求）：稍后重试或等待 whenReady() */
    | Backpressured
    /** 流已结束（完成、取消或失败）：不要再生产 */
    | Closed

    public func isEnqueued(): Bool {
        match (this) {
            case OfferResult.Enqueued => true
            case _ => false
        }
    }

    public func isBackpressured(): Bool {
        match (this) {
            case OfferResult.Backpressured => true
            case _ => false
        }
    }

    public func isClosed(): Bool {
        match (this) {
            case OfferResult.Closed => true
            case _ => false
        }
    }

    public func toString(): String {
        match (this) {
            case OfferResult.Enqueued => "Enqueued"
            case OfferResult.Backpressured => "Backpressured"
            case OfferResult.Closed => "Closed"
        }
    }
}

// ================= 设置 =================

/**
 * 流参数（生产方与消费方各用自己那份）。
 *
 * - bufferSize：生产方 = offer 可以预先排队的元素上限；消费方 = 预取窗口，
 *   向上游授予的需求永远不超过"已交付给订阅者的数量 + bufferSize"，因此在途 + 消费方缓冲 ≤ bufferSize。
 * - subscriptionTimeout：生产方在此时间内没人挂接就失败；消费方在此时间内没收到 OnSubscribe 就失败。
 * - idleTimeout（消费方，可选）：有未满足的需求但这么久没收到生产方任何消息就失败。
 *   默认关闭：LLM 生成本来就可能很慢，是否超时是业务决定。
 * - finalTerminationSignalDeadline：远程生产方被判定终止后，再等这么久终止信号（Completed 与 DEATH
 *   走不同通道，可能被后者超车），过期才失败。同进程引用之间有序，不等待。
 * - reliable（默认 false）：跨节点流的可靠模式。开启后生产方保留未确认元素（受 bufferSize 约束），
 *   消费方按累计序号 ACK；短暂断连不立即失败，在 reconnectWindow 内用同一 streamId + 恢复令牌
 *   重新挂接并从 lastAck+1 重发，消费方按序号去重。默认关闭时行为与线格式与 v2.42 完全一致。
 *   本进程内的流（SourceRef 带本地引用）仍走单次挂接，不启用可靠路径：没有传输可断。
 * - reconnectWindow：可靠模式下判定短暂断连后等待恢复的最长时间；同时决定恢复重试间隔
 *   （min(1s, max(100ms, reconnectWindow/4))）。只有 reliable = true 时才生效。
 */
public struct StreamRefSettings {
    public let bufferSize: Int64
    public let subscriptionTimeout: Duration
    public let idleTimeout: Option<Duration>
    public let finalTerminationSignalDeadline: Duration
    public let reliable: Bool
    public let reconnectWindow: Duration

    public init() {
        this(16, Duration.second * 10, None<Duration>, Duration.second, false, Duration.second * 10)
    }

    public init(bufferSize: Int64, subscriptionTimeout: Duration, idleTimeout: Option<Duration>,
        finalTerminationSignalDeadline: Duration) {
        this(bufferSize, subscriptionTimeout, idleTimeout, finalTerminationSignalDeadline, false, Duration.second * 10)
    }

    public init(bufferSize: Int64, subscriptionTimeout: Duration, idleTimeout: Option<Duration>,
        finalTerminationSignalDeadline: Duration, reliable: Bool, reconnectWindow: Duration) {
        if (bufferSize <= 0) {
            throw IllegalArgumentException("StreamRef bufferSize 必须为正数")
        }
        if (subscriptionTimeout <= Duration.Zero || finalTerminationSignalDeadline < Duration.Zero) {
            throw IllegalArgumentException("StreamRef 超时必须为正数")
        }
        if (let Some(d) <- idleTimeout) {
            if (d <= Duration.Zero) {
                throw IllegalArgumentException("StreamRef idleTimeout 必须为正数")
            }
        }
        if (reliable && reconnectWindow <= Duration.Zero) {
            throw IllegalArgumentException("StreamRef reliable 模式的 reconnectWindow 必须为正数")
        }
        this.bufferSize = bufferSize
        this.subscriptionTimeout = subscriptionTimeout
        this.idleTimeout = idleTimeout
        this.finalTerminationSignalDeadline = finalTerminationSignalDeadline
        this.reliable = reliable
        this.reconnectWindow = reconnectWindow
    }

    public func withBufferSize(n: Int64): StreamRefSettings {
        StreamRefSettings(n, subscriptionTimeout, idleTimeout, finalTerminationSignalDeadline, reliable, reconnectWindow)
    }

    public func withSubscriptionTimeout(d: Duration): StreamRefSettings {
        StreamRefSettings(bufferSize, d, idleTimeout, finalTerminationSignalDeadline, reliable, reconnectWindow)
    }

    public func withIdleTimeout(d: Duration): StreamRefSettings {
        StreamRefSettings(bufferSize, subscriptionTimeout, Some(d), finalTerminationSignalDeadline, reliable, reconnectWindow)
    }

    public func withFinalTerminationSignalDeadline(d: Duration): StreamRefSettings {
        StreamRefSettings(bufferSize, subscriptionTimeout, idleTimeout, d, reliable, reconnectWindow)
    }

    /** 开启/关闭可靠模式（跨节点流） */
    public func withReliable(enabled: Bool): StreamRefSettings {
        StreamRefSettings(bufferSize, subscriptionTimeout, idleTimeout, finalTerminationSignalDeadline, enabled,
            reconnectWindow)
    }

    /** 可靠模式的重连窗口 */
    public func withReconnectWindow(d: Duration): StreamRefSettings {
        StreamRefSettings(bufferSize, subscriptionTimeout, idleTimeout, finalTerminationSignalDeadline, reliable, d)
    }

    /** 恢复重试间隔：窗口的 1/4，夹在 [100ms, 1s] 之间（既不会把窗口切得太碎，也不会拖太久） */
    func recoveryRetryInterval(): Duration {
        let quarter = reconnectWindow / 4
        if (quarter < Duration.millisecond * 100) {
            Duration.millisecond * 100
        } else if (quarter > Duration.second) {
            Duration.second
        } else {
            quarter
        }
    }
}

// ================= SourceRef =================

private let SOURCE_REF_PREFIX: String = "srcref:v1|"

/**
 * 指向生产方阶段的引用，可以放进 ask 回复里交给消费方（对标 Akka SourceRef<T>）。
 *
 * - location：远程位置 "remote://host:port/name"；生产方没有远程层时为空（只能在本进程使用）。
 * - localRef：同进程时直接持有阶段引用；toWireString/fromWireString 之后为 None。
 * T 是幻象类型：线上只有位置与流标识，元素在消费方以 `as T` 还原，类型不符按协议违例失败。
 */
public class SourceRef<T> {
    public let streamId: String
    public let location: String
    public let localRef: Option<TypedActorRef<StreamRefMsg>>

    /** 一般由 StreamRefs.sourceRef 创建；直接构造用于自定义生产端（例如测试中的手写协议端点） */
    public init(streamId: String, location: String, localRef: Option<TypedActorRef<StreamRefMsg>>) {
        if (streamId.isEmpty() || streamId.contains("|")) {
            throw IllegalArgumentException("流标识不能为空且不能包含 '|'")
        }
        if (location.isEmpty() && localRef.isNone()) {
            throw IllegalArgumentException("SourceRef 至少需要远程位置或本地引用之一")
        }
        this.streamId = streamId
        this.location = location
        this.localRef = localRef
    }

    /** 能否跨进程传递（生产方节点有远程层） */
    public func isSerializable(): Bool {
        !location.isEmpty()
    }

    /** 过线文本，放进回复消息（StringMessage 或自定义编解码）即可；本地专用引用抛 SerializationException */
    public func toWireString(): String {
        if (location.isEmpty()) {
            throw SerializationException("SourceRef ${streamId} 只在本进程有效（生产方的 StreamRefs 没有远程层），不能跨节点传递")
        }
        "${SOURCE_REF_PREFIX}${streamId}|${location}"
    }

    /** 由 toWireString 的文本还原（不带本地引用） */
    public static func fromWireString(text: String): SourceRef<T> {
        if (!text.startsWith(SOURCE_REF_PREFIX)) {
            throw DeserializationException("不是 SourceRef 文本：${text}")
        }
        let rest = text[SOURCE_REF_PREFIX.size..]
        match (rest.indexOf("|")) {
            case Some(i) where i > 0 && i + 1 < rest.size =>
                SourceRef<T>(rest[0..i], rest[(i + 1)..], None<TypedActorRef<StreamRefMsg>>)
            case _ => throw DeserializationException("SourceRef 文本不完整：${text}")
        }
    }

    public func toString(): String {
        "SourceRef(${streamId}@${if (location.isEmpty()) { "local" } else { location }})"
    }
}

// ================= 线上编解码 =================

private let ELEM_MESSAGE: UInt8 = 1
private let ELEM_NAMED: UInt8 = 2
private let ELEM_STRING: UInt8 = 3

class WireWriter {
    let out = ArrayList<UInt8>()

    func u8(v: UInt8): Unit {
        out.add(v)
    }

    func i64(v: Int64): Unit {
        if (v < 0) {
            throw SerializationException("StreamRef 线上整数不能为负：${v}")
        }
        let u = UInt64(v)
        for (i in 0..8) {
            out.add(UInt8((u >> UInt64((7 - i) * 8)) & 0xFF))
        }
    }

    func bytes(b: Array<UInt8>): Unit {
        i64(b.size)
        for (x in b) {
            out.add(x)
        }
    }

    func str(s: String): Unit {
        bytes(s.toArray())
    }
}

class WireReader {
    let data: Array<UInt8>
    var pos: Int64 = 0

    init(data: Array<UInt8>) {
        this.data = data
    }

    private func need(n: Int64): Unit {
        if (n < 0 || pos + n > data.size) {
            throw DeserializationException("StreamRef 帧不完整")
        }
    }

    func u8(): UInt8 {
        need(1)
        let v = data[pos]
        pos++
        v
    }

    func i64(): Int64 {
        need(8)
        var u: UInt64 = 0
        for (i in 0..8) {
            u = (u << 8) | UInt64(data[pos + i])
        }
        pos += 8
        if (u > UInt64(Int64.Max)) {
            throw DeserializationException("StreamRef 线上整数越界")
        }
        Int64(u)
    }

    func bytes(): Array<UInt8> {
        let n = i64()
        need(n)
        let b = data.slice(pos, n)
        pos += n
        b
    }

    func str(): String {
        String.fromUtf8(bytes())
    }
}

/** 元素编码：Message 帧 > 注册的命名编解码器 > 内置 String；都不满足时在发送侧抛出 */
func encodeElement(w: WireWriter, value: Any): Unit {
    match (value) {
        case m: Message =>
            w.u8(ELEM_MESSAGE)
            w.bytes(MessageSerializer().serialize(m))
            return
        case _ => ()
    }
    match (TypedPayloadCodecs.findFor(value)) {
        case Some(codec) =>
            w.u8(ELEM_NAMED)
            w.str(codec.typeName())
            w.bytes(codec.encode(value))
            return
        case None => ()
    }
    match (value) {
        case s: String =>
            w.u8(ELEM_STRING)
            w.str(s)
        case _ => throw SerializationException(
            "StreamRef 元素无法跨进程传输：既不是 Message，也没有注册 TypedPayloadCodecs，也不是 String。" +
            "请用 TypedPayloadCodecs.register<T>(typeName:, encode:, decode:) 注册元素类型")
    }
}

/** 元素解码：失败不抛（抛了整帧会被远程层丢掉，变成难查的缺号），而是返回 StreamRefUndecodable */
func decodeElement(r: WireReader): Any {
    let kind = r.u8()
    let body = r.bytes()
    match (kind) {
        case 1u8 =>
            try {
                MessageSerializer().deserialize(body)
            } catch (e: Exception) {
                StreamRefUndecodable("元素消息帧解码失败：${e.message}")
            }
        case 2u8 =>
            let name = String.fromUtf8(body)
            let data = r.bytes()
            match (TypedPayloadCodecs.find(name)) {
                case Some(codec) =>
                    try {
                        codec.decode(data)
                    } catch (e: Exception) {
                        StreamRefUndecodable("元素类型 ${name} 解码失败：${e.message}")
                    }
                case None => StreamRefUndecodable("本节点没有注册元素类型 ${name} 的编解码器（两端 TypedPayloadCodecs 配置不一致）")
            }
        case 3u8 => String.fromUtf8(body)
        case other => StreamRefUndecodable("未知的元素编码种类 ${other}（对端版本更新？）")
    }
}

func encodeStreamRefMsg(m: StreamRefMsg): Array<UInt8> {
    let w = WireWriter()
    match (m) {
        case StreamRefMsg.Subscribe(s, loc, ver) =>
            w.u8(1)
            w.str(s)
            w.str(loc)
            w.i64(ver)
        case StreamRefMsg.OnSubscribe(s) =>
            w.u8(2)
            w.str(s)
        case StreamRefMsg.Rejected(s, reason) =>
            w.u8(3)
            w.str(s)
            w.str(reason)
        case StreamRefMsg.Demand(s, upTo) =>
            w.u8(4)
            w.str(s)
            w.i64(upTo)
        case StreamRefMsg.Element(s, seq, value) =>
            w.u8(5)
            w.str(s)
            w.i64(seq)
            encodeElement(w, value)
        case StreamRefMsg.Completed(s, lastSeq) =>
            w.u8(6)
            w.str(s)
            w.i64(lastSeq)
        case StreamRefMsg.Failed(s, reason) =>
            w.u8(7)
            w.str(s)
            w.str(reason)
        case StreamRefMsg.Cancel(s, reason) =>
            w.u8(8)
            w.str(s)
            w.str(reason)
        // ---------- 可靠模式（版本 2）：旧生产方在版本协商阶段已拒绝，不会收到这些帧 ----------
        case StreamRefMsg.SubscribeReliable(s, loc, ver, token, lastAck) =>
            w.u8(9)
            w.str(s)
            w.str(loc)
            w.i64(ver)
            w.str(token)
            w.i64(lastAck)
        case StreamRefMsg.Ack(s, seq) =>
            w.u8(10)
            w.str(s)
            w.i64(seq)
        case StreamRefMsg.AckCompleted(s) =>
            w.u8(11)
            w.str(s)
        case StreamRefMsg.ReliableProbeOk(s) =>
            w.u8(12)
            w.str(s)
        case _ => throw SerializationException("StreamRef 本地控制消息不能跨进程发送")
    }
    w.out.toArray()
}

func decodeStreamRefMsg(b: Array<UInt8>): StreamRefMsg {
    let r = WireReader(b)
    match (r.u8()) {
        case 1u8 =>
            let s = r.str()
            let loc = r.str()
            StreamRefMsg.Subscribe(s, loc, r.i64())
        case 2u8 => StreamRefMsg.OnSubscribe(r.str())
        case 3u8 =>
            let s = r.str()
            StreamRefMsg.Rejected(s, r.str())
        case 4u8 =>
            let s = r.str()
            StreamRefMsg.Demand(s, r.i64())
        case 5u8 =>
            let s = r.str()
            let seq = r.i64()
            StreamRefMsg.Element(s, seq, decodeElement(r))
        case 6u8 =>
            let s = r.str()
            StreamRefMsg.Completed(s, r.i64())
        case 7u8 =>
            let s = r.str()
            StreamRefMsg.Failed(s, r.str())
        case 8u8 =>
            let s = r.str()
            StreamRefMsg.Cancel(s, r.str())
        case 9u8 =>
            let s = r.str()
            let loc = r.str()
            let ver = r.i64()
            let token = r.str()
            StreamRefMsg.SubscribeReliable(s, loc, ver, token, r.i64())
        case 10u8 =>
            let s = r.str()
            StreamRefMsg.Ack(s, r.i64())
        case 11u8 => StreamRefMsg.AckCompleted(r.str())
        case 12u8 => StreamRefMsg.ReliableProbeOk(r.str())
        case other => throw DeserializationException("未知的 StreamRef 消息种类 ${other}")
    }
}

let codecLock: Mutex = Mutex()

/**
 * 注册流协议的命名编解码器（幂等）。StreamRefs 每次建流时都会调用：
 * 进程级注册表可能被测试清空（clearForTest），按需补登记比假设"启动时登记一次"更稳。
 */
public func ensureStreamRefCodecRegistered(): Bool {
    synchronized(codecLock) {
        if (TypedPayloadCodecs.find(STREAM_REF_WIRE_TYPE).isSome()) {
            return false
        }
        TypedPayloadCodecs.register<StreamRefMsg>(typeName: STREAM_REF_WIRE_TYPE,
            encode: { m => encodeStreamRefMsg(m) }, decode: { b => decodeStreamRefMsg(b) })
        true
    }
}
