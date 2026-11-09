/*
 * CActor Remote - 远程 ask、远程 watch、跨节点回复与可靠投递
 *
 * 发送方：远程消息中的发送方编码为 "remote://host:port/name"。对端 Actor 用 context.reply 回复时，
 * 回复经网络送回该名称（本地 Actor 作为发送方时会自动注册一个回复入口）。
 *
 * 远程 ask：注册一次性的回复入口 "$ask-N"，收到回复或超时后注销。
 *
 * 远程 watch（对标 Akka 远程死亡监视）：
 * - 节点级：监视方定期向被监视节点发送 PING，对方回 PONG；用 Phi Accrual 检测器判断节点是否可达。
 *   节点不可达时，向所有监视该节点上 Actor 的本地 Actor 投递 Terminated。
 * - Actor 级：被监视节点用一个本地监视 Actor（enableRemoteWatch 创建）监视已暴露的 Actor，
 *   它停止后向订阅的节点发送 DEATH，监视方投递 Terminated。
 *
 * 可靠投递（对标 Akka Reliable Delivery，至少一次 + 去重 = 恰好一次、按序）：
 * - 发送端为每个 (目标节点, 目标名称) 维护一个通道：消息带序号，保存到确认为止，超时重发。
 * - 接收端按通道记录期望序号：重复的丢弃，乱序的缓存（有上限），按序投递；累计确认定期发回。
 * - 通道 ID = 发送端地址/目标/发送端启动标识#代数。代数每新建一个通道加一，因此发送端重启、
 *   通道重置或放弃后重建，都是接收端从未见过的新通道；接收端见到新一代通道就回收同一发送端的旧通道。
 * - 对端重启（E41）：确认（ACK）带接收端的启动标识。标识变了说明对端是新实例、序号状态已丢失，
 *   发送端把未确认的消息改用新通道从 1 重新编号重发——跨重启是"至少一次"（旧实例收到但没来得及确认的会再投一次），
 *   与 Akka 的 ProducerController 在消费端重启后重发未确认消息相同。
 * - 上限与放弃（对标 Akka system-message-buffer-size / give-up-system-message-after）：
 *   每个通道最多 maxUnacked 条未确认消息，满了丢弃新消息并计数；最老的未确认消息超过 giveUpAfter
 *   仍未确认，就放弃该节点的全部通道，并向监视该节点上 Actor 的本地 Actor 投递 Terminated。
 *
 * 控制消息通过名称 "$sys" 以 StringMessage 发送，格式为 "命令|参数..."，因此暴露的名称不能包含 '|'。
 * WATCH / UNWATCH / DEATH 走可靠通道（丢一条就会让监视永远收不到 Terminated）；
 * PING / PONG / ACK 是周期性、幂等的，丢了下一轮会补，走普通发送。
 * PONG 带被监视节点的启动标识：标识变了说明节点重启过（哪怕没超过心跳容忍时间），
 * 旧实例上的 Actor 都已不在，立即投递 Terminated。
 */
package cactor_remote

import std.collection.*
import std.random.*
import std.sync.*
import std.time.*
import std.convert.*
import std.math.*
import cactor_core.actor.*
import cactor_core.message.*
import cactor_core.typed.{TypedPromise, TypedAskTimeoutException}
import cactor_core.dsl.{AskInbox}
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TransportHandshake, TransportSecurity, TcpTransport}
import cactor_foundation.hashing.{hmacSha256, toHex, fromHex, constantTimeEquals}
import cactor_foundation.detector.{PhiAccrualFailureDetector, monotonicMillis}
import cactor_foundation.logging.{Logger, Logging, LogRateLimiter}

let log_remoting: Logger = Logging.logger("cactor_remote.remoting")
// 可靠通道溢出可能成批出现：每 5 分钟逐条记前 10 条
let overflowLog: LogRateLimiter = LogRateLimiter(10, Duration.minute * 5)

const SYS_NAME: String = "$sys"
const REMOTE_PREFIX: String = "remote://"

/** 把 (地址, 名称) 编码为发送方字符串 */
public func encodeRemoteSender(address: NetworkAddress, name: String): String {
    "${REMOTE_PREFIX}${address.toString()}/${name}"
}

/** 解析 "remote://host:port/name"；不是远程发送方时返回 None */
public func parseRemoteSender(text: String): Option<(NetworkAddress, String)> {
    if (!text.startsWith(REMOTE_PREFIX)) {
        return None<(NetworkAddress, String)>
    }
    let rest = text[REMOTE_PREFIX.size..]
    match (rest.indexOf("/")) {
        case Some(i) =>
            try {
                Some((NetworkAddress.parse(rest[0..i]), rest[(i + 1)..]))
            } catch (_: Exception) {
                None<(NetworkAddress, String)>
            }
        case None => None<(NetworkAddress, String)>
    }
}

// 默认值同 Akka：system-message-buffer-size = 20000，give-up-system-message-after = 6 h
let DEFAULT_MAX_UNACKED: Int64 = 20000
let DEFAULT_GIVE_UP_AFTER: Duration = Duration.hour * 6

/** 远程层参数 */
public struct RemotingSettings {
    /** 节点心跳间隔 */
    public let heartbeatInterval: Duration
    /** Phi Accrual 阈值 */
    public let phiThreshold: Float64
    /** 可以容忍的心跳停顿 */
    public let acceptableHeartbeatPause: Duration
    /** 可靠投递：未确认消息的重发间隔 */
    public let resendInterval: Duration
    /** 可靠投递：确认的发送间隔（累计确认） */
    public let ackInterval: Duration
    /** 可靠投递：每个通道最多保存的未确认消息数（接收端乱序缓冲同样以此为上限）；满了丢弃新消息并计数 */
    public let maxUnacked: Int64
    /** 可靠投递：最老的未确认消息超过这个时长仍未确认，放弃该节点的全部通道并投递 Terminated */
    public let giveUpAfter: Duration
    /**
     * 共享密钥（E7′）：设置后连接握手带 HMAC-SHA256，双向校验，不知道密钥的节点连不上；
     * 不设置时握手只交换身份（地址 + 启动标识），不鉴权
     */
    public let secureCookie: Option<String>
    /**
     * 传输加密（E65）：设置后每条连接先完成加密握手（如 RemoteTls 的 TLS 1.3 双向认证），明文连接被拒绝；
     * 与共享密钥可以叠加（TLS 认证"持有本集群 CA 签发的证书"，密钥再认证"知道本集群的密钥"）
     */
    public let transportSecurity: Option<TransportSecurity>

    public init(heartbeatInterval: Duration, phiThreshold: Float64, acceptableHeartbeatPause: Duration,
        resendInterval: Duration, ackInterval: Duration) {
        this(heartbeatInterval, phiThreshold, acceptableHeartbeatPause, resendInterval, ackInterval,
            DEFAULT_MAX_UNACKED, DEFAULT_GIVE_UP_AFTER)
    }

    public init(heartbeatInterval: Duration, phiThreshold: Float64, acceptableHeartbeatPause: Duration,
        resendInterval: Duration, ackInterval: Duration, maxUnacked: Int64, giveUpAfter: Duration) {
        this(heartbeatInterval, phiThreshold, acceptableHeartbeatPause, resendInterval, ackInterval, maxUnacked,
            giveUpAfter, None<String>)
    }

    public init(heartbeatInterval: Duration, phiThreshold: Float64, acceptableHeartbeatPause: Duration,
        resendInterval: Duration, ackInterval: Duration, maxUnacked: Int64, giveUpAfter: Duration,
        secureCookie: Option<String>) {
        this(heartbeatInterval, phiThreshold, acceptableHeartbeatPause, resendInterval, ackInterval, maxUnacked,
            giveUpAfter, secureCookie, None<TransportSecurity>)
    }

    public init(heartbeatInterval: Duration, phiThreshold: Float64, acceptableHeartbeatPause: Duration,
        resendInterval: Duration, ackInterval: Duration, maxUnacked: Int64, giveUpAfter: Duration,
        secureCookie: Option<String>, transportSecurity: Option<TransportSecurity>) {
        if (maxUnacked <= 0) {
            throw IllegalArgumentException("maxUnacked 必须为正数: ${maxUnacked}")
        }
        this.heartbeatInterval = heartbeatInterval
        this.phiThreshold = phiThreshold
        this.acceptableHeartbeatPause = acceptableHeartbeatPause
        this.resendInterval = resendInterval
        this.ackInterval = ackInterval
        this.maxUnacked = maxUnacked
        this.giveUpAfter = giveUpAfter
        if (let Some(c) <- secureCookie) {
            if (c.isEmpty()) {
                throw IllegalArgumentException("secureCookie 不能为空字符串")
            }
        }
        this.secureCookie = secureCookie
        this.transportSecurity = transportSecurity
    }

    /**
     * 默认值：心跳 1 s、φ 阈值 10、容忍停顿 10 s（同 Akka watch-failure-detector），重发 200 ms，确认 10 ms，
     * 未确认上限 20000 条，6 小时后放弃（同 Akka）。
     * 心跳判定不可达会隔离对端且不可撤回（E7′），所以容忍停顿取得宽：此前 1 s，一次 GC 停顿就可能误判
     */
    public static func defaults(): RemotingSettings {
        RemotingSettings(Duration.second, 10.0, Duration.second * 10, Duration.millisecond * 200,
            Duration.millisecond * 10)
    }

    /** 换掉可靠投递的上限与放弃时长，其余不变 */
    public func withLimits(maxUnacked: Int64, giveUpAfter: Duration): RemotingSettings {
        RemotingSettings(heartbeatInterval, phiThreshold, acceptableHeartbeatPause, resendInterval, ackInterval,
            maxUnacked, giveUpAfter, secureCookie, transportSecurity)
    }

    /** 设置共享密钥，其余不变（集群内所有节点须相同） */
    public func withSecureCookie(cookie: String): RemotingSettings {
        RemotingSettings(heartbeatInterval, phiThreshold, acceptableHeartbeatPause, resendInterval, ackInterval,
            maxUnacked, giveUpAfter, Some(cookie), transportSecurity)
    }

    /** 设置传输加密，其余不变（集群内所有节点须一致：加密节点拒绝明文连接） */
    public func withTransportSecurity(security: TransportSecurity): RemotingSettings {
        RemotingSettings(heartbeatInterval, phiThreshold, acceptableHeartbeatPause, resendInterval, ackInterval,
            maxUnacked, giveUpAfter, secureCookie, Some(security))
    }

    /** 启用 TLS（E65），等同 withTransportSecurity(tls) */
    public func withTls(tls: RemoteTls): RemotingSettings {
        withTransportSecurity(tls)
    }
}

/** 可靠投递统计（单调递增的计数，unacked 除外） */
public struct ReliableDeliveryStats {
    /** 超时重发的次数 */
    public let resent: Int64
    /** 接收端丢弃的重复消息数 */
    public let duplicatesDropped: Int64
    /** 当前未确认的消息数 */
    public let unacked: Int64
    /** 通道已满而丢弃的新消息数 */
    public let overflowDropped: Int64
    /** 超过 giveUpAfter 而放弃的未确认消息数 */
    public let gaveUp: Int64
    /** 发现对端重启（启动标识变化）的次数，含 ACK 与 PONG 两条途径 */
    public let peerRestarts: Int64

    public init(resent: Int64, duplicatesDropped: Int64, unacked: Int64, overflowDropped: Int64, gaveUp: Int64,
        peerRestarts: Int64) {
        this.resent = resent
        this.duplicatesDropped = duplicatesDropped
        this.unacked = unacked
        this.overflowDropped = overflowDropped
        this.gaveUp = gaveUp
        this.peerRestarts = peerRestarts
    }
}

// ======================= 可靠投递信封 =======================

/** 可靠投递的信封：通道 ID、序号、确认回送地址、原发送方、载荷 */
public class ReliableEnvelope <: Message {
    public let channelId: String
    public let seq: Int64
    public let replyAddress: String
    public let senderInfo: String
    public let payload: Message

    public init(channelId: String, seq: Int64, replyAddress: String, senderInfo: String, payload: Message) {
        this.channelId = channelId
        this.seq = seq
        this.replyAddress = replyAddress
        this.senderInfo = senderInfo
        this.payload = payload
    }

    public func messageType(): String {
        "cactor.ReliableEnvelope"
    }
}

class ReliableEnvelopeCodec <: MessageCodec {
    private let inner = MessageSerializer()

    public func encode(message: Message): Array<UInt8> {
        let e = (message as ReliableEnvelope).getOrThrow()
        let out = ArrayList<UInt8>()
        writeText(out, e.channelId)
        writeLong(out, e.seq)
        writeText(out, e.replyAddress)
        writeText(out, e.senderInfo)
        let body = inner.serialize(e.payload)
        writeLong(out, body.size)
        out.add(all: body)
        out.toArray()
    }

    public func decode(payload: Array<UInt8>): Message {
        let r = ByteReader(payload)
        let channelId = r.text()
        let seq = r.long()
        let replyAddress = r.text()
        let senderInfo = r.text()
        let n = r.long()
        let body = r.bytes(n)
        ReliableEnvelope(channelId, seq, replyAddress, senderInfo, inner.deserialize(body))
    }

    private func writeText(out: ArrayList<UInt8>, s: String): Unit {
        let b = s.toArray()
        writeLong(out, b.size)
        out.add(all: b)
    }

    private func writeLong(out: ArrayList<UInt8>, v: Int64): Unit {
        var i = 56
        while (i >= 0) {
            out.add(UInt8((v >> i) & 0xFF))
            i -= 8
        }
    }
}

class ByteReader {
    private let data: Array<UInt8>
    private var pos: Int64 = 0

    init(data: Array<UInt8>) {
        this.data = data
    }

    func long(): Int64 {
        if (pos + 8 > data.size) {
            throw DeserializationException("可靠投递信封数据不完整")
        }
        var v: Int64 = 0
        for (_ in 0..8) {
            v = (v << 8) | Int64(data[pos])
            pos++
        }
        v
    }

    func bytes(n: Int64): Array<UInt8> {
        if (n < 0 || pos + n > data.size) {
            throw DeserializationException("可靠投递信封数据不完整")
        }
        let b = data[pos..(pos + n)].clone()
        pos += n
        b
    }

    func text(): String {
        String.fromUtf8(bytes(long()))
    }
}

/** 无法判断终止的回复入口（AskInbox 等）空闲多久后注销 */
public let REPLY_ENTRY_IDLE_TTL_MS: Int64 = 10 * 60 * 1000
/** 回复入口的清扫间隔 */
let REPLY_SWEEP_INTERVAL: Duration = Duration.second

/**
 * 回复入口（N7、v1 B6）：本地 Actor 作为远程消息的发送方时登记，对端 reply 按名称送回。
 * 名称带实例序号（"$ref<路径>#n"）：同一路径上重建的 Actor 是新实例、换新名称，
 * 发给旧实例的迟到回复找不到入口、计为不可投递，而不是投给已死的旧 Cell 或串到新实例。
 */
class ReplyEntry {
    let ref: ActorRef
    let name: String
    // 路径索引的键（含系统名，同进程多个系统共用一个远程管理器时不冲突）
    let key: String
    // 能否判断终止：AskInbox 这类收件箱没有生命周期，只能按空闲时长回收
    let observable: Bool
    let lastUsed: AtomicInt64

    init(ref: ActorRef, name: String, key: String, observable: Bool, now: Int64) {
        this.ref = ref
        this.name = name
        this.key = key
        this.observable = observable
        this.lastUsed = AtomicInt64(now)
    }

    func expired(now: Int64): Bool {
        if (observable) {
            ref.isTerminated()
        } else {
            now - lastUsed.load() > REPLY_ENTRY_IDLE_TTL_MS
        }
    }
}

/** 本地 Cell、Promise 引用等都能报告终止；AskInbox 没有生命周期（用户随时可能再 get） */
func hasObservableLifecycle(ref: ActorRef): Bool {
    match (ref) {
        case _: AskInbox => false
        case _ => true
    }
}

/** 发送端通道中尚未确认的消息 */
class PendingMessage {
    // 对端重启后在新通道里重新编号
    var seq: Int64
    let message: Message
    let senderInfo: String
    var lastSent: MonoTime
    // 入队时刻：判断是否超过 giveUpAfter（重新编号不重置）
    let enqueued: MonoTime

    init(seq: Int64, message: Message, senderInfo: String, lastSent: MonoTime) {
        this.seq = seq
        this.message = message
        this.senderInfo = senderInfo
        this.lastSent = lastSent
        this.enqueued = lastSent
    }
}

class OutChannel {
    // 对端重启后换成新一代 ID
    var id: String
    let address: NetworkAddress
    let target: String
    var nextSeq: Int64 = 1
    let unacked = ArrayList<PendingMessage>()
    // 最近一次 ACK 中对端的启动标识
    var peerUid: Option<UInt64> = None<UInt64>

    init(id: String, address: NetworkAddress, target: String) {
        this.id = id
        this.address = address
        this.target = target
    }
}

/** 把通道 ID 拆成 (发送端前缀, 代数)；不是 "前缀#代数" 形式时返回 None */
func splitChannelId(id: String): Option<(String, Int64)> {
    match (id.lastIndexOf("#")) {
        case Some(i) =>
            match (Int64.tryParse(id[(i + 1)..])) {
                case Some(g) => Some((id[0..i], g))
                case None => None<(String, Int64)>
            }
        case None => None<(String, Int64)>
    }
}

class InChannel {
    var expected: Int64 = 1
    let buffer = HashMap<Int64, ReliableEnvelope>()
    var duplicates: Int64 = 0
}

/** 被监视节点的状态 */
class WatchedNode {
    let address: NetworkAddress
    let detector: PhiAccrualFailureDetector

    init(address: NetworkAddress, detector: PhiAccrualFailureDetector) {
        this.address = address
        this.detector = detector
    }
}

/** 监视已暴露 Actor 的本地 Actor（远程 watch 的目标端） */
class WatchExposed <: Message {
    let ref: ActorRef

    init(ref: ActorRef) {
        this.ref = ref
    }

    public func messageType(): String {
        "WatchExposed"
    }
}

class RemoteDeathWatcher <: Actor {
    private let core: RemotingCore

    init(core: RemotingCore) {
        this.core = core
    }

    public prop name: String {
        get() { "remote-death-watcher" }
    }

    public prop description: String {
        get() { "远程死亡监视（目标端）" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case w: WatchExposed =>
                context.watch(w.ref)
                MessageResult.Handled
            case t: Terminated =>
                core.onExposedActorTerminated(t.actor)
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

/** 握手允许的时钟偏差（配置了共享密钥时校验时间戳，限制截获的握手被重放的窗口） */
let HANDSHAKE_CLOCK_SKEW_MILLIS: Int64 = 5 * 60 * 1000
const HELLO_KIND: String = "HELLO"
const HELLO_ACK_KIND: String = "HELLO-ACK"
const HANDSHAKE_VERSION: String = "1"

func wallMillis(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}

/**
 * 远程层的连接握手（E7′，对标 Akka Artery 的 handshake + 早期 remoting 的 secure-cookie）：
 *   连接方 → HELLO|1|自己的地址|uid|时间戳|目标地址|mac
 *   接受方 ← HELLO-ACK|1|自己的地址|uid|时间戳|HELLO 的 mac|mac
 * mac = HMAC-SHA256(共享密钥, 之前的全部字段)；没有密钥时为 "-"、不校验。
 * ACK 回显 HELLO 的 mac，把应答绑定到本次握手（旧的 ACK 不能重放）。
 * 双方据此得知对端的启动标识：同一地址出现新 uid 即对端重启，旧 uid 被隔离。
 * 已知局限：没有 TLS，能截获流量的攻击者可在时间窗内重放 HELLO（TLS 归 E65）
 */
class RemoteHandshake <: TransportHandshake {
    private let core: RemotingCore

    init(core: RemotingCore) {
        this.core = core
    }

    public func hello(peer: NetworkAddress): Array<UInt8> {
        let body = "${HELLO_KIND}|${HANDSHAKE_VERSION}|${core.selfAddressText()}|${core.selfUid()}|${wallMillis()}|${peer.toString()}"
        let mac = core.macOf(body)
        core.rememberHello(peer.toString(), mac)
        "${body}|${mac}".toArray()
    }

    public func accept(hello: Array<UInt8>): Option<(Array<UInt8>, String)> {
        let parts = String.fromUtf8(hello).split("|")
        if (parts.size != 7 || parts[0] != HELLO_KIND || parts[1] != HANDSHAKE_VERSION) {
            return core.rejectHandshake("非法的握手帧")
        }
        let (addr, uidText, tsText, mac) = (parts[2], parts[3], parts[4], parts[6])
        let uid = match (UInt64.tryParse(uidText)) {
            case Some(u) => u
            case None => return core.rejectHandshake("握手中的启动标识非法")
        }
        let body = "${parts[0]}|${parts[1]}|${addr}|${uidText}|${tsText}|${parts[5]}"
        if (!core.verifyMac(body, mac, tsText)) {
            return core.rejectHandshake("来自 ${addr} 的握手鉴权失败")
        }
        if (!core.admitPeer(addr, uid, inbound: true)) {
            return core.rejectHandshake("${addr}#${uid} 已被隔离")
        }
        let reply = "${HELLO_ACK_KIND}|${HANDSHAKE_VERSION}|${core.selfAddressText()}|${core.selfUid()}|${wallMillis()}|${mac}"
        Some(("${reply}|${core.macOf(reply)}".toArray(), quarantineKey(addr, uid)))
    }

    public func confirm(peer: NetworkAddress, reply: Array<UInt8>): Bool {
        let parts = String.fromUtf8(reply).split("|")
        if (parts.size != 7 || parts[0] != HELLO_ACK_KIND || parts[1] != HANDSHAKE_VERSION) {
            core.rejectHandshake("${peer.toString()} 的握手应答非法") |> ignoreRejected
            return false
        }
        let uid = match (UInt64.tryParse(parts[3])) {
            case Some(u) => u
            case None => return false
        }
        let body = "${parts[0]}|${parts[1]}|${parts[2]}|${parts[3]}|${parts[4]}|${parts[5]}"
        if (!core.verifyMac(body, parts[6], parts[4]) || !core.helloMatches(peer.toString(), parts[5])) {
            core.rejectHandshake("${peer.toString()} 的握手应答鉴权失败") |> ignoreRejected
            return false
        }
        if (!core.admitPeer(peer.toString(), uid)) {
            core.rejectHandshake("${peer.toString()}#${uid} 已被隔离") |> ignoreRejected
            return false
        }
        true
    }
}

func ignoreRejected(_: Option<(Array<UInt8>, String)>): Unit {}

func ignoreCount(_: Int64): Unit {}

func ignoreBool(_: Bool): Unit {}

func quarantineKey(addr: String, uid: UInt64): String {
    "${addr}#${uid}"
}

/**
 * 远程层的状态与后台任务（每个 SimpleRemoteManager 一个）
 */
public class RemotingCore {
    private let sender: SimpleRemoteSender
    private let receiver: SimpleRemoteReceiver
    private var managerValue: Option<SimpleRemoteManager> = None<SimpleRemoteManager>
    private var settings: RemotingSettings = RemotingSettings.defaults()
    private let lock = Mutex()
    private let running = AtomicBool(false)
    private let incarnation: UInt64
    private let askSeq = AtomicInt64(0)

    // 已暴露的 Actor：名称 -> 引用
    private let exposed = HashMap<String, ActorRef>()
    // 作为发送方登记的本地回复入口（E12′）：路径 -> 当前实例的入口；入口名称 -> 入口
    private let replyByKey = HashMap<String, ReplyEntry>()
    private let replyByName = HashMap<String, ReplyEntry>()
    private let replySeq = AtomicInt64(0)
    let replyEntriesReleased = AtomicInt64(0)

    // 远程 watch（监视方）："地址|名称" -> 本地监视者
    private let remoteWatchers = HashMap<String, ArrayList<ActorRef>>()
    private let watchedNodes = HashMap<String, WatchedNode>()
    // 远程 watch（目标端）：名称 -> 订阅的节点地址
    private let subscribers = HashMap<String, HashSet<String>>()
    private var deathWatcher: Option<ActorRef> = None<ActorRef>

    // 可靠投递
    private let outChannels = HashMap<String, OutChannel>()
    private let inChannels = HashMap<String, InChannel>()
    // 接收端：发送端前缀 -> (最新代数, 通道 ID)，用于回收旧一代通道
    private let inLatestGen = HashMap<String, (Int64, String)>()
    private let pendingAcks = HashMap<String, (String, Int64)>()
    private let channelGen = AtomicInt64(0)

    // 统计
    let resent = AtomicInt64(0)
    let duplicatesDropped = AtomicInt64(0)
    let unreachableEvents = AtomicInt64(0)
    let overflowDropped = AtomicInt64(0)
    let gaveUp = AtomicInt64(0)
    let peerRestarts = AtomicInt64(0)
    let handshakesRejected = AtomicInt64(0)

    // 身份与隔离（E7′）：地址 -> 最近一次握手 / 心跳 / ACK 得知的启动标识；被隔离的 "地址#uid"
    private let peerUids = HashMap<String, UInt64>()
    private let quarantined = HashSet<String>()
    // 连接方：目标地址 -> 最近一次 HELLO 的 mac（校验 ACK 的回显）
    private let lastHelloMac = HashMap<String, String>()

    init(sender: SimpleRemoteSender, receiver: SimpleRemoteReceiver) {
        this.sender = sender
        this.receiver = receiver
        this.incarnation = Random().nextUInt64()
        MessageCodecRegistry.register("cactor.ReliableEnvelope", ReliableEnvelopeCodec())
    }

    func attach(manager: SimpleRemoteManager): Unit {
        managerValue = Some(manager)
        let me = this
        receiver.setInterceptor({ nm => me.intercept(nm) })
        receiver.registerHandler(SYS_NAME, { m: Message, _: Option<String> => me.onSystemMessage(m) })
    }

    private func manager(): SimpleRemoteManager {
        managerValue.getOrThrow()
    }

    func currentSettings(): RemotingSettings {
        settings
    }

    func configure(s: RemotingSettings): Unit {
        settings = s
    }

    private func localAddress(): NetworkAddress {
        sender.getLocalAddress()
    }

    // ---------------- 身份、鉴权与隔离（E7′） ----------------

    func handshake(): TransportHandshake {
        RemoteHandshake(this)
    }

    func selfAddressText(): String {
        localAddress().toString()
    }

    func selfUid(): UInt64 {
        incarnation
    }

    /** 共享密钥的 HMAC（十六进制）；没有密钥时为 "-" */
    func macOf(body: String): String {
        match (settings.secureCookie) {
            case Some(c) => toHex(hmacSha256(c.toArray(), body.toArray()))
            case None => "-"
        }
    }

    /** 没有密钥时一律通过；有密钥时校验 mac（常量时间比较）与时间戳窗口 */
    func verifyMac(body: String, mac: String, tsText: String): Bool {
        match (settings.secureCookie) {
            case None => true
            case Some(c) =>
                let expected = hmacSha256(c.toArray(), body.toArray())
                let macOk = match (fromHex(mac)) {
                    case Some(m) => constantTimeEquals(m, expected)
                    case None => false
                }
                let tsOk = match (Int64.tryParse(tsText)) {
                    case Some(ts) => abs(wallMillis() - ts) <= HANDSHAKE_CLOCK_SKEW_MILLIS
                    case None => false
                }
                macOk && tsOk
        }
    }

    func rememberHello(peer: String, mac: String): Unit {
        synchronized(lock) {
            lastHelloMac[peer] = mac
        }
    }

    func helloMatches(peer: String, echoed: String): Bool {
        if (settings.secureCookie.isNone()) {
            return true
        }
        synchronized(lock) {
            match (lastHelloMac.get(peer)) {
                case Some(m) => m == echoed
                case None => false
            }
        }
    }

    func rejectHandshake(reason: String): Option<(Array<UInt8>, String)> {
        handshakesRejected.fetchAdd(1)
        overflowLog.tryLog({ text => log_remoting.warn(text) }, { => "拒绝握手：${reason}" })
        None<(Array<UInt8>, String)>
    }

    /**
     * 握手 / 心跳 / ACK 得知对端身份：被隔离的 uid 拒绝；同一地址出现新 uid 说明对端重启——
     * 隔离旧 uid、断开它残留的入站连接、向监视者投递 Terminated（旧实例上的 Actor 都已不在）。
     *
     * inbound：新实例是从入站握手得知的。此时本节点发往该地址的出站关联还属于旧实例，
     * 旧进程退出后它多半正处于重连退避（最长 5 s），新实例的应答（例如集群 JOIN 的回复）要等退避结束才发得出去；
     * 这里重建出站关联，让发往新实例的消息立即重连（同 Akka：新实例出现时旧关联被隔离、换新关联）。
     * 出站握手（confirm）得知时不重建：那条关联本身就刚连上新实例
     */
    func admitPeer(addr: String, uid: UInt64): Bool {
        admitPeer(addr, uid, inbound: false)
    }

    func admitPeer(addr: String, uid: UInt64, inbound!: Bool): Bool {
        let (admitted, previous) = synchronized(lock) {
            if (quarantined.contains(quarantineKey(addr, uid))) {
                (false, None<UInt64>)
            } else {
                match (peerUids.get(addr)) {
                    case Some(old) where old != uid =>
                        quarantined.add(quarantineKey(addr, old))
                        peerUids[addr] = uid
                        (true, Some(old))
                    case Some(_) => (true, None<UInt64>)
                    case None =>
                        peerUids[addr] = uid
                        (true, None<UInt64>)
                }
            }
        }
        if (let Some(old) <- previous) {
            peerRestarts.fetchAdd(1)
            log_remoting.warn("远程节点 ${addr} 已重启（启动标识 ${old} → ${uid}），隔离旧实例并向监视者投递 Terminated")
            let t = manager().transportInternal()
            t.closeInboundFrom(quarantineKey(addr, old)) |> ignoreCount
            if (inbound) {
                try {
                    t.resetOutbound(NetworkAddress.parse(addr))
                } catch (_: Exception) {
                    ()
                }
            }
            terminateWatchersOf(addr)
        }
        admitted
    }

    /**
     * 隔离对端的当前实例（对标 Akka quarantine）：该 uid 再也连不进来、已有入站连接断开、
     * 出站连接重建时握手被拒；发往它的未确认可靠消息丢弃；监视者收到 Terminated。
     * 误判同样不可撤回：被隔离的节点必须重启（换新 uid）才能重新通信
     */
    func quarantineAddress(addrText: String, reason: String): Unit {
        let uid = synchronized(lock) {
            let u = peerUids.get(addrText)
            if (let Some(x) <- u) {
                quarantined.add(quarantineKey(addrText, x))
            }
            u
        }
        let dropped = dropChannelsTo(addrText)
        log_remoting.warn("隔离远程节点 ${addrText}${match (uid) { case Some(u) => "#${u}" case None => "（启动标识未知）" }}：${reason}；丢弃 ${dropped} 条未确认消息")
        let t = manager().transportInternal()
        if (let Some(u) <- uid) {
            t.closeInboundFrom(quarantineKey(addrText, u)) |> ignoreCount
        }
        try {
            t.resetOutbound(NetworkAddress.parse(addrText))
        } catch (_: Exception) {
            ()
        }
        terminateWatchersOf(addrText)
    }

    /**
     * 隔离指定实例（"地址#uid"，E12′：集群移除被 down 的成员时调用，对标 Akka 隔离被移除成员的 UID）。
     * 该地址当前已知的就是这个实例时做完整隔离（断开连接、丢弃通道、通知监视者）；
     * 地址上已换成新实例时只记下旧实例，不影响新实例。重复调用不重复处理
     */
    func quarantineInstance(addrText: String, uid: UInt64, reason: String): Unit {
        let (already, current) = synchronized(lock) {
            let k = quarantineKey(addrText, uid)
            let a = quarantined.contains(k)
            quarantined.add(k)
            (a, peerUids.get(addrText))
        }
        if (already) {
            return
        }
        match (current) {
            case Some(u) where u == uid => quarantineAddress(addrText, reason)
            case _ => log_remoting.info("隔离 ${quarantineKey(addrText, uid)}（${reason}）：该地址当前不是这个实例，只阻止它再连入")
        }
    }

    func isQuarantined(addrText: String): Bool {
        synchronized(lock) {
            match (peerUids.get(addrText)) {
                case Some(u) => quarantined.contains(quarantineKey(addrText, u))
                case None => false
            }
        }
    }

    func quarantinedCount(): Int64 {
        synchronized(lock) { quarantined.size }
    }

    /** 丢弃发往该地址的全部可靠通道，返回丢弃的未确认消息数 */
    private func dropChannelsTo(addrText: String): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            let keys = ArrayList<String>()
            for ((k, c) in outChannels) {
                if (c.address.toString() == addrText) {
                    n += c.unacked.size
                    keys.add(k)
                }
            }
            for (k in keys) {
                outChannels.remove(k)
            }
            n
        }
    }

    // ---------------- 生命周期 ----------------

    func start(): Unit {
        if (!running.compareAndSwap(false, true)) {
            return
        }
        spawn {
            reliableLoop()
        }
        spawn {
            heartbeatLoop()
        }
    }

    func stop(): Unit {
        running.store(false)
    }

    // ---------------- 暴露与发送方 ----------------

    func expose(name: String, ref: ActorRef): Unit {
        if (name.contains("|")) {
            throw IllegalArgumentException("暴露的名称不能包含 '|': ${name}")
        }
        synchronized(lock) {
            exposed[name] = ref
        }
        let me = this
        receiver.registerHandler(name, { m: Message, from: Option<String> => me.deliverLocal(ref, m, from) })
    }

    func unexpose(name: String): Unit {
        synchronized(lock) {
            exposed.remove(name)
        }
        receiver.unregisterHandler(name)
    }

    /** 投递给本地 Actor：远程发送方转换为 RemoteActorRef，使 reply 经网络送回 */
    func deliverLocal(ref: ActorRef, message: Message, from: Option<String>): Unit {
        match (from) {
            case Some(f) =>
                match (parseRemoteSender(f)) {
                    case Some((addr, name)) => ref.tell(message, RemoteActorRef(manager(), addr, name))
                    case None => ref.tell(message, from)
                }
            case None => ref.tell(message)
        }
    }

    /**
     * 发送方的远程表示；本地 Actor 登记一个回复入口（E12′：按 Actor 生命周期注册与注销）。
     * - 同一路径已有入口且其 Actor 仍存活：沿用（刷新空闲时间）；
     * - 入口的 Actor 已终止（例如分片实体钝化后在同一路径重建）：换新实例的入口并注销旧入口——
     *   此前入口永远绑定第一次登记的引用，重建后的回复全投给已死的旧 Cell（N7）；
     * - 已终止 Actor 的入口由可靠投递线程每秒清扫注销（此前永不删除，v1 B6）。
     * 处理器的登记与注销在同一把锁内完成：另一个线程拿到名称时处理器一定已就位
     */
    public func senderInfoFor(ref: ActorRef): String {
        match (ref) {
            case r: RemoteActorRef => encodeRemoteSender(r.getTargetAddress(), r.getActorName())
            case _ =>
                let key = ref.path.toString()
                let now = monotonicMillis()
                let me = this
                let name = synchronized(lock) {
                    match (replyByKey.get(key)) {
                        case Some(e) where !e.ref.isTerminated() =>
                            e.lastUsed.store(now)
                            e.name
                        case old =>
                            let e = ReplyEntry(ref, "$ref${ref.path.getPath()}#${replySeq.fetchAdd(1) + 1}", key,
                                hasObservableLifecycle(ref), now)
                            if (let Some(o) <- old) {
                                replyByName.remove(o.name)
                                receiver.unregisterHandler(o.name)
                                replyEntriesReleased.fetchAdd(1)
                            }
                            replyByKey[key] = e
                            replyByName[e.name] = e
                            receiver.registerHandler(e.name, { m: Message, from: Option<String> => me.deliverReply(e, m, from) })
                            e.name
                    }
                }
                encodeRemoteSender(localAddress(), name)
        }
    }

    private func deliverReply(e: ReplyEntry, m: Message, from: Option<String>): Unit {
        e.lastUsed.store(monotonicMillis())
        deliverLocal(e.ref, m, from)
    }

    /** 注销已终止 Actor（或空闲超时的收件箱）的回复入口，返回注销数 */
    func sweepReplyEntries(): Int64 {
        let now = monotonicMillis()
        let n = synchronized(lock) {
            let dead = ArrayList<ReplyEntry>()
            for ((_, e) in replyByName) {
                if (e.expired(now)) {
                    dead.add(e)
                }
            }
            for (e in dead) {
                replyByName.remove(e.name)
                receiver.unregisterHandler(e.name)
                // 路径索引仍指向这一条时才删除（可能已被新实例的入口替换）
                if (let Some(cur) <- replyByKey.get(e.key)) {
                    if (refEq(cur, e)) {
                        replyByKey.remove(e.key)
                    }
                }
            }
            dead.size
        }
        replyEntriesReleased.fetchAdd(n)
        n
    }

    func replyEntryCount(): Int64 {
        synchronized(lock) {
            replyByName.size
        }
    }

    // ---------------- 远程 ask ----------------

    /** 向远程 Actor 发送请求并等待其 reply；超时后 Promise 以 TypedAskTimeoutException 失败 */
    public func ask(target: RemoteActorRef, message: Message, timeout: Duration): TypedPromise<Message> {
        let promise = TypedPromise<Message>()
        let name = "$ask-${askSeq.fetchAdd(1) + 1}"
        let rcv = receiver
        rcv.registerHandler(name, { m: Message, _: Option<String> =>
            rcv.unregisterHandler(name)
            promise.trySuccess(m)
            ()
        })
        let targetText = "${target.getTargetAddress().toString()}/${target.getActorName()}"
        let timer = Timer.once(timeout, { =>
            rcv.unregisterHandler(name)
            promise.tryFailure(TypedAskTimeoutException("远程 ask ${targetText} 在 ${timeout} 内没有回复"))
            ()
        })
        promise.onComplete({ _ => timer.cancel() })
        try {
            sender.sendMessage(target.getTargetAddress(), target.getActorName(), message,
                Some(encodeRemoteSender(localAddress(), name)))
        } catch (e: Exception) {
            rcv.unregisterHandler(name)
            promise.tryFailure(e)
        }
        promise
    }

    // ---------------- 远程 watch ----------------

    /** 目标端：允许其他节点监视本节点暴露的 Actor（spawn 用于创建本地监视 Actor） */
    func enableRemoteWatch(spawnWatcher: (() -> Actor) -> ActorRef): Unit {
        let me = this
        let w = spawnWatcher({ => RemoteDeathWatcher(me) })
        synchronized(lock) {
            deathWatcher = Some(w)
        }
    }

    private static func key(address: String, name: String): String {
        "${address}|${name}"
    }

    /** 监视方：target 停止或其节点不可达时，watcher 收到 Terminated(target) */
    func watchRemote(target: RemoteActorRef, watcher: ActorRef): Unit {
        let addr = target.getTargetAddress()
        let k = key(addr.toString(), target.getActorName())
        let first = synchronized(lock) {
            let list = match (remoteWatchers.get(k)) {
                case Some(l) => l
                case None =>
                    let l = ArrayList<ActorRef>()
                    remoteWatchers[k] = l
                    l
            }
            let isFirst = list.isEmpty()
            list.add(watcher)
            if (!watchedNodes.contains(addr.toString())) {
                let d = PhiAccrualFailureDetector(settings.phiThreshold, 1000, Duration.millisecond * 50,
                    settings.acceptableHeartbeatPause, settings.heartbeatInterval, monotonicMillis)
                // 从开始监视时计时：一直收不到 PONG 也会判定不可达
                d.heartbeat()
                watchedNodes[addr.toString()] = WatchedNode(addr, d)
            }
            isFirst
        }
        if (first) {
            sendSysReliable(addr, "WATCH|${target.getActorName()}|${localAddress().toString()}")
        }
    }

    func unwatchRemote(target: RemoteActorRef, watcher: ActorRef): Unit {
        let addr = target.getTargetAddress()
        let k = key(addr.toString(), target.getActorName())
        let last = synchronized(lock) {
            match (remoteWatchers.get(k)) {
                case Some(list) =>
                    list.removeIf({ w => w.path.getPath() == watcher.path.getPath() })
                    if (list.isEmpty()) {
                        remoteWatchers.remove(k)
                        dropNodeIfUnused(addr.toString())
                        true
                    } else {
                        false
                    }
                case None => false
            }
        }
        if (last) {
            sendSysReliable(addr, "UNWATCH|${target.getActorName()}|${localAddress().toString()}")
        }
    }

    /** 调用方持有锁 */
    private func dropNodeIfUnused(addr: String): Unit {
        let prefix = addr + "|"
        for ((k, _) in remoteWatchers) {
            if (k.startsWith(prefix)) {
                return
            }
        }
        watchedNodes.remove(addr)
    }

    /** 目标端：暴露的 Actor 停止，通知订阅的节点 */
    func onExposedActorTerminated(ref: ActorRef): Unit {
        let path = ref.path.getPath()
        let notices = synchronized(lock) {
            let result = ArrayList<(String, String)>()
            let names = ArrayList<String>()
            for ((name, r) in exposed) {
                if (r.path.getPath() == path) {
                    names.add(name)
                }
            }
            for (name in names) {
                exposed.remove(name)
                if (let Some(subs) <- subscribers.remove(name)) {
                    for (s in subs) {
                        result.add((s, name))
                    }
                }
            }
            (names.toArray(), result.toArray())
        }
        for (name in notices[0]) {
            receiver.unregisterHandler(name)
        }
        for ((addr, name) in notices[1]) {
            sendSysReliableTo(addr, "DEATH|${name}|${localAddress().toString()}")
        }
    }

    /** 监视方：投递 Terminated 并清理 */
    private func deliverTerminated(addrText: String, name: String): Unit {
        let k = key(addrText, name)
        let ws = synchronized(lock) {
            let list = remoteWatchers.remove(k)
            dropNodeIfUnused(addrText)
            list
        }
        if (let Some(list) <- ws) {
            let ref = RemoteActorRef(manager(), NetworkAddress.parse(addrText), name)
            for (w in list) {
                w.tell(Terminated(ref))
            }
        }
    }

    // ---------------- 可靠投递 ----------------

    /** 调用方持有锁 */
    private func channelAddress(channelId: String): Option<String> {
        for ((_, c) in outChannels) {
            if (c.id == channelId) {
                return Some(c.address.toString())
            }
        }
        None<String>
    }

    /** 新一代通道 ID：代数在本节点内单调递增，接收端据此区分新旧通道 */
    private func newChannelId(target: String): String {
        "${localAddress().toString()}/${target}/${incarnation}#${channelGen.fetchAdd(1) + 1}"
    }

    /**
     * 可靠地发送到远程 Actor（按序、恰好一次；对端重启后未确认的消息在新通道重发，跨重启为至少一次）。
     * 通道已有 maxUnacked 条未确认消息时丢弃这条并计数（不抛异常，与 tell 一致）
     */
    func sendReliable(target: RemoteActorRef, message: Message, senderInfo: String): Unit {
        let addr = target.getTargetAddress()
        let k = key(addr.toString(), target.getActorName())
        let limit = settings.maxUnacked
        let queued = synchronized(lock) {
            let ch = match (outChannels.get(k)) {
                case Some(c) => c
                case None =>
                    let c = OutChannel(newChannelId(target.getActorName()), addr, target.getActorName())
                    outChannels[k] = c
                    c
            }
            if (ch.unacked.size >= limit) {
                None<(OutChannel, ReliableEnvelope)>
            } else {
                let seq = ch.nextSeq
                ch.nextSeq++
                let p = PendingMessage(seq, message, senderInfo, MonoTime.now())
                ch.unacked.add(p)
                Some((ch, envelopeOf(ch, p)))
            }
        }
        match (queued) {
            case Some((ch, e)) => transmit(ch, e)
            case None =>
                overflowDropped.fetchAdd(1)
                overflowLog.tryLog({ text => log_remoting.warn(text) }, { =>
                    "可靠通道 ${k} 已有 ${limit} 条未确认消息，丢弃新消息 ${message.messageType()}"
                })
        }
    }

    /**
     * 在锁内生成信封：通道 ID 与序号必须取自同一时刻。对端重启时二者会一起换掉（新一代 ID、重新编号），
     * 锁外分别读取可能拼出"新 ID + 旧序号"，接收端会把它当成另一位置的消息缓存，顶掉真正的那条
     */
    private func envelopeOf(ch: OutChannel, p: PendingMessage): ReliableEnvelope {
        ReliableEnvelope(ch.id, p.seq, localAddress().toString(), p.senderInfo, p.message)
    }

    private func transmit(ch: OutChannel, e: ReliableEnvelope): Unit {
        try {
            // 发往 $sys 的系统消息（WATCH / DEATH 等）走优先通道
            if (ch.target == SYS_NAME) {
                sender.sendControl(ch.address, ch.target, e)
            } else {
                sender.sendMessage(ch.address, ch.target, e)
            }
        } catch (_: Exception) {
            // 连接失败：由重发任务稍后重试
            ()
        }
    }

    /** 尚未确认的消息数 */
    public func unackedCount(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for ((_, c) in outChannels) {
                n += c.unacked.size
            }
            n
        }
    }

    /**
     * 累计确认。peerUid 是接收端的启动标识：与上次不同说明对端重启过，它的序号状态已经丢失
     * （新实例会把本通道当作从 1 开始，后面的消息全部缓存、永远不投递，N2）。
     * 先按确认移除新实例确实已投递的消息，剩下的换新一代通道、从 1 重新编号，立即重发
     */
    private func onAck(channelId: String, upTo: Int64, peerUid: Option<UInt64>): Unit {
        // 统一登记对端身份（重启时隔离旧实例、通知监视者）；通道层面的重置在下面按通道处理
        if (let Some(u) <- peerUid) {
            if (let Some(addr) <- synchronized(lock) { channelAddress(channelId) }) {
                admitPeer(addr, u) |> ignoreBool
            }
        }
        let now = MonoTime.now()
        let resend = synchronized(lock) {
            let out = ArrayList<(OutChannel, ReliableEnvelope)>()
            for ((_, c) in outChannels) {
                if (c.id == channelId) {
                    c.unacked.removeIf({ p => p.seq <= upTo })
                    match ((c.peerUid, peerUid)) {
                        case (Some(old), Some(u)) where old != u =>
                            peerRestarts.fetchAdd(1)
                            log_remoting.info("对端 ${c.address.toString()} 已重启，通道 ${c.target} 的 ${c.unacked.size} 条未确认消息改用新通道重发")
                            c.id = newChannelId(c.target)
                            c.peerUid = peerUid
                            var seq: Int64 = 1
                            for (p in c.unacked) {
                                p.seq = seq
                                p.lastSent = now
                                seq++
                                out.add((c, envelopeOf(c, p)))
                            }
                            c.nextSeq = seq
                        case (None, Some(_)) => c.peerUid = peerUid
                        case _ => ()
                    }
                }
            }
            out
        }
        for ((c, e) in resend) {
            transmit(c, e)
        }
    }

    /** 接收端：同一发送端出现新一代通道时回收旧通道；返回 false 表示信封来自已被取代的旧通道 */
    private func admitGeneration(channelId: String): Bool {
        match (splitChannelId(channelId)) {
            case Some((prefix, gen)) =>
                match (inLatestGen.get(prefix)) {
                    case Some((latest, _)) where latest > gen => false
                    case Some((latest, oldId)) where latest < gen =>
                        inChannels.remove(oldId)
                        pendingAcks.remove(oldId)
                        inLatestGen[prefix] = (gen, channelId)
                        true
                    case Some(_) => true
                    case None =>
                        inLatestGen[prefix] = (gen, channelId)
                        true
                }
            case None => true
        }
    }

    private func onEnvelope(targetActor: String, e: ReliableEnvelope): Unit {
        let deliverable = ArrayList<ReliableEnvelope>()
        let bufferLimit = settings.maxUnacked
        synchronized(lock) {
            if (!admitGeneration(e.channelId)) {
                // 旧一代通道在途的信封：发送端已经在新通道重发，丢弃即可
                duplicatesDropped.fetchAdd(1)
            } else {
                let ch = match (inChannels.get(e.channelId)) {
                    case Some(c) => c
                    case None =>
                        let c = InChannel()
                        inChannels[e.channelId] = c
                        c
                }
                if (e.seq < ch.expected || ch.buffer.contains(e.seq)) {
                    duplicatesDropped.fetchAdd(1)
                } else if (e.seq == ch.expected) {
                    deliverable.add(e)
                    ch.expected++
                    while (let Some(next) <- ch.buffer.remove(ch.expected)) {
                        deliverable.add(next)
                        ch.expected++
                    }
                } else if (ch.buffer.size < bufferLimit) {
                    ch.buffer[e.seq] = e
                }
                // 乱序缓冲已满时不缓存：发送端会重发，缺口补上后再按序收下
                // 无论是否重复都确认，使发送端停止重发
                pendingAcks[e.channelId] = (e.replyAddress, ch.expected - 1)
            }
        }
        for (d in deliverable) {
            let from = if (d.senderInfo.isEmpty()) { None<String> } else { Some(d.senderInfo) }
            receiver.dispatch(targetActor, d.payload, from)
        }
    }

    private func reliableLoop(): Unit {
        var lastResend = MonoTime.now()
        var lastSweep = MonoTime.now()
        while (running.load()) {
            sleep(settings.ackInterval)
            if (MonoTime.now() - lastSweep >= REPLY_SWEEP_INTERVAL) {
                lastSweep = MonoTime.now()
                sweepReplyEntries() |> ignoreCount
            }
            // 发送累计确认
            let acks = synchronized(lock) {
                let a = ArrayList<(String, String, Int64)>()
                for ((id, (addr, upTo)) in pendingAcks) {
                    a.add((id, addr, upTo))
                }
                pendingAcks.clear()
                a
            }
            for ((id, addr, upTo) in acks) {
                sendSysTo(addr, "ACK|${id}|${upTo}|${incarnation}")
            }
            // 重发超时未确认的消息
            let now = MonoTime.now()
            if (now - lastResend >= settings.resendInterval) {
                lastResend = now
                let due = synchronized(lock) {
                    let d = ArrayList<(OutChannel, ReliableEnvelope)>()
                    for ((_, c) in outChannels) {
                        for (p in c.unacked) {
                            if (now - p.lastSent >= settings.resendInterval) {
                                p.lastSent = now
                                d.add((c, envelopeOf(c, p)))
                            }
                        }
                    }
                    d
                }
                for ((c, e) in due) {
                    resent.fetchAdd(1)
                    transmit(c, e)
                }
                for (addr in expiredNodes(now)) {
                    giveUp(addr)
                }
            }
        }
    }

    /** 最老的未确认消息已超过 giveUpAfter 的节点 */
    private func expiredNodes(now: MonoTime): Array<String> {
        let limit = settings.giveUpAfter
        synchronized(lock) {
            let addrs = HashSet<String>()
            for ((_, c) in outChannels) {
                if (!c.unacked.isEmpty() && now - c.unacked[0].enqueued >= limit) {
                    addrs.add(c.address.toString())
                }
            }
            addrs.toArray()
        }
    }

    /**
     * 放弃一个节点：丢弃发往它的全部未确认消息（含 WATCH 等系统消息），
     * 监视它上面 Actor 的本地 Actor 收到 Terminated（对标 Akka 系统消息超时后隔离对端）。
     * 之后再发往该节点的消息会建新一代通道
     */
    private func giveUp(addrText: String): Unit {
        let dropped = dropChannelsTo(addrText)
        gaveUp.fetchAdd(dropped)
        log_remoting.warn("节点 ${addrText} 超过 ${settings.giveUpAfter} 仍未确认，放弃 ${dropped} 条未确认消息")
        // 系统消息送不到，对端的状态已不可知：与 Akka 相同，隔离它
        quarantineAddress(addrText, "可靠消息超过 ${settings.giveUpAfter} 未确认")
    }

    // ---------------- 节点心跳 ----------------

    private func heartbeatLoop(): Unit {
        while (running.load()) {
            sleep(settings.heartbeatInterval)
            let nodes = synchronized(lock) { watchedNodes.values().toArray() }
            let me = localAddress().toString()
            for (n in nodes) {
                if (n.detector.isAvailable()) {
                    sendSys(n.address, "PING|${me}")
                } else {
                    unreachableEvents.fetchAdd(1)
                    log_remoting.warn("远程节点 ${n.address.toString()} 不可达（φ=${n.detector.phi()}）")
                    onNodeUnreachable(n.address.toString())
                }
            }
        }
    }

    /** 心跳判定不可达：与 Akka 远程 watch 相同，隔离该节点（误判不可撤回，对端须重启） */
    private func onNodeUnreachable(addrText: String): Unit {
        quarantineAddress(addrText, "心跳判定不可达")
    }

    /** 向监视该地址上 Actor 的本地 Actor 投递 Terminated，并停止对它的心跳 */
    private func terminateWatchersOf(addrText: String): Unit {
        let names = synchronized(lock) {
            let prefix = addrText + "|"
            let ns = ArrayList<String>()
            for ((k, _) in remoteWatchers) {
                if (k.startsWith(prefix)) {
                    ns.add(k[prefix.size..])
                }
            }
            watchedNodes.remove(addrText)
            ns.toArray()
        }
        for (name in names) {
            deliverTerminated(addrText, name)
        }
    }

    // ---------------- 控制消息 ----------------

    private func sendSys(address: NetworkAddress, text: String): Unit {
        try {
            sender.sendControl(address, SYS_NAME, StringMessage(text))
        } catch (_: Exception) {
            // 对端不可达由心跳检测处理
            ()
        }
    }

    private func sendSysTo(addrText: String, text: String): Unit {
        try {
            sendSys(NetworkAddress.parse(addrText), text)
        } catch (_: Exception) {
            ()
        }
    }

    /** 经可靠通道发送控制消息（目标为对端的 "$sys"） */
    private func sendSysReliable(address: NetworkAddress, text: String): Unit {
        sendReliable(RemoteActorRef(manager(), address, SYS_NAME), StringMessage(text), "")
    }

    private func sendSysReliableTo(addrText: String, text: String): Unit {
        let addr = try {
            NetworkAddress.parse(addrText)
        } catch (_: Exception) {
            log_remoting.debug("无法解析控制消息的回送地址: ${addrText}")
            return
        }
        sendSysReliable(addr, text)
    }

    /** 心跳回应：刷新故障检测器；启动标识变了说明节点重启过，旧实例上被监视的 Actor 都已不在 */
    private func onPong(addrText: String, uid: Option<UInt64>): Unit {
        let watched = synchronized(lock) {
            match (watchedNodes.get(addrText)) {
                case Some(n) =>
                    n.detector.heartbeat()
                    true
                case None => false
            }
        }
        // 启动标识变化（节点重启）由 admitPeer 统一处理；被隔离的实例发来的 PONG 不刷新检测器之外的状态
        if (watched) {
            if (let Some(u) <- uid) {
                admitPeer(addrText, u) |> ignoreBool
            }
        }
    }

    /** 返回 true 表示消息已被远程层处理 */
    func intercept(nm: NetworkMessage): Bool {
        match (nm.message) {
            case e: ReliableEnvelope =>
                onEnvelope(nm.targetActor, e)
                true
            case _ => false
        }
    }

    func onSystemMessage(m: Message): Unit {
        let text = match (m) {
            case s: StringMessage => s.getContent()
            case _ => return
        }
        let parts = text.split("|")
        match (parts[0]) {
            case "PING" where parts.size >= 2 => sendSysTo(parts[1], "PONG|${localAddress().toString()}|${incarnation}")
            case "PONG" where parts.size >= 2 => onPong(parts[1], if (parts.size >= 3) { UInt64.tryParse(parts[2]) } else { None<UInt64> })
            case "WATCH" where parts.size >= 3 =>
                let (name, from) = (parts[1], parts[2])
                let (target, w) = synchronized(lock) {
                    let t = exposed.get(name)
                    if (t.isSome()) {
                        let subs = match (subscribers.get(name)) {
                            case Some(s) => s
                            case None =>
                                let s = HashSet<String>()
                                subscribers[name] = s
                                s
                        }
                        subs.add(from)
                    }
                    (t, deathWatcher)
                }
                match ((target, w)) {
                    case (Some(t), Some(dw)) => dw.tell(WatchExposed(t))
                    case (Some(_), None) =>
                        log_remoting.warn("收到对 ${name} 的远程监视请求，但本节点没有调用 enableRemoteWatch，只能检测节点故障")
                    case (None, _) =>
                        // 与本地语义一致：监视不存在（已停止）的 Actor 立即收到 Terminated
                        sendSysReliableTo(from, "DEATH|${name}|${localAddress().toString()}")
                }
            case "UNWATCH" where parts.size >= 3 =>
                synchronized(lock) {
                    if (let Some(s) <- subscribers.get(parts[1])) {
                        s.remove(parts[2])
                    }
                }
            case "DEATH" where parts.size >= 3 => deliverTerminated(parts[2], parts[1])
            case "ACK" where parts.size >= 3 =>
                if (let Some(n) <- Int64.tryParse(parts[2])) {
                    onAck(parts[1], n, if (parts.size >= 4) { UInt64.tryParse(parts[3]) } else { None<UInt64> })
                }
            case _ => log_remoting.debug("未知的远程控制消息: ${text}")
        }
    }
}

extend SimpleRemoteManager {
    /** 调整心跳、重发等参数（在 start 之前调用；含传输加密的设置在 start 之后调用会抛 IllegalStateException） */
    public func configure(settings: RemotingSettings): Unit {
        if (settings.transportSecurity.isSome() && isStarted()) {
            throw IllegalStateException("传输加密须在 start 之前配置")
        }
        core.configure(settings)
    }

    /** 经传输加密建立的连接数（出站 + 入站）；未启用加密或不是 TCP 传输时为 0 */
    public func securedConnections(): Int64 {
        match (transportInternal() as TcpTransport) {
            case Some(t) => t.getSecuredConnections()
            case None => 0
        }
    }

    /** 传输层拒绝的入站连接数：加密握手失败（明文连接、证书不通过）与远程层握手被拒之和 */
    public func rejectedConnections(): Int64 {
        match (transportInternal() as TcpTransport) {
            case Some(t) => t.getRejectedHandshakes()
            case None => 0
        }
    }

    /** 向远程 Actor 发起请求，等待其 context.reply 的回复 */
    public func ask(target: RemoteActorRef, message: Message, timeout: Duration): TypedPromise<Message> {
        core.ask(target, message, timeout)
    }

    /** 允许其他节点监视本节点暴露的 Actor；spawn 用于创建一个本地系统 Actor */
    public func enableRemoteWatch(spawnWatcher: (() -> Actor) -> ActorRef): Unit {
        core.enableRemoteWatch(spawnWatcher)
    }

    /** 监视远程 Actor：它停止或其节点不可达时，watcher 收到 Terminated(target) */
    public func watchRemote(target: RemoteActorRef, watcher: ActorRef): Unit {
        core.watchRemote(target, watcher)
    }

    public func unwatchRemote(target: RemoteActorRef, watcher: ActorRef): Unit {
        core.unwatchRemote(target, watcher)
    }

    /** 可靠投递：按序、恰好一次（丢包或断连后自动重发，接收端去重；接收端重启后未确认的消息在新通道重发） */
    public func tellReliable(target: RemoteActorRef, message: Message): Unit {
        core.sendReliable(target, message, "")
    }

    public func tellReliable(target: RemoteActorRef, message: Message, sender: ActorRef): Unit {
        core.sendReliable(target, message, core.senderInfoFor(sender))
    }

    /** 可靠投递统计：重发次数、接收端丢弃的重复消息数、未确认消息数 */
    public func reliableStats(): (Int64, Int64, Int64) {
        (core.resent.load(), core.duplicatesDropped.load(), core.unackedCount())
    }

    /**
     * 隔离远程节点的当前实例（对标 Akka quarantine）：它再也连不进来、已有连接断开，发往它的未确认可靠消息丢弃，
     * 监视它上面 Actor 的本地 Actor 收到 Terminated。对端须重启（换新的启动标识）才能重新通信
     */
    public func quarantine(address: NetworkAddress, reason: String): Unit {
        core.quarantineAddress(address.toString(), reason)
    }

    /** 隔离指定实例（"地址#uid"）：地址上已换成新实例时不影响新实例（见 RemotingCore.quarantineInstance） */
    public func quarantine(address: NetworkAddress, uid: UInt64, reason: String): Unit {
        core.quarantineInstance(address.toString(), uid, reason)
    }

    /**
     * 当前登记的回复入口数（本地 Actor 作为远程消息发送方时登记，Actor 终止后每秒清扫注销）。
     * 诊断用：稳定负载下应保持在"仍存活且发过远程消息的 Actor 数"附近，不随请求数增长
     */
    public func replyEntryCount(): Int64 {
        core.replyEntryCount()
    }

    /** 该地址当前已知的实例是否已被隔离 */
    public func isQuarantined(address: NetworkAddress): Bool {
        core.isQuarantined(address.toString())
    }

    /** 被拒绝的握手数（鉴权失败、格式非法、来自被隔离的实例） */
    public func rejectedHandshakes(): Int64 {
        core.handshakesRejected.load()
    }

    /** 已隔离的实例数 */
    public func quarantinedCount(): Int64 {
        core.quarantinedCount()
    }

    /** 可靠投递的完整统计（含溢出丢弃、放弃、对端重启次数） */
    public func reliableDeliveryStats(): ReliableDeliveryStats {
        ReliableDeliveryStats(core.resent.load(), core.duplicatesDropped.load(), core.unackedCount(),
            core.overflowDropped.load(), core.gaveUp.load(), core.peerRestarts.load())
    }
}
