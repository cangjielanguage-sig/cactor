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
 * - 接收端按通道记录期望序号：重复的丢弃，乱序的缓存，按序投递；累计确认定期发回。
 * - 通道 ID 包含发送端的启动标识，发送端重启后是新通道。
 *
 * 控制消息通过名称 "$sys" 以 StringMessage 发送，格式为 "命令|参数..."，因此暴露的名称不能包含 '|'。
 */
package cactor_remote

import std.collection.*
import std.random.*
import std.sync.*
import std.time.*
import std.convert.*
import cactor_core.actor.*
import cactor_core.message.*
import cactor_core.typed.{TypedPromise, TypedAskTimeoutException}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.detector.{PhiAccrualFailureDetector, monotonicMillis}
import cactor_foundation.logging.{Logger, Logging}

let log_remoting: Logger = Logging.logger("cactor_remote.remoting")

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

    public init(heartbeatInterval: Duration, phiThreshold: Float64, acceptableHeartbeatPause: Duration,
        resendInterval: Duration, ackInterval: Duration) {
        this.heartbeatInterval = heartbeatInterval
        this.phiThreshold = phiThreshold
        this.acceptableHeartbeatPause = acceptableHeartbeatPause
        this.resendInterval = resendInterval
        this.ackInterval = ackInterval
    }

    /** 默认：心跳 200ms，φ 阈值 8，容忍停顿 1s，重发 200ms，确认 10ms */
    public static func defaults(): RemotingSettings {
        RemotingSettings(Duration.millisecond * 200, 8.0, Duration.second, Duration.millisecond * 200,
            Duration.millisecond * 10)
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

/** 发送端通道中尚未确认的消息 */
class PendingMessage {
    let seq: Int64
    let message: Message
    let senderInfo: String
    var lastSent: MonoTime

    init(seq: Int64, message: Message, senderInfo: String, lastSent: MonoTime) {
        this.seq = seq
        this.message = message
        this.senderInfo = senderInfo
        this.lastSent = lastSent
    }
}

class OutChannel {
    let id: String
    let address: NetworkAddress
    let target: String
    var nextSeq: Int64 = 1
    let unacked = ArrayList<PendingMessage>()

    init(id: String, address: NetworkAddress, target: String) {
        this.id = id
        this.address = address
        this.target = target
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
    // 作为发送方注册的本地回复入口：名称 -> 引用
    private let replyEntries = HashMap<String, ActorRef>()

    // 远程 watch（监视方）："地址|名称" -> 本地监视者
    private let remoteWatchers = HashMap<String, ArrayList<ActorRef>>()
    private let watchedNodes = HashMap<String, WatchedNode>()
    // 远程 watch（目标端）：名称 -> 订阅的节点地址
    private let subscribers = HashMap<String, HashSet<String>>()
    private var deathWatcher: Option<ActorRef> = None<ActorRef>

    // 可靠投递
    private let outChannels = HashMap<String, OutChannel>()
    private let inChannels = HashMap<String, InChannel>()
    private let pendingAcks = HashMap<String, (String, Int64)>()

    // 统计
    let resent = AtomicInt64(0)
    let duplicatesDropped = AtomicInt64(0)
    let unreachableEvents = AtomicInt64(0)

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

    func configure(s: RemotingSettings): Unit {
        settings = s
    }

    private func localAddress(): NetworkAddress {
        sender.getLocalAddress()
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

    /** 发送方的远程表示；本地 Actor 会注册一个回复入口 */
    public func senderInfoFor(ref: ActorRef): String {
        match (ref) {
            case r: RemoteActorRef => encodeRemoteSender(r.getTargetAddress(), r.getActorName())
            case _ =>
                let name = "$ref" + ref.path.getPath()
                let isNew = synchronized(lock) {
                    if (replyEntries.contains(name)) {
                        false
                    } else {
                        replyEntries[name] = ref
                        true
                    }
                }
                if (isNew) {
                    let me = this
                    receiver.registerHandler(name, { m: Message, from: Option<String> => me.deliverLocal(ref, m, from) })
                }
                encodeRemoteSender(localAddress(), name)
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
            sendSys(addr, "WATCH|${target.getActorName()}|${localAddress().toString()}")
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
            sendSys(addr, "UNWATCH|${target.getActorName()}|${localAddress().toString()}")
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
            sendSysTo(addr, "DEATH|${name}|${localAddress().toString()}")
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

    /** 可靠地发送到远程 Actor（按序、恰好一次；对端节点重启后的通道状态不保留） */
    func sendReliable(target: RemoteActorRef, message: Message, senderInfo: String): Unit {
        let addr = target.getTargetAddress()
        let k = key(addr.toString(), target.getActorName())
        let (ch, seq) = synchronized(lock) {
            let ch = match (outChannels.get(k)) {
                case Some(c) => c
                case None =>
                    let c = OutChannel("${localAddress().toString()}/${target.getActorName()}/${incarnation}", addr,
                        target.getActorName())
                    outChannels[k] = c
                    c
            }
            let seq = ch.nextSeq
            ch.nextSeq++
            ch.unacked.add(PendingMessage(seq, message, senderInfo, MonoTime.now()))
            (ch, seq)
        }
        transmit(ch, seq, message, senderInfo)
    }

    private func transmit(ch: OutChannel, seq: Int64, message: Message, senderInfo: String): Unit {
        try {
            sender.sendMessage(ch.address, ch.target,
                ReliableEnvelope(ch.id, seq, localAddress().toString(), senderInfo, message))
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

    private func onAck(channelId: String, upTo: Int64): Unit {
        synchronized(lock) {
            for ((_, c) in outChannels) {
                if (c.id == channelId) {
                    c.unacked.removeIf({ p => p.seq <= upTo })
                }
            }
        }
    }

    private func onEnvelope(targetActor: String, e: ReliableEnvelope): Unit {
        let deliverable = ArrayList<ReliableEnvelope>()
        synchronized(lock) {
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
            } else {
                ch.buffer[e.seq] = e
            }
            // 无论是否重复都确认，使发送端停止重发
            pendingAcks[e.channelId] = (e.replyAddress, ch.expected - 1)
        }
        for (d in deliverable) {
            let from = if (d.senderInfo.isEmpty()) { None<String> } else { Some(d.senderInfo) }
            receiver.dispatch(targetActor, d.payload, from)
        }
    }

    private func reliableLoop(): Unit {
        var lastResend = MonoTime.now()
        while (running.load()) {
            sleep(settings.ackInterval)
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
                sendSysTo(addr, "ACK|${id}|${upTo}")
            }
            // 重发超时未确认的消息
            let now = MonoTime.now()
            if (now - lastResend >= settings.resendInterval) {
                lastResend = now
                let due = synchronized(lock) {
                    let d = ArrayList<(OutChannel, PendingMessage)>()
                    for ((_, c) in outChannels) {
                        for (p in c.unacked) {
                            if (now - p.lastSent >= settings.resendInterval) {
                                p.lastSent = now
                                d.add((c, p))
                            }
                        }
                    }
                    d
                }
                for ((c, p) in due) {
                    resent.fetchAdd(1)
                    transmit(c, p.seq, p.message, p.senderInfo)
                }
            }
        }
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

    private func onNodeUnreachable(addrText: String): Unit {
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
            sender.sendMessage(address, SYS_NAME, StringMessage(text))
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
            case "PING" where parts.size >= 2 => sendSysTo(parts[1], "PONG|${localAddress().toString()}")
            case "PONG" where parts.size >= 2 =>
                if (let Some(n) <- synchronized(lock) { watchedNodes.get(parts[1]) }) {
                    n.detector.heartbeat()
                }
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
                        sendSysTo(from, "DEATH|${name}|${localAddress().toString()}")
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
                    onAck(parts[1], n)
                }
            case _ => log_remoting.debug("未知的远程控制消息: ${text}")
        }
    }
}

extend SimpleRemoteManager {
    /** 调整心跳、重发等参数（在 start 之前调用） */
    public func configure(settings: RemotingSettings): Unit {
        core.configure(settings)
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

    /** 可靠投递：按序、恰好一次（丢包或断连后自动重发，接收端去重） */
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
}
