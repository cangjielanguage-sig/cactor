/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 远程通信
 *
 * 一个 SimpleRemoteManager 对应一个节点：持有一个 NetworkTransport（同时用于收发），
 * 通过 NetworkMessageSerializer 编解码消息，按目标 Actor 名称分发到本地处理器。
 * RemoteActorRef 把远程 Actor 包装成普通 ActorRef，实现位置透明。
 */
package cactor_remote

import std.collection.*
import std.sync.*
import cactor_core.actor.{ActorRef, ActorPath}
import cactor_core.message.{Message, NetworkMessage, NetworkMessageSerializer, WireIncompatibleException,
    UnsupportedWireFormatException, WireProtocol, DeserializationException}
import cactor_core.typed.{TypedActorRef, ClassicTypedRef, ensureTypedPayloadCodecRegistered, PromiseActorRef}
import cactor_foundation.network.{NetworkTransport, NetworkAddress, TransportFactory, TransportProtocol}
import cactor_foundation.logging.{Logger, Logging, LogRateLimiter}

let log_simple_remote: Logger = Logging.logger("cactor_remote.simple_remote")
// 线格式不兼容往往成批出现（混合版本期间每条消息都会撞上）：每 5 分钟逐条记前 10 条
let wireIncompatibleLog: LogRateLimiter = LogRateLimiter(10, Duration.minute * 5)

// 远程接收线程的当前 traceparent（P7/G2）：接收分发与本地投递在同一线程同步完成，
// 用线程局部把它交给 deliverLocal，避免改动公开的 registerHandler 签名。
let RECEIVED_TRACE: ThreadLocal<String> = ThreadLocal<String>()

/**
 * 远程消息发送器
 */
public class SimpleRemoteSender {
    private let transport: NetworkTransport
    private let ownsTransport: Bool
    private let networkMessageSerializer: NetworkMessageSerializer
    private let isStarted: AtomicBool
    private let sentCount: AtomicInt64
    // 分片消息 id 序号（与发送方地址组合即为全局唯一）
    private let chunkSeq: AtomicInt64 = AtomicInt64(0)
    // 线协议能力门禁（DA6）：由远程层装上；没有时按本节点全部能力编码（旧行为）
    private var wireGate: Option<WireGate> = None<WireGate>

    public init(localAddress: NetworkAddress, protocol: TransportProtocol) {
        this(TransportFactory.createTransport(protocol, localAddress), true)
    }

    /**
     * 使用共享传输创建（ownsTransport 为 false 时不负责启动/停止传输）
     */
    public init(transport: NetworkTransport, ownsTransport: Bool) {
        this.transport = transport
        this.ownsTransport = ownsTransport
        this.networkMessageSerializer = NetworkMessageSerializer()
        this.isStarted = AtomicBool(false)
        this.sentCount = AtomicInt64(0)
    }

    public func start(): Unit {
        if (isStarted.compareAndSwap(false, true) && ownsTransport) {
            transport.start()
        }
    }

    public func stop(): Unit {
        if (isStarted.compareAndSwap(true, false) && ownsTransport) {
            transport.stop()
        }
    }

    /**
     * 发送消息到远程节点上的指定 Actor；网络失败时抛出异常
     */
    public func sendMessage(targetAddress: NetworkAddress, targetActor: String, message: Message): Unit {
        sendMessage(targetAddress, targetActor, message, None<String>)
    }

    public func sendMessage(targetAddress: NetworkAddress, targetActor: String, message: Message, sender: Option<String>): Unit {
        if (!isStarted.load()) {
            throw IllegalStateException("发送器未启动")
        }
        let networkMessage = NetworkMessage(targetActor, message, sender)
        transmit(targetAddress, networkMessage, false)
        sentCount.fetchAdd(1)
    }

    func setWireGate(gate: WireGate): Unit {
        wireGate = Some(gate)
    }

    /** 可靠投递入队前的能力预检：对端读不了时计数、告警并抛 WireIncompatibleException */
    func checkWire(targetAddress: NetworkAddress, targetActor: String, message: Message): Unit {
        if (let Some(g) <- wireGate) {
            g.check(targetAddress, targetActor, message)
        }
    }

    /**
     * 编码并发送：协商了短类型 id / 压缩时按对端能力编码；
     * 超过协商帧上限时若对端支持分片则分片发送，否则明确抛 FrameTooLargeException（不静默丢弃）。
     */
    private func transmit(targetAddress: NetworkAddress, networkMessage: NetworkMessage, priority: Bool): Unit {
        var caps = WireProtocol.LOCAL_CAPABILITIES
        var maxFrame = 16 * 1024 * 1024
        var table: Option<HashMap<String, UInt16>> = None<HashMap<String, UInt16>>
        match (wireGate) {
            case Some(g) =>
                caps = g.capabilitiesOf(targetAddress)
                maxFrame = g.maxFrameOf(targetAddress)
                table = g.typeTableOf(targetAddress)
            case None => ()
        }
        let encoded = try {
            networkMessageSerializer.serialize(networkMessage, caps, table)
        } catch (e: WireIncompatibleException) {
            match (wireGate) {
                case Some(g) => g.onIncompatible(targetAddress, networkMessage.targetActor, networkMessage.message, e)
                case None => ()
            }
            throw e
        }
        // B3：达到阈值且对端能解压才压缩；压缩后不划算就发原文
        var data = encoded
        if (WireProtocol.supports(caps, WireProtocol.CAP_COMPRESSION) && encoded.size >= COMPRESSION_THRESHOLD) {
            let compressed = compressFrame(encoded)
            if (compressed.size < encoded.size) {
                data = compressed
            }
        }
        if (data.size <= maxFrame) {
            if (priority) {
                transport.sendPriority(targetAddress, data)
            } else {
                transport.send(targetAddress, data)
            }
            return
        }
        // 超过协商上限：对端支持分片就分片，否则明确失败（旧对端知道原因）
        if (!WireProtocol.supports(caps, WireProtocol.CAP_CHUNKING)) {
            throw FrameTooLargeException(data.size, maxFrame)
        }
        sendChunked(targetAddress, data, maxFrame, caps, table, priority)
    }

    /** 把已编码帧切片发送：每片是一个普通 NetworkMessage，目标为内部入口 $chunk */
    private func sendChunked(targetAddress: NetworkAddress, data: Array<UInt8>, maxFrame: Int64,
        caps: UInt64, table: Option<HashMap<String, UInt16>>, priority: Bool): Unit {
        let chunkSize = if (maxFrame > CHUNK_OVERHEAD_RESERVE + 256) { maxFrame - CHUNK_OVERHEAD_RESERVE } else { maxFrame / 2 }
        if (chunkSize < 64) {
            throw FrameTooLargeException(data.size, maxFrame)
        }
        let total = (data.size + chunkSize - 1) / chunkSize
        if (total <= 0 || total > 4096) {
            throw FrameTooLargeException(data.size, maxFrame)
        }
        let id = "${transport.getLocalAddress().toString()}#${chunkSeq.fetchAdd(1)}"
        let target = CHUNK_TARGET
        var index: Int64 = 0
        var offset: Int64 = 0
        while (offset < data.size) {
            let n = if (offset + chunkSize > data.size) { data.size - offset } else { chunkSize }
            let piece = data.slice(offset, n)
            let chunk = ChunkedPayload(id, index, total, target, piece)
            let nm = NetworkMessage(target, chunk, None<String>)
            let bytes = networkMessageSerializer.serialize(nm, caps, table)
            if (bytes.size > maxFrame) {
                throw FrameTooLargeException(bytes.size, maxFrame)
            }
            if (priority) {
                transport.sendPriority(targetAddress, bytes)
            } else {
                transport.send(targetAddress, bytes)
            }
            offset += n
            index++
        }
    }

    /**
     * 经传输的优先通道发送控制消息（远程层 $sys、集群心跳与 gossip，E40）：
     * 先于已积压的普通消息写出，大流量下心跳不被拖延成"节点不可达"
     */
    public func sendControl(targetAddress: NetworkAddress, targetActor: String, message: Message): Unit {
        if (!isStarted.load()) {
            throw IllegalStateException("发送器未启动")
        }
        let networkMessage = NetworkMessage(targetActor, message, None<String>)
        transmit(targetAddress, networkMessage, true)
        sentCount.fetchAdd(1)
    }

    public func getSentCount(): Int64 {
        sentCount.load()
    }

    public func isActive(): Bool {
        isStarted.load()
    }

    public func getLocalAddress(): NetworkAddress {
        transport.getLocalAddress()
    }
}

/**
 * 大消息分片重组缓冲。按片索引保存，集齐后按序拼回原帧。
 * 上限由发送端保证（≤4096 片、总长 ≤16 MiB），这里再兜一次，防止恶意对端占满内存。
 */
class ChunkBuffer {
    let total: Int64
    let parts = HashMap<Int64, Array<UInt8>>()
    var byteCount: Int64 = 0

    init(total: Int64) {
        this.total = total
    }

    /** 收下一片；集齐时返回完整帧 */
    func add(index: Int64, data: Array<UInt8>): Option<Array<UInt8>> {
        if (total <= 0 || total > 4096 || index < 0 || index >= total) {
            throw DeserializationException("非法的分片头 index=${index} total=${total}")
        }
        if (!parts.contains(index)) {
            byteCount += data.size
            if (byteCount > MAX_DECOMPRESSED_FRAME) {
                throw DeserializationException("分片重组超过 ${MAX_DECOMPRESSED_FRAME} 字节上限")
            }
            parts[index] = data
        }
        if (parts.size < total) {
            return None<Array<UInt8>>
        }
        let out = ArrayList<UInt8>()
        for (i in 0..total) {
            match (parts.get(i)) {
                case Some(b) => out.add(all: b)
                case None => return None<Array<UInt8>>
            }
        }
        Some(out.toArray())
    }
}

/**
 * 远程消息接收器
 */
public class SimpleRemoteReceiver {
    private let transport: NetworkTransport
    private let ownsTransport: Bool
    private let networkMessageSerializer: NetworkMessageSerializer
    private let isStarted: AtomicBool
    private let messageHandlers: HashMap<String, (Message, Option<String>) -> Unit>
    private let mutex: Mutex
    private let receivedCount: AtomicInt64
    private let undeliverableCount: AtomicInt64
    private let decodeFailures: AtomicInt64
    // 读不懂的版本 / 种类（混合版本不兼容，DA6）：decodeFailures 的子集，单独计数便于告警
    private let wireIncompatible: AtomicInt64 = AtomicInt64(0)
    private var wireListener: Option<(WireIncompatibleEvent) -> Unit> = None<(WireIncompatibleEvent) -> Unit>
    private var interceptor: Option<(NetworkMessage) -> Bool> = None<(NetworkMessage) -> Bool>
    // 协商的入站单帧上限（F5）；TCP 传输在更底层也会按它裁掉超限连接，这里兜住测试替身与其他传输
    private var maxInboundFrame: Int64 = 16 * 1024 * 1024
    private var oversizeHandler: Option<(Int64, Option<String>) -> Unit> = None<(Int64, Option<String>) -> Unit>
    // 大消息分片重组：(来源地址|id) -> 已收到的片
    private let reassembly = HashMap<String, ChunkBuffer>()
    private let reassembledFrames = AtomicInt64(0)
    private let compressedFrames = AtomicInt64(0)
    private let oversizeFrames = AtomicInt64(0)

    public init(localAddress: NetworkAddress, protocol: TransportProtocol) {
        this(TransportFactory.createTransport(protocol, localAddress), true)
    }

    public init(transport: NetworkTransport, ownsTransport: Bool) {
        this.transport = transport
        this.ownsTransport = ownsTransport
        this.networkMessageSerializer = NetworkMessageSerializer()
        this.isStarted = AtomicBool(false)
        this.messageHandlers = HashMap<String, (Message, Option<String>) -> Unit>()
        this.mutex = Mutex()
        this.receivedCount = AtomicInt64(0)
        this.undeliverableCount = AtomicInt64(0)
        this.decodeFailures = AtomicInt64(0)
        transport.setDataHandler(handleReceivedData)
    }

    public func start(): Unit {
        if (isStarted.compareAndSwap(false, true) && ownsTransport) {
            transport.start()
        }
    }

    public func stop(): Unit {
        if (isStarted.compareAndSwap(true, false) && ownsTransport) {
            transport.stop()
        }
    }

    /**
     * 注册消息处理器（同名覆盖）
     */
    public func registerHandler(actorName: String, handler: (Message) -> Unit): Unit {
        registerHandler(actorName, { m: Message, _: Option<String> => handler(m) })
    }

    public func registerHandler(actorName: String, handler: (Message, Option<String>) -> Unit): Unit {
        synchronized(mutex) {
            messageHandlers[actorName] = handler
        }
    }

    public func unregisterHandler(actorName: String): Unit {
        synchronized(mutex) {
            messageHandlers.remove(actorName)
        }
    }

    /** 在按名称分发之前处理消息（返回 true 表示已处理），供远程层处理控制消息和可靠投递信封 */
    public func setInterceptor(f: (NetworkMessage) -> Bool): Unit {
        synchronized(mutex) {
            interceptor = Some(f)
        }
    }

    private func handleReceivedData(data: Array<UInt8>): Unit {
        processFrame(data, true)
    }

    /** 解压 → 长度检查 → 反序列化 → 分片重组/分发；分片重组完成后递归进入本函数 */
    private func processFrame(raw: Array<UInt8>, allowChunks: Bool): Unit {
        // 入站上限只约束**线上的单帧**：分片重组后的逻辑消息、以及压缩帧解压出的原文，
        // 都可能大于单帧上限——这正是分片/压缩的意义。若在重组/解压后再查一次上限，
        // 刚拼好的大消息会被判成超限并回 GoAway（F5 用例 handshakeNegotiatesMaxFrameBytes 的根因）。
        if (allowChunks) {
            let frameLimit = synchronized(mutex) { maxInboundFrame }
            if (raw.size > frameLimit) {
                // 先在可解析范围内尽量取出源地址，供 GoAway 回送；取不到也不静默：计数 + 告警
                let source = try {
                    if (raw.size <= MAX_DECOMPRESSED_FRAME) {
                        networkMessageSerializer.deserialize(raw).sourceAddress
                    } else {
                        None<String>
                    }
                } catch (_: Exception) {
                    None<String>
                }
                onOversizeFrame(raw.size, source)
                return
            }
        }
        let data = try {
            if (isCompressedFrame(raw)) {
                compressedFrames.fetchAdd(1)
                decompressFrame(raw)
            } else {
                raw
            }
        } catch (e: Exception) {
            decodeFailures.fetchAdd(1)
            log_simple_remote.error("解压远程帧失败: ${e.message}")
            return
        }
        let networkMessage = try {
            networkMessageSerializer.deserialize(data)
        } catch (e: UnsupportedWireFormatException) {
            decodeFailures.fetchAdd(1)
            onUnsupportedWireFormat(e)
            return
        } catch (e: Exception) {
            decodeFailures.fetchAdd(1)
            log_simple_remote.error("反序列化网络消息失败: ${e.message}")
            return
        }
        // 大消息分片：不投递给业务，先按 (来源, id) 重组
        if (allowChunks) {
            if (let Some(chunk) <- (networkMessage.message as ChunkedPayload)) {
                onChunk(networkMessage, chunk)
                return
            }
        }
        receivedCount.fetchAdd(1)
        deliverDecoded(networkMessage)
    }

    private func deliverDecoded(networkMessage: NetworkMessage): Unit {
        if (let Some(f) <- synchronized(mutex) { interceptor }) {
            let consumed = try {
                f(networkMessage)
            } catch (e: Exception) {
                log_simple_remote.error("远程控制消息处理失败: ${e.message}")
                true
            }
            if (consumed) {
                return
            }
        }
        // 线程局部只覆盖本次同步分发；handler 内部（deliverLocal）据此给消息加上追踪包装
        RECEIVED_TRACE.set(networkMessage.traceParent)
        try {
            dispatch(networkMessage.targetActor, networkMessage.message, networkMessage.sender)
        } finally {
            RECEIVED_TRACE.set(None<String>)
        }
    }

    /** 收片；集齐后把重组出的原始帧按普通帧处理（不再接受嵌套分片） */
    private func onChunk(networkMessage: NetworkMessage, chunk: ChunkedPayload): Unit {
        // 分片 id 已经包含发送方地址与序号（见 SimpleRemoteSender.sendChunked），
        // 不再依赖帧尾的来源地址块——普通消息不为它多付字节。
        let key = chunk.id
        let complete = synchronized(mutex) {
            let buf = match (reassembly.get(key)) {
                case Some(b) => b
                case None =>
                    let b = ChunkBuffer(chunk.total)
                    reassembly[key] = b
                    b
            }
            let ready = try {
                buf.add(chunk.index, chunk.payload)
            } catch (e: Exception) {
                reassembly.remove(key)
                throw e
            }
            if (ready.isSome()) {
                reassembly.remove(key)
            }
            ready
        }
        match (complete) {
            case Some(frame) =>
                reassembledFrames.fetchAdd(1)
                processFrame(frame, false)
            case None => ()
        }
    }

    /** 入站帧超过协商上限：计数、告警，并交给远程层回 GoAway（TCP 传输在更底层已先发 GOAWAY 并断开） */
    private func onOversizeFrame(size: Int64, source: Option<String>) {
        decodeFailures.fetchAdd(1)
        oversizeFrames.fetchAdd(1)
        wireIncompatibleLog.tryLog({ text => log_simple_remote.warn(text) }, { =>
            "收到超过协商上限的帧：${size} 字节，上限 ${synchronized(mutex) { maxInboundFrame }}；" +
                "发送方 ${source ?? "?"}，将回 GoAway(FrameTooLarge)"
        })
        if (let Some(h) <- synchronized(mutex) { oversizeHandler }) {
            try {
                h(size, source)
            } catch (e: Exception) {
                log_simple_remote.error("处理超限帧失败: ${e.message}")
            }
        }
    }

    func setMaxInboundFrame(bytes: Int64): Unit {
        synchronized(mutex) {
            maxInboundFrame = bytes
        }
    }

    func setOversizeHandler(handler: (Int64, Option<String>) -> Unit): Unit {
        synchronized(mutex) {
            oversizeHandler = Some(handler)
        }
    }

    /** 入站超限帧数（含分片/压缩后超限） */
    public func getOversizeFrameCount(): Int64 {
        oversizeFrames.load()
    }

    /** 解压过的帧数 */
    public func getCompressedFrameCount(): Int64 {
        compressedFrames.load()
    }

    /** 重组完成的大消息数 */
    public func getReassembledFrameCount(): Int64 {
        reassembledFrames.load()
    }

    func closeInboundFrom(peerKey: String): Int64 {
        transport.closeInboundFrom(peerKey)
    }

    /** 读不懂的帧：不投递、单独计数、限速告警，并交给死信监听（帧本身无法还原成消息） */
    private func onUnsupportedWireFormat(e: UnsupportedWireFormatException): Unit {
        wireIncompatible.fetchAdd(1)
        let target = e.targetActor ?? "?"
        wireIncompatibleLog.tryLog({ text => log_simple_remote.warn(text) }, { =>
            "收到本节点读不懂的帧（发送方 ${e.senderPath ?? "?"} → ${target}，类型 ${e.messageType}）：${e.message}；" +
            "集群处于不兼容的混合版本，请先升级本节点或回退发送方"
        })
        if (let Some(l) <- synchronized(mutex) { wireListener }) {
            try {
                l(WireIncompatibleEvent(WireDirection.Inbound, e.senderPath, target, e.messageType, e.message,
                    None<Message>))
            } catch (ex: Exception) {
                log_simple_remote.error("线格式不兼容监听失败: ${ex.message}")
            }
        }
    }

    func setWireListener(l: (WireIncompatibleEvent) -> Unit): Unit {
        synchronized(mutex) {
            wireListener = Some(l)
        }
    }

    /** 读不懂（版本更高或种类未知）而丢弃的帧数，是 getDecodeFailures 的子集 */
    public func getWireIncompatibleCount(): Int64 {
        wireIncompatible.load()
    }

    /** 按目标名称把消息交给已注册的处理器；找不到时计为不可投递 */
    public func dispatch(targetActor: String, message: Message, sender: Option<String>): Unit {
        // 选择器：目标名写成 "$sel:<pattern>"，在这里展开通配并逐个投递（对端本地完成，零额外往返）
        if (targetActor.startsWith(SELECTION_PREFIX)) {
            dispatchSelection(targetActor[SELECTION_PREFIX.size..], message, sender)
            return
        }
        let handler = synchronized(mutex) { messageHandlers.get(targetActor) }
        match (handler) {
            case Some(h) =>
                try {
                    h(message, sender)
                } catch (e: Exception) {
                    log_simple_remote.error("远程消息处理失败(${targetActor}): ${e.message}")
                }
            case None =>
                undeliverableCount.fetchAdd(1)
        }
    }

    /**
     * 选择器展开：只匹配业务处理器；以 `$` 开头的内部入口（$sys、回复入口、ask 临时入口、
     * 其它选择器回复入口）永远不参与匹配，避免选择器打到内部协议或递归匹配自身。
     */
    private func dispatchSelection(pattern: String, message: Message, sender: Option<String>): Unit {
        let names = synchronized(mutex) {
            let out = ArrayList<String>()
            for ((name, _) in messageHandlers) {
                if (!name.startsWith("$") && selectionGlobMatch(pattern, name)) {
                    out.add(name)
                }
            }
            out.toArray()
        }
        if (names.isEmpty()) {
            undeliverableCount.fetchAdd(1)
            return
        }
        for (name in names) {
            dispatch(name, message, sender)
        }
    }

    /** 已登记的处理器数（暴露的 Actor、回复入口、ask 临时入口等；诊断用） */
    public func handlerCount(): Int64 {
        synchronized(mutex) {
            messageHandlers.size
        }
    }

    public func hasHandler(actorName: String): Bool {
        synchronized(mutex) {
            messageHandlers.contains(actorName)
        }
    }

    public func getReceivedCount(): Int64 {
        receivedCount.load()
    }

    /** 找不到目标 Actor 的消息数（死信） */
    public func getUndeliverableCount(): Int64 {
        undeliverableCount.load()
    }

    public func getDecodeFailures(): Int64 {
        decodeFailures.load()
    }

    public func isActive(): Bool {
        isStarted.load()
    }

    public func getLocalAddress(): NetworkAddress {
        transport.getLocalAddress()
    }
}

/**
 * 远程通信管理器（一个节点）
 */
public class SimpleRemoteManager {
    private let transport: NetworkTransport
    private let sender: SimpleRemoteSender
    private let receiver: SimpleRemoteReceiver
    private let started: AtomicBool
    let core: RemotingCore

    public init(localAddress: NetworkAddress, protocol: TransportProtocol) {
        this(TransportFactory.createTransport(protocol, localAddress))
    }

    /** 使用指定的传输（例如带故障注入的传输）创建；管理器负责启动和停止它 */
    public init(transport: NetworkTransport) {
        this.transport = transport
        this.sender = SimpleRemoteSender(transport, false)
        this.receiver = SimpleRemoteReceiver(transport, false)
        this.started = AtomicBool(false)
        this.core = RemotingCore(sender, receiver)
        core.attach(this)
    }

    public func start(): Unit {
        if (started.compareAndSwap(false, true)) {
            // 类型化消息的跨进程编解码（E63）：类型化 tell 的载荷是 TypedPayloadBox，默认序列化器不认识
            ensureTypedPayloadCodecRegistered()
            // 传输加密（E65）与握手（身份交换 + 可选的共享密钥鉴权，E7′）都须在传输开始收发之前装上；
            // 不支持加密的传输在这里抛异常，不会悄悄以明文运行
            if (let Some(sec) <- core.currentSettings().transportSecurity) {
                try {
                    transport.setSecurity(sec)
                } catch (e: Exception) {
                    started.store(false)
                    throw e
                }
                log_simple_remote.info("远程传输加密：${sec.describe()}")
            }
            transport.setHandshake(core.handshake())
            // F5：协商的帧上限下沉到传输层，入站超限时由传输回 GoAway 并断开
            try {
                transport.setMaxFrameBytes(core.currentWireSettings().maxFrameBytes)
            } catch (_: Exception) {
                // 测试替身或不支持该设置的传输：远程层仍会用接收器的上限兜住
            }
            transport.start()
            sender.start()
            receiver.start()
            core.start()
        }
    }

    public func stop(): Unit {
        if (started.compareAndSwap(true, false)) {
            core.stop()
            receiver.stop()
            sender.stop()
            transport.stop()
        }
    }

    public func sendMessage(targetAddress: NetworkAddress, targetActor: String, message: Message): Unit {
        sender.sendMessage(targetAddress, targetActor, message)
    }

    public func sendMessage(targetAddress: NetworkAddress, targetActor: String, message: Message, senderPath: Option<String>): Unit {
        sender.sendMessage(targetAddress, targetActor, message, senderPath)
    }

    /** 控制消息走优先通道（见 SimpleRemoteSender.sendControl） */
    public func sendControl(targetAddress: NetworkAddress, targetActor: String, message: Message): Unit {
        sender.sendControl(targetAddress, targetActor, message)
    }

    public func registerHandler(actorName: String, handler: (Message) -> Unit): Unit {
        receiver.registerHandler(actorName, handler)
    }

    /**
     * 把本地 Actor 以指定名称暴露给远程节点：收到的消息投递到该 Actor 的邮箱
     */
    public func exposeActor(actorName: String, localRef: ActorRef): Unit {
        core.expose(actorName, localRef)
    }

    public func unexposeActor(actorName: String): Unit {
        core.unexpose(actorName)
    }

    /**
     * 获取远程 Actor 的引用
     */
    public func remoteActorOf(targetAddress: NetworkAddress, actorName: String): RemoteActorRef {
        RemoteActorRef(this, targetAddress, actorName)
    }

    /**
     * 远程地址上的类型化引用（E63）：`tell` 的载荷以 TypedPayloadBox 过线，
     * 载荷类型必须是实现了 Message 的协议类型；`ask` 仍受限于"回复引用不能过线"，见计划 E63 记录。
     */
    public func typedRemoteActorOf<T>(targetAddress: NetworkAddress, actorName: String): TypedActorRef<T> {
        ClassicTypedRef<T>(remoteActorOf(targetAddress, actorName))
    }

    public func isActive(): Bool {
        started.load() && transport.isActive()
    }

    /** 实际监听地址（绑定端口 0 时为系统分配的端口） */
    public func getLocalAddress(): NetworkAddress {
        transport.getLocalAddress()
    }

    /**
     * 本进程的启动标识（每次创建管理器随机生成，握手时交给对端）。
     * 同一地址重启后标识不同，"地址#标识"才能唯一确定一个节点实例（对标 Akka UniqueAddress），
     * 集群成员与 CRDT 的节点标识都用它，避免新进程继承旧进程的身份（N4）
     */
    public func selfUid(): UInt64 {
        core.selfUid()
    }

    public func getSender(): SimpleRemoteSender {
        sender
    }

    /** 远程层内部使用：隔离对端时断开连接 */
    func isStarted(): Bool {
        started.load()
    }

    func transportInternal(): NetworkTransport {
        transport
    }

    public func getReceiver(): SimpleRemoteReceiver {
        receiver
    }
}

/**
 * 远程 Actor 引用：tell 通过网络发送到远程节点。
 * 与 Akka 相同，tell 只入队、不阻塞：对端不可达或出站队列满时消息被丢弃并计数（TcpTransport.getDroppedFrames），
 * 不抛异常；需要送达保证用 SimpleRemoteManager.tellReliable。传输未启动时抛 IllegalStateException
 */
public class RemoteActorRef <: ActorRef {
    private let manager: SimpleRemoteManager
    private let targetAddress: NetworkAddress
    private let actorName: String
    private let actorPath: ActorPath

    public init(manager: SimpleRemoteManager, targetAddress: NetworkAddress, actorName: String) {
        this.manager = manager
        this.targetAddress = targetAddress
        this.actorName = actorName
        this.actorPath = ActorPath("remote@${targetAddress.toString()}", "/user/${actorName}")
    }

    public prop path: ActorPath {
        get() { actorPath }
    }

    public func tell(message: Message): Unit {
        manager.sendMessage(targetAddress, actorName, message)
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        manager.sendMessage(targetAddress, actorName, message, sender)
    }

    /** 带发送方发送：对端 Actor 用 context.reply 回复时，回复会经网络送回 sender */
    public func tell(message: Message, sender: ActorRef): Unit {
        match (sender) {
            case reply: PromiseActorRef =>
                // 保留本地代理转发的 ask 生命周期；准入失败通过原 Promise 返回。
                try { manager.core.sendTypedAsk(this, message, reply) }
                catch (e: Exception) { reply.fail(e) }
            case _ => manager.sendMessage(targetAddress, actorName, message, Some(manager.core.senderInfoFor(sender)))
        }
    }

    public func equals(other: ActorRef): Bool {
        other.path.toString() == actorPath.toString()
    }

    public func hashCode(): Int32 {
        Int32(actorPath.toString().hashCode() & 0x7FFFFFFF)
    }

    public func getTargetAddress(): NetworkAddress {
        targetAddress
    }

    public func getActorName(): String {
        actorName
    }
}

/**
 * 远程通信工厂
 */
public class RemoteFactory {
    public static func createSender(address: NetworkAddress, protocol: TransportProtocol): SimpleRemoteSender {
        SimpleRemoteSender(address, protocol)
    }

    public static func createReceiver(address: NetworkAddress, protocol: TransportProtocol): SimpleRemoteReceiver {
        SimpleRemoteReceiver(address, protocol)
    }

    public static func createManager(address: NetworkAddress, protocol: TransportProtocol): SimpleRemoteManager {
        SimpleRemoteManager(address, protocol)
    }

    public static func createDefaultManager(port: Int32): SimpleRemoteManager {
        SimpleRemoteManager(NetworkAddress("127.0.0.1", port), TransportProtocol.TCP)
    }
}
