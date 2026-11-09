/*
 * Copyright (c) 2024. All rights reserved.
 * Foundation Layer - 网络传输层
 * 基于 std.net 的 TCP / UDP 字节传输
 *
 * TCP：长度前缀分帧（4 字节大端长度 + 负载），每个入站连接一个读协程；
 *      出站按目标地址建关联（OutboundConnection），连接、重连、写入都在关联的写线程里，send 只入队、永不阻塞
 * UDP：一条消息一个数据报（最大 64KB）
 * 加密（E65）：TCP 连接建立后、握手之前可经 TransportSecurity 升级为加密流（TLS 实现在 cactor_remote，本层不依赖 stdx）
 */
package cactor_foundation.network

import std.sync.*
import std.net.*
import std.time.*
import std.convert.*
import std.collection.*
import std.math.*
import cactor_foundation.logging.{Logger, Logging, LogRateLimiter}

let log_transport: Logger = Logging.logger("cactor_foundation.network.transport")

/**
 * 网络地址
 */
public struct NetworkAddress {
    public let host: String
    public let port: Int32

    public init(host: String, port: Int32) {
        this.host = host
        this.port = port
    }

    public func toString(): String {
        "${host}:${port}"
    }

    /**
     * 解析 "host:port"；缺少端口或端口非法时抛出 IllegalArgumentException
     */
    public static func parse(address: String): NetworkAddress {
        match (address.lastIndexOf(":")) {
            case Some(i) =>
                let host = address[0..i]
                let portText = address[i + 1..]
                match (Int32.tryParse(portText)) {
                    case Some(p) where p >= 0 && p <= 65535 && !host.isEmpty() => NetworkAddress(host, p)
                    case _ => throw IllegalArgumentException("非法的网络地址: ${address}")
                }
            case None => throw IllegalArgumentException("非法的网络地址（缺少端口）: ${address}")
        }
    }
}

/**
 * 网络数据包
 */
public struct NetworkPacket {
    public let targetAddress: String
    public let data: Array<UInt8>
    public let sender: Option<String>

    public init(targetAddress: String, data: Array<UInt8>, sender: Option<String>) {
        this.targetAddress = targetAddress
        this.data = data
        this.sender = sender
    }
}

/**
 * 传输协议
 */
public enum TransportProtocol {
    | TCP
    | UDP
}

/**
 * 连接握手（E7′）：远程层据此交换身份（地址 + 启动标识）并做共享密钥鉴权
 */
public interface TransportHandshake {
    /** 连接方：连上 peer 后发出的第一帧 */
    func hello(peer: NetworkAddress): Array<UInt8>

    /** 接受方：校验第一帧；接受时返回 (应答帧, 对端标识)，拒绝返回 None（连接被断开） */
    func accept(hello: Array<UInt8>): Option<(Array<UInt8>, String)>

    /** 连接方：校验应答帧；返回 false 时断开，本批帧按连接失败丢弃 */
    func confirm(peer: NetworkAddress, reply: Array<UInt8>): Bool
}

/**
 * 传输加密（E65）：TCP 连接建立后、应用层握手（TransportHandshake）之前，把连接升级为加密流。
 * 基础层只定义接口、不依赖 stdx / OpenSSL；TLS 实现见 cactor_remote.RemoteTls。
 * 两个方法都在该连接自己的协程里调用（出站在关联的写线程，入站在该连接的读协程），可以阻塞到握手完成或超时；
 * 失败抛异常，连接由传输关闭，计入握手失败（出站）/ 握手被拒（入站）
 */
public interface TransportSecurity {
    /** 出站：对已连接的 TCP 连接做客户端加密握手，返回在其上收发的流 */
    func secureOutbound(socket: TcpSocket, peer: NetworkAddress, timeout: Duration): StreamingSocket

    /** 入站：对已接受的 TCP 连接做服务端加密握手，返回在其上收发的流 */
    func secureInbound(socket: TcpSocket, timeout: Duration): StreamingSocket

    /** 描述（日志与诊断），例如 "TLS 1.3 mTLS" */
    func describe(): String
}

/**
 * 一条 TCP 连接及其上的收发流：未加密时就是 TCP 连接本身，加密后是 TransportSecurity 返回的流。
 * 读写只由拥有它的协程进行；close 可能来自别的线程（停止传输、隔离对端、重置出站连接），
 * 所以先关原始 TCP 连接：TLS 流的 close 要等阻塞在 read 上的读协程退出（stdx 实测约 1 s），
 * 关底层连接则立即唤醒它（实测 1 ms），之后再关加密流（此时不再阻塞）
 */
class Channel {
    let raw: TcpSocket
    private let lock = Mutex()
    private var stream: StreamingSocket
    private var secured: Bool = false

    init(raw: TcpSocket) {
        this.raw = raw
        this.stream = raw
    }

    /** 换成加密流（在发布给其他线程使用之前、由拥有它的协程调用） */
    func upgrade(s: StreamingSocket): Unit {
        synchronized(lock) {
            stream = s
            secured = true
        }
    }

    func isSecured(): Bool {
        synchronized(lock) { secured }
    }

    func read(buf: Array<UInt8>): Int64 {
        stream.read(buf)
    }

    func write(data: Array<UInt8>): Unit {
        stream.write(data)
    }

    func setReadTimeout(t: ?Duration): Unit {
        stream.readTimeout = t
    }

    func close(): Unit {
        let (s, sec) = synchronized(lock) { (stream, secured) }
        closeQuietly({ => raw.close() })
        if (sec) {
            closeQuietly({ => s.close() })
        }
    }
}

/** 握手帧的长度上限：握手只有一行文本，防止未鉴权的对端用超大长度字段耗尽内存 */
public let MAX_HANDSHAKE_FRAME: Int64 = 4096
/** 接受方等待第一帧的时限：防止不发握手的连接一直占着读协程 */
public let HANDSHAKE_READ_TIMEOUT: Duration = Duration.second * 5

/** 从 reader 读一帧（限长）；对端关闭或长度非法返回 None */
func readFrameLimited(reader: FrameReader, maxLen: Int64): Option<Array<UInt8>> {
    let header = Array<UInt8>(4, repeat: 0)
    if (!reader.readFully(header)) {
        return None<Array<UInt8>>
    }
    let len = decodeLength(header)
    if (len < 0 || len > maxLen) {
        return None<Array<UInt8>>
    }
    let payload = Array<UInt8>(len, repeat: 0)
    if (len > 0 && !reader.readFully(payload)) {
        return None<Array<UInt8>>
    }
    Some(payload)
}

/**
 * 网络传输接口
 */
public interface NetworkTransport {
    /** 开始监听本地地址 */
    func start(): Unit

    /** 停止监听并关闭所有连接 */
    func stop(): Unit

    /**
     * 发送一条完整消息。TCP 实现只入队、不阻塞：对端不可达或队列满时丢弃并计数，不抛异常；
     * 传输未启动、消息超过帧上限时抛异常
     */
    func send(address: NetworkAddress, data: Array<UInt8>): Unit

    /**
     * 经优先通道发送（控制消息：心跳、gossip、远程层的 $sys）。
     * 默认等同 send；TCP 实现中优先于已积压的普通数据写出（E40）
     */
    func sendPriority(address: NetworkAddress, data: Array<UInt8>): Unit {
        send(address, data)
    }

    /** 设置收到完整消息时的回调 */
    func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit

    /**
     * 设置连接握手（E7′）：连接方连上后先发 hello、读应答并 confirm；接受方先读第一帧 accept，
     * 不通过就断开，帧不会交给数据回调。须在 start 之前设置。默认实现忽略（不支持握手的传输）
     */
    func setHandshake(handshake: TransportHandshake): Unit {}

    /**
     * 设置传输加密（E65）：此后建立的每条连接在握手之前先完成加密握手，明文或证书不通过的连接被断开。
     * 须在 start 之前设置。默认实现抛异常：不支持加密的传输（UDP、测试替身）不能悄悄退回明文
     */
    func setSecurity(security: TransportSecurity): Unit {
        throw UnsupportedException("该传输不支持加密（${security.describe()} 只用于 TCP 传输）")
    }

    /** 断开握手时登记为 peerKey 的全部入站连接（隔离对端用），返回断开的连接数 */
    func closeInboundFrom(peerKey: String): Int64 {
        0
    }

    /** 关闭发往该地址的出站连接，下次发送重新连接并握手 */
    func resetOutbound(address: NetworkAddress): Unit {}

    /** 实际监听地址（绑定端口 0 时返回系统分配的端口） */
    func getLocalAddress(): NetworkAddress

    func isActive(): Bool
}

/** 帧大小上限，防止恶意或损坏的长度字段导致内存耗尽 */
public let MAX_FRAME_SIZE: Int64 = 16 * 1024 * 1024

/** 可增长的字节缓冲区（只追加，整体取出） */
class ByteBuffer {
    var data: Array<UInt8>
    var size: Int64 = 0

    init(capacity: Int64) {
        this.data = Array<UInt8>(capacity, repeat: 0)
    }

    func append(bytes: Array<UInt8>): Unit {
        let need = size + bytes.size
        if (need > data.size) {
            var cap = data.size * 2
            while (cap < need) {
                cap *= 2
            }
            let grown = Array<UInt8>(cap, repeat: 0)
            data.copyTo(grown, 0, 0, size)
            data = grown
        }
        bytes.copyTo(data, 0, size, bytes.size)
        size = need
    }

    func clear(): Unit {
        size = 0
        // 偶尔出现的大批量不要一直占着内存
        if (data.size > 1024 * 1024) {
            data = Array<UInt8>(64 * 1024, repeat: 0)
        }
    }
}

/** 单个目标地址允许积压的普通数据上限，超过后丢弃新帧并计数（不阻塞发送方） */
public let MAX_PENDING_BYTES: Int64 = 64 * 1024 * 1024
/** 单个目标地址允许积压的控制数据上限（优先通道） */
public let MAX_CONTROL_PENDING_BYTES: Int64 = 4 * 1024 * 1024
/** 普通数据每次 write 的上限：两段之间先写控制帧，控制消息最多等一段 */
let WRITE_CHUNK_BYTES: Int64 = 256 * 1024
/** 连接失败后的退避：100 ms 起翻倍，封顶 5 s，连上后复位 */
let RECONNECT_BACKOFF_MIN: Duration = Duration.millisecond * 100
let RECONNECT_BACKOFF_MAX: Duration = Duration.second * 5
/** 没有连接、也没有待发数据超过这个时长，关联退出并从传输中移除（防止伪造或失效地址堆积写线程） */
let IDLE_RETIRE_AFTER: Duration = Duration.second * 10

// 丢帧可能成批出现：每 5 分钟逐条记前 10 条
let dropLog: LogRateLimiter = LogRateLimiter(10, Duration.minute * 5)

/** 关联的统计，由 TcpTransport 持有、各关联共享 */
class TransportCounters {
    let framesDropped = AtomicInt64(0)
    let connectFailures = AtomicInt64(0)
    let writes = AtomicInt64(0)
    // 出站握手失败（对端应答不通过或超时）
    let handshakeFailures = AtomicInt64(0)
    // 入站握手被拒绝（第一帧不通过、超时或非法；加密握手失败，如明文连接或证书不通过）
    let handshakeRejected = AtomicInt64(0)
    // 经 TransportSecurity 建立的加密连接数（出站 + 入站）
    let securedConnections = AtomicInt64(0)
}

/** 入队结果 */
enum Enqueued {
    | Accepted
    | Dropped
    | Retired
}

/** 写线程取批次的结果 */
enum Taken {
    | Batch(FrameQueue, FrameQueue)
    | Idle
    | Exit
}

/** 一组已编码的帧：字节缓冲 + 帧数（连接失败时按帧计丢弃） */
class FrameQueue {
    var buf: ByteBuffer
    var frames: Int64 = 0

    init(capacity: Int64) {
        this.buf = ByteBuffer(capacity)
    }
}

/**
 * 出站关联：一个目标地址一个，对标 Akka Artery 的 Association（N1 / E40）。
 *
 * - 发送方只把帧追加到队列并唤醒写线程，**从不做网络操作、从不阻塞**：
 *   连接、重连、写入都在关联自己的写线程里完成。黑洞节点只拖住它自己的写线程。
 * - 两条队列：控制帧（优先通道，远程层 $sys、集群心跳与 gossip）与普通帧。
 *   写线程先写控制帧；普通数据按 WRITE_CHUNK_BYTES 分段写，段间再检查控制队列。
 * - 队列有界：超过上限丢弃新帧并计数（与 Akka 出站队列满时丢弃一致）。
 * - 连接失败：本批帧丢弃并计数（至多一次；需要可靠的由远程层的可靠投递重发），按指数退避重连。
 * - 没有连接且空闲超过 idleRetireAfter：写线程退出、关联从传输中移除，下次发送重新建立。
 * - 写失败：关闭连接，下一批重新连接（对端重启后自动恢复）。
 */
class OutboundConnection {
    let address: NetworkAddress
    private let connectTimeout: Duration
    private let idleRetireAfter: Duration
    private let counters: TransportCounters
    private let onRetire: (OutboundConnection) -> Unit
    private let handshakeOf: () -> Option<TransportHandshake>
    private let securityOf: () -> Option<TransportSecurity>
    private let lock: Mutex = Mutex()
    private let cond: Condition
    private var control: FrameQueue = FrameQueue(4 * 1024)
    private var controlSpare: FrameQueue = FrameQueue(4 * 1024)
    private var pending: FrameQueue = FrameQueue(64 * 1024)
    private var pendingSpare: FrameQueue = FrameQueue(64 * 1024)
    private var socket: Option<Channel> = None<Channel>
    private var closed: Bool = false
    private var retired: Bool = false
    private var writerDone: Bool = false
    private var writerWaiting: Bool = false

    init(address: NetworkAddress, connectTimeout: Duration, idleRetireAfter: Duration, counters: TransportCounters,
        onRetire: (OutboundConnection) -> Unit, handshakeOf: () -> Option<TransportHandshake>,
        securityOf: () -> Option<TransportSecurity>) {
        this.address = address
        this.connectTimeout = connectTimeout
        this.idleRetireAfter = idleRetireAfter
        this.counters = counters
        this.onRetire = onRetire
        this.handshakeOf = handshakeOf
        this.securityOf = securityOf
        this.cond = synchronized(lock) { lock.condition() }
    }

    func startWriter(): Unit {
        spawn {
            writerLoop()
        }
    }

    /** 追加一帧（不阻塞）；Retired 表示关联已退出，调用方应换一个新关联 */
    func enqueue(frame: Array<UInt8>, priority: Bool): Enqueued {
        synchronized(lock) {
            if (retired) {
                return Enqueued.Retired
            }
            if (closed) {
                return Enqueued.Dropped
            }
            let q = if (priority) { control } else { pending }
            let limit = if (priority) { MAX_CONTROL_PENDING_BYTES } else { MAX_PENDING_BYTES }
            if (q.buf.size + frame.size > limit) {
                return Enqueued.Dropped
            }
            q.buf.append(frame)
            q.frames++
            if (writerWaiting) {
                cond.notifyAll()
            }
            Enqueued.Accepted
        }
    }

    /** 关闭：已连接时等积压写出（最多 1 秒），再关闭 socket；未连接时直接丢弃积压 */
    func close(): Unit {
        let deadline = MonoTime.now() + Duration.second
        let s = synchronized(lock) {
            closed = true
            cond.notifyAll()
            while (!writerDone && MonoTime.now() < deadline) {
                cond.wait(timeout: Duration.millisecond * 10)
            }
            socket
        }
        if (let Some(sock) <- s) {
            closeQuietly({ => sock.close() })
        }
    }

    private func dropFrames(n: Int64, reason: String): Unit {
        if (n > 0) {
            counters.framesDropped.fetchAdd(n)
            dropLog.tryLog({ text => log_transport.warn(text) }, { => "发往 ${address.toString()} 的 ${n} 帧被丢弃：${reason}" })
        }
    }

    /**
     * 取走两条队列中的全部帧；没有数据时等待。
     * 没有连接时最多等 idleRetireAfter，超时返回 Idle（写线程据此退出）；已关闭且无数据返回 Exit
     */
    private func takeBatch(): Taken {
        let idleDeadline = MonoTime.now() + idleRetireAfter
        synchronized(lock) {
            while (control.frames == 0 && pending.frames == 0 && !closed) {
                if (socket.isNone() && MonoTime.now() >= idleDeadline) {
                    return Taken.Idle
                }
                writerWaiting = true
                cond.wait(timeout: Duration.millisecond * 100)
                writerWaiting = false
            }
            if (control.frames == 0 && pending.frames == 0) {
                return Taken.Exit
            }
            if (closed && socket.isNone()) {
                // 关闭时还没连上：不再为这批数据去连接
                let n = control.frames + pending.frames
                control.buf.clear()
                control.frames = 0
                pending.buf.clear()
                pending.frames = 0
                dropFrames(n, "传输已关闭")
                return Taken.Exit
            }
            let c = control
            control = controlSpare
            controlSpare = c
            let p = pending
            pending = pendingSpare
            pendingSpare = p
            Taken.Batch(c, p)
        }
    }

    /** 取走控制队列（分段写普通数据时插队用） */
    private func takeControl(): Option<FrameQueue> {
        synchronized(lock) {
            if (control.frames == 0) {
                return None<FrameQueue>
            }
            // controlSpare 此时一定为空：上一批控制帧已写完并清空
            let c = control
            control = controlSpare
            controlSpare = c
            Some(c)
        }
    }

    private func release(q: FrameQueue): Unit {
        synchronized(lock) {
            q.buf.clear()
            q.frames = 0
        }
    }

    /** 退避等待：新数据不提前结束等待（留到下一次连接尝试），只有关闭会提前结束 */
    private func backoffWait(d: Duration): Unit {
        let deadline = MonoTime.now() + d
        synchronized(lock) {
            while (!closed) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    break
                }
                cond.wait(timeout: remaining)
            }
        }
    }

    /** 空闲且没有连接：退出并从传输中移除（入队与退出在同一把锁下互斥，不会丢帧） */
    private func tryRetire(): Bool {
        let ok = synchronized(lock) {
            if (control.frames == 0 && pending.frames == 0) {
                retired = true
                true
            } else {
                false
            }
        }
        if (ok) {
            onRetire(this)
        }
        ok
    }

    private func connect(): Bool {
        let sock = try {
            let sock = TcpSocket(address.host, UInt16(address.port))
            sock.connect(timeout: connectTimeout)
            sock.noDelay = true
            sock
        } catch (e: Exception) {
            counters.connectFailures.fetchAdd(1)
            log_transport.debug("连接 ${address.toString()} 失败: ${e.message}")
            return false
        }
        let s = Channel(sock)
        // 加密握手先于应用层握手：身份与鉴权帧只在加密流上传输
        if (let Some(sec) <- securityOf()) {
            try {
                s.upgrade(sec.secureOutbound(sock, address, connectTimeout))
                counters.securedConnections.fetchAdd(1)
            } catch (e: Exception) {
                counters.handshakeFailures.fetchAdd(1)
                log_transport.debug("与 ${address.toString()} 的加密握手失败: ${e.message}")
                s.close()
                return false
            }
        }
        if (let Some(h) <- handshakeOf()) {
            // 握手：先发 hello，再在 connectTimeout 内读对端应答并校验（双向鉴权）
            let ok = try {
                s.write(encodeFrame(h.hello(address)))
                s.setReadTimeout(connectTimeout)
                let reply = readFrameLimited(FrameReader(s), MAX_HANDSHAKE_FRAME)
                s.setReadTimeout(None)
                match (reply) {
                    case Some(r) => h.confirm(address, r)
                    case None => false
                }
            } catch (e: Exception) {
                log_transport.debug("与 ${address.toString()} 握手失败: ${e.message}")
                false
            }
            if (!ok) {
                counters.handshakeFailures.fetchAdd(1)
                s.close()
                return false
            }
        }
        synchronized(lock) {
            socket = Some(s)
        }
        true
    }

    private func disconnect(): Unit {
        let s = synchronized(lock) {
            let old = socket
            socket = None<Channel>
            old
        }
        if (let Some(sock) <- s) {
            sock.close()
        }
    }

    private func writeAll(q: FrameQueue): Bool {
        if (q.frames == 0) {
            return true
        }
        let sock = synchronized(lock) { socket }
        match (sock) {
            case Some(s) =>
                try {
                    s.write(q.buf.data[0..q.buf.size])
                    counters.writes.fetchAdd(1)
                    true
                } catch (e: Exception) {
                    log_transport.debug("写 ${address.toString()} 失败: ${e}")
                    false
                }
            case None => false
        }
    }

    /** 从 offset 起累计整帧，直到不少于 WRITE_CHUNK_BYTES 或缓冲区末尾，返回段尾（必为帧边界） */
    private static func frameBoundary(buf: ByteBuffer, offset: Int64): Int64 {
        let d = buf.data
        var pos = offset
        while (pos < buf.size && pos - offset < WRITE_CHUNK_BYTES) {
            let len = (Int64(d[pos]) << 24) | (Int64(d[pos + 1]) << 16) | (Int64(d[pos + 2]) << 8) | Int64(d[pos + 3])
            pos += 4 + len
        }
        pos
    }

    /** 普通数据分段写（段在帧边界上），段间先写控制帧 */
    private func writeChunked(p: FrameQueue): Bool {
        let sock = synchronized(lock) { socket }
        let s = match (sock) {
            case Some(x) => x
            case None => return false
        }
        var offset: Int64 = 0
        while (offset < p.buf.size) {
            // 段必须落在帧边界上：控制帧会插在两段之间，切在帧中间会破坏帧流
            let end = frameBoundary(p.buf, offset)
            try {
                s.write(p.buf.data[offset..end])
                counters.writes.fetchAdd(1)
            } catch (e: Exception) {
                log_transport.debug("写 ${address.toString()} 失败: ${e}")
                return false
            }
            offset = end
            if (offset < p.buf.size) {
                if (let Some(c) <- takeControl()) {
                    let ok = writeAll(c)
                    let n = c.frames
                    release(c)
                    if (!ok) {
                        dropFrames(n, "写入失败")
                        return false
                    }
                }
            }
        }
        true
    }

    private func writerLoop(): Unit {
        var backoff = RECONNECT_BACKOFF_MIN
        try {
            while (true) {
                let (c, p) = match (takeBatch()) {
                    case Batch(cq, pq) => (cq, pq)
                    case Idle =>
                        if (tryRetire()) {
                            break
                        }
                        continue
                    case Exit => break
                }
                let connected = synchronized(lock) { socket.isSome() }
                if (!connected && !connect()) {
                    dropFrames(c.frames + p.frames, "连接失败")
                    release(c)
                    release(p)
                    backoffWait(backoff)
                    backoff = if (backoff * 2 > RECONNECT_BACKOFF_MAX) { RECONNECT_BACKOFF_MAX } else { backoff * 2 }
                    continue
                }
                backoff = RECONNECT_BACKOFF_MIN
                // 先写控制帧并立即归还（分段写普通帧时插队的控制帧要用这块备用缓冲），再分段写普通帧
                let okControl = writeAll(c)
                let controlFrames = c.frames
                release(c)
                let okPending = okControl && writeChunked(p)
                if (!okControl || !okPending) {
                    // 写失败：对端多半已关闭（重启），丢弃本批，下一批重新连接
                    dropFrames(if (okControl) { p.frames } else { controlFrames + p.frames }, "写入失败")
                    disconnect()
                }
                release(p)
            }
        } finally {
            let wasClosed = synchronized(lock) {
                writerDone = true
                cond.notifyAll()
                closed
            }
            // close() 只等 1 秒：写线程若当时正卡在 connect，连上后由这里关闭
            if (wasClosed) {
                disconnect()
            }
        }
    }
}

/** 带缓冲的帧读取：一次 read 读入多帧，减少系统调用 */
class FrameReader {
    private let socket: Channel
    private let buffer: Array<UInt8> = Array<UInt8>(64 * 1024, repeat: 0)
    private var start: Int64 = 0
    private var end: Int64 = 0

    init(socket: Channel) {
        this.socket = socket
    }

    /** 读满 dst；对端关闭返回 false */
    func readFully(dst: Array<UInt8>): Bool {
        var offset: Int64 = 0
        while (offset < dst.size) {
            if (start >= end) {
                // 大块数据直接读入目标，避免二次拷贝
                if (dst.size - offset >= buffer.size) {
                    let n = socket.read(dst[offset..])
                    if (n <= 0) {
                        return false
                    }
                    offset += n
                    continue
                }
                let n = socket.read(buffer)
                if (n <= 0) {
                    return false
                }
                start = 0
                end = n
            }
            let take = min(end - start, dst.size - offset)
            buffer.copyTo(dst, start, offset, take)
            start += take
            offset += take
        }
        true
    }
}

/**
 * TCP 传输
 */
public class TcpTransport <: NetworkTransport {
    private let requestedAddress: NetworkAddress
    private var boundPort: Int32
    private let isRunning: AtomicBool
    private var dataHandler: Option<(Array<UInt8>) -> Unit>
    private let mutex: Mutex
    private var server: Option<TcpServerSocket>
    private let outbound: HashMap<String, OutboundConnection>
    private let inbound: ArrayList<Channel>
    // 握手通过的入站连接：(连接, 对端标识)，隔离对端时按标识断开
    private let inboundPeers: ArrayList<(Channel, String)> = ArrayList<(Channel, String)>()
    private var handshake: Option<TransportHandshake> = None<TransportHandshake>
    private var security: Option<TransportSecurity> = None<TransportSecurity>
    private let connectTimeout: Duration
    // 包内可调（测试用）：没有连接的空闲关联多久后退出
    var idleRetireAfter: Duration = IDLE_RETIRE_AFTER

    // 统计
    private let framesSent: AtomicInt64 = AtomicInt64(0)
    private let framesReceived: AtomicInt64 = AtomicInt64(0)
    private let counters: TransportCounters = TransportCounters()

    public init(localAddress: NetworkAddress) {
        this(localAddress, Duration.second * 3)
    }

    public init(localAddress: NetworkAddress, connectTimeout: Duration) {
        this.requestedAddress = localAddress
        this.boundPort = localAddress.port
        this.isRunning = AtomicBool(false)
        this.dataHandler = None
        this.mutex = Mutex()
        this.server = None
        this.outbound = HashMap<String, OutboundConnection>()
        this.inbound = ArrayList<Channel>()
        this.connectTimeout = connectTimeout
    }

    public func start(): Unit {
        if (!isRunning.compareAndSwap(false, true)) {
            return
        }
        let s = TcpServerSocket(bindAt: UInt16(requestedAddress.port))
        s.reuseAddress = true
        s.bind()
        if (let Some(addr) <- (s.localAddress as IPSocketAddress)) {
            boundPort = Int32(addr.port)
        }
        synchronized(mutex) {
            server = Some(s)
        }
        spawn {
            acceptLoop(s)
        }
    }

    public func stop(): Unit {
        if (!isRunning.compareAndSwap(true, false)) {
            return
        }
        let (srv, outs, ins) = synchronized(mutex) {
            let srv = server
            server = None
            let outs = ArrayList<OutboundConnection>()
            for ((_, c) in outbound) {
                outs.add(c)
            }
            outbound.clear()
            let ins = inbound.toArray()
            inbound.clear()
            (srv, outs, ins)
        }
        if (let Some(s) <- srv) {
            closeQuietly({ => s.close() })
        }
        for (c in outs) {
            closeQuietly({ => c.close() })
        }
        for (c in ins) {
            closeQuietly({ => c.close() })
        }
    }

    /**
     * 入队发送（不阻塞、不做网络操作）：对端不可达、连接失败或队列满时丢弃并计数，不抛异常。
     * 传输未启动、消息超过帧上限时抛异常（调用方的编程错误）
     */
    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        enqueue(address, data, false)
    }

    /** 优先通道：先于已积压的普通数据写出 */
    public func sendPriority(address: NetworkAddress, data: Array<UInt8>): Unit {
        enqueue(address, data, true)
    }

    private func enqueue(address: NetworkAddress, data: Array<UInt8>, priority: Bool): Unit {
        if (!isRunning.load()) {
            throw IllegalStateException("TCP 传输未启动")
        }
        if (data.size > MAX_FRAME_SIZE) {
            throw IllegalArgumentException("消息过大: ${data.size} 字节，上限 ${MAX_FRAME_SIZE}")
        }
        let frame = encodeFrame(data)
        // 关联可能恰好因长期连不上而退出：换一个新关联再试一次
        for (_ in 0..2) {
            match (association(address).enqueue(frame, priority)) {
                case Enqueued.Accepted =>
                    framesSent.fetchAdd(1)
                    return
                case Enqueued.Dropped =>
                    counters.framesDropped.fetchAdd(1)
                    dropLog.tryLog({ text => log_transport.warn(text) }, { =>
                        "发往 ${address.toString()} 的队列已满（${if (priority) { "控制" } else { "普通" }}），丢弃 1 帧"
                    })
                    return
                case Enqueued.Retired => ()
            }
        }
        counters.framesDropped.fetchAdd(1)
    }

    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit {
        synchronized(mutex) {
            dataHandler = Some(handler)
        }
    }

    public func setHandshake(handshake: TransportHandshake): Unit {
        synchronized(mutex) {
            this.handshake = Some(handshake)
        }
    }

    private func currentHandshake(): Option<TransportHandshake> {
        synchronized(mutex) { handshake }
    }

    /** 设置传输加密（E65）。启动后再设置会让已建立的明文连接与新的加密连接并存，所以只允许在 start 之前设置 */
    public func setSecurity(security: TransportSecurity): Unit {
        synchronized(mutex) {
            if (isRunning.load()) {
                throw IllegalStateException("传输加密须在 start 之前设置")
            }
            this.security = Some(security)
        }
    }

    private func currentSecurity(): Option<TransportSecurity> {
        synchronized(mutex) { security }
    }

    /** 经传输加密建立的连接数（出站 + 入站）；未设置加密时恒为 0 */
    public func getSecuredConnections(): Int64 { counters.securedConnections.load() }

    public func closeInboundFrom(peerKey: String): Int64 {
        let victims = synchronized(mutex) {
            let v = ArrayList<Channel>()
            for ((sock, key) in inboundPeers) {
                if (key == peerKey) {
                    v.add(sock)
                }
            }
            inboundPeers.removeIf({ p => p[1] == peerKey })
            v
        }
        for (sock in victims) {
            sock.close()
        }
        victims.size
    }

    public func resetOutbound(address: NetworkAddress): Unit {
        let removed = synchronized(mutex) { outbound.remove(address.toString()) }
        if (let Some(c) <- removed) {
            spawn {
                c.close()
            }
        }
    }

    /** 出站握手失败次数（对端应答不通过或超时） */
    public func getHandshakeFailures(): Int64 { counters.handshakeFailures.load() }

    /** 入站握手被拒绝的连接数 */
    public func getRejectedHandshakes(): Int64 { counters.handshakeRejected.load() }

    public func getLocalAddress(): NetworkAddress {
        NetworkAddress(requestedAddress.host, boundPort)
    }

    public func isActive(): Bool {
        isRunning.load()
    }

    /** 已入队的帧数（含之后因连接失败被丢弃的） */
    public func getFramesSent(): Int64 { framesSent.load() }

    public func getFramesReceived(): Int64 { framesReceived.load() }

    /** 没能写出的帧数：队列满、连接失败、写失败、关闭时未连上（E40 起 send 不再抛异常，失败都计在这里） */
    public func getSendFailures(): Int64 { counters.framesDropped.load() }

    /** 同 getSendFailures */
    public func getDroppedFrames(): Int64 { counters.framesDropped.load() }

    /** 连接失败次数（每次尝试计一次，含退避后的重试） */
    public func getConnectFailures(): Int64 { counters.connectFailures.load() }

    /** 当前的出站关联数（长期连不上且没有新数据的关联会自动退出） */
    public func getAssociationCount(): Int64 {
        synchronized(mutex) { outbound.size }
    }

    /** 实际执行的 write 系统调用次数（与 getFramesSent 对比可看出合并效果） */
    public func getSocketWrites(): Int64 {
        counters.writes.load()
    }

    /** 取或建目标地址的关联：只操作内存，不做网络操作 */
    private func association(address: NetworkAddress): OutboundConnection {
        let key = address.toString()
        let me = this
        let (conn, created) = synchronized(mutex) {
            // 与 stop() 的"清空关联表"互斥：停止之后不再建新关联（否则写线程泄漏）
            if (!isRunning.load()) {
                throw IllegalStateException("TCP 传输未启动")
            }
            match (outbound.get(key)) {
                case Some(existing) => (existing, false)
                case None =>
                    let c = OutboundConnection(address, connectTimeout, idleRetireAfter, counters, { retired => me.removeAssociation(retired) },
                        { => me.currentHandshake() }, { => me.currentSecurity() })
                    outbound.add(key, c)
                    (c, true)
            }
        }
        if (created) {
            conn.startWriter()
        }
        conn
    }

    private func removeAssociation(conn: OutboundConnection): Unit {
        synchronized(mutex) {
            let key = conn.address.toString()
            if (let Some(current) <- outbound.get(key)) {
                if (refEq(current, conn)) {
                    outbound.remove(key)
                }
            }
        }
    }

    private func acceptLoop(s: TcpServerSocket): Unit {
        while (isRunning.load()) {
            try {
                let raw = s.accept()
                raw.noDelay = true
                let client = Channel(raw)
                synchronized(mutex) {
                    inbound.add(client)
                }
                // 加密握手与应用层握手都在该连接自己的协程里做：慢速或恶意的对端不会卡住 accept
                spawn {
                    readLoop(client)
                }
            } catch (e: Exception) {
                if (isRunning.load()) {
                    log_transport.error("TCP accept 失败: ${e.message}")
                }
            }
        }
    }

    private func readLoop(client: Channel): Unit {
        let header = Array<UInt8>(4, repeat: 0)
        let reader = FrameReader(client)
        if (let Some(sec) <- currentSecurity()) {
            // 明文连接、证书不通过或握手超时：断开，帧不会交给握手与数据回调
            let ok = try {
                client.upgrade(sec.secureInbound(client.raw, HANDSHAKE_READ_TIMEOUT))
                counters.securedConnections.fetchAdd(1)
                true
            } catch (e: Exception) {
                log_transport.debug("入站加密握手失败: ${e.message}")
                false
            }
            if (!ok) {
                counters.handshakeRejected.fetchAdd(1)
                synchronized(mutex) {
                    inbound.removeIf({ c => refEq(c, client) })
                }
                client.close()
                return
            }
        }
        if (let Some(h) <- currentHandshake()) {
            if (!acceptHandshake(client, reader, h)) {
                counters.handshakeRejected.fetchAdd(1)
                synchronized(mutex) {
                    inbound.removeIf({ c => refEq(c, client) })
                }
                client.close()
                return
            }
        }
        try {
            while (isRunning.load()) {
                if (!reader.readFully(header)) {
                    break
                }
                let len = decodeLength(header)
                if (len < 0 || len > MAX_FRAME_SIZE) {
                    log_transport.debug("TCP 收到非法帧长度 ${len}，关闭连接")
                    break
                }
                let payload = Array<UInt8>(len, repeat: 0)
                if (len > 0 && !reader.readFully(payload)) {
                    break
                }
                framesReceived.fetchAdd(1)
                let handler = synchronized(mutex) { dataHandler }
                if (let Some(h) <- handler) {
                    try {
                        h(payload)
                    } catch (e: Exception) {
                        log_transport.error("TCP 数据处理回调失败: ${e.message}")
                    }
                }
            }
        } catch (e: Exception) {
            // 对端断开或传输已停止
            ()
        }
        synchronized(mutex) {
            inbound.removeIf({ c => refEq(c, client) })
            inboundPeers.removeIf({ p => refEq(p[0], client) })
        }
        client.close()
    }

    /** 入站握手：限时读第一帧交给 accept，通过则回应答帧并登记对端标识 */
    private func acceptHandshake(client: Channel, reader: FrameReader, h: TransportHandshake): Bool {
        try {
            client.setReadTimeout(HANDSHAKE_READ_TIMEOUT)
            let accepted = match (readFrameLimited(reader, MAX_HANDSHAKE_FRAME)) {
                case Some(first) => h.accept(first)
                case None => None<(Array<UInt8>, String)>
            }
            client.setReadTimeout(None)
            match (accepted) {
                case Some((reply, peerKey)) =>
                    client.write(encodeFrame(reply))
                    synchronized(mutex) {
                        inboundPeers.add((client, peerKey))
                    }
                    true
                case None => false
            }
        } catch (e: Exception) {
            log_transport.debug("入站握手失败: ${e.message}")
            false
        }
    }
}

/** 编码帧：4 字节大端长度 + 负载 */
public func encodeFrame(data: Array<UInt8>): Array<UInt8> {
    let n = data.size
    let frame = Array<UInt8>(n + 4, repeat: 0)
    frame[0] = UInt8((n >> 24) & 0xFF)
    frame[1] = UInt8((n >> 16) & 0xFF)
    frame[2] = UInt8((n >> 8) & 0xFF)
    frame[3] = UInt8(n & 0xFF)
    data.copyTo(frame, 0, 4, n)
    frame
}

/** 解码 4 字节大端长度 */
public func decodeLength(header: Array<UInt8>): Int64 {
    (Int64(header[0]) << 24) | (Int64(header[1]) << 16) | (Int64(header[2]) << 8) | Int64(header[3])
}

func closeQuietly(action: () -> Unit): Unit {
    try {
        action()
    } catch (_: Exception) {
        ()
    }
}

/** UDP 单个数据报上限 */
public let MAX_DATAGRAM_SIZE: Int64 = 65507

/**
 * UDP 传输：每条消息一个数据报，不保证送达与顺序
 */
public class UdpTransport <: NetworkTransport {
    private let requestedAddress: NetworkAddress
    private var boundPort: Int32
    private let isRunning: AtomicBool
    private var dataHandler: Option<(Array<UInt8>) -> Unit>
    private let mutex: Mutex
    private var socket: Option<UdpSocket>

    public init(localAddress: NetworkAddress) {
        this.requestedAddress = localAddress
        this.boundPort = localAddress.port
        this.isRunning = AtomicBool(false)
        this.dataHandler = None
        this.mutex = Mutex()
        this.socket = None
    }

    public func start(): Unit {
        if (!isRunning.compareAndSwap(false, true)) {
            return
        }
        let s = UdpSocket(bindAt: UInt16(requestedAddress.port))
        s.bind()
        if (let Some(addr) <- (s.localAddress as IPSocketAddress)) {
            boundPort = Int32(addr.port)
        }
        synchronized(mutex) {
            socket = Some(s)
        }
        spawn {
            receiveLoop(s)
        }
    }

    public func stop(): Unit {
        if (!isRunning.compareAndSwap(true, false)) {
            return
        }
        let s = synchronized(mutex) {
            let cur = socket
            socket = None
            cur
        }
        if (let Some(sock) <- s) {
            closeQuietly({ => sock.close() })
        }
    }

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        if (data.size > MAX_DATAGRAM_SIZE) {
            throw IllegalArgumentException("UDP 消息过大: ${data.size} 字节，上限 ${MAX_DATAGRAM_SIZE}")
        }
        let s = synchronized(mutex) { socket }
        match (s) {
            case Some(sock) => sock.sendTo(IPSocketAddress(address.host, UInt16(address.port)), data)
            case None => throw IllegalStateException("UDP 传输未启动")
        }
    }

    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit {
        synchronized(mutex) {
            dataHandler = Some(handler)
        }
    }

    public func getLocalAddress(): NetworkAddress {
        NetworkAddress(requestedAddress.host, boundPort)
    }

    public func isActive(): Bool {
        isRunning.load()
    }

    private func receiveLoop(s: UdpSocket): Unit {
        let buffer = Array<UInt8>(MAX_DATAGRAM_SIZE, repeat: 0)
        while (isRunning.load()) {
            try {
                let (_, n) = s.receiveFrom(buffer)
                let payload = buffer[0..n].clone()
                let handler = synchronized(mutex) { dataHandler }
                if (let Some(h) <- handler) {
                    try {
                        h(payload)
                    } catch (e: Exception) {
                        log_transport.error("UDP 数据处理回调失败: ${e.message}")
                    }
                }
            } catch (e: Exception) {
                if (isRunning.load()) {
                    log_transport.error("UDP 接收失败: ${e.message}")
                }
            }
        }
    }
}

/**
 * 传输工厂
 */
public class TransportFactory {
    public static func createTcpTransport(address: NetworkAddress): NetworkTransport {
        TcpTransport(address)
    }

    public static func createUdpTransport(address: NetworkAddress): NetworkTransport {
        UdpTransport(address)
    }

    public static func createTransport(protocol: TransportProtocol, address: NetworkAddress): NetworkTransport {
        match (protocol) {
            case TransportProtocol.TCP => createTcpTransport(address)
            case TransportProtocol.UDP => createUdpTransport(address)
        }
    }
}
