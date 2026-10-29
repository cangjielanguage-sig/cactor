/*
 * Copyright (c) 2024. All rights reserved.
 * Foundation Layer - 网络传输层
 * 基于 std.net 的 TCP / UDP 字节传输
 *
 * TCP：长度前缀分帧（4 字节大端长度 + 负载），每个入站连接一个读协程，
 *      出站连接按目标地址复用，写入按连接加锁，失败时重连重试一次
 * UDP：一条消息一个数据报（最大 64KB）
 */
package cactor_foundation.network

import std.sync.*
import std.net.*
import std.time.*
import std.convert.*
import std.collection.*
import std.math.*
import cactor_foundation.logging.{Logger, Logging}

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
 * 网络传输接口
 */
public interface NetworkTransport {
    /** 开始监听本地地址 */
    func start(): Unit

    /** 停止监听并关闭所有连接 */
    func stop(): Unit

    /** 发送一条完整消息；失败时抛出异常 */
    func send(address: NetworkAddress, data: Array<UInt8>): Unit

    /** 设置收到完整消息时的回调 */
    func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit

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

/** 单个连接允许积压的最大字节数，超过后发送方阻塞等待（背压） */
public let MAX_PENDING_BYTES: Int64 = 64 * 1024 * 1024

/**
 * 出站连接：独立写线程 + 合并写（对应 Akka Artery 的出站流）
 *
 * 发送方只把帧追加到 pending 缓冲区并唤醒写线程；写线程每次把积累的全部帧一次 write 出去。
 * 发送方比网络快时自动合并成大块写，系统调用次数随负载下降；积压超过 MAX_PENDING_BYTES 时发送方阻塞。
 * 写失败后连接标记为失败，之后的 send 抛出异常，由传输层丢弃连接并重连。
 */
class OutboundConnection {
    let socket: TcpSocket
    private let lock: Mutex = Mutex()
    private let cond: Condition
    private var pending: ByteBuffer = ByteBuffer(64 * 1024)
    private var spare: ByteBuffer = ByteBuffer(64 * 1024)
    private var closed: Bool = false
    private var failure: Option<Exception> = None<Exception>
    private var writerWaiting: Bool = false
    private var senderWaiting: Bool = false
    let writes: AtomicInt64 = AtomicInt64(0)

    init(socket: TcpSocket) {
        this.socket = socket
        this.cond = synchronized(lock) { lock.condition() }
    }

    func startWriter(): Unit {
        spawn {
            writerLoop()
        }
    }

    /** 追加一帧；连接已失败或关闭时抛出异常 */
    func send(frame: Array<UInt8>): Unit {
        synchronized(lock) {
            while (pending.size >= MAX_PENDING_BYTES && !closed && failure.isNone()) {
                senderWaiting = true
                cond.wait(timeout: Duration.millisecond * 100)
            }
            if (let Some(e) <- failure) {
                throw e
            }
            if (closed) {
                throw IllegalStateException("连接已关闭")
            }
            pending.append(frame)
            if (writerWaiting) {
                cond.notifyAll()
            }
        }
    }

    /** 关闭：先等待已积压的数据写出（最多 1 秒），再关闭 socket */
    func close(): Unit {
        let deadline = MonoTime.now() + Duration.second
        synchronized(lock) {
            closed = true
            cond.notifyAll()
            while ((pending.size > 0 || spare.size > 0) && failure.isNone() && MonoTime.now() < deadline) {
                cond.wait(timeout: Duration.millisecond * 10)
            }
        }
        try {
            socket.close()
        } catch (_: Exception) {
            ()
        }
    }

    private func writerLoop(): Unit {
        while (true) {
            let batch = synchronized(lock) {
                while (pending.size == 0 && !closed) {
                    writerWaiting = true
                    cond.wait(timeout: Duration.millisecond * 100)
                    writerWaiting = false
                }
                if (pending.size == 0) {
                    // 已关闭且没有剩余数据
                    return
                }
                let b = pending
                pending = spare
                spare = b
                if (senderWaiting) {
                    senderWaiting = false
                    cond.notifyAll()
                }
                b
            }
            try {
                socket.write(batch.data[0..batch.size])
                writes.fetchAdd(1)
            } catch (e: Exception) {
                synchronized(lock) {
                    failure = Some(e)
                    closed = true
                    cond.notifyAll()
                }
                return
            } finally {
                synchronized(lock) {
                    batch.clear()
                    cond.notifyAll()
                }
            }
        }
    }
}

/** 带缓冲的帧读取：一次 read 读入多帧，减少系统调用 */
class FrameReader {
    private let socket: TcpSocket
    private let buffer: Array<UInt8> = Array<UInt8>(64 * 1024, repeat: 0)
    private var start: Int64 = 0
    private var end: Int64 = 0

    init(socket: TcpSocket) {
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
    private let inbound: ArrayList<TcpSocket>
    private let connectTimeout: Duration

    // 统计
    private let framesSent: AtomicInt64 = AtomicInt64(0)
    private let framesReceived: AtomicInt64 = AtomicInt64(0)
    private let sendFailures: AtomicInt64 = AtomicInt64(0)

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
        this.inbound = ArrayList<TcpSocket>()
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

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        if (!isRunning.load()) {
            throw IllegalStateException("TCP 传输未启动")
        }
        if (data.size > MAX_FRAME_SIZE) {
            throw IllegalArgumentException("消息过大: ${data.size} 字节，上限 ${MAX_FRAME_SIZE}")
        }
        let frame = encodeFrame(data)
        try {
            writeFrame(address, frame)
        } catch (e: Exception) {
            // 连接可能已被对端关闭：丢弃后重连重试一次
            dropConnection(address)
            try {
                writeFrame(address, frame)
            } catch (e2: Exception) {
                sendFailures.fetchAdd(1)
                dropConnection(address)
                throw e2
            }
        }
        framesSent.fetchAdd(1)
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

    public func getFramesSent(): Int64 { framesSent.load() }

    public func getFramesReceived(): Int64 { framesReceived.load() }

    public func getSendFailures(): Int64 { sendFailures.load() }

    private func writeFrame(address: NetworkAddress, frame: Array<UInt8>): Unit {
        getOrConnect(address).send(frame)
    }

    /** 实际执行的 write 系统调用次数（与 getFramesSent 对比可看出合并效果） */
    public func getSocketWrites(): Int64 {
        let conns = synchronized(mutex) { outbound.values().toArray() }
        var total: Int64 = 0
        for (c in conns) {
            total += c.writes.load()
        }
        total
    }

    private func getOrConnect(address: NetworkAddress): OutboundConnection {
        let key = address.toString()
        if (let Some(c) <- synchronized(mutex) { outbound.get(key) }) {
            return c
        }
        let socket = TcpSocket(address.host, UInt16(address.port))
        socket.connect(timeout: connectTimeout)
        socket.noDelay = true
        let conn = OutboundConnection(socket)
        let winner = synchronized(mutex) {
            match (outbound.get(key)) {
                case Some(existing) => existing
                case None =>
                    outbound.add(key, conn)
                    conn
            }
        }
        if (refEq(winner, conn)) {
            conn.startWriter()
        } else {
            // 其他线程已建立连接，关闭多余的连接
            closeQuietly({ => socket.close() })
        }
        return winner
    }

    private func dropConnection(address: NetworkAddress): Unit {
        let removed = synchronized(mutex) { outbound.remove(address.toString()) }
        if (let Some(c) <- removed) {
            closeQuietly({ => c.close() })
        }
    }

    private func acceptLoop(s: TcpServerSocket): Unit {
        while (isRunning.load()) {
            try {
                let client = s.accept()
                client.noDelay = true
                synchronized(mutex) {
                    inbound.add(client)
                }
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

    private func readLoop(client: TcpSocket): Unit {
        let header = Array<UInt8>(4, repeat: 0)
        let reader = FrameReader(client)
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
        closeQuietly({ => client.close() })
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
