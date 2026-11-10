/*
 * CActor Foundation.Network - 黑洞注入传输（E62 故障注入的基础设施）
 *
 * 放在 foundation 而不是 testkit：进程级故障注入需要示例进程与运维端点也能构造它，
 * 而 foundation 是零依赖层，任何上层都能用。`BlackholeTransport` 包装任意 `NetworkTransport`，
 * 对指定目标地址丢掉**全部**出站帧（普通通道与优先通道都丢，等价于拔掉这条网线），可随时恢复。
 *
 * 只影响出站方向：入站连接受对端的黑洞控制，这与真实的分区一致——
 * 两侧都黑洞对方才是完全隔离；单向黑洞用来构造非对称故障。
 */
package cactor_foundation.network

import std.sync.*
import std.collection.*

/**
 * 黑洞注入：对选定目标地址丢弃出站帧。
 *
 * 只影响**出站**方向：入站连接受对端的黑洞控制，这与真实的分区一致——
 * 两侧都黑洞对方才是完全隔离；单向黑洞用来构造非对称故障。
 */
public class BlackholeTransport <: NetworkTransport {
    private let inner: NetworkTransport
    private let lock = Mutex()
    private let blocked = HashSet<String>()
    /** 被丢弃的帧数（含优先通道），供断言"确实丢了东西" */
    public let dropped = AtomicInt64(0)

    public init(inner: NetworkTransport) {
        this.inner = inner
    }

    /** 黑洞掉发往该地址的全部后续帧 */
    public func blackhole(address: String): Unit {
        synchronized(lock) {
            blocked.add(address)
        }
    }

    /** 黑洞掉发往该地址的全部后续帧 */
    public func blackhole(address: NetworkAddress): Unit {
        blackhole(address.toString())
    }

    /** 恢复发往该地址的帧 */
    public func heal(address: String): Unit {
        synchronized(lock) {
            blocked.remove(address)
        }
    }

    /** 恢复发往该地址的帧 */
    public func heal(address: NetworkAddress): Unit {
        heal(address.toString())
    }

    /** 恢复全部地址 */
    public func healAll(): Unit {
        synchronized(lock) {
            blocked.clear()
        }
    }

    public func isBlackholed(address: String): Bool {
        synchronized(lock) {
            blocked.contains(address)
        }
    }

    public func droppedCount(): Int64 {
        dropped.load()
    }

    /** 当前被黑洞的地址快照（排序，供运维端点与断言） */
    public func blackholedAddresses(): Array<String> {
        synchronized(lock) {
            let out = ArrayList<String>(blocked)
            out.sort()
            out.toArray()
        }
    }

    /** 黑洞的地址个数 */
    public func blackholedCount(): Int64 {
        synchronized(lock) {
            blocked.size
        }
    }

    private func shouldDrop(address: NetworkAddress): Bool {
        synchronized(lock) {
            blocked.contains(address.toString())
        }
    }

    public func start(): Unit {
        inner.start()
    }

    public func stop(): Unit {
        inner.stop()
    }

    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit {
        inner.setDataHandler(handler)
    }

    public func setHandshake(handshake: TransportHandshake): Unit {
        inner.setHandshake(handshake)
    }

    public func setSecurity(security: TransportSecurity): Unit {
        inner.setSecurity(security)
    }

    public func closeInboundFrom(peerKey: String): Int64 {
        inner.closeInboundFrom(peerKey)
    }

    public func resetOutbound(address: NetworkAddress): Unit {
        inner.resetOutbound(address)
    }

    public func getLocalAddress(): NetworkAddress {
        inner.getLocalAddress()
    }

    public func isActive(): Bool {
        inner.isActive()
    }

    /** 观测计数必须穿透装饰器：内层是 TcpTransport 时给出真实的加密/拒绝连接数 */
    public func securedConnectionCount(): Int64 {
        inner.securedConnectionCount()
    }

    public func rejectedConnectionCount(): Int64 {
        inner.rejectedConnectionCount()
    }

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        if (shouldDrop(address)) {
            dropped.fetchAdd(1)
            return
        }
        inner.send(address, data)
    }

    public func sendPriority(address: NetworkAddress, data: Array<UInt8>): Unit {
        if (shouldDrop(address)) {
            dropped.fetchAdd(1)
            return
        }
        inner.sendPriority(address, data)
    }
}
