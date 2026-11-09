/*
 * CActor TestKit - 网络故障注入（E62 的多节点 harness 部分）
 *
 * `BlackholeTransport` 包装任意 `NetworkTransport`，对指定目标地址丢掉**全部**出站帧
 * （普通通道与优先通道都丢，等价于拔掉这条网线），可以随时恢复。用它验证：
 *   - 故障检测把节点判为不可达、分片只缓冲它拥有的分片；
 *   - 恢复连通后成员重新可达、缓冲的消息送达；
 * 而不需要真的操作防火墙。进程级黑洞（多进程测试）见 `tools/cluster_demo.sh` 的说明。
 */
package cactor_testkit

import std.sync.*
import std.collection.*
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TransportHandshake, TransportSecurity}

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
