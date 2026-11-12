/* CActor Foundation.Network - 全节点字节预算回归。
 * 出站：所有出站关联合计的待发字节（普通与控制分开预算）。
 * 入站：所有入站连接合计的在途解码字节。
 * 预算满额时丢弃并计数，写出后预算归还。
 */
package cactor_foundation.network

import std.collection.*
import std.net.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*

/** 仓颉的 enum 不直接可比较，这里转成布尔便于断言 */
func enqueuedAccepted(e: Enqueued): Bool {
    match (e) {
        case Enqueued.Accepted => true
        case _ => false
    }
}

@Test
public class TransportByteBudgetTest {
    @TestCase
    func limitsRejectNonPositiveByteBudget(): Unit {
        @ExpectThrows[IllegalArgumentException](TcpTransportLimits(maxPendingBytes: 0))
        @ExpectThrows[IllegalArgumentException](TcpTransportLimits(maxControlPendingBytes: -1))
        @ExpectThrows[IllegalArgumentException](TcpTransportLimits(maxInboundPendingBytes: 0))
    }

    @TestCase
    func outboundByteBudgetIsSharedByAllDestinations(): Unit {
        let counters = TransportCounters()
        let limits = TcpTransportLimits(maxPendingBytes: 100, maxControlPendingBytes: 100)
        let a = OutboundConnection(NetworkAddress("127.0.0.1", 1), Duration.second, Duration.hour,
            counters, limits, { _ => }, { => None }, { => None })
        let b = OutboundConnection(NetworkAddress("127.0.0.1", 2), Duration.second, Duration.hour,
            counters, limits, { _ => }, { => None }, { => None })
        let frame = Array<UInt8>(30, repeat: 0)
        @Expect(enqueuedAccepted(a.enqueue(frame, false)), true)
        @Expect(counters.pendingBytes.load(), 30)
        // 第二个目标共用同一份节点级预算：不是各自 100
        @Expect(enqueuedAccepted(b.enqueue(frame, false)), true)
        @Expect(counters.pendingBytes.load(), 60)
        @Expect(enqueuedAccepted(b.enqueue(frame, false)), true)
        @Expect(counters.pendingBytes.load(), 90)
        // 90 + 30 > 100：被拒
        @Expect(enqueuedAccepted(b.enqueue(frame, false)), false)
        @Expect(counters.pendingBytes.load(), 90)
        @Expect(counters.byteRejections.load(), 1)
    }

    @TestCase
    func controlChannelHasItsOwnByteBudget(): Unit {
        let counters = TransportCounters()
        // 普通预算 60：控制预算 100，两者互不占用
        let limits = TcpTransportLimits(maxPendingBytes: 60, maxControlPendingBytes: 100)
        let conn = OutboundConnection(NetworkAddress("127.0.0.1", 1), Duration.second, Duration.hour,
            counters, limits, { _ => }, { => None }, { => None })
        let frame = Array<UInt8>(30, repeat: 0)
        @Expect(enqueuedAccepted(conn.enqueue(frame, false)), true)
        @Expect(enqueuedAccepted(conn.enqueue(frame, false)), true)
        @Expect(counters.pendingBytes.load(), 60)
        // 普通通道已满，控制帧仍然可以入队
        @Expect(enqueuedAccepted(conn.enqueue(frame, true)), true)
        @Expect(counters.controlBytes.load(), 30)
        @Expect(counters.pendingBytes.load(), 60)
        // 控制通道自己也有上限
        @Expect(enqueuedAccepted(conn.enqueue(frame, true)), true)
        @Expect(enqueuedAccepted(conn.enqueue(frame, true)), false)
        @Expect(counters.controlBytes.load(), 60)
    }

    @TestCase
    func byteBudgetIsReleasedWhenTransportStops(): Unit {
        let counters = TransportCounters()
        let limits = TcpTransportLimits(maxPendingBytes: 1024, maxControlPendingBytes: 1024)
        let conn = OutboundConnection(NetworkAddress("127.0.0.1", 1), Duration.second, Duration.hour,
            counters, limits, { _ => }, { => None }, { => None })
        let frame = Array<UInt8>(100, repeat: 0)
        conn.enqueue(frame, false)
        conn.enqueue(frame, true)
        @Expect(counters.pendingBytes.load(), 100)
        @Expect(counters.controlBytes.load(), 100)
        // 写线程退出（异常路径）也必须归还预算，否则预算会被慢慢耗尽
        conn.startWriter()
        conn.close()
        @Expect(waitUntil(Duration.second * 5, { => counters.pendingBytes.load() == 0 }), true)
        @Expect(counters.controlBytes.load(), 0)
    }

    @TestCase
    func tcpDeliveryReturnsByteBudgetAfterWrite(): Unit {
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0))
        let got = AtomicInt64(0)
        server.setDataHandler({ _ => got.fetchAdd(1) |> ignoreInt })
        server.start()
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0))
        client.start()
        try {
            client.send(server.getLocalAddress(), bytesOf("hello"))
            @Expect(waitUntil(Duration.second * 5, { => got.load() == 1 }), true)
            // 写出后预算必须回到 0：否则长时间运行会把节点预算耗尽
            @Expect(waitUntil(Duration.second * 5, { => client.getPendingBytes() == 0 }), true)
            @Expect(client.getByteCapacityRejections(), 0)
        } finally {
            client.stop()
            server.stop()
        }
    }

    @TestCase
    func inboundByteBudgetClosesConnectionWhenExceeded(): Unit {
        // 入站预算 1 KB：超过后连接被关闭并计数
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0), limits: TcpTransportLimits(
            maxInboundPendingBytes: 1024, idleRetireAfter: Duration.hour))
        server.start()
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0))
        client.start()
        try {
            let big = Array<UInt8>(4096, repeat: 7)
            client.send(server.getLocalAddress(), big)
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundByteRejections() > 0 }), true)
            @Expect(server.getInboundPendingBytes(), 0)
        } finally {
            client.stop()
            server.stop()
        }
    }

    @TestCase
    func inboundByteBudgetReturnsAfterNormalDelivery(): Unit {
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0))
        let got = AtomicInt64(0)
        server.setDataHandler({ _ => got.fetchAdd(1) |> ignoreInt })
        server.start()
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0))
        client.start()
        try {
            client.send(server.getLocalAddress(), bytesOf("hello"))
            @Expect(waitUntil(Duration.second * 5, { => got.load() == 1 }), true)
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundPendingBytes() == 0 }), true)
            @Expect(server.getInboundByteRejections(), 0)
        } finally {
            client.stop()
            server.stop()
        }
    }
}
