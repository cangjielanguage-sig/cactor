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
        @ExpectThrows[IllegalArgumentException](TcpTransportLimits(inboundFrameReadTimeout: Duration.Zero))
        @ExpectThrows[IllegalArgumentException](TcpTransportLimits(inboundFrameReadTimeout: Duration.second * -1))
        @Expect(TcpTransportLimits().inboundFrameReadTimeout, Duration.second * 30)
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
        // 普通预算 60、控制预算 80，两者互不占用（控制 30 + 30 后再来 30 即超 80）
        let limits = TcpTransportLimits(maxPendingBytes: 60, maxControlPendingBytes: 80)
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

    /** 裸 TCP 连上服务端，发一个声称 len 字节的帧头和少量负载，返回连接（帧未写完） */
    func openPartialFrame(server: TcpTransport, len: Int64, sent: Int64): TcpSocket {
        let raw = TcpSocket("127.0.0.1", UInt16(server.getLocalAddress().port))
        raw.connect()
        let header: Array<UInt8> = [UInt8((len >> 24) & 0xFF), UInt8((len >> 16) & 0xFF),
            UInt8((len >> 8) & 0xFF), UInt8(len & 0xFF)]
        raw.write(header)
        raw.write(Array<UInt8>(sent, repeat: 1))
        raw
    }

    /** 正常对端在 got 计数上再收到一帧 */
    func legitFrameGetsThrough(server: TcpTransport, got: AtomicInt64): Bool {
        let before = got.load()
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0))
        client.start()
        try {
            client.send(server.getLocalAddress(), bytesOf("hello"))
            waitUntil(Duration.second * 5, { => got.load() > before })
        } finally {
            client.stop()
        }
    }

    @TestCase
    func inboundBudgetIsReleasedWhenPeerResetsMidFrame(): Unit {
        // 预算恰好容纳一个 1 MB 帧：只要泄漏一次，之后所有入站帧都会被拒（远程 DoS）
        let frame: Int64 = 1024 * 1024
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0), limits: TcpTransportLimits(
            maxInboundPendingBytes: frame, idleRetireAfter: Duration.hour))
        let got = AtomicInt64(0)
        server.setDataHandler({ _ => got.fetchAdd(1) |> ignoreInt })
        server.start()
        try {
            let raw = openPartialFrame(server, frame, 100)
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundPendingBytes() == frame }), true)
            // SO_LINGER = 0：close 发 RST，服务端读负载时抛异常而不是读到 EOF
            raw.linger = Duration.Zero
            raw.close()
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundPendingBytes() == 0 }), true)
            @Expect(legitFrameGetsThrough(server, got), true)
            @Expect(server.getInboundByteRejections(), 0)
        } finally {
            server.stop()
        }
    }

    @TestCase
    func inboundBudgetIsReleasedWhenTransportStopsMidFrame(): Unit {
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0), limits: TcpTransportLimits(
            idleRetireAfter: Duration.hour))
        server.start()
        let raw = openPartialFrame(server, 4096, 10)
        try {
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundPendingBytes() == 4096 }), true)
            // 停机关闭入站连接：读协程从阻塞的 read 中以异常退出，预算也必须归还
            server.stop()
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundPendingBytes() == 0 }), true)
        } finally {
            closeQuietly({ => raw.close() })
            server.stop()
        }
    }

    @TestCase
    func tricklingPeerIsClosedAtFrameDeadlineAndBudgetReturns(): Unit {
        // 预算只够一个 4 KB 帧：慢速对端若能无限期占着，其他对端的帧全部被拒
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0), limits: TcpTransportLimits(
            maxInboundPendingBytes: 4096, inboundFrameReadTimeout: Duration.millisecond * 300,
            idleRetireAfter: Duration.hour))
        let got = AtomicInt64(0)
        server.setDataHandler({ _ => got.fetchAdd(1) |> ignoreInt })
        server.start()
        let raw = openPartialFrame(server, 4096, 10)
        let trickling = AtomicBool(true)
        // 每 50 ms 挤一个字节：单次 read 超时永远等得到数据，只有整帧总时限能截断它
        spawn {
            let deadline = MonoTime.now() + Duration.second * 10
            try {
                while (MonoTime.now() < deadline) {
                    raw.write([UInt8(1)])
                    sleep(Duration.millisecond * 50)
                }
            } catch (_: Exception) {
                ()
            }
            trickling.store(false)
        }
        try {
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundPendingBytes() == 4096 }), true)
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundFrameTimeouts() == 1 }), true)
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundPendingBytes() == 0 }), true)
            // 被关闭的是慢连接本身：对端继续写会失败，正常对端照常收发
            @Expect(waitUntil(Duration.second * 8, { => !trickling.load() }), true)
            @Expect(legitFrameGetsThrough(server, got), true)
        } finally {
            closeQuietly({ => raw.close() })
            server.stop()
        }
    }

    @TestCase
    func frameDeadlineDoesNotCutHealthyLargeFrames(): Unit {
        // 时限针对单帧：连续的大帧各自计时，正常速度的对端不受影响
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0), limits: TcpTransportLimits(
            inboundFrameReadTimeout: Duration.second * 5, idleRetireAfter: Duration.hour))
        let got = AtomicInt64(0)
        server.setDataHandler({ d => if (d.size == 1024 * 1024) { got.fetchAdd(1) |> ignoreInt } })
        server.start()
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0))
        client.start()
        try {
            for (_ in 0..8) {
                client.send(server.getLocalAddress(), Array<UInt8>(1024 * 1024, repeat: 3))
            }
            @Expect(waitUntil(Duration.second * 20, { => got.load() == 8 }), true)
            @Expect(server.getInboundFrameTimeouts(), 0)
            @Expect(waitUntil(Duration.second * 5, { => server.getInboundPendingBytes() == 0 }), true)
        } finally {
            client.stop()
            server.stop()
        }
    }
}
