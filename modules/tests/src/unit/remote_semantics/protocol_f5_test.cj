/*
 * CActor - B3 压缩/大消息 + F5 版本化 socket 协议的验收用例
 *
 * 覆盖：
 *   - handshakeNegotiatesMaxFrameBytes：握手协商单帧上限，大消息按协商值分片，不触发 FrameTooLarge；
 *   - goAwayCarriesReason：对端发超大帧 → 回 GoAway(FrameTooLarge) 并优雅关闭（不是等超时）；
 *   - backpressureErrorCarriesCapacity：OfferResult.Backpressured 携带容量；
 *   - leaseExpiredIsDistinctFromFenced：租约过期与 fencing 拒绝可区分（类型、原因、指标）。
 * 等待一律用 awaitCondition，不用固定 sleep。
 */
package cactor_tests.unit.remote_semantics

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import std.net.{TcpSocket}
import cactor_core.message.*
import cactor_foundation.network.{NetworkAddress, TransportProtocol, encodeFrame}
import cactor_foundation.coordination.*
import cactor_persistence.{MemoryFencingTokenStore, StaleFencingTokenException}
import cactor_patterns.backpressure.{OfferResult}
import cactor_remote.*
import cactor_testkit.*

func f5Node(settings: WireProtocolSettings): SimpleRemoteManager {
    let m = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    m.configureWire(settings)
    m.start()
    m
}

/** 高熵 ASCII 文本：避免被 deflate 压到阈值以下，从而真正走到分片路径 */
func incompressibleText(bytes: Int64): String {
    let out = ArrayList<UInt8>()
    var x: UInt64 = 0x9E3779B97F4A7C15
    for (_ in 0..bytes) {
        x = x * 6364136223846793005 + 1442695040888963407
        out.add(UInt8(33 + Int64((x >> 33) % 90)))
    }
    String.fromUtf8(out.toArray())
}

func f5ReadFrame(s: TcpSocket): Array<UInt8> {
    let header = Array<UInt8>(4, repeat: 0)
    var off = 0
    while (off < header.size) {
        let n = s.read(header[off..])
        if (n <= 0) {
            throw Exception("连接已关闭")
        }
        off += n
    }
    let len = (Int64(header[0]) << 24) | (Int64(header[1]) << 16) | (Int64(header[2]) << 8) | Int64(header[3])
    let body = Array<UInt8>(len, repeat: 0)
    off = 0
    while (off < body.size) {
        let n = s.read(body[off..])
        if (n <= 0) {
            throw Exception("连接已关闭")
        }
        off += n
    }
    body
}

@Test
public class WireProtocolF5Test {
    /** 握手协商单帧上限；大消息按协商值分片送达，不触发 FrameTooLarge */
    @TestCase
    public func handshakeNegotiatesMaxFrameBytes(): Unit {
        let a = f5Node(WireProtocolSettings.defaults())
        let b = f5Node(WireProtocolSettings.defaults().withMaxFrameBytes(32 * 1024))
        try {
            let received = EventRecorder()
            b.registerHandler("sink", { m: Message =>
                match (m as StringMessage) {
                    case Some(s) => received.record("${s.getContent().size}")
                    case None => ()
                }
            })
            let ref = a.remoteActorOf(b.getLocalAddress(), "sink")
            // 先发一条小消息完成握手，后续编码才能用到协商出的上限与能力
            ref.tell(StringMessage("warmup"))
            @Expect(received.await("6", T5S), true)
            @Expect(awaitCondition(T5S, { => a.peerWireProfile(b.getLocalAddress()).isSome() }), true)
            @Expect(a.peerMaxFrameBytes(b.getLocalAddress()), 32 * 1024)
            @Expect(a.peerTypeIdCount(b.getLocalAddress()) > 0, true)

            // 100 KiB 高熵文本：压缩后仍超过 32 KiB，只能走分片
            let big = incompressibleText(100 * 1024)
            ref.tell(StringMessage(big))
            @Expect(received.await("${big.size}", T5S), true)
            @Expect(awaitCondition(T5S, { => b.reassembledFrameCount() >= 1 }), true)
            // 没有触发超限：分片每片都 ≤ 协商上限
            @Expect(a.goAwaySentCount(), 0)
            @Expect(b.oversizeFrameCount(), 0)
            @Expect(b.wireIncompatibleReceived(), 0)
        } finally {
            a.stop()
            b.stop()
        }
    }

    /** 对端发超大帧：收到带原因的 GoAway，连接被优雅关闭（读端立即 EOF，而不是等超时） */
    @TestCase
    public func goAwayCarriesReason(): Unit {
        let b = f5Node(WireProtocolSettings.defaults().withMaxFrameBytes(4 * 1024))
        let raw = TcpSocket("127.0.0.1", UInt16(b.getLocalAddress().port))
        try {
            raw.connect()
            raw.readTimeout = T5S
            // 旧格式握手（不宣告能力），随后发一个 8 KiB 的超限帧
            raw.write(encodeFrame("HELLO|1|127.0.0.1:7301|99|0|${b.getLocalAddress().toString()}|-".toArray()))
            let ack = String.fromUtf8(f5ReadFrame(raw)).split("|")
            @Expect(ack[0], "HELLO-ACK")
            raw.write(encodeFrame(Array<UInt8>(8 * 1024, repeat: 65)))
            let goAway = String.fromUtf8(f5ReadFrame(raw))
            @Expect(goAway.startsWith("GOAWAY|FrameTooLarge"), true)
            @Expect(goAway.contains("limit=4096"), true)
            // 优雅关闭：随后读立即返回 EOF/异常，不需要等 readTimeout
            var closed = false
            let started = MonoTime.now()
            try {
                let buf = Array<UInt8>(1, repeat: 0)
                if (raw.read(buf) <= 0) {
                    closed = true
                }
            } catch (_: Exception) {
                closed = true
            }
            @Expect(closed, true)
            @Expect(MonoTime.now() - started < T5S, true)
            @Expect(awaitCondition(T5S, { => b.goAwaySentCount() >= 1 }), true)
        } finally {
            try { raw.close() } catch (_: Exception) {}
            b.stop()
        }
    }

    /** OfferResult.Backpressured 携带容量（F5：背压错误不只是一个布尔） */
    @TestCase
    public func backpressureErrorCarriesCapacity(): Unit {
        let r = OfferResult.Backpressured(3, 8)
        @Expect(r.isBackpressured(), true)
        @Expect(r.inFlight(), 3)
        @Expect(r.limit(), 8)
        @Expect(r.capacity(), 8)
        @Expect(r.toString(), "Backpressured(3/8)")
    }

    /** 租约过期（LeaseLostException.Expired）与存储侧 fencing 拒绝是两类可区分的事件 */
    @TestCase
    public func leaseExpiredIsDistinctFromFenced(): Unit {
        let now = AtomicInt64(0)
        let store = InMemoryLeaseStore({ => now.load() })
        let settings = LeaseSettings("res", "a", Duration.millisecond * 1000, Duration.millisecond * 50,
            Duration.millisecond * 100)
        let a = InMemoryLease(store, settings)
        let b = InMemoryLease(store, settings.withOwnerName("b"))
        let lostReason = AtomicInt64(-1)
        @Expect(a.acquire({ e =>
            match (e) {
                case Some(x: LeaseLostException) =>
                    match (x.reason) {
                        case LeaseLostReason.Expired => lostReason.store(1)
                        case LeaseLostReason.Revoked => lostReason.store(2)
                        case LeaseLostReason.Backend => lostReason.store(3)
                    }
                case _ => ()
            }
        }), true)
        // 跨过 ttl：本地先失效，竞争者接手；原持有者的心跳以 Expired 报告失去
        now.fetchAdd(1500)
        @Expect(a.checkLease(), false)
        @Expect(b.acquire({ _ => () }), true)
        @Expect(awaitCondition(T5S, { => lostReason.load() == 1 }), true)
        @Expect(a.expiredCount(), 1)
        @Expect(a.revokedCount(), 0)
        b.release()

        // fencing：旧 token 的写入是 StaleFencingTokenException（与租约过期不同类、不同消息）
        let fence = MemoryFencingTokenStore()
        fence.fenced("res", 2, { => () })
        var fenced = false
        try {
            fence.fenced("res", 1, { => () })
        } catch (e: StaleFencingTokenException) {
            fenced = true
            @Expect(e.token, 1)
            @Expect(e.highestSeen, 2)
            @Expect(e.message.contains("fencing token"), true)
        }
        @Expect(fenced, true)
    }
}
