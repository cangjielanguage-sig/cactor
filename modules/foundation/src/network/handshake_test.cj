/*
 * 传输层连接握手（E7′）：接受方先校验第一帧、连接方校验应答；不通过就断开，数据帧不交给上层
 */
package cactor_foundation.network

import std.sync.*
import std.time.*
import std.net.*
import std.unittest.*
import std.unittest.testmacro.*

/** 只认 "token:<secret>" 的握手；应答 "ok:<secret>" */
class TokenHandshake <: TransportHandshake {
    let secret: String
    let accepted = AtomicInt64(0)

    init(secret: String) {
        this.secret = secret
    }

    public func hello(peer: NetworkAddress): Array<UInt8> {
        "token:${secret}".toArray()
    }

    public func accept(hello: Array<UInt8>): Option<(Array<UInt8>, String)> {
        if (String.fromUtf8(hello) == "token:${secret}") {
            accepted.fetchAdd(1) |> ignoreInt
            Some(("ok:${secret}".toArray(), "peer-${secret}"))
        } else {
            None<(Array<UInt8>, String)>
        }
    }

    public func confirm(peer: NetworkAddress, reply: Array<UInt8>): Bool {
        String.fromUtf8(reply) == "ok:${secret}"
    }
}

func handshakeNode(secret: String, got: AtomicInt64): TcpTransport {
    let t = TcpTransport(NetworkAddress("127.0.0.1", 0), Duration.second)
    t.setHandshake(TokenHandshake(secret))
    t.setDataHandler({ _ => got.fetchAdd(1) |> ignoreInt })
    t.start()
    t
}

@Test
public class HandshakeTest {
    @TestCase
    func matchingHandshakeDeliversFrames(): Unit {
        let gotA = AtomicInt64(0)
        let gotB = AtomicInt64(0)
        let a = handshakeNode("s3cret", gotA)
        let b = handshakeNode("s3cret", gotB)
        for (i in 0..10) {
            a.send(b.getLocalAddress(), bytesOf("m${i}"))
        }
        @Expect(waitUntil(Duration.second * 5, { => gotB.load() == 10 }), true)
        @Expect(b.getRejectedHandshakes(), 0)
        @Expect(a.getHandshakeFailures(), 0)
        a.stop()
        b.stop()
    }

    @TestCase
    func wrongSecretIsRejectedBothWays(): Unit {
        let gotGood = AtomicInt64(0)
        let gotEvil = AtomicInt64(0)
        let good = handshakeNode("s3cret", gotGood)
        let evil = handshakeNode("evil", gotEvil)
        // evil → good：good 拒绝第一帧，数据帧不交给上层
        evil.send(good.getLocalAddress(), bytesOf("forged"))
        @Expect(waitUntil(Duration.second * 5, { => good.getRejectedHandshakes() >= 1 }), true)
        @Expect(waitUntil(Duration.second * 5, { => evil.getDroppedFrames() >= 1 }), true)
        // good → evil：evil 拒绝；即使 evil 放行，good 也会拒绝它的应答（双向校验）
        good.send(evil.getLocalAddress(), bytesOf("secret-data"))
        @Expect(waitUntil(Duration.second * 5, { => good.getHandshakeFailures() >= 1 }), true)
        @Expect(gotGood.load(), 0)
        @Expect(gotEvil.load(), 0)
        good.stop()
        evil.stop()
    }

    @TestCase
    func rawConnectionWithoutHandshakeIsClosed(): Unit {
        let got = AtomicInt64(0)
        let server = handshakeNode("s3cret", got)
        // 不握手、直接发数据帧的连接：第一帧校验不过，连接被关闭
        let raw = TcpSocket("127.0.0.1", UInt16(server.getLocalAddress().port))
        raw.connect(timeout: Duration.second * 3)
        raw.write(encodeFrame(bytesOf("DEATH|target|127.0.0.1:1")))
        raw.readTimeout = Duration.second * 5
        let buf = Array<UInt8>(16, repeat: 0)
        let n = try {
            raw.read(buf)
        } catch (_: Exception) {
            -1
        }
        @Expect(n <= 0, true)
        @Expect(server.getRejectedHandshakes(), 1)
        @Expect(got.load(), 0)
        raw.close()
        server.stop()
    }

    @TestCase
    func closeInboundFromDisconnectsThatPeer(): Unit {
        let gotA = AtomicInt64(0)
        let gotB = AtomicInt64(0)
        let a = handshakeNode("k", gotA)
        let b = handshakeNode("k", gotB)
        a.send(b.getLocalAddress(), bytesOf("one"))
        @Expect(waitUntil(Duration.second * 5, { => gotB.load() == 1 }), true)
        @Expect(b.closeInboundFrom("peer-k"), 1)
        @Expect(b.closeInboundFrom("peer-k"), 0)
        a.stop()
        b.stop()
    }
}
