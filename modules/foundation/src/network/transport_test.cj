/*
 * 网络传输测试：基于本机回环地址的真实 TCP / UDP 收发
 */
package cactor_foundation.network

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        if (cond()) {
            return true
        }
        sleep(Duration.millisecond * 2)
    }
    cond()
}

func bytesOf(s: String): Array<UInt8> {
    s.toArray()
}

@Test
public class TransportTest {
    @TestCase
    func addressParsing(): Unit {
        let a = NetworkAddress.parse("10.0.0.5:2552")
        @Expect(a.host, "10.0.0.5")
        @Expect(a.port, 2552)
        @ExpectThrows[IllegalArgumentException](NetworkAddress.parse("no-port"))
        @ExpectThrows[IllegalArgumentException](NetworkAddress.parse("host:99999"))
    }

    @TestCase
    func frameEncoding(): Unit {
        let frame = encodeFrame(bytesOf("abc"))
        @Expect(frame.size, 7)
        @Expect(decodeLength(frame[0..4]), 3)
        @Expect(String.fromUtf8(frame[4..]), "abc")
    }

    @TestCase
    func tcpRoundTripAndLargeFrames(): Unit {
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0))
        let received = ArrayList<Array<UInt8>>()
        let lock = Mutex()
        server.setDataHandler({ data => synchronized(lock) { received.add(data) } })
        server.start()
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0))
        client.start()
        let target = server.getLocalAddress()
        @Expect(target.port > 0, true)

        client.send(target, bytesOf("hello"))
        client.send(target, Array<UInt8>())  // 空帧
        let big = Array<UInt8>(1024 * 1024, { i => UInt8(i % 251) })
        client.send(target, big)
        @Expect(waitUntil(Duration.second * 5, { => synchronized(lock) { received.size == 3 } }), true)
        let got = synchronized(lock) { received.toArray() }
        @Expect(String.fromUtf8(got[0]), "hello")
        @Expect(got[1].size, 0)
        @Expect(got[2].size, big.size)
        var same = true
        for (i in 0..big.size) {
            if (got[2][i] != big[i]) {
                same = false
                break
            }
        }
        @Expect(same, true)
        @Expect(server.getFramesReceived(), 3)
        client.stop()
        server.stop()
        @Expect(server.isActive(), false)
    }

    @TestCase
    func tcpConcurrentSendersPreserveFrames(): Unit {
        let server = TcpTransport(NetworkAddress("127.0.0.1", 0))
        let count = AtomicInt64(0)
        let bytes = AtomicInt64(0)
        server.setDataHandler({ data =>
            count.fetchAdd(1)
            bytes.fetchAdd(data.size)
        })
        server.start()
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0))
        client.start()
        let target = server.getLocalAddress()
        let futures = ArrayList<Future<Unit>>()
        for (t in 0..8) {
            futures.add(spawn {
                for (i in 0..250) {
                    client.send(target, Array<UInt8>(100, repeat: UInt8(t)))
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        @Expect(waitUntil(Duration.second * 10, { => count.load() == 2000 }), true)
        @Expect(bytes.load(), 200_000)
        client.stop()
        server.stop()
    }

    @TestCase
    func tcpReconnectsAfterPeerRestart(): Unit {
        let count = AtomicInt64(0)
        let server1 = TcpTransport(NetworkAddress("127.0.0.1", 0))
        server1.setDataHandler({ _ => count.fetchAdd(1) })
        server1.start()
        let port = server1.getLocalAddress().port
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0))
        client.start()
        client.send(NetworkAddress("127.0.0.1", port), bytesOf("one"))
        @Expect(waitUntil(Duration.second * 5, { => count.load() == 1 }), true)
        server1.stop()
        sleep(Duration.millisecond * 50)

        // 同一端口重新启动对端，发送方应自动丢弃旧连接并重连
        let server2 = TcpTransport(NetworkAddress("127.0.0.1", port))
        server2.setDataHandler({ _ => count.fetchAdd(1) })
        server2.start()
        var delivered = false
        for (_ in 0..5) {
            try {
                client.send(NetworkAddress("127.0.0.1", port), bytesOf("two"))
                if (waitUntil(Duration.second, { => count.load() >= 2 })) {
                    delivered = true
                    break
                }
            } catch (_: Exception) {
                sleep(Duration.millisecond * 50)
            }
        }
        @Expect(delivered, true)
        client.stop()
        server2.stop()
    }

    @TestCase
    func tcpSendToClosedPortFails(): Unit {
        let probe = TcpTransport(NetworkAddress("127.0.0.1", 0))
        probe.start()
        let deadPort = probe.getLocalAddress().port
        probe.stop()
        let client = TcpTransport(NetworkAddress("127.0.0.1", 0), Duration.millisecond * 500)
        client.start()
        var failed = false
        try {
            client.send(NetworkAddress("127.0.0.1", deadPort), bytesOf("x"))
        } catch (_: Exception) {
            failed = true
        }
        @Expect(failed, true)
        @Expect(client.getSendFailures(), 1)
        client.stop()
        // 未启动时发送直接失败
        @ExpectThrows[IllegalStateException](client.send(NetworkAddress("127.0.0.1", deadPort), bytesOf("x")))
    }

    @TestCase
    func udpDatagrams(): Unit {
        let server = UdpTransport(NetworkAddress("127.0.0.1", 0))
        let received = AtomicInt64(0)
        let lastSize = AtomicInt64(0)
        server.setDataHandler({ data =>
            lastSize.store(data.size)
            received.fetchAdd(1)
        })
        server.start()
        let client = UdpTransport(NetworkAddress("127.0.0.1", 0))
        client.start()
        // 回环地址上丢包概率极低，逐个发送并等待
        for (i in 0..20) {
            client.send(server.getLocalAddress(), Array<UInt8>(i + 1, repeat: 1))
            waitUntil(Duration.second, { => received.load() == i + 1 })
        }
        @Expect(received.load(), 20)
        @Expect(lastSize.load(), 20)
        @ExpectThrows[IllegalArgumentException](client.send(server.getLocalAddress(), Array<UInt8>(70000, repeat: 0)))
        client.stop()
        server.stop()
    }
}
