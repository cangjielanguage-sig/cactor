/*
 * CActor - 远程传输 TLS / mTLS（E65）
 *
 *   - 两端都配置 TLS（本集群 CA 签发的证书）的节点双向通信，连接确实经 TLS 建立
 *   - 验收：明文连接被拒绝——不配 TLS 的节点、裸 TCP 客户端发来的消息到不了 Actor；TLS 节点也不会向明文节点降级
 *   - 证书由别的 CA 签发（主题、SAN 与本集群证书相同）的节点：两个方向都被拒绝
 *   - 线上抓包：经中间转发代理记录的字节里，TLS 下找不到消息内容与握手身份；同一代理在明文配置下能找到（对照）
 *   - 配置错误立即报出：本节点证书不是信任的 CA 签发、不支持加密的传输、启动后再配置
 */
package cactor_tests.unit.remote_semantics

import std.sync.*
import std.time.*
import std.net.{TcpSocket, TcpServerSocket, IPSocketAddress}
import std.collection.{ArrayList}
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol, UdpTransport, encodeFrame}
import cactor_remote.*
import cactor_testkit.*

/** 本集群的 TLS 配置（每次新建：拒绝计数按节点区分） */
func clusterTls(): RemoteTls {
    RemoteTls.fromPem(TEST_TLS_NODE_CERT, TEST_TLS_NODE_KEY, TEST_TLS_CA)
}

/** 冒充者：持有另一个 CA 签发的证书；它自己同时信任两个 CA，所以失败只来自对端的校验 */
func rogueTls(): RemoteTls {
    RemoteTls.fromPem(TEST_TLS_ROGUE_CERT, TEST_TLS_ROGUE_KEY, TEST_TLS_CA + TEST_TLS_ROGUE_CA)
}

func tlsNode(tls: Option<RemoteTls>): SimpleRemoteManager {
    let m = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    m.configure(match (tls) {
        case Some(t) => FAST.withTls(t)
        case None => FAST
    })
    m.start()
    m
}

func containsBytes(hay: Array<UInt8>, needle: Array<UInt8>): Bool {
    if (needle.size == 0 || hay.size < needle.size) {
        return needle.size == 0
    }
    for (i in 0..(hay.size - needle.size + 1)) {
        var j = 0
        while (j < needle.size && hay[i + j] == needle[j]) {
            j++
        }
        if (j == needle.size) {
            return true
        }
    }
    false
}

/** 透明转发代理：把连到它的连接原样转发到 target，并记录客户端 → 目标方向的全部字节（模拟能截获流量的攻击者） */
class SniffingProxy {
    private let server: TcpServerSocket
    private let target: NetworkAddress
    private let lock = Mutex()
    private let captured = ArrayList<UInt8>()
    private let sockets = ArrayList<TcpSocket>()
    private let running = AtomicBool(true)

    init(target: NetworkAddress) {
        this.target = target
        this.server = TcpServerSocket(bindAt: 0)
        server.bind()
        spawn {
            acceptLoop()
        }
    }

    func address(): NetworkAddress {
        NetworkAddress("127.0.0.1", Int32((server.localAddress as IPSocketAddress).getOrThrow().port))
    }

    func bytes(): Array<UInt8> {
        synchronized(lock) { captured.toArray() }
    }

    private func acceptLoop(): Unit {
        while (running.load()) {
            try {
                let client = server.accept()
                let upstream = TcpSocket(target.host, UInt16(target.port))
                upstream.connect()
                synchronized(lock) {
                    sockets.add(client)
                    sockets.add(upstream)
                }
                spawn {
                    pump(client, upstream, true)
                }
                spawn {
                    pump(upstream, client, false)
                }
            } catch (_: Exception) {
                ()
            }
        }
    }

    private func pump(from: TcpSocket, to: TcpSocket, record: Bool): Unit {
        let buf = Array<UInt8>(16 * 1024, repeat: 0)
        try {
            while (true) {
                let n = from.read(buf)
                if (n <= 0) {
                    break
                }
                if (record) {
                    synchronized(lock) {
                        captured.add(all: buf[0..n])
                    }
                }
                to.write(buf[0..n])
            }
        } catch (_: Exception) {
            ()
        }
        try {
            to.close()
        } catch (_: Exception) {}
    }

    func close(): Unit {
        running.store(false)
        try {
            server.close()
        } catch (_: Exception) {}
        let all = synchronized(lock) { sockets.toArray() }
        for (s in all) {
            try {
                s.close()
            } catch (_: Exception) {}
        }
    }
}

@Test
public class RemoteTlsTest {
    /** 两端都配 TLS：双向请求应答，连接确实经 TLS 建立，没有拒绝 */
    @TestCase
    public func nodesWithTlsCommunicateBothWays(): Unit {
        let kit = ActorTestKit("e65-tls-basic")
        let a = tlsNode(Some(clusterTls()))
        let b = tlsNode(Some(clusterTls()))
        a.exposeActor("echo-a", kit.actorOf({ => Echo() }, "echo-a"))
        b.exposeActor("echo-b", kit.actorOf({ => Echo() }, "echo-b"))
        let r1 = a.ask(a.remoteActorOf(b.getLocalAddress(), "echo-b"), StringMessage("ping"), T5S).get(T5S)
        @Expect((r1 as StringMessage).getOrThrow().getContent(), "echo:ping")
        let r2 = b.ask(b.remoteActorOf(a.getLocalAddress(), "echo-a"), StringMessage("pong"), T5S).get(T5S)
        @Expect((r2 as StringMessage).getOrThrow().getContent(), "echo:pong")
        // 每个节点至少一条出站、一条入站的 TLS 连接
        @Expect(a.securedConnections() >= 2, true)
        @Expect(b.securedConnections() >= 2, true)
        @Expect(a.rejectedConnections() + b.rejectedConnections(), 0)
        a.stop()
        b.stop()
        kit.shutdown()
    }

    /**
     * E65 验收：明文连接被拒绝。不配 TLS 的远程节点与裸 TCP 客户端都进不来；
     * 反方向 TLS 节点连明文节点也失败（不降级为明文）
     */
    @TestCase
    public func plaintextConnectionsAreRejected(): Unit {
        let kit = ActorTestKit("e65-plaintext")
        let secured = tlsNode(Some(clusterTls()))
        let sink = Collect()
        secured.exposeActor("sink", kit.actorOf({ => sink }, "sink"))
        // 1. 明文远程节点
        let plain = tlsNode(None<RemoteTls>)
        plain.remoteActorOf(secured.getLocalAddress(), "sink").tell(StringMessage("from-plaintext-node"))
        @Expect(awaitCondition(T5S, { => secured.rejectedConnections() >= 1 }), true)
        // 2. 裸 TCP 客户端：发一个格式正确的明文帧，连接被断开（读到 EOF 或异常），没有任何应答
        let raw = TcpSocket("127.0.0.1", UInt16(secured.getLocalAddress().port))
        raw.connect()
        raw.readTimeout = Duration.second * 8
        raw.write(encodeFrame("HELLO|1|127.0.0.1:1|1|0|x|-".toArray()))
        let buf = Array<UInt8>(64, repeat: 0)
        let n = try {
            raw.read(buf)
        } catch (_: Exception) {
            -1
        }
        @Expect(n <= 0, true)
        try {
            raw.close()
        } catch (_: Exception) {}
        @Expect(awaitCondition(T5S, { => secured.rejectedConnections() >= 2 }), true)
        // 3. TLS 节点 → 明文节点：TLS 握手失败，不降级
        let plainSink = Collect()
        plain.exposeActor("sink", kit.actorOf({ => plainSink }, "plain-sink"))
        secured.remoteActorOf(plain.getLocalAddress(), "sink").tell(StringMessage("to-plaintext-node"))
        @Expect(awaitCondition(T5S, { => plain.rejectedConnections() >= 1 }), true)
        // 负向断言：再等 300 ms 确认两边都没有消息漏进来
        sleep(Duration.millisecond * 300)
        @Expect(sink.snapshot().size, 0)
        @Expect(plainSink.snapshot().size, 0)
        @Expect(secured.securedConnections(), 0)
        plain.stop()
        secured.stop()
        kit.shutdown()
    }

    /** 另一个 CA 签发的证书（主题、SAN 都与本集群证书相同）：作为客户端与作为服务端都被拒绝 */
    @TestCase
    public func certificatesFromAnotherCaAreRejected(): Unit {
        let kit = ActorTestKit("e65-rogue")
        let goodTls = clusterTls()
        let good = tlsNode(Some(goodTls))
        let rogue = tlsNode(Some(rogueTls()))
        let goodSink = Collect()
        good.exposeActor("sink", kit.actorOf({ => goodSink }, "good-sink"))
        let rogueSink = Collect()
        rogue.exposeActor("sink", kit.actorOf({ => rogueSink }, "rogue-sink"))
        // 冒充者作为客户端：本集群节点（服务端）校验客户端证书失败
        rogue.remoteActorOf(good.getLocalAddress(), "sink").tell(StringMessage("from-rogue"))
        @Expect(awaitCondition(T5S, { => goodTls.rejectedPeerCertificates() >= 1 }), true)
        // 本集群节点作为客户端连冒充者：校验服务端证书失败
        let before = goodTls.rejectedPeerCertificates()
        good.remoteActorOf(rogue.getLocalAddress(), "sink").tell(StringMessage("to-rogue"))
        @Expect(awaitCondition(T5S, { => goodTls.rejectedPeerCertificates() > before }), true)
        sleep(Duration.millisecond * 300)
        @Expect(goodSink.snapshot().size, 0)
        @Expect(rogueSink.snapshot().size, 0)
        @Expect(good.securedConnections(), 0)
        rogue.stop()
        good.stop()
        kit.shutdown()
    }

    /**
     * 线上抓包：消息经透明代理转发。明文配置下代理记录的字节里能找到消息内容与握手里的地址（对照，证明抓包有效）；
     * TLS 下消息照常送达，但字节里两者都找不到
     */
    @TestCase
    public func trafficOnTheWireIsEncrypted(): Unit {
        let kit = ActorTestKit("e65-sniff")
        let marker = "TOP-SECRET-e65-${DateTime.now().toUnixTimeStamp().toMilliseconds()}"
        for (useTls in [false, true]) {
            let a = tlsNode(if (useTls) { Some(clusterTls()) } else { None<RemoteTls> })
            let b = tlsNode(if (useTls) { Some(clusterTls()) } else { None<RemoteTls> })
            let sink = Collect()
            b.exposeActor("sink", kit.actorOf({ => sink }, "sink-${useTls}"))
            let proxy = SniffingProxy(b.getLocalAddress())
            a.remoteActorOf(proxy.address(), "sink").tell(StringMessage("${marker}-${useTls}"))
            @Expect(awaitCondition(T5S, { => sink.snapshot().size == 1 }), true)
            let wire = proxy.bytes()
            @Expect(wire.size > 0, true)
            @Expect(containsBytes(wire, "${marker}-${useTls}".toArray()), !useTls)
            // 握手帧里的发起方地址（身份信息）同样只在明文下可见
            @Expect(containsBytes(wire, a.getLocalAddress().toString().toArray()), !useTls)
            if (useTls) {
                @Expect(a.securedConnections() >= 1, true)
            }
            proxy.close()
            a.stop()
            b.stop()
        }
        kit.shutdown()
    }

    /**
     * 停止节点时 TLS 连接立即关闭：入站读协程阻塞在 TLS read 上，直接关 TLS 流每条要等约 1 s；
     * 传输先关底层 TCP 连接唤醒读协程，4 条入站连接的停止应远低于 4 s
     */
    @TestCase
    public func stoppingClosesTlsConnectionsPromptly(): Unit {
        let kit = ActorTestKit("e65-stop")
        let server = tlsNode(Some(clusterTls()))
        let sink = Collect()
        server.exposeActor("sink", kit.actorOf({ => sink }, "sink"))
        let clients = Array<SimpleRemoteManager>(4, { _ => tlsNode(Some(clusterTls())) })
        for (c in clients) {
            c.remoteActorOf(server.getLocalAddress(), "sink").tell(StringMessage("hi"))
        }
        @Expect(awaitCondition(T5S, { => sink.snapshot().size == 4 }), true)
        @Expect(server.securedConnections() >= 4, true)
        let t0 = MonoTime.now()
        server.stop()
        let took = MonoTime.now() - t0
        @Expect(took < Duration.millisecond * 1500, true)
        for (c in clients) {
            c.stop()
        }
        kit.shutdown()
    }

    /** 配置错误立即报出 */
    @TestCase
    public func misconfigurationFailsFast(): Unit {
        // 本节点证书不是由信任的 CA 签发
        var rejected = false
        try {
            RemoteTls.fromPem(TEST_TLS_ROGUE_CERT, TEST_TLS_ROGUE_KEY, TEST_TLS_CA) |> ignoreTls
        } catch (_: IllegalArgumentException) {
            rejected = true
        }
        @Expect(rejected, true)
        // 不支持加密的传输（UDP）：start 抛异常，不会以明文运行
        let udp = SimpleRemoteManager(UdpTransport(NetworkAddress("127.0.0.1", 0)))
        udp.configure(FAST.withTls(clusterTls()))
        var unsupported = false
        try {
            udp.start()
        } catch (_: UnsupportedException) {
            unsupported = true
        }
        @Expect(unsupported, true)
        // 启动后再配置加密
        let started = tlsNode(None<RemoteTls>)
        var late = false
        try {
            started.configure(FAST.withTls(clusterTls()))
        } catch (_: IllegalStateException) {
            late = true
        }
        @Expect(late, true)
        started.stop()
        @Expect(clusterTls().describe().contains("TLS 1.3"), true)
        @Expect(clusterTls().isMutual(), true)
        @Expect(clusterTls().withoutMutualAuthentication().isMutual(), false)
    }
}

func ignoreTls(_: RemoteTls): Unit {}
