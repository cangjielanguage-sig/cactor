/*
 * CActor - 远程握手、鉴权与隔离（E7′，B1 / B2 / N3）
 *
 *   - 共享密钥相同的节点照常通信；密钥不同或没有密钥的节点连不上（握手被拒，消息到不了 Actor）
 *   - 伪造的 DEATH 被拒绝：此前任何能连上端口的进程都能让监视者收到假的 Terminated
 *   - 隔离（quarantine）：被隔离的实例双向断开，必须重启（新的启动标识）才能重新通信
 *   - 误判不可撤回（对标 Akka）：心跳判定不可达后该实例被隔离，即使它其实还活着、之后恢复发送，也连不回来
 */
package cactor_tests.unit.remote_semantics

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Terminated}
import cactor_core.message.{StringMessage}
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TcpTransport, TransportProtocol, TransportHandshake}
import cactor_remote.*
import cactor_testkit.*

func secureNode(cookie: Option<String>): SimpleRemoteManager {
    let m = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    m.configure(match (cookie) {
        case Some(c) => FAST.withSecureCookie(c)
        case None => FAST
    })
    m.start()
    m
}

/** 可整体"黑洞"的传输：打开时丢弃全部发出的数据帧（握手由内层直接完成，不受影响） */
class SwitchableTransport <: NetworkTransport {
    private let inner: TcpTransport
    let blackhole = AtomicBool(false)

    init() {
        this.inner = TcpTransport(NetworkAddress("127.0.0.1", 0))
    }

    init(port: Int32) {
        this.inner = TcpTransport(NetworkAddress("127.0.0.1", port))
    }

    public func start(): Unit { inner.start() }
    public func stop(): Unit { inner.stop() }
    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit { inner.setDataHandler(handler) }
    public func getLocalAddress(): NetworkAddress { inner.getLocalAddress() }
    public func isActive(): Bool { inner.isActive() }
    public func setHandshake(h: TransportHandshake): Unit { inner.setHandshake(h) }
    public func closeInboundFrom(peerKey: String): Int64 { inner.closeInboundFrom(peerKey) }
    public func resetOutbound(address: NetworkAddress): Unit { inner.resetOutbound(address) }

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        if (!blackhole.load()) {
            inner.send(address, data)
        }
    }
}

@Test
public class RemoteSecurityTest {
    @TestCase
    public func nodesWithTheSameCookieCommunicate(): Unit {
        let kit = ActorTestKit("e7-same-cookie")
        let a = secureNode(Some("s3cret"))
        let b = secureNode(Some("s3cret"))
        b.exposeActor("echo", kit.actorOf({ => Echo() }, "echo"))
        let r = a.ask(a.remoteActorOf(b.getLocalAddress(), "echo"), StringMessage("hi"), T5S).get(T5S)
        @Expect((r as StringMessage).getOrThrow().getContent(), "echo:hi")
        @Expect(a.rejectedHandshakes() + b.rejectedHandshakes(), 0)
        a.stop()
        b.stop()
        kit.shutdown()
    }

    @TestCase
    public func wrongOrMissingCookieCannotReachTheNode(): Unit {
        let kit = ActorTestKit("e7-wrong-cookie")
        let b = secureNode(Some("s3cret"))
        let sink = Collect()
        b.exposeActor("sink", kit.actorOf({ => sink }, "sink"))
        let evil = secureNode(Some("evil"))
        let anon = secureNode(None<String>)
        evil.remoteActorOf(b.getLocalAddress(), "sink").tell(StringMessage("from-evil"))
        anon.remoteActorOf(b.getLocalAddress(), "sink").tell(StringMessage("from-anon"))
        @Expect(awaitCondition(T5S, { => b.rejectedHandshakes() >= 2 }), true)
        // 负向断言：再等 300 ms 确认没有消息漏进来
        sleep(Duration.millisecond * 300)
        @Expect(sink.snapshot().size, 0)
        evil.stop()
        anon.stop()
        b.stop()
        kit.shutdown()
    }

    @TestCase
    public func forgedDeathIsRejected(): Unit {
        let kit = ActorTestKit("e7-forged-death")
        let a = secureNode(Some("s3cret"))
        let b = secureNode(Some("s3cret"))
        b.enableRemoteWatch({ creator => kit.system.actorOf(creator, "remote-watcher") })
        let target = kit.actorOf({ => Silent() }, "target")
        b.exposeActor("target", target)
        let probe = kit.createTestProbe()
        a.watchRemote(a.remoteActorOf(b.getLocalAddress(), "target"), probe.ref)
        @Expect(awaitCondition(T5S, { => a.reliableDeliveryStats().unacked == 0 }), true)
        // 伪造方不知道密钥：冒充 b 发 DEATH
        let forger = secureNode(None<String>)
        forger.sendControl(a.getLocalAddress(), "$sys", StringMessage("DEATH|target|${b.getLocalAddress().toString()}"))
        @Expect(awaitCondition(T5S, { => a.rejectedHandshakes() >= 1 }), true)
        probe.expectNoMsg(Duration.millisecond * 300)
        // 真实的停止照常送达
        kit.system.stop(target)
        let t = probe.expectMsgType<Terminated>(T5S)
        @Expect(t.actor.path.getPath(), "/user/target")
        forger.stop()
        a.stop()
        b.stop()
        kit.shutdown()
    }

    @TestCase
    public func quarantinedPeerIsCutOffUntilItRestarts(): Unit {
        let kit = ActorTestKit("e7-quarantine")
        let a = secureNode(None<String>)
        let b = secureNode(None<String>)
        let port = b.getLocalAddress().port
        let sinkA = Collect()
        a.exposeActor("sinkA", kit.actorOf({ => sinkA }, "sinkA"))
        let sinkB = Collect()
        b.exposeActor("sinkB", kit.actorOf({ => sinkB }, "sinkB"))
        b.exposeActor("target", kit.actorOf({ => Silent() }, "target"))
        let probe = kit.createTestProbe()
        a.watchRemote(a.remoteActorOf(b.getLocalAddress(), "target"), probe.ref)
        a.remoteActorOf(b.getLocalAddress(), "sinkB").tell(StringMessage("before"))
        b.remoteActorOf(a.getLocalAddress(), "sinkA").tell(StringMessage("before"))
        @Expect(awaitCondition(T5S, { => sinkB.snapshot().size == 1 && sinkA.snapshot().size == 1 }), true)
        a.quarantine(b.getLocalAddress(), "测试")
        @Expect(a.isQuarantined(b.getLocalAddress()), true)
        // 监视者立即收到 Terminated
        probe.expectMsgType<Terminated>(T5S)
        // 双向断开：被隔离的实例发来的、发给它的都到不了
        for (i in 0..3) {
            a.remoteActorOf(b.getLocalAddress(), "sinkB").tell(StringMessage("after-${i}"))
            b.remoteActorOf(a.getLocalAddress(), "sinkA").tell(StringMessage("after-${i}"))
            sleep(Duration.millisecond * 100)
        }
        @Expect(sinkB.snapshot().size, 1)
        @Expect(sinkA.snapshot().size, 1)
        // 对端重启（新的启动标识）后恢复通信
        b.stop()
        let b2 = nodeAt(port, FAST)
        let sinkB2 = Collect()
        b2.exposeActor("sinkB", kit.actorOf({ => sinkB2 }, "sinkB2"))
        @Expect(awaitCondition(Duration.second * 10, { =>
            a.remoteActorOf(b2.getLocalAddress(), "sinkB").tell(StringMessage("again"))
            b2.remoteActorOf(a.getLocalAddress(), "sinkA").tell(StringMessage("again"))
            sinkB2.snapshot().size >= 1 && sinkA.snapshot().size >= 2
        }), true)
        @Expect(a.isQuarantined(b2.getLocalAddress()), false)
        a.stop()
        b2.stop()
        kit.shutdown()
    }

    @TestCase
    public func misjudgedNodeStaysQuarantined(): Unit {
        let kit = ActorTestKit("e7-misjudged")
        let a = secureNode(None<String>)
        let tb = SwitchableTransport()
        let b = nodeWith(tb, FAST)
        let sinkA = Collect()
        a.exposeActor("sinkA", kit.actorOf({ => sinkA }, "sinkA"))
        b.exposeActor("target", kit.actorOf({ => Silent() }, "target"))
        let probe = kit.createTestProbe()
        a.watchRemote(a.remoteActorOf(b.getLocalAddress(), "target"), probe.ref)
        b.remoteActorOf(a.getLocalAddress(), "sinkA").tell(StringMessage("before"))
        @Expect(awaitCondition(T5S, { => sinkA.snapshot().size == 1 }), true)
        // b 停顿（发出的帧全丢）超过心跳容忍时间：a 判定不可达，投递 Terminated 并隔离 b
        tb.blackhole.store(true)
        probe.expectMsgType<Terminated>(T5S)
        @Expect(a.isQuarantined(b.getLocalAddress()), true)
        // b 恢复：误判不可撤回，它发来的消息仍被拒绝
        tb.blackhole.store(false)
        for (i in 0..5) {
            b.remoteActorOf(a.getLocalAddress(), "sinkA").tell(StringMessage("after-${i}"))
            sleep(Duration.millisecond * 100)
        }
        @Expect(sinkA.snapshot().size, 1)
        @Expect(a.rejectedHandshakes() >= 1, true)
        a.stop()
        b.stop()
        kit.shutdown()
    }
}
