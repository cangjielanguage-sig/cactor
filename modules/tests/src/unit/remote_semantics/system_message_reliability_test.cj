/*
 * CActor - 远程系统消息可靠化（E41；含 E31 先行的 N2 用例）
 *
 *   - 接收端在同一地址重启后，可靠投递照常继续（N2：此前新实例的期望序号从 1 开始，
 *     发送端继续发 seq > 1，消息全部进缓冲、永远不投递）
 *   - 30% 丢包下，远程 watch 全部收到 Terminated（此前 WATCH / DEATH 是普通发送，丢一条就永远收不到）
 *   - 被监视节点在心跳容忍时间内重启：凭 PONG 中的启动标识识别，立即投递 Terminated
 *     （此前只要新实例按时回应心跳，监视者就永远不知道旧实例上的 Actor 已经不在）
 *   - 未确认消息超过 giveUpAfter：放弃该节点的全部通道，监视者收到 Terminated
 *   - 通道未确认消息达到 maxUnacked：丢弃新消息并计数，内存有界
 */
package cactor_tests.unit.remote_semantics

import std.collection.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{ActorRef, Terminated}
import cactor_core.message.{StringMessage}
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TcpTransport, TransportProtocol}
import cactor_remote.*
import cactor_testkit.*

let FAST: RemotingSettings = RemotingSettings(Duration.millisecond * 50, 8.0, Duration.millisecond * 300,
    Duration.millisecond * 50, Duration.millisecond * 5)

func nodeWith(transport: NetworkTransport, settings: RemotingSettings): SimpleRemoteManager {
    let m = SimpleRemoteManager(transport)
    m.configure(settings)
    m.start()
    m
}

func nodeAt(port: Int32, settings: RemotingSettings): SimpleRemoteManager {
    nodeWith(TcpTransport(NetworkAddress("127.0.0.1", port)), settings)
}

@Test
public class SystemMessageReliabilityTest {
    @TestCase
    public func reliableDeliveryContinuesAfterReceiverRestart(): Unit {
        let kit = ActorTestKit("n2-restart")
        let a = nodeAt(0, FAST)
        let b = nodeAt(0, FAST)
        let port = b.getLocalAddress().port
        let first = Collect()
        b.exposeActor("sink", kit.actorOf({ => first }, "sink1"))
        let sink = a.remoteActorOf(b.getLocalAddress(), "sink")
        for (i in 0..5) {
            a.tellReliable(sink, StringMessage("a${i}"))
        }
        @Expect(awaitCondition(Duration.second * 10, { => first.snapshot().size >= 5 }), true)
        @Expect(awaitCondition(Duration.second * 5, { => a.reliableDeliveryStats().unacked == 0 }), true)
        // 接收端在同一地址重启：新实例没有旧通道的序号状态
        b.stop()
        let b2 = nodeAt(port, FAST)
        let second = Collect()
        b2.exposeActor("sink", kit.actorOf({ => second }, "sink2"))
        for (i in 0..5) {
            a.tellReliable(sink, StringMessage("b${i}"))
        }
        @Expect(awaitCondition(Duration.second * 10, { => second.snapshot().size >= 5 }), true)
        // 按序、不重复
        let got = second.snapshot()
        @Expect(got.size, 5)
        for (i in 0..got.size) {
            @Expect(got[i], "b${i}")
        }
        @Expect(a.reliableDeliveryStats().peerRestarts >= 1, true)
        @Expect(awaitCondition(Duration.second * 5, { => a.reliableDeliveryStats().unacked == 0 }), true)
        a.stop()
        b2.stop()
        kit.shutdown()
    }

    @TestCase
    public func remoteWatchSurvivesPacketLoss(): Unit {
        let kit = ActorTestKit("e41-watch-loss")
        // 心跳容忍 3 s：30% 丢包下不应误判节点不可达，Terminated 只能来自 DEATH
        let settings = RemotingSettings(Duration.millisecond * 50, 8.0, Duration.second * 3, Duration.millisecond * 30,
            Duration.millisecond * 5)
        let ta = LossyTransport(0.3, 21)
        let tb = LossyTransport(0.3, 23)
        let a = nodeWith(ta, settings)
        let b = nodeWith(tb, settings)
        b.enableRemoteWatch({ creator => kit.system.actorOf(creator, "remote-watcher") })
        let n = 20
        let targets = ArrayList<ActorRef>()
        for (i in 0..n) {
            let t = kit.actorOf({ => Silent() }, "t${i}")
            b.exposeActor("t${i}", t)
            targets.add(t)
        }
        let probe = kit.createTestProbe()
        for (i in 0..n) {
            a.watchRemote(a.remoteActorOf(b.getLocalAddress(), "t${i}"), probe.ref)
        }
        // 全部 WATCH 已确认：之后的 Terminated 必须靠 DEATH 送达
        @Expect(awaitCondition(Duration.second * 10, { => a.reliableDeliveryStats().unacked == 0 }), true)
        for (t in targets) {
            kit.system.stop(t)
        }
        let seen = HashSet<String>()
        for (_ in 0..n) {
            let t = probe.expectMsgType<Terminated>(Duration.second * 10)
            seen.add(t.actor.path.getPath())
        }
        @Expect(seen.size, n)
        @Expect(ta.dropped.load() > 0 && tb.dropped.load() > 0, true)
        a.stop()
        b.stop()
        kit.shutdown()
    }

    @TestCase
    public func remoteWatchDetectsNodeRestartWithinHeartbeatPause(): Unit {
        let kit = ActorTestKit("e41-watch-restart")
        // 心跳容忍 10 s：节点不可达检测在本用例内不会触发
        let settings = RemotingSettings(Duration.millisecond * 50, 8.0, Duration.second * 10, Duration.millisecond * 50,
            Duration.millisecond * 5)
        let a = nodeAt(0, settings)
        let b = nodeAt(0, settings)
        let port = b.getLocalAddress().port
        b.exposeActor("target", kit.actorOf({ => Silent() }, "target"))
        let probe = kit.createTestProbe()
        a.watchRemote(a.remoteActorOf(b.getLocalAddress(), "target"), probe.ref)
        // 几轮心跳，让监视方记下被监视节点的启动标识
        probe.expectNoMsg(Duration.millisecond * 300)
        b.stop()
        let b2 = nodeAt(port, settings)
        b2.exposeActor("target", kit.actorOf({ => Silent() }, "target2"))
        let t = probe.expectMsgType<Terminated>(Duration.second * 3)
        @Expect(t.actor.path.getPath(), "/user/target")
        @Expect(a.reliableDeliveryStats().peerRestarts >= 1, true)
        a.stop()
        b2.stop()
        kit.shutdown()
    }

    @TestCase
    public func reliableChannelGivesUpAndNotifiesWatchers(): Unit {
        let kit = ActorTestKit("e41-give-up")
        let settings = RemotingSettings(Duration.millisecond * 50, 8.0, Duration.second * 10, Duration.millisecond * 50,
            Duration.millisecond * 5).withLimits(1000, Duration.millisecond * 500)
        let a = nodeAt(0, settings)
        // 黑洞节点：收得到，但发出的帧（ACK、PONG）全部丢弃
        let b = nodeWith(LossyTransport(1.0, 5), settings)
        b.exposeActor("sink", kit.actorOf({ => Collect() }, "sink"))
        let sink = a.remoteActorOf(b.getLocalAddress(), "sink")
        let probe = kit.createTestProbe()
        a.watchRemote(sink, probe.ref)
        for (i in 0..5) {
            a.tellReliable(sink, StringMessage("m${i}"))
        }
        let t = probe.expectMsgType<Terminated>(Duration.second * 5)
        @Expect(t.actor.path.getPath(), "/user/sink")
        let stats = a.reliableDeliveryStats()
        // 5 条用户消息 + 1 条 WATCH
        @Expect(stats.gaveUp, 6)
        @Expect(stats.unacked, 0)
        a.stop()
        b.stop()
        kit.shutdown()
    }

    @TestCase
    public func fullChannelDropsNewMessagesAndCounts(): Unit {
        let kit = ActorTestKit("e41-overflow")
        let settings = FAST.withLimits(10, Duration.hour)
        let a = nodeAt(0, settings)
        let b = nodeWith(LossyTransport(1.0, 9), settings)
        b.exposeActor("sink", kit.actorOf({ => Collect() }, "sink"))
        let sink = a.remoteActorOf(b.getLocalAddress(), "sink")
        for (i in 0..15) {
            a.tellReliable(sink, StringMessage("m${i}"))
        }
        let stats = a.reliableDeliveryStats()
        @Expect(stats.unacked, 10)
        @Expect(stats.overflowDropped, 5)
        a.stop()
        b.stop()
        kit.shutdown()
    }
}
