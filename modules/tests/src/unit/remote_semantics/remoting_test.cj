/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R6 测试：跨节点回复、远程 ask、远程 watch（Actor 停止 / 节点故障）、可靠投递（故障注入）
 * 全部使用本机回环 TCP 上的两个真实节点。
 */
package cactor_tests.unit.remote_semantics

import std.sync.*
import std.time.*
import std.random.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorRef, ActorContext, Terminated}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.typed.{TypedAskTimeoutException}
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TcpTransport, TransportProtocol}
import cactor_remote.*
import cactor_testkit.*

let T5S: Duration = Duration.second * 5

/** 回显：用 reply 回复发送方 */
class Echo <: Actor {
    public prop name: String { get() { "echo" } }
    public prop description: String { get() { "echo" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage => context.reply(StringMessage("echo:${s.getContent()}"))
            case _ => ()
        }
        MessageResult.Handled
    }
}

/** 收集收到的字符串 */
class Collect <: Actor {
    let lock = Mutex()
    let items = ArrayList<String>()
    public prop name: String { get() { "collect" } }
    public prop description: String { get() { "collect" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            synchronized(lock) { items.add(s.getContent()) }
        }
        MessageResult.Handled
    }
    func snapshot(): Array<String> {
        synchronized(lock) { items.toArray() }
    }
}

class Silent <: Actor {
    public prop name: String { get() { "silent" } }
    public prop description: String { get() { "silent" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

/** 故障注入：按概率丢弃发出的帧 */
class LossyTransport <: NetworkTransport {
    private let inner: TcpTransport
    private let dropRate: Float64
    private let lock = Mutex()
    private let rnd: Random
    let dropped = AtomicInt64(0)

    init(dropRate: Float64, seed: UInt64) {
        this.inner = TcpTransport(NetworkAddress("127.0.0.1", 0))
        this.dropRate = dropRate
        this.rnd = Random(seed)
    }

    public func start(): Unit { inner.start() }
    public func stop(): Unit { inner.stop() }
    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit { inner.setDataHandler(handler) }
    public func getLocalAddress(): NetworkAddress { inner.getLocalAddress() }
    public func isActive(): Bool { inner.isActive() }

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        let drop = synchronized(lock) { rnd.nextFloat64() < dropRate }
        if (drop) {
            dropped.fetchAdd(1)
            return
        }
        inner.send(address, data)
    }
}

func node(): SimpleRemoteManager {
    let m = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    m.configure(RemotingSettings(Duration.millisecond * 50, 8.0, Duration.millisecond * 300,
        Duration.millisecond * 50, Duration.millisecond * 5))
    m.start()
    m
}

@Test
public class RemotingTest {
    @TestCase
    public func testReplyTravelsBackAcrossNodes(): Unit {
        let kit = ActorTestKit("rm-reply")
        let a = node()
        let b = node()
        b.exposeActor("echo", kit.actorOf({ => Echo() }, "echo"))
        let probe = kit.createTestProbe()
        let remote = a.remoteActorOf(b.getLocalAddress(), "echo")
        probe.send(remote, StringMessage("hi"))
        let reply = probe.expectMsgType<StringMessage>(T5S)
        @Expect(reply.getContent(), "echo:hi")
        a.stop()
        b.stop()
        kit.shutdown()
    }

    @TestCase
    public func testRemoteAskAndTimeout(): Unit {
        let kit = ActorTestKit("rm-ask")
        let a = node()
        let b = node()
        b.exposeActor("echo", kit.actorOf({ => Echo() }, "echo"))
        b.exposeActor("silent", kit.actorOf({ => Silent() }, "silent"))
        let echo = a.remoteActorOf(b.getLocalAddress(), "echo")
        for (i in 0..20) {
            let r = a.ask(echo, StringMessage("q${i}"), T5S).get(T5S)
            @Expect((r as StringMessage).getOrThrow().getContent(), "echo:q${i}")
        }
        var timedOut = false
        try {
            a.ask(a.remoteActorOf(b.getLocalAddress(), "silent"), StringMessage("x"), Duration.millisecond * 100).get(T5S)
        } catch (_: TypedAskTimeoutException) {
            timedOut = true
        }
        @Expect(timedOut, true)
        a.stop()
        b.stop()
        kit.shutdown()
    }

    @TestCase
    public func testRemoteWatchActorStop(): Unit {
        let kit = ActorTestKit("rm-watch")
        let a = node()
        let b = node()
        b.enableRemoteWatch({ creator => kit.system.actorOf(creator, "remote-watcher") })
        let target = kit.actorOf({ => Silent() }, "target")
        b.exposeActor("target", target)
        let probe = kit.createTestProbe()
        let remote = a.remoteActorOf(b.getLocalAddress(), "target")
        a.watchRemote(remote, probe.ref)
        sleep(Duration.millisecond * 100)
        probe.expectNoMsg(Duration.millisecond * 100)
        kit.system.stop(target)
        let t = probe.expectMsgType<Terminated>(T5S)
        @Expect(t.actor.path.getPath(), "/user/target")
        a.stop()
        b.stop()
        kit.shutdown()
    }

    @TestCase
    public func testRemoteWatchUnknownNameTerminatesImmediately(): Unit {
        let kit = ActorTestKit("rm-watch-none")
        let a = node()
        let b = node()
        let probe = kit.createTestProbe()
        a.watchRemote(a.remoteActorOf(b.getLocalAddress(), "nobody"), probe.ref)
        probe.expectMsgType<Terminated>(T5S)
        a.stop()
        b.stop()
        kit.shutdown()
    }

    @TestCase
    public func testRemoteWatchNodeFailure(): Unit {
        let kit = ActorTestKit("rm-watch-node")
        let a = node()
        let b = node()
        b.exposeActor("target", kit.actorOf({ => Silent() }, "target"))
        let probe = kit.createTestProbe()
        a.watchRemote(a.remoteActorOf(b.getLocalAddress(), "target"), probe.ref)
        // 节点正常时不应误判
        probe.expectNoMsg(Duration.millisecond * 600)
        b.stop()
        let t = probe.expectMsgType<Terminated>(T5S)
        @Expect(t.actor.path.getPath(), "/user/target")
        a.stop()
        kit.shutdown()
    }

    @TestCase
    public func testUnwatchStopsNotifications(): Unit {
        let kit = ActorTestKit("rm-unwatch")
        let a = node()
        let b = node()
        b.enableRemoteWatch({ creator => kit.system.actorOf(creator, "remote-watcher") })
        let target = kit.actorOf({ => Silent() }, "target")
        b.exposeActor("target", target)
        let probe = kit.createTestProbe()
        let remote = a.remoteActorOf(b.getLocalAddress(), "target")
        a.watchRemote(remote, probe.ref)
        a.unwatchRemote(remote, probe.ref)
        sleep(Duration.millisecond * 100)
        kit.system.stop(target)
        probe.expectNoMsg(Duration.millisecond * 500)
        a.stop()
        b.stop()
        kit.shutdown()
    }

    @TestCase
    public func testReliableDeliveryUnderPacketLoss(): Unit {
        let kit = ActorTestKit("rm-reliable")
        let ta = LossyTransport(0.3, 7)
        let tb = LossyTransport(0.3, 11)
        let a = SimpleRemoteManager(ta)
        let b = SimpleRemoteManager(tb)
        let settings = RemotingSettings(Duration.millisecond * 50, 8.0, Duration.second, Duration.millisecond * 30,
            Duration.millisecond * 5)
        a.configure(settings)
        b.configure(settings)
        a.start()
        b.start()
        let collector = Collect()
        b.exposeActor("sink", kit.actorOf({ => collector }, "sink"))
        let sink = a.remoteActorOf(b.getLocalAddress(), "sink")
        let n = 2000
        for (i in 0..n) {
            a.tellReliable(sink, StringMessage("m${i}"))
        }
        @Expect(awaitCondition(Duration.second * 30, { => collector.snapshot().size >= n }), true)
        sleep(Duration.millisecond * 200)
        let got = collector.snapshot()
        // 恰好一次、按序
        @Expect(got.size, n)
        var inOrder = true
        for (i in 0..got.size) {
            if (got[i] != "m${i}") {
                inOrder = false
            }
        }
        @Expect(inOrder, true)
        let (resent, dups, _) = a.reliableStats()
        @Expect(ta.dropped.load() > 0 && tb.dropped.load() > 0, true)
        @Expect(resent > 0, true)
        // 确认最终全部到达，发送端不再积压
        @Expect(awaitCondition(Duration.second * 10, { => a.reliableStats()[2] == 0 }), true)
        a.stop()
        b.stop()
        kit.shutdown()
    }
}
