/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R6 测试：跨节点回复、远程 ask、远程 watch（Actor 停止 / 节点故障）、可靠投递（故障注入）
 * 跨节点交互使用本机回环 TCP；发送异常与跨实例旧回复采用显式传输注入。
 */
package cactor_tests.unit.remote_semantics

import std.sync.*
import std.time.*
import std.random.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorRef, ActorContext, Terminated}
import cactor_core.message.{Message, MessageResult, StringMessage, NetworkMessage, NetworkMessageSerializer}
import cactor_core.typed.*
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TcpTransport, TransportProtocol, TransportHandshake}
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
    let sent = AtomicInt64(0)

    init(dropRate: Float64, seed: UInt64) {
        this.inner = TcpTransport(NetworkAddress("127.0.0.1", 0))
        this.dropRate = dropRate
        this.rnd = Random(seed)
    }

    public func start(): Unit { inner.start() }
    public func stop(): Unit { inner.stop() }
    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit { inner.setDataHandler(handler) }
    // 握手由内层 TCP 传输直接读写，不经过丢帧逻辑（丢的是数据帧）
    public func setHandshake(h: TransportHandshake): Unit { inner.setHandshake(h) }
    public func closeInboundFrom(peerKey: String): Int64 { inner.closeInboundFrom(peerKey) }
    public func resetOutbound(address: NetworkAddress): Unit { inner.resetOutbound(address) }
    public func getLocalAddress(): NetworkAddress { inner.getLocalAddress() }
    public func isActive(): Bool { inner.isActive() }

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        sent.fetchAdd(1)
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

/** 发送异常注入，不替代其余用例的真实 TCP。 */
class AskFailingTransport <: NetworkTransport {
    let sends = AtomicInt64(0)
    let active = AtomicBool(false)
    public func start(): Unit { active.store(true) }
    public func stop(): Unit { active.store(false) }
    public func isActive(): Bool { active.load() }
    public func getLocalAddress(): NetworkAddress { NetworkAddress("127.0.0.1", 1) }
    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit {}
    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        sends.fetchAdd(1)
        throw IllegalStateException("injected-send-failure")
    }
}

/** 同址新实例重放旧回复；数据仍经过真实网络消息序列化与接收分发。 */
class AskReplayTransport <: NetworkTransport {
    let active = AtomicBool(false)
    let frames = ArrayList<NetworkMessage>()
    let codec = NetworkMessageSerializer()
    var handler: Option<(Array<UInt8>) -> Unit> = None<(Array<UInt8>) -> Unit>
    public func start(): Unit { active.store(true) }
    public func stop(): Unit { active.store(false) }
    public func isActive(): Bool { active.load() }
    public func getLocalAddress(): NetworkAddress { NetworkAddress("127.0.0.1", 1) }
    public func setDataHandler(h: (Array<UInt8>) -> Unit): Unit { handler = Some(h) }
    public func send(address: NetworkAddress, data: Array<UInt8>): Unit { frames.add(codec.deserialize(data)) }
    func lastReplyName(): String {
        let (_, name) = parseRemoteSender(frames[frames.size - 1].sender.getOrThrow()).getOrThrow()
        name
    }
    func reply(name: String, text: String): Unit {
        handler.getOrThrow()(codec.serialize(NetworkMessage(name, StringMessage(text), None<String>)))
    }
}

func limitedAskNode(limit: Int64,
    transport!: NetworkTransport = TcpTransport(NetworkAddress("127.0.0.1", 0))): SimpleRemoteManager {
    let m = SimpleRemoteManager(transport)
    m.configure(RemotingSettings.defaults().withAskLimit(limit))
    m.start()
    m
}

@Test
public class RemoteAskLifecycleTest {
    @TestCase
    public func validatesLimitTimeoutAndStoppedAdmission(): Unit {
        let settings = RemotingSettings.defaults().withAskLimit(3)
            .withLimits(10, Duration.minute).withSecureCookie("test-cookie")
        @Expect(settings.maxPendingAsks, 3)
        @ExpectThrows[IllegalArgumentException](settings.withAskLimit(0))
        @ExpectThrows[IllegalArgumentException](settings.withAskLimit(-1))
        let a = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        let target = a.remoteActorOf(NetworkAddress("127.0.0.1", 1), "missing")
        @ExpectThrows[IllegalArgumentException](a.ask(target, StringMessage("zero"), Duration.Zero))
        @ExpectThrows[IllegalArgumentException](a.ask(target, StringMessage("negative"), Duration.second * -1))
        @ExpectThrows[IllegalStateException](a.ask(target, StringMessage("stopped"), T5S).get(T5S))
        @Expect(a.pendingAskCount(), 0)
    }

    @TestCase
    public func rejectsAtCapacityAndReusesCapacityAfterReply(): Unit {
        let kit = ActorTestKit("rm-ask-limit")
        let transport = LossyTransport(0.0, 42u64)
        let a = limitedAskNode(1, transport: transport)
        let b = node()
        try {
            let senders = TestInbox<ActorRef>("limit-senders")
            let target = kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ ctx, _ =>
                senders.tell(ctx.toClassic().getOrThrow().senderRef().getOrThrow())
                TypedBehaviors.same<StringMessage>()
            }), "held")
            b.exposeActor("held", target.toClassic().getOrThrow())
            b.exposeActor("echo", kit.actorOf({ => Echo() }, "echo"))
            let remote = a.remoteActorOf(b.getLocalAddress(), "held")
            let first = a.ask(remote, StringMessage("held"), Duration.minute)
            @Expect(awaitCondition(T5S, { => senders.hasMessages() }), true)
            @Expect(a.pendingAskCount(), 1)
            let sent = transport.sent.load()
            @ExpectThrows[RemoteAskCapacityException](a.ask(remote, StringMessage("rejected"), T5S).get(T5S))
            @Expect(transport.sent.load(), sent)
            senders.receiveMessage().tell(StringMessage("accepted"))
            @Expect((first.get(T5S) as StringMessage).getOrThrow().getContent(), "accepted")
            @Expect(awaitCondition(T5S, { => a.pendingAskCount() == 0 }), true)
            let echoed = a.ask(a.remoteActorOf(b.getLocalAddress(), "echo"), StringMessage("again"), T5S)
            @Expect((echoed.get(T5S) as StringMessage).getOrThrow().getContent(), "echo:again")
            @Expect(awaitCondition(T5S, { => a.pendingAskCount() == 0 }), true)
        } finally {
            a.stop()
            b.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func timeoutReleasesCapacityAndLateReplyCannotCompleteNextAsk(): Unit {
        let kit = ActorTestKit("rm-ask-late")
        let a = limitedAskNode(1)
        let b = node()
        try {
            let senders = TestInbox<ActorRef>("late-senders")
            let target = kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ ctx, _ =>
                senders.tell(ctx.toClassic().getOrThrow().senderRef().getOrThrow())
                TypedBehaviors.same<StringMessage>()
            }), "held")
            b.exposeActor("held", target.toClassic().getOrThrow())
            let remote = a.remoteActorOf(b.getLocalAddress(), "held")
            let first = a.ask(remote, StringMessage("first"), Duration.millisecond * 100)
            @Expect(awaitCondition(T5S, { => senders.hasMessages() }), true)
            let oldSender = senders.receiveMessage()
            @ExpectThrows[TypedAskTimeoutException](first.get(T5S))
            @Expect(awaitCondition(T5S, { => a.pendingAskCount() == 0 }), true)
            let second = a.ask(remote, StringMessage("second"), Duration.minute)
            @Expect(awaitCondition(T5S, { => senders.hasMessages() }), true)
            oldSender.tell(StringMessage("late-first"))
            senders.receiveMessage().tell(StringMessage("second"))
            @Expect((second.get(T5S) as StringMessage).getOrThrow().getContent(), "second")
            @ExpectThrows[TypedAskTimeoutException](first.get(T5S))
            @Expect(awaitCondition(T5S, { => a.pendingAskCount() == 0 }), true)
        } finally {
            a.stop()
            b.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func shutdownCompletesAllAndAllowsCallbackReentry(): Unit {
        let kit = ActorTestKit("rm-ask-stop")
        let a = limitedAskNode(2)
        let b = node()
        try {
            b.exposeActor("silent", kit.actorOf({ => Silent() }, "silent"))
            let remote = a.remoteActorOf(b.getLocalAddress(), "silent")
            let first = a.ask(remote, StringMessage("one"), Duration.minute)
            let second = a.ask(remote, StringMessage("two"), Duration.minute)
            let reentered = AtomicBool(false)
            first.onComplete({ _ =>
                a.stop()
                reentered.store(a.pendingAskCount() == 0 &&
                    a.ask(remote, StringMessage("after-stop"), T5S).isCompleted())
            })
            a.stop()
            @Expect(first.isCompleted() && second.isCompleted(), true)
            @ExpectThrows[IllegalStateException](first.get(T5S))
            @ExpectThrows[IllegalStateException](second.get(T5S))
            @Expect(reentered.load(), true)
            @Expect(a.pendingAskCount(), 0)
        } finally {
            a.stop()
            b.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func replyNamesAreIsolatedAcrossManagerInstances(): Unit {
        let kit = ActorTestKit("rm-reply-incarnation")
        let oldWire = AskReplayTransport()
        let newWire = AskReplayTransport()
        let oldManager = limitedAskNode(1, transport: oldWire)
        let newManager = limitedAskNode(1, transport: newWire)
        try {
            let destination = NetworkAddress("127.0.0.1", 2)
            let probe = kit.createTestProbe()
            let oldRemote = oldManager.remoteActorOf(destination, "held")
            let oldAsk = oldManager.ask(oldRemote, StringMessage("old"), Duration.minute)
            let oldAskName = oldWire.lastReplyName()
            oldRemote.tell(StringMessage("old-sender"), probe.ref)
            let oldSenderName = oldWire.lastReplyName()
            oldManager.stop()
            @Expect(oldAsk.isCompleted(), true)
            let newRemote = newManager.remoteActorOf(destination, "held")
            let current = newManager.ask(newRemote, StringMessage("current"), Duration.minute)
            let newAskName = newWire.lastReplyName()
            newRemote.tell(StringMessage("new-sender"), probe.ref)
            let newSenderName = newWire.lastReplyName()
            @Expect(oldAskName != newAskName, true)
            @Expect(oldSenderName != newSenderName, true)
            newWire.reply(oldAskName, "stale-ask")
            newWire.reply(newAskName, "current-ask")
            @Expect((current.get(T5S) as StringMessage).getOrThrow().getContent(), "current-ask")
            newWire.reply(oldSenderName, "stale-sender")
            newWire.reply(newSenderName, "current-sender")
            @Expect(probe.expectMsgType<StringMessage>(T5S).getContent(), "current-sender")
            @Expect(newManager.pendingAskCount(), 0)
        } finally {
            oldManager.stop()
            newManager.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func sendFailureReleasesEveryAttempt(): Unit {
        let transport = AskFailingTransport()
        let a = limitedAskNode(1, transport: transport)
        try {
            let target = a.remoteActorOf(NetworkAddress("127.0.0.1", 2), "echo")
            for (_ in 0..3) {
                let result = a.ask(target, StringMessage("failure"), Duration.minute)
                @Expect(result.isCompleted(), true)
                @ExpectThrows[IllegalStateException](result.get(T5S))
                @Expect(a.pendingAskCount(), 0)
            }
            @Expect(transport.sends.load(), 3)
        } finally { a.stop() }
    }

    @TestCase
    public func stopRacesWithAdmissionWithoutLeavingWaiters(): Unit {
        let kit = ActorTestKit("rm-ask-race")
        let b = node()
        try {
            b.exposeActor("echo", kit.actorOf({ => Echo() }, "echo"))
            for (_ in 0..20) {
                let a = limitedAskNode(1)
                try {
                    let stop = spawn { a.stop() }
                    let result = a.ask(a.remoteActorOf(b.getLocalAddress(), "echo"), StringMessage("race"), Duration.minute)
                    stop.get(T5S)
                    @Expect(result.isCompleted(), true)
                    try {
                        @Expect((result.get(T5S) as StringMessage).getOrThrow().getContent(), "echo:race")
                    } catch (e: Exception) {
                        @Expect((e as TypedAskTimeoutException).isNone(), true)
                    }
                    @Expect(a.pendingAskCount(), 0)
                } finally { a.stop() }
            }
        } finally {
            b.stop()
            kit.shutdown()
        }
    }
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
