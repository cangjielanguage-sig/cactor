/* CActor Remote - 可靠通道数量准入与去重状态保护。 */
package cactor_tests.unit.remote_semantics

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{StringMessage, NetworkMessage, NetworkMessageSerializer}
import cactor_core.typed.*
import cactor_foundation.network.{NetworkAddress, NetworkTransport}
import cactor_remote.*
import cactor_testkit.*

/** 显式信封注入经过真实编解码与接收分派；出站丢弃使未确认状态保持稳定。 */
class ChannelBudgetWire <: NetworkTransport {
    let active = AtomicBool(false)
    var handler: Option<(Array<UInt8>) -> Unit> = None<(Array<UInt8>) -> Unit>
    public func start(): Unit { active.store(true) }
    public func stop(): Unit { active.store(false) }
    public func isActive(): Bool { active.load() }
    public func getLocalAddress(): NetworkAddress { NetworkAddress("127.0.0.1", 1) }
    public func setDataHandler(h: (Array<UInt8>) -> Unit): Unit { handler = Some(h) }
    public func send(address: NetworkAddress, data: Array<UInt8>): Unit { () }
    func inject(id: String, seq: Int64, text: String): Unit {
        let envelope = ReliableEnvelope(id, seq, "127.0.0.1:2", "", StringMessage(text))
        handler.getOrThrow()(NetworkMessageSerializer().serialize(NetworkMessage("sink", envelope, None<String>)))
    }
}

@Test
public class ReliableChannelBudgetTest {
    @TestCase
    public func settingsValidateAndBuildersPreserveChannelBudgets(): Unit {
        let s = RemotingSettings.defaults().withChannelLimits(2, 3).withAskLimit(4)
            .withLimits(20, Duration.minute).withSecureCookie("cookie")
        @Expect(s.maxOutboundReliableChannels, 2)
        @Expect(s.maxInboundReliableChannels, 3)
        @Expect(s.maxPendingAsks, 4)
        @Expect(RemotingSettings.defaults().maxInboundReliableChannels, 4096)
        @ExpectThrows[IllegalArgumentException](s.withChannelLimits(0, 1))
        @ExpectThrows[IllegalArgumentException](s.withChannelLimits(1, -1))
    }

    @TestCase
    public func concurrentOutboundAdmissionKeepsExistingChannelUsable(): Unit {
        let m = nodeWith(ChannelBudgetWire(), RemotingSettings.defaults().withChannelLimits(2, 2))
        try {
            let address = NetworkAddress("127.0.0.1", 2)
            let kept = m.remoteActorOf(address, "kept")
            m.tellReliable(kept, StringMessage("first"))
            let done = AtomicInt64(0)
            for (i in 0..100) {
                spawn {
                    m.tellReliable(m.remoteActorOf(address, "new${i}"), StringMessage("new"))
                    done.fetchAdd(1)
                }
            }
            @Expect(awaitCondition(T5S, { => done.load() == 100 }), true)
            @Expect(m.reliableChannelStats().outboundChannels, 2)
            @Expect(m.reliableChannelStats().outboundRejected, 99)
            @Expect(m.reliableDeliveryStats().overflowDropped, 99)
            m.tellReliable(kept, StringMessage("second"))
            @Expect(m.reliableDeliveryStats().unacked, 3)
        } finally { m.stop() }
    }

    @TestCase
    public func concurrentInboundAdmissionDoesNotExceedBudget(): Unit {
        let wire = ChannelBudgetWire()
        let m = nodeWith(wire, RemotingSettings.defaults().withChannelLimits(2, 2))
        try {
            let done = AtomicInt64(0)
            for (i in 0..100) {
                spawn {
                    wire.inject("source${i}#1", 1, "message")
                    done.fetchAdd(1)
                }
            }
            @Expect(awaitCondition(T5S, { => done.load() == 100 }), true)
            @Expect(m.reliableChannelStats().inboundChannels, 2)
            @Expect(m.reliableChannelStats().inboundRejected, 98)
        } finally { m.stop() }
    }

    @TestCase
    public func newGenerationReplacesAtCapacityAndOldReplayCannotDeliver(): Unit {
        let kit = ActorTestKit("channel-generation-budget")
        let wire = ChannelBudgetWire()
        let m = nodeWith(wire, RemotingSettings.defaults().withChannelLimits(1, 1))
        let inbox = TestInbox<StringMessage>("generation-events")
        try {
            let ref = kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ _, msg =>
                inbox.tell(msg)
                TypedBehaviors.same<StringMessage>()
            }), "sink")
            m.exposeActor("sink", ref.toClassic().getOrThrow())
            wire.inject("source#1", 1, "old-first")
            wire.inject("other#1", 1, "rejected")
            wire.inject("source#2", 1, "new-first")
            wire.inject("source#1", 2, "stale")
            wire.inject("source#2", 3, "new-third")
            wire.inject("source#2", 2, "new-second")
            @Expect(awaitCondition(T5S, { => inbox.size() == 4 }), true)
            @Expect(inbox.receiveMessage().getContent(), "old-first")
            @Expect(inbox.receiveMessage().getContent(), "new-first")
            @Expect(inbox.receiveMessage().getContent(), "new-second")
            @Expect(inbox.receiveMessage().getContent(), "new-third")
            @Expect(m.reliableChannelStats().inboundChannels, 1)
            @Expect(m.reliableChannelStats().inboundRejected, 1)
            @Expect(m.reliableDeliveryStats().duplicatesDropped >= 1, true)
        } finally { m.stop(); kit.shutdown() }
    }

    @TestCase
    public func shrinkingBelowRetainedStateFailsAndRestartPreservesBudget(): Unit {
        let wire = ChannelBudgetWire()
        let s = RemotingSettings.defaults().withChannelLimits(2, 2)
        let m = nodeWith(wire, s)
        let address = NetworkAddress("127.0.0.1", 2)
        try {
            m.tellReliable(m.remoteActorOf(address, "a"), StringMessage("a"))
            m.tellReliable(m.remoteActorOf(address, "b"), StringMessage("b"))
            wire.inject("one#1", 1, "one")
            wire.inject("two#1", 1, "two")
            m.stop()
            @ExpectThrows[IllegalArgumentException](m.configure(s.withChannelLimits(1, 2)))
            @ExpectThrows[IllegalArgumentException](m.configure(s.withChannelLimits(3, 1)))
            @Expect(m.reliableChannelStats().outboundChannels, 2)
            @Expect(m.reliableChannelStats().inboundChannels, 2)
            // 失败的候选配置出站为 3；必须仍按原上限 2 拒绝，证明没有部分覆盖设置。
            m.start()
            m.tellReliable(m.remoteActorOf(address, "c"), StringMessage("not-admitted"))
            @Expect(m.reliableChannelStats().outboundChannels, 2)
            @Expect(m.reliableChannelStats().outboundRejected, 1)
            m.stop()
            m.configure(s.withChannelLimits(3, 3))
            m.start()
            m.tellReliable(m.remoteActorOf(address, "c"), StringMessage("c"))
            wire.inject("three#1", 1, "three")
            @Expect(m.reliableChannelStats().outboundChannels, 3)
            @Expect(m.reliableChannelStats().inboundChannels, 3)
        } finally { m.stop() }
    }

    @TestCase
    public func tcpSaturationDoesNotAcknowledgeRejectedChannelOrBreakExistingOne(): Unit {
        let kit = ActorTestKit("channel-budget-tcp")
        let s = RemotingSettings.defaults().withChannelLimits(1, 1)
        let a = nodeAt(0, s)
        let b = nodeAt(0, s)
        let c = nodeAt(0, s)
        let inbox = TestInbox<StringMessage>("tcp-events")
        try {
            let ref = kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ _, msg =>
                inbox.tell(msg)
                TypedBehaviors.same<StringMessage>()
            }), "sink")
            b.exposeActor("sink", ref.toClassic().getOrThrow())
            let target = a.remoteActorOf(b.getLocalAddress(), "sink")
            a.tellReliable(target, StringMessage("first"))
            @Expect(awaitCondition(T5S, { => a.reliableDeliveryStats().unacked == 0 && inbox.size() == 1 }), true)
            c.tellReliable(c.remoteActorOf(b.getLocalAddress(), "sink"), StringMessage("rejected"))
            @Expect(awaitCondition(T5S, { => b.reliableChannelStats().inboundRejected > 0 }), true)
            @Expect(c.reliableDeliveryStats().unacked, 1)
            a.tellReliable(a.remoteActorOf(b.getLocalAddress(), "another"), StringMessage("local-rejected"))
            a.tellReliable(target, StringMessage("second"))
            @Expect(awaitCondition(T5S, { => a.reliableDeliveryStats().unacked == 0 && inbox.size() == 2 }), true)
            @Expect(inbox.receiveMessage().getContent(), "first")
            @Expect(inbox.receiveMessage().getContent(), "second")
            @Expect(a.reliableChannelStats().outboundRejected, 1)
            @Expect(b.reliableChannelStats().inboundChannels, 1)
        } finally { a.stop(); b.stop(); c.stop(); kit.shutdown() }
    }
}
