/* CActor Remote - 全节点可靠消息预算（DA3）。
 * 出站：所有通道的未确认消息合计不超过 maxTotalUnacked，ACK / 隔离 / 放弃都要归还预算。
 * 入站：所有通道的乱序缓冲合计不超过 maxInboundBuffered，按序取出与换代回收都要归还。
 * 对端重启：只回收已隔离旧实例的入站通道，迟到的旧实例信封被拒且不会再投一次。
 * 每个用例都核对节点级计数与通道实际保留量一致（防止计数漂移）。
 */
package cactor_tests.unit.remote_semantics

import std.collection.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{StringMessage, NetworkMessage, NetworkMessageSerializer}
import cactor_core.typed.*
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TransportHandshake}
import cactor_remote.*
import cactor_testkit.*

/**
 * 内存传输替身：握手、入站信封和 ACK 都经真实编解码与接收分派；
 * 出站帧不发出去，只记下每个目标最近的通道 ID 与最大序号，供测试回 ACK
 */
class NodeBudgetWire <: NetworkTransport {
    let active = AtomicBool(false)
    let lock = Mutex()
    var handler: Option<(Array<UInt8>) -> Unit> = None<(Array<UInt8>) -> Unit>
    var handshake: Option<TransportHandshake> = None<TransportHandshake>
    let outbound = HashMap<String, (String, Int64)>()

    public func start(): Unit { active.store(true) }
    public func stop(): Unit { active.store(false) }
    public func isActive(): Bool { active.load() }
    public func getLocalAddress(): NetworkAddress { NetworkAddress("127.0.0.1", 1) }
    public func setDataHandler(h: (Array<UInt8>) -> Unit): Unit { handler = Some(h) }
    public func setHandshake(h: TransportHandshake): Unit { handshake = Some(h) }

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        let nm = NetworkMessageSerializer().deserialize(data)
        if (let Some(e) <- (nm.message as ReliableEnvelope)) {
            synchronized(lock) {
                let top = match (outbound.get(nm.targetActor)) {
                    case Some((id, s)) where id == e.channelId => max(s, e.seq)
                    case _ => e.seq
                }
                outbound[nm.targetActor] = (e.channelId, top)
            }
        }
    }

    func admit(port: Int64, uid: UInt64): Bool {
        handshake.getOrThrow().accept("HELLO|1|127.0.0.1:${port}|${uid}|0|127.0.0.1:1|-".toArray()).isSome()
    }

    func inject(id: String, seq: Int64, text: String): Unit {
        let envelope = ReliableEnvelope(id, seq, "127.0.0.1:2", "", StringMessage(text))
        handler.getOrThrow()(NetworkMessageSerializer().serialize(NetworkMessage("sink", envelope, None<String>)))
    }

    /** 对每个已见过的出站通道回累计 ACK（不带启动标识，不触发重启判定） */
    func ackAll(): Unit {
        let acks = synchronized(lock) { outbound.values().toArray() }
        for ((id, upTo) in acks) {
            let ack = StringMessage("ACK|${id}|${upTo}")
            handler.getOrThrow()(NetworkMessageSerializer().serialize(NetworkMessage("$sys", ack, None<String>)))
        }
    }
}

/** 节点级计数必须等于通道实际保留的条数：任何删除路径漏减都会在这里暴露 */
func budgetConsistent(m: SimpleRemoteManager, unacked: Int64, buffered: Int64): Bool {
    let u = m.reliableBudgetUsage()
    u.unacked == unacked && u.retainedUnacked == unacked && u.inboundBuffered == buffered &&
        u.retainedInboundBuffered == buffered
}

func sinkInbox(kit: ActorTestKit, m: SimpleRemoteManager, inbox: TestInbox<StringMessage>): Unit {
    let ref = kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ _, msg =>
        inbox.tell(msg)
        TypedBehaviors.same<StringMessage>()
    }), "sink")
    m.exposeActor("sink", ref.toClassic().getOrThrow())
}

@Test
public class RemoteNodeBudgetTest {
    @TestCase
    public func nodeBudgetValidatesAndSurvivesBuilders(): Unit {
        let s = RemotingSettings.defaults().withNodeBudget(10, 20).withChannelLimits(3, 4).withAskLimit(5)
            .withPeerLimit(6).withLimits(7, Duration.minute).withSecureCookie("cookie")
        @Expect(s.maxTotalUnacked, 10)
        @Expect(s.maxInboundBuffered, 20)
        @Expect(RemotingSettings.defaults().maxTotalUnacked, 200000)
        @Expect(RemotingSettings.defaults().maxInboundBuffered, 200000)
        @ExpectThrows[IllegalArgumentException](s.withNodeBudget(0, 1))
        @ExpectThrows[IllegalArgumentException](s.withNodeBudget(1, 0))
        @ExpectThrows[IllegalArgumentException](s.withNodeBudget(-1, 1))
    }

    @TestCase
    public func totalUnackedCapsAllChannelsAndAckOrQuarantineReturnsBudget(): Unit {
        let wire = NodeBudgetWire()
        let m = nodeWith(wire, RemotingSettings.defaults().withNodeBudget(10, 100))
        let address = NetworkAddress("127.0.0.1", 2)
        try {
            // 5 个通道各 4 条：每通道远低于 maxUnacked，节点合计只放行 10 条
            for (_ in 0..4) {
                for (t in 0..5) {
                    m.tellReliable(m.remoteActorOf(address, "t${t}"), StringMessage("m"))
                }
            }
            @Expect(m.reliableDeliveryStats().unacked, 10)
            @Expect(m.reliableDeliveryStats().nodeBudgetDropped, 10)
            @Expect(m.reliableDeliveryStats().overflowDropped, 10)
            @Expect(budgetConsistent(m, 10, 0), true)
            // ACK 归还预算：之后可以重新占满
            wire.ackAll()
            @Expect(awaitCondition(T5S, { => m.reliableDeliveryStats().unacked == 0 }), true)
            @Expect(budgetConsistent(m, 0, 0), true)
            for (_ in 0..12) {
                m.tellReliable(m.remoteActorOf(address, "t0"), StringMessage("again"))
            }
            @Expect(m.reliableDeliveryStats().unacked, 10)
            @Expect(m.reliableDeliveryStats().nodeBudgetDropped, 12)
            // 隔离丢弃通道（dropChannelsTo）同样归还
            m.quarantine(address, "test")
            @Expect(m.reliableDeliveryStats().unacked, 0)
            @Expect(budgetConsistent(m, 0, 0), true)
        } finally { m.stop() }
    }

    @TestCase
    public func giveUpReturnsNodeBudget(): Unit {
        let wire = NodeBudgetWire()
        let s = RemotingSettings.defaults().withLimits(100, Duration.millisecond * 100).withNodeBudget(10, 100)
        let m = nodeWith(wire, s)
        let address = NetworkAddress("127.0.0.1", 2)
        try {
            for (t in 0..15) {
                m.tellReliable(m.remoteActorOf(address, "t${t % 3}"), StringMessage("m"))
            }
            @Expect(m.reliableDeliveryStats().nodeBudgetDropped, 5)
            // 无人确认：超过 giveUpAfter 后放弃全部通道，节点预算回到 0
            @Expect(awaitCondition(T5S, { => m.reliableDeliveryStats().gaveUp == 10 }), true)
            @Expect(m.reliableDeliveryStats().unacked, 0)
            @Expect(budgetConsistent(m, 0, 0), true)
        } finally { m.stop() }
    }

    @TestCase
    public func inboundBufferedIsCappedNodeWideAndDrainReturnsBudget(): Unit {
        let kit = ActorTestKit("node-budget-inbound")
        let wire = NodeBudgetWire()
        let m = nodeWith(wire, RemotingSettings.defaults().withNodeBudget(1000, 5))
        let inbox = TestInbox<StringMessage>("node-budget-inbound")
        try {
            sinkInbox(kit, m, inbox)
            // 两个通道各 3 条乱序：每通道远低于上限，节点合计只缓存 5 条（B 的 4 被拒）
            for (seq in 2..5) {
                wire.inject("A#1", seq, "A${seq}")
                wire.inject("B#1", seq, "B${seq}")
            }
            @Expect(m.reliableDeliveryStats().inboundBuffered, 5)
            @Expect(budgetConsistent(m, 0, 5), true)
            // 缺口补上：按序取出归还预算
            wire.inject("A#1", 1, "A1")
            @Expect(budgetConsistent(m, 0, 2), true)
            wire.inject("B#1", 1, "B1")
            @Expect(budgetConsistent(m, 0, 0), true)
            // 被节点上限拒绝的那条由发送端重发后照常按序收下
            wire.inject("B#1", 4, "B4")
            @Expect(awaitCondition(T5S, { => inbox.size() == 8 }), true)
            // 同前缀换代：旧通道的乱序缓冲回收、预算归还
            wire.inject("G#1", 3, "G-old")
            @Expect(budgetConsistent(m, 0, 1), true)
            wire.inject("G#2", 1, "G-new")
            @Expect(budgetConsistent(m, 0, 0), true)
            @Expect(awaitCondition(T5S, { => inbox.size() == 9 }), true)
        } finally { m.stop(); kit.shutdown() }
    }

    @TestCase
    public func peerRestartReclaimsOnlyQuarantinedGenerationAndRejectsLateEnvelopes(): Unit {
        let kit = ActorTestKit("node-budget-restart")
        let wire = NodeBudgetWire()
        let m = nodeWith(wire, RemotingSettings.defaults())
        let inbox = TestInbox<StringMessage>("node-budget-restart")
        try {
            sinkInbox(kit, m, inbox)
            @Expect(wire.admit(2, 1), true)
            wire.inject("127.0.0.1:2/sink/1#1", 1, "old-1")
            wire.inject("127.0.0.1:2/sink/1#1", 3, "old-3")
            // 同地址上一个从未握手、也未被隔离的启动标识：不能证明它的在途信封会被拒，去重状态必须保留
            wire.inject("127.0.0.1:2/sink/9#1", 1, "unknown-1")
            wire.inject("127.0.0.1:2/sink/9#1", 3, "unknown-3")
            // 当前标识未知的地址：隔离只记下实例，不回收它的通道
            wire.inject("127.0.0.1:3/sink/7#1", 2, "other-2")
            m.quarantine(NetworkAddress("127.0.0.1", 3), 7, "removed")
            @Expect(awaitCondition(T5S, { => inbox.size() == 2 }), true)
            @Expect(m.reliableChannelStats().inboundChannels, 3)
            @Expect(budgetConsistent(m, 0, 3), true)
            // 对端重启：旧标识 1 被隔离、当前标识 2 已知 -> 只回收标识 1 的通道
            @Expect(wire.admit(2, 2), true)
            @Expect(m.reliableChannelStats().inboundChannels, 2)
            @Expect(budgetConsistent(m, 0, 2), true)
            // 旧实例迟到/重放的信封一律拒绝，绝不再投
            wire.inject("127.0.0.1:2/sink/1#1", 2, "old-2")
            wire.inject("127.0.0.1:2/sink/1#1", 1, "old-1-replay")
            wire.inject("127.0.0.1:2/sink/1#2", 1, "old-gen2")
            wire.inject("127.0.0.1:3/sink/7#1", 1, "other-1")
            @Expect(m.reliableDeliveryStats().staleGenerationRejected, 4)
            // 未回收的通道仍按去重水位丢弃重放
            wire.inject("127.0.0.1:2/sink/9#1", 1, "unknown-1-replay")
            wire.inject("127.0.0.1:2/sink/2#1", 1, "new-1")
            @Expect(awaitCondition(T5S, { => inbox.size() == 3 }), true)
            @Expect(inbox.receiveMessage().getContent(), "old-1")
            @Expect(inbox.receiveMessage().getContent(), "unknown-1")
            @Expect(inbox.receiveMessage().getContent(), "new-1")
            @Expect(m.reliableChannelStats().inboundChannels, 3)
            @Expect(budgetConsistent(m, 0, 2), true)
        } finally { m.stop(); kit.shutdown() }
    }
}
