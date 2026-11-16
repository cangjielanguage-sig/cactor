/*
 * CActor - StreamRef 可靠模式（DA7，StreamRefSettings.reliable）
 *
 * 覆盖：
 *   - 默认关闭：设置默认值、协议版本与既有行为（含本进程流仍走单次挂接）不变；
 *   - 版本互通：可靠消费方 ↔ 旧生产方、旧消费方 ↔ 可靠生产方 都是**明确拒绝**（Rejected）；
 *   - 真实回环 TCP + 双向黑洞：短暂断连后按恢复令牌重新挂接、从 lastAck+1 重发，
 *     消费方去重，最终无丢失无重复；断连期间生产方保留量不超过 bufferSize；
 *   - 重连窗口超时：消费方以 StreamRefReconnectTimeoutException 终止，不会永远挂着；
 *   - 协议级去重：手写可靠生产方故意重发同一序号，消费方丢弃重复并正常完成。
 *
 * 等待一律用 awaitCondition / EventRecorder；黑洞恢复用例把节点故障判定的容忍调长
 * （30s），只制造"短暂断连"而不会让远程层隔离对端——这是可靠模式要覆盖的窗口。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.sync.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.*
import cactor_foundation.network.{NetworkAddress, TransportProtocol, BlackholeTransport, TcpTransport}
import cactor_remote.*
import cactor_cluster.streamref.*
import cactor_testkit.*

/** 心跳 50ms、容忍 30s：测试里的黑洞时长远小于故障判定，节点不会被隔离 */
let SR_RELIABLE_REMOTING = RemotingSettings(Duration.millisecond * 50, 8.0, Duration.second * 30,
    Duration.millisecond * 50, Duration.millisecond * 10)

/** 带黑洞注入的 StreamRef 节点（真实 TCP） */
class SrReliableNode {
    let kit: ActorTestKit
    let transport: BlackholeTransport
    let remote: SimpleRemoteManager
    let refs: StreamRefs

    init(name: String) {
        kit = ActorTestKit(name)
        transport = BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", 0)))
        remote = SimpleRemoteManager(transport)
        remote.configure(SR_RELIABLE_REMOTING)
        remote.start()
        refs = StreamRefs(kit.system, remote: Some(remote))
    }

    func addr(): NetworkAddress {
        remote.getLocalAddress()
    }

    /** 双向黑洞：出站方向各自丢弃，等价于拔掉这条网线 */
    func partition(other: SrReliableNode): Unit {
        transport.blackhole(other.addr())
        other.transport.blackhole(addr())
    }

    func healAll(): Unit {
        transport.healAll()
    }

    func close(): Unit {
        remote.stop()
        kit.shutdown()
    }
}

/** 可靠模式设置：窗口 4s → 恢复重试间隔 1s */
func srReliableSettings(): StreamRefSettings {
    StreamRefSettings().withBufferSize(4).withReliable(true).withReconnectWindow(Duration.second * 4)
}

@Test
public class StreamRefReliableTest {
    /** 默认关闭：字段默认值、版本常量与 withX 的保留语义；本进程流不启用可靠路径 */
    @TestCase
    public func reliableDefaultsOffAndLocalStreamsUnchanged(): Unit {
        let d = StreamRefSettings()
        @Expect(d.reliable, false)
        @Expect(d.reconnectWindow > Duration.Zero, true)
        @Expect(d.withReliable(true).reliable, true)
        @Expect(d.withBufferSize(3).reliable, false)
        @Expect(d.withReliable(true).withBufferSize(3).reliable, true)
        @Expect(d.withReliable(true).withReconnectWindow(Duration.second * 2).reliable, true)
        @Expect(STREAM_REF_PROTOCOL_VERSION, 1)
        @Expect(STREAM_REF_PROTOCOL_VERSION_RELIABLE, 2)

        // 本进程的 SourceRef 没有传输可断：即使 reliable=true 也按单次挂接运行
        let kit = ActorTestKit("sr-rel-local")
        try {
            let refs = StreamRefs(kit.system)
            let settings = StreamRefSettings().withReliable(true).withBufferSize(4)
            let (ref, producer) = refs.sourceRef<String>(settings: settings)
            let rec = EventRecorder()
            refs.subscribe<String>(ref, SrSubscriber(rec, Int64.Max, 0), settings: settings)
            @Expect(srOfferAll(producer, "e", 1, 5), true)
            producer.complete()
            @Expect(rec.await("complete", SR_WAIT), true)
            @Expect(srExpectAll(rec, "e", 5), true)
            @Expect(producer.retainedCount(), 0)
        } finally {
            kit.shutdown()
        }
    }

    /** 可靠消费方 ↔ 旧生产方：旧生产方明确 Rejected，而不是半可靠地跑起来 */
    @TestCase
    public func reliableConsumerRejectedByLegacyProducer(): Unit {
        let a = SrReliableNode("sr-rel-oldp-a")
        let b = SrReliableNode("sr-rel-oldp-b")
        try {
            let (refB, producer) = b.refs.sourceRef<String>() // reliable 默认 false
            let rec = EventRecorder()
            a.refs.subscribe<String>(SourceRef<String>.fromWireString(refB.toWireString()), SrSubscriber(rec, Int64.Max, 0),
                settings: StreamRefSettings().withReliable(true).withReconnectWindow(Duration.second * 2))
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:rejected:") }), true)
            let err = rec.snapshot()[rec.size() - 1]
            // 版本 2 探测用旧帧发出：旧/非可靠生产方明确以版本不符拒绝，而不是等订阅超时
            @Expect(err.contains("协议版本不符"), true)
            @Expect(rec.contains("complete"), false)
            @Expect(producer.sentCount(), 0)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 旧消费方 ↔ 可靠生产方：可靠生产方要求版本 2，明确拒绝版本 1 的挂接 */
    @TestCase
    public func legacyConsumerRejectedByReliableProducer(): Unit {
        let a = SrReliableNode("sr-rel-oldc-a")
        let b = SrReliableNode("sr-rel-oldc-b")
        try {
            let (refB, producer) = b.refs.sourceRef<String>(settings: srReliableSettings())
            let rec = EventRecorder()
            // 消费方默认 reliable=false → 发送 Subscribe(version 1)
            a.refs.subscribe<String>(SourceRef<String>.fromWireString(refB.toWireString()), SrSubscriber(rec, Int64.Max, 0))
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:rejected:") }), true)
            @Expect(rec.snapshot()[rec.size() - 1].contains("可靠模式需要协议版本 2"), true)
            @Expect(producer.sentCount(), 0)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 真实 TCP 双向黑洞：短暂断连 → 恢复挂接 → 从 lastAck+1 重发，无丢失无重复 */
    @TestCase
    public func reliableRecoversFromBlackholeWithoutLossOrDuplicate(): Unit {
        let a = SrReliableNode("sr-rel-rec-a")
        let b = SrReliableNode("sr-rel-rec-b")
        try {
            let settings = srReliableSettings()
            let (refB, producer) = b.refs.sourceRef<String>(settings: settings)
            let rec = EventRecorder()
            let sub = a.refs.subscribe<String>(SourceRef<String>.fromWireString(refB.toWireString()),
                SrSubscriber(rec, Int64.Max, 0), settings: settings)
            // 先正常走 6 个
            @Expect(srOfferAll(producer, "e", 1, 6), true)
            @Expect(awaitCondition(SR_WAIT, { => sub.deliveredCount() >= 6 }), true)

            // 双向黑洞：元素、ACK、Demand、心跳全部丢
            a.partition(b)
            @Expect(awaitCondition(SR_WAIT, { => a.transport.droppedCount() > 0 && b.transport.droppedCount() > 0 }), true)
            // 黑洞期间继续生产到背压：生产方缓冲 + 保留都受 bufferSize 约束
            let next = AtomicInt64(7)
            srFill(producer, "e", next)
            @Expect(producer.retainedCount() <= 4, true)
            @Expect(producer.bufferedCount() <= 4, true)
            // 消费方应已发起（被丢弃的）恢复挂接，但还没有失败——还在重连窗口内
            @Expect(awaitCondition(SR_WAIT, { => a.transport.droppedCount() >= 20 }), true)
            @Expect(srCountPrefix(rec, "error:"), 0)

            // 恢复：下一次恢复挂接会到达生产方，它按 lastAck 重发保留元素
            a.healAll()
            b.healAll()
            @Expect(srOfferAll(producer, "e", next.load(), 20), true)
            producer.complete()
            @Expect(rec.await("complete", SR_WAIT), true)
            // 恰好 1..20 各一次 + 一个 complete（去重生效，没有重复元素）
            @Expect(srExpectAll(rec, "e", 20), true)
            @Expect(producer.termination().get(SR_WAIT).isCompleted(), true)
            @Expect(producer.retainedCount(), 0)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 重连窗口超时：消费方以 ReconnectTimeout 终止，不会永远挂着 */
    @TestCase
    public func reconnectWindowExpiryFailsConsumer(): Unit {
        let a = SrReliableNode("sr-rel-exp-a")
        let b = SrReliableNode("sr-rel-exp-b")
        try {
            let settings = StreamRefSettings().withBufferSize(4).withReliable(true)
                .withReconnectWindow(Duration.millisecond * 600)
            let (refB, producer) = b.refs.sourceRef<String>(settings: settings)
            let rec = EventRecorder()
            let sub = a.refs.subscribe<String>(SourceRef<String>.fromWireString(refB.toWireString()),
                SrSubscriber(rec, Int64.Max, 0), settings: settings)
            @Expect(srOfferAll(producer, "e", 1, 6), true)
            @Expect(awaitCondition(SR_WAIT, { => sub.deliveredCount() >= 6 }), true)

            let started = MonoTime.now()
            a.partition(b)
            @Expect(awaitCondition(Duration.second * 10, { => srHasPrefix(rec, "error:") }), true)
            // 不会在第一次探测就失败：至少等满重连窗口
            @Expect(MonoTime.now() - started >= Duration.millisecond * 500, true)
            let err = rec.snapshot()[rec.size() - 1]
            @Expect(err.contains("重连窗口"), true)
            @Expect(srCountPrefix(rec, "error:"), 1)
            @Expect(rec.contains("complete"), false)
            // 生产方不会把"消费方失败"误判成完成（它等不到恢复，最终由自身重连窗口/属主停止收尾）
            @Expect(producer.termination().isCompleted(), false)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 协议级去重：手写可靠生产方故意重发 #2，消费方应当丢弃重复并正常完成 */
    @TestCase
    public func consumerDeduplicatesResentSequenceAcrossTcp(): Unit {
        let a = SrReliableNode("sr-rel-dup-a")
        let b = SrReliableNode("sr-rel-dup-b")
        try {
            let fakeRec = EventRecorder()
            let fake = b.kit.spawnTyped<StreamRefMsg>(srReliableFakeProducer(b.remote, fakeRec), "rel-dup-producer")
            b.remote.exposeActor("rel-dup-producer", fake.toClassic().getOrThrow())
            let location = encodeRemoteSender(b.addr(), "rel-dup-producer")
            let rec = EventRecorder()
            a.refs.subscribe<String>(SourceRef<String>("rel-dup-1", location, None<TypedActorRef<StreamRefMsg>>),
                SrSubscriber(rec, Int64.Max, 0), settings: srReliableSettings())
            @Expect(rec.await("complete", SR_WAIT), true)
            let got = rec.snapshot()
            @Expect(got.size, 4)
            @Expect(got[0] == "next:e1" && got[1] == "next:e2" && got[2] == "next:e3" && got[3] == "complete", true)
            // 重复 #2 被去重，但累计确认仍推进到 3
            @Expect(awaitCondition(SR_WAIT, { => fakeRec.contains("ack:3") }), true)
            @Expect(awaitCondition(SR_WAIT, { => fakeRec.contains("ack-complete") }), true)
        } finally {
            a.close()
            b.close()
        }
    }
}

/** 手写可靠生产方的可变状态（仓颉不允许把捕获 var 的闭包存起来，用对象承载） */
class SrReliableFakeState {
    var consumer: Option<TypedActorRef<StreamRefMsg>> = None<TypedActorRef<StreamRefMsg>>
    var sent: Bool = false
}

/** 手写可靠生产方：按序发 1,2,2,3（故意重复）后 Completed(3)；只服务一轮需求 */
func srReliableFakeProducer(manager: SimpleRemoteManager, rec: EventRecorder): TypedBehavior<StreamRefMsg> {
    TypedBehaviors.setup<StreamRefMsg>({ _ =>
        let state = SrReliableFakeState()
        TypedBehaviors.receive<StreamRefMsg>({ _, msg =>
            match (msg) {
                case StreamRefMsg.Subscribe(sid, loc, ver) =>
                    // 版本 2 探测：回 ProbeOk，等消费方发带令牌的 SubscribeReliable
                    rec.record("probe:${ver}")
                    if (ver == STREAM_REF_PROTOCOL_VERSION_RELIABLE) {
                        match (parseRemoteSender(loc)) {
                            case Some((addr, name)) =>
                                manager.typedRemoteActorOf<StreamRefMsg>(addr, name) << StreamRefMsg.ReliableProbeOk(sid)
                            case None => ()
                        }
                    }
                case StreamRefMsg.SubscribeReliable(sid, loc, ver, token, lastAck) =>
                    rec.record("subscribe:${ver}:${lastAck}")
                    match (parseRemoteSender(loc)) {
                        case Some((addr, name)) =>
                            let ref = manager.typedRemoteActorOf<StreamRefMsg>(addr, name)
                            state.consumer = Some(ref)
                            ref << StreamRefMsg.OnSubscribe(sid)
                        case None => ()
                    }
                case StreamRefMsg.Demand(sid, upTo) =>
                    rec.record("demand:${upTo}")
                    if (!state.sent) {
                        match (state.consumer) {
                            case Some(c) =>
                                state.sent = true
                                c << StreamRefMsg.Element(sid, 1, "e1")
                                c << StreamRefMsg.Element(sid, 2, "e2")
                                c << StreamRefMsg.Element(sid, 2, "e2") // 重复序号
                                c << StreamRefMsg.Element(sid, 3, "e3")
                                c << StreamRefMsg.Completed(sid, 3)
                            case None => ()
                        }
                    }
                case StreamRefMsg.Ack(_, seq) => rec.record("ack:${seq}")
                case StreamRefMsg.AckCompleted(_) => rec.record("ack-complete")
                case StreamRefMsg.Cancel(_, reason) => rec.record("cancel:${reason}")
                case _ => ()
            }
            TypedBehaviors.same<StreamRefMsg>()
        })
    })
}
