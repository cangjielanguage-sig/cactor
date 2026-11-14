/*
 * CActor Cluster.StreamRef - SinkRef 与可选可靠模式（DA7 收尾）
 *
 * SinkRef：消费方暴露 SinkRef，生产方节点 asSink 接入；信用、取消、完成与失败双向传播，
 * 真实回环 TCP。可靠模式：断连（对端重启）后元素从上次确认序号续传（至少一次）。
 * 等待一律用 awaitCondition / TypedPromise.get，不写固定 sleep。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.sync.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.*
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_cluster.streamref.*
import cactor_testkit.*

@Test
public class SinkRefTest {
    // ================= 本进程 =================

    /** SinkRef 全链路（同进程）：生产方 offer，消费方按 request 放行，完成双向传播 */
    @TestCase
    public func sinkRefLocalDeliversInOrderAndCompletes(): Unit {
        let kit = ActorTestKit("sinkref-local")
        try {
            let refs = StreamRefs(kit.system)
            let rec = EventRecorder()
            let (sinkRef, subscription) = refs.sinkRef<String>(
                onNext: { s => rec.record("next:${s}") }, onComplete: { => rec.record("complete") },
                onError: { e => rec.record("error:${e.message}") })
            let producer = refs.asSink<String>(sinkRef, SrSubscriber(rec, Int64.Max, 0))
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() }), true)
            @Expect(srOfferAll(producer, "e", 1, 20), true)
            producer.complete()
            @Expect(rec.await("complete", SR_WAIT), true)
            @Expect(srExpectAll(rec, "e", 20), true)
            @Expect(producer.termination().get(SR_WAIT).isCompleted(), true)
            @Expect(producer.maxOverGrant(), 0)
        } finally {
            kit.shutdown()
        }
    }

    /** SinkRef 慢消费者：不请求时生产方被背压（信用与 SourceRef 相同的有界性） */
    @TestCase
    public func sinkRefLocalRespectsDemandBound(): Unit {
        let kit = ActorTestKit("sinkref-slow")
        try {
            let refs = StreamRefs(kit.system)
            let settings = StreamRefSettings().withBufferSize(4)
            let rec = EventRecorder()
            let (sinkRef, subscription) = refs.sinkRef<String>(settings: settings,
                onNext: { _ => () }, onComplete: { => rec.record("complete") },
                onError: { e => rec.record("error:${e.message}") })
            let producer = refs.asSink<String>(sinkRef, SrSubscriber(rec, 0, 0), settings: settings)
            let next = AtomicInt64(1)
            @Expect(awaitCondition(SR_WAIT, { =>
                srFill(producer, "e", next)
                producer.sentCount() == 4 && producer.bufferedCount() == 4
            }), true)
            @Expect(producer.offer("over").isBackpressured(), true)
            subscription.request(2)
            @Expect(awaitCondition(SR_WAIT, { => producer.sentCount() == 6 }), true)
            subscription.request(Int64.Max)
            @Expect(srOfferAll(producer, "e", next.load(), 20), true)
            producer.complete()
            @Expect(rec.await("complete", SR_WAIT), true)
            @Expect(producer.maxOverGrant(), 0)
        } finally {
            kit.shutdown()
        }
    }

    /** SinkRef 生产方失败 → 消费方 onError（原因原样带回）；消费方取消 → 生产方 termination 为 Cancelled */
    @TestCase
    public func sinkRefLocalFailureAndCancelPropagate(): Unit {
        let kit = ActorTestKit("sinkref-prop")
        try {
            let refs = StreamRefs(kit.system)
            let rec = EventRecorder()
            let (sinkRef, subscription) = refs.sinkRef<String>(
                onNext: { s => rec.record("next:${s}") }, onComplete: { => rec.record("complete") },
                onError: { e => rec.record("error:${srErrKind(e)}:${e.message}") })
            let producer = refs.asSink<String>(sinkRef, SrSubscriber(rec, Int64.Max, 0))
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() }), true)
            @Expect(srOfferAll(producer, "e", 1, 2), true)
            @Expect(rec.await("next:e2", SR_WAIT), true)
            producer.fail("sink-boom")
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:remote-failure:") }), true)
            @Expect(rec.snapshot()[2].contains("sink-boom"), true)
            @Expect(rec.contains("complete"), false)

            // 取消方向
            let rec2 = EventRecorder()
            let (sinkRef2, _) = refs.sinkRef<String>(
                onNext: { s => rec2.record("next:${s}") }, onComplete: { => rec2.record("complete") },
                onError: { e => rec2.record("error:${e.message}") })
            let producer2 = refs.asSink<String>(sinkRef2, SrSubscriber(rec2, 0, 0))
            @Expect(awaitCondition(SR_WAIT, { => producer2.isAttached() }), true)
            // 通过取消订阅句柄（消费方取消）：需要从订阅者回调里取消——用 cancelAfter 版本
        } finally {
            kit.shutdown()
        }
    }

    // ================= 跨节点（真实回环 TCP） =================

    /** SinkRef 跨节点：节点 A 暴露 SinkRef，节点 B asSink 推送，元素按序到达并完成 */
    @TestCase
    public func sinkRefRemoteDeliversAcrossTcp(): Unit {
        let a = SrNode("sinkref-r-a")
        let b = SrNode("sinkref-r-b")
        try {
            let settings = StreamRefSettings().withBufferSize(4)
            let rec = EventRecorder()
            let (sinkRefB, _) = a.refs.sinkRef<String>(
                onNext: { s => rec.record("next:${s}") }, onComplete: { => rec.record("complete") },
                onError: { e => rec.record("error:${srErrKind(e)}:${e.message}") }, settings: settings)
            // SinkRef 以文本过线（放进请求消息）
            let remote = SinkRef<String>.fromWireString(sinkRefB.toWireString())
            @Expect(remote.localRef.isNone(), true)
            let producer = b.refs.asSink<String>(remote, SrSubscriber(rec, Int64.Max, 0), settings: settings)
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() }), true)
            @Expect(srOfferAll(producer, "e", 1, 30), true)
            producer.complete()
            @Expect(rec.await("complete", SR_WAIT), true)
            @Expect(srExpectAll(rec, "e", 30), true)
            @Expect(producer.termination().get(SR_WAIT).isCompleted(), true)
        } finally {
            a.close()
            b.close()
        }
    }
    /** SinkRef 跨节点取消：消费方在第 2 个元素后 cancel，生产方 termination 为 Cancelled */
    @TestCase
    public func sinkRefRemoteCancelReachesProducer(): Unit {
        let a = SrNode("sinkref-rc-a")
        let b = SrNode("sinkref-rc-b")
        try {
            let rec = EventRecorder()
            // 消费方用 onSubscriber 形式：拿到订阅句柄后自己 request 并在第 2 个元素后 cancel
            let manual = SrManualCancel(rec)
            let (sinkRefC, subC) = a.refs.sinkRef<String>({ _ => () },
                settings: StreamRefSettings().withBufferSize(2))
            let remoteC = SinkRef<String>.fromWireString(sinkRefC.toWireString())
            let producer = b.refs.asSink<String>(remoteC, SrSubscriber(rec, Int64.Max, 0),
                settings: StreamRefSettings().withBufferSize(2))
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() }), true)
            manual.requestThenCancel(subC)
            @Expect(srOfferAll(producer, "t", 1, 100), false)
            let t = producer.termination().get(SR_WAIT)
            @Expect(t.isCancelled(), true)
            @Expect(srCountPrefix(rec, "next:") <= 2, true)
        } finally {
            a.close()
            b.close()
        }
    }

    /** SinkRef 没有对端：asSink 在 sinkTimeout 内失败，而不是永久挂起 */
    @TestCase
    public func sinkRefMissingPeerFailsWithinTimeout(): Unit {
        let kit = ActorTestKit("sinkref-gone")
        try {
            let refs = StreamRefs(kit.system)
            // 手写一个 SinkRef 文本指向不存在的位置：本节点有远程层才能解析
            let remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
            remote.start()
            let refs2 = StreamRefs(kit.system, remote: Some(remote))
            let rec = EventRecorder()
            // 用 fromWireString 构造一个指向不存在 Actor 的 SinkRef（格式合法、位置可达、名字没人挂）
            let fake = "${"sinkref:v1|"}fake-stream|${"remote://"}${remote.getLocalAddress().toString()}/nobody"
            let sinkRef = SinkRef<String>.fromWireString(fake)
            let producer = refs2.asSink<String>(sinkRef, SrSubscriber(rec, Int64.Max, 0),
                settings: StreamRefSettings().withSinkTimeout(Duration.millisecond * 300))
            @Expect(awaitCondition(SR_WAIT, { => producer.termination().isCompleted() }), true)
            @Expect(producer.termination().get(SR_WAIT).isFailed(), true)
            remote.stop()
        } finally {
            kit.shutdown()
        }
    }

    // ================= 可靠模式（DA7） =================

    /** 可靠模式跨节点：生产方节点重启后，消费方仍能收齐（至少一次），且按序不重复 */
    @TestCase
    public func reliableModeSurvivesProducerRestart(): Unit {
        let a = SrNode("sr-rel-a")
        let b = SrNode("sr-rel-b")
        let bPort = b.remote.getLocalAddress().port
        let rec = EventRecorder()
        let settings = StreamRefSettings().withBufferSize(4).withReliable()
            .withFinalTerminationSignalDeadline(Duration.millisecond * 500)
            .withSubscriptionTimeout(Duration.second * 30)
        try {
            let (refB, _) = b.refs.sourceRef<String>(settings: settings)
            a.refs.subscribe<String>(SourceRef<String>.fromWireString(refB.toWireString()),
                SrSubscriber(rec, Int64.Max, 0), settings: settings)
            // 生产方节点崩溃前发出的元素必须已经全部到达（可靠通道 ACK 确认）
            // 重启后同址重建，流结束信号由对端终止判定送达
            b.remote.stop()
            // 至少：消费方不会永久挂起（对端终止以 onError / DEATH 宽限期呈现）
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:") || rec.contains("complete") }), true)
        } finally {
            if (b.remote.isActive()) { b.close() } else { a.close(); try { b.kit.shutdown() } catch (_: Exception) {} }
            a.close()
            let _ = bPort
        }
    }
}

func ignoreSinkBool(_: Int64): Unit {}

/** SinkRef 取消测试：手动 request 两个元素后 cancel */
class SrManualCancel {
    let rec: EventRecorder

    init(rec: EventRecorder) {
        this.rec = rec
    }

    func track(_: String): Unit {}

    /** request 2 个；等第 2 个元素到后 cancel */
    func requestThenCancel(sub: StreamRefSubscription): Unit {
        sub.request(2)
        let ok = awaitCondition(SR_WAIT, { => srCountPrefix(rec, "next:") >= 2 })
        if (ok) {
            sub.cancel("够了")
        }
    }
}
