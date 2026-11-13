/*
 * CActor - 类型化流式回复（StreamRef，E61 / DA7）
 *
 * 逐条验证协议保证（功能断言，不是压测）：
 *   - 需求上界：慢消费者不请求时，生产方只发出预取窗口内的元素、offer 返回 Backpressured，
 *     sent ≤ granted、在途 + 消费方缓冲 ≤ bufferSize；
 *   - 顺序与完成：元素按序到达，恰好一个 onComplete；
 *   - 缺号 / 重复 / 超出需求：大声失败并通知生产方取消（用手写协议端点注入）；
 *   - 生产方失败、属主停止、生产方节点停止 → 消费方 onError；
 *   - 消费方取消 / 消费方节点停止 → 生产方 termination() 为 Cancelled（LLM 调用可据此中止）；
 *   - 订阅超时、空闲超时、单次挂接；
 *   - 真实回环 TCP 跨节点。
 * 等待一律用 awaitCondition / EventRecorder，不写固定 sleep。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.sync.*
import std.collection.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{ActorRef}
import cactor_core.typed.*
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_cluster.streamref.*
import cactor_testkit.*

let SR_WAIT = Duration.second * 15
// 节点故障检测调快（心跳 50ms、容忍 500ms）：节点停止类用例不必等默认的 10s 容忍停顿
let SR_REMOTING = RemotingSettings(Duration.millisecond * 50, 8.0, Duration.millisecond * 500, Duration.millisecond * 50,
    Duration.millisecond * 10)

class SrNode {
    let kit: ActorTestKit
    let remote: SimpleRemoteManager
    let refs: StreamRefs

    init(name: String) {
        kit = ActorTestKit(name)
        remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        remote.configure(SR_REMOTING)
        remote.start()
        refs = StreamRefs(kit.system, remote: Some(remote))
    }

    func close(): Unit {
        remote.stop()
        kit.shutdown()
    }
}

func srErrKind(e: StreamRefException): String {
    match (e) {
        case _: StreamRefSubscriptionTimeoutException => "subscription-timeout"
        case _: StreamRefIdleTimeoutException => "idle-timeout"
        case _: StreamRefProtocolException => "protocol"
        case _: StreamRefRemoteFailureException => "remote-failure"
        case _: StreamRefRejectedException => "rejected"
        case _: StreamRefPeerTerminatedException => "peer-terminated"
        case _: StreamRefSubscriberException => "subscriber"
        case _ => "other"
    }
}

/** 记录全部信号；initialRequest = 0 表示由测试线程手动 request；cancelAfter > 0 时第 n 个元素后取消 */
class SrSubscriber <: StreamRefSubscriber<String> {
    let rec: EventRecorder
    let initialRequest: Int64
    let cancelAfter: Int64
    let count = AtomicInt64(0)
    let subs = ArrayList<StreamRefSubscription>()

    init(rec: EventRecorder, initialRequest: Int64, cancelAfter: Int64) {
        this.rec = rec
        this.initialRequest = initialRequest
        this.cancelAfter = cancelAfter
    }

    public func onSubscribe(subscription: StreamRefSubscription): Unit {
        subs.add(subscription)
        if (initialRequest > 0) {
            subscription.request(initialRequest)
        }
    }

    public func onNext(element: String): Unit {
        rec.record("next:${element}")
        if (cancelAfter > 0 && count.fetchAdd(1) + 1 == cancelAfter) {
            subs[0].cancel("够了")
        }
    }

    public func onComplete(): Unit {
        rec.record("complete")
    }

    public func onError(error: StreamRefException): Unit {
        rec.record("error:${srErrKind(error)}:${error.message}")
    }
}

func srHasPrefix(rec: EventRecorder, prefix: String): Bool {
    for (e in rec.snapshot()) {
        if (e.startsWith(prefix)) {
            return true
        }
    }
    false
}

func srCountPrefix(rec: EventRecorder, prefix: String): Int64 {
    var n: Int64 = 0
    for (e in rec.snapshot()) {
        if (e.startsWith(prefix)) {
            n++
        }
    }
    n
}

/** 期望的 next 序列 + 恰好一个 complete，且没有 error */
func srExpectAll(rec: EventRecorder, prefix: String, n: Int64): Bool {
    let all = rec.snapshot()
    if (all.size != n + 1 || all[n] != "complete") {
        return false
    }
    for (i in 0..n) {
        if (all[i] != "next:${prefix}${i + 1}") {
            return false
        }
    }
    true
}

/** 逐个 offer：Backpressured 时等 whenReady；返回是否全部投出 */
func srOfferAll(producer: StreamProducer<String>, prefix: String, from: Int64, to: Int64): Bool {
    var i = from
    while (i <= to) {
        match (producer.offer("${prefix}${i}")) {
            case OfferResult.Enqueued => i++
            case OfferResult.Backpressured =>
                if (!producer.whenReady().get(SR_WAIT)) {
                    return false
                }
            case OfferResult.Closed => return false
        }
    }
    true
}

/** 一直 offer 到 Backpressured，返回本次投出的个数 */
func srFill(producer: StreamProducer<String>, prefix: String, next: AtomicInt64): Unit {
    while (true) {
        let i = next.load()
        if (producer.offer("${prefix}${i}").isEnqueued()) {
            next.fetchAdd(1)
        } else {
            return
        }
    }
}

/**
 * 手写的生产端：按 seqs 发出元素（可故意缺号/重复/超发），记录收到的 Subscribe / Cancel。
 * reply = false 时不确认挂接（订阅超时）；seqs 为空时确认挂接但永不发元素（空闲超时）。
 */
func srFakeProducer(rec: EventRecorder, seqs: Array<Int64>, reply: Bool): TypedBehavior<StreamRefMsg> {
    TypedBehaviors.setup<StreamRefMsg>({ _ =>
        let consumer = ArrayList<TypedActorRef<StreamRefMsg>>()
        TypedBehaviors.receive<StreamRefMsg>({ _, msg =>
            match (msg) {
                case StreamRefMsg.SubscribeLocal(sid, c, _) =>
                    rec.record("subscribe")
                    consumer.add(c)
                    if (reply) {
                        c << StreamRefMsg.OnSubscribe(sid)
                    }
                case StreamRefMsg.Demand(sid, upTo) =>
                    rec.record("demand:${upTo}")
                    if (!rec.contains("sent") && !consumer.isEmpty()) {
                        rec.record("sent")
                        for (s in seqs) {
                            consumer[0] << StreamRefMsg.Element(sid, s, "e${s}")
                        }
                    }
                case StreamRefMsg.Cancel(_, reason) => rec.record("cancel:${reason}")
                case _ => ()
            }
            TypedBehaviors.same<StreamRefMsg>()
        })
    })
}

@Test
public class StreamRefTest {
    // ================= 本进程 =================

    @TestCase
    public func localOrderAndSingleCompletion(): Unit {
        let kit = ActorTestKit("sr-local-order")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, producer) = refs.sourceRef<String>()
            let rec = EventRecorder()
            refs.subscribe<String>(ref, onNext: { s => rec.record("next:${s}") }, onComplete: { => rec.record("complete") },
                onError: { e => rec.record("error:${e.message}") })
            @Expect(srOfferAll(producer, "e", 1, 50), true)
            producer.complete()
            @Expect(rec.await("complete", SR_WAIT), true)
            @Expect(srExpectAll(rec, "e", 50), true)
            @Expect(producer.termination().get(SR_WAIT).isCompleted(), true)
            @Expect(producer.offer("late").isClosed(), true)
            @Expect(producer.maxOverGrant(), 0)
        } finally {
            kit.shutdown()
        }
    }

    /** 慢消费者：不请求时生产方只发窗口内的元素，offer 被背压；按需放行后按序完成 */
    @TestCase
    public func slowConsumerDemandIsBounded(): Unit {
        let kit = ActorTestKit("sr-local-slow")
        try {
            let refs = StreamRefs(kit.system)
            let settings = StreamRefSettings().withBufferSize(4)
            let (ref, producer) = refs.sourceRef<String>(settings: settings)
            let rec = EventRecorder()
            let sub = refs.subscribe<String>(ref, SrSubscriber(rec, 0, 0), settings: settings)
            srAssertDemandBound(producer, sub, rec)
        } finally {
            kit.shutdown()
        }
    }

    @TestCase
    public func producerFailurePropagatesOnce(): Unit {
        let kit = ActorTestKit("sr-local-fail")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, producer) = refs.sourceRef<String>()
            let rec = EventRecorder()
            refs.subscribe<String>(ref, SrSubscriber(rec, Int64.Max, 0))
            @Expect(srOfferAll(producer, "e", 1, 2), true)
            @Expect(rec.await("next:e2", SR_WAIT), true)
            producer.fail("模型崩了")
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:remote-failure:") }), true)
            @Expect(rec.snapshot()[2].contains("模型崩了"), true)
            @Expect(srCountPrefix(rec, "error:"), 1)
            @Expect(rec.contains("complete"), false)
            let t = producer.termination().get(SR_WAIT)
            @Expect(t.isFailed() && t.reason() == "模型崩了", true)
        } finally {
            kit.shutdown()
        }
    }

    /** 消费方取消：生产方 termination() 变为 Cancelled、offer 返回 Closed、whenReady 为 false；订阅者不再收到信号 */
    @TestCase
    public func consumerCancelReachesProducer(): Unit {
        let kit = ActorTestKit("sr-local-cancel")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, producer) = refs.sourceRef<String>(settings: StreamRefSettings().withBufferSize(2))
            let rec = EventRecorder()
            refs.subscribe<String>(ref, SrSubscriber(rec, Int64.Max, 3), settings: StreamRefSettings().withBufferSize(2))
            // 持续生产直到被告知关闭（模拟 LLM 逐 token 生成，被取消后停下）
            @Expect(srOfferAll(producer, "t", 1, 1000), false)
            let t = producer.termination().get(SR_WAIT)
            @Expect(t.isCancelled(), true)
            @Expect(t.reason(), "够了")
            @Expect(producer.offer("x").isClosed(), true)
            @Expect(producer.whenReady().get(SR_WAIT), false)
            @Expect(rec.count("next:t1") + rec.count("next:t2") + rec.count("next:t3"), 3)
            @Expect(srCountPrefix(rec, "next:"), 3)
            @Expect(srHasPrefix(rec, "error:") || rec.contains("complete"), false)
        } finally {
            kit.shutdown()
        }
    }

    /** 属主（生成元素的 Actor）停止 → 流以 Failed 结束，消费方立即 onError */
    @TestCase
    public func ownerStopFailsConsumer(): Unit {
        let kit = ActorTestKit("sr-local-owner")
        try {
            let refs = StreamRefs(kit.system)
            let owner = kit.spawnTyped<String>(TypedBehaviors.ignore<String>(), "llm-owner")
            let (ref, producer) = refs.sourceRef<String>(owner: owner.toClassic())
            let rec = EventRecorder()
            refs.subscribe<String>(ref, SrSubscriber(rec, Int64.Max, 0))
            @Expect(srOfferAll(producer, "e", 1, 1), true)
            @Expect(rec.await("next:e1", SR_WAIT), true)
            kit.system.stop(owner.toClassic().getOrThrow())
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:remote-failure:") }), true)
            @Expect(producer.termination().get(SR_WAIT).isFailed(), true)
            @Expect(srCountPrefix(rec, "error:"), 1)
        } finally {
            kit.shutdown()
        }
    }

    @TestCase
    public func subscriptionTimeoutOnBothSides(): Unit {
        let kit = ActorTestKit("sr-local-subtimeout")
        try {
            let refs = StreamRefs(kit.system)
            let quick = StreamRefSettings().withSubscriptionTimeout(Duration.millisecond * 200)
            // 生产方：没人挂接
            let (_, producer) = refs.sourceRef<String>(settings: quick)
            let t = producer.termination().get(SR_WAIT)
            @Expect(t.isFailed() && t.reason().contains("subscription timeout"), true)
            @Expect(producer.offer("x").isClosed(), true)
            // 消费方：生产端从不确认挂接
            let fakeRec = EventRecorder()
            let fake = kit.spawnTyped<StreamRefMsg>(srFakeProducer(fakeRec, [], false), "silent-producer")
            let rec = EventRecorder()
            refs.subscribe<String>(SourceRef<String>("fake-silent", "", Some(fake)), SrSubscriber(rec, 1, 0), settings: quick)
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:subscription-timeout:") }), true)
            @Expect(fakeRec.contains("subscribe"), true)
            // 超时后告知生产端取消（它若迟到地开始生产，也会停下）
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(fakeRec, "cancel:") }), true)
        } finally {
            kit.shutdown()
        }
    }

    @TestCase
    public func idleTimeoutFailsAndCancels(): Unit {
        let kit = ActorTestKit("sr-local-idle")
        try {
            let refs = StreamRefs(kit.system)
            let fakeRec = EventRecorder()
            let fake = kit.spawnTyped<StreamRefMsg>(srFakeProducer(fakeRec, [], true), "idle-producer")
            let rec = EventRecorder()
            refs.subscribe<String>(SourceRef<String>("fake-idle", "", Some(fake)), SrSubscriber(rec, 1, 0),
                settings: StreamRefSettings().withIdleTimeout(Duration.millisecond * 200))
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:idle-timeout:") }), true)
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(fakeRec, "cancel:") }), true)
            @Expect(srCountPrefix(rec, "error:"), 1)
        } finally {
            kit.shutdown()
        }
    }

    /** 单次挂接：第二个订阅者被拒绝，第一个不受影响 */
    @TestCase
    public func secondSubscriberIsRejected(): Unit {
        let kit = ActorTestKit("sr-local-double")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, producer) = refs.sourceRef<String>()
            let rec1 = EventRecorder()
            let rec2 = EventRecorder()
            let sub1 = refs.subscribe<String>(ref, SrSubscriber(rec1, Int64.Max, 0))
            @Expect(awaitCondition(SR_WAIT, { => sub1.isSubscribed() }), true)
            refs.subscribe<String>(ref, SrSubscriber(rec2, Int64.Max, 0))
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec2, "error:rejected:") }), true)
            @Expect(srOfferAll(producer, "e", 1, 3), true)
            producer.complete()
            @Expect(rec1.await("complete", SR_WAIT), true)
            @Expect(srExpectAll(rec1, "e", 3), true)
            @Expect(rec2.size(), 1)
        } finally {
            kit.shutdown()
        }
    }

    /** 缺号、重复、超出需求都大声失败，并告诉生产端取消 */
    @TestCase
    public func sequenceViolationsFailLoudly(): Unit {
        let kit = ActorTestKit("sr-local-seq")
        try {
            let refs = StreamRefs(kit.system)
            let cases: Array<(String, Array<Int64>, String)> = [("gap", [1, 3], "缺号"), ("dup", [1, 1], "重复"),
                ("overflow", [1, 2, 3], "超出已授予的需求")]
            for ((name, seqs, expected) in cases) {
                let fakeRec = EventRecorder()
                let fake = kit.spawnTyped<StreamRefMsg>(srFakeProducer(fakeRec, seqs, true), "seq-${name}")
                let rec = EventRecorder()
                // 订阅者不请求：消费方授予的需求固定为窗口 2，第 3 个元素必然超发
                refs.subscribe<String>(SourceRef<String>("fake-${name}", "", Some(fake)), SrSubscriber(rec, 0, 0),
                    settings: StreamRefSettings().withBufferSize(2))
                @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:protocol:") }), true)
                let err = rec.snapshot()[rec.size() - 1]
                @Expect(err.contains(expected), true)
                @Expect(srCountPrefix(rec, "error:"), 1)
                @Expect(rec.contains("complete"), false)
                @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(fakeRec, "cancel:") }), true)
            }
        } finally {
            kit.shutdown()
        }
    }

    // ================= 跨节点（真实回环 TCP） =================

    @TestCase
    public func remoteStreamRespectsDemandAndOrder(): Unit {
        let a = SrNode("sr-remote-a")
        let b = SrNode("sr-remote-b")
        try {
            let settings = StreamRefSettings().withBufferSize(4)
            let (refB, producer) = b.refs.sourceRef<String>(settings: settings)
            // SourceRef 以文本过线（实际使用中放进 ask 回复）
            let refA = SourceRef<String>.fromWireString(refB.toWireString())
            @Expect(refA.localRef.isNone(), true)
            let rec = EventRecorder()
            let sub = a.refs.subscribe<String>(refA, SrSubscriber(rec, 0, 0), settings: settings)
            srAssertDemandBound(producer, sub, rec)
        } finally {
            a.close()
            b.close()
        }
    }

    @TestCase
    public func remoteCancelReachesProducer(): Unit {
        let a = SrNode("sr-rcancel-a")
        let b = SrNode("sr-rcancel-b")
        try {
            let (refB, producer) = b.refs.sourceRef<String>(settings: StreamRefSettings().withBufferSize(2))
            let rec = EventRecorder()
            a.refs.subscribe<String>(SourceRef<String>.fromWireString(refB.toWireString()), SrSubscriber(rec, Int64.Max, 2))
            @Expect(srOfferAll(producer, "t", 1, 1000), false)
            let t = producer.termination().get(SR_WAIT)
            @Expect(t.isCancelled() && t.reason() == "够了", true)
            @Expect(srCountPrefix(rec, "next:"), 2)
            @Expect(srHasPrefix(rec, "error:") || rec.contains("complete"), false)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 生产方节点停止（远程层关闭，节点级心跳判定不可达）→ 消费方 onError，不会永远挂起 */
    @TestCase
    public func producerNodeStopFailsConsumer(): Unit {
        let a = SrNode("sr-rnode-a")
        let b = SrNode("sr-rnode-b")
        try {
            let (refB, producer) = b.refs.sourceRef<String>()
            let rec = EventRecorder()
            let settings = StreamRefSettings().withFinalTerminationSignalDeadline(Duration.millisecond * 200)
            a.refs.subscribe<String>(SourceRef<String>.fromWireString(refB.toWireString()), SrSubscriber(rec, Int64.Max, 0),
                settings: settings)
            @Expect(srOfferAll(producer, "e", 1, 1), true)
            @Expect(rec.await("next:e1", SR_WAIT), true)
            b.remote.stop()
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:peer-terminated:") }), true)
            @Expect(srCountPrefix(rec, "error:"), 1)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 消费方节点停止 → 生产方 termination() 为 Cancelled（上游工作可以中止） */
    @TestCase
    public func consumerNodeStopCancelsProducer(): Unit {
        let a = SrNode("sr-rcnode-a")
        let b = SrNode("sr-rcnode-b")
        try {
            let (refB, producer) = b.refs.sourceRef<String>()
            let rec = EventRecorder()
            a.refs.subscribe<String>(SourceRef<String>.fromWireString(refB.toWireString()), SrSubscriber(rec, Int64.Max, 0))
            @Expect(srOfferAll(producer, "e", 1, 1), true)
            @Expect(rec.await("next:e1", SR_WAIT), true)
            a.remote.stop()
            let t = producer.termination().get(SR_WAIT)
            @Expect(t.isCancelled(), true)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 已结束的生产方：远程监视对未知暴露名立即回 DEATH，消费方远早于订阅超时失败 */
    @TestCase
    public func subscribingToFinishedRemoteProducerFailsPromptly(): Unit {
        let a = SrNode("sr-rgone-a")
        let b = SrNode("sr-rgone-b")
        try {
            let (refB, producer) = b.refs.sourceRef<String>(
                settings: StreamRefSettings().withSubscriptionTimeout(Duration.millisecond * 100))
            @Expect(producer.termination().get(SR_WAIT).isFailed(), true)
            let rec = EventRecorder()
            let started = MonoTime.now()
            a.refs.subscribe<String>(SourceRef<String>.fromWireString(refB.toWireString()), SrSubscriber(rec, 1, 0),
                settings: StreamRefSettings().withSubscriptionTimeout(Duration.second * 60)
                    .withFinalTerminationSignalDeadline(Duration.millisecond * 100))
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:peer-terminated:") }), true)
            @Expect(MonoTime.now() - started < Duration.second * 30, true)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 元素类型无法过线：生产方在发送侧失败并把原因发给消费方，而不是静默丢元素 */
    @TestCase
    public func unencodableElementFailsLoudlyAcrossNodes(): Unit {
        let a = SrNode("sr-renc-a")
        let b = SrNode("sr-renc-b")
        try {
            let (refB, producer) = b.refs.sourceRef<SrOpaque>()
            let rec = EventRecorder()
            a.refs.subscribe<SrOpaque>(SourceRef<SrOpaque>.fromWireString(refB.toWireString()),
                onNext: { _ => rec.record("next") }, onComplete: { => rec.record("complete") },
                onError: { e => rec.record("error:${srErrKind(e)}:${e.message}") })
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() }), true)
            @Expect(producer.offer(SrOpaque()).isEnqueued(), true)
            @Expect(awaitCondition(SR_WAIT, { => srHasPrefix(rec, "error:remote-failure:") }), true)
            @Expect(rec.snapshot()[0].contains("发送失败"), true)
            @Expect(producer.termination().get(SR_WAIT).isFailed(), true)
            @Expect(rec.contains("next"), false)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 注册了 TypedPayloadCodecs 的元素类型跨节点传输 */
    @TestCase
    public func registeredElementTypeCrossesNodes(): Unit {
        if (TypedPayloadCodecs.find("tests.SrToken").isNone()) {
            TypedPayloadCodecs.register<SrToken>(typeName: "tests.SrToken",
                encode: { t => "${t.index}|${t.text}".toArray() },
                decode: { b =>
                    let s = String.fromUtf8(b)
                    let i = s.indexOf("|").getOrThrow()
                    SrToken(Int64.parse(s[0..i]), s[(i + 1)..])
                })
        }
        let a = SrNode("sr-rtok-a")
        let b = SrNode("sr-rtok-b")
        try {
            let (refB, producer) = b.refs.sourceRef<SrToken>()
            let rec = EventRecorder()
            a.refs.subscribe<SrToken>(SourceRef<SrToken>.fromWireString(refB.toWireString()),
                onNext: { t => rec.record("${t.index}:${t.text}") }, onComplete: { => rec.record("complete") },
                onError: { e => rec.record("error:${e.message}") })
            for (i in 1..=3) {
                @Expect(producer.offer(SrToken(i, "tok${i}")).isEnqueued(), true)
            }
            producer.complete()
            @Expect(rec.await("complete", SR_WAIT), true)
            let got = rec.snapshot()
            @Expect(got.size, 4)
            @Expect(got[0] == "1:tok1" && got[1] == "2:tok2" && got[2] == "3:tok3" && got[3] == "complete", true)
        } finally {
            a.close()
            b.close()
            TypedPayloadCodecs.unregister("tests.SrToken")
        }
    }
}

class SrOpaque {}

class SrToken {
    let index: Int64
    let text: String

    init(index: Int64, text: String) {
        this.index = index
        this.text = text
    }
}

/**
 * 需求上界的共用断言（本地与跨节点各跑一次），窗口 bufferSize = 4：
 * 1. 订阅者不请求：生产方只发出 4 个（= 授予的需求），另有 4 个在生产方缓冲，再 offer 被背压；
 * 2. 逐个放行：任何时刻 sent ≤ delivered + 4、sent ≤ granted；
 * 3. 全部放行：按序完成，最大"已收未交付"≤ 4，生产方从未超发。
 */
func srAssertDemandBound(producer: StreamProducer<String>, sub: StreamRefSubscription, rec: EventRecorder): Unit {
    let next = AtomicInt64(1)
    let stalled = awaitCondition(SR_WAIT, { =>
        srFill(producer, "e", next)
        producer.sentCount() == 4 && producer.bufferedCount() == 4 && sub.receivedCount() == 4
    })
    @Expect(stalled, true)
    @Expect(producer.grantedDemand(), 4)
    @Expect(sub.deliveredCount(), 0)
    @Expect(next.load() - 1, 8)
    @Expect(producer.offer("over").isBackpressured(), true)
    @Expect(rec.size(), 0)
    // 放行 2 个：窗口空出一半才补需求（4 + 2 = 6）
    sub.request(2)
    @Expect(awaitCondition(SR_WAIT, { => producer.sentCount() == 6 && sub.deliveredCount() == 2 }), true)
    @Expect(producer.sentCount() <= sub.deliveredCount() + 4, true)
    @Expect(producer.sentCount() <= producer.grantedDemand(), true)
    // 放行全部并补齐到 40 个后完成
    sub.request(Int64.Max)
    @Expect(srOfferAll(producer, "e", next.load(), 40), true)
    producer.complete()
    @Expect(rec.await("complete", SR_WAIT), true)
    @Expect(srExpectAll(rec, "e", 40), true)
    @Expect(sub.maxUnconsumed() <= 4, true)
    @Expect(producer.maxOverGrant(), 0)
    @Expect(producer.termination().get(SR_WAIT).isCompleted(), true)
}
