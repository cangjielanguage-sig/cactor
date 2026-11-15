/*
 * CActor - SinkRef（DA7 收尾）：sink 拥有方拉取、远端生产方推送
 *
 * 逐条验证 SinkRef 的协议保证（功能断言，不是压测）：
 *   - 本地顺序与完成：按序 pull、恰好一个 Completed；
 *   - 信用背压：不 pull 时本地缓冲最多 bufferSize，生产方 offer 随即 Backpressured；
 *   - 失败/取消双向传播：生产方 fail → pull 异常；消费方 cancel → 生产方 termination Cancelled；
 *   - 单次挂接：第二个生产方被拒绝，第一个不受影响；
 *   - 属主停止 → 整条流结束；订阅超时 / 空闲超时；
 *   - Reactive Streams 适配：Sinks.fromSubscriber(asSink(...)) 把本地流接到 SinkRef；
 *   - 真实回环 TCP：SinkRef 过线、需求（序号上界）与取消/失败传播。
 * 等待一律用 awaitCondition，不写固定 sleep。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.sync.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.*
import cactor_cluster.streamref.*
import cactor_streams.*
import cactor_testkit.*

/** 一直 offer 到 Backpressured，返回是否全部投出（等 whenReady 不丢元素） */
func srSinkOfferAll(producer: SinkRefProducer<String>, prefix: String, from: Int64, to: Int64): Bool {
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
func srSinkFill(producer: SinkRefProducer<String>, prefix: String, next: AtomicInt64): Unit {
    while (true) {
        let i = next.load()
        if (producer.offer("${prefix}${i}").isEnqueued()) {
            next.fetchAdd(1)
        } else {
            return
        }
    }
}

/** 顺序拉取直到完成，校验前缀序列；返回拉到的元素数 */
func srSinkDrain(consumer: StreamConsumer<String>, prefix: String, expectFrom: Int64): Int64 {
    var expect = expectFrom
    while (true) {
        match (consumer.pull().get(SR_WAIT)) {
            case Some(v) =>
                @Expect(v, "${prefix}${expect}")
                expect++
            case None => break
        }
    }
    expect - expectFrom
}

@Test
public class StreamRefSinkTest {
    // ================= 本进程 =================

    /** 顺序、完成、背压上界：不 pull 时最多 bufferSize 个元素越过线上 */
    @TestCase
    public func localOrderBackpressureAndCompletion(): Unit {
        let kit = ActorTestKit("sink-local-order")
        try {
            let refs = StreamRefs(kit.system)
            let settings = StreamRefSettings().withBufferSize(4)
            let (ref, consumer) = refs.sinkRef<String>(settings: settings)
            let producer = refs.asSink<String>(ref, settings: settings)
            @Expect(awaitCondition(SR_WAIT, { =>
                producer.isAttached() && consumer.isAttached() && consumer.grantedDemand() == 4
            }), true)

            // 不 pull：线上 4 个、生产方缓冲 4 个，再 offer 被背压
            let next = AtomicInt64(1)
            let stalled = awaitCondition(SR_WAIT, { =>
                srSinkFill(producer, "e", next)
                producer.sentCount() == 4 && producer.bufferedCount() == 4 && consumer.receivedCount() == 4
            })
            @Expect(stalled, true)
            @Expect(producer.grantedDemand(), 4)
            @Expect(consumer.deliveredCount(), 0)
            @Expect(next.load() - 1, 8)
            @Expect(producer.offer("over").isBackpressured(), true)
            @Expect(consumer.bufferedCount(), 4)
            @Expect(consumer.maxBuffered() <= 4, true)

            // 拉 2 个：窗口空出一半才补需求（4 + 2 = 6）
            @Expect(consumer.pull().get(SR_WAIT).getOrThrow(), "e1")
            @Expect(consumer.pull().get(SR_WAIT).getOrThrow(), "e2")
            @Expect(awaitCondition(SR_WAIT, { => producer.sentCount() == 6 && consumer.deliveredCount() == 2 }), true)
            @Expect(producer.sentCount() <= consumer.deliveredCount() + 4, true)
            @Expect(producer.sentCount() <= producer.grantedDemand(), true)

            // 再补 2 个（生产方缓冲重新有空位）后完成，按序排空
            @Expect(srSinkOfferAll(producer, "e", next.load(), next.load() + 1), true)
            producer.complete()
            @Expect(srSinkDrain(consumer, "e", 3), 8)
            @Expect(consumer.pull().get(SR_WAIT).isNone(), true)
            @Expect(consumer.termination().get(SR_WAIT).isCompleted(), true)
            @Expect(producer.termination().get(SR_WAIT).isCompleted(), true)
            @Expect(producer.sentCount(), 10)
            @Expect(producer.maxOverGrant(), 0)
            @Expect(consumer.maxBuffered() <= 4, true)
            @Expect(producer.offer("late").isClosed(), true)
        } finally {
            kit.shutdown()
        }
    }

    /** 生产方失败：挂起的 pull 以异常结束，结局恰好一次，之后的 pull 仍然失败而不是挂起 */
    @TestCase
    public func producerFailurePropagatesOnce(): Unit {
        let kit = ActorTestKit("sink-local-fail")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, consumer) = refs.sinkRef<String>()
            let producer = refs.asSink<String>(ref)
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() }), true)
            @Expect(producer.offer("e1").isEnqueued(), true)
            @Expect(consumer.pull().get(SR_WAIT).getOrThrow(), "e1")

            let pending = consumer.pull()
            producer.fail("模型崩了")
            var message = ""
            try {
                let _ = pending.get(SR_WAIT)
            } catch (e: StreamRefRemoteFailureException) {
                message = e.message
            }
            @Expect(message.contains("模型崩了"), true)
            let t = consumer.termination().get(SR_WAIT)
            @Expect(t.isFailed() && t.reason() == "模型崩了", true)
            let pt = producer.termination().get(SR_WAIT)
            @Expect(pt.isFailed() && pt.reason() == "模型崩了", true)
            // 终止后 pull 立刻失败（不会挂起到超时）
            var second = false
            try {
                let _ = consumer.pull().get(SR_WAIT)
            } catch (_: StreamRefRemoteFailureException) {
                second = true
            }
            @Expect(second, true)
        } finally {
            kit.shutdown()
        }
    }

    /** 消费方取消：生产方 termination 变 Cancelled、offer 变 Closed，本地 pull 得到 None */
    @TestCase
    public func consumerCancelReachesProducer(): Unit {
        let kit = ActorTestKit("sink-local-cancel")
        try {
            let refs = StreamRefs(kit.system)
            let settings = StreamRefSettings().withBufferSize(2)
            let (ref, consumer) = refs.sinkRef<String>(settings: settings)
            let producer = refs.asSink<String>(ref, settings: settings)
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() }), true)
            @Expect(srSinkOfferAll(producer, "t", 1, 4), true)
            @Expect(consumer.pull().get(SR_WAIT).getOrThrow(), "t1")
            consumer.cancel("够了")
            // Cancel 与对端停止的 DEATH 是两条通道，谁先到都可能：只要求是 Cancelled
            let t = producer.termination().get(SR_WAIT)
            @Expect(t.isCancelled(), true)
            @Expect(producer.offer("x").isClosed(), true)
            @Expect(producer.whenReady().get(SR_WAIT), false)
            @Expect(consumer.termination().get(SR_WAIT).isCancelled(), true)
            @Expect(consumer.pull().get(SR_WAIT).isNone(), true)
        } finally {
            kit.shutdown()
        }
    }

    /** 单次挂接：第二个生产方被拒绝，第一个不受影响 */
    @TestCase
    public func secondProducerIsRejected(): Unit {
        let kit = ActorTestKit("sink-local-double")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, consumer) = refs.sinkRef<String>()
            let first = refs.asSink<String>(ref)
            @Expect(awaitCondition(SR_WAIT, { => first.isAttached() }), true)
            let second = refs.asSink<String>(ref)
            let rejected = second.termination().get(SR_WAIT)
            @Expect(rejected.isFailed() && rejected.reason().contains("只能挂接一个生产方"), true)
            @Expect(first.isClosed(), false)
            @Expect(first.offer("e1").isEnqueued(), true)
            @Expect(consumer.pull().get(SR_WAIT).getOrThrow(), "e1")
            consumer.cancel()
            @Expect(first.termination().get(SR_WAIT).isCancelled(), true)
        } finally {
            kit.shutdown()
        }
    }

    /** 属主（消费该流的 Actor）停止 → 整条流结束，生产方收到 Cancel */
    @TestCase
    public func ownerStopEndsStream(): Unit {
        let kit = ActorTestKit("sink-local-owner")
        try {
            let refs = StreamRefs(kit.system)
            let owner = kit.spawnTyped<String>(TypedBehaviors.ignore<String>(), "sink-owner")
            let (ref, consumer) = refs.sinkRef<String>(owner: owner.toClassic())
            let producer = refs.asSink<String>(ref)
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() }), true)
            @Expect(producer.offer("e1").isEnqueued(), true)
            @Expect(consumer.pull().get(SR_WAIT).getOrThrow(), "e1")
            let pending = consumer.pull()
            kit.system.stop(owner.toClassic().getOrThrow())
            var failed = false
            try {
                let _ = pending.get(SR_WAIT)
            } catch (_: StreamRefPeerTerminatedException) {
                failed = true
            }
            @Expect(failed, true)
            @Expect(consumer.termination().get(SR_WAIT).isFailed(), true)
            let pt = producer.termination().get(SR_WAIT)
            @Expect(pt.isCancelled(), true)
            @Expect(pt.reason().contains("属主终止"), true)
            @Expect(producer.offer("late").isClosed(), true)
        } finally {
            kit.shutdown()
        }
    }

    /** 订阅超时：没人挂接时 sink 侧失败，pull 不会永久挂起 */
    @TestCase
    public func subscriptionTimeoutFailsSink(): Unit {
        let kit = ActorTestKit("sink-local-subtimeout")
        try {
            let refs = StreamRefs(kit.system)
            let quick = StreamRefSettings().withSubscriptionTimeout(Duration.millisecond * 200)
            let (_, consumer) = refs.sinkRef<String>(settings: quick)
            let t = consumer.termination().get(SR_WAIT)
            @Expect(t.isFailed() && t.reason().contains("subscription timeout"), true)
            var timedOut = false
            try {
                let _ = consumer.pull().get(SR_WAIT)
            } catch (_: StreamRefSubscriptionTimeoutException) {
                timedOut = true
            }
            @Expect(timedOut, true)
        } finally {
            kit.shutdown()
        }
    }

    /** 空闲超时：sink 已授信但生产方久久不发，sink 失败并通知生产方取消 */
    @TestCase
    public func idleTimeoutFailsAndCancels(): Unit {
        let kit = ActorTestKit("sink-local-idle")
        try {
            let refs = StreamRefs(kit.system)
            let settings = StreamRefSettings().withBufferSize(2).withIdleTimeout(Duration.millisecond * 200)
            let (ref, consumer) = refs.sinkRef<String>(settings: settings)
            let producer = refs.asSink<String>(ref, settings: settings)
            @Expect(awaitCondition(SR_WAIT, { => consumer.grantedDemand() == 2 && producer.isAttached() }), true)
            let t = consumer.termination().get(SR_WAIT)
            @Expect(t.isFailed() && t.reason().contains("idle timeout"), true)
            let pt = producer.termination().get(SR_WAIT)
            @Expect(pt.isCancelled(), true)
        } finally {
            kit.shutdown()
        }
    }

    /** cactor_streams 兼容：Sinks.fromSubscriber(asSink(...)) 把本地流接到 SinkRef */
    @TestCase
    public func streamsSinkAdapter(): Unit {
        let kit = ActorTestKit("sink-local-streams")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, consumer) = refs.sinkRef<String>()
            let producer = refs.asSink<String>(ref)
            Sources.from<String>(["a", "b", "c"]).runWith(Sinks.fromSubscriber<String>(producer), kit.system)
            let got = ArrayList<String>()
            while (true) {
                match (consumer.pull().get(SR_WAIT)) {
                    case Some(v) => got.add(v)
                    case None => break
                }
            }
            @Expect(got.size, 3)
            @Expect(got[0] == "a" && got[1] == "b" && got[2] == "c", true)
            @Expect(consumer.termination().get(SR_WAIT).isCompleted(), true)
        } finally {
            kit.shutdown()
        }
    }

    // ================= 跨节点（真实回环 TCP） =================

    /** SinkRef 以文本过线；跨节点需求上界、顺序与完成 */
    @TestCase
    public func remoteSinkRefRespectsDemandAndOrder(): Unit {
        let a = SrNode("sink-remote-a")
        let b = SrNode("sink-remote-b")
        try {
            let settings = StreamRefSettings().withBufferSize(4)
            let (sinkB, consumer) = b.refs.sinkRef<String>(settings: settings)
            let wire = sinkB.toWireString()
            @Expect(wire.startsWith("sinkref:v1|"), true)
            let sinkA = SinkRef<String>.fromWireString(wire)
            @Expect(sinkA.localRef.isNone(), true)
            let producer = a.refs.asSink<String>(sinkA, settings: settings)
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() && consumer.isAttached() }), true)

            // 不 pull：线上最多 4 个，生产方缓冲最多 4 个，再 offer 背压
            let next = AtomicInt64(1)
            let stalled = awaitCondition(SR_WAIT, { =>
                srSinkFill(producer, "e", next)
                producer.sentCount() == 4 && producer.bufferedCount() == 4 && consumer.receivedCount() == 4
            })
            @Expect(stalled, true)
            @Expect(producer.grantedDemand(), consumer.grantedDemand())
            @Expect(producer.offer("over").isBackpressured(), true)
            @Expect(consumer.maxBuffered() <= 4, true)

            // 拉 2 个后需求增长，生产方继续发出
            @Expect(consumer.pull().get(SR_WAIT).getOrThrow(), "e1")
            @Expect(consumer.pull().get(SR_WAIT).getOrThrow(), "e2")
            @Expect(awaitCondition(SR_WAIT, { => producer.sentCount() == 6 && consumer.deliveredCount() == 2 }), true)
            @Expect(producer.sentCount() <= producer.grantedDemand(), true)

            @Expect(srSinkOfferAll(producer, "e", next.load(), next.load() + 1), true)
            producer.complete()
            @Expect(srSinkDrain(consumer, "e", 3), 8)
            @Expect(consumer.termination().get(SR_WAIT).isCompleted(), true)
            @Expect(producer.termination().get(SR_WAIT).isCompleted(), true)
            @Expect(producer.sentCount(), 10)
            @Expect(producer.maxOverGrant(), 0)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 跨节点取消：sink 侧 cancel 传到生产方，termination 为 Cancelled */
    @TestCase
    public func remoteCancelReachesProducer(): Unit {
        let a = SrNode("sink-rcancel-a")
        let b = SrNode("sink-rcancel-b")
        try {
            let settings = StreamRefSettings().withBufferSize(2)
            let (sinkB, consumer) = b.refs.sinkRef<String>(settings: settings)
            let producer = a.refs.asSink<String>(SinkRef<String>.fromWireString(sinkB.toWireString()), settings: settings)
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() && consumer.isAttached() }), true)
            @Expect(srSinkOfferAll(producer, "t", 1, 4), true)
            @Expect(consumer.pull().get(SR_WAIT).getOrThrow(), "t1")
            consumer.cancel("够了")
            let t = producer.termination().get(SR_WAIT)
            @Expect(t.isCancelled(), true)
            @Expect(producer.offer("x").isClosed(), true)
            @Expect(consumer.termination().get(SR_WAIT).isCancelled(), true)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 跨节点失败：生产方 fail 传到 sink，挂起的 pull 以异常结束 */
    @TestCase
    public func remoteProducerFailureReachesSink(): Unit {
        let a = SrNode("sink-rfail-a")
        let b = SrNode("sink-rfail-b")
        try {
            let (sinkB, consumer) = b.refs.sinkRef<String>()
            let producer = a.refs.asSink<String>(SinkRef<String>.fromWireString(sinkB.toWireString()))
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() && consumer.isAttached() }), true)
            let pending = consumer.pull()
            producer.fail("远端崩了")
            var message = ""
            try {
                let _ = pending.get(SR_WAIT)
            } catch (e: StreamRefRemoteFailureException) {
                message = e.message
            }
            @Expect(message.contains("远端崩了"), true)
            let t = consumer.termination().get(SR_WAIT)
            @Expect(t.isFailed() && t.reason() == "远端崩了", true)
            @Expect(producer.termination().get(SR_WAIT).isFailed(), true)
        } finally {
            a.close()
            b.close()
        }
    }
}
