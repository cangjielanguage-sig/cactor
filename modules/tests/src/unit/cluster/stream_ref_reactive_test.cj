/*
 * CActor Cluster.StreamRef - Reactive Streams 桥接的需求、取消与真实 TCP 回归
 *
 * 同步上游刻意在 request 内发元素，用以检验请求串行化和端到端有界信用。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_cluster.streamref.*
import cactor_core.typed.*
import cactor_streams.*
import cactor_streams.reactive.*
import cactor_testkit.*

/** 严格同步的有限/无限上游：每次 request 只发获准的元素，计数独立于适配器。 */
class SrCountingPublisher <: Publisher<String> & Subscription {
    let limit: Int64
    let requested = AtomicInt64(0)
    let emitted = AtomicInt64(0)
    let cancelled = AtomicBool(false)
    let depth = AtomicInt64(0)
    let peakDepth = AtomicInt64(0)
    var target: Option<Subscriber<String>> = None<Subscriber<String>>

    init(limit: Int64) { this.limit = limit }

    public func subscribe(s: Subscriber<String>): Unit {
        target = Some(s)
        s.onSubscribe(this)
    }

    public func request(n: Int64): Unit {
        let d = depth.fetchAdd(1) + 1
        if (d > peakDepth.load()) { peakDepth.store(d) }
        try {
            requested.fetchAdd(n)
            var i: Int64 = 0
            while (i < n && !cancelled.load() && emitted.load() < limit) {
                let next = emitted.fetchAdd(1) + 1
                target.getOrThrow().onNext("e${next}")
                i++
                if (next == limit) { target.getOrThrow().onComplete() }
            }
        } finally {
            depth.fetchSub(1)
        }
    }

    public func cancel(): Unit { cancelled.store(true) }
}

class SrImmediateFailurePublisher <: Publisher<String> {
    let throwOnSubscribe: Bool
    init(throwOnSubscribe: Bool) { this.throwOnSubscribe = throwOnSubscribe }
    public func subscribe(s: Subscriber<String>): Unit {
        s.onSubscribe(ReactiveStreams.cancelledSubscription())
        if (throwOnSubscribe) { throw IllegalStateException("subscribe-boom") }
        s.onError(IllegalStateException("immediate-boom"))
    }
}

class SrReactiveSubscriber <: Subscriber<String> {
    let rec = EventRecorder()
    let ready = TypedPromise<Subscription>()
    let failure = TypedPromise<Exception>()
    public func onSubscribe(s: Subscription): Unit { ready.trySuccess(s) |> ignoreSrBool }
    public func onNext(s: String): Unit { rec.record("next:${s}") }
    public func onError(e: Exception): Unit {
        rec.record("error:${e.message}")
        failure.trySuccess(e) |> ignoreSrBool
    }
    public func onComplete(): Unit { rec.record("complete") }
}

func ignoreSrBool(value: Bool): Unit {}

@Test
public class StreamRefReactiveTest {
    @TestCase
    public func localPublisherToStreamDslPreservesOrderAndCompletion(): Unit {
        let kit = ActorTestKit("sr-rs-local")
        try {
            let refs = StreamRefs(kit.system)
            let upstream = SrCountingPublisher(1000)
            let ref = refs.fromPublisher<String>(upstream, settings: StreamRefSettings().withBufferSize(4))
            let result = Sources.fromPublisher<String>(refs.asPublisher<String>(ref))
                .runWith(Sinks.seq<String>(), kit.system).get(SR_WAIT)
            @Expect(result.size, 1000)
            for (i in 0..1000) { @Expect(result[i], "e${i + 1}") }
            @Expect(upstream.emitted.load(), 1000)
            @Expect(upstream.requested.load(), 1000)
            @Expect(upstream.peakDepth.load(), 1)
            @Expect(upstream.cancelled.load(), false)
        } finally { kit.shutdown() }
    }

    @TestCase
    public func subscriberBridgeBoundsActualUpstreamCreditAndPropagatesCancel(): Unit {
        let kit = ActorTestKit("sr-rs-bounds")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, producer) = refs.sourceRef<String>(settings: StreamRefSettings().withBufferSize(4))
            let upstream = SrCountingPublisher(Int64.Max)
            upstream.subscribe(refs.asSubscriber<String>(producer))
            @Expect(awaitCondition(SR_WAIT, { => producer.bufferedCount() == 4 }), true)
            @Expect(upstream.emitted.load(), 4)
            let downstream = SrReactiveSubscriber()
            refs.asPublisher<String>(ref, settings: StreamRefSettings().withBufferSize(3)).subscribe(downstream)
            let subscription = downstream.ready.get(SR_WAIT)
            @Expect(awaitCondition(SR_WAIT, { => producer.sentCount() == 3 && producer.bufferedCount() == 4 }), true)
            @Expect(upstream.emitted.load(), 7)
            @Expect(upstream.requested.load(), 7)
            @Expect(downstream.rec.size(), 0)
            subscription.request(2)
            @Expect(awaitCondition(SR_WAIT, { => srCountPrefix(downstream.rec, "next:") == 2 }), true)
            @Expect(awaitCondition(SR_WAIT, { => producer.sentCount() == 5 && producer.bufferedCount() == 4 }), true)
            @Expect(upstream.emitted.load(), 9)
            @Expect(producer.maxOverGrant(), 0)
            @Expect(upstream.peakDepth.load(), 1)
            subscription.cancel()
            @Expect(producer.termination().get(SR_WAIT).isCancelled(), true)
            @Expect(awaitCondition(SR_WAIT, { => upstream.cancelled.load() }), true)
            @Expect(srCountPrefix(downstream.rec, "next:"), 2)
            @Expect(srCountPrefix(downstream.rec, "error:"), 0)
            @Expect(downstream.rec.contains("complete"), false)
        } finally { kit.shutdown() }
    }

    @TestCase
    public func sinkFromSubscriberFeedsStreamRef(): Unit {
        let kit = ActorTestKit("sr-rs-sink")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, producer) = refs.sourceRef<String>(settings: StreamRefSettings().withBufferSize(2))
            let result = Sources.fromPublisher<String>(refs.asPublisher<String>(ref))
                .runWith(Sinks.seq<String>(), kit.system)
            Sources.range(1, 100).map<String>({ x => "e${x}" })
                .runWith(Sinks.fromSubscriber<String>(refs.asSubscriber<String>(producer)), kit.system)
            let all = result.get(SR_WAIT)
            @Expect(all.size, 100)
            @Expect(all[0], "e1")
            @Expect(all[99], "e100")
            @Expect(producer.termination().get(SR_WAIT).isCompleted(), true)
        } finally { kit.shutdown() }
    }

    @TestCase
    public func nonPositiveRequestCancelsPublisherUpstream(): Unit {
        let kit = ActorTestKit("sr-rs-invalid")
        try {
            let refs = StreamRefs(kit.system)
            let upstream = SrCountingPublisher(Int64.Max)
            let ref = refs.fromPublisher<String>(upstream, settings: StreamRefSettings().withBufferSize(2))
            let subscriber = SrReactiveSubscriber()
            refs.asPublisher<String>(ref).subscribe(subscriber)
            subscriber.ready.get(SR_WAIT).request(0)
            @Expect((subscriber.failure.get(SR_WAIT) as IllegalArgumentException).isSome(), true)
            @Expect(awaitCondition(SR_WAIT, { => srCountPrefix(subscriber.rec, "error:") == 1 }), true)
            @Expect(awaitCondition(SR_WAIT, { => upstream.cancelled.load() }), true)
            @Expect(srCountPrefix(subscriber.rec, "next:"), 0)
        } finally { kit.shutdown() }
    }

    @TestCase
    public func duplicateUpstreamSubscriptionIsCancelled(): Unit {
        let kit = ActorTestKit("sr-rs-duplicate")
        try {
            let refs = StreamRefs(kit.system)
            let (_, producer) = refs.sourceRef<String>()
            let subscriber = refs.asSubscriber<String>(producer)
            let first = SrCountingPublisher(Int64.Max)
            let second = SrCountingPublisher(Int64.Max)
            first.subscribe(subscriber)
            second.subscribe(subscriber)
            @Expect(second.cancelled.load(), true)
            @Expect(second.requested.load(), 0)
            producer.fail("test-finished")
            @Expect(awaitCondition(SR_WAIT, { => first.cancelled.load() }), true)
        } finally { kit.shutdown() }
    }

    @TestCase
    public func lateUpstreamAndUnsolicitedElementAreCancelled(): Unit {
        let kit = ActorTestKit("sr-rs-late")
        try {
            let refs = StreamRefs(kit.system)
            let (_, producer) = refs.sourceRef<String>()
            let subscriber = refs.asSubscriber<String>(producer)
            // 尚未 onSubscribe，更没有 request；不允许把元素偷偷塞进生产端。
            subscriber.onNext("unsolicited")
            @Expect(producer.termination().get(SR_WAIT).isFailed(), true)
            @Expect(producer.sentCount(), 0)
            let late = SrCountingPublisher(Int64.Max)
            late.subscribe(subscriber)
            @Expect(late.cancelled.load(), true)
            @Expect(late.requested.load(), 0)
        } finally { kit.shutdown() }
    }

    @TestCase
    public func upstreamFailureReachesDsl(): Unit {
        let kit = ActorTestKit("sr-rs-failure")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, producer) = refs.sourceRef<String>()
            let result = Sources.fromPublisher<String>(refs.asPublisher<String>(ref))
                .runWith(Sinks.seq<String>(), kit.system)
            @Expect(awaitCondition(SR_WAIT, { => producer.isAttached() }), true)
            let subscriber = refs.asSubscriber<String>(producer)
            subscriber.onError(IllegalStateException("upstream-boom"))
            var message = ""
            try { result.get(SR_WAIT) } catch (e: Exception) { message = e.message }
            @Expect(message.contains("upstream-boom"), true)
            @Expect(producer.termination().get(SR_WAIT).isFailed(), true)

            let infinite = SrCountingPublisher(Int64.Max)
            let failedRef = refs.fromPublisher<String>(infinite)
            let failed = Sources.fromPublisher<String>(refs.asPublisher<String>(failedRef))
                .map<String>({ _ => throw IllegalStateException("downstream-boom") })
                .runWith(Sinks.ignore<String>(), kit.system)
            var downstreamMessage = ""
            try { failed.get(SR_WAIT) } catch (e: Exception) { downstreamMessage = e.message }
            @Expect(downstreamMessage.contains("downstream-boom"), true)
            @Expect(awaitCondition(SR_WAIT, { => infinite.cancelled.load() }), true)
        } finally { kit.shutdown() }
    }

    @TestCase
    public func failureBeforeAttachmentRetainsOriginalCause(): Unit {
        let kit = ActorTestKit("sr-rs-immediate-failure")
        try {
            let refs = StreamRefs(kit.system)
            let (ref, producer) = refs.sourceRef<String>()
            @Expect(producer.offer("discard-me").isEnqueued(), true)
            producer.fail("before-attachment-boom")
            // 等失败已真正被生产阶段处理后才挂接，旧实现会丢失原因，收到 PeerTerminated。
            @Expect(producer.termination().get(SR_WAIT).isFailed(), true)
            @Expect(producer.offer("late").isClosed(), true)
            let first = SrReactiveSubscriber()
            refs.asPublisher<String>(ref).subscribe(first)
            @Expect(first.failure.get(SR_WAIT).message.contains("before-attachment-boom"), true)
            @Expect((first.failure.get(SR_WAIT) as StreamRefRemoteFailureException).isSome(), true)
            @Expect(srCountPrefix(first.rec, "next:"), 0)
            for (throws in [false, true]) {
                let immediate = refs.fromPublisher<String>(SrImmediateFailurePublisher(throws))
                let downstream = SrReactiveSubscriber()
                refs.asPublisher<String>(immediate).subscribe(downstream)
                let expected = if (throws) { "subscribe-boom" } else { "immediate-boom" }
                @Expect(downstream.failure.get(SR_WAIT).message.contains(expected), true)
            }
        } finally { kit.shutdown() }
    }

    @TestCase
    public func secondPublisherSubscriberIsRejectedAfterCompletion(): Unit {
        let kit = ActorTestKit("sr-rs-second")
        try {
            let refs = StreamRefs(kit.system)
            let ref = refs.fromPublisher<String>(SrCountingPublisher(1))
            let publisher = refs.asPublisher<String>(ref)
            let all = Sources.fromPublisher<String>(publisher).runWith(Sinks.seq<String>(), kit.system).get(SR_WAIT)
            @Expect(all.size, 1)
            let second = SrReactiveSubscriber()
            publisher.subscribe(second)
            // 即使生产阶段已停止，仍先收到订阅再收到明确拒绝，而不是挂接超时。
            second.ready.get(SR_WAIT)
            @Expect(srCountPrefix(second.rec, "error:"), 1)
            @Expect(srCountPrefix(second.rec, "next:"), 0)
        } finally { kit.shutdown() }
    }

    @TestCase
    public func realTcpPublisherToDslAndCancellation(): Unit {
        let a = SrNode("sr-rs-tcp-a")
        let b = SrNode("sr-rs-tcp-b")
        try {
            let (boundedRef, producer) = a.refs.sourceRef<String>(settings: StreamRefSettings().withBufferSize(3))
            let bounded = SrCountingPublisher(Int64.Max)
            bounded.subscribe(a.refs.asSubscriber<String>(producer))
            let slow = SrReactiveSubscriber()
            b.refs.asPublisher<String>(SourceRef<String>.fromWireString(boundedRef.toWireString()),
                settings: StreamRefSettings().withBufferSize(2)).subscribe(slow)
            let slowSubscription = slow.ready.get(SR_WAIT)
            @Expect(awaitCondition(SR_WAIT, { => producer.sentCount() == 2 && producer.bufferedCount() == 3 }), true)
            @Expect(bounded.requested.load(), 5)
            @Expect(bounded.emitted.load(), 5)
            @Expect(slow.rec.size(), 0)
            slowSubscription.request(1)
            @Expect(awaitCondition(SR_WAIT, { => producer.sentCount() == 3 && producer.bufferedCount() == 3 }), true)
            @Expect(bounded.requested.load(), 6)
            @Expect(srCountPrefix(slow.rec, "next:"), 1)
            @Expect(producer.maxOverGrant(), 0)
            slowSubscription.cancel()
            @Expect(producer.termination().get(SR_WAIT).isCancelled(), true)
            @Expect(awaitCondition(SR_WAIT, { => bounded.cancelled.load() }), true)

            let upstream = SrCountingPublisher(200)
            let ref = a.refs.fromPublisher<String>(upstream, settings: StreamRefSettings().withBufferSize(3))
            let remote = SourceRef<String>.fromWireString(ref.toWireString())
            let all = Sources.fromPublisher<String>(b.refs.asPublisher<String>(remote,
                settings: StreamRefSettings().withBufferSize(2))).runWith(Sinks.seq<String>(), b.kit.system).get(SR_WAIT)
            @Expect(all.size, 200)
            for (i in 0..200) { @Expect(all[i], "e${i + 1}") }
            @Expect(upstream.peakDepth.load(), 1)

            let infinite = SrCountingPublisher(Int64.Max)
            let ref2 = a.refs.fromPublisher<String>(infinite, settings: StreamRefSettings().withBufferSize(3))
            let remote2 = SourceRef<String>.fromWireString(ref2.toWireString())
            let taken = Sources.fromPublisher<String>(b.refs.asPublisher<String>(remote2,
                settings: StreamRefSettings().withBufferSize(2))).take(5).runWith(Sinks.seq<String>(), b.kit.system).get(SR_WAIT)
            @Expect(taken.size, 5)
            @Expect(awaitCondition(SR_WAIT, { => infinite.cancelled.load() }), true)
            @Expect(infinite.emitted.load() <= 5 + 16 + 2 + 3 + 1, true)
        } finally {
            b.close()
            a.close()
        }
    }
}
