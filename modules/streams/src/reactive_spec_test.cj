/*
 * CActor Streams - Reactive Streams 规范要点测试
 *
 * 用手动 request 的订阅者 / 手动发信号的发布者，从流的两个边界（Sink.asPublisher、Source.fromPublisher）
 * 检查：需求不超发（1.1）、onSubscribe 先行（1.9）、终止信号只发一次（1.7）、非正 request 报错（3.9）、
 * cancel 传到上游（3.5/3.7）、单订阅者（1.10）、重复 onSubscribe 被取消（2.5）、上游超发被识别。
 */
package cactor_streams

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.dsl.*
import cactor_core.typed.*
import cactor_streams.reactive.*
import cactor.api.CActor

@Test
public class ReactiveSpecTest {
    @TestCase
    public func testPublisherNeverExceedsDemand(): Unit {
        let system = CActor.system("rs-demand")
        let pub = Sources.range(1, 100).runWith(Sinks.asPublisher<Int64>(), system)
        let sub = TestSubscriber<Int64>()
        pub.subscribe(sub)
        @Expect(awaitTrue(3.seconds, { => sub.hasSubscription() }), true)
        sub.request(3)
        @Expect(awaitTrue(3.seconds, { => sub.received() == 3 }), true)
        sub.request(2)
        @Expect(awaitTrue(3.seconds, { => sub.received() == 5 }), true)
        // 需求饱和到 Int64.Max 不溢出（规范 3.17），随后收完剩余元素并完成
        sub.request(Int64.Max)
        sub.request(Int64.Max)
        @Expect(awaitTrue(3.seconds, { => sub.terminals() == 1 }), true)
        @Expect(sub.received(), 100)
        @Expect(sub.demandViolations, 0)
        @Expect(sub.signalList()[0], "subscribe")
        @Expect(sub.signalList()[sub.signalList().size - 1], "complete")
        system.terminate()
    }

    @TestCase
    public func testNonPositiveRequestSignalsErrorAndCancelsUpstream(): Unit {
        let system = CActor.system("rs-nonpositive")
        let (done, pub) = Sources.repeat(1).watchTermination()
            .toMat(Sinks.asPublisher<Int64>(), { w, p => (w, p) }).run(system)
        let sub = TestSubscriber<Int64>()
        pub.subscribe(sub)
        @Expect(awaitTrue(3.seconds, { => sub.hasSubscription() }), true)
        sub.request(0)
        @Expect(awaitTrue(3.seconds, { => sub.terminals() == 1 }), true)
        @Expect((sub.error.getOrThrow() as IllegalArgumentException).isSome(), true)
        // 上游被取消：无限源停止
        @Expect(failureOf(done, 3.seconds).isNone(), true)
        @Expect(sub.signalsAfterTerminal, 0)
        system.terminate()
    }

    @TestCase
    public func testCancelPropagatesUpstreamAndIsIdempotent(): Unit {
        let system = CActor.system("rs-cancel")
        let (done, pub) = Sources.repeat(7).watchTermination()
            .toMat(Sinks.asPublisher<Int64>(), { w, p => (w, p) }).run(system)
        let sub = TestSubscriber<Int64>()
        pub.subscribe(sub)
        @Expect(awaitTrue(3.seconds, { => sub.hasSubscription() }), true)
        sub.request(4)
        @Expect(awaitTrue(3.seconds, { => sub.received() == 4 }), true)
        sub.cancel()
        sub.cancel()
        @Expect(failureOf(done, 3.seconds).isNone(), true)
        // 取消后的 request 无效果（规范 3.6）
        sub.request(10)
        @Expect(sub.received(), 4)
        @Expect(sub.terminals(), 0)
        @Expect(sub.demandViolations, 0)
        system.terminate()
    }

    @TestCase
    public func testFailureIsSignalledOnceAfterOnSubscribe(): Unit {
        let system = CActor.system("rs-failure")
        let pub = Sources.failed<Int64>(IllegalStateException("boom")).runWith(Sinks.asPublisher<Int64>(), system)
        let sub = TestSubscriber<Int64>()
        pub.subscribe(sub)
        @Expect(awaitTrue(3.seconds, { => sub.terminals() == 1 }), true)
        let signals = sub.signalList()
        @Expect(signals.size, 2)
        @Expect(signals[0], "subscribe")
        @Expect(signals[1], "error")
        @Expect(sub.error.getOrThrow().message, "boom")
        system.terminate()
    }

    @TestCase
    public func testSecondSubscriberIsRejected(): Unit {
        let system = CActor.system("rs-second")
        let pub = Sources.repeat(1).runWith(Sinks.asPublisher<Int64>(), system)
        let first = TestSubscriber<Int64>()
        pub.subscribe(first)
        let second = TestSubscriber<Int64>()
        pub.subscribe(second)
        @Expect(second.signalList().size, 2)
        @Expect(second.signalList()[0], "subscribe")
        @Expect((second.error.getOrThrow() as ReactiveStreamsSpecException).isSome(), true)
        @Expect(awaitTrue(3.seconds, { => first.hasSubscription() }), true)
        first.cancel()
        system.terminate()
    }

    @TestCase
    public func testLateSubscriberSeesCompletion(): Unit {
        let system = CActor.system("rs-late")
        let (done, pub) = Sources.empty<Int64>().watchTermination()
            .toMat(Sinks.asPublisher<Int64>(), { w, p => (w, p) }).run(system)
        @Expect(failureOf(done, 3.seconds).isNone(), true)
        let sub = TestSubscriber<Int64>()
        pub.subscribe(sub)
        @Expect(awaitTrue(3.seconds, { => sub.terminals() == 1 }), true)
        @Expect(sub.signalList()[0], "subscribe")
        @Expect(sub.signalList()[1], "complete")
        system.terminate()
    }

    @TestCase
    public func testFromPublisherRequestsInBoundedBatches(): Unit {
        let system = CActor.system("rs-from")
        let mat = Materializer.create(system).withSettings(StreamSettings().withInputBufferSize(8))
        let pub = ManualPublisher<Int64>()
        let q = Sources.fromPublisher<Int64>(pub).runWith(Sinks.queue<Int64>(), mat)
        @Expect(awaitTrue(3.seconds, { => pub.subscription.totalRequested() == 8 }), true)
        for (i in 1..=8) {
            pub.sub().onNext(i)
        }
        // 消费一半后才补请求，且补的数量就是腾出的位置：在途 + 缓冲永不超过 8
        for (i in 1..=3) {
            @Expect(q.pull().get(3.seconds), Some(i))
        }
        @Expect(pub.subscription.totalRequested(), 8)
        @Expect(q.pull().get(3.seconds), Some(4))
        @Expect(awaitTrue(3.seconds, { => pub.subscription.totalRequested() == 12 }), true)
        pub.sub().onComplete()
        for (i in 5..=8) {
            @Expect(q.pull().get(3.seconds), Some(i))
        }
        @Expect(q.pull().get(3.seconds), None<Int64>)
        system.terminate()
    }

    @TestCase
    public func testUpstreamOverflowBeyondDemandFailsStream(): Unit {
        let system = CActor.system("rs-overflow")
        let mat = Materializer.create(system).withSettings(StreamSettings().withInputBufferSize(2))
        let pub = ManualPublisher<Int64>()
        let q = Sources.fromPublisher<Int64>(pub).runWith(Sinks.queue<Int64>(), mat)
        @Expect(awaitTrue(3.seconds, { => pub.subscription.totalRequested() == 2 }), true)
        pub.sub().onNext(1)
        pub.sub().onNext(2)
        pub.sub().onNext(3)
        let failure = failureOf(q.pull(), 3.seconds)
        // 第一个 pull 可能拿到 1（失败前），之后的 pull 一定看到违规
        let err = if (failure.isSome()) { failure } else { failureOf(q.pull(), 3.seconds) }
        @Expect((err.getOrThrow() as ReactiveStreamsSpecException).isSome(), true)
        @Expect(awaitTrue(3.seconds, { => pub.subscription.isCancelled() }), true)
        system.terminate()
    }

    @TestCase
    public func testDuplicateOnSubscribeIsCancelled(): Unit {
        let system = CActor.system("rs-dup")
        let (sub, q) = Sources.asSubscriber<Int64>().toMat(Sinks.queue<Int64>(), { s, q => (s, q) }).run(system)
        let first = ManualSubscription()
        let second = ManualSubscription()
        sub.onSubscribe(first)
        sub.onSubscribe(second)
        @Expect(awaitTrue(3.seconds, { => second.isCancelled() }), true)
        @Expect(first.isCancelled(), false)
        @Expect(first.totalRequested() > 0, true)
        sub.onNext(42)
        @Expect(q.pull().get(3.seconds), Some(42))
        // 下游取消时取消上游订阅
        q.cancel()
        @Expect(awaitTrue(3.seconds, { => first.isCancelled() }), true)
        system.terminate()
    }

    @TestCase
    public func testProcessorRoundTrip(): Unit {
        let system = CActor.system("rs-processor")
        let mat = Materializer.create(system)
        let processor = Flows.create<Int64>().map<Int64>({ x => x * 10 }).toProcessor(mat)
        let result = Sources.fromPublisher<Int64>(processor).runWith(Sinks.seq<Int64>(), mat)
        Sources.range(1, 5).runWith(Sinks.fromSubscriber<Int64>(processor), mat)
        @Expect(result.get(3.seconds), [10, 20, 30, 40, 50])
        system.terminate()
    }
}
