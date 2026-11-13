/*
 * CActor Streams - DSL 与同步算子测试
 */
package cactor_streams

import std.collection.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.dsl.*
import cactor_core.typed.*
import cactor.api.CActor

@Test
public class StreamDslTest {
    @TestCase
    public func testBlueprintIsLazyAndReusable(): Unit {
        let system = CActor.system("dsl-lazy")
        let calls = AtomicInt64(0)
        let graph = Sources.range(1, 5).map<Int64>({ x =>
            calls.fetchAdd(1)
            x * x
        }).toMat(Sinks.seq<Int64>(), { _, r => r })
        // 构建蓝图不执行任何用户函数
        @Expect(calls.load(), 0)
        @Expect(graph.run(system).get(3.seconds), [1, 4, 9, 16, 25])
        // 同一蓝图再次物化是一条独立的流
        @Expect(graph.run(system).get(3.seconds), [1, 4, 9, 16, 25])
        @Expect(calls.load(), 10)
        system.terminate()
    }

    @TestCase
    public func testBasicOperators(): Unit {
        let system = CActor.system("dsl-ops")
        let r1 = Sources.range(1, 10).filter({ x => x % 2 == 0 }).map<String>({ x => "v${x}" })
            .runWith(Sinks.seq<String>(), system)
        @Expect(r1.get(3.seconds), ["v2", "v4", "v6", "v8", "v10"])
        let r2 = Sources.range(1, 10).drop(2).take(3).runWith(Sinks.seq<Int64>(), system)
        @Expect(r2.get(3.seconds), [3, 4, 5])
        let r3 = Sources.range(1, 10).takeWhile({ x => x < 4 }).runWith(Sinks.seq<Int64>(), system)
        @Expect(r3.get(3.seconds), [1, 2, 3])
        let r4 = Sources.range(1, 10).takeWhile({ x => x < 4 }, true).runWith(Sinks.seq<Int64>(), system)
        @Expect(r4.get(3.seconds), [1, 2, 3, 4])
        let r5 = Sources.range(1, 6).dropWhile({ x => x < 4 }).runWith(Sinks.seq<Int64>(), system)
        @Expect(r5.get(3.seconds), [4, 5, 6])
        let r6 = Sources.range(1, 3).mapConcat<Int64>({ x => [x, x * 10] }).runWith(Sinks.seq<Int64>(), system)
        @Expect(r6.get(3.seconds), [1, 10, 2, 20, 3, 30])
        let r7 = Sources.range(1, 7).via(Flows.grouped<Int64>(3)).map<Int64>({ g => g.size })
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(r7.get(3.seconds), [3, 3, 1])
        let r8 = Sources.range(1, 4).scan<Int64>(0, { a, x => a + x }).runWith(Sinks.seq<Int64>(), system)
        @Expect(r8.get(3.seconds), [0, 1, 3, 6, 10])
        let r9 = Sources.range(1, 4).fold<Int64>(0, { a, x => a + x }).runWith(Sinks.seq<Int64>(), system)
        @Expect(r9.get(3.seconds), [10])
        let r10 = Sources.range(1, 10).collect<Int64>({ x => if (x > 8) { Some(x) } else { None<Int64> } })
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(r10.get(3.seconds), [9, 10])
        system.terminate()
    }

    @TestCase
    public func testSourcesAndSinks(): Unit {
        let system = CActor.system("dsl-sources")
        @Expect(Sources.single("x").runWith(Sinks.head<String>(), system).get(3.seconds), "x")
        @Expect(Sources.empty<Int64>().runWith(Sinks.headOption<Int64>(), system).get(3.seconds), None<Int64>)
        let headOfEmpty = failureOf(Sources.empty<Int64>().runWith(Sinks.head<Int64>(), system), 3.seconds)
        @Expect((headOfEmpty.getOrThrow() as EmptyStreamException).isSome(), true)
        @Expect(Sources.from<Int64>([3, 1, 2]).runFold<Int64>(0, { a, x => a * 10 + x }, system).get(3.seconds), 312)
        // 无限源：只在需要时生成
        @Expect(Sources.iterate<Int64>(1, { x => x * 2 }).take(5).runWith(Sinks.seq<Int64>(), system).get(3.seconds),
            [1, 2, 4, 8, 16])
        @Expect(Sources.unfold<Int64, Int64>(0, { s => if (s < 3) { Some((s + 1, s * s)) } else { None<(Int64, Int64)> } })
            .runWith(Sinks.seq<Int64>(), system).get(3.seconds), [0, 1, 4])
        @Expect(Sources.range(10, 1, -3).runWith(Sinks.seq<Int64>(), system).get(3.seconds), [10, 7, 4, 1])
        @Expect(Sources.fromPromise<Int64>(TypedPromises.successful<Int64>(9)).runWith(Sinks.seq<Int64>(), system)
            .get(3.seconds), [9])
        let rec = Recorder<Int64>()
        Sources.range(1, 3).runForeach({ x => rec.add(x) }, system).get(3.seconds)
        @Expect(rec.snapshot(), [1, 2, 3])
        // Sink.seq 的上限保护
        let limited = failureOf(Sources.repeat(1).runWith(Sinks.seq<Int64>(5), system), 3.seconds)
        @Expect((limited.getOrThrow() as StreamLimitReachedException).isSome(), true)
        // contramap
        @Expect(Sources.range(1, 3).runWith(Sinks.seq<String>().contramap<Int64>({ x => "${x}" }), system)
            .get(3.seconds), ["1", "2", "3"])
        system.terminate()
    }

    @TestCase
    public func testTickAndCancellable(): Unit {
        let system = CActor.system("dsl-tick")
        let r = Sources.tick<String>(Duration.Zero, 10.millis, "t").take(3).runWith(Sinks.seq<String>(), system)
        @Expect(r.get(3.seconds), ["t", "t", "t"])
        let (c, done) = Sources.tick<Int64>(Duration.Zero, 10.millis, 1)
            .toMat(Sinks.ignore<Int64>(), { c, d => (c, d) }).run(system)
        @Expect(c.cancel(), true)
        @Expect(c.cancel(), false)
        @Expect(failureOf(done, 3.seconds).isNone(), true)
        system.terminate()
    }

    @TestCase
    public func testThrottleSpacesElements(): Unit {
        let system = CActor.system("dsl-throttle")
        let start = MonoTime.now()
        // 突发 1 个，之后每 20ms 一个：6 个元素至少需要 5 个间隔
        let r = Sources.range(1, 6).throttle(1, 20.millis, 1).runWith(Sinks.seq<Int64>(), system)
        @Expect(r.get(5.seconds), [1, 2, 3, 4, 5, 6])
        @Expect(MonoTime.now() - start >= 90.millis, true)
        system.terminate()
    }

    @TestCase
    public func testFlatMapConcatRunsSubstreamsInOrder(): Unit {
        let system = CActor.system("dsl-flatmap")
        let r = Sources.range(1, 3).flatMapConcat<Int64>({ x => Sources.repeat(x).take(x) })
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(r.get(3.seconds), [1, 2, 2, 3, 3, 3])
        // 子流里也可以有异步边界
        let r2 = Sources.range(1, 3).flatMapConcat<Int64>({ x => Sources.range(1, x).async().map<Int64>({ y => x * 10 + y }) })
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(r2.get(3.seconds), [11, 21, 22, 31, 32, 33])
        // 无限展开 + take：子流按需拉取并在下游取消时被取消
        let r3 = Sources.repeat(1).flatMapConcat<Int64>({ x => Sources.repeat(x) }).take(4)
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(r3.get(3.seconds), [1, 1, 1, 1])
        system.terminate()
    }

    @TestCase
    public func testRecoverAndRecoverWithRetries(): Unit {
        let system = CActor.system("dsl-recover")
        let failing = Sources.range(1, 5).map<Int64>({ x => if (x == 3) { throw IllegalStateException("bad") } else { x } })
        let r1 = failing.recover({ _ => Some(-1) }).runWith(Sinks.seq<Int64>(), system)
        @Expect(r1.get(3.seconds), [1, 2, -1])
        let r2 = failing.recoverWithRetries(1, { _ => Some(Sources.from<Int64>([100, 200])) })
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(r2.get(3.seconds), [1, 2, 100, 200])
        // 备用源也失败且重试次数用完：流失败
        let r3 = failing.recoverWithRetries(1, { _ => Some(Sources.failed<Int64>(IllegalStateException("again"))) })
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(failureOf(r3, 3.seconds).getOrThrow().message, "again")
        // recover 不处理时照常失败
        let r4 = failing.recover({ _ => None<Int64> }).runWith(Sinks.seq<Int64>(), system)
        @Expect(failureOf(r4, 3.seconds).getOrThrow().message, "bad")
        system.terminate()
    }

    @TestCase
    public func testWatchTerminationAndErrorPropagation(): Unit {
        let system = CActor.system("dsl-watch")
        let (w1, r1) = Sources.range(1, 3).watchTermination().toMat(Sinks.seq<Int64>(), { w, r => (w, r) }).run(system)
        @Expect(r1.get(3.seconds), [1, 2, 3])
        @Expect(failureOf(w1, 3.seconds).isNone(), true)
        let (w2, r2) = Sources.failed<Int64>(IllegalStateException("x")).watchTermination()
            .toMat(Sinks.seq<Int64>(), { w, r => (w, r) }).run(system)
        @Expect(failureOf(w2, 3.seconds).getOrThrow().message, "x")
        @Expect(failureOf(r2, 3.seconds).getOrThrow().message, "x")
        // 下游取消（take）传到上游：无限源的终止 Promise 成功
        let (w3, r3) = Sources.repeat(0).watchTermination().take(2).toMat(Sinks.seq<Int64>(), { w, r => (w, r) })
            .run(system)
        @Expect(r3.get(3.seconds), [0, 0])
        @Expect(failureOf(w3, 3.seconds).isNone(), true)
        system.terminate()
    }

    @TestCase
    public func testInvalidArgumentsAreRejectedAtBlueprintTime(): Unit {
        @Expect(throwsIllegal({ => Sources.range(1, 2).mapAsync<Int64>(0, { x => TypedPromises.successful<Int64>(x) }) }), true)
        @Expect(throwsIllegal({ => Sources.range(1, 2).buffer(0, OverflowStrategy.DropHead) }), true)
        @Expect(throwsIllegal({ => Flows.grouped<Int64>(0) }), true)
        @Expect(throwsIllegal({ => Sources.queue<Int64>(0, OverflowStrategy.DropNew) }), true)
        @Expect(throwsIllegal({ => Sources.actorRef<Int64>(4, OverflowStrategy.Backpressure, { _ => false }, { _ => None<Exception> }) }), true)
        @Expect(throwsIllegal({ => StreamSettings().withInputBufferSize(0) }), true)
    }
}

func throwsIllegal<T>(f: () -> T): Bool {
    try {
        f()
        false
    } catch (_: IllegalArgumentException) {
        true
    }
}
