/*
 * CActor Streams - mapAsync / ask、Source.queue / actorRef、流内监督、kill switch、Actor 内物化测试
 */
package cactor_streams

import std.collection.*
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.dsl.*
import cactor_core.typed.*
import cactor.api.{CActor, CActorSystem}

/** 由测试手动完成的异步调用：记录每个元素对应的 Promise 与同时在途的峰值 */
class ManualAsync {
    let lock = Mutex()
    let promises = ArrayList<(Int64, TypedPromise<Int64>)>()
    var inFlight: Int64 = 0
    var peak: Int64 = 0

    func call(x: Int64): TypedPromise<Int64> {
        let p = TypedPromise<Int64>()
        synchronized(lock) {
            promises.add((x, p))
            inFlight++
            if (inFlight > peak) { peak = inFlight }
        }
        p
    }

    func started(): Int64 {
        synchronized(lock) { promises.size }
    }

    func complete(index: Int64): Unit {
        let (x, p) = synchronized(lock) {
            inFlight--
            promises[index]
        }
        p.trySuccess(x * 100)
    }

    func peakInFlight(): Int64 {
        synchronized(lock) { peak }
    }
}

enum DoubleCmd {
    | Double(Int64, TypedActorRef<Int64>)
    | Ignore(Int64, TypedActorRef<Int64>)
}

@Test
public class AsyncAndControlTest {
    @TestCase
    public func testMapAsyncPreservesOrderAndBoundsParallelism(): Unit {
        let system = CActor.system("async-ordered")
        let calls = ManualAsync()
        let r = Sources.range(1, 9).mapAsync<Int64>(3, { x => calls.call(x) }).runWith(Sinks.seq<Int64>(), system)
        // 每批 3 个按倒序完成；下一批只有在前一批腾出名额后才会开始
        var batch: Int64 = 0
        while (batch < 3) {
            let target = (batch + 1) * 3
            @Expect(awaitTrue(3.seconds, { => calls.started() == target }), true)
            @Expect(calls.peakInFlight() <= 3, true)
            calls.complete(target - 1)
            calls.complete(target - 2)
            calls.complete(target - 3)
            batch++
        }
        @Expect(r.get(3.seconds), [100, 200, 300, 400, 500, 600, 700, 800, 900])
        @Expect(calls.peakInFlight(), 3)
        system.terminate()
    }

    @TestCase
    public func testMapAsyncUnorderedEmitsInCompletionOrder(): Unit {
        let system = CActor.system("async-unordered")
        let calls = ManualAsync()
        let out = Recorder<Int64>()
        let done = Sources.range(1, 3).mapAsyncUnordered<Int64>(3, { x => calls.call(x) })
            .runForeach({ x => out.add(x) }, system)
        @Expect(awaitTrue(3.seconds, { => calls.started() == 3 }), true)
        calls.complete(2)
        @Expect(awaitTrue(3.seconds, { => out.size() == 1 }), true)
        calls.complete(0)
        @Expect(awaitTrue(3.seconds, { => out.size() == 2 }), true)
        calls.complete(1)
        done.get(3.seconds)
        @Expect(out.snapshot(), [300, 100, 200])
        system.terminate()
    }

    @TestCase
    public func testMapAsyncFailureFollowsSupervision(): Unit {
        let system = CActor.system("async-failure")
        let f = { x: Int64 =>
            if (x == 2) { TypedPromises.failed<Int64>(IllegalStateException("async-bad")) } else { TypedPromises.successful<Int64>(x) }
        }
        let failed = Sources.range(1, 3).mapAsync<Int64>(2, f).runWith(Sinks.seq<Int64>(), system)
        @Expect(failureOf(failed, 3.seconds).getOrThrow().message, "async-bad")
        let resumed = Sources.range(1, 3).mapAsync<Int64>(2, f).withSupervision(StreamSupervision.resumingDecider)
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(resumed.get(3.seconds), [1, 3])
        system.terminate()
    }

    @TestCase
    public func testAskStageQueriesTypedActor(): Unit {
        let system = CActor.system("async-ask")
        let doubler = system.typedActorOf<DoubleCmd>(TypedBehaviors.receive<DoubleCmd>({ _, m =>
            match (m) {
                case DoubleCmd.Double(x, replyTo) => replyTo << x * 2
                case DoubleCmd.Ignore(_, _) => ()
            }
            TypedBehaviors.same<DoubleCmd>()
        }), "doubler")
        let r = Sources.range(1, 20).ask<DoubleCmd, Int64>(4, doubler, { x, r => DoubleCmd.Double(x, r) }, 3.seconds)
            .runWith(Sinks.seq<Int64>(), system)
        let expected = Array<Int64>(20, { i => (i + 1) * 2 })
        @Expect(r.get(5.seconds), expected)
        // 不回复：ask 超时让流失败（默认 Stop）
        let timedOut = Sources.single(1).ask<DoubleCmd, Int64>(1, doubler, { x, r => DoubleCmd.Ignore(x, r) }, 50.millis)
            .runWith(Sinks.seq<Int64>(), system)
        @Expect((failureOf(timedOut, 3.seconds).getOrThrow() as TypedAskTimeoutException).isSome(), true)
        system.terminate()
    }

    @TestCase
    public func testSourceQueueOverflowStrategies(): Unit {
        let system = CActor.system("queue-overflow")
        // 下游（Sink.queue）不拉取时，队列状态完全由 offer 决定，结果是确定的
        let qDropNew = queueRun(system, OverflowStrategy.DropNew)
        @Expect(qDropNew[0], ["E", "E", "D", "D"])
        @Expect(qDropNew[1], [1, 2])
        let qDropHead = queueRun(system, OverflowStrategy.DropHead)
        @Expect(qDropHead[0], ["E", "E", "E", "E"])
        @Expect(qDropHead[1], [3, 4])
        let qDropTail = queueRun(system, OverflowStrategy.DropTail)
        @Expect(qDropTail[0], ["E", "E", "E", "E"])
        @Expect(qDropTail[1], [1, 4])
        let qDropBuffer = queueRun(system, OverflowStrategy.DropBuffer)
        @Expect(qDropBuffer[0], ["E", "E", "E", "E"])
        @Expect(qDropBuffer[1], [3, 4])
        system.terminate()
    }

    @TestCase
    public func testSourceQueueBackpressureAndFail(): Unit {
        let system = CActor.system("queue-bp")
        let (sq, q) = Sources.queue<Int64>(1, OverflowStrategy.Backpressure)
            .toMat(Sinks.queue<Int64>(), { a, b => (a, b) }).run(system)
        @Expect(sq.offer(1).get(3.seconds).isEnqueued(), true)
        // 缓冲区满：offer 挂起，直到消费者腾出位置
        let pending = sq.offer(2)
        @Expect(pending.isCompleted(), false)
        // 同时只允许 1 个挂起的 offer
        @Expect(sq.offer(3).get(3.seconds).toString().startsWith("Failure"), true)
        @Expect(q.pull().get(3.seconds), Some(1))
        @Expect(pending.get(3.seconds).isEnqueued(), true)
        sq.complete()
        @Expect(q.pull().get(3.seconds), Some(2))
        @Expect(q.pull().get(3.seconds), None<Int64>)
        @Expect(failureOf(sq.watchCompletion(), 3.seconds).isNone(), true)
        @Expect(sq.offer(9).get(3.seconds).toString(), "QueueClosed")
        // Fail：溢出让流失败
        let (fq, q2) = Sources.queue<Int64>(1, OverflowStrategy.Fail)
            .toMat(Sinks.queue<Int64>(), { a, b => (a, b) }).run(system)
        fq.offer(1)
        @Expect(fq.offer(2).get(3.seconds).toString().startsWith("Failure"), true)
        @Expect((failureOf(q2.pull(), 3.seconds).getOrThrow() as BufferOverflowException).isSome(), true)
        // fail()：流以给定原因失败
        let (eq, r3) = Sources.queue<Int64>(4, OverflowStrategy.DropNew)
            .toMat(Sinks.seq<Int64>(), { a, b => (a, b) }).run(system)
        eq.fail(IllegalStateException("producer-died"))
        @Expect(failureOf(r3, 3.seconds).getOrThrow().message, "producer-died")
        system.terminate()
    }

    @TestCase
    public func testActorRefSource(): Unit {
        let system = CActor.system("actor-source")
        let (ref, r) = Sources.actorRef<Int64>(8, OverflowStrategy.DropNew, { x => x < 0 }, { _ => None<Exception> })
            .toMat(Sinks.seq<Int64>(), { a, b => (a, b) }).run(system)
        ref << 1 << 2 << 3 << -1
        @Expect(r.get(3.seconds), [1, 2, 3])
        let (ref2, r2) = Sources.actorRef<String>(8, OverflowStrategy.DropHead, { _ => false },
            { s => if (s == "fail") { Some<Exception>(IllegalStateException("by-message")) } else { None<Exception> } })
            .toMat(Sinks.seq<String>(), { a, b => (a, b) }).run(system)
        ref2 << "a" << "fail"
        @Expect(failureOf(r2, 3.seconds).getOrThrow().message, "by-message")
        system.terminate()
    }

    @TestCase
    public func testSupervisionStopResumeRestart(): Unit {
        let system = CActor.system("supervision")
        let bad = { x: Int64 => if (x == 3) { throw IllegalArgumentException("three") } else { x } }
        let stopped = Sources.range(1, 5).map<Int64>(bad).runWith(Sinks.seq<Int64>(), system)
        @Expect(failureOf(stopped, 3.seconds).getOrThrow().message, "three")
        let resumed = Sources.range(1, 5).map<Int64>(bad).withSupervision(StreamSupervision.resumingDecider)
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(resumed.get(3.seconds), [1, 2, 4, 5])
        // 有状态阶段：Resume 保留累积值，Restart 重置为 zero
        let scanF = { a: Int64, x: Int64 => if (x == 3) { throw IllegalArgumentException("three") } else { a + x } }
        let scanResume = Sources.range(1, 5).scan<Int64>(0, scanF).withSupervision(StreamSupervision.resumingDecider)
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(scanResume.get(3.seconds), [0, 1, 3, 7, 12])
        let scanRestart = Sources.range(1, 5).scan<Int64>(0, scanF).withSupervision(StreamSupervision.restartingDecider)
            .runWith(Sinks.seq<Int64>(), system)
        @Expect(scanRestart.get(3.seconds), [0, 1, 3, 4, 9])
        // 按异常类型决定；内层策略优先于外层
        let decider: Decider = { e => if (e is IllegalArgumentException) { SupervisionDirective.Resume } else { SupervisionDirective.Stop } }
        let mixed = Sources.range(1, 5).map<Int64>(bad).withSupervision(decider)
            .map<Int64>({ x => if (x == 4) { throw IllegalStateException("four") } else { x } })
            .withSupervision(StreamSupervision.resumingDecider).runWith(Sinks.seq<Int64>(), system)
        @Expect(mixed.get(3.seconds), [1, 2, 5])
        let innerWins = Sources.range(1, 5).map<Int64>(bad).withSupervision(StreamSupervision.stoppingDecider)
            .withSupervision(StreamSupervision.resumingDecider).runWith(Sinks.seq<Int64>(), system)
        @Expect(failureOf(innerWins, 3.seconds).getOrThrow().message, "three")
        // 汇也遵循监督策略
        let rec = Recorder<Int64>()
        Sources.range(1, 4).runWith(Sinks.foreach<Int64>({ x =>
            if (x == 2) { throw IllegalArgumentException("sink") }
            rec.add(x)
        }).withSupervision(StreamSupervision.resumingDecider), system).get(3.seconds)
        @Expect(rec.snapshot(), [1, 3, 4])
        system.terminate()
    }

    @TestCase
    public func testUniqueKillSwitchShutdownAndAbort(): Unit {
        let system = CActor.system("kill-unique")
        let (ks, done) = Sources.repeat(1).viaMat<Int64, UniqueKillSwitch, UniqueKillSwitch>(KillSwitches.single<Int64>(), { _, k => k })
            .toMat(Sinks.ignore<Int64>(), { k, d => (k, d) }).run(system)
        ks.shutdown()
        @Expect(failureOf(done, 3.seconds).isNone(), true)
        // 跨异步边界：上游无限源也被取消
        let (w, ks2, done2) = Sources.repeat(1).watchTermination().async()
            .viaMat<Int64, UniqueKillSwitch, (TypedPromise<Unit>, UniqueKillSwitch)>(KillSwitches.single<Int64>(), { w, k => (w, k) })
            .toMat(Sinks.ignore<Int64>(), { wk, d => (wk[0], wk[1], d) }).run(system)
        ks2.abort(IllegalStateException("killed"))
        @Expect(failureOf(done2, 3.seconds).getOrThrow().message, "killed")
        @Expect(failureOf(w, 3.seconds).isNone(), true)
        // 流结束后再调用是空操作
        ks2.shutdown()
        system.terminate()
    }

    @TestCase
    public func testSharedKillSwitchStopsAllStreams(): Unit {
        let system = CActor.system("kill-shared")
        let shared = KillSwitches.shared("all")
        let d1 = Sources.repeat(1).via(shared.flow<Int64>()).runWith(Sinks.ignore<Int64>(), system)
        let d2 = Sources.repeat("x").via(shared.flow<String>()).runWith(Sinks.ignore<String>(), system)
        shared.shutdown()
        @Expect(failureOf(d1, 3.seconds).isNone(), true)
        @Expect(failureOf(d2, 3.seconds).isNone(), true)
        // 触发之后物化的流立即结束
        let d3 = Sources.repeat(1).via(shared.flow<Int64>()).runWith(Sinks.ignore<Int64>(), system)
        @Expect(failureOf(d3, 3.seconds).isNone(), true)
        system.terminate()
    }

    @TestCase
    public func testStreamInsideActorStopsWithHost(): Unit {
        let system = CActor.system("ctx-materializer")
        let holder = Recorder<TypedPromise<Unit>>()
        let host = system.typedActorOf<String>(TypedBehaviors.setup<String>({ ctx =>
            // 岛是宿主的子 Actor：宿主停止时流被终止
            let done = Sources.repeat(1).runWith(Sinks.ignore<Int64>(), Materializer.create<String>(ctx))
            holder.add(done)
            TypedBehaviors.receive<String>({ _, _ => TypedBehaviors.stopped<String>() })
        }), "host")
        @Expect(awaitTrue(3.seconds, { => holder.size() == 1 }), true)
        host << "stop"
        let failure = failureOf(holder.snapshot()[0], 3.seconds)
        @Expect((failure.getOrThrow() as AbruptStageTerminationException).isSome(), true)
        system.terminate()
    }
}

/** 容量 2 的队列依次 offer 1..4，然后完成并读出全部：返回（每次 offer 的结果，最终元素） */
func queueRun(system: CActorSystem, strategy: OverflowStrategy): (Array<String>, Array<Int64>) {
    let (sq, q) = Sources.queue<Int64>(2, strategy).toMat(Sinks.queue<Int64>(), { a, b => (a, b) }).run(system)
    let results = ArrayList<String>()
    for (i in 1..=4) {
        let r = sq.offer(i).get(3.seconds)
        results.add(match (r) {
            case Enqueued => "E"
            case Dropped => "D"
            case _ => r.toString()
        })
    }
    sq.complete()
    let out = ArrayList<Int64>()
    while (true) {
        match (q.pull().get(3.seconds)) {
            case Some(v) => out.add(v)
            case None => break
        }
    }
    (results.toArray(), out.toArray())
}
