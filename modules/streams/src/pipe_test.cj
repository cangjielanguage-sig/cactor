/*
 * CActor Streams - 管道写法测试：source |> via(flow) |> to(sink) |> run(system)
 *
 * 管道段只是转调链式方法，这里要证明的是：写法本身在仓颉里成立（类型参数与 lambda 参数类型
 * 都由左侧推断）、结果与链式写法一致、物化值的保留方式一致，以及与 std.collection 的同名管道函数共存。
 * 所以测试里的 lambda 刻意不写参数类型。
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
public class StreamPipeTest {
    @TestCase
    public func pipeEqualsChain(): Unit {
        let system = CActor.system("pipe-eq")
        let enrich = Flows.create<Int64>().map<String>({ x => "v${x}" })
        // 管道写法：to 保留左侧物化值，要汇的结果就用 toMat 选右侧
        let piped = Sources.range(1, 10)
            |> filter { x => x % 2 == 0 }
            |> map { x => x * 10 }
            |> via(enrich)
            |> toMat(Sinks.seq<String>(), { _, r => r })
            |> run(system)
        let chained = Sources.range(1, 10)
            .filter({ x => x % 2 == 0 })
            .map<Int64>({ x => x * 10 })
            .via(enrich)
            .toMat(Sinks.seq<String>(), { _, r => r })
            .run(system)
        let expected = ["v20", "v40", "v60", "v80", "v100"]
        @Expect(piped.get(3.seconds), expected)
        @Expect(chained.get(3.seconds), expected)
        system.terminate()
    }

    @TestCase
    public func pipeOperatorsWithInferredLambdas(): Unit {
        let system = CActor.system("pipe-ops")
        let r1 = Sources.range(1, 10) |> drop(2) |> take(3) |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r1.get(3.seconds), [3, 4, 5])
        let r2 = Sources.range(1, 10) |> takeWhile { x => x < 4 } |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r2.get(3.seconds), [1, 2, 3])
        let r3 = Sources.range(1, 6) |> dropWhile { x => x < 4 } |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r3.get(3.seconds), [4, 5, 6])
        let r4 = Sources.range(1, 3) |> mapConcat { x => [x, x * 10] } |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r4.get(3.seconds), [1, 10, 2, 20, 3, 30])
        let r5 = Sources.range(1, 7) |> grouped(3) |> map { g => g.size } |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r5.get(3.seconds), [3, 3, 1])
        let r6 = Sources.range(1, 4) |> scan(0) { a, x => a + x } |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r6.get(3.seconds), [0, 1, 3, 6, 10])
        let r7 = Sources.range(1, 4) |> fold(0) { a, x => a + x } |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r7.get(3.seconds), [10])
        let r8 = Sources.range(1, 10) |> collect { x => if (x > 8) { Some(x) } else { None<Int64> } }
            |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r8.get(3.seconds), [9, 10])
        let r9 = Sources.range(1, 5) |> filterNot { x => x % 2 == 0 } |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r9.get(3.seconds), [1, 3, 5])
        // mapAsync / flatMapConcat 的尾随 lambda
        let r10 = Sources.range(1, 4) |> mapAsync(2) { x => TypedPromises.successful<Int64>(x * x) }
            |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r10.get(3.seconds), [1, 4, 9, 16])
        let r11 = Sources.range(1, 2) |> flatMapConcat { x => Sources.from<Int64>([x, x]) }
            |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r11.get(3.seconds), [1, 1, 2, 2])
        let r12 = Sources.range(1, 3) |> runFold(system, 0) { a, x => a + x }
        @Expect(r12.get(3.seconds), 6)
        system.terminate()
    }

    @TestCase
    public func pipeKeepsMaterializedValueLikeChain(): Unit {
        let system = CActor.system("pipe-mat")
        let seen = ArrayList<Int64>()
        let lock = Mutex()
        // to 保留左侧（队列）的物化值：管道里的 Mat 类型由 |> 从 Sources.queue 推断
        let record = Sinks.foreach<Int64>({ x => synchronized(lock) { seen.add(x) } })
        let queue = Sources.queue<Int64>(8, OverflowStrategy.Backpressure)
            |> map { x => x * 2 }
            |> to(record)
            |> run(system)
        @Expect(queue.offer(1).get(3.seconds).isEnqueued(), true)
        @Expect(queue.offer(2).get(3.seconds).isEnqueued(), true)
        queue.complete()
        @Expect(queue.watchCompletion().get(3.seconds), ())
        @Expect(awaitTrue(3.seconds, { => synchronized(lock) { seen.size == 2 } }), true)
        @Expect(synchronized(lock) { seen.toArray() }, [2, 4])
        // toMat 同时保留两侧
        let (q2, result) = Sources.queue<Int64>(8, OverflowStrategy.Backpressure)
            |> filter { x => x > 0 }
            |> toMat(Sinks.seq<Int64>(), { q, r => (q, r) })
            |> run(system)
        q2.offer(-1).get(3.seconds)
        q2.offer(5).get(3.seconds)
        q2.complete()
        @Expect(result.get(3.seconds), [5])
        system.terminate()
    }

    @TestCase
    public func flowsComposeWithPipe(): Unit {
        let system = CActor.system("pipe-flow")
        // Flow 也能用管道拼：左侧是 Flow 时选中 Flow 版本的 map / filter / via
        let tag = Flows.create<Int64>() |> map { x => "#${x}" }
        let pipeline = Flows.create<Int64>()
            |> filter { x => x % 3 == 0 }
            |> map { x => x + 1 }
            |> via(tag)
        let r = Sources.range(1, 9) |> via(pipeline) |> runWith(Sinks.seq<String>(), system)
        @Expect(r.get(3.seconds), ["#4", "#7", "#10"])
        // Flow |> to(sink) 得到新的汇
        let total = AtomicInt64(0)
        let add = Sinks.foreach<Int64>({ x => total.fetchAdd(x) })
        let sumSink = Flows.create<Int64>() |> map { x => x * 100 } |> to(add)
        Sources.range(1, 3) |> to(sumSink) |> run(system)
        @Expect(awaitTrue(3.seconds, { => total.load() == 600 }), true)
        system.terminate()
    }

    @TestCase
    public func coexistsWithStdCollectionPipes(): Unit {
        // std.collection 的 map / filter / fold / take 与本包同名函数同时导入：按 |> 左侧类型区分
        let xs = [1, 2, 3, 4, 5] |> filter { x => x % 2 == 1 } |> map { x => x * 10 } |> collectArray
        @Expect(xs, [10, 30, 50])
        let sum = [1, 2, 3] |> fold(0) { a, x => a + x }
        @Expect(sum, 6)
        let system = CActor.system("pipe-std")
        let r = Sources.from<Int64>(xs) |> take(2) |> map { x => x + 1 } |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r.get(3.seconds), [11, 31])
        system.terminate()
    }

    @TestCase
    public func pipeSupportsAsyncBoundaryAndSupervision(): Unit {
        let system = CActor.system("pipe-async")
        let r = Sources.range(1, 6)
            |> map { x => if (x == 3) { throw IllegalStateException("bad") } else { x } }
            |> withSupervision(StreamSupervision.resumingDecider)
            |> async()
            |> map { x => x * 2 }
            |> runWith(Sinks.seq<Int64>(), system)
        @Expect(r.get(3.seconds), [2, 4, 8, 10, 12])
        system.terminate()
    }
}
