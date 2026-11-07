/*
 * CActor Streams - DSL 语义回归
 *   - distinct：保序去重（此前恒只剩第一个元素）；只对可哈希、可判等的元素类型提供（约束泛型）
 *   - 空源：filter 全部过滤掉 / Source.empty 交给 Sink 按自身语义处理（此前直接抛异常）
 *   - runAsync：在轻量线程上异步执行、Sink 只执行一次（此前执行两次并以固定名创建 Actor）
 */
package cactor_streams.dsl

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_streams.core.{SimpleStream, SimpleSource, SimpleSink, StreamResult}

@Test
public class StreamDslSemanticsTest {
    @TestCase
    public func distinctKeepsFirstOccurrenceInOrder(): Unit {
        let out = StreamSource<Int64>([3, 1, 3, 2, 1, 2, 4], "src").distinct().getElements()
        @Expect(out, [3, 1, 2, 4])
    }

    @TestCase
    public func distinctFlowRemovesDuplicates(): Unit {
        let out = StreamSource<String>(["a", "b", "a", "c", "b"], "src")
            .via(FlowFactory.distinct<String>())
            .runWith(SinkFactory.collect<String>())
        @Expect(out, ["a", "b", "c"])
    }

    @TestCase
    public func emptySourceFlowsToSink(): Unit {
        // filter 全部过滤掉后仍是合法的流：fold 返回初值、collect 返回空数组
        let total = StreamSource<Int64>([1, 2, 3], "src")
            .filter({ x => x > 100 })
            .runWith(SinkFactory.fold<Int64, Int64>(0, { acc, x => acc + x }))
        @Expect(total, 0)
        let none = SourceFactory.empty<Int64>().runWith(SinkFactory.collect<Int64>())
        @Expect(none.size, 0)
    }

    @TestCase
    public func runAsyncRunsSinkExactlyOnceOffThread(): Unit {
        let calls = AtomicInt64(0)
        let stream = SimpleStream<Int64, Int64>(
            SimpleSource<Int64>([1, 2, 3], "src"),
            SimpleSink<Int64, Int64>({ xs =>
                calls.fetchAdd(1)
                var s: Int64 = 0
                for (x in xs) { s += x }
                s
            }, "sum"))
        let future = stream.runAsync()
        match (future.get(Duration.second * 5)) {
            case StreamResult.Success(v) => @Expect(v, 6)
            case _ => @Expect(false, true)
        }
        @Expect(calls.load(), 1)
        // 可重复运行（此前第二次调用因 Actor 重名抛异常）
        let again = stream.runAsync().get(Duration.second * 5)
        match (again) {
            case StreamResult.Success(v) => @Expect(v, 6)
            case _ => @Expect(false, true)
        }
        @Expect(calls.load(), 2)
    }
}
