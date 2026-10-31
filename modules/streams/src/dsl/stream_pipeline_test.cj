/*
 * CActor Streams - 管道 API 测试：Source |> Flow |> Sink
 */
package cactor_streams.dsl

import std.unittest.*
import std.unittest.testmacro.*

@Test
public class StreamPipelineTest {
    @TestCase
    public func testPipelineEqualsChainedApi(): Unit {
        let source = StreamSource<Int64>([1, 2, 3, 4], "src")
        let flow = StreamFlow<Int64, String>({ elements =>
            Array<String>(elements.size, { i => "n${elements[i]}" })
        }, "flow")
        let sink = StreamSink<String, Int64>({ elements =>
            var total: Int64 = 0
            for (e in elements) {
                total += e.size
            }
            total
        }, "sink")

        // 管道写法
        let piped = source |> via(flow) |> to(sink)
        // 链式写法（同一语义）
        let chained = source.via(flow).to(sink).run()
        @Expect(piped, chained)
    }

    @TestCase
    public func testPipelineWithKeep(): Unit {
        let source = StreamSource<Int64>([1, 2, 3, 4, 5, 6], "src")
        let flow = keep<Int64>({ x => x % 2 == 0 })
        let sink = StreamSink<Int64, Int64>({ elements =>
            var total: Int64 = 0
            for (e in elements) {
                total += e
            }
            total
        }, "sum")
        let result = source |> via(flow) |> to(sink)
        // 2 + 4 + 6 = 12
        @Expect(result, 12)
    }

    @TestCase
    public func testSinkConsumeMatchesProcess(): Unit {
        let source = StreamSource<Int64>([1, 2, 3], "src")
        let sink = StreamSink<Int64, Int64>({ elements => Int64(elements.size) }, "count")
        @Expect(sink.consume(source), 3)
        @Expect(sink.process(source.getElements()), 3)
    }
}
