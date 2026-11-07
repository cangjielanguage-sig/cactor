/*
 * CActor Streams - Reactive Streams 语义测试
 */
package cactor_streams.reactive

import std.time.*
import cactor_core.dsl.*
import cactor.api.CActor
import std.unittest.*
import std.unittest.testmacro.*

@Test
public class ReactiveStreamsTest {
    @TestCase
    public func testRunStreamTransformsAllElements(): Unit {
        let run = runStream(["a", "b", "c"], 3.seconds) { v => "n${v}" }
        @Expect(run.isSuccess(), true)
        @Expect(run.collected.size, 3)
        @Expect(run.collected[0], "na")
        @Expect(run.collected[2], "nc")
    }

    @TestCase
    public func testDemandSignalBackpressuresPublisher(): Unit {
        // 订阅者只声明 2 的需求：发布者最多发出 2 条，剩下的留在源里（内存不增长）
        let box = DemandBox()
        box.request(2)
        let source = ReactiveSource(["1", "2", "3", "4", "5"])
        let sent = source.publishTo(box, 200.millis)
        @Expect(sent, 2)
        @Expect(source.remaining(), 3)
        @Expect(box.pending(), 2)
        @Expect(box.outstandingDemand(), 0)
        // 再声明需求，剩下的元素继续发出
        box.request(3)
        let sent2 = source.publishTo(box, 200.millis)
        @Expect(sent2, 3)
        @Expect(source.remaining(), 0)
    }

    @TestCase
    public func testSinkCollectsOnDemand(): Unit {
        let sink = ReactiveSink()
        sink.request(3)
        sink.accept(Next("x"))
        sink.accept(Next("y"))
        @Expect(sink.awaitCount(2, 1.seconds), true)
        @Expect(sink.collected().size, 2)
        @Expect(sink.isCompleted(), false)
        sink.accept(Completed())
        @Expect(sink.isCompleted(), true)
        @Expect(sink.errorMessage().isNone(), true)
    }

    @TestCase
    public func testErrorSignalPropagates(): Unit {
        let sink = ReactiveSink()
        sink.request(1)
        sink.accept(Next("1"))
        sink.accept(Failed("工具调用失败"))
        @Expect(sink.errorMessage() ?? "", "工具调用失败")
        @Expect(sink.isCompleted(), false)
        // 终止之后的信号被忽略
        sink.accept(Next("2"))
        @Expect(sink.collected().size, 1)
    }

    @TestCase
    public func testCompletionNeedsNoDemand(): Unit {
        // 需求刚好等于元素数：完成信号不消耗需求，照样发出
        let box = DemandBox()
        box.request(1)
        let sent = ReactiveSource(["x"]).publishTo(box, 200.millis)
        @Expect(sent, 1)
        @Expect(box.pending(), 2)
        @Expect((box.poll().getOrThrow() as Next).isSome(), true)
        @Expect((box.poll().getOrThrow() as Completed).isSome(), true)
    }

    @TestCase
    public func testCloseCancelsWaitingPublisher(): Unit {
        let box = DemandBox()
        box.close()
        let start = MonoTime.now()
        @Expect(ReactiveSource(["1", "2"]).publishTo(box, 5.seconds), 0)
        @Expect(MonoTime.now() - start < 1.seconds, true)
    }

    @TestCase
    public func testDemandRequestedBeforeAttachReachesUpstream(): Unit {
        let sink = ReactiveSink()
        sink.request(2)
        let up = DemandBox()
        sink.attachUpstream(up)
        @Expect(up.outstandingDemand(), 2)
        // 连接之后的需求直接传给上游
        sink.request(3)
        @Expect(up.outstandingDemand(), 5)
        @Expect(sink.outstandingDemand(), 5)
    }

    @TestCase
    public func testPartialDemandReturnsWithoutWaitingForTimeout(): Unit {
        // 慢消费者只要 2 条：拿到 2 条就返回（不等超时），流没有完成，源被取消、剩余元素留在源里
        let system = CActor.system("reactive-partial")
        let sink = ReactiveSink()
        let processor = system.actorOf({ => ProcessorActor({ v => "n${v}" }, sink) }, "processor")
        sink.request(2)
        let source = ReactiveSource(["1", "2", "3", "4", "5"])
        let start = MonoTime.now()
        let run = runVia(source, processor, sink, 5.seconds)
        @Expect(MonoTime.now() - start < 3.seconds, true)
        @Expect(run.collected.size, 2)
        @Expect(run.collected[1], "n2")
        @Expect(run.completed, false)
        @Expect(source.remaining(), 3)
        system.terminate()
    }

    @TestCase
    public func testTransformFailureBecomesErrorSignal(): Unit {
        let run = runStream(["a", "boom", "c"], 3.seconds) { v =>
            if (v == "boom") {
                throw IllegalArgumentException("bad element")
            }
            v
        }
        @Expect(run.isSuccess(), false)
        @Expect(run.error ?? "", "bad element")
        @Expect(run.collected.size, 1)
    }

    @TestCase
    public func testEmptySourceCompletes(): Unit {
        let run = runStream([], 3.seconds) { v => v }
        @Expect(run.isSuccess(), true)
        @Expect(run.collected.size, 0)
    }
}
