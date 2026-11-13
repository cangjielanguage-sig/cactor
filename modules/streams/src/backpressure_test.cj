/*
 * CActor Streams - 背压、异步边界与缓冲策略测试
 *
 * 验收（E60）："慢消费者面对无限流时内存有界"。这里是功能断言而不是压测：消费者由测试逐个拉取，
 * 每一步都检查"已生产 - 已消费"不超过图的理论上界（边界缓冲区 + 每条连接 1 个在途元素）。
 * 没有背压的实现会在第一次检查前就生产出远超上界的元素。
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

@Test
public class BackpressureTest {
    @TestCase
    public func testSlowConsumerOnInfiniteSourceAcrossAsyncBoundaryIsBounded(): Unit {
        let system = CActor.system("bp-async")
        let bufferSize: Int64 = 16
        let mat = Materializer.create(system).withSettings(StreamSettings().withInputBufferSize(bufferSize))
        let produced = AtomicInt64(0)
        let q = Sources.repeat(1).map<Int64>({ x =>
            produced.fetchAdd(1)
            x
        }).async().runWith(Sinks.queue<Int64>(), mat)
        // 上界：下游岛缓冲 bufferSize 个 + 上游岛 map 与边界之间 1 个在途
        let bound = bufferSize + 1
        // 消费者一个都没拉：上游填满信用后停下
        @Expect(awaitTrue(3.seconds, { => produced.load() >= bufferSize }), true)
        @Expect(produced.load() <= bound, true)
        var consumed: Int64 = 0
        for (_ in 0..200) {
            @Expect(q.pull().get(3.seconds), Some(1))
            consumed++
            @Expect(produced.load() - consumed <= bound, true)
        }
        // 稳态：上游被补到的信用总是追上消费，但永远不超过上界
        let consumedSoFar = consumed
        @Expect(awaitTrue(3.seconds, { => produced.load() - consumedSoFar >= bufferSize / 2 }), true)
        @Expect(produced.load() - consumed <= bound, true)
        q.cancel()
        system.terminate()
    }

    @TestCase
    public func testSlowConsumerOnInfiniteSourceFusedIsBounded(): Unit {
        let system = CActor.system("bp-fused")
        let produced = AtomicInt64(0)
        let q = Sources.repeat(1).map<Int64>({ x =>
            produced.fetchAdd(1)
            x
        }).runWith(Sinks.queue<Int64>(), system)
        for (i in 1..=50) {
            @Expect(q.pull().get(3.seconds), Some(1))
            // 融合执行：只在汇 pull 时才生产，最多比消费多 0 个
            @Expect(produced.load() <= i, true)
        }
        q.cancel()
        system.terminate()
    }

    @TestCase
    public func testAsyncBoundariesPreserveOrderAndContent(): Unit {
        let system = CActor.system("bp-order")
        let r = Sources.range(1, 2000).map<Int64>({ x => x * 2 }).async().filter({ x => x % 3 != 0 }).async(1)
            .map<Int64>({ x => x + 1 }).async(4).runWith(Sinks.seq<Int64>(), system)
        let expected = ArrayList<Int64>()
        for (x in 1..=2000) {
            if ((x * 2) % 3 != 0) {
                expected.add(x * 2 + 1)
            }
        }
        @Expect(r.get(5.seconds), expected.toArray())
        system.terminate()
    }

    @TestCase
    public func testFailureAndCancellationCrossAsyncBoundary(): Unit {
        let system = CActor.system("bp-cross")
        // 上游岛失败 → 下游岛的汇失败
        let failed = Sources.range(1, 10).map<Int64>({ x => if (x == 5) { throw IllegalStateException("up") } else { x } })
            .async().runWith(Sinks.seq<Int64>(), system)
        @Expect(failureOf(failed, 3.seconds).getOrThrow().message, "up")
        // 下游岛取消 → 上游岛的无限源停止
        let (w, r) = Sources.repeat(3).watchTermination().async().take(5)
            .toMat(Sinks.seq<Int64>(), { w, r => (w, r) }).run(system)
        @Expect(r.get(3.seconds), [3, 3, 3, 3, 3])
        @Expect(failureOf(w, 3.seconds).isNone(), true)
        // 汇放在独立的岛
        let r2 = Sources.range(1, 5).runWith(Sinks.seq<Int64>().async(), system)
        @Expect(r2.get(3.seconds), [1, 2, 3, 4, 5])
        system.terminate()
    }

    @TestCase
    public func testBufferOverflowStrategies(): Unit {
        let system = CActor.system("bp-buffer")
        @Expect(drainBuffered(system, OverflowStrategy.DropHead), [8, 9, 10])
        @Expect(drainBuffered(system, OverflowStrategy.DropTail), [1, 2, 10])
        @Expect(drainBuffered(system, OverflowStrategy.DropNew), [1, 2, 3])
        @Expect(drainBuffered(system, OverflowStrategy.DropBuffer), [10])
        @Expect(drainBuffered(system, OverflowStrategy.Backpressure), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        // Fail：缓冲区满时流失败
        let q = Sources.range(1, 10).buffer(3, OverflowStrategy.Fail).runWith(Sinks.queue<Int64>(), system)
        var failure = None<Exception>
        for (_ in 0..4) {
            if (failure.isNone()) {
                failure = failureOf(q.pull(), 3.seconds)
            }
        }
        @Expect((failure.getOrThrow() as BufferOverflowException).isSome(), true)
        system.terminate()
    }

    @TestCase
    public func testActorRefWithBackpressureAckProtocol(): Unit {
        let system = CActor.system("bp-ack")
        let rec = Recorder<String>()
        let acks = Recorder<TypedActorRef<Unit>>()
        let target = system.typedActorOf<AckSinkMsg>(TypedBehaviors.receive<AckSinkMsg>({ _, m =>
            match (m) {
                case AckSinkMsg.Init(ack) =>
                    rec.add("init")
                    acks.add(ack)
                case AckSinkMsg.Elem(x, ack) =>
                    rec.add("elem${x}")
                    acks.add(ack)
                case AckSinkMsg.Done => rec.add("done")
                case AckSinkMsg.Failed(e) => rec.add("failed:${e}")
            }
            TypedBehaviors.same<AckSinkMsg>()
        }), "ack-target")
        let produced = AtomicInt64(0)
        Sources.range(1, 3).map<Int64>({ x =>
            produced.fetchAdd(1)
            x
        }).runWith(Sinks.actorRefWithBackpressure<Int64, AckSinkMsg, Unit>(target, { ack, x => AckSinkMsg.Elem(x, ack) },
            { ack => AckSinkMsg.Init(ack) }, AckSinkMsg.Done, { e => AckSinkMsg.Failed(e.message) }), system)
        @Expect(awaitTrue(3.seconds, { => rec.size() == 1 }), true)
        @Expect(rec.snapshot()[0], "init")
        // 未确认 init 之前不会请求任何元素
        @Expect(produced.load(), 0)
        for (i in 1..=3) {
            acks.snapshot()[i - 1].tell(())
            @Expect(awaitTrue(3.seconds, { => rec.size() == i + 1 }), true)
            @Expect(rec.snapshot()[i], "elem${i}")
            // 一次只有一条未确认：已生产的不超过已确认数
            @Expect(produced.load() <= i, true)
        }
        // 最后一条确认后才发完成消息
        @Expect(rec.snapshot().size, 4)
        acks.snapshot()[3].tell(())
        @Expect(awaitTrue(3.seconds, { => rec.size() == 5 }), true)
        @Expect(rec.snapshot()[4], "done")
        // 失败：立即发失败消息
        let rec2Size = rec.size()
        Sources.failed<Int64>(IllegalStateException("oops")).runWith(
            Sinks.actorRefWithBackpressure<Int64, AckSinkMsg, Unit>(target, { ack, x => AckSinkMsg.Elem(x, ack) },
                { ack => AckSinkMsg.Init(ack) }, AckSinkMsg.Done, { e => AckSinkMsg.Failed(e.message) }), system)
        @Expect(awaitTrue(3.seconds, { => rec.snapshot().size >= rec2Size + 2 }), true)
        @Expect(rec.snapshot()[rec2Size + 1], "failed:oops")
        system.terminate()
    }
}

enum AckSinkMsg {
    | Init(TypedActorRef<Unit>)
    | Elem(Int64, TypedActorRef<Unit>)
    | Done
    | Failed(String)
}

/** 等上游跑完（buffer 已按策略处理完溢出）后逐个拉取剩余元素 */
func drainBuffered(system: CActorSystem, strategy: OverflowStrategy): Array<Int64> {
    let (w, q) = Sources.range(1, 10).watchTermination().buffer(3, strategy)
        .toMat(Sinks.queue<Int64>(), { w, q => (w, q) }).run(system)
    let out = ArrayList<Int64>()
    if (let Backpressure <- strategy) {
        // 背压：上游不会提前跑完，边拉边收
        ()
    } else {
        w.get(3.seconds)
    }
    while (true) {
        match (q.pull().get(3.seconds)) {
            case Some(v) => out.add(v)
            case None => break
        }
    }
    out.toArray()
}
