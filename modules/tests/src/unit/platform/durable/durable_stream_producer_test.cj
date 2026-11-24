/*
 * CActor Platform.Durable - producer 状态（PRODUCER_STATE_TTL_MS）
 *
 * 覆盖：按 producerId 记录最近写入 offset、同 store 新实例可读回、超过保留期自动清理、
 * fork 时把源流的 producer 位置复制到新流。
 */
package cactor_tests.unit.platform.durable

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_persistence.{MemoryDurableStateStore}
import cactor_platform.durable.*

@Test
public class DurableStreamProducerTest {
    @TestCase
    public func producerStateTracksLastOffsetAcrossInstances(): Unit {
        let store = MemoryDurableStateStore()
        let reg = DurableStreamRegistry(store)
        let s = reg.stream("producer/room")
        s.append("a", producerId: "p1")
        s.append("b", producerId: "p1")
        s.append("c", producerId: "p2")

        @Expect(s.producerState("p1").getOrThrow().lastOffset, 2)
        @Expect(s.producerState("p2").getOrThrow().lastOffset, 3)
        @Expect(s.producerState("nobody").isNone(), true)
        @Expect(s.producers().size, 2)

        // 同一 store 的新注册表：生产者状态随记录一起持久化
        let reg2 = DurableStreamRegistry(store)
        let s2 = reg2.stream("producer/room")
        @Expect(s2.producerState("p1").getOrThrow().lastOffset, 2)
    }

    @TestCase
    public func producerStateExpiresAfterTtl(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore(), producerStateTtlMillis: 300)
        let s = reg.stream("producer/ttl")
        s.append("x", producerId: "p1")
        @Expect(s.producers().size, 1)

        sleep(Duration.millisecond * 400)
        @Expect(s.producers().size, 0)
        @Expect(s.producerState("p1").isNone(), true)

        // 再次写入即重新登记，offset 继续单调
        s.append("y", producerId: "p1")
        let back = s.producerState("p1").getOrThrow()
        @Expect(back.lastOffset, 2)
    }

    @TestCase
    public func forkCopiesProducerState(): Unit {
        let reg = DurableStreamRegistry(MemoryDurableStateStore())
        let src = reg.stream("producer/src")
        src.append("a", producerId: "p1")
        src.append("b", producerId: "p2")
        src.append("c", producerId: "p1")

        let forked = src.fork("producer/fork", 2)
        // 只复制前两条：p1 的最后位置是 1、p2 是 2
        @Expect(forked.producerState("p1").getOrThrow().lastOffset, 1)
        @Expect(forked.producerState("p2").getOrThrow().lastOffset, 2)
        @Expect(forked.producers().size, 2)
    }
}
