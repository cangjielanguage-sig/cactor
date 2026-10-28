/*
 * 队列并发压力测试
 * 多线程同时读写，检查：不丢、不重、单生产者内有序
 */
package cactor_foundation.queue

import std.sync.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*

// 元素编码：producerId * STRIDE + seq
let STRIDE: Int64 = 1_000_000

/** 检查结果：每个生产者的序号严格递增且完整 */
func verifyPerProducer(received: ArrayList<Int64>, producers: Int64, perProducer: Int64): (Int64, Int64) {
    var outOfOrder: Int64 = 0
    var missingOrDup: Int64 = 0
    let next = Array<Int64>(producers, { _ => 0 })
    let seen = Array<Int64>(producers, { _ => 0 })
    for (v in received) {
        let p = v / STRIDE
        let s = v % STRIDE
        if (s != next[p]) {
            outOfOrder++
        }
        next[p] = s + 1
        seen[p] += 1
    }
    for (p in 0..producers) {
        if (seen[p] != perProducer) {
            missingOrDup++
        }
    }
    (outOfOrder, missingOrDup)
}

@Test
public class QueueStressTest {
    @TestCase
    func mpscManyProducersOneConsumer(): Unit {
        let producers: Int64 = 8
        let perProducer: Int64 = 20_000
        let total = producers * perProducer
        let q = MPSCQueue<Int64>(64)
        let rejected = AtomicInt64(0)
        let futures = ArrayList<Future<Unit>>()
        for (p in 0..producers) {
            futures.add(spawn {
                for (i in 0..perProducer) {
                    while (!q.offer(p * STRIDE + i)) {
                        rejected.fetchAdd(1)
                    }
                }
            })
        }
        let received = ArrayList<Int64>()
        while (received.size < total) {
            match (q.poll()) {
                case Some(v) => received.add(v)
                case None => ()
            }
        }
        for (f in futures) {
            f.get()
        }
        let (outOfOrder, bad) = verifyPerProducer(received, producers, perProducer)
        @Expect(outOfOrder, 0)
        @Expect(bad, 0)
        @Expect(q.isEmpty(), true)
        @Expect(q.poll().isNone(), true)
    }

    @TestCase
    func mpscBatchOperations(): Unit {
        let q = MPSCQueue<Int64>(16)
        @Expect(q.offerBatch([1, 2, 3, 4, 5]), 5)
        @Expect(q.size(), 5)
        let out = q.pollBatch(3)
        @Expect(out.size, 3)
        @Expect(out[0], 1)
        @Expect(out[2], 3)
        @Expect(q.pollBatch(10).size, 2)
        @Expect(q.isEmpty(), true)
    }

    @TestCase
    func lockFreeMpmcConcurrent(): Unit {
        let producers: Int64 = 4
        let consumers: Int64 = 4
        let perProducer: Int64 = 20_000
        let total = producers * perProducer
        let q = LockFreeQueue<Int64>(1024)
        let consumed = AtomicInt64(0)
        let sum = AtomicInt64(0)
        let seen = Array<AtomicInt64>(total, { _ => AtomicInt64(0) })
        let futures = ArrayList<Future<Unit>>()
        for (p in 0..producers) {
            futures.add(spawn {
                for (i in 0..perProducer) {
                    let v = p * perProducer + i
                    while (!q.enqueue(v)) {
                        ()
                    }
                }
            })
        }
        for (_ in 0..consumers) {
            futures.add(spawn {
                while (consumed.load() < total) {
                    match (q.dequeue()) {
                        case Some(v) =>
                            seen[v].fetchAdd(1)
                            sum.fetchAdd(v)
                            consumed.fetchAdd(1)
                        case None => ()
                    }
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        var dupOrMissing: Int64 = 0
        for (s in seen) {
            if (s.load() != 1) {
                dupOrMissing++
            }
        }
        @Expect(consumed.load(), total)
        @Expect(dupOrMissing, 0)
        @Expect(sum.load(), total * (total - 1) / 2)
        @Expect(q.isEmpty(), true)
    }

    @TestCase
    func lockFreeBoundedSemantics(): Unit {
        let q = LockFreeQueue<Int64>(4)
        @Expect(q.capacity(), 4)
        for (i in 0..4) {
            @Expect(q.enqueue(i), true)
        }
        // 满时拒绝
        @Expect(q.enqueue(99), false)
        @Expect(q.size(), 4)
        @Expect(q.dequeue(), Some(0))
        @Expect(q.enqueue(4), true)
        let rest = q.dequeueBatch(10)
        @Expect(rest.size, 4)
        @Expect(rest[3], 4)
        @Expect(q.dequeue().isNone(), true)
        q.enqueueBatch([7, 8])
        q.clear()
        @Expect(q.isEmpty(), true)
    }

    @TestCase
    func spscOneProducerOneConsumer(): Unit {
        let n: Int64 = 200_000
        let q = SPSCQueue<Int64>(256)
        let producer = spawn {
            for (i in 0..n) {
                while (!q.offer(i)) {
                    ()
                }
            }
        }
        var expected: Int64 = 0
        var outOfOrder: Int64 = 0
        while (expected < n) {
            match (q.poll()) {
                case Some(v) =>
                    if (v != expected) {
                        outOfOrder++
                    }
                    expected++
                case None => ()
            }
        }
        producer.get()
        @Expect(outOfOrder, 0)
        @Expect(q.isEmpty(), true)
    }
}
