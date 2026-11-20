/*
 * CActor Platform.KV - 单节点实现（F6 / AR9）
 *
 * 覆盖往返、前缀列举、删除，以及并发 incr 的原子性。
 */
package cactor_tests.unit.platform.kv

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_platform.kv.*

@Test
public class KvStoreTest {
    @TestCase
    public func memoryKvRoundTripsAndLists(): Unit {
        let kv = MemoryKvStore()
        @Expect(kv.get("a").isNone(), true)
        kv.put("a", "1")
        kv.put("ab", "2")
        kv.put("b", "3")
        @Expect(kv.get("a") ?? "", "1")
        @Expect(kv.keys().size, 3)
        let prefix = kv.list("a")
        @Expect(prefix.size, 2)
        @Expect(kv.delete("a"), true)
        @Expect(kv.delete("a"), false)
        @Expect(kv.get("a").isNone(), true)
        @Expect(kv.list("a").size, 1)
    }

    @TestCase
    public func kvIncrIsAtomicUnderConcurrency(): Unit {
        let kv = MemoryKvStore()
        let workers = 8
        let perWorker = 1000
        var futures = ArrayList<Future<Unit>>()
        for (_ in 0..workers) {
            futures.add(spawn {
                for (_ in 0..perWorker) {
                    kv.incr("counter", 1)
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        @Expect(kv.get("counter") ?? "", (workers * perWorker).toString())
        @Expect(kv.incr("counter", -5), Int64(workers * perWorker - 5))
    }
}
