/*
 * CActor Cluster - CRDT 并发读/合并回归测试
 *
 * 背景（P0-4）：`ReplicatorDigestTest.testIdleAntiEntropyOnlySendsDigests` 在全量并行跑时
 * 曾约 1/3 的运行 SIGSEGV（ORSet.merge → HashMap.contains，addr 0x8）。根因是
 * `other.entries.get(k) ?? HashSet()` 在缺键时创建临时空集，多线程下这些临时对象被并发触达；
 * 同时 `Replicator.get` 在锁内取出值引用、锁外遍历其内部 HashMap，与 Actor 线程的 merge 并发。
 *
 * 本文件把这些并发路径固化成回归用例：
 *   1. 纯 CRDT 层：多线程对两个"键集合不相交"的 ORSet/GCounter 反复 merge + 遍历 + 编解码；
 *   2. Replicator 层：多线程并发 get/setElements/keys/stats，同时 Actor 线程持续做本地更新
 *      （onLocalUpdate 内的 merge 与读路径必须串行化，不能崩溃）。
 *
 * 断言只依赖本地确定性条件（元素数、无异常），不依赖集群拓扑或选主时机，避免宿主机负载导致 flake。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_cluster.ddata.*
import cactor_foundation.network.{NetworkTransport}
import cactor_testkit.*

@Test
public class CrdtConcurrencyTest {
    /**
     * 两个副本的键集合不相交：merge 时每一侧都会走到"对方缺这个键"的分支
     * （正是旧实现 `?? HashSet()` 临时空集被并发触达的路径）。
     * 多线程同时 merge/遍历/编码解码，任何崩溃或元素缺失都会被抓到。
     */
    @TestCase
    public func testConcurrentMergeDisjointOrSets(): Unit {
        var a = ORSet()
        var b = ORSet()
        for (i in 0..50) {
            a = a.add("n1", "e${i}")
        }
        for (i in 50..100) {
            b = b.add("n2", "e${i}")
        }
        var ga = GCounter()
        var gb = GCounter()
        for (i in 0..50) {
            ga = ga.increment("n1", i)
            gb = gb.increment("n2", i)
        }
        // 闭包只能捕获 let（仓颉不允许逃逸捕获 var）
        let sa = a
        let sb = b
        let c1 = ga
        let c2 = gb
        let threads = 8
        let rounds = 400
        let errors = AtomicInt64(0)
        let errSize = AtomicInt64(0)
        let errContains = AtomicInt64(0)
        let errElements = AtomicInt64(0)
        let errDigest = AtomicInt64(0)
        let errCounter = AtomicInt64(0)
        let done = AtomicInt64(0)

        let futures = ArrayList<Future<Unit>>()
        for (t in 0..threads) {
            futures.add(spawn {
                for (_ in 0..rounds) {
                    let m = if (t % 2 == 0) { sa.merge(sb) } else { sb.merge(sa) }
                    if (m.size() != 100) {
                        errors.fetchAdd(1)
                        errSize.fetchAdd(1)
                    }
                    if (!m.contains("e0") || !m.contains("e99") || m.contains("nope")) {
                        errors.fetchAdd(1)
                        errContains.fetchAdd(1)
                    }
                    // 遍历 + 编码/解码：旧实现在这些读路径上与 merge 并发时会触碰内部 HashMap
                    let es = m.elements()
                    if (es.size != 100) {
                        errors.fetchAdd(1)
                        errElements.fetchAdd(1)
                    }
                    let value = ReplicatedValue.ORSetValue(m)
                    if (ReplicatedValue.decode(value.encode()).digest() != value.digest()) {
                        errors.fetchAdd(1)
                        errDigest.fetchAdd(1)
                    }
                    let g = c1.merge(c2)
                    // 两侧分量节点不同（n1 / n2）：合并后总值为两者之和
                    if (g.value() != c1.value() + c2.value()) {
                        errors.fetchAdd(1)
                        errCounter.fetchAdd(1)
                    }
                }
                done.fetchAdd(1)
            })
        }
        for (f in futures) {
            f.get()
        }
        @Expect(done.load(), threads)
        @Expect(errSize.load(), 0)
        @Expect(errContains.load(), 0)
        @Expect(errElements.load(), 0)
        @Expect(errDigest.load(), 0)
        @Expect(errCounter.load(), 0)
        @Expect(errors.load(), 0)
    }

    /**
     * Replicator 读路径与本地更新并发：get() 的"锁内编码 + 锁外解码"必须让
     * encode/merge 的遍历互斥。持续写入由 Replicator Actor 线程执行 merge，
     * 多个读线程同时读，验证不再出现并发遍历崩溃。
     */
    @TestCase
    public func testConcurrentReadsDuringLocalUpdates(): Unit {
        let node = Node("rep-crdt-concurrent", None<NetworkTransport>)
        let stop = AtomicBool(false)
        let keys: Int64 = 20
        let readers = 4
        let futures = ArrayList<Future<Unit>>()

        for (r in 0..readers) {
            futures.add(spawn {
                var i: Int64 = 0
                while (!stop.load() && i < 200000) {
                    let k = "set-${i % keys}"
                    match (node.rep.get(k)) {
                        case Some(ORSetValue(s)) =>
                            // 在锁外遍历返回的副本：get 返回独立副本后这才是安全的
                            s.elements()
                        case _ => ()
                    }
                    node.rep.setElements(k)
                    node.rep.keys()
                    node.rep.stats()
                    i++
                }
            })
        }

        // 持续本地更新（Replicator Actor 线程做 ORSet.add / merge）
        for (i in 0..4000) {
            node.rep.addToSetAsync("set-${i % keys}", "element-${i}")
        }
        let expected: Int64 = 4000 / keys
        @Expect(awaitCondition(Duration.second * 20, { =>
            node.rep.setElements("set-0").size >= expected
        }), true)
        stop.store(true)
        for (f in futures) {
            f.get()
        }
        @Expect(node.rep.setElements("set-0").size, expected)
        node.shutdown()
    }
}
