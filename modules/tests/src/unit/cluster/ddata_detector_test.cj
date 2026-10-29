/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R6 测试：CRDT 合并定律与收敛、Phi Accrual 故障检测器
 */
package cactor_tests.unit.cluster

import std.time.*
import std.random.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_cluster.ddata.*
import cactor_cluster.detector.*

/** 手动时钟（毫秒） */
class ManualClock {
    var now: Int64 = 0
    func advance(ms: Int64): Unit {
        now += ms
    }
}

func detector(clock: ManualClock): PhiAccrualFailureDetector {
    PhiAccrualFailureDetector(8.0, 1000, Duration.millisecond * 10, Duration.Zero, Duration.millisecond * 100,
        { => clock.now })
}

@Test
public class CrdtTest {
    @TestCase
    public func testGCounterLaws(): Unit {
        let a = GCounter().increment("a", 3)
        let b = GCounter().increment("b", 5).increment("a", 1)
        let c = GCounter().increment("c", 2)
        @Expect(a.merge(b) == b.merge(a), true)                         // 交换律
        @Expect(a.merge(b).merge(c) == a.merge(b.merge(c)), true)       // 结合律
        @Expect(a.merge(a) == a, true)                                  // 幂等律
        @Expect(a.merge(b).merge(c).value(), 10)
    }

    @TestCase
    public func testGCounterRejectsNegative(): Unit {
        var thrown = false
        try {
            GCounter().increment("a", -1)
        } catch (_: IllegalArgumentException) {
            thrown = true
        }
        @Expect(thrown, true)
    }

    @TestCase
    public func testPNCounterConvergesUnderRandomGossip(): Unit {
        // 3 个副本各自随机增减，然后以随机顺序两两合并，最终收敛到同一值
        let rnd = Random(42)
        let replicas = ArrayList<PNCounter>([PNCounter(), PNCounter(), PNCounter()])
        var expected: Int64 = 0
        for (i in 0..300) {
            let r = Int64(rnd.nextUInt64() % 3)
            let delta = Int64(rnd.nextUInt64() % 11) - 5
            replicas[r] = replicas[r].increment("n${r}", delta)
            expected += delta
            if (i % 7 == 0) {
                let x = Int64(rnd.nextUInt64() % 3)
                let y = Int64(rnd.nextUInt64() % 3)
                replicas[x] = replicas[x].merge(replicas[y])
            }
        }
        for (_ in 0..2) {
            for (x in 0..3) {
                for (y in 0..3) {
                    replicas[x] = replicas[x].merge(replicas[y])
                }
            }
        }
        @Expect(replicas[0].value(), expected)
        @Expect(replicas[0] == replicas[1] && replicas[1] == replicas[2], true)
    }

    @TestCase
    public func testGSet(): Unit {
        let a = GSet().add("x").add("y")
        let b = GSet().add("y").add("z")
        let m = a.merge(b)
        @Expect(m.size(), 3)
        @Expect(m == b.merge(a), true)
        @Expect(m.merge(m) == m, true)
    }

    @TestCase
    public func testORSetRemoveAndConcurrentAddWins(): Unit {
        let base = ORSet().add("a", "x")
        // 副本 B 同步了 base
        var b = ORSet().merge(base)
        // A 删除 x；B 并发地再次 add x
        let a = base.remove("x")
        b = b.add("b", "x")
        let ab = a.merge(b)
        let ba = b.merge(a)
        @Expect(ab.contains("x"), true)          // add 优先
        @Expect(ab == ba, true)
        // 没有并发 add 时删除生效，且不会被旧状态“复活”
        let removed = ab.remove("x")
        @Expect(removed.merge(ab).contains("x"), false)
        @Expect(removed.merge(base).contains("x"), false)
    }

    @TestCase
    public func testORSetLaws(): Unit {
        let a = ORSet().add("a", "1").add("a", "2")
        let b = ORSet().add("b", "2").add("b", "3").remove("3")
        let c = ORSet().add("c", "4")
        @Expect(a.merge(b) == b.merge(a), true)
        @Expect(a.merge(b).merge(c) == a.merge(b.merge(c)), true)
        @Expect(a.merge(a) == a, true)
        @Expect(a.merge(b).merge(c).size(), 3)
    }

    @TestCase
    public func testLWWRegister(): Unit {
        let r1 = LWWRegister<String>("v1", 10, "a")
        let r2 = LWWRegister<String>("v2", 20, "b")
        @Expect(r1.merge(r2).value, "v2")
        @Expect(r2.merge(r1).value, "v2")
        // 时间戳相同按节点名决定，结果确定
        let t1 = LWWRegister<String>("x", 5, "a")
        let t2 = LWWRegister<String>("y", 5, "b")
        @Expect(t1.merge(t2).value, t2.merge(t1).value)
        // 本地时钟回退时写入仍然有序
        let s = r2.set("v3", 1, "a")
        @Expect(s.timestamp > r2.timestamp, true)
        @Expect(r2.merge(s).value, "v3")
    }
}

@Test
public class PhiAccrualTest {
    @TestCase
    public func testRegularHeartbeatsStayAvailable(): Unit {
        let clock = ManualClock()
        let d = detector(clock)
        for (_ in 0..20) {
            d.heartbeat()
            clock.advance(100)
        }
        @Expect(d.isAvailable(), true)
        @Expect(d.phi() < 1.0, true)
    }

    @TestCase
    public func testPhiGrowsWithSilence(): Unit {
        let clock = ManualClock()
        let d = detector(clock)
        for (_ in 0..20) {
            d.heartbeat()
            clock.advance(100)
        }
        let p1 = d.phi()
        clock.advance(200)
        let p2 = d.phi()
        clock.advance(500)
        let p3 = d.phi()
        @Expect(p1 < p2 && p2 < p3, true)
        @Expect(d.isAvailable(), false)
        // 恢复心跳后重新可用
        d.heartbeat()
        @Expect(d.isAvailable(), true)
    }

    @TestCase
    public func testJitterToleratesLongerPauses(): Unit {
        // 心跳间隔抖动大时，同样的停顿产生的 φ 更小
        let c1 = ManualClock()
        let steady = detector(c1)
        let c2 = ManualClock()
        let jittery = detector(c2)
        for (i in 0..40) {
            steady.heartbeat()
            c1.advance(100)
            jittery.heartbeat()
            c2.advance(if (i % 2 == 0) { 40 } else { 160 })
        }
        c1.advance(150)
        c2.advance(150)
        @Expect(jittery.phi() < steady.phi(), true)
    }

    @TestCase
    public func testPhiFormula(): Unit {
        // elapsed == mean 时 P = 0.5，φ = log10(2)
        let p = PhiAccrualFailureDetector.phiOf(100.0, 100.0, 10.0)
        @Expect(p > 0.29 && p < 0.31, true)
    }

    @TestCase
    public func testRegistry(): Unit {
        let clock = ManualClock()
        let reg = FailureDetectorRegistry({ => detector(clock) })
        for (_ in 0..10) {
            reg.heartbeat("n1")
            reg.heartbeat("n2")
            clock.advance(100)
        }
        for (_ in 0..10) {
            reg.heartbeat("n1")
            clock.advance(100)
        }
        @Expect(reg.isAvailable("n1"), true)
        @Expect(reg.isAvailable("n2"), false)
        @Expect(reg.unavailableNodes(), ["n2"])
        @Expect(reg.isAvailable("unknown"), true)
    }
}
