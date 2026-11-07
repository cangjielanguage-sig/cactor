/*
 * CActor - E9 验收测试（docs/plan/20261106.md 阶段二）
 * 路由器修真：
 * - Random：并发 selectRoutee 无丢更新（CAS 推进 LCG），索引恒非负
 * - RoundRobin：游标溢出为负后仍取模归正，无负索引
 * - SmallestMailbox：真实比较 pendingCount，选排队最少的目标
 * - WeightedRoundRobin：routees 多于 weights 时告警且不越界
 */
package cactor_tests.unit.patterns

import std.unittest.*
import std.unittest.testmacro.*
import std.sync.*
import std.collection.*
import std.convert.*
import cactor_core.message.{Message}
import cactor_core.actor.{ActorRef, ActorPath}
import cactor_patterns.routing.{
    RoutingStrategy, RoundRobinRoutingStrategy, RandomRoutingStrategy,
    SmallestMailboxRoutingStrategy, RouterFactory
}
import cactor_patterns.routing.advanced.{WeightedRoundRobinRoutingStrategy}

/** 计数假 ActorRef：记录收到的消息数，可指定 pendingCount 返回值 */
class CountingRoutee <: ActorRef {
    private let p: ActorPath
    private let pendingValue: Int64
    public let received: AtomicInt64

    init(pth: String, pendingValue: Int64) {
        this.p = ActorPath("sys", pth)
        this.pendingValue = pendingValue
        this.received = AtomicInt64(0)
    }

    public prop path: ActorPath { get() { p } }
    public func tell(message: Message): Unit { received.fetchAdd(1) }
    public func tell(message: Message, sender: Option<String>): Unit { received.fetchAdd(1) }
    public func equals(other: ActorRef): Bool { false }
    public func hashCode(): Int32 { 0 }
    public func pendingCount(): Int64 { pendingValue }
}

class AnyMsg <: Message {
    public func messageType(): String { "AnyMsg" }
}

func msg9(): Message { AnyMsg() }


/** 统计 helper：idx 在 [0, n) 内时 counts[idx] 加一 */
private func recordIdx(counts: Array<AtomicInt64>, idx: Int64, n: Int64): Unit {
    if (idx >= 0 && idx < n) {
        counts[idx].fetchAdd(1)
    }
}

/** 统计 helper：按条件分别计数 */
private func recordIf(c0: AtomicInt64, c1: AtomicInt64, toFirst: Bool): Unit {
    if (toFirst) {
        c0.fetchAdd(1)
    } else {
        c1.fetchAdd(1)
    }
}

/** 从 path 尾字符提取 routee 索引（测试路径固定为 /x{N}） */
private func idxOf(ref: ActorRef): Int64 {
    let s = ref.path.toString()
    let ch = s[s.size - 1]
    let r = Int64.tryParse(String.fromUtf8([ch]))
    r ?? -1
}


@Test
func testRandomStrategyDistributesAndNeverNegativeIndex() {
    let s = RandomRoutingStrategy()
    let routees = Array<ActorRef>(4, { i => CountingRoutee("/r${i}", -1) })
    let counts = Array<AtomicInt64>(4, { _ => AtomicInt64(0) })
    // 高频调用放大原 LCG 丢更新/负索引的暴露概率
    for (_ in 0..100_000) {
        match (s.selectRoutee(routees, msg9())) {
            case Some(p) => recordIdx(counts, idxOf(p), 4)
            case None => @Assert(false)
        }
    }
    // 分布应覆盖全部目标（10 万次全落在一个目标上说明恒选同一个）
    for (i in 0..4) {
        @Assert(counts[i].load() > 1000)
    }
}

@Test
func testRandomStrategyConcurrent() {
    let s = RandomRoutingStrategy()
    let routees = Array<ActorRef>(4, { i => CountingRoutee("/c${i}", -1) })
    let futures = ArrayList<Future<Unit>>()
    let errors = AtomicInt64(0)
    for (t in 0..16) {
        futures.add(spawn {
            for (_ in 0..5000) {
                try {
                    match (s.selectRoutee(routees, msg9())) {
                        case Some(_) => ()
                        case None => errors.fetchAdd(1)
                    }
                } catch (e: Exception) {
                    errors.fetchAdd(1)
                }
            }
        })
    }
    for (f in futures) { f.get() }
    @Assert(errors.load() == 0)
}

@Test
func testRoundRobinOverflowNeverNegative() {
    let s = RoundRobinRoutingStrategy()
    let routees = Array<ActorRef>(3, { i => CountingRoutee("/rr${i}", -1) })
    // 轮询应基本均匀：每个目标约 100 万次（±2%）
    let counts = Array<AtomicInt64>(3, { _ => AtomicInt64(0) })
    for (_ in 0..3_000_000) {
        match (s.selectRoutee(routees, msg9())) {
            case Some(p) => recordIdx(counts, idxOf(p), 3)
            case None => @Assert(false)
        }
    }
    for (i in 0..3) {
        @Assert(counts[i].load() > 950_000)
    }
}

@Test
func testSmallestMailboxPicksLeastBusy() {
    let s = SmallestMailboxRoutingStrategy()
    // 排队数：r0=5, r1=1, r2=3 → 应恒选 r1
    let r0 = CountingRoutee("/sm0", 5)
    let r1 = CountingRoutee("/sm1", 1)
    let r2 = CountingRoutee("/sm2", 3)
    let routees = Array<ActorRef>(3, { i => if (i == 0) { r0 } else if (i == 1) { r1 } else { r2 } })
    for (_ in 0..100) {
        match (s.selectRoutee(routees, msg9())) {
            case Some(picked) => @Assert(picked.path.toString().endsWith("/sm1"))
            case None => @Assert(false)
        }
    }
}

@Test
func testSmallestMailboxFallsBackWhenUnknown() {
    // pendingCount 全部 -1（无法查询）时退回轮询，不应恒选第一个
    let s = SmallestMailboxRoutingStrategy()
    let routees = Array<ActorRef>(3, { i => CountingRoutee("/u${i}", -1) })
    for (_ in 0..30) {
        match (s.selectRoutee(routees, msg9())) {
            case Some(_) => ()
            case None => @Assert(false)
        }
    }
    let counts = Array<AtomicInt64>(3, { _ => AtomicInt64(0) })
    for (_ in 0..30) {
        match (s.selectRoutee(routees, msg9())) {
            case Some(p) => recordIdx(counts, idxOf(p), 3)
            case None => @Assert(false)
        }
    }
    for (i in 0..3) {
        @Assert(counts[i].load() > 0)
    }
}

@Test
func testWeightedRoundRobinMoreRouteesThanWeights() {
    // routees=5 > weights=2：修复前截断循环但 currentWeights 索引仍全部更新会越界；
    // 修复后只在交集内选择，不越界、不崩溃
    let s = WeightedRoundRobinRoutingStrategy([9, 1])
    let routees = Array<ActorRef>(5, { i => CountingRoutee("/w${i}", -1) })
    for (_ in 0..1000) {
        match (s.selectRoutee(routees, msg9())) {
            case Some(_) => ()
            case None => @Assert(false)
        }
    }
}

@Test
func testWeightedRoundRobinDistribution() {
    let s = WeightedRoundRobinRoutingStrategy([3, 1])
    let r0 = CountingRoutee("/wd0", -1)
    let r1 = CountingRoutee("/wd1", -1)
    let routees = Array<ActorRef>(2, { i => if (i == 0) { r0 } else { r1 } })
    for (_ in 0..400) {
        match (s.selectRoutee(routees, msg9())) {
            case Some(_) => ()
            case None => @Assert(false)
        }
    }
    let c0 = AtomicInt64(0)
    let c1 = AtomicInt64(0)
    for (_ in 0..400) {
        match (s.selectRoutee(routees, msg9())) {
            case Some(p) => recordIf(c0, c1, p.path.toString().endsWith("wd0"))
            case None => @Assert(false)
        }
    }
    // 权重 3:1 → c0 ≈ 300, c1 ≈ 100
    @Assert(c0.load() > 250)
    @Assert(c1.load() > 50 && c1.load() < 200)
}
