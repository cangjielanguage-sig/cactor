/*
 * CActor - 分片再平衡策略（RebalancePolicy / decideRebalance）的纯函数测试
 *
 * 决策不读时钟、不碰 Actor、不加锁，所以这里不需要回环 TCP、不需要 waitCondition：
 * 每个用例直接构造一份视图快照，断言"搬哪个分片、从哪到哪"或"因为什么理由不搬"。
 * 这组用例覆盖旧实现根本测不到的分支：源节点不是领导者、报告缺失、不可达、冷却、
 * 同值 tie-break（HashMap 迭代顺序不稳定，决定必须与迭代顺序无关）。
 */
package cactor_tests.unit.cluster

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.dsl.*
import cactor_cluster.membership.*

/** 一个节点的报告：[(分片号, 实体数), ...] */
func load(pairs: Array<(Int64, Int64)>): HashMap<Int64, Int64> {
    let m = HashMap<Int64, Int64>()
    for ((sh, c) in pairs) {
        m[sh] = c
    }
    m
}

/** 视图：节点地址 -> 该节点报告；members 就是这些地址（保持给定顺序） */
func viewOf(nodes: Array<(String, Array<(Int64, Int64)>)>, unreachable!: Int64 = 0,
    lastMoveAtMs!: Int64 = Int64.Min / 2, nowMs!: Int64 = 0): RebalanceView {
    let members = ArrayList<String>()
    let loads = HashMap<String, Map<Int64, Int64>>()
    for ((addr, pairs) in nodes) {
        members.add(addr)
        loads[addr] = load(pairs)
    }
    RebalanceView(members.toArray(), loads, unreachable: unreachable, lastMoveAtMs: lastMoveAtMs, nowMs: nowMs)
}

func moveOf(d: RebalanceDecision): ShardMove {
    match (d) {
        case RebalanceDecision.Move(m) => m
        case RebalanceDecision.Skip(r) => throw IllegalStateException("期望迁移，实际跳过：${rebalanceSkipText(r)}")
    }
}

/** 枚举没有实现 Equatable（仓颉 enum 不支持 @Expect 直接比较），用可读文本比较，失败信息也更好读 */
func expectSkip(d: RebalanceDecision, want: RebalanceSkipReason): Unit {
    match (d) {
        case RebalanceDecision.Skip(r) => @Expect(rebalanceSkipText(r), rebalanceSkipText(want))
        case RebalanceDecision.Move(m) => @Fail("期望跳过（${rebalanceSkipText(want)}），实际迁移 ${m}")
    }
}

@Test
public class RebalancePolicyTest {
    /** 关闭后无论多偏都不搬（压测/演示里需要固定放置） */
    @TestCase
    public func disabledPolicyNeverMoves(): Unit {
        let v = viewOf([("a", [(0, 100)]), ("b", [(0, 0)]), ("c", [(0, 0)])])
        expectSkip(decideRebalance(v, RebalancePolicy.disabled()), RebalanceSkipReason.Disabled)
        // withEnabled 返回新实例，原实例保持关闭
        let p = RebalancePolicy.disabled()
        @Expect(p.withEnabled(true).enabled, true)
        @Expect(p.enabled, false)
    }

    /** 有不可达成员时不动：分区期间两侧各自搬，恢复后又要搬回来 */
    @TestCase
    public func unreachableMembersBlockRebalancing(): Unit {
        let v = viewOf([("a", [(0, 100)]), ("b", [(0, 0)])], unreachable: 1)
        expectSkip(decideRebalance(v, RebalancePolicy.defaults()), RebalanceSkipReason.UnreachableMembers)
    }

    /** 单机/单成员：不需要搬，也不该把"最空闲"算成自己 */
    @TestCase
    public func singleMemberIsNotEnough(): Unit {
        let v = viewOf([("a", [(0, 100), (1, 50)])])
        expectSkip(decideRebalance(v, RebalancePolicy.defaults()), RebalanceSkipReason.NotEnoughMembers)
    }

    /** 成员没有（未过期的）报告：此时"最空闲"可能只是没上报的节点，放弃而不是瞎搬 */
    @TestCase
    public func missingReportBlocksDecision(): Unit {
        let members = ["a", "b", "c"]
        let loads = HashMap<String, Map<Int64, Int64>>()
        loads["a"] = load([(0, 100)])
        loads["b"] = load([(0, 0)])
        let v = RebalanceView(members, loads)
        expectSkip(decideRebalance(v, RebalancePolicy.defaults()), RebalanceSkipReason.MissingLoadReport)
    }

    /** 14 / 10 = 1.4 < 1.5：比例没到阈值，不搬 */
    @TestCase
    public func skewBelowRatioIsBalanced(): Unit {
        let v = viewOf([("a", [(0, 14)]), ("b", [(0, 10)])])
        expectSkip(decideRebalance(v, RebalancePolicy.defaults()), RebalanceSkipReason.Balanced)
    }

    /** 6 / 0 比例够大，但绝对差没到 minEntityDelta：小规模抖动不值得搬 */
    @TestCase
    public func entityDeltaBelowMinimumIsBalanced(): Unit {
        let v = viewOf([("a", [(0, 6)]), ("b", [(0, 0)])])
        expectSkip(decideRebalance(v, RebalancePolicy.defaults()), RebalanceSkipReason.Balanced)
    }

    /** 冷却窗口内不搬第二次：上一轮的报告还没刷新，视图里的偏斜是旧的 */
    @TestCase
    public func cooldownBlocksSecondMove(): Unit {
        let v = viewOf([("a", [(0, 100)]), ("b", [(0, 0)])], lastMoveAtMs: 1000, nowMs: 2000)
        expectSkip(decideRebalance(v, RebalancePolicy.defaults()), RebalanceSkipReason.Cooldown)
        // 冷却过了就搬
        let later = viewOf([("a", [(0, 100)]), ("b", [(0, 0)])], lastMoveAtMs: 1000, nowMs: 5000)
        @Expect(moveOf(decideRebalance(later, RebalancePolicy.defaults())).shardId, 0)
    }

    /** 源可以不是决策者：最忙节点 a 上最重的分片 1 迁到最空闲的 b */
    @TestCase
    public func movesHeaviestShardFromBusiestToIdlest(): Unit {
        let v = viewOf([("a", [(0, 2), (1, 9), (2, 4)]), ("b", [(0, 0)]), ("c", [(0, 1)])],
            lastMoveAtMs: 0, nowMs: 10_000)
        let m = moveOf(decideRebalance(v, RebalancePolicy.defaults()))
        @Expect(m.shardId, 1)
        @Expect(m.source, "a")
        @Expect(m.target, "b")
        @Expect(m.entityCount, 9)
        @Expect(m.sourceTotal, 15)
        @Expect(m.targetTotal, 0)
    }

    /** 两个节点同样忙时取地址较小者，且与成员数组顺序无关（各节点算出的决定必须一致） */
    @TestCase
    public func tieBreakIsDeterministic(): Unit {
        let forward = viewOf([("a", [(0, 20)]), ("b", [(0, 20)]), ("c", [(0, 0)])], lastMoveAtMs: 0, nowMs: 10_000)
        let backward = viewOf([("c", [(0, 0)]), ("b", [(0, 20)]), ("a", [(0, 20)])], lastMoveAtMs: 0, nowMs: 10_000)
        let m1 = moveOf(decideRebalance(forward, RebalancePolicy.defaults()))
        let m2 = moveOf(decideRebalance(backward, RebalancePolicy.defaults()))
        @Expect(m1.source, "a")
        @Expect(m1.target, "c")
        @Expect("${m1}", "${m2}")
    }

    /** 自定义策略生效：差 3 就搬、比例 200% */
    @TestCase
    public func customPolicyChangesThresholds(): Unit {
        let v = viewOf([("a", [(0, 5)]), ("b", [(0, 2)]), ("c", [(0, 2)])], lastMoveAtMs: 0, nowMs: 10_000)
        expectSkip(decideRebalance(v, RebalancePolicy.defaults()), RebalanceSkipReason.Balanced)
        let aggressive = RebalancePolicy.defaults().withMinEntityDelta(3).withRatioPercent(200)
        let m = moveOf(decideRebalance(v, aggressive))
        @Expect(m.source, "a")
        @Expect(m.target, "b")
        @Expect(m.entityCount, 5)
    }

    /** 参数校验：非法策略立即失败，而不是在运行期悄悄不搬 */
    @TestCase
    public func policyRejectsInvalidValues(): Unit {
        @ExpectThrows[IllegalArgumentException](RebalancePolicy(minEntityDelta: -1))
        @ExpectThrows[IllegalArgumentException](RebalancePolicy(ratioPercent: 90))
        @ExpectThrows[IllegalArgumentException](RebalancePolicy(cooldown: Duration.Zero))
        @ExpectThrows[IllegalArgumentException](RebalancePolicy(reportTtl: Duration.Zero))
    }

    /** 设置链式组合：换策略不影响 bufferSize，开钝化会清掉 remember-entities（互斥） */
    @TestCase
    public func settingsComposeWithRebalance(): Unit {
        let s = ShardingSettings.defaults()
            .withBufferSize(7)
            .withRebalance(RebalancePolicy.disabled().withCooldown(1.seconds))
        @Expect(s.bufferSize, 7)
        @Expect(s.rebalance.enabled, false)
        @Expect(s.rebalance.cooldown.toMilliseconds(), 1000)
        let p = s.withPassivation(30.seconds)
        @Expect(p.passivateAfter.isSome(), true)
        @Expect(p.rebalance.enabled, false)
        @Expect(p.withBufferSize(9).bufferSize, 9)
    }
}
