/*
 * CActor Cluster.Membership - 分片再平衡策略（v2.19）
 *
 * 为什么把决策从 ShardRegionActor 里抽出来：
 *   - 原来的阈值、冷却、TTL 是三个内联常量，节点之间无法差异化配置，也没法在不启集群的情况下测试；
 *   - 再平衡涉及"不可达、报告缺失、比例、绝对差、冷却、没东西可搬"六种放弃理由，写成 if 链时
 *     只能看到"没搬"，看不出为什么没搬；这里用枚举把理由变成返回值，调用方和测试都能看见，
 *     以后新增理由时穷尽 match 会在编译期报错（不会静默漏判）。
 * 决策本身是纯函数：输入一次视图快照 + 策略，输出决定；不读时钟、不碰 Actor、不加锁，
 * 因此可以用普通单测穷举边界，不需要回环 TCP 与真实成员协议。
 *
 *   let v = RebalanceView(ups, loads, lastMoveAtMs: last, nowMs: now)
 *   match (decideRebalance(v, policy)) {
 *       case RebalanceDecision.Move(m) => 搬 m.shardId 从 m.source 到 m.target
 *       case RebalanceDecision.Skip(r) => 记录理由
 *   }
 */
package cactor_cluster.membership

import std.collection.*
import std.time.*
import cactor_core.dsl.*

/**
 * 再平衡策略（不可变，`withXxx` 返回新实例）。
 *
 * 默认值与 Akka 的经验值对齐：最忙节点实体数达到最空闲的 1.5 倍、且绝对差超过 5 才动，
 * 两次自动迁移至少间隔 3 秒，节点负载报告 5 秒内有效。理由：
 *   - 只按比例判断时，"20 / 13" 这种绝对值很小的偏斜也会触发，搬一个分片就让两个节点都抖动；
 *   - 只按绝对差判断时，大集群里"1000 / 990" 早该平衡了却还在搬；
 *   - 冷却防止一次 tick 里连环搬迁（上一轮刚搬完，负载报告还没刷新，视图里仍是旧的偏斜）。
 */
public struct RebalancePolicy <: ToString {
    /** 关掉后完全不自动迁移（手动 `rebalanceShard` 不受影响） */
    public let enabled: Bool
    /** 最忙与最空闲的实体数差至少这么大才考虑迁移 */
    public let minEntityDelta: Int64
    /** 比例阈值，百分比：最忙 * 100 >= 最空闲 * ratioPercent 才考虑迁移（默认 150） */
    public let ratioPercent: Int64
    /** 两次自动迁移的最短间隔 */
    public let cooldown: Duration
    /** 其他节点的负载报告多久算过期；过期后不参与决策（否则"最空闲"可能只是没上报的节点） */
    public let reportTtl: Duration

    public init(enabled!: Bool = true, minEntityDelta!: Int64 = 5, ratioPercent!: Int64 = 150,
        cooldown!: Duration = 3.seconds, reportTtl!: Duration = 5.seconds) {
        if (minEntityDelta < 0) {
            throw IllegalArgumentException("minEntityDelta 不能为负，实际 ${minEntityDelta}")
        }
        if (ratioPercent < 100) {
            throw IllegalArgumentException("ratioPercent 必须 >= 100（否则任何负载都会触发迁移），实际 ${ratioPercent}")
        }
        if (cooldown <= Duration.Zero) {
            throw IllegalArgumentException("cooldown 必须大于 0，实际 ${cooldown}")
        }
        if (reportTtl <= Duration.Zero) {
            throw IllegalArgumentException("reportTtl 必须大于 0，实际 ${reportTtl}")
        }
        this.enabled = enabled
        this.minEntityDelta = minEntityDelta
        this.ratioPercent = ratioPercent
        this.cooldown = cooldown
        this.reportTtl = reportTtl
    }

    public static func defaults(): RebalancePolicy {
        RebalancePolicy()
    }

    /** 显式关闭：单机多节点演示、性能压测里不希望分片移动时使用 */
    public static func disabled(): RebalancePolicy {
        RebalancePolicy(enabled: false)
    }

    public func withEnabled(on: Bool): RebalancePolicy {
        RebalancePolicy(enabled: on, minEntityDelta: minEntityDelta, ratioPercent: ratioPercent, cooldown: cooldown,
            reportTtl: reportTtl)
    }

    public func withMinEntityDelta(delta: Int64): RebalancePolicy {
        RebalancePolicy(enabled: enabled, minEntityDelta: delta, ratioPercent: ratioPercent, cooldown: cooldown,
            reportTtl: reportTtl)
    }

    public func withRatioPercent(percent: Int64): RebalancePolicy {
        RebalancePolicy(enabled: enabled, minEntityDelta: minEntityDelta, ratioPercent: percent, cooldown: cooldown,
            reportTtl: reportTtl)
    }

    public func withCooldown(d: Duration): RebalancePolicy {
        RebalancePolicy(enabled: enabled, minEntityDelta: minEntityDelta, ratioPercent: ratioPercent, cooldown: d,
            reportTtl: reportTtl)
    }

    public func withReportTtl(d: Duration): RebalancePolicy {
        RebalancePolicy(enabled: enabled, minEntityDelta: minEntityDelta, ratioPercent: ratioPercent, cooldown: cooldown,
            reportTtl: d)
    }

    public func toString(): String {
        "RebalancePolicy(enabled=${enabled}, minEntityDelta=${minEntityDelta}, ratioPercent=${ratioPercent}%, " +
        "cooldown=${cooldown}, reportTtl=${reportTtl})"
    }
}

/** 没有迁移的理由：返回值而不是日志，便于调用方区分"不该搬"与"没数据"（也便于断言） */
public enum RebalanceSkipReason {
    /** 策略关闭 */
    | Disabled
    /** 视图里有不可达成员：分区期间两侧各自搬，恢复后又要搬回来 */
    | UnreachableMembers
    /** Up 成员少于两个 */
    | NotEnoughMembers
    /** 有成员没有（未过期的）负载报告：此时"最空闲"可能是没上报的节点 */
    | MissingLoadReport
    /** 负载差没有超过比例阈值或绝对差阈值 */
    | Balanced
    /** 距上次自动迁移还没到冷却时间 */
    | Cooldown
}

/** 放弃理由的可读说明（日志与排障用；枚举本身不实现 ToString，避免日志里只出现英文变体名） */
public func rebalanceSkipText(r: RebalanceSkipReason): String {
    match (r) {
        case RebalanceSkipReason.Disabled => "策略已关闭"
        case RebalanceSkipReason.UnreachableMembers => "视图里有不可达成员"
        case RebalanceSkipReason.NotEnoughMembers => "Up 成员不足两个"
        case RebalanceSkipReason.MissingLoadReport => "有成员缺少未过期的负载报告"
        case RebalanceSkipReason.Balanced => "负载差未超过阈值"
        case RebalanceSkipReason.Cooldown => "距上次迁移未到冷却时间"
    }
}

/** 一次具体的迁移：把 source 上实体数最多的分片交给 target */
public struct ShardMove <: ToString {
    public let shardId: Int64
    public let source: String
    public let target: String
    /** 该分片在 source 上的实体数（> 0） */
    public let entityCount: Int64
    /** 迁移前两个节点的实体总数，便于日志与断言 */
    public let sourceTotal: Int64
    public let targetTotal: Int64

    public init(shardId: Int64, source: String, target: String, entityCount: Int64, sourceTotal: Int64,
        targetTotal: Int64) {
        this.shardId = shardId
        this.source = source
        this.target = target
        this.entityCount = entityCount
        this.sourceTotal = sourceTotal
        this.targetTotal = targetTotal
    }

    public func toString(): String {
        "ShardMove(shard=${shardId}, ${source}(${sourceTotal}) -> ${target}(${targetTotal}), entities=${entityCount})"
    }
}

/** 决策结果：要么搬一个分片，要么带理由不搬 */
public enum RebalanceDecision {
    | Move(ShardMove)
    | Skip(RebalanceSkipReason)
}

/**
 * 一次决策所需的视图快照（全部是值，创建之后不变）。
 *
 * members 必须包含**全部 Up 成员**（包括本节点）；loads 的键是成员地址、值是"分片 -> 实体数"。
 * 本节点自己的负载也要放进来：让"最忙"可能是自己，这样源节点不必是领导者（旧实现的缺陷）。
 */
public class RebalanceView {
    public let members: Array<String>
    public let loads: Map<String, Map<Int64, Int64>>
    /** 视图里不可达成员的数量（> 0 时放弃） */
    public let unreachable: Int64
    /** 上次自动迁移的时刻（单调毫秒），从未迁移过传 Int64.Min / 2 */
    public let lastMoveAtMs: Int64
    public let nowMs: Int64

    public init(members: Array<String>, loads: Map<String, Map<Int64, Int64>>, unreachable!: Int64 = 0,
        lastMoveAtMs!: Int64 = Int64.Min / 2, nowMs!: Int64 = 0) {
        this.members = members
        this.loads = loads
        this.unreachable = unreachable
        this.lastMoveAtMs = lastMoveAtMs
        this.nowMs = nowMs
    }
}

/**
 * 计算一次再平衡决定（纯函数）。
 *
 * 判定顺序就是"代价从低到高"：先排除不该动的全局条件（关闭、不可达、成员不足），
 * 再看数据是否齐全（报告缺失），最后才做数值比较与冷却；这样 Skip 理由总是最有信息量的那个。
 * 选最忙/最空闲时用地址字典序做 tie-break，选分片时用最小分片号，保证同样的输入在任何节点、
 * 任何迭代顺序下都得到同一个决定（HashMap 迭代顺序不保证稳定）。
 */
public func decideRebalance(view: RebalanceView, policy: RebalancePolicy): RebalanceDecision {
    if (!policy.enabled) {
        return RebalanceDecision.Skip(RebalanceSkipReason.Disabled)
    }
    if (view.unreachable > 0) {
        return RebalanceDecision.Skip(RebalanceSkipReason.UnreachableMembers)
    }
    if (view.members.size < 2) {
        return RebalanceDecision.Skip(RebalanceSkipReason.NotEnoughMembers)
    }
    // 每个成员都必须有报告：缺一个就可能把"没上报"误判成"最空闲"
    for (m in view.members) {
        if (!view.loads.contains(m)) {
            return RebalanceDecision.Skip(RebalanceSkipReason.MissingLoadReport)
        }
    }
    var busiest = ""
    var busiestTotal: Int64 = -1
    var idlest = ""
    var idlestTotal: Int64 = 0
    var idlestKnown = false
    for (m in view.members) {
        let total = totalLoad(view.loads.get(m) ?? HashMap<Int64, Int64>())
        // 同值时取地址较小者：各节点算出来一致，避免两个节点各自决定搬给对方
        if (busiestTotal < 0 || total > busiestTotal || (total == busiestTotal && m < busiest)) {
            busiestTotal = total
            busiest = m
        }
        if (!idlestKnown || total < idlestTotal || (total == idlestTotal && m < idlest)) {
            idlestKnown = true
            idlestTotal = total
            idlest = m
        }
    }
    if (busiest == idlest) {
        return RebalanceDecision.Skip(RebalanceSkipReason.Balanced)
    }
    // 比例用浮点比较：Int64 上 total * 100 在极端值下会溢出（仓颉默认检查溢出，直接 panic）
    let idleBase = if (idlestTotal > 0) { idlestTotal } else { 1 }
    if (Float64(busiestTotal) / Float64(idleBase) < Float64(policy.ratioPercent) / 100.0) {
        return RebalanceDecision.Skip(RebalanceSkipReason.Balanced)
    }
    if (busiestTotal - idlestTotal < policy.minEntityDelta) {
        return RebalanceDecision.Skip(RebalanceSkipReason.Balanced)
    }
    if (view.nowMs - view.lastMoveAtMs < policy.cooldown.toMilliseconds()) {
        return RebalanceDecision.Skip(RebalanceSkipReason.Cooldown)
    }
    // 从最忙节点挑最重的分片；同值时取最小分片号，保证决定稳定
    var shardId: Int64 = -1
    var shardLoad: Int64 = 0
    match (view.loads.get(busiest)) {
        case Some(l) =>
            for ((sh, c) in l) {
                if (sh < 0 || c <= 0) {
                    continue
                }
                if (c > shardLoad || (c == shardLoad && (shardId < 0 || sh < shardId))) {
                    shardLoad = c
                    shardId = sh
                }
            }
        case None => return RebalanceDecision.Skip(RebalanceSkipReason.MissingLoadReport)
    }
    // 能走到这里说明 busiestTotal > idlestTotal >= 0，因此最忙节点上至少有一个正数负载的分片
    RebalanceDecision.Move(ShardMove(shardId, busiest, idlest, shardLoad, busiestTotal, idlestTotal))
}

/** 一个节点的实体总数（负数与非法分片号按 0 计：报告来自网络，不能信任） */
private func totalLoad(l: Map<Int64, Int64>): Int64 {
    var t: Int64 = 0
    for ((_, c) in l) {
        if (c > 0) {
            t += c
        }
    }
    t
}
