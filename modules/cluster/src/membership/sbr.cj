/*
 * CActor Cluster - 脑裂处理（Split Brain Resolver，对标 Akka 的 SBR 策略）
 *
 * 网络分区后，两侧都会把对方判定为不可达。如果两侧都把对方 down 掉，就会各自继续运行，
 * Singleton、分片实体会各有一份（脑裂）。SBR 让每个节点在可达性视图稳定一段时间后，
 * 按同一个确定的规则判断“自己这一侧是否存活”：
 * - 存活的一侧由 Leader 把不可达成员标记为 Down（随后移除）；
 * - 不存活的一侧每个节点自行停止参与集群（Singleton、分片会随之停止本地实例）。
 * 两侧用同样的规则在各自的视图上计算，因此最多只有一侧存活。
 *
 * 策略：
 * - KeepMajority：可达成员超过半数的一侧存活；正好一半时，包含地址最小成员的一侧存活。
 * - KeepOldest：最老成员（upNumber 最小）所在的一侧存活。
 * - StaticQuorum(n)：可达成员数不少于 n 的一侧存活（n 应大于集群规模的一半）。
 * - AutoDownAfter：旧的超时自动 down，分区时两侧会互相 down，只适合不会分区的环境（例如测试）。
 */
package cactor_cluster.membership

import std.time.*
import std.collection.*

/** 不可达成员的处理方式 */
public enum DowningStrategy {
    /** 不自动处理，只能手动 down */
    | NoDowning
    /** 不可达超过给定时长后由 Leader down（分区时不安全） */
    | AutoDownAfter(Duration)
    /** 多数派存活；参数为可达性视图需要稳定的时长 */
    | KeepMajority(Duration)
    /** 最老成员所在一侧存活 */
    | KeepOldest(Duration)
    /** 可达成员数不少于 quorum 的一侧存活 */
    | StaticQuorum(Int64, Duration)

    public func stableAfter(): Option<Duration> {
        match (this) {
            case KeepMajority(d) => Some(d)
            case KeepOldest(d) => Some(d)
            case StaticQuorum(_, d) => Some(d)
            case _ => None<Duration>
        }
    }
}

/** SBR 的判定结果 */
public enum SbrDecision {
    /** 本侧存活：down 掉这些不可达成员 */
    | DownUnreachable(Array<String>)
    /** 本侧不存活：本节点停止参与集群 */
    | DownSelf
    /** 没有不可达成员，无需处理 */
    | NoAction
}

/**
 * 根据成员表与本节点看到的不可达集合做出判定（纯函数，便于测试）
 *
 * members：未被移除、未 down 的成员；参与计票的是 Up / Leaving / Exiting 成员（Joining 不计票，与 Akka 相同）
 */
public func sbrDecide(strategy: DowningStrategy, members: Array<Member>, unreachable: HashSet<String>): SbrDecision {
    let voters = ArrayList<Member>()
    for (m in members) {
        if (m.status == MemberStatus.Up || m.status == MemberStatus.Leaving || m.status == MemberStatus.Exiting) {
            voters.add(m)
        }
    }
    let downList = ArrayList<String>()
    for (m in members) {
        if (unreachable.contains(m.address)) {
            downList.add(m.address)
        }
    }
    if (downList.isEmpty()) {
        return SbrDecision.NoAction
    }
    var reachableVoters: Int64 = 0
    for (m in voters) {
        if (!unreachable.contains(m.address)) {
            reachableVoters++
        }
    }
    let survive = match (strategy) {
        case KeepMajority(_) =>
            let total = voters.size
            if (reachableVoters * 2 > total) {
                true
            } else if (reachableVoters * 2 < total) {
                false
            } else {
                // 正好一半：包含地址最小的计票成员的一侧存活
                var lowest: Option<String> = None
                for (m in voters) {
                    match (lowest) {
                        case Some(l) where l <= m.address => ()
                        case _ => lowest = Some(m.address)
                    }
                }
                match (lowest) {
                    case Some(l) => !unreachable.contains(l)
                    case None => false
                }
            }
        case KeepOldest(_) =>
            var oldest: Option<Member> = None
            for (m in voters) {
                match (oldest) {
                    case Some(o) where o.isOlderThan(m) => ()
                    case _ => oldest = Some(m)
                }
            }
            match (oldest) {
                case Some(o) => !unreachable.contains(o.address)
                case None => false
            }
        case StaticQuorum(q, _) => reachableVoters >= q
        case _ => true
    }
    if (survive) {
        SbrDecision.DownUnreachable(downList.toArray())
    } else {
        SbrDecision.DownSelf
    }
}
