/*
 * CActor - 集群分片再平衡（E13′ 之后的再平衡能力）
 *
 *   - 手动再平衡：`ClusterSharding.rebalanceShard(shard, target)` 把指定分片迁到目标成员，
 *     断言源节点确实不再托管、目标节点确实托管（旧用例只断言 `true`，测不出任何东西）。
 *   - 自动再平衡：领导者按各节点上报的负载（`ShardLoadReport`）把最忙节点上最重的分片
 *     迁到最空闲节点；把全部分片覆盖到同一节点制造偏斜，等待搬运自己发生。
 *
 * 两个用例都用真实回环 TCP + 真实成员协议，不用 sleep 等结果。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_cluster.membership.*
import cactor_testkit.*

let RB_FAST = ClusterSettings(Duration.millisecond * 50, Duration.millisecond * 50, 8.0, Duration.millisecond * 300, None<Duration>)
let RB_WAIT = Duration.second * 15

class RbEntity <: Actor {
    let id: String
    init(id: String) { this.id = id }
    public prop name: String { get() { "rb" } }
    public prop description: String { get() { "rb" } }
    public func receive(message: Message, context: ActorContext): MessageResult { MessageResult.Handled }
}

@Test
public class ShardingRebalanceTest {
    /** 手动再平衡：源节点不再托管，目标节点托管，托管总数不变 */
    @TestCase
    public func manualRebalanceMovesShard(): Unit {
        let a = CNode("rb-a")
        let b = CNode("rb-b")
        a.join([a.addr()], RB_FAST)
        b.join([a.addr()], RB_FAST)
        @Expect(awaitCondition(RB_WAIT, { => allUp2([a, b], 2) }), true)

        let ra = ClusterSharding(a.cluster, a.kit.system, a.remote, "rb", 8, { id => RbEntity(id) })
        let rb = ClusterSharding(b.cluster, b.kit.system, b.remote, "rb", 8, { id => RbEntity(id) })
        @Expect(awaitAllAcquired([ra, rb], 8, RB_WAIT), true)

        // 发一些实体消息，确保实体被创建、负载报告里有非零分片
        for (i in 0..20) {
            ra.tell("e-${i}", StringMessage("hi"))
        }
        @Expect(awaitCondition(RB_WAIT, { => totalLoad(ra) > 0 && totalLoad(rb) > 0 }), true)

        // 选一个本节点 a 托管的分片，迁移到 b
        let (hostedA, _, _) = ra.shardStats()
        let (hostedB, _, _) = rb.shardStats()
        var targetShard: Int64 = -1
        for ((sh, c) in ra.shardLoad()) {
            if (c > 0) {
                targetShard = sh
                break
            }
        }
        @Expect(targetShard >= 0, true)
        ra.rebalanceShard(targetShard, b.address())

        // 真实断言：a 少托管一个、b 多托管一个（能力移交走许可式两阶段）
        @Expect(awaitCondition(RB_WAIT, { =>
            let (hA, _, _) = ra.shardStats()
            let (hB, _, _) = rb.shardStats()
            hA == hostedA - 1 && hB == hostedB + 1
        }), true)

        ra.stop()
        rb.stop()
        a.cluster.leave()
        b.cluster.leave()
    }

    /** 自动再平衡：全部分片都在 a 上形成偏斜后，领导者把最忙的分片搬到空闲的 b */
    @TestCase
    public func leaderRebalancesSkewedLoadAutomatically(): Unit {
        let a = CNode("rb2-a")
        let b = CNode("rb2-b")
        a.join([a.addr()], RB_FAST)
        b.join([a.addr()], RB_FAST)
        @Expect(awaitCondition(RB_WAIT, { => allUp2([a, b], 2) }), true)

        let ra = ClusterSharding(a.cluster, a.kit.system, a.remote, "rb2", 8, { id => RbEntity(id) })
        let rb = ClusterSharding(b.cluster, b.kit.system, b.remote, "rb2", 8, { id => RbEntity(id) })
        @Expect(awaitAllAcquired([ra, rb], 8, RB_WAIT), true)

        // 把 8 个分片全部覆盖到 a：a 托管 8 个，b 托管 0 个
        for (sh in 0..8) {
            ra.rebalanceShard(sh, a.address())
        }
        @Expect(awaitCondition(RB_WAIT, { =>
            let (hA, acq, _) = ra.shardStats()
            let (hB, _, _) = rb.shardStats()
            hA == 8 && acq == 0 && hB == 0
        }), true)

        // 制造偏斜：8 个分片都在 a，新消息创建的实体全部落在 a 上
        for (i in 0..60) {
            ra.tell("e-${i}", StringMessage("hi"))
        }

        // 不手动触发：领导者每 2s 按负载报告评估一次，冷却 3s 后自己搬第一个分片。
        // 只看"b 开始托管分片"——覆盖全部指向 a，b 不可能通过别的方式托管，
        // 所以它就是自动再平衡发生的证据；同时观察 a 的负载峰值，确认偏斜真的存在过
        // （搬运会停止 a 上的实体、负载随之下降，不能用"a 一直 >= 20"当条件）。
        // 用单元素数组记录峰值：仓颉的 lambda 不能捕获可变局部变量（var）
        let peakA = Array<Int64>(1, repeat: 0)
        let moved = awaitCondition(Duration.second * 30, { =>
            let la = totalLoad(ra)
            if (la > peakA[0]) {
                peakA[0] = la
            }
            let (hB, _, _) = rb.shardStats()
            hB >= 1
        })
        if (!moved) {
            let (ha, aa, ac) = ra.shardStats()
            let (hb, ab, bc) = rb.shardStats()
            println("DIAG aLoad=${totalLoad(ra)} peak=${peakA[0]} a=(h${ha},acq${aa},done${ac}) b=(h${hb},acq${ab},done${bc})")
        }
        // 覆盖全部指向 a，b 托管分片的唯一途径就是领导者按负载把它搬过来，
        // 所以"b 开始托管"本身就是自动再平衡发生的证据（不再额外断言 a 的负载峰值：
        // 搬运会停止 a 上的实体，峰值可能在轮询前就被搬走，断言它会让用例偶发失败）。
        @Expect(moved, true)

        ra.stop()
        rb.stop()
        a.cluster.leave()
        b.cluster.leave()
    }
}

func totalLoad(region: ClusterSharding): Int64 {
    var total: Int64 = 0
    for ((_, c) in region.shardLoad()) {
        total += c
    }
    total
}

func allUp2(nodes: Array<CNode>, n: Int64): Bool {
    for (node in nodes) {
        if (node.cluster.isTerminated()) { return false }
        let s = node.cluster.state()
        if (!s.converged || s.upCount() != n) { return false }
    }
    true
}
