/*
 * CActor - 差距 4.4：分片分配策略 SPI（ShardAllocationStrategy）
 *
 * 覆盖：
 *   - 默认 rendezvous 行为不变（现有 sharding 套件另行覆盖）；
 *   - 注入自定义策略后分片宿主随策略改变（本用例用"总是选地址最大的成员"）；
 *   - 空成员集返回 None；
 *   - ShardingSettings.withAllocationStrategy 保留其它字段。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_cluster.membership.{ClusterSharding, FirstMemberAllocation, RendezvousAllocation,
    ShardAllocationStrategy, ShardingSettings}
import cactor_testkit.*

class AllocationEntity <: Actor {
    let id: String

    init(id: String) {
        this.id = id
    }

    public prop name: String { get() { "allocation-entity" } }
    public prop description: String { get() { "allocation entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

func highestAddressIndex(nodes: Array<CNode>): Int64 {
    var best = 0
    for (i in 1..nodes.size) {
        if (nodes[i].address() > nodes[best].address()) {
            best = i
        }
    }
    best
}

/** 测试策略：总是选地址最大的成员（与默认 rendezvous 的可观测结果不同） */
class LastMemberAllocation <: ShardAllocationStrategy {
    public init() {}

    public func allocate(shard: Int64, members: Array<String>): Option<String> {
        if (members.isEmpty()) {
            return None<String>
        }
        var best = members[0]
        for (i in 1..members.size) {
            if (members[i] > best) {
                best = members[i]
            }
        }
        Some(best)
    }
}

@Test
public class ShardAllocationStrategyTest {
    @TestCase
    public func allocationStrategyIsPluggable(): Unit {
        let nodes = Array<CNode>(3, { i => CNode("alloc-${i}") })
        let seed = nodes[0].addr()
        for (n in nodes) {
            n.join([seed], FAST)
        }
        let settings = ShardingSettings.defaults()
            .withAllocationStrategy(LastMemberAllocation())
        @Expect((settings.allocation as LastMemberAllocation).isSome(), true)

        let regions = Array<ClusterSharding>(3, { i =>
            ClusterSharding(nodes[i].cluster, nodes[i].kit.system, nodes[i].remote, "alloc-entity", 16,
                { id => AllocationEntity(id) }, settings)
        })
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[0].cluster.state().upCount() == 3 }), true)
            // 三个节点的视图都到 3 再发，避免个别节点按不完整视图先行托管
            for (n in nodes) {
                @Expect(awaitCondition(CLUSTER_WAIT, { => n.cluster.state().upCount() == 3 }), true)
            }
            // 找到地址最大的节点（先算成 let，闭包不能捕获 var）
            let highest = highestAddressIndex(nodes)
            for (i in 0..12) {
                regions[0].tell("e-${i}", StringMessage("x"))
            }
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                regions[highest].localEntityCount() == 12
            }), true)
            for (i in 0..3) {
                if (i != highest) {
                    @Expect(regions[i].localEntityCount(), 0)
                }
            }
        } finally {
            for (r in regions) {
                r.stop()
            }
            closeAll(nodes)
        }
    }

    @TestCase
    public func builtInStrategiesHandleEmptyMembers(): Unit {
        @Expect(RendezvousAllocation().allocate(1, Array<String>()).isNone(), true)
        @Expect(FirstMemberAllocation().allocate(1, Array<String>()).isNone(), true)
        @Expect(FirstMemberAllocation().allocate(1, ["b", "a"]), Some("a"))
        @Expect(LastMemberAllocation().allocate(1, ["b", "a"]), Some("b"))
    }
}
