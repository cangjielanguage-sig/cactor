/*
 * CActor - 差距 4.3：分片宿主角色（对标 Akka ClusterShardingSettings.withRole）
 *
 * 覆盖：
 *   - 只有带目标角色的 Up 成员托管分片；不带角色的节点即使运行区域也不托管；
 *   - 不带角色的节点仍可转发消息（消息由角色节点上的实体处理）；
 *   - withRole 保留其它设置字段，默认无角色限制。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_cluster.membership.*
import cactor_core.message.{StringMessage}
import cactor_testkit.*

func roleShardings(nodes: Array<CNode>, ledger: Ledgers, role: String): Array<ClusterSharding> {
    Array<ClusterSharding>(nodes.size, { i =>
        let n = nodes[i]
        let addr = n.address()
        ClusterSharding(n.cluster, n.kit.system, n.remote, "role-counter", 64,
            { id => CountingEntity(id, addr, ledger) }, staticShardingSettings().withRole(role))
    })
}

/** 按给定角色建 3 节点集群：workerCount 个 worker，其余 gateway */
func startRoleCluster(name: String, workerCount: Int64): Array<CNode> {
    let nodes = Array<CNode>(3, { i => CNode("${name}-${i}") })
    let seed = nodes[0].addr()
    for (i in 0..nodes.size) {
        if (i < workerCount) {
            nodes[i].join([seed], FAST, roles: ["worker"])
        } else {
            nodes[i].join([seed], FAST, roles: ["gateway"])
        }
    }
    nodes
}

@Test
public class ShardingRolesTest {
    /** 只有一个 worker：64 个分片全部落在它上面，两个 gateway 区域托管 0 个但能转发 */
    @TestCase
    public func shardsOnlyOnRoleMembers(): Unit {
        let nodes = startRoleCluster("shard-role", 1)
        let ledger = Ledgers()
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
            let regions = roleShardings(nodes, ledger, "worker")
            try {
                @Expect(awaitAllAcquired(regions, 64, CLUSTER_WAIT), true)
                let (hostedWorker, _, _) = regions[0].shardStats()
                @Expect(hostedWorker, 64)
                for (i in 1..3) {
                    let (hosted, acquiring, _) = regions[i].shardStats()
                    @Expect(hosted, 0)
                    @Expect(acquiring, 0)
                    @Expect(regions[i].localEntityCount(), 0)
                }

                // 每个节点各发一批消息；gateway 只转发，实体只在 worker 上创建
                for (i in 0..3) {
                    for (k in 0..8) {
                        regions[i].tell("e${k}", StringMessage("from-${i}"))
                    }
                }
                @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 24 }), true)
                @Expect(regions[0].localEntityCount(), 8)
                for (i in 1..3) {
                    @Expect(regions[i].localEntityCount(), 0)
                }
                for (k in 0..8) {
                    let hs = ledger.hostsOf("e${k}")
                    @Expect(hs.size >= 1, true)
                    for (h in hs) {
                        @Expect(h, nodes[0].address())
                    }
                }
            } finally {
                for (r in regions) {
                    r.stop()
                }
            }
        } finally {
            closeAll(nodes)
        }
    }

    /** withRole 保留其它字段；默认无角色限制 */
    @TestCase
    public func withRolePreservesSettings(): Unit {
        @Expect(ShardingSettings.defaults().role.isNone(), true)
        let idle = Duration.second * 5
        let s = ShardingSettings.passivating(idle).withRole("worker")
        @Expect(s.role, Some("worker"))
        @Expect(s.passivateAfter, Some(idle))
        // 再换别的设置，角色不丢
        @Expect(s.withBufferSize(77).role, Some("worker"))
        @Expect(s.withBufferSize(77).bufferSize, 77)
    }
}
