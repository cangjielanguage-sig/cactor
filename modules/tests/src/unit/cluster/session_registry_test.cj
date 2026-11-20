/*
 * CActor Platform.Session - 按 sessionId 寻址（P8）验证
 *
 * 复用集群分片 harness：2 个真实节点、静态分片（关闭按负载再平衡）。
 * 断言：同一 sessionId 从两个节点投递都落到同一个实体（单宿主、计数连续）；
 * 路由型 TypedActorRef 的 tell 与 registry.tell 走同一条分片路径。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{StringMessage}
import cactor_cluster.membership.*
import cactor_platform.session.{SessionRegistry}
import cactor_testkit.*

@Test
public class SessionRegistryTest {
    @TestCase
    public func sessionRoutesToSameActorAcrossNodes(): Unit {
        let nodes = startCluster(2, "session-reg", FAST)
        let ledger = Ledgers()
        let regions = Array<ClusterSharding>(2, { i =>
            let n = nodes[i]
            ClusterSharding(n.cluster, n.kit.system, n.remote, "session", 32,
                { id => CountingEntity(id, n.address(), ledger) }, staticShardingSettings())
        })
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                regions[0].ownerCount() == 2 && regions[1].ownerCount() == 2
            }), true)
            let registry0 = SessionRegistry(regions[0])
            let registry1 = SessionRegistry(regions[1])
            let ids = 20
            for (i in 0..ids) {
                registry0.tell("session-${i}", StringMessage("first"))
            }
            @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == ids }), true)
            for (i in 0..ids) {
                @Expect(ledger.hostsOf("session-${i}").size, 1)
                @Expect(ledger.latestOf("session-${i}"), 1)
            }
            // 同一个 sessionId 从另一个节点（以及路由型引用）继续投递：仍是同一个实体
            for (k in 0..5) {
                registry1.tell("session-3", StringMessage("more"))
            }
            registry0.session<StringMessage>("session-3").tell(StringMessage("typed"))
            @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.latestOf("session-3") == 7 }), true)
            @Expect(ledger.hostsOf("session-3").size, 1)
            @Expect(registry0.pathOf("session-3"), "/session/session/session-3")
        } finally {
            for (r in regions) {
                r.stop()
            }
            closeAll(nodes)
        }
    }
}
