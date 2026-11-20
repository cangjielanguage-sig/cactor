/*
 * CActor Platform.KV - DData 实现（F6 / AR9）
 *
 * 两个真实节点共用同一 namespace：写后跨节点可见；PNCounter 增量合并；
 * 过半读在副本不足时明确失败（KvReadConsistencyException），而不是返回本地旧值。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_cluster.ddata.{Replicator}
import cactor_platform.kv.*
import cactor_testkit.*

@Test
public class DDataKvTest {
    @TestCase
    public func dDataKvStoreRoundTrips(): Unit {
        let nodes = startCluster(2, "kv-ddata", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let repA = Replicator(nodes[0].kit.system, nodes[0].remote, GOSSIP)
        let repB = Replicator(nodes[1].kit.system, nodes[1].remote, GOSSIP)
        repA.join(nodes[1].addr())
        repB.join(nodes[0].addr())
        let kvA = DDataKvStore(repA, namespace: "test")
        let kvB = DDataKvStore(repB, namespace: "test")

        kvA.put("user:1", "alice")
        @Expect(awaitCondition(CLUSTER_WAIT, { => (kvB.get("user:1") ?? "") == "alice" }), true)
        kvB.put("user:2", "bob")
        @Expect(awaitCondition(CLUSTER_WAIT, { => (kvA.get("user:2") ?? "") == "bob" }), true)
        @Expect(kvA.list("user:").size, 2)
        kvA.delete("user:1")
        @Expect(awaitCondition(CLUSTER_WAIT, { => kvB.get("user:1").isNone() }), true)

        // PNCounter 增量跨节点合并（计数 key 由 DDataKvStore 内部约定）
        kvA.incr("hits", 5)
        kvB.incr("hits", 3)
        @Expect(awaitCondition(CLUSTER_WAIT, { => repA.counterValue("kv-counter/test/hits") == 8 }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => repB.counterValue("kv-counter/test/hits") == 8 }), true)

        repA.stop()
        repB.stop()
        closeAll(nodes)
    }

    @TestCase
    public func kvReadMajorityFailsWhenUnreachable(): Unit {
        let nodes = startCluster(2, "kv-majority", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let repA = Replicator(nodes[0].kit.system, nodes[0].remote, GOSSIP)
        let repB = Replicator(nodes[1].kit.system, nodes[1].remote, GOSSIP)
        repA.join(nodes[1].addr())
        repB.join(nodes[0].addr())
        let kvA = DDataKvStore(repA, namespace: "test")
        kvA.put("k", "v")
        @Expect((kvA.getMajority("k", timeout: Duration.second * 3) ?? ""), "v")

        // B 崩溃后，2 节点集群的过半读（2 个）无法满足
        nodes[1].crash()
        var failed = false
        try {
            let _ = kvA.getMajority("k", timeout: Duration.millisecond * 300)
        } catch (e: KvReadConsistencyException) {
            failed = true
        }
        @Expect(failed, true)
        repA.stop()
        nodes[0].remote.stop()
        nodes[0].kit.shutdown()
    }
}
