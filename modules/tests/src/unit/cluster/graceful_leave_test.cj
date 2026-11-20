/*
 * CActor - 集群优雅离开接入协调关闭（计划 C1：`CoordinatedShutdown.PhaseClusterLeave` ↔ `Cluster.leave`）
 *
 * 断言：启用 `Cluster.installCoordinatedShutdown()` 后，节点正常 terminate 时
 * 先进入 Leaving/Exiting（其余成员收到 MemberLeft/MemberExited），而不是直接变成不可达。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{StringMessage}
import cactor_testkit.*

func sawLeave(rec: EventRecorder): Bool {
    for (e in rec.snapshot()) {
        if (e.contains("MemberLeft") || e.contains("MemberExited")) {
            return true
        }
    }
    false
}

@Test
public class GracefulLeaveTest {
    @TestCase
    public func coordinatedShutdownLeavesClusterGracefully(): Unit {
        let nodes = startCluster(3, "grace-leave", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        for (n in nodes) {
            n.cluster.installCoordinatedShutdown()
        }
        // 正常关闭（不是 crash）：协调关闭会先发送 leave
        nodes[2].kit.system.terminate()
        nodes[2].remote.stop()
        let seen = awaitCondition(CLUSTER_WAIT, { =>
            sawLeave(nodes[0].events) || sawLeave(nodes[1].events)
        })
        if (!seen) {
            println("events0=" + nodes[0].events.snapshot().toString())
            println("events1=" + nodes[1].events.snapshot().toString())
        }
        @Expect(seen, true)
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            nodes[0].cluster.state().upCount() == 2 && nodes[1].cluster.state().upCount() == 2
        }), true)
        closeAll([nodes[0], nodes[1]])
    }

    /**
     * C3：带分片区域的优雅离开不阻塞、离开后本地实体清空、幸存节点收敛为 2。
     * 移交是"分片归属重算 + 旧节点停止实体"，这里验证流程在预算内完成且无崩溃；
     * 实体在新节点按需重建由分片用例（sharding_test）覆盖。
     */
    @TestCase
    public func leaveWithShardingsCompletesWithinBudget(): Unit {
        let nodes = startCluster(3, "grace-shard", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let ledger = Ledgers()
        let regions = staticShardings(nodes, ledger)
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            regions[0].ownerCount() == 3 && regions[1].ownerCount() == 3 && regions[2].ownerCount() == 3
        }), true)
        for (e in 0..200) {
            regions[0].tell("e${e}", StringMessage("x"))
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 200 }), true)
        // 大概率有实体落在 node2；即便没有，下面的关闭流程也必须正常完成
        nodes[2].cluster.installCoordinatedShutdown(waitForLeave: Duration.second * 10,
            shardings: [regions[2]])
        let t0 = MonoTime.now()
        nodes[2].kit.system.terminate()
        nodes[2].remote.stop()
        let elapsed = MonoTime.now() - t0
        @Expect(elapsed <= Duration.second * 15, true)
        @Expect(regions[2].localEntityCount(), 0)
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            nodes[0].cluster.state().upCount() == 2 && nodes[1].cluster.state().upCount() == 2
        }), true)
        closeAll([nodes[0], nodes[1]])
    }
}
