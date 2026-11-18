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
}
