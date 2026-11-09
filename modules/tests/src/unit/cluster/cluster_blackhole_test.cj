/*
 * CActor - 黑洞注入 harness（E62 的多节点 harness 部分）
 *
 * 用 `cactor_testkit.BlackholeTransport` 在出站方向丢弃全部帧（含优先通道），
 * 验证：故障检测把对端判为不可达 → 恢复连通后重新可达并收敛。
 * 相比 `PartitionTransport`（只做分区表），它是可复用的测试辅助，任何 `NetworkTransport` 都能包。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.network.{NetworkAddress, TcpTransport}
import cactor_cluster.membership.*
import cactor_testkit.*

@Test
public class ClusterBlackholeTest {
    @TestCase
    public func blackholeMakesPeerUnreachableThenHeals(): Unit {
        let ta = BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", 0)))
        let tb = BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", 0)))
        let a = CNode("bh-a", ta)
        let b = CNode("bh-b", tb)
        a.join([a.addr()], FAST)
        b.join([a.addr()], FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)

        // 双向黑洞：心跳、gossip、业务帧全部丢弃（NoDowning：只是不可达，不会被移除）
        ta.blackhole(b.address())
        tb.blackhole(a.address())
        @Expect(awaitCondition(Duration.second * 15, { =>
            a.cluster.state().unreachable.contains(b.address()) &&
            b.cluster.state().unreachable.contains(a.address())
        }), true)
        @Expect(ta.droppedCount() > 0, true)
        @Expect(tb.droppedCount() > 0, true)

        // 恢复：成员重新可达、视图重新收敛，且两个节点都还在（没有被误 down）
        ta.healAll()
        tb.healAll()
        @Expect(awaitCondition(Duration.second * 20, { =>
            !a.cluster.state().unreachable.contains(b.address()) &&
            !b.cluster.state().unreachable.contains(a.address()) &&
            allUp2([a, b], 2)
        }), true)

        a.close()
        b.close()
    }
}
