/*
 * CActor - DA5 差距 4.6：Map 系 CRDT（ORMap / LWWMap）与 Replicator 集成
 *
 * 覆盖：
 *   - ORMap 按键独立合并：不同 key 的并发更新互不干扰；
 *   - ORMap 同一 key 并发 put 不同值：add-wins，两个值都保留；
 *   - ORMap 删除值 / 删除键；删除后旧副本的旧数据不会复活；
 *   - LWWMap 按 (timestamp, node) 决胜；删除墓碑在合并后不回退；
 *   - 线编码往返与摘要确定性（相同内容不同节点编码一致）；
 *   - 两个真实 TCP 节点上的 Replicator 便捷方法（putInMap / mapValues / lwwMapGet）收敛。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.ddata.*
import cactor_testkit.*

let MAP_WAIT: Duration = Duration.second * 10

@Test
public class CrdtMapTest {
    @TestCase
    public func orMapMergesPerKeyIndependently(): Unit {
        // 两个副本并发改不同 key：合并后两个 key 都在
        let a = ORMap().put("n1", "region", "east")
        let b = ORMap().put("n2", "tier", "gold")
        let merged = a.merge(b)
        @Expect(merged.keys().size, 2)
        @Expect(merged.values("region"), ["east"])
        @Expect(merged.values("tier"), ["gold"])
        // 合并满足交换律
        @Expect(merged == b.merge(a), true)
        @Expect(merged.keys(), ["region", "tier"])
    }

    @TestCase
    public func orMapConcurrentSameKeyKeepsBoth(): Unit {
        // 同一 key 并发 put 不同值：add-wins，两个值都保留（与 ORSet 语义一致）
        let a = ORMap().put("n1", "owners", "alice")
        let b = ORMap().put("n2", "owners", "bob")
        let merged = a.merge(b)
        @Expect(merged.values("owners"), ["alice", "bob"])
        @Expect(merged.containsKey("owners"), true)
    }

    @TestCase
    public func orMapRemoveValueAndKey(): Unit {
        let base = ORMap().put("n1", "owners", "alice").put("n1", "owners", "bob")
        @Expect(base.values("owners"), ["alice", "bob"])
        // 删除一个值：key 仍在
        let one = base.removeValue("owners", "alice")
        @Expect(one.values("owners"), ["bob"])
        // 删除最后一个值：key 消失
        let none = one.removeValue("owners", "bob")
        @Expect(none.containsKey("owners"), false)
        @Expect(none.size(), 0)
        // 删除键
        @Expect(base.removeKey("owners").containsKey("owners"), false)
        // 旧副本（仍含 alice/bob）与删除后的副本合并：删除生效（观察-删除）
        @Expect(base.merge(none).containsKey("owners"), false)
    }

    @TestCase
    public func lwwMapConvergesToHighestTimestampAndTombstone(): Unit {
        let older = LWWMap().put("region", "east", 10, "n1")
        let newer = LWWMap().put("region", "west", 20, "n2")
        @Expect(older.merge(newer).get("region"), Some("west"))
        @Expect(newer.merge(older).get("region"), Some("west"))
        // 相同时间戳按节点名决胜
        let tieA = LWWMap().put("k", "a", 30, "n1")
        let tieB = LWWMap().put("k", "b", 30, "n2")
        @Expect(tieA.merge(tieB).get("k"), Some("b"))
        // 删除墓碑：更晚时间戳的删除合并后不回退
        let removed = newer.remove("region", 40, "n1")
        @Expect(removed.get("region").isNone(), true)
        @Expect(removed.merge(older).get("region").isNone(), true)
        @Expect(removed.containsKey("region"), false)
        @Expect(removed.size(), 0)
    }

    @TestCase
    public func mapWireRoundtripAndDigest(): Unit {
        let x = ORMap().put("n1", "region", "east")
        let y = ORMap().put("n2", "tier", "gold")
        // 合并顺序不同、内容相同：规范编码与摘要一致
        let va = ReplicatedValue.ORMapValue(x.merge(y))
        let vb = ReplicatedValue.ORMapValue(y.merge(x))
        @Expect(va.digest(), vb.digest())
        let back = ReplicatedValue.decode(va.encode())
        @Expect(back.sameValue(va), true)
        @Expect(back.digest(), va.digest())
        // 值相同但来源节点不同 → 点标记不同，摘要必须不同（不是只比用户可见值）
        @Expect(ReplicatedValue.ORMapValue(x).digest() != ReplicatedValue.ORMapValue(
            ORMap().put("n2", "region", "east")).digest(), true)

        let lww = LWWMap().put("region", "east", 10, "n1").put("tier", "gold", 20, "n2")
        let vl = ReplicatedValue.LWWMapValue(lww)
        @Expect(ReplicatedValue.decode(vl.encode()).sameValue(vl), true)
        @Expect(ReplicatedValue.decode(vl.encode()).digest(), vl.digest())
        // 墓碑也参与编码：往返后删除仍生效
        let tomb = LWWMap().put("region", "east", 10, "n1").remove("region", 20, "n1")
        let vt = ReplicatedValue.LWWMapValue(tomb)
        let decoded = ReplicatedValue.decode(vt.encode())
        @Expect(decoded.sameValue(vt), true)
        match (decoded) {
            case LWWMapValue(m) => @Expect(m.get("region").isNone(), true)
            case _ => @Expect(false, true)
        }
    }
}

@Test
public class MapReplicatorTest {
    @TestCase
    public func putInMapConvergesAcrossTwoNodes(): Unit {
        let kitA = ActorTestKit("map-rep-a")
        let kitB = ActorTestKit("map-rep-b")
        let a = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        let b = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        a.start()
        b.start()
        let ra = Replicator(kitA.system, a, GOSSIP)
        let rb = Replicator(kitB.system, b, GOSSIP)
        try {
            ra.join(b.getLocalAddress())
            rb.join(a.getLocalAddress())
            // 两个节点并发改同一 Map 的不同 key
            ra.putInMap("config", "region", "east")
            rb.putInMap("config", "tier", "gold")
            @Expect(awaitCondition(MAP_WAIT, { =>
                ra.mapValues("config", "tier").contains("gold") &&
                rb.mapValues("config", "region").contains("east")
            }), true)
            @Expect(ra.mapValues("config", "region"), ["east"])
            @Expect(rb.mapValues("config", "tier"), ["gold"])

            // LWWMap：更晚的时间戳覆盖较早的值
            ra.putInLWWMap("cfg-lww", "mode", "fast", 10)
            rb.putInLWWMap("cfg-lww", "mode", "safe", 20)
            @Expect(awaitCondition(MAP_WAIT, { =>
                ra.lwwMapGet("cfg-lww", "mode") == Some("safe") &&
                rb.lwwMapGet("cfg-lww", "mode") == Some("safe")
            }), true)
        } finally {
            ra.stop()
            rb.stop()
            a.stop()
            b.stop()
            kitA.shutdown()
            kitB.shutdown()
        }
    }
}
