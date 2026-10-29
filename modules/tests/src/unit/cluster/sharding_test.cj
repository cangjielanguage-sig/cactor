/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R6 测试：集群分片（真实节点）与分片哈希
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage, MessageSerializer}
import cactor_cluster.membership.*
import cactor_testkit.*

/** 实体内的计数（单节点内 Actor 串行处理，不需要锁）；每条消息记录 "实体@节点=累计" */
class CountingEntity <: Actor {
    let id: String
    let node: String
    let ledger: Ledgers
    var total: Int64 = 0

    init(id: String, node: String, ledger: Ledgers) {
        this.id = id
        this.node = node
        this.ledger = ledger
    }

    public prop name: String { get() { "entity" } }
    public prop description: String { get() { "entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            total++
            ledger.record(id, node, total, s.getContent())
        }
        MessageResult.Handled
    }
}

/** 线程安全的记录：每个实体出现在哪些节点、最近的累计值、总消息数 */
class Ledgers {
    let lock = Mutex()
    let hosts = HashMap<String, HashSet<String>>()
    let latest = HashMap<String, Int64>()
    var messages: Int64 = 0

    func record(id: String, node: String, total: Int64, content: String): Unit {
        synchronized(lock) {
            match (hosts.get(id)) {
                case Some(s) => s.add(node)
                case None =>
                    let s = HashSet<String>()
                    s.add(node)
                    hosts[id] = s
            }
            latest[id] = total
            messages++
        }
    }

    func count(): Int64 {
        synchronized(lock) { messages }
    }

    func hostsOf(id: String): Array<String> {
        synchronized(lock) {
            match (hosts.get(id)) {
                case Some(s) => s.toArray()
                case None => Array<String>()
            }
        }
    }

    func latestOf(id: String): Int64 {
        synchronized(lock) { latest.get(id) ?? 0 }
    }

    func clearHosts(): Unit {
        synchronized(lock) {
            hosts.clear()
        }
    }
}

func shardings(nodes: Array<CNode>, ledger: Ledgers): Array<ClusterSharding> {
    Array<ClusterSharding>(nodes.size, { i =>
        let n = nodes[i]
        let addr = n.address()
        ClusterSharding(n.cluster, n.kit.system, n.remote, "counter", 64, { id => CountingEntity(id, addr, ledger) })
    })
}

@Test
public class ShardingHashTest {
    @TestCase
    public func testStableHashKnownVectors(): Unit {
        // FNV-1a 64 位标准测试向量
        @Expect(stableHash(""), 0xcbf29ce484222325)
        @Expect(stableHash("a"), 0xaf63dc4c8601ec8c)
        @Expect(stableHash("foobar"), 0x85944171f73967e8)
        @Expect(shardOf("user-1", 100), shardOf("user-1", 100))
    }

    @TestCase
    public func testRendezvousMovesOnlyToNewMember(): Unit {
        let before = ["10.0.0.1:2551", "10.0.0.2:2551", "10.0.0.3:2551"]
        let after = ["10.0.0.1:2551", "10.0.0.2:2551", "10.0.0.3:2551", "10.0.0.4:2551"]
        var moved: Int64 = 0
        let perOwner = HashMap<String, Int64>()
        for (s in 0..1000) {
            let a = shardOwner(s, before).getOrThrow()
            let b = shardOwner(s, after).getOrThrow()
            if (a != b) {
                moved++
                // 只会移到新成员上
                @Expect(b, "10.0.0.4:2551")
            }
            perOwner[a] = (perOwner.get(a) ?? 0) + 1
        }
        // 约 1/4 的分片移动
        @Expect(moved > 180 && moved < 320, true)
        // 分布大致均匀
        for ((_, n) in perOwner) {
            @Expect(n > 250 && n < 420, true)
        }
        // 成员顺序不影响结果
        @Expect(shardOwner(7, ["b", "a", "c"]), shardOwner(7, ["c", "b", "a"]))
        @Expect(shardOwner(7, Array<String>()), None<String>)
    }

    @TestCase
    public func testEnvelopeCodecRoundTrip(): Unit {
        // 注册编解码器（构造 ClusterSharding 时也会注册）
        let nodes = startCluster(1, "sh-codec", FAST)
        let _ = ClusterSharding(nodes[0].cluster, nodes[0].kit.system, nodes[0].remote, "codec", 4, { id => CountingEntity(id, "", Ledgers()) })
        let ser = MessageSerializer()
        let env = ShardingEnvelope("用户:42/a%b", StringMessage("内容:1"))
        let back = (ser.deserialize(ser.serialize(env)) as ShardingEnvelope).getOrThrow()
        @Expect(back.entityId, "用户:42/a%b")
        @Expect((back.payload as StringMessage).getOrThrow().getContent(), "内容:1")
        closeAll(nodes)
    }
}

@Test
public class ClusterShardingTest {
    @TestCase
    public func testEachEntityLivesOnExactlyOneNode(): Unit {
        let nodes = startCluster(3, "sh-basic", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let ledger = Ledgers()
        let regions = shardings(nodes, ledger)
        // 30 个实体 × 10 条消息，轮流从三个节点发送
        for (k in 0..10) {
            for (e in 0..30) {
                regions[(e + k) % 3].tell("entity-${e}", StringMessage("${k}"))
            }
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 300 }), true)
        var hostedTotal: Int64 = 0
        for (e in 0..30) {
            let id = "entity-${e}"
            @Expect(ledger.hostsOf(id).size, 1)
            // 计数准确：每个实体只有一个实例，10 条消息全部到达
            @Expect(ledger.latestOf(id), 10)
        }
        for (r in regions) {
            // 三个节点都承担了一部分实体
            @Expect(r.localEntityCount() > 0, true)
            hostedTotal += r.localEntityCount()
        }
        @Expect(hostedTotal, 30)
        let (_, forwarded, _, dropped, _) = regions[0].stats()
        @Expect(forwarded > 0, true)
        @Expect(dropped, 0)
        for (r in regions) {
            r.stop()
        }
        closeAll(nodes)
    }

    @TestCase
    public func testRebalanceWhenNodeJoins(): Unit {
        let nodes = startCluster(2, "sh-join", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let ledger = Ledgers()
        let regions = ArrayList<ClusterSharding>(shardings(nodes, ledger))
        for (e in 0..40) {
            regions[0].tell("e${e}", StringMessage("first"))
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 40 }), true)
        // 第三个节点加入
        let third = CNode("sh-join-2").join([nodes[0].addr()], FAST)
        let all = [nodes[0], nodes[1], third]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(all, 3) }), true)
        let addr = third.address()
        regions.add(ClusterSharding(third.cluster, third.kit.system, third.remote, "counter", 64,
            { id => CountingEntity(id, addr, ledger) }))
        // 旧节点移交不再拥有的实体
        @Expect(awaitCondition(CLUSTER_WAIT, { => regions[0].stats()[2] + regions[1].stats()[2] > 0 }), true)
        ledger.clearHosts()
        for (e in 0..40) {
            regions[1].tell("e${e}", StringMessage("second"))
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 80 }), true)
        // 新节点承担了一部分实体；任何实体都只在一个节点上
        @Expect(regions[2].localEntityCount() > 0, true)
        let seen = HashSet<String>()
        var total: Int64 = 0
        for (r in regions) {
            for (id in r.localEntityIds()) {
                @Expect(seen.contains(id), false)
                seen.add(id)
                total++
            }
        }
        @Expect(total, 40)
        for (e in 0..40) {
            @Expect(ledger.hostsOf("e${e}").size, 1)
        }
        for (r in regions) {
            r.stop()
        }
        closeAll(all)
    }

    @TestCase
    public func testEntitiesOfCrashedNodeAreRecreatedElsewhere(): Unit {
        let nodes = startCluster(3, "sh-crash", FAST_AUTODOWN)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let ledger = Ledgers()
        let regions = shardings(nodes, ledger)
        for (e in 0..30) {
            regions[0].tell("c${e}", StringMessage("x"))
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 30 }), true)
        let victimIds = regions[2].localEntityIds()
        @Expect(victimIds.size > 0, true)
        nodes[2].crash()
        let survivors = [nodes[0], nodes[1]]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(survivors, 2) }), true)
        ledger.clearHosts()
        let before = ledger.count()
        for (id in victimIds) {
            regions[1].tell(id, StringMessage("y"))
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == before + victimIds.size }), true)
        for (id in victimIds) {
            let hosts = ledger.hostsOf(id)
            @Expect(hosts.size, 1)
            @Expect(hosts[0] != nodes[2].address(), true)
            // 没有持久化：新实体从 0 开始计数
            @Expect(ledger.latestOf(id), 1)
        }
        regions[0].stop()
        regions[1].stop()
        nodes[0].close()
        nodes[1].close()
    }
}
