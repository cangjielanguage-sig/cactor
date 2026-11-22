/*
 * CActor - 差距 4.4：分片协调状态持久化
 *
 * 覆盖：
 *   - ShardingSettings.withStateStore 后，手动再平衡产生的放置覆盖落盘；
 *   - 区域停止、集群整体重建（新系统、同一持久存储）后，覆盖仍被加载；
 *   - 未配置存储时只保留内存覆盖，重启后为空（行为不回退）；
 *   - withStateStore 保留其它设置字段。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_cluster.membership.*
import cactor_persistence.{MemoryDurableStateStore}
import cactor_testkit.*

class StateEntity <: Actor {
    let id: String

    init(id: String) {
        this.id = id
    }

    public prop name: String { get() { "state-entity" } }
    public prop description: String { get() { "state entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

func stateSettings(store: MemoryDurableStateStore): ShardingSettings {
    ShardingSettings.defaults()
        .withRebalance(RebalancePolicy.disabled())
        .withStateStore(store)
}

@Test
public class ShardingStateTest {
    @TestCase
    public func placementOverridesSurviveRegionRestart(): Unit {
        let store = MemoryDurableStateStore()

        // 第一次运行：把分片 3 覆盖到本节点
        let nodes = startCluster(1, "shard-state", FAST)
        let n = nodes[0]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        let settings = stateSettings(store)
        let region = ClusterSharding(n.cluster, n.kit.system, n.remote, "state-counter", 16,
            { id => StateEntity(id) }, settings)
        try {
            // 新存储没有历史覆盖
            @Expect(awaitCondition(CLUSTER_WAIT, { => region.loadedOverrideCount() == 0 }), true)
            region.rebalanceShard(3, n.address())
            @Expect(awaitCondition(CLUSTER_WAIT, { => region.loadedOverrideOf(3) == Some(n.address()) }), true)
        } finally {
            region.stop()
            closeAll(nodes)
        }

        // 第二次运行：全新系统 + 同一持久存储，覆盖在区域启动时恢复
        let nodes2 = startCluster(1, "shard-state2", FAST)
        let n2 = nodes2[0]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes2, 1) }), true)
        let settings2 = stateSettings(store)
        let revived = ClusterSharding(n2.cluster, n2.kit.system, n2.remote, "state-counter", 16,
            { id => StateEntity(id) }, settings2)
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => revived.loadedOverrideCount() == 1 }), true)
            // 覆盖目标仍是旧地址：分配时会用成员视图校验并回退，但"加载了什么"是持久化事实
            @Expect(revived.loadedOverrideOf(3), Some(n.address()))
        } finally {
            revived.stop()
            closeAll(nodes2)
        }
    }

    @TestCase
    public func withStateStoreDoesNotMutateOriginal(): Unit {
        let base = ShardingSettings.defaults()
        @Expect(base.stateStore().isNone(), true)
        let s1 = base.withStateStore(MemoryDurableStateStore())
        @Expect(base.stateStore().isNone(), true)
        @Expect(s1.stateStore().isSome(), true)
        // 再链式换其它设置，存储不丢；原对象也不被改写
        let s2 = s1.withRole("worker").withBufferSize(99)
        @Expect(s2.stateStore().isSome(), true)
        @Expect(s2.role, Some("worker"))
        @Expect(s2.bufferSize, 99)
        @Expect(base.bufferSize, 10000)
        @Expect(base.role.isNone(), true)
    }

    @TestCase
    public func persistedAssignmentKeepsHostsAcrossRestart(): Unit {
        let store = MemoryDurableStateStore()
        let nodes = startCluster(2, "shard-assign", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let n0 = nodes[0].address()
        let n1 = nodes[1].address()
        // 预置归属：0..8 给 node0、9 给 node1（与 rendezvous 的分布刻意不同）
        let sb = StringBuilder()
        for (sh in 0..9) {
            sb.append("${sh}|${n0}\n")
        }
        sb.append("9|${n1}\n")
        let key = ClusterSharding.assignmentKey("assigned-counter")
        store.upsert(key, store.revisionOf(key) + 1, sb.toString())

        let r0 = ClusterSharding(nodes[0].cluster, nodes[0].kit.system, nodes[0].remote, "assigned-counter", 10,
            { id => StateEntity(id) }, stateSettings(store))
        let r1 = ClusterSharding(nodes[1].cluster, nodes[1].kit.system, nodes[1].remote, "assigned-counter", 10,
            { id => StateEntity(id) }, stateSettings(store))
        try {
            // 两个区域都在启动时恢复了全量归属
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                r0.loadedAssignmentCount() == 10 && r1.loadedAssignmentCount() == 10
            }), true)
            @Expect(r0.loadedAssignmentOf(0), Some(n0))
            @Expect(r0.loadedAssignmentOf(9), Some(n1))
            @Expect(r1.loadedAssignmentOf(9), Some(n1))
            // 实际托管按存储里的归属：9 / 1，而不是 rendezvous 的近似均分
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                let (h0, a0, _) = r0.shardStats()
                let (h1, a1, _) = r1.shardStats()
                h0 == 9 && h1 == 1 && a0 == 0 && a1 == 0
            }), true)
        } finally {
            r0.stop()
            r1.stop()
            closeAll(nodes)
        }
    }

    @TestCase
    public func staleAssignmentFallsBackToAllocation(): Unit {
        let store = MemoryDurableStateStore()
        let nodes = startCluster(1, "shard-stale", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        // 预置一个不存在的宿主：加载成功但托管必须回退到分配策略，不能卡住
        let key = ClusterSharding.assignmentKey("stale-counter")
        store.upsert(key, store.revisionOf(key) + 1, "0|127.0.0.1:1\n")
        let region = ClusterSharding(nodes[0].cluster, nodes[0].kit.system, nodes[0].remote, "stale-counter", 4,
            { id => StateEntity(id) }, stateSettings(store))
        try {
            // 加载过归属（随后领导者会按实际视图重写，所以只断言"加载非空"）
            @Expect(awaitCondition(CLUSTER_WAIT, { => region.loadedAssignmentCount() >= 1 }), true)
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                let (h, a, _) = region.shardStats()
                h == 4 && a == 0
            }), true)
        } finally {
            region.stop()
            closeAll(nodes)
        }
    }

    @TestCase
    public func withoutStateStoreOverridesAreOnlyInMemory(): Unit {
        let nodes = startCluster(1, "shard-state-volatile", FAST)
        let n = nodes[0]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        let settings = ShardingSettings.defaults().withRebalance(RebalancePolicy.disabled())
        @Expect(settings.stateStore().isNone(), true)
        let region = ClusterSharding(n.cluster, n.kit.system, n.remote, "volatile-counter", 16,
            { id => StateEntity(id) }, settings)
        try {
            region.rebalanceShard(5, n.address())
            @Expect(awaitCondition(CLUSTER_WAIT, { => region.loadedOverrideOf(5) == Some(n.address()) }), true)
        } finally {
            region.stop()
            closeAll(nodes)
        }
        // 没有存储可恢复：新存储下重建区域，覆盖为空
        let nodes2 = startCluster(1, "shard-state-volatile2", FAST)
        let n2 = nodes2[0]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes2, 1) }), true)
        let revived = ClusterSharding(n2.cluster, n2.kit.system, n2.remote, "volatile-counter", 16,
            { id => StateEntity(id) }, ShardingSettings.defaults().withRebalance(RebalancePolicy.disabled()))
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => revived.loadedOverrideCount() == 0 }), true)
        } finally {
            revived.stop()
            closeAll(nodes2)
        }
    }
}
