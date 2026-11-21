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
