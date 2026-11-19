/*
 * CActor - 差距 4.10：集群 / 远程侧 Prometheus 指标
 *
 * 覆盖：
 *   - 成员数、不可达数、收敛标志、gossip 计数从真实集群读取；
 *   - 挂接分片后输出托管分片数 / 本地实体数；挂接 Replicator 后输出键数；
 *   - 隔离远程实例后 quarantined 指标变化；
 *   - 同一份数据同步进 MetricRegistry（唯一指标源），程序化读取与文本一致。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.monitoring.{GaugeMetric}
import cactor_cluster.membership.{Cluster, ClusterMetrics, ClusterSharding, ClusterSettings}
import cactor_cluster.ddata.{Replicator}
import cactor_testkit.*

class MetricsEntity <: Actor {
    let id: String

    init(id: String) {
        this.id = id
    }

    public prop name: String { get() { "metrics-entity" } }
    public prop description: String { get() { "metrics entity" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

func gaugeOf(metrics: ClusterMetrics, name: String): Float64 {
    match (metrics.registryValue().getMetric(name)) {
        case Some(m: GaugeMetric) => m.getCurrentValue()
        case _ => -1.0
    }
}

@Test
public class ClusterMetricsTest {
    @TestCase
    public func exportsMembersShardsAndRemoteGauges(): Unit {
        let nodes = Array<CNode>(2, { i => CNode("metrics-${i}") })
        let seed = nodes[0].addr()
        for (n in nodes) {
            n.join([seed], FAST)
        }
        let rep = Replicator(nodes[0].kit.system, nodes[0].remote, GOSSIP)
        let regions = Array<ClusterSharding>(2, { i =>
            ClusterSharding(nodes[i].cluster, nodes[i].kit.system, nodes[i].remote, "metrics-entity", 8,
                { id => MetricsEntity(id) })
        })
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[0].cluster.state().upCount() == 2 }), true)

            // 让分片真正托管实体
            for (i in 0..4) {
                regions[0].tell("entity-${i}", StringMessage("x"))
            }
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                regions[0].localEntityCount() + regions[1].localEntityCount() >= 1
            }), true)

            let metrics = ClusterMetrics(nodes[0].cluster, nodes[0].remote)
                .attachSharding(regions[0]).attachReplicator(rep)
            rep.putInMap("config", "mode", "fast")

            let text = metrics.renderPrometheus()
            @Expect(text.contains("cactor_cluster_members_up 2"), true)
            @Expect(text.contains("cactor_cluster_members_total 2"), true)
            @Expect(text.contains("cactor_cluster_converged 1"), true)
            @Expect(text.contains("cactor_cluster_shards_owned"), true)
            @Expect(text.contains("cactor_cluster_entities_local"), true)
            @Expect(text.contains("cactor_cluster_ddata_keys"), true)
            @Expect(text.contains("# TYPE cactor_cluster_members_up gauge"), true)

            // 指标源与文本一致
            @Expect(gaugeOf(metrics, "cactor_cluster_members_up"), 2.0)
            @Expect(gaugeOf(metrics, "cactor_cluster_converged"), 1.0)
            @Expect(gaugeOf(metrics, "cactor_cluster_ddata_keys") >= 1.0, true)
            @Expect(gaugeOf(metrics, "cactor_cluster_entities_local"), Float64(regions[0].localEntityCount()))

            // 隔离对端：quarantine 指标立即反映
            nodes[0].remote.quarantine(nodes[1].addr(), "metrics-test")
            let after = metrics.renderPrometheus()
            @Expect(after.contains("cactor_cluster_quarantined 1"), true)
            @Expect(gaugeOf(metrics, "cactor_cluster_quarantined"), 1.0)
        } finally {
            for (r in regions) {
                r.stop()
            }
            rep.stop()
            closeAll(nodes)
        }
    }
}
