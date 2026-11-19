/*
 * CActor Cluster - 集群 / 远程侧 Prometheus 指标（差距 4.10 的指标部分）
 *
 * 此前 `/metrics` 只有运行时指标（Actor 数、死信、调度器），集群侧 0 指标：
 * 看不到成员数、不可达数、分片托管数、隔离表大小、可靠投递未确认量。
 *
 * 设计：
 * - 抓取时一次性从 Cluster / ClusterSharding / Replicator / SimpleRemoteManager 取快照，
 *   写入 foundation 的 `MetricRegistry`（唯一指标源），同时渲染 Prometheus 文本；
 * - 不在业务路径打点：这些都是"读现有原子计数"的 gauge，抓取频率由 Prometheus 决定；
 * - 可选组件（分片、Replicator）未挂接时不输出对应指标，避免出现恒为 0 的误导序列。
 */
package cactor_cluster.membership

import std.collection.*
import cactor_foundation.monitoring.{MetricRegistry}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.ddata.{Replicator}

/** 集群指标采集与 Prometheus 渲染（每次 render 读取一次实时快照） */
public class ClusterMetrics {
    private let cluster: Cluster
    private let remote: SimpleRemoteManager
    private var sharding: Option<ClusterSharding> = None<ClusterSharding>
    private var replicator: Option<Replicator> = None<Replicator>
    private let registry = MetricRegistry()

    public init(cluster: Cluster, remote: SimpleRemoteManager) {
        this.cluster = cluster
        this.remote = remote
        // 固定指标集：值在每次 render 时刷新
        registry.gauge("cactor_cluster_members_up")
        registry.gauge("cactor_cluster_members_total")
        registry.gauge("cactor_cluster_unreachable")
        registry.gauge("cactor_cluster_converged")
        registry.gauge("cactor_cluster_gossip_total")
        registry.gauge("cactor_cluster_quarantined")
        registry.gauge("cactor_cluster_pending_asks")
        registry.gauge("cactor_remote_reliable_resent_total")
        registry.gauge("cactor_remote_reliable_duplicates_total")
        registry.gauge("cactor_remote_reliable_unacked")
    }

    /** 挂接分片：之后输出分片托管 / 获取中 / 实体数指标 */
    public func attachSharding(s: ClusterSharding): ClusterMetrics {
        sharding = Some(s)
        registry.gauge("cactor_cluster_shards_owned")
        registry.gauge("cactor_cluster_shards_acquiring")
        registry.gauge("cactor_cluster_entities_local")
        registry.gauge("cactor_cluster_entities_passivated_total")
        this
    }

    /** 挂接 Replicator：输出键数量 */
    public func attachReplicator(r: Replicator): ClusterMetrics {
        replicator = Some(r)
        registry.gauge("cactor_cluster_ddata_keys")
        this
    }

    /** 程序化读取（唯一指标源，已由最近一次 render 刷新） */
    public func registryValue(): MetricRegistry {
        registry
    }

    /**
     * 读取实时值、同步进 MetricRegistry，并返回 Prometheus 文本（text/plain; version=0.0.4）。
     */
    public func renderPrometheus(): String {
        let snap = cluster.state()
        var up: Int64 = 0
        for (m in snap.members) {
            if (m.status == MemberStatus.Up) {
                up++
            }
        }
        let total = Int64(snap.members.size)
        let unreachable = Int64(snap.unreachable.size)
        let converged: Int64 = if (snap.converged) { 1 } else { 0 }
        let gossip = cluster.gossipCount()
        let quarantined = remote.quarantinedCount()
        let pendingAsks = remote.pendingAskCount()
        let (resent, duplicates, unacked) = remote.reliableStats()

        set("cactor_cluster_members_up", up)
        set("cactor_cluster_members_total", total)
        set("cactor_cluster_unreachable", unreachable)
        set("cactor_cluster_converged", converged)
        set("cactor_cluster_gossip_total", gossip)
        set("cactor_cluster_quarantined", quarantined)
        set("cactor_cluster_pending_asks", pendingAsks)
        set("cactor_remote_reliable_resent_total", resent)
        set("cactor_remote_reliable_duplicates_total", duplicates)
        set("cactor_remote_reliable_unacked", unacked)

        let sb = StringBuilder()
        writeGauge(sb, "cactor_cluster_members_up", "Cluster members in Up state", up)
        writeGauge(sb, "cactor_cluster_members_total", "Cluster members not yet removed", total)
        writeGauge(sb, "cactor_cluster_unreachable", "Members currently considered unreachable", unreachable)
        writeGauge(sb, "cactor_cluster_converged", "1 when the local gossip view is converged", converged)
        writeGauge(sb, "cactor_cluster_gossip_total", "Gossip messages sent by this node", gossip)
        writeGauge(sb, "cactor_cluster_quarantined", "Quarantined remote node instances", quarantined)
        writeGauge(sb, "cactor_cluster_pending_asks", "Remote asks awaiting a reply", pendingAsks)
        writeGauge(sb, "cactor_remote_reliable_resent_total", "Reliable messages resent", resent)
        writeGauge(sb, "cactor_remote_reliable_duplicates_total", "Reliable messages dropped as duplicates", duplicates)
        writeGauge(sb, "cactor_remote_reliable_unacked", "Reliable messages awaiting acknowledgement", unacked)

        match (sharding) {
            case Some(s) =>
                let (hosted, acquiring, completed) = s.shardStats()
                let entities = s.localEntityCount()
                let passivated = s.passivatedCount()
                set("cactor_cluster_shards_owned", hosted)
                set("cactor_cluster_shards_acquiring", acquiring)
                set("cactor_cluster_entities_local", entities)
                set("cactor_cluster_entities_passivated_total", passivated)
                writeGauge(sb, "cactor_cluster_shards_owned", "Shards hosted on this node", hosted)
                writeGauge(sb, "cactor_cluster_shards_acquiring", "Shards this node is acquiring", acquiring)
                writeGauge(sb, "cactor_cluster_shards_acquisitions_total", "Completed shard acquisitions", completed)
                writeGauge(sb, "cactor_cluster_entities_local", "Entities running on this node", entities)
                writeGauge(sb, "cactor_cluster_entities_passivated_total", "Entities passivated by idle timeout", passivated)
            case None => ()
        }
        match (replicator) {
            case Some(r) =>
                let keys = Int64(r.keys().size)
                set("cactor_cluster_ddata_keys", keys)
                writeGauge(sb, "cactor_cluster_ddata_keys", "Replicated data keys known to this node", keys)
            case None => ()
        }
        sb.toString()
    }

    private func set(name: String, value: Int64): Unit {
        registry.gauge(name).setValue(Float64(value))
    }

    private func writeGauge(sb: StringBuilder, name: String, help: String, value: Int64): Unit {
        sb.append("# HELP ${name} ${help}\n")
        sb.append("# TYPE ${name} gauge\n")
        sb.append("${name} ${value}\n")
    }
}
