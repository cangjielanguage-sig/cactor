/*
 * CActor Cluster.Monitoring - 统一指标渲染入口（差距 4.10 的指标合并部分）
 *
 * 此前运行时指标走 `CActorSystem.prometheusMetrics`，集群/远程指标走 `ClusterMetrics.renderPrometheus`，
 * 应用指标又各自拼字符串：一个 `/metrics` 端点要调用三方并手工拼接，标签/换行/顺序容易出错。
 *
 * 这里提供**单一入口**：
 * - `MetricsExporter(system)`：运行时指标作为第一段；
 * - `addCluster(metrics)`：追加集群/远程指标（可多个，例如不同子系统各自持有 ClusterMetrics）；
 * - `addProvider(name, render)`：追加应用自定义指标（Prometheus 文本），带来源注释；
 * - `renderPrometheus(perActor!)`：一次性返回完整文本，供 HTTP 端点直接输出。
 *
 * 不做指标重命名/去重：各来源的指标名本应全局唯一（cactor_actors_* / cactor_cluster_* / 应用前缀）；
 * 如重名，Prometheus 侧会报重复序列，应由接入方修正，而不是在这里悄悄吞掉。
 */
package cactor_cluster.monitoring

import std.collection.*
import cactor.api.{CActorSystem}
import cactor_cluster.membership.{ClusterMetrics}

public class MetricsExporter {
    private let system: CActorSystem
    private let clusterSources = ArrayList<ClusterMetrics>()
    private let providers = ArrayList<(String, () -> String)>()

    public init(system: CActorSystem) {
        this.system = system
    }

    /** 追加一个集群指标来源（可多次调用） */
    public func addCluster(metrics: ClusterMetrics): MetricsExporter {
        clusterSources.add(metrics)
        this
    }

    /** 追加应用自定义指标来源；render 返回 Prometheus 文本 */
    public func addProvider(name: String, render: () -> String): MetricsExporter {
        if (name.isEmpty()) {
            throw IllegalArgumentException("指标来源名称不能为空")
        }
        providers.add((name, render))
        this
    }

    /**
     * 渲染完整 Prometheus 文本。段与段之间保证恰好一个换行；空来源不产生多余空行。
     */
    public func renderPrometheus(perActor!: Bool = false): String {
        let sb = StringBuilder()
        appendSection(sb, system.metricsText(perActor))
        for (m in clusterSources) {
            appendSection(sb, m.renderPrometheus())
        }
        for ((name, render) in providers) {
            appendSection(sb, "# metrics from ${name}\n${render()}")
        }
        sb.toString()
    }

    private func appendSection(sb: StringBuilder, text: String): Unit {
        if (text.isEmpty()) {
            return
        }
        if (sb.size > 0) {
            sb.append("\n")
        }
        sb.append(text)
        if (!text.endsWith("\n")) {
            sb.append("\n")
        }
    }
}
