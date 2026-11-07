/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Runtime - Prometheus 文本格式指标导出
 *
 * 输出 Prometheus exposition format（text/plain; version=0.0.4），可直接作为 /metrics 的响应体：
 * - 运行时：存活 Actor 数、死信数、调度器线程数 / 执行次数 / 就绪队列长度
 * - 每个 Actor（可选）：已处理消息数、邮箱长度、重启次数、未处理消息数、死信数
 *
 * 邮箱长度是遍历得到的近似值（O(邮箱长度)），Actor 很多或邮箱很长时建议关闭逐 Actor 指标。
 */
package cactor_runtime

import std.collection.*
import std.sort.*
import cactor_runtime.guardian.{ActorCellDispatcher, SimpleUserActorRef}

/** 转义标签值中的反斜杠、双引号和换行 */
public func escapePrometheusLabel(value: String): String {
    var out = StringBuilder()
    for (r in value.runes()) {
        match (r) {
            case '\\' => out.append("\\\\")
            case '"' => out.append("\\\"")
            case '\n' => out.append("\\n")
            case _ => out.append(r)
        }
    }
    out.toString()
}

class PrometheusWriter {
    let sb = StringBuilder()

    func metric(name: String, kind: String, help: String, value: Int64): Unit {
        header(name, kind, help)
        sb.append("${name} ${value}\n")
    }

    func header(name: String, kind: String, help: String): Unit {
        sb.append("# HELP ${name} ${help}\n")
        sb.append("# TYPE ${name} ${kind}\n")
    }

    func sample(name: String, labels: String, value: Int64): Unit {
        sb.append("${name}{${labels}} ${value}\n")
    }
}

/**
 * 每 Actor 指标默认最多输出的 Actor 数（标签基数上限）：path 标签每个 Actor 一个值，
 * 5 万个 Actor × 5 个指标就是 25 万条时间序列，足以拖垮 Prometheus
 */
public let DEFAULT_MAX_ACTOR_SERIES: Int64 = 1000

extend CActorRuntime {
    /**
     * 以 Prometheus 文本格式导出指标
     *
     * @param perActor 是否输出每个 Actor 的指标（标签 path）
     */
    public func prometheusMetrics(perActor: Bool): String {
        prometheusMetrics(perActor, DEFAULT_MAX_ACTOR_SERIES)
    }

    /**
     * @param maxActorSeries 每 Actor 指标最多覆盖的 Actor 数。超出时按路径排序取前 maxActorSeries 个
     *        （同一组 Actor 在多次抓取间保持稳定，不会因遍历顺序变化而产生序列抖动），
     *        并输出 cactor_actor_series_omitted 说明省略了多少个
     */
    public func prometheusMetrics(perActor: Bool, maxActorSeries: Int64): String {
        let w = PrometheusWriter()
        let sys = escapePrometheusLabel(getSystemName())
        let guardian = getUserGuardian()
        w.metric("cactor_actors_live", "gauge", "Number of live actors (including children)", guardian.liveCellCount())
        w.metric("cactor_dead_letters_total", "counter", "Messages that could not be delivered", guardian.deadLetterCount())
        w.metric("cactor_dispatcher_threads", "gauge", "Worker threads of the default dispatcher", ActorCellDispatcher.getWorkerCount())
        w.metric("cactor_dispatcher_executions_total", "counter", "Actor runs executed by the default dispatcher", ActorCellDispatcher.getExecutedCount())
        w.metric("cactor_dispatcher_queued", "gauge", "Actors waiting in the default dispatcher run queues", ActorCellDispatcher.getQueuedCount())
        w.metric("cactor_dispatcher_starvation_warnings_total", "counter",
            "Times a default-dispatcher worker was stuck in one actor run longer than CACTOR_STARVATION_WARN_MS",
            ActorCellDispatcher.starvationWarningCount())
        w.metric("cactor_blocking_dispatcher_threads", "gauge", "Worker threads of the blocking dispatcher", ActorCellDispatcher.getBlockingWorkerCount())
        if (perActor) {
            let all = guardian.liveCells()
            let limit = if (maxActorSeries < 0) { 0 } else { maxActorSeries }
            let cells = if (all.size <= limit) {
                all
            } else {
                let paths = Array<(String, SimpleUserActorRef)>(all.size, { i => (all[i].path.getPath(), all[i]) })
                sort(paths, lessThan: { a: (String, SimpleUserActorRef), b: (String, SimpleUserActorRef) => a[0] < b[0] })
                Array<SimpleUserActorRef>(limit, { i => paths[i][1] })
            }
            w.metric("cactor_actor_series_omitted", "gauge", "Live actors left out of per-actor metrics by the series limit",
                all.size - cells.size)
            let series: Array<(String, String, (SimpleUserActorRef) -> Int64)> = [
                ("cactor_actor_processed_total", "counter", { c => c.processed() }),
                ("cactor_actor_mailbox_size", "gauge", { c => c.pending() }),
                ("cactor_actor_restarts_total", "counter", { c => c.restarts() }),
                ("cactor_actor_unhandled_total", "counter", { c => c.unhandled() }),
                ("cactor_actor_dead_letters_total", "counter", { c => c.deadLetters() })
            ]
            for ((name, kind, f) in series) {
                w.header(name, kind, "Per-actor ${name.replace("cactor_actor_", "").replace("_", " ")}")
                for (c in cells) {
                    w.sample(name, "system=\"${sys}\",path=\"${escapePrometheusLabel(c.path.getPath())}\"", f(c))
                }
            }
        }
        w.sb.toString()
    }

    public func prometheusMetrics(): String {
        prometheusMetrics(true)
    }
}
