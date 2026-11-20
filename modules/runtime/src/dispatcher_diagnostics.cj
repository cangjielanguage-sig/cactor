/*
 * CActor Runtime - 默认调度器诊断（E39）
 *
 * 饥饿检测：某个工作线程卡在同一次 Actor 运行里超过 CACTOR_STARVATION_WARN_MS（默认 1000，0 关闭）时，
 * 运行时告警一次并点名占住线程的 Actor。这里把计数与最近一次点名公开给监控与测试
 * （Prometheus 指标 cactor_dispatcher_starvation_warnings_total 是同一个计数）。
 */
package cactor_runtime

import cactor_runtime.guardian.{ActorCellDispatcher, RunDurationMetrics}

public struct DispatcherDiagnostics {
    /** 饥饿告警次数（进程内累计） */
    public static func starvationWarnings(): Int64 {
        ActorCellDispatcher.starvationWarningCount()
    }

    /** 最近一次饥饿告警点名的 Actor 路径 */
    public static func lastStarvationPath(): Option<String> {
        ActorCellDispatcher.lastStarvationPath()
    }

    /** 默认（亲和）调度器的工作线程数 */
    public static func affinityWorkerCount(): Int64 {
        ActorCellDispatcher.getWorkerCount()
    }

    /** ThreadPerActor 调度器当前存活的专属线程数 */
    public static func threadPerActorThreads(): Int64 {
        ActorCellDispatcher.getThreadPerActorThreadCount()
    }

    /** ThreadPerActor 调度器累计创建 / 回收的线程数 */
    public static func threadPerActorCreated(): Int64 {
        ActorCellDispatcher.getThreadPerActorCreatedCount()
    }

    public static func threadPerActorReaped(): Int64 {
        ActorCellDispatcher.getThreadPerActorReapedCount()
    }

    /** 调整 ThreadPerActor 的空闲回收阈值（毫秒，0 = 不回收）；测试与调优用 */
    public static func setThreadPerActorIdleMillis(ms: Int64): Unit {
        ActorCellDispatcher.setThreadPerActorIdleMillis(ms)
    }

    /** 指定调度模式的累计 Actor 运行次数（A1b 直方图） */
    public static func runCount(mode: String): Int64 {
        RunDurationMetrics.runCount(mode)
    }

    /** 直方图的 Prometheus 文本（A1b） */
    public static func runDurationPrometheus(): String {
        RunDurationMetrics.prometheusText()
    }
}
