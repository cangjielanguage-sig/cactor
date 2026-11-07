/*
 * CActor Runtime - 默认调度器诊断（E39）
 *
 * 饥饿检测：某个工作线程卡在同一次 Actor 运行里超过 CACTOR_STARVATION_WARN_MS（默认 1000，0 关闭）时，
 * 运行时告警一次并点名占住线程的 Actor。这里把计数与最近一次点名公开给监控与测试
 * （Prometheus 指标 cactor_dispatcher_starvation_warnings_total 是同一个计数）。
 */
package cactor_runtime

import cactor_runtime.guardian.{ActorCellDispatcher}

public struct DispatcherDiagnostics {
    /** 饥饿告警次数（进程内累计） */
    public static func starvationWarnings(): Int64 {
        ActorCellDispatcher.starvationWarningCount()
    }

    /** 最近一次饥饿告警点名的 Actor 路径 */
    public static func lastStarvationPath(): Option<String> {
        ActorCellDispatcher.lastStarvationPath()
    }
}
