/*
 * CActor Runtime.Guardian - 邮箱积压告警（C5）
 *
 * C2-a 已经暴露每个 Actor 的 `mailboxBacklogSeconds`（最老一条待处理消息的等待时长）。
 * 这里补 C5 的另一半：**超阈值时告警并计数，但绝不丢弃消息**（自动丢弃会掩盖问题）。
 *
 * 检测点放在 Cell 的批边界（见 SimpleUserActorRef.warnIfBacklogged）：
 *   - 先看 `pending() > 0`（原子读，热路径几乎零成本），只有确实有积压才去取时间戳（加邮箱锁）；
 *   - 每个 Actor 在一个阈值窗口内最多告警一次（时间戳懒分配在 CellExtras，叶 Actor 不付代价）；
 *   - 全局计数 `cactor_actor_backlog_warnings_total` 供 Prometheus 告警。
 *
 * 阈值：`CACTOR_MAILBOX_BACKLOG_WARN_SECONDS`（默认 30，0 关闭）。
 */
protected package cactor_runtime.guardian

import std.sync.*

public class BacklogMonitor {
    // -1 = 尚未解析（按环境变量/默认值初始化一次）
    private static let threshold = AtomicInt64(-1)
    private static let warnings = AtomicInt64(0)

    /** 告警阈值（毫秒；0 = 关闭） */
    public static func thresholdMillis(): Int64 {
        let v = threshold.load()
        if (v >= 0) {
            return v
        }
        let resolved = envInt("CACTOR_MAILBOX_BACKLOG_WARN_SECONDS", 30, 0) * 1000
        threshold.store(resolved)
        resolved
    }

    public static func enabled(): Bool {
        thresholdMillis() > 0
    }

    /** 记录一次超阈值告警（由 Cell 在批边界调用） */
    public static func recordWarning(): Unit {
        warnings.fetchAdd(1)
    }

    public static func warningCount(): Int64 {
        warnings.load()
    }

    /**
     * 覆盖阈值（毫秒，0 关闭）。生产优先用环境变量；此入口供嵌入方与测试使用。
     */
    public static func configure(thresholdMs: Int64): Unit {
        threshold.store(if (thresholdMs < 0) { 0 } else { thresholdMs })
    }

    /** 恢复为环境变量/默认值（测试收尾用） */
    public static func reset(): Unit {
        threshold.store(-1)
    }
}
