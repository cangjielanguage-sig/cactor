/*
 * CActor Runtime - Actor 单次运行时长直方图（A1b）
 *
 * 目的：让"某个 Actor 在一次调度里卡了很久"变成可观测的信号，而不是只表现为其他 Actor 变慢。
 * 按调度模式（default / blocking / thread-per-actor）分别统计，用来区分：
 * - default 上的长运行：可能是在 receive 里阻塞（应改用 blocking / thread-per-actor）；
 * - blocking / thread-per-actor 上的长运行：符合预期（阻塞被隔离在自己的线程里）。
 *
 * 计数点是 Cell 的一次 run（一批消息），不是每条消息：每次只多两次 MonoTime.now 与几个原子自增，
 * 对 14M msg/s 的热路径影响可忽略。
 *
 * 导出口径与 Prometheus 直方图一致：
 *   cactor_actor_run_duration_seconds_bucket{mode="...",le="..."}  （累计）
 *   cactor_actor_run_duration_seconds_sum{mode="..."}
 *   cactor_actor_run_duration_seconds_count{mode="..."}
 */
protected package cactor_runtime.guardian

import std.sync.*
import std.collection.*

/**
 * 单次运行时长直方图（秒）。桶边界固定，避免动态桶带来的配置漂移；
 * 覆盖"正常处理（百微秒）→ 慢消息（数十毫秒）→ 阻塞（秒级）"。
 */
public struct RunDurationMetrics {
    static let modeNames: Array<String> = ["default", "blocking", "thread-per-actor"]
    static let bucketBoundsNanos: Array<Int64> = [
        100000,        // 0.1ms
        500000,        // 0.5ms
        1000000,       // 1ms
        5000000,       // 5ms
        10000000,      // 10ms
        50000000,      // 50ms
        100000000,     // 100ms
        500000000,     // 500ms
        1000000000,    // 1s
        5000000000     // 5s
    ]
    static let bucketLabels: Array<String> = [
        "0.0001", "0.0005", "0.001", "0.005", "0.01",
        "0.05", "0.1", "0.5", "1", "5"
    ]
    static let data: Array<RunDurationBuckets> = Array<RunDurationBuckets>(3, { _ => RunDurationBuckets() })

    /** 记录一次运行（nanos 为该次 run 的墙钟时长） */
    public static func record(mode: String, nanos: Int64): Unit {
        let idx = modeIndex(mode)
        let b = data[idx]
        b.count.fetchAdd(1)
        b.sumNanos.fetchAdd(nanos)
        var bi = 0
        while (bi < bucketBoundsNanos.size && nanos > bucketBoundsNanos[bi]) {
            bi++
        }
        b.buckets[bi].fetchAdd(1)
    }

    /** 指定模式的累计运行次数（测试与监控用） */
    public static func runCount(mode: String): Int64 {
        data[modeIndex(mode)].count.load()
    }

    /** 指定模式的累计运行时长（纳秒） */
    public static func totalNanos(mode: String): Int64 {
        data[modeIndex(mode)].sumNanos.load()
    }

    /** 渲染为 Prometheus 文本（histogram 族）；未产生过运行的 mode 不输出，避免空序列 */
    public static func prometheusText(): String {
        let sb = StringBuilder()
        var any = false
        for (i in 0..modeNames.size) {
            let b = data[i]
            let count = b.count.load()
            if (count == 0) {
                continue
            }
            if (!any) {
                sb.append("# HELP cactor_actor_run_duration_seconds Actor run duration by dispatcher mode\n")
                sb.append("# TYPE cactor_actor_run_duration_seconds histogram\n")
                any = true
            }
            let mode = modeNames[i]
            var cumulative: Int64 = 0
            for (bi in 0..bucketBoundsNanos.size) {
                cumulative += b.buckets[bi].load()
                sb.append("cactor_actor_run_duration_seconds_bucket{mode=\"${mode}\",le=\"${bucketLabels[bi]}\"} ${cumulative}\n")
            }
            sb.append("cactor_actor_run_duration_seconds_bucket{mode=\"${mode}\",le=\"+Inf\"} ${count}\n")
            sb.append("cactor_actor_run_duration_seconds_sum{mode=\"${mode}\"} ${b.sumNanos.load()}\n")
            sb.append("cactor_actor_run_duration_seconds_count{mode=\"${mode}\"} ${count}\n")
        }
        sb.toString()
    }

    static func modeIndex(mode: String): Int64 {
        match (mode) {
            case "blocking" => 1
            case "thread-per-actor" => 2
            case _ => 0
        }
    }
}

/** 一个调度模式的桶：最后一个槽是 +Inf */
class RunDurationBuckets {
    let buckets: Array<AtomicInt64> = Array<AtomicInt64>(RunDurationMetrics.bucketBoundsNanos.size + 1, { _ => AtomicInt64(0) })
    let sumNanos: AtomicInt64 = AtomicInt64(0)
    let count: AtomicInt64 = AtomicInt64(0)
}
