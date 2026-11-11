/*
 * CActor Foundation - 指标直方图与百分位（唯一真值实现）
 *
 * 为什么放在 foundation：
 *   - 这是全仓唯一的指标直方图 / 百分位语义实现。原实现位于 modules/integration/src/monitoring/
 *     （metrics.cj 的 Histogram 与 performance_analyzer.cj 的 calculatePercentile），
 *     而 integration 模块已被整体删除；
 *   - 日志、指标采集、远程统计等上层能力都需要直方图与百分位，语义必须只有一份，
 *     否则会出现"第二套直方图"，导致同一份指标在不同模块被解释成不同数值。
 *   - 因此把真值下沉到 foundation：foundation 不依赖任何仓内模块，只依赖标准库，
 *     上层模块依赖 foundation，依赖方向向下、无环。
 *
 * 语义（P15，Prometheus 直方图一致）：
 *   - 桶上界必须严格递增，否则抛 IllegalArgumentException；
 *   - 观测值落入第一个 `value <= 上界` 的桶；大于全部上界的计入隐含的 +Inf 桶；
 *   - getBucketCount(i) 是单桶计数，getCumulativeCount(i) 是累积计数，+Inf 累积 == _count；
 *   - 求和以"千分之一整数"存储，避免浮点原子累加误差；
 *   - toPrometheus 输出 exposition format 0.0.4 的累积 _bucket{le=...}、le="+Inf"、_sum、_count。
 *   - calculatePercentile 用最近秩法（nearest-rank）：先排序，rank = ceil(p * n)。
 */
package cactor_foundation.monitoring

import std.collection.*
import std.sync.*
import std.sort.*

/**
 * 直方图指标，语义与 Prometheus 直方图一致（P15）：
 * - 桶上界 buckets 必须严格递增；观测值落入第一个 `value <= 上界` 的桶，大于全部上界的计入隐含的 +Inf 桶
 *   （此前直接丢弃，计数与 _count 对不上）；
 * - `getCumulativeCount(i)` / `toPrometheus()` 输出累积计数（le 桶包含所有更小的桶），最后一个 `le="+Inf"` 等于 _count。
 *   `getBucketCount(i)` 仍是单桶计数。
 * 求和以千分之一为单位存成整数，避免浮点原子累加。
 */
public class Histogram {
    private let buckets: Array<Int64>
    // counts[buckets.size] 是 +Inf 桶
    private let counts: Array<AtomicInt64>
    private let totalCount: AtomicInt64
    private let totalSum: AtomicInt64  // 使用整数表示，实际值需要除以1000
    private let name: String
    private let tags: HashMap<String, String>

    public init(name: String, buckets: Array<Int64>, tags: HashMap<String, String>) {
        for (i in 1..buckets.size) {
            if (buckets[i] <= buckets[i - 1]) {
                throw IllegalArgumentException("直方图 ${name} 的桶上界必须严格递增: ${buckets}")
            }
        }
        this.buckets = buckets
        this.counts = Array<AtomicInt64>(buckets.size + 1, { _ => AtomicInt64(0) })
        this.totalCount = AtomicInt64(0)
        this.totalSum = AtomicInt64(0)
        this.name = name
        this.tags = tags
    }

    public init(name: String, buckets: Array<Int64>) {
        this(name, buckets, HashMap<String, String>())
    }

    public func observe(value: Float64): Unit {
        let intValue = Int64(value * 1000.0)
        totalCount.fetchAdd(1)
        totalSum.fetchAdd(intValue)
        var idx = buckets.size
        for (i in 0..buckets.size) {
            if (intValue <= buckets[i] * 1000) {
                idx = i
                break
            }
        }
        counts[idx].fetchAdd(1)
    }

    public func getCount(): Int64 {
        totalCount.load()
    }

    public func getSum(): Float64 {
        Float64(totalSum.load()) / 1000.0
    }

    public func getMean(): Float64 {
        let count = totalCount.load()
        if (count == 0) {
            return 0.0
        }
        Float64(totalSum.load()) / (Float64(count) * 1000.0)
    }

    /** 单桶计数；bucketIndex == 桶数时为 +Inf 桶（大于全部上界的观测） */
    public func getBucketCount(bucketIndex: Int64): Int64 {
        if (bucketIndex >= 0 && bucketIndex < counts.size) {
            counts[bucketIndex].load()
        } else {
            0
        }
    }

    /** 累积计数：上界 <= buckets[bucketIndex] 的观测总数；bucketIndex == 桶数时为 +Inf，等于全部观测数 */
    public func getCumulativeCount(bucketIndex: Int64): Int64 {
        if (bucketIndex < 0) {
            return 0
        }
        let last = if (bucketIndex < counts.size) { bucketIndex } else { counts.size - 1 }
        var total: Int64 = 0
        for (i in 0..=last) {
            total += counts[i].load()
        }
        total
    }

    public func getBuckets(): Array<Int64> {
        buckets
    }

    /**
     * Prometheus 文本格式（exposition format 0.0.4）：累积的 `_bucket{le=...}`、`le="+Inf"`、`_sum`、`_count`。
     * 先读取各桶再推导 +Inf 与 _count，保证同一次输出内部一致（+Inf == _count，桶单调不减）
     */
    public func toPrometheus(metricName: String): String {
        let sb = StringBuilder()
        sb.append("# TYPE ${metricName} histogram\n")
        var cumulative: Int64 = 0
        for (i in 0..buckets.size) {
            cumulative += counts[i].load()
            sb.append("${metricName}_bucket{le=\"${buckets[i]}\"} ${cumulative}\n")
        }
        cumulative += counts[buckets.size].load()
        sb.append("${metricName}_bucket{le=\"+Inf\"} ${cumulative}\n")
        sb.append("${metricName}_sum ${getSum()}\n")
        sb.append("${metricName}_count ${cumulative}\n")
        sb.toString()
    }

    public func getName(): String {
        name
    }

    public func getTags(): HashMap<String, String> {
        tags
    }
}

/**
 * 最近秩法（nearest-rank）百分位：先排序（此前按插入顺序取下标，结果与百分位无关，P15），
 * 取第 ceil(p × n) 个值
 */
public func calculatePercentile(values: Array<Float64>, percentile: Float64): Float64 {
    if (values.size == 0) {
        return 0.0
    }
    let sorted = values.clone()
    sort(sorted)
    let rank = Int64(Float64(values.size) * percentile + 0.999999999)
    let index = if (rank <= 0) { 0 } else if (rank > sorted.size) { sorted.size - 1 } else { rank - 1 }
    sorted[index]
}
