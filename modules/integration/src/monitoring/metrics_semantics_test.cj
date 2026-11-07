/*
 * CActor Integration - 直方图与百分位语义（E45，P15）
 *
 *   - 大于全部上界的观测计入 +Inf 桶（此前直接丢弃）；累积计数单调不减，+Inf == _count
 *   - Prometheus 文本：累积的 _bucket{le=...}、le="+Inf"、_sum、_count
 *   - 百分位先排序再取（此前按插入顺序取下标）
 */
package cactor_integration.monitoring

import std.unittest.*
import std.unittest.testmacro.*
import std.convert.*

@Test
public class MetricsSemanticsTest {
    @TestCase
    public func histogramCountsOverflowInInfBucketAndIsCumulative(): Unit {
        let h = Histogram("latency", [10, 50, 100])
        for (v in [1.0, 5.0, 20.0, 60.0, 100.0, 250.0, 900.0]) {
            h.observe(v)
        }
        @Expect(h.getCount(), 7)
        // 单桶：<=10 两个，<=50 一个，<=100 两个，+Inf 两个
        @Expect(h.getBucketCount(0), 2)
        @Expect(h.getBucketCount(1), 1)
        @Expect(h.getBucketCount(2), 2)
        @Expect(h.getBucketCount(3), 2)
        // 累积
        @Expect(h.getCumulativeCount(0), 2)
        @Expect(h.getCumulativeCount(1), 3)
        @Expect(h.getCumulativeCount(2), 5)
        @Expect(h.getCumulativeCount(3), 7)
    }

    @TestCase
    public func prometheusTextIsCumulativeWithInf(): Unit {
        let h = Histogram("latency", [10, 100])
        for (v in [3.0, 30.0, 300.0]) {
            h.observe(v)
        }
        let text = h.toPrometheus("cactor_latency_ms")
        @Expect(text.contains("# TYPE cactor_latency_ms histogram\n"), true)
        @Expect(text.contains("cactor_latency_ms_bucket{le=\"10\"} 1\n"), true)
        @Expect(text.contains("cactor_latency_ms_bucket{le=\"100\"} 2\n"), true)
        @Expect(text.contains("cactor_latency_ms_bucket{le=\"+Inf\"} 3\n"), true)
        @Expect(text.contains("cactor_latency_ms_count 3\n"), true)
        @Expect(text.contains("cactor_latency_ms_sum 333"), true)
        // 每个 bucket 行的值单调不减
        var last: Int64 = -1
        for (line in text.split("\n")) {
            if (line.startsWith("cactor_latency_ms_bucket")) {
                let v = Int64.parse(line.split(" ")[1])
                @Expect(v >= last, true)
                last = v
            }
        }
    }

    @TestCase
    public func bucketsMustBeStrictlyIncreasing(): Unit {
        var threw = false
        try {
            Histogram("bad", [10, 10, 5]) |> ignoreHistogram
        } catch (e: IllegalArgumentException) {
            threw = true
        }
        @Expect(threw, true)
    }

    @TestCase
    public func percentileSortsBeforeRanking(): Unit {
        let values: Array<Float64> = [50.0, 10.0, 40.0, 20.0, 30.0, 90.0, 60.0, 80.0, 70.0, 100.0]
        @Expect(calculatePercentile(values, 0.5), 50.0)
        @Expect(calculatePercentile(values, 0.9), 90.0)
        @Expect(calculatePercentile(values, 0.99), 100.0)
        @Expect(calculatePercentile(values, 0.0), 10.0)
        // 不修改调用方的数组
        @Expect(values[0], 50.0)
    }
}

func ignoreHistogram(_: Histogram): Unit {}
