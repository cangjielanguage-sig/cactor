/*
 * CActor Tests - Span 导出（差距 4.10：导出器接口 / 批量 / 失败隔离 / OTLP JSON）
 *
 * 覆盖：
 *   - Tracer 显式安装导出器后，完成的 span 以快照形式批量导出（父 span 与标签保留）；
 *   - 下游导出器抛异常：finishSpan 不抛、失败计数增加、本地缓冲不受影响；
 *   - 未安装导出器时行为与之前一致；
 *   - OTLP/JSON 包含 traceId/spanId/parentSpanId/name、service.name 与转义后的属性。
 */
package cactor_tests.unit.foundation

import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.monitoring.*

@Test
public class SpanExporterTest {
    @TestCase
    public func tracerExportsFinishedSpansInBatches(): Unit {
        let sink = InMemorySpanExporter()
        let batch = BatchSpanExporter(sink, batchSize: 2)
        let tracer = Tracer("orders")
        tracer.attachExporter(batch)

        let root = tracer.startSpan("handle-order")
        root.setTag("order.id", "o-1")
        let child = tracer.startChildSpan(root, "charge")
        child.setTag("tenant", "acme")
        tracer.finishSpan(child)
        tracer.finishSpan(root)
        // batchSize=2：第二个 finish 后已自动导出
        batch.flush()

        let records = sink.snapshot()
        @Expect(records.size, 2)
        @Expect(batch.exportedCount(), 2)
        @Expect(batch.failureCount(), 0)

        // 找到 root 与 child
        var rootRec = records[0]
        var childRec = records[1]
        for (r in records) {
            if (r.spanId == root.spanId) { rootRec = r }
            if (r.spanId == child.spanId) { childRec = r }
        }
        @Expect(rootRec.traceId, childRec.traceId)
        @Expect(childRec.parentSpanId, Some(root.spanId))
        @Expect(rootRec.parentSpanId.isNone(), true)
        @Expect(rootRec.service, "orders")
        @Expect(rootRec.tags.get("order.id"), Some("o-1"))
        @Expect(childRec.tags.get("tenant"), Some("acme"))
        @Expect(tracer.getFinishedSpanCount(), 2)
    }

    @TestCase
    public func failingExporterDoesNotAffectBusiness(): Unit {
        let bad = ThrowingSpanExporter()
        let batch = BatchSpanExporter(bad, batchSize: 1)
        let tracer = Tracer("payments")
        tracer.attachExporter(batch)

        // 业务路径只调用 finishSpan：下游抛异常也必须正常返回
        let span = tracer.startSpan("pay")
        tracer.finishSpan(span)
        @Expect(bad.attempts.load(), 1)
        @Expect(batch.failureCount(), 1)
        @Expect(batch.exportedCount(), 0)
        // 本地环形缓冲仍然完整
        @Expect(tracer.getFinishedSpanCount(), 1)
        @Expect(tracer.getActiveSpanCount(), 0)

        // 后续 span 继续可用
        let again = tracer.startSpan("pay-again")
        tracer.finishSpan(again)
        @Expect(batch.failureCount(), 2)
        @Expect(tracer.getFinishedSpanCount(), 2)
    }

    @TestCase
    public func noExporterKeepsPreviousBehaviour(): Unit {
        let tracer = Tracer("plain")
        let span = tracer.startSpan("work")
        tracer.finishSpan(span)
        @Expect(tracer.getFinishedSpanCount(), 1)
        @Expect(tracer.getDroppedSpanCount(), 0)
    }

    @TestCase
    public func otlpJsonEncodesParentAndAttributes(): Unit {
        let tags = HashMap<String, String>()
        tags["http.route"] = "/orders"
        tags["quote\"key"] = "a\nb"
        let parent = SpanRecord("gateway", "trace-1", "span-parent", None<String>,
            "gateway", 1000, 5, HashMap<String, String>())
        let child = SpanRecord("gateway", "trace-1", "span-child", Some<String>("span-parent"),
            "orders", 1010, 7, tags)
        let json = toOtlpJson("gateway", [parent, child])
        @Expect(json.contains("\"service.name\""), true)
        @Expect(json.contains("{\"stringValue\":\"gateway\"}"), true)
        @Expect(json.contains("\"traceId\":\"trace-1\""), true)
        @Expect(json.contains("\"spanId\":\"span-child\""), true)
        @Expect(json.contains("\"parentSpanId\":\"span-parent\""), true)
        @Expect(json.contains("\"startTimeUnixNano\":\"1000000000\""), true)
        @Expect(json.contains("\"endTimeUnixNano\":\"1017000000\""), true)
        @Expect(json.contains("\"http.route\""), true)
        // 属性里的引号与换行必须被转义
        @Expect(json.contains("quote\\\"key") || json.contains("\\\"quote"), true)
        @Expect(json.contains("a\\nb") || json.contains("a\\\\nb"), true)
    }
}
