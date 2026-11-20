/*
 * CActor Foundation - Span 导出（差距 4.10 的导出部分：导出器接口 + 批量 + 失败隔离 + OTLP/JSON）
 *
 * 设计约束：
 * - foundation 不依赖 stdx/HTTP：这里只提供**导出器接口**与 OTLP JSON 编码，
 *   由应用用自己的 HTTP 客户端 POST 到 collector（示例 ex35 用 Lou 侧的内存导出 + curl 展示）；
 * - 导出失败绝不能影响业务：`BatchSpanExporter` 吞掉下游异常并计数；
 * - Tracer 默认不导出：必须显式 `attachExporter`，未安装时行为与之前完全一致。
 */
package cactor_foundation.monitoring

import std.collection.*
import std.sync.*

/** 已结束 span 的不可变快照（Span 本身可变，导出快照保证下游看到稳定数据） */
public struct SpanRecord <: ToString {
    public let service: String
    public let traceId: String
    public let spanId: String
    public let parentSpanId: Option<String>
    public let name: String
    public let startTimeMillis: Int64
    public let durationMillis: Int64
    public let tags: HashMap<String, String>

    public init(service: String, traceId: String, spanId: String, parentSpanId: Option<String>,
        name: String, startTimeMillis: Int64, durationMillis: Int64, tags: HashMap<String, String>) {
        this.service = service
        this.traceId = traceId
        this.spanId = spanId
        this.parentSpanId = parentSpanId
        this.name = name
        this.startTimeMillis = startTimeMillis
        this.durationMillis = durationMillis
        this.tags = tags
    }

    public func toString(): String {
        "${service} ${traceId}/${spanId} ${name} ${durationMillis}ms"
    }
}

/** 下游导出器（OTLP、日志、内存……）。实现抛出的异常由 BatchSpanExporter 兜底 */
public interface SpanExporter {
    func export(spans: Array<SpanRecord>): Unit
}

/** 把已结束的 Span 拍成快照 */
public func spanRecordOf(span: Span, service: String): SpanRecord {
    let duration = match (span.getDuration()) {
        case Some(d) => d
        case None => 0
    }
    SpanRecord(service, span.traceId, span.spanId, span.parentSpanId, span.operationName,
        span.startTime, duration, span.getTags())
}

/**
 * 批量导出：业务线程只入队；缓冲达到 batchSize 或显式 `flush()` 时一次性交给下游。
 * 下游异常在这里被捕获并计为失败，不会沿调用栈影响业务。
 */
public class BatchSpanExporter {
    private let downstream: SpanExporter
    private let batchSize: Int64
    private let mutex = Mutex()
    private let buffer = ArrayList<SpanRecord>()
    private let exported = AtomicInt64(0)
    private let failures = AtomicInt64(0)

    /** @param batchSize 达到多少条自动导出（缓冲在每次导出后清空，不会无限增长） */
    public init(downstream: SpanExporter, batchSize!: Int64 = 128) {
        if (batchSize <= 0) {
            throw IllegalArgumentException("batchSize 必须为正")
        }
        this.downstream = downstream
        this.batchSize = batchSize
    }

    /** 入队一条；缓冲满则同步导出一次（失败只计数） */
    public func enqueue(record: SpanRecord): Unit {
        let batch = synchronized(mutex) {
            buffer.add(record)
            if (buffer.size >= batchSize) {
                takeBuffer()
            } else {
                Array<SpanRecord>()
            }
        }
        if (!batch.isEmpty()) {
            deliver(batch)
        }
    }

    /** 导出当前缓冲；下游异常被吞掉并计为失败，本方法不抛异常 */
    public func flush(): Unit {
        let batch = synchronized(mutex) {
            takeBuffer()
        }
        if (!batch.isEmpty()) {
            deliver(batch)
        }
    }

    private func takeBuffer(): Array<SpanRecord> {
        let out = buffer.toArray()
        buffer.clear()
        out
    }

    private func deliver(batch: Array<SpanRecord>): Unit {
        try {
            downstream.export(batch)
            exported.fetchAdd(Int64(batch.size))
        } catch (_: Exception) {
            failures.fetchAdd(1)
        }
    }

    public func exportedCount(): Int64 {
        exported.load()
    }

    /** 失败次数（以批次计） */
    public func failureCount(): Int64 {
        failures.load()
    }

}

/** 内存导出器：测试与示例用；线程安全 */
public class InMemorySpanExporter <: SpanExporter {
    private let lock = Mutex()
    private let items = ArrayList<SpanRecord>()

    public init() {}

    public func export(spans: Array<SpanRecord>): Unit {
        synchronized(lock) {
            for (s in spans) {
                items.add(s)
            }
        }
    }

    public func snapshot(): Array<SpanRecord> {
        synchronized(lock) {
            items.toArray()
        }
    }

    public func clear(): Unit {
        synchronized(lock) {
            items.clear()
        }
    }
}

/** 总是抛异常的导出器（故障注入：验证业务不受影响） */
public class ThrowingSpanExporter <: SpanExporter {
    public let attempts = AtomicInt64(0)

    public init() {}

    public func export(spans: Array<SpanRecord>): Unit {
        attempts.fetchAdd(1)
        throw IllegalStateException("collector unavailable")
    }
}

/**
 * OTLP/JSON（v1 `resourceSpans`）编码。时间用纳秒字符串（OTLP JSON 约定 int64 以字符串表示）。
 * 应用直接把返回值作为 `POST /v1/traces` 的 body（Content-Type: application/json）。
 */
public func toOtlpJson(serviceName: String, spans: Array<SpanRecord>): String {
    let sb = StringBuilder()
    sb.append("{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":")
    appendJsonString(sb, serviceName)
    sb.append("}}]},\"scopeSpans\":[{\"scope\":{\"name\":\"cactor\"},\"spans\":[")
    var first = true
    for (s in spans) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("{\"traceId\":")
        appendJsonString(sb, s.traceId)
        sb.append(",\"spanId\":")
        appendJsonString(sb, s.spanId)
        match (s.parentSpanId) {
            case Some(p) =>
                sb.append(",\"parentSpanId\":")
                appendJsonString(sb, p)
            case None => ()
        }
        sb.append(",\"name\":")
        appendJsonString(sb, s.name)
        sb.append(",\"kind\":1,\"startTimeUnixNano\":\"${s.startTimeMillis * 1000000}\"")
        sb.append(",\"endTimeUnixNano\":\"${(s.startTimeMillis + s.durationMillis) * 1000000}\"")
        sb.append(",\"attributes\":[")
        var firstAttr = true
        for ((k, v) in s.tags) {
            if (!firstAttr) {
                sb.append(",")
            }
            firstAttr = false
            sb.append("{\"key\":")
            appendJsonString(sb, k)
            sb.append(",\"value\":{\"stringValue\":")
            appendJsonString(sb, v)
            sb.append("}}")
        }
        sb.append("]}")
    }
    sb.append("]}]}]}")
    sb.toString()
}

func appendJsonString(sb: StringBuilder, value: String): Unit {
    sb.append("\"")
    for (r in value.runes()) {
        match (r) {
            case '"' => sb.append("\\\"")
            case '\\' => sb.append("\\\\")
            case '\n' => sb.append("\\n")
            case '\r' => sb.append("\\r")
            case '\t' => sb.append("\\t")
            case _ => sb.append(r)
        }
    }
    sb.append("\"")
}
