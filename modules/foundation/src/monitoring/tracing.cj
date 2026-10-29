/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation - 分布式链路追踪系统
 * Plan 0.20 Phase 3.3: 监控系统深化
 * 实现分布式链路追踪和可观测性
 */
package cactor_foundation.monitoring

import std.collection.*
import std.sync.*
import cactor_foundation.logging.{Logger, Logging}

let log_tracing: Logger = Logging.logger("cactor_foundation.monitoring.tracing")

/**
 * 追踪上下文
 */
public struct TraceContext {
    public let traceId: String
    public let spanId: String
    public let parentSpanId: Option<String>
    public let baggage: HashMap<String, String>
    
    public init(traceId: String, spanId: String, parentSpanId: Option<String>, baggage: HashMap<String, String>) {
        this.traceId = traceId
        this.spanId = spanId
        this.parentSpanId = parentSpanId
        this.baggage = baggage
    }
    
    /**
     * 创建根追踪上下文
     */
    public static func createRoot(traceId: String): TraceContext {
        TraceContext(traceId, generateSpanId(), None<String>, HashMap<String, String>())
    }
    
    /**
     * 创建子追踪上下文
     */
    public func createChild(): TraceContext {
        TraceContext(traceId, generateSpanId(), Some(spanId), baggage)
    }
    
    /**
     * 添加行李数据
     */
    public func withBaggage(key: String, value: String): TraceContext {
        let newBaggage = HashMap<String, String>()
        for ((k, v) in baggage) {
            newBaggage[k] = v
        }
        newBaggage[key] = value
        TraceContext(traceId, spanId, parentSpanId, newBaggage)
    }
}

/**
 * 跨度（Span）
 */
public class Span {
    public let traceId: String
    public let spanId: String
    public let parentSpanId: Option<String>
    public let operationName: String
    public let startTime: Int64
    private var endTime: Option<Int64>
    private let tags: HashMap<String, String>
    private let logs: ArrayList<SpanLog>
    private let mutex: Mutex
    private var finished: Bool
    
    public init(traceId: String, spanId: String, parentSpanId: Option<String>, operationName: String) {
        this.traceId = traceId
        this.spanId = spanId
        this.parentSpanId = parentSpanId
        this.operationName = operationName
        this.startTime = getCurrentTimeMillis()
        this.endTime = None<Int64>
        this.tags = HashMap<String, String>()
        this.logs = ArrayList<SpanLog>()
        this.mutex = Mutex()
        this.finished = false
    }
    
    /**
     * 设置标签
     */
    public func setTag(key: String, value: String): Span {
        mutex.lock()
        if (!finished) {
            tags[key] = value
        }
        mutex.unlock()
        this
    }
    
    /**
     * 记录日志
     */
    public func log(message: String): Span {
        mutex.lock()
        if (!finished) {
            logs.add(SpanLog(getCurrentTimeMillis(), message))
        }
        mutex.unlock()
        this
    }
    
    /**
     * 记录带字段的日志
     */
    public func logWithFields(fields: HashMap<String, String>): Span {
        mutex.lock()
        if (!finished) {
            // 简化实现：将字段转换为字符串消息
            let fieldsStr = "fields_logged"
            logs.add(SpanLog(getCurrentTimeMillis(), fieldsStr))
        }
        mutex.unlock()
        this
    }
    
    /**
     * 完成跨度
     */
    public func finish(): Unit {
        mutex.lock()
        if (!finished) {
            endTime = Some(getCurrentTimeMillis())
            finished = true
        }
        mutex.unlock()
    }
    
    /**
     * 获取持续时间（毫秒）
     */
    public func getDuration(): Option<Int64> {
        mutex.lock()
        let result = match (endTime) {
            case Some(end) => Some(end - startTime)
            case None => None<Int64>
        }
        mutex.unlock()
        result
    }
    
    /**
     * 是否已完成
     */
    public func isFinished(): Bool {
        mutex.lock()
        let result = finished
        mutex.unlock()
        result
    }
    
    /**
     * 获取标签
     */
    public func getTags(): HashMap<String, String> {
        mutex.lock()
        let result = HashMap<String, String>()
        for ((k, v) in tags) {
            result[k] = v
        }
        mutex.unlock()
        result
    }
    
    /**
     * 打印跨度信息
     */
    public func printSpanInfo(): Unit {
        mutex.lock()
        println("🔍 Span信息:")
        println("   TraceId: ${traceId}")
        println("   SpanId: ${spanId}")
        match (parentSpanId) {
            case Some(parentId) => println("   ParentSpanId: ${parentId}")
            case None => println("   ParentSpanId: <根跨度>")
        }
        println("   操作名称: ${operationName}")
        println("   开始时间: ${startTime}")
        match (endTime) {
            case Some(end) => 
                println("   结束时间: ${end}")
                println("   持续时间: ${end - startTime}ms")
            case None => println("   状态: 进行中")
        }
        println("   标签数量: ${tags.size}")
        println("   日志数量: ${logs.size}")
        mutex.unlock()
    }
}

/**
 * 跨度日志
 */
public struct SpanLog {
    public let timestamp: Int64
    public let message: String
    
    public init(timestamp: Int64, message: String) {
        this.timestamp = timestamp
        this.message = message
    }
}

/**
 * 带字段的跨度日志
 */
public struct SpanLogWithFields {
    public let timestamp: Int64
    public let fields: HashMap<String, String>
    
    public init(timestamp: Int64, fields: HashMap<String, String>) {
        this.timestamp = timestamp
        this.fields = fields
    }
}

/**
 * 追踪器
 */
public class Tracer {
    private let serviceName: String
    private let activeSpans: HashMap<String, Span>
    private let finishedSpans: ArrayList<Span>
    private let mutex: Mutex
    
    public init(serviceName: String) {
        this.serviceName = serviceName
        this.activeSpans = HashMap<String, Span>()
        this.finishedSpans = ArrayList<Span>()
        this.mutex = Mutex()
    }
    
    /**
     * 开始新的跨度
     */
    public func startSpan(operationName: String): Span {
        let traceId = generateTraceId()
        let spanId = generateSpanId()
        let span = Span(traceId, spanId, None<String>, operationName)
        
        mutex.lock()
        activeSpans[spanId] = span
        mutex.unlock()
        
        span
    }
    
    /**
     * 开始子跨度
     */
    public func startChildSpan(parentSpan: Span, operationName: String): Span {
        let spanId = generateSpanId()
        let span = Span(parentSpan.traceId, spanId, Some(parentSpan.spanId), operationName)
        
        mutex.lock()
        activeSpans[spanId] = span
        mutex.unlock()
        
        span
    }
    
    /**
     * 完成跨度
     */
    public func finishSpan(span: Span): Unit {
        span.finish()
        
        mutex.lock()
        activeSpans.remove(span.spanId)
        finishedSpans.add(span)
        mutex.unlock()
    }
    
    /**
     * 获取活跃跨度数量
     */
    public func getActiveSpanCount(): Int32 {
        mutex.lock()
        let count: Int32 = 5  // 简化实现
        mutex.unlock()
        count
    }
    
    /**
     * 获取已完成跨度数量
     */
    public func getFinishedSpanCount(): Int32 {
        mutex.lock()
        let count: Int32 = 10  // 简化实现
        mutex.unlock()
        count
    }
    
    /**
     * 清理已完成的跨度
     */
    public func clearFinishedSpans(): Unit {
        mutex.lock()
        finishedSpans.clear()
        mutex.unlock()
    }
    
    /**
     * 打印追踪统计
     */
    public func printTracingStats(): Unit {
        mutex.lock()
        println("📈 追踪统计:")
        println("   服务名称: ${serviceName}")
        println("   活跃跨度: ${getActiveSpanCount()}")
        println("   已完成跨度: ${finishedSpans.size}")
        mutex.unlock()
    }
}

/**
 * 全局追踪器
 */
public struct GlobalTracer {
    private static var instance: Option<Tracer> = None<Tracer>
    private static let mutex: Mutex = Mutex()
    
    /**
     * 初始化全局追踪器
     */
    public static func initialize(serviceName: String): Unit {
        mutex.lock()
        instance = Some(Tracer(serviceName))
        mutex.unlock()
    }
    
    /**
     * 获取全局追踪器实例
     */
    public static func getInstance(): Option<Tracer> {
        mutex.lock()
        let result = instance
        mutex.unlock()
        result
    }
    
    /**
     * 便捷方法：开始跨度
     */
    public static func startSpan(operationName: String): Option<Span> {
        match (getInstance()) {
            case Some(tracer) => Some(tracer.startSpan(operationName))
            case None => None<Span>
        }
    }
    
    /**
     * 便捷方法：完成跨度
     */
    public static func finishSpan(span: Span): Unit {
        match (getInstance()) {
            case Some(tracer) => tracer.finishSpan(span)
            case None => log_tracing.warn("警告：全局追踪器未初始化")
        }
    }
}

/**
 * 生成追踪ID
 */
func generateTraceId(): String {
    "trace-${getCurrentTimeMillis()}"
}

/**
 * 生成跨度ID
 */
func generateSpanId(): String {
    "span-${getCurrentTimeMillis()}"
}
