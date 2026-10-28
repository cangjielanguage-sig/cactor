/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Streams - 流处理消息协议
 * Plan20阶段2：流处理系统 - 消息协议定义
 *
 * 这是CActor Streams的核心消息协议，定义了：
 * 1. 流数据传输消息
 * 2. 背压控制消息
 * 3. 流生命周期管理消息
 * 4. 错误处理和监控消息
 */
package cactor_streams.core

import std.time.*
import std.sync.*
import cactor_core.message.{Message}

/**
 * 流数据消息 - 携带实际的流数据
 */
public class StreamDataMessage <: Message {
    private let data: StreamElement
    private let sequenceNumber: Int64
    private let sourceId: String
    
    public init(data: StreamElement, sequenceNumber: Int64, sourceId: String) {
        this.data = data
        this.sequenceNumber = sequenceNumber
        this.sourceId = sourceId
    }
    
    public func getData(): StreamElement { data }
    public func getSequenceNumber(): Int64 { sequenceNumber }
    public func getSourceId(): String { sourceId }
    public func messageType(): String { "StreamDataMessage" }
    
    public func toString(): String {
        "StreamDataMessage(seq=${sequenceNumber}, source=${sourceId}, data=${data.getElementId()})"
    }
}

/**
 * 背压控制消息 - 用于流量控制
 */
public class BackpressureMessage <: Message {
    private let demand: Demand
    private let targetId: String
    private let sourceId: String
    
    public init(demand: Demand, targetId: String, sourceId: String) {
        this.demand = demand
        this.targetId = targetId
        this.sourceId = sourceId
    }
    
    public func getDemand(): Demand { demand }
    public func getTargetId(): String { targetId }
    public func getSourceId(): String { sourceId }
    public func messageType(): String { "BackpressureMessage" }
    
    public func toString(): String {
        "BackpressureMessage(demand=${demand.count}, target=${targetId}, source=${sourceId})"
    }
}

/**
 * 流完成消息 - 表示流的正常结束
 */
public class StreamCompleteMessage <: Message {
    private let sourceId: String
    private let totalElements: Int64
    
    public init(sourceId: String, totalElements: Int64) {
        this.sourceId = sourceId
        this.totalElements = totalElements
    }
    
    public func getSourceId(): String { sourceId }
    public func getTotalElements(): Int64 { totalElements }
    public func messageType(): String { "StreamCompleteMessage" }
    
    public func toString(): String {
        "StreamCompleteMessage(source=${sourceId}, total=${totalElements})"
    }
}

/**
 * 流错误消息 - 表示流处理中的错误
 */
public class StreamErrorMessage <: Message {
    private let error: Exception
    private let sourceId: String
    private let failedElement: Option<StreamElement>
    
    public init(error: Exception, sourceId: String, failedElement: Option<StreamElement>) {
        this.error = error
        this.sourceId = sourceId
        this.failedElement = failedElement
    }
    
    public func getError(): Exception { error }
    public func getSourceId(): String { sourceId }
    public func getFailedElement(): Option<StreamElement> { failedElement }
    public func messageType(): String { "StreamErrorMessage" }
    
    public func toString(): String {
        "StreamErrorMessage(source=${sourceId}, error=${error.message})"
    }
}

/**
 * 流初始化消息 - 启动流处理
 */
public class StreamInitMessage <: Message {
    private let streamId: String
    private let settings: MaterializerSettings
    
    public init(streamId: String, settings: MaterializerSettings) {
        this.streamId = streamId
        this.settings = settings
    }
    
    public func getStreamId(): String { streamId }
    public func getSettings(): MaterializerSettings { settings }
    public func messageType(): String { "StreamInitMessage" }
    
    public func toString(): String {
        "StreamInitMessage(streamId=${streamId})"
    }
}

/**
 * 流取消消息 - 取消流处理
 */
public class StreamCancelMessage <: Message {
    private let streamId: String
    private let reason: String
    
    public init(streamId: String, reason: String) {
        this.streamId = streamId
        this.reason = reason
    }
    
    public func getStreamId(): String { streamId }
    public func getReason(): String { reason }
    public func messageType(): String { "StreamCancelMessage" }
    
    public func toString(): String {
        "StreamCancelMessage(streamId=${streamId}, reason=${reason})"
    }
}

/**
 * 流状态查询消息
 */
public class StreamStatusMessage <: Message {
    private let streamId: String
    
    public init(streamId: String) {
        this.streamId = streamId
    }
    
    public func getStreamId(): String { streamId }
    public func messageType(): String { "StreamStatusMessage" }
    
    public func toString(): String {
        "StreamStatusMessage(streamId=${streamId})"
    }
}

/**
 * 流状态响应消息
 */
public class StreamStatusResponse <: Message {
    private let streamId: String
    private let isRunning: Bool
    private let stats: StreamStats
    
    public init(streamId: String, isRunning: Bool, stats: StreamStats) {
        this.streamId = streamId
        this.isRunning = isRunning
        this.stats = stats
    }
    
    public func getStreamId(): String { streamId }
    public func getIsRunning(): Bool { isRunning }
    public func getStats(): StreamStats { stats }
    public func messageType(): String { "StreamStatusResponse" }
    
    public func toString(): String {
        "StreamStatusResponse(streamId=${streamId}, running=${isRunning})"
    }
}

/**
 * 批量数据消息 - 用于批量传输优化
 */
public class BatchDataMessage <: Message {
    private let elements: Array<StreamElement>
    private let batchId: String
    private let sourceId: String
    
    public init(elements: Array<StreamElement>, batchId: String, sourceId: String) {
        this.elements = elements
        this.batchId = batchId
        this.sourceId = sourceId
    }
    
    public func getElements(): Array<StreamElement> { elements }
    public func getBatchId(): String { batchId }
    public func getSourceId(): String { sourceId }
    public func getBatchSize(): Int64 { elements.size }
    public func messageType(): String { "BatchDataMessage" }
    
    public func toString(): String {
        "BatchDataMessage(batchId=${batchId}, size=${elements.size}, source=${sourceId})"
    }
}

/**
 * 流配置消息 - 动态配置流参数
 */
public class StreamConfigMessage <: Message {
    private let streamId: String
    private let bufferSize: Int32
    private let overflowStrategy: OverflowStrategy
    
    public init(streamId: String, bufferSize: Int32, overflowStrategy: OverflowStrategy) {
        this.streamId = streamId
        this.bufferSize = bufferSize
        this.overflowStrategy = overflowStrategy
    }
    
    public func getStreamId(): String { streamId }
    public func getBufferSize(): Int32 { bufferSize }
    public func getOverflowStrategy(): OverflowStrategy { overflowStrategy }
    public func messageType(): String { "StreamConfigMessage" }
    
    public func toString(): String {
        "StreamConfigMessage(streamId=${streamId}, bufferSize=${bufferSize})"
    }
}

/**
 * 简单流元素实现
 */
public class SimpleStreamElement <: StreamElement {
    private let elementId: String
    private let timestamp: DateTime
    private let data: String
    
    public init(elementId: String, data: String) {
        this.elementId = elementId
        this.timestamp = DateTime.now()
        this.data = data
    }
    
    public func getElementId(): String { elementId }
    public func getTimestamp(): DateTime { timestamp }
    public func serialize(): String { data }
    
    public func getData(): String { data }
    
    public func toString(): String {
        "SimpleStreamElement(id=${elementId}, data=${data})"
    }
}

/**
 * 数值流元素实现
 */
public class NumberStreamElement <: StreamElement {
    private let elementId: String
    private let timestamp: DateTime
    private let value: Int64
    
    public init(elementId: String, value: Int64) {
        this.elementId = elementId
        this.timestamp = DateTime.now()
        this.value = value
    }
    
    public func getElementId(): String { elementId }
    public func getTimestamp(): DateTime { timestamp }
    public func serialize(): String { value.toString() }
    
    public func getValue(): Int64 { value }
    
    public func toString(): String {
        "NumberStreamElement(id=${elementId}, value=${value})"
    }
}

/**
 * 流消息工厂
 */
public class StreamMessageFactory {
    private var sequenceCounter: Int64
    private let mutex: Mutex
    
    public init() {
        this.sequenceCounter = 0
        this.mutex = Mutex()
    }
    
    public func createDataMessage(data: StreamElement, sourceId: String): StreamDataMessage {
        mutex.lock()
        sequenceCounter += 1
        let seq = sequenceCounter
        mutex.unlock()
        
        StreamDataMessage(data, seq, sourceId)
    }
    
    public func createBackpressureMessage(demand: Demand, targetId: String, sourceId: String): BackpressureMessage {
        BackpressureMessage(demand, targetId, sourceId)
    }
    
    public func createCompleteMessage(sourceId: String, totalElements: Int64): StreamCompleteMessage {
        StreamCompleteMessage(sourceId, totalElements)
    }
    
    public func createErrorMessage(error: Exception, sourceId: String, failedElement: Option<StreamElement>): StreamErrorMessage {
        StreamErrorMessage(error, sourceId, failedElement)
    }
    
    public func createBatchMessage(elements: Array<StreamElement>, sourceId: String): BatchDataMessage {
        mutex.lock()
        sequenceCounter += 1
        let batchId = "batch-${sequenceCounter}"
        mutex.unlock()
        
        BatchDataMessage(elements, batchId, sourceId)
    }
    
    public func createSimpleElement(data: String): SimpleStreamElement {
        mutex.lock()
        sequenceCounter += 1
        let elementId = "element-${sequenceCounter}"
        mutex.unlock()
        
        SimpleStreamElement(elementId, data)
    }
    
    public func createNumberElement(value: Int64): NumberStreamElement {
        mutex.lock()
        sequenceCounter += 1
        let elementId = "number-${sequenceCounter}"
        mutex.unlock()
        
        NumberStreamElement(elementId, value)
    }
    
    public func reset(): Unit {
        mutex.lock()
        sequenceCounter = 0
        mutex.unlock()
    }
}
