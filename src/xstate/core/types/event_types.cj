/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 基于仓颉语言特性的事件类型定义
 * 充分利用仓颉0.60.5的强类型系统、枚举、模式匹配等特性
 */
package cactor.xstate.core.types

import std.collection.*
import std.time.*
import cactor.core.message.{Message}

/**
 * 状态机事件基础接口
 * 继承CActor的Message接口，实现与Actor系统的无缝集成
 */
public interface StateMachineEvent <: Message {
    /**
     * 获取事件类型标识
     */
    func getEventType(): String
    
    /**
     * 获取事件数据
     */
    func getEventData(): HashMap<String, Any> {
        HashMap<String, Any>()
    }
    
    /**
     * 获取事件来源
     */
    func getSource(): Option<String> {
        None
    }
    
    /**
     * 检查事件是否匹配指定类型
     */
    func matchesType(eventType: String): Bool {
        getEventType() == eventType
    }
    
    /**
     * 检查事件是否匹配模式（支持通配符）
     */
    func matchesPattern(pattern: String): Bool {
        let eventType = getEventType()
        
        // 简单的通配符匹配
        if (pattern.endsWith("*")) {
            let prefix = pattern.substring(0, pattern.size - 1)
            return eventType.startsWith(prefix)
        }
        
        return eventType == pattern
    }
}

/**
 * 基础状态机事件实现
 * 提供默认的事件实现，符合CActor的Message接口规范
 */
public struct BaseStateMachineEvent <: StateMachineEvent {
    private let eventType: String
    private let eventData: HashMap<String, Any>
    private let source: Option<String>
    private let ts: Int64
    private let msgId: String
    
    public init(
        eventType: String,
        eventData: HashMap<String, Any> = HashMap<String, Any>(),
        source: Option<String> = None
    ) {
        this.eventType = eventType
        this.eventData = eventData
        this.source = source
        this.ts = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.msgId = "event-${ts}-${eventType}"
    }
    
    // StateMachineEvent接口实现
    public func getEventType(): String { eventType }
    public func getEventData(): HashMap<String, Any> { eventData }
    public func getSource(): Option<String> { source }
    
    // Message接口实现
    public func messageType(): String { "StateMachineEvent" }
    public func priority(): Int32 { 0 }
    public func messageId(): String { msgId }
    public func timestamp(): Int64 { ts }
}

/**
 * 内部状态机事件枚举
 * 利用仓颉枚举的强类型特性定义系统内部事件
 */
public enum InternalEvent <: StateMachineEvent {
    | Start                    // 启动事件
    | Stop                     // 停止事件
    | Reset                    // 重置事件
    | Pause                    // 暂停事件
    | Resume                   // 恢复事件
    | Snapshot                 // 快照事件
    | Restore(data: String)    // 恢复事件（带数据）
    
    public func getEventType(): String {
        match (this) {
            case Start => "internal.start"
            case Stop => "internal.stop"
            case Reset => "internal.reset"
            case Pause => "internal.pause"
            case Resume => "internal.resume"
            case Snapshot => "internal.snapshot"
            case Restore(_) => "internal.restore"
        }
    }
    
    public func getEventData(): HashMap<String, Any> {
        let data = HashMap<String, Any>()
        match (this) {
            case Restore(restoreData) =>
                data.put("data", restoreData)
            case _ => ()
        }
        return data
    }
    
    public func getSource(): Option<String> {
        Some("internal")
    }
    
    // Message接口实现
    public func messageType(): String { "InternalEvent" }
    public func priority(): Int32 { 10 }  // 高优先级
    public func messageId(): String { "internal-${timestamp()}-${getEventType()}" }
    public func timestamp(): Int64 { DateTime.now().toUnixTimeStamp().toNanoseconds() }
}

/**
 * 定时器事件
 * 支持延迟执行和定时触发的事件
 */
public struct TimerEvent <: StateMachineEvent {
    private let timerId: String
    private let delay: Duration
    private let ts: Int64
    private let msgId: String
    
    public init(timerId: String, delay: Duration) {
        this.timerId = timerId
        this.delay = delay
        this.ts = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.msgId = "timer-${ts}-${timerId}"
    }
    
    public func getTimerId(): String { timerId }
    public func getDelay(): Duration { delay }
    
    public func getEventType(): String { "timer.${timerId}" }
    
    public func getEventData(): HashMap<String, Any> {
        let data = HashMap<String, Any>()
        data.put("timerId", timerId)
        data.put("delay", delay.toNanoseconds())
        return data
    }
    
    public func getSource(): Option<String> { Some("timer") }
    
    // Message接口实现
    public func messageType(): String { "TimerEvent" }
    public func priority(): Int32 { 1 }
    public func messageId(): String { msgId }
    public func timestamp(): Int64 { ts }
}

/**
 * 错误事件
 * 用于处理状态机执行过程中的错误
 */
public struct ErrorEvent <: StateMachineEvent {
    private let error: Exception
    private let errorCode: String
    private let ts: Int64
    private let msgId: String
    
    public init(error: Exception, errorCode: String = "UNKNOWN") {
        this.error = error
        this.errorCode = errorCode
        this.ts = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.msgId = "error-${ts}-${errorCode}"
    }
    
    public func getError(): Exception { error }
    public func getErrorCode(): String { errorCode }
    
    public func getEventType(): String { "error.${errorCode}" }
    
    public func getEventData(): HashMap<String, Any> {
        let data = HashMap<String, Any>()
        data.put("error", error.message)
        data.put("errorCode", errorCode)
        return data
    }
    
    public func getSource(): Option<String> { Some("error") }
    
    // Message接口实现
    public func messageType(): String { "ErrorEvent" }
    public func priority(): Int32 { 100 }  // 最高优先级
    public func messageId(): String { msgId }
    public func timestamp(): Int64 { ts }
}

/**
 * 状态查询事件
 * 用于查询状态机的当前状态
 */
public struct StateQueryEvent <: StateMachineEvent {
    private let queryType: String
    private let ts: Int64
    private let msgId: String
    
    public init(queryType: String = "current") {
        this.queryType = queryType
        this.ts = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.msgId = "query-${ts}-${queryType}"
    }
    
    public func getQueryType(): String { queryType }
    
    public func getEventType(): String { "query.${queryType}" }
    
    public func getEventData(): HashMap<String, Any> {
        let data = HashMap<String, Any>()
        data.put("queryType", queryType)
        return data
    }
    
    public func getSource(): Option<String> { Some("query") }
    
    // Message接口实现
    public func messageType(): String { "StateQueryEvent" }
    public func priority(): Int32 { 5 }
    public func messageId(): String { msgId }
    public func timestamp(): Int64 { ts }
}

/**
 * 上下文更新事件
 * 用于更新状态机的上下文数据
 */
public struct ContextUpdateEvent<TContext> <: StateMachineEvent where TContext <: Equatable {
    private let contextUpdate: (TContext) -> TContext
    private let updateType: String
    private let ts: Int64
    private let msgId: String
    
    public init(
        contextUpdate: (TContext) -> TContext,
        updateType: String = "update"
    ) {
        this.contextUpdate = contextUpdate
        this.updateType = updateType
        this.ts = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.msgId = "context-${ts}-${updateType}"
    }
    
    public func getContextUpdate(): (TContext) -> TContext { contextUpdate }
    public func getUpdateType(): String { updateType }
    
    public func getEventType(): String { "context.${updateType}" }
    
    public func getEventData(): HashMap<String, Any> {
        let data = HashMap<String, Any>()
        data.put("updateType", updateType)
        return data
    }
    
    public func getSource(): Option<String> { Some("context") }
    
    // Message接口实现
    public func messageType(): String { "ContextUpdateEvent" }
    public func priority(): Int32 { 3 }
    public func messageId(): String { msgId }
    public func timestamp(): Int64 { ts }
}

/**
 * 事件工厂
 * 提供便捷的事件创建方法
 */
public struct EventFactory {
    /**
     * 创建基础事件
     */
    public static func createEvent(
        eventType: String,
        data: HashMap<String, Any> = HashMap<String, Any>()
    ): BaseStateMachineEvent {
        BaseStateMachineEvent(eventType, data)
    }
    
    /**
     * 创建定时器事件
     */
    public static func createTimerEvent(
        timerId: String,
        delay: Duration
    ): TimerEvent {
        TimerEvent(timerId, delay)
    }
    
    /**
     * 创建错误事件
     */
    public static func createErrorEvent(
        error: Exception,
        errorCode: String = "UNKNOWN"
    ): ErrorEvent {
        ErrorEvent(error, errorCode)
    }
    
    /**
     * 创建状态查询事件
     */
    public static func createQueryEvent(
        queryType: String = "current"
    ): StateQueryEvent {
        StateQueryEvent(queryType)
    }
    
    /**
     * 创建上下文更新事件
     */
    public static func createContextUpdateEvent<TContext>(
        contextUpdate: (TContext) -> TContext,
        updateType: String = "update"
    ): ContextUpdateEvent<TContext> where TContext <: Equatable {
        ContextUpdateEvent(contextUpdate, updateType)
    }
}
