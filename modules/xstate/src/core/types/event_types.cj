/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 事件类型定义
 * 基于仓颉0.60.5语言特性的强类型事件系统
 */
package cactor_xstate.core.types

import std.collection.*
import std.time.*

/**
 * 基础状态机事件实现
 * 提供默认的事件行为
 */
public struct BaseStateMachineEvent <: StateMachineEvent {
    private let eventType: String
    private let eventData: HashMap<String, Any>
    private let source: Option<String>
    private let msgId: String
    private let eventTimestamp: Int64

    public init(eventType: String) {
        this.eventType = eventType
        this.eventData = HashMap<String, Any>()
        this.source = None
        this.msgId = "event-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
        this.eventTimestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
    
    public init(eventType: String, eventData: HashMap<String, Any>) {
        this.eventType = eventType
        this.eventData = eventData
        this.source = None
        this.msgId = "event-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
        this.eventTimestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }

    public init(eventType: String, eventData: HashMap<String, Any>, source: Option<String>) {
        this.eventType = eventType
        this.eventData = eventData
        this.source = source
        this.msgId = "event-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
        this.eventTimestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
    
    // StateMachineEvent接口实现
    public func getEventType(): String {
        eventType
    }
    
    public func getEventData(): HashMap<String, Any> {
        eventData
    }
    
    public func getSource(): Option<String> {
        source
    }
    
    public func matchesType(eventType: String): Bool {
        this.eventType == eventType
    }
    
    // Message接口实现
    public func messageType(): String {
        "StateMachineEvent"
    }
    
    public func priority(): Int32 {
        0
    }
    
    public func messageId(): String {
        msgId
    }
    
    public func timestamp(): Int64 {
        this.eventTimestamp
    }
}

/**
 * 内部事件
 * 用于状态机内部的自动转换
 */
public struct InternalEvent <: StateMachineEvent {
    private let eventType: String
    private let msgId: String
    private let eventTimestamp: Int64

    public init(eventType: String) {
        this.eventType = eventType
        this.msgId = "internal-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
        this.eventTimestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
    
    public func getEventType(): String {
        eventType
    }
    
    public func getEventData(): HashMap<String, Any> {
        HashMap<String, Any>()
    }
    
    public func getSource(): Option<String> {
        Some("internal")
    }
    
    public func matchesType(eventType: String): Bool {
        this.eventType == eventType
    }
    
    public func messageType(): String {
        "InternalEvent"
    }
    
    public func priority(): Int32 {
        1
    }
    
    public func messageId(): String {
        msgId
    }
    
    public func timestamp(): Int64 {
        this.eventTimestamp
    }
}

/**
 * 定时器事件
 * 用于延迟转换和超时处理
 */
public struct TimerEvent <: StateMachineEvent {
    private let timerId: String
    private let delay: Int64
    private let msgId: String
    private let eventTimestamp: Int64

    public init(timerId: String, delay: Int64) {
        this.timerId = timerId
        this.delay = delay
        this.msgId = "timer-${timerId}-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
        this.eventTimestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
    
    public func getEventType(): String {
        "timer.${timerId}"
    }
    
    public func getEventData(): HashMap<String, Any> {
        let data = HashMap<String, Any>()
        data.add("timerId", timerId)
        data.add("delay", delay)
        return data
    }
    
    public func getSource(): Option<String> {
        Some("timer")
    }
    
    public func matchesType(eventType: String): Bool {
        eventType == "timer.${timerId}" || eventType == "timer.*"
    }
    
    public func messageType(): String {
        "TimerEvent"
    }
    
    public func priority(): Int32 {
        2
    }
    
    public func messageId(): String {
        msgId
    }
    
    public func timestamp(): Int64 {
        this.eventTimestamp
    }
    
    /**
     * 获取定时器ID
     */
    public func getTimerId(): String {
        timerId
    }
    
    /**
     * 获取延迟时间
     */
    public func getDelay(): Int64 {
        delay
    }
}

/**
 * 错误事件
 * 用于处理状态机执行过程中的错误
 */
public struct ErrorEvent <: StateMachineEvent {
    private let error: String
    private let errorCode: String
    private let msgId: String
    private let eventTimestamp: Int64

    public init(error: String, errorCode: String) {
        this.error = error
        this.errorCode = errorCode
        this.msgId = "error-${errorCode}-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
        this.eventTimestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
    
    public func getEventType(): String {
        "error.${errorCode}"
    }
    
    public func getEventData(): HashMap<String, Any> {
        let data = HashMap<String, Any>()
        data.add("error", error)
        data.add("errorCode", errorCode)
        return data
    }
    
    public func getSource(): Option<String> {
        Some("error")
    }
    
    public func matchesType(eventType: String): Bool {
        eventType == "error.${errorCode}" || eventType == "error.*"
    }
    
    public func messageType(): String {
        "ErrorEvent"
    }
    
    public func priority(): Int32 {
        10
    }
    
    public func messageId(): String {
        msgId
    }
    
    public func timestamp(): Int64 {
        this.eventTimestamp
    }
    
    /**
     * 获取错误信息
     */
    public func getError(): String {
        error
    }
    
    /**
     * 获取错误代码
     */
    public func getErrorCode(): String {
        errorCode
    }
}
