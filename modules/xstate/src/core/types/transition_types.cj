/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 转换类型定义
 * 基于仓颉0.60.5语言特性的强类型转换系统
 */
package cactor_xstate.core.types

import std.collection.*
import std.time.*

/**
 * 转换结果枚举
 * 表示状态转换的不同结果
 */
public enum TransitionResult {
    | Success      // 转换成功
    | Invalid      // 无效转换
    | Deferred     // 延迟转换
    | Forbidden    // 禁止转换
    
    /**
     * 检查转换是否成功
     */
    public func isSuccess(): Bool {
        match (this) {
            case Success => true
            case _ => false
        }
    }
    
    /**
     * 检查转换是否失败
     */
    public func isFailure(): Bool {
        match (this) {
            case Invalid | Forbidden => true
            case _ => false
        }
    }
    
    /**
     * 转换为字符串
     */
    public func toString(): String {
        match (this) {
            case Success => "success"
            case Invalid => "invalid"
            case Deferred => "deferred"
            case Forbidden => "forbidden"
        }
    }
}

/**
 * 转换副作用枚举
 * 表示状态转换过程中产生的副作用
 */
public enum TransitionEffect {
    | LogEffect        // 日志副作用
    | NotifyEffect     // 通知副作用
    | TimerEffect      // 定时器副作用
    | CancelTimerEffect // 取消定时器副作用
    | CustomEffect     // 自定义副作用
    
    /**
     * 获取副作用类型
     */
    public func getEffectType(): String {
        match (this) {
            case LogEffect => "log"
            case NotifyEffect => "notify"
            case TimerEffect => "timer"
            case CancelTimerEffect => "cancel_timer"
            case CustomEffect => "custom"
        }
    }
}

/**
 * 转换定义
 * 定义状态之间的转换规则
 */
public struct TransitionDefinition {
    public let id: String
    public let sourceStateId: String
    public let targetStateId: String
    public let eventType: String
    public let priority: Int32
    public let metadata: HashMap<String, String>
    
    public init(
        id: String,
        sourceStateId: String,
        targetStateId: String,
        eventType: String,
        priority: Int32,
        metadata: HashMap<String, String>
    ) {
        this.id = id
        this.sourceStateId = sourceStateId
        this.targetStateId = targetStateId
        this.eventType = eventType
        this.priority = priority
        this.metadata = metadata
    }
    
    /**
     * 创建简单转换定义
     */
    public static func create(
        sourceStateId: String,
        targetStateId: String,
        eventType: String
    ): TransitionDefinition {
        let id = "${sourceStateId}-${eventType}-${targetStateId}"
        TransitionDefinition(
            id,
            sourceStateId,
            targetStateId,
            eventType,
            0,
            HashMap<String, String>()
        )
    }
    
    /**
     * 检查转换是否匹配指定的源状态和事件
     */
    public func matches(sourceStateId: String, eventType: String): Bool {
        this.sourceStateId == sourceStateId && this.eventType == eventType
    }
    
    /**
     * 检查是否为内部转换
     */
    public func isInternal(): Bool {
        sourceStateId == targetStateId
    }
    
    /**
     * 获取转换描述
     */
    public func getDescription(): String {
        "${sourceStateId} --[${eventType}]--> ${targetStateId}"
    }
}

/**
 * 转换构建器
 * 提供流式API构建转换定义
 */
public struct TransitionBuilder {
    private var id: String
    private var sourceStateId: String
    private var targetStateId: String
    private var eventType: String
    private var priority: Int32
    private var metadata: HashMap<String, String>
    
    public init() {
        this.id = ""
        this.sourceStateId = ""
        this.targetStateId = ""
        this.eventType = ""
        this.priority = 0
        this.metadata = HashMap<String, String>()
    }
    
    /**
     * 设置转换ID
     */
    public mut func withId(id: String): Unit {
        this.id = id
    }
    
    /**
     * 设置源状态
     */
    public mut func from(sourceStateId: String): Unit {
        this.sourceStateId = sourceStateId
    }
    
    /**
     * 设置目标状态
     */
    public mut func to(targetStateId: String): Unit {
        this.targetStateId = targetStateId
    }
    
    /**
     * 设置触发事件
     */
    public mut func on(eventType: String): Unit {
        this.eventType = eventType
    }
    
    /**
     * 设置优先级
     */
    public mut func withPriority(priority: Int32): Unit {
        this.priority = priority
    }
    
    /**
     * 添加元数据
     */
    public mut func withMeta(key: String, value: String): Unit {
        metadata.add(key, value)
    }
    
    /**
     * 构建转换定义
     */
    public func build(): TransitionDefinition {
        var finalId = id
        if (finalId.isEmpty()) {
            finalId = "${sourceStateId}-${eventType}-${targetStateId}"
        }
        
        TransitionDefinition(
            finalId,
            sourceStateId,
            targetStateId,
            eventType,
            priority,
            metadata
        )
    }
}

/**
 * 守卫条件接口
 * 定义状态转换的守卫条件
 */
public interface GuardCondition {
    /**
     * 评估守卫条件
     */
    func evaluate(): Bool
    
    /**
     * 获取守卫条件描述
     */
    func getDescription(): String
}

/**
 * 简单守卫条件实现
 */
public struct SimpleGuard <: GuardCondition {
    private let condition: Bool
    private let description: String
    
    public init(condition: Bool, description: String) {
        this.condition = condition
        this.description = description
    }
    
    public func evaluate(): Bool {
        condition
    }
    
    public func getDescription(): String {
        description
    }
}

/**
 * 转换动作接口
 * 定义状态转换时执行的动作
 */
public interface TransitionAction {
    /**
     * 执行转换动作
     */
    func execute(): Unit
    
    /**
     * 获取动作描述
     */
    func getDescription(): String
}

/**
 * 简单动作实现
 */
public struct SimpleAction <: TransitionAction {
    private let description: String
    
    public init(description: String) {
        this.description = description
    }
    
    public func execute(): Unit {
        println("执行动作: ${description}")
    }
    
    public func getDescription(): String {
        description
    }
}
