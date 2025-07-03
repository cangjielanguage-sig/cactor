/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 基于仓颉语言特性的转换类型定义
 * 充分利用仓颉0.60.5的强类型系统、函数类型、模式匹配等特性
 */
package cactor.xstate.core.types

import std.collection.*
import std.time.*

/**
 * 转换结果枚举
 * 利用仓颉枚举的强类型特性表示转换的不同结果
 */
public enum TransitionResult<TState, TContext> 
where TState <: StateMachineState, TContext <: Equatable {
    | Success(targetState: TState, context: TContext, effects: Array<TransitionEffect>)
    | Invalid(reason: String)
    | Deferred
    | Forbidden(reason: String)
    
    /**
     * 检查转换是否成功
     */
    public func isSuccess(): Bool {
        match (this) {
            case Success(_, _, _) => true
            case _ => false
        }
    }
    
    /**
     * 检查转换是否无效
     */
    public func isInvalid(): Bool {
        match (this) {
            case Invalid(_) | Forbidden(_) => true
            case _ => false
        }
    }
    
    /**
     * 获取目标状态（如果成功）
     */
    public func getTargetState(): Option<TState> {
        match (this) {
            case Success(targetState, _, _) => Some(targetState)
            case _ => None
        }
    }
    
    /**
     * 获取新上下文（如果成功）
     */
    public func getContext(): Option<TContext> {
        match (this) {
            case Success(_, context, _) => Some(context)
            case _ => None
        }
    }
    
    /**
     * 获取副作用列表（如果成功）
     */
    public func getEffects(): Array<TransitionEffect> {
        match (this) {
            case Success(_, _, effects) => effects
            case _ => Array<TransitionEffect>()
        }
    }
    
    /**
     * 获取失败原因
     */
    public func getFailureReason(): Option<String> {
        match (this) {
            case Invalid(reason) | Forbidden(reason) => Some(reason)
            case _ => None
        }
    }
}

/**
 * 转换副作用
 * 表示状态转换过程中产生的副作用
 */
public enum TransitionEffect {
    | LogEffect(message: String, level: String)
    | NotifyEffect(target: String, message: String)
    | TimerEffect(timerId: String, delay: Duration)
    | CancelTimerEffect(timerId: String)
    | CustomEffect(name: String, data: HashMap<String, Any>)
    
    /**
     * 获取副作用类型
     */
    public func getEffectType(): String {
        match (this) {
            case LogEffect(_, _) => "log"
            case NotifyEffect(_, _) => "notify"
            case TimerEffect(_, _) => "timer"
            case CancelTimerEffect(_) => "cancel_timer"
            case CustomEffect(name, _) => name
        }
    }
    
    /**
     * 获取副作用数据
     */
    public func getEffectData(): HashMap<String, Any> {
        let data = HashMap<String, Any>()
        match (this) {
            case LogEffect(message, level) =>
                data.put("message", message)
                data.put("level", level)
            case NotifyEffect(target, message) =>
                data.put("target", target)
                data.put("message", message)
            case TimerEffect(timerId, delay) =>
                data.put("timerId", timerId)
                data.put("delay", delay.toNanoseconds())
            case CancelTimerEffect(timerId) =>
                data.put("timerId", timerId)
            case CustomEffect(_, effectData) =>
                return effectData
        }
        return data
    }
}

/**
 * 守卫条件接口
 * 定义状态转换的守卫条件
 */
public interface GuardCondition<TEvent, TContext> 
where TEvent <: StateMachineEvent, TContext <: Equatable {
    /**
     * 评估守卫条件
     */
    func evaluate(event: TEvent, context: TContext): Bool
    
    /**
     * 获取守卫条件的描述
     */
    func getDescription(): String {
        "GuardCondition"
    }
}

/**
 * 函数式守卫条件实现
 * 使用函数类型实现灵活的守卫条件
 */
public struct FunctionGuard<TEvent, TContext> <: GuardCondition<TEvent, TContext>
where TEvent <: StateMachineEvent, TContext <: Equatable {
    private let guardFunction: (TEvent, TContext) -> Bool
    private let description: String
    
    public init(guardFunction: (TEvent, TContext) -> Bool, description: String = "FunctionGuard") {
        this.guardFunction = guardFunction
        this.description = description
    }
    
    public func evaluate(event: TEvent, context: TContext): Bool {
        guardFunction(event, context)
    }
    
    public func getDescription(): String {
        description
    }
}

/**
 * 转换动作接口
 * 定义状态转换时执行的动作
 */
public interface TransitionAction<TEvent, TContext> 
where TEvent <: StateMachineEvent, TContext <: Equatable {
    /**
     * 执行转换动作
     */
    func execute(event: TEvent, context: TContext): TContext
    
    /**
     * 获取动作的描述
     */
    func getDescription(): String {
        "TransitionAction"
    }
}

/**
 * 函数式转换动作实现
 * 使用函数类型实现灵活的转换动作
 */
public struct FunctionAction<TEvent, TContext> <: TransitionAction<TEvent, TContext>
where TEvent <: StateMachineEvent, TContext <: Equatable {
    private let actionFunction: (TEvent, TContext) -> TContext
    private let description: String
    
    public init(actionFunction: (TEvent, TContext) -> TContext, description: String = "FunctionAction") {
        this.actionFunction = actionFunction
        this.description = description
    }
    
    public func execute(event: TEvent, context: TContext): TContext {
        actionFunction(event, context)
    }
    
    public func getDescription(): String {
        description
    }
}

/**
 * 转换定义
 * 定义状态之间的转换规则
 */
public struct TransitionDefinition<TState, TEvent, TContext> 
where TState <: StateMachineState, TEvent <: StateMachineEvent, TContext <: Equatable {
    public let id: String
    public let source: TState
    public let target: TState
    public let eventType: String
    public let guard: Option<GuardCondition<TEvent, TContext>>
    public let actions: Array<TransitionAction<TEvent, TContext>>
    public let effects: Array<TransitionEffect>
    public let priority: Int32
    public let meta: HashMap<String, String>
    
    public init(
        id: String,
        source: TState,
        target: TState,
        eventType: String,
        guard: Option<GuardCondition<TEvent, TContext>> = None,
        actions: Array<TransitionAction<TEvent, TContext>> = Array<TransitionAction<TEvent, TContext>>(),
        effects: Array<TransitionEffect> = Array<TransitionEffect>(),
        priority: Int32 = 0,
        meta: HashMap<String, String> = HashMap<String, String>()
    ) {
        this.id = id
        this.source = source
        this.target = target
        this.eventType = eventType
        this.guard = guard
        this.actions = actions
        this.effects = effects
        this.priority = priority
        this.meta = meta
    }
    
    /**
     * 检查转换是否匹配指定的源状态和事件
     */
    public func matches(sourceState: TState, event: TEvent): Bool {
        source.equals(sourceState) && event.matchesType(eventType)
    }
    
    /**
     * 检查事件是否匹配此转换
     */
    public func matchesEvent(event: TEvent): Bool {
        event.matchesType(eventType)
    }
    
    /**
     * 评估守卫条件
     */
    public func evaluateGuard(event: TEvent, context: TContext): Bool {
        match (guard) {
            case Some(guardCondition) => guardCondition.evaluate(event, context)
            case None => true  // 没有守卫条件，默认通过
        }
    }
    
    /**
     * 执行转换动作
     */
    public func executeActions(event: TEvent, context: TContext): TContext {
        var currentContext = context
        
        for (action in actions) {
            currentContext = action.execute(event, currentContext)
        }
        
        return currentContext
    }
    
    /**
     * 获取转换的优先级
     */
    public func getPriority(): Int32 {
        priority
    }
    
    /**
     * 获取转换的副作用
     */
    public func getEffects(): Array<TransitionEffect> {
        effects
    }
    
    /**
     * 检查是否是内部转换（源状态和目标状态相同）
     */
    public func isInternal(): Bool {
        source.equals(target)
    }
    
    /**
     * 获取转换的描述信息
     */
    public func getDescription(): String {
        "${source.getStateId()} --[${eventType}]--> ${target.getStateId()}"
    }
}

/**
 * 转换构建器
 * 提供流畅的API来构建转换定义
 */
public class TransitionBuilder<TState, TEvent, TContext> 
where TState <: StateMachineState, TEvent <: StateMachineEvent, TContext <: Equatable {
    private var id: String = ""
    private var source: Option<TState> = None
    private var target: Option<TState> = None
    private var eventType: String = ""
    private var guard: Option<GuardCondition<TEvent, TContext>> = None
    private let actions: ArrayList<TransitionAction<TEvent, TContext>> = ArrayList()
    private let effects: ArrayList<TransitionEffect> = ArrayList()
    private var priority: Int32 = 0
    private let meta: HashMap<String, String> = HashMap()
    
    public init() {}
    
    /**
     * 设置转换ID
     */
    public func withId(id: String): TransitionBuilder<TState, TEvent, TContext> {
        this.id = id
        return this
    }
    
    /**
     * 设置源状态
     */
    public func from(source: TState): TransitionBuilder<TState, TEvent, TContext> {
        this.source = Some(source)
        return this
    }
    
    /**
     * 设置目标状态
     */
    public func to(target: TState): TransitionBuilder<TState, TEvent, TContext> {
        this.target = Some(target)
        return this
    }
    
    /**
     * 设置触发事件类型
     */
    public func on(eventType: String): TransitionBuilder<TState, TEvent, TContext> {
        this.eventType = eventType
        return this
    }
    
    /**
     * 设置守卫条件
     */
    public func when(guard: GuardCondition<TEvent, TContext>): TransitionBuilder<TState, TEvent, TContext> {
        this.guard = Some(guard)
        return this
    }
    
    /**
     * 添加转换动作
     */
    public func withAction(action: TransitionAction<TEvent, TContext>): TransitionBuilder<TState, TEvent, TContext> {
        actions.add(action)
        return this
    }
    
    /**
     * 添加副作用
     */
    public func withEffect(effect: TransitionEffect): TransitionBuilder<TState, TEvent, TContext> {
        effects.add(effect)
        return this
    }
    
    /**
     * 设置优先级
     */
    public func withPriority(priority: Int32): TransitionBuilder<TState, TEvent, TContext> {
        this.priority = priority
        return this
    }
    
    /**
     * 构建转换定义
     */
    public func build(): TransitionDefinition<TState, TEvent, TContext> {
        if (source.isNone() || target.isNone() || eventType.isEmpty()) {
            throw Exception("转换定义不完整：必须指定源状态、目标状态和事件类型")
        }
        
        let finalId = if (id.isEmpty()) {
            "${source.getOrThrow().getStateId()}-${eventType}-${target.getOrThrow().getStateId()}"
        } else {
            id
        }
        
        return TransitionDefinition(
            finalId,
            source.getOrThrow(),
            target.getOrThrow(),
            eventType,
            guard,
            actions.toArray(),
            effects.toArray(),
            priority,
            meta
        )
    }
}
