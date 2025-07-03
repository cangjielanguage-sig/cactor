/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 基于仓颉语言特性的状态机定义
 * 充分利用仓颉0.60.5的强类型系统、泛型、构建器模式等特性
 */
package cactor.xstate.core.machine

import std.collection.*
import std.time.*
import cactor.xstate.core.types.*

/**
 * 状态机定义
 * 定义状态机的完整结构和行为
 */
public struct StateMachineDefinition<TState, TEvent, TContext> 
where TState <: StateMachineState, TEvent <: StateMachineEvent, TContext <: Equatable {
    public let id: String
    public let version: String
    public let initialState: TState
    public let initialContext: TContext
    public let states: HashMap<String, StateDefinition<TState, TEvent, TContext>>
    public let transitions: Array<TransitionDefinition<TState, TEvent, TContext>>
    public let globalGuards: HashMap<String, GuardCondition<TEvent, TContext>>
    public let globalActions: HashMap<String, TransitionAction<TEvent, TContext>>
    public let meta: HashMap<String, String>
    public let createdAt: Int64
    
    public init(
        id: String,
        version: String = "1.0.0",
        initialState: TState,
        initialContext: TContext,
        states: HashMap<String, StateDefinition<TState, TEvent, TContext>> = HashMap<String, StateDefinition<TState, TEvent, TContext>>(),
        transitions: Array<TransitionDefinition<TState, TEvent, TContext>> = Array<TransitionDefinition<TState, TEvent, TContext>>(),
        globalGuards: HashMap<String, GuardCondition<TEvent, TContext>> = HashMap<String, GuardCondition<TEvent, TContext>>(),
        globalActions: HashMap<String, TransitionAction<TEvent, TContext>> = HashMap<String, TransitionAction<TEvent, TContext>>(),
        meta: HashMap<String, String> = HashMap<String, String>()
    ) {
        this.id = id
        this.version = version
        this.initialState = initialState
        this.initialContext = initialContext
        this.states = states
        this.transitions = transitions
        this.globalGuards = globalGuards
        this.globalActions = globalActions
        this.meta = meta
        this.createdAt = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
    
    /**
     * 获取状态定义
     */
    public func getState(stateId: String): Option<StateDefinition<TState, TEvent, TContext>> {
        states.get(stateId)
    }
    
    /**
     * 获取所有状态定义
     */
    public func getAllStates(): Array<StateDefinition<TState, TEvent, TContext>> {
        let stateList = ArrayList<StateDefinition<TState, TEvent, TContext>>()
        for ((_, state) in states) {
            stateList.add(state)
        }
        return stateList.toArray()
    }
    
    /**
     * 查找从指定状态出发的转换
     */
    public func getTransitionsFrom(sourceState: TState): Array<TransitionDefinition<TState, TEvent, TContext>> {
        let matchingTransitions = ArrayList<TransitionDefinition<TState, TEvent, TContext>>()
        
        for (transition in transitions) {
            if (transition.source.equals(sourceState)) {
                matchingTransitions.add(transition)
            }
        }
        
        return matchingTransitions.toArray()
    }
    
    /**
     * 查找匹配事件的转换
     */
    public func getTransitionsForEvent(sourceState: TState, event: TEvent): Array<TransitionDefinition<TState, TEvent, TContext>> {
        let matchingTransitions = ArrayList<TransitionDefinition<TState, TEvent, TContext>>()
        
        for (transition in transitions) {
            if (transition.matches(sourceState, event)) {
                matchingTransitions.add(transition)
            }
        }
        
        // 按优先级排序
        matchingTransitions.sort({ a, b => b.getPriority() - a.getPriority() })
        
        return matchingTransitions.toArray()
    }
    
    /**
     * 获取全局守卫条件
     */
    public func getGlobalGuard(name: String): Option<GuardCondition<TEvent, TContext>> {
        globalGuards.get(name)
    }
    
    /**
     * 获取全局动作
     */
    public func getGlobalAction(name: String): Option<TransitionAction<TEvent, TContext>> {
        globalActions.get(name)
    }
    
    /**
     * 验证状态机定义的完整性
     */
    public func validate(): Array<String> {
        let errors = ArrayList<String>()
        
        // 检查初始状态是否存在
        if (!states.contains(initialState.getStateId())) {
            errors.add("初始状态 '${initialState.getStateId()}' 未在状态定义中找到")
        }
        
        // 检查转换的源状态和目标状态是否存在
        for (transition in transitions) {
            if (!states.contains(transition.source.getStateId())) {
                errors.add("转换 '${transition.id}' 的源状态 '${transition.source.getStateId()}' 未定义")
            }
            if (!states.contains(transition.target.getStateId())) {
                errors.add("转换 '${transition.id}' 的目标状态 '${transition.target.getStateId()}' 未定义")
            }
        }
        
        return errors.toArray()
    }
    
    /**
     * 获取状态机的统计信息
     */
    public func getStatistics(): HashMap<String, Int64> {
        let stats = HashMap<String, Int64>()
        stats.put("stateCount", states.size.toInt64())
        stats.put("transitionCount", transitions.size.toInt64())
        stats.put("globalGuardCount", globalGuards.size.toInt64())
        stats.put("globalActionCount", globalActions.size.toInt64())
        return stats
    }
}

/**
 * 状态机构建器
 * 提供流畅的API来构建状态机定义
 */
public class StateMachineBuilder<TState, TEvent, TContext> 
where TState <: StateMachineState, TEvent <: StateMachineEvent, TContext <: Equatable {
    private var id: String = ""
    private var version: String = "1.0.0"
    private var initialState: Option<TState> = None
    private var initialContext: Option<TContext> = None
    private let states: HashMap<String, StateDefinition<TState, TEvent, TContext>> = HashMap()
    private let transitions: ArrayList<TransitionDefinition<TState, TEvent, TContext>> = ArrayList()
    private let globalGuards: HashMap<String, GuardCondition<TEvent, TContext>> = HashMap()
    private let globalActions: HashMap<String, TransitionAction<TEvent, TContext>> = HashMap()
    private let meta: HashMap<String, String> = HashMap()
    
    public init() {}
    
    /**
     * 设置状态机ID
     */
    public func withId(id: String): StateMachineBuilder<TState, TEvent, TContext> {
        this.id = id
        return this
    }
    
    /**
     * 设置版本
     */
    public func withVersion(version: String): StateMachineBuilder<TState, TEvent, TContext> {
        this.version = version
        return this
    }
    
    /**
     * 设置初始状态
     */
    public func withInitialState(state: TState): StateMachineBuilder<TState, TEvent, TContext> {
        this.initialState = Some(state)
        return this
    }
    
    /**
     * 设置初始上下文
     */
    public func withInitialContext(context: TContext): StateMachineBuilder<TState, TEvent, TContext> {
        this.initialContext = Some(context)
        return this
    }
    
    /**
     * 添加状态定义
     */
    public func addState(state: StateDefinition<TState, TEvent, TContext>): StateMachineBuilder<TState, TEvent, TContext> {
        states.put(state.id, state)
        return this
    }
    
    /**
     * 添加多个状态定义
     */
    public func addStates(stateList: Array<StateDefinition<TState, TEvent, TContext>>): StateMachineBuilder<TState, TEvent, TContext> {
        for (state in stateList) {
            states.put(state.id, state)
        }
        return this
    }
    
    /**
     * 添加转换定义
     */
    public func addTransition(transition: TransitionDefinition<TState, TEvent, TContext>): StateMachineBuilder<TState, TEvent, TContext> {
        transitions.add(transition)
        return this
    }
    
    /**
     * 添加多个转换定义
     */
    public func addTransitions(transitionList: Array<TransitionDefinition<TState, TEvent, TContext>>): StateMachineBuilder<TState, TEvent, TContext> {
        for (transition in transitionList) {
            transitions.add(transition)
        }
        return this
    }
    
    /**
     * 添加全局守卫条件
     */
    public func addGlobalGuard(name: String, guard: GuardCondition<TEvent, TContext>): StateMachineBuilder<TState, TEvent, TContext> {
        globalGuards.put(name, guard)
        return this
    }
    
    /**
     * 添加全局动作
     */
    public func addGlobalAction(name: String, action: TransitionAction<TEvent, TContext>): StateMachineBuilder<TState, TEvent, TContext> {
        globalActions.put(name, action)
        return this
    }
    
    /**
     * 添加元数据
     */
    public func addMeta(key: String, value: String): StateMachineBuilder<TState, TEvent, TContext> {
        meta.put(key, value)
        return this
    }
    
    /**
     * 构建状态机定义
     */
    public func build(): StateMachineDefinition<TState, TEvent, TContext> {
        if (id.isEmpty()) {
            throw Exception("状态机ID不能为空")
        }
        
        if (initialState.isNone()) {
            throw Exception("必须指定初始状态")
        }
        
        if (initialContext.isNone()) {
            throw Exception("必须指定初始上下文")
        }
        
        let definition = StateMachineDefinition(
            id,
            version,
            initialState.getOrThrow(),
            initialContext.getOrThrow(),
            states,
            transitions.toArray(),
            globalGuards,
            globalActions,
            meta
        )
        
        // 验证状态机定义
        let validationErrors = definition.validate()
        if (!validationErrors.isEmpty()) {
            let errorMessage = validationErrors.join("; ")
            throw Exception("状态机定义验证失败: ${errorMessage}")
        }
        
        return definition
    }
}

/**
 * 状态机工厂
 * 提供便捷的状态机创建方法
 */
public struct StateMachineFactory {
    /**
     * 创建简单的状态机
     */
    public static func createSimple<TState, TEvent, TContext>(
        id: String,
        initialState: TState,
        initialContext: TContext,
        transitions: Array<TransitionDefinition<TState, TEvent, TContext>>
    ): StateMachineDefinition<TState, TEvent, TContext> 
    where TState <: StateMachineState, TEvent <: StateMachineEvent, TContext <: Equatable {
        
        let builder = StateMachineBuilder<TState, TEvent, TContext>()
            .withId(id)
            .withInitialState(initialState)
            .withInitialContext(initialContext)
            .addTransitions(transitions)
        
        return builder.build()
    }
    
    /**
     * 创建状态机构建器
     */
    public static func builder<TState, TEvent, TContext>(): StateMachineBuilder<TState, TEvent, TContext> 
    where TState <: StateMachineState, TEvent <: StateMachineEvent, TContext <: Equatable {
        StateMachineBuilder<TState, TEvent, TContext>()
    }
}
