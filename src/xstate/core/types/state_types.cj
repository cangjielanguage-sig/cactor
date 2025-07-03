/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 基于仓颉语言特性的状态类型定义
 * 充分利用仓颉0.60.5的强类型系统、泛型、枚举等现代特性
 */
package cactor.xstate.core.types

import std.collection.*
import std.time.*

/**
 * 状态机状态基础接口
 * 利用仓颉的接口和泛型系统实现类型安全
 */
public interface StateMachineState {
    /**
     * 获取状态的唯一标识
     */
    func getStateId(): String
    
    /**
     * 获取状态的显示名称
     */
    func getDisplayName(): String {
        getStateId()
    }
    
    /**
     * 检查状态是否相等
     */
    func equals(other: StateMachineState): Bool {
        getStateId() == other.getStateId()
    }
}

/**
 * 状态类型枚举
 * 利用仓颉枚举的强类型特性
 */
public enum StateType {
    | Atomic      // 原子状态
    | Compound    // 复合状态（有子状态）
    | Parallel    // 并行状态（多个正交区域）
    | Final       // 最终状态
    | History     // 历史状态
    
    /**
     * 转换为字符串表示
     */
    public func toString(): String {
        match (this) {
            case Atomic => "atomic"
            case Compound => "compound"
            case Parallel => "parallel"
            case Final => "final"
            case History => "history"
        }
    }
    
    /**
     * 检查是否是复合状态类型
     */
    public func isCompound(): Bool {
        match (this) {
            case Compound | Parallel => true
            case _ => false
        }
    }
}

/**
 * 状态值 - 表示状态机的当前状态配置
 * 利用仓颉的结构体和泛型实现类型安全的状态表示
 */
public struct StateValue<TState> where TState <: StateMachineState {
    public let value: TState
    public let children: HashMap<String, StateValue<TState>>
    public let timestamp: Int64
    
    public init(value: TState) {
        this.value = value
        this.children = HashMap<String, StateValue<TState>>()
        this.timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
    
    public init(value: TState, children: HashMap<String, StateValue<TState>>) {
        this.value = value
        this.children = children
        this.timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
    
    /**
     * 检查是否匹配指定状态
     * 利用仓颉的模式匹配和类型安全
     */
    public func matches(state: TState): Bool {
        if (value.equals(state)) {
            return true
        }
        
        // 检查子状态
        for ((_, childState) in children) {
            if (childState.matches(state)) {
                return true
            }
        }
        
        return false
    }
    
    /**
     * 获取所有活跃状态的路径
     */
    public func getActivePaths(): Array<String> {
        let paths = ArrayList<String>()
        paths.add(value.getStateId())
        
        for ((key, childState) in children) {
            let childPaths = childState.getActivePaths()
            for (childPath in childPaths) {
                paths.add("${key}.${childPath}")
            }
        }
        
        return paths.toArray()
    }
    
    /**
     * 检查是否是叶子状态（没有子状态）
     */
    public func isLeaf(): Bool {
        children.isEmpty()
    }
    
    /**
     * 获取子状态
     */
    public func getChild(key: String): Option<StateValue<TState>> {
        children.get(key)
    }
    
    /**
     * 添加子状态
     */
    public func withChild(key: String, child: StateValue<TState>): StateValue<TState> {
        let newChildren = children.clone()
        newChildren.put(key, child)
        StateValue(value, newChildren)
    }
}

/**
 * 状态定义 - 定义状态的结构和行为
 * 利用仓颉的泛型和函数类型实现灵活的状态定义
 */
public struct StateDefinition<TState, TEvent, TContext> 
where TState <: StateMachineState, TEvent <: StateMachineEvent, TContext <: Equatable {
    public let id: String
    public let stateType: StateType
    public let initial: Option<String>
    public let entry: Option<(TContext, TEvent) -> TContext>
    public let exit: Option<(TContext, TEvent) -> TContext>
    public let transitions: Array<TransitionDefinition<TState, TEvent, TContext>>
    public let children: HashMap<String, StateDefinition<TState, TEvent, TContext>>
    public let meta: HashMap<String, String>
    
    public init(
        id: String,
        stateType: StateType = StateType.Atomic,
        initial: Option<String> = None,
        entry: Option<(TContext, TEvent) -> TContext> = None,
        exit: Option<(TContext, TEvent) -> TContext> = None,
        transitions: Array<TransitionDefinition<TState, TEvent, TContext>> = Array<TransitionDefinition<TState, TEvent, TContext>>(),
        children: HashMap<String, StateDefinition<TState, TEvent, TContext>> = HashMap<String, StateDefinition<TState, TEvent, TContext>>(),
        meta: HashMap<String, String> = HashMap<String, String>()
    ) {
        this.id = id
        this.stateType = stateType
        this.initial = initial
        this.entry = entry
        this.exit = exit
        this.transitions = transitions
        this.children = children
        this.meta = meta
    }
    
    /**
     * 检查是否有子状态
     */
    public func hasChildren(): Bool {
        !children.isEmpty()
    }
    
    /**
     * 检查是否是复合状态
     */
    public func isCompound(): Bool {
        stateType.isCompound()
    }
    
    /**
     * 检查是否是最终状态
     */
    public func isFinal(): Bool {
        stateType == StateType.Final
    }
    
    /**
     * 获取初始状态
     */
    public func getInitialState(): Option<String> {
        initial
    }
    
    /**
     * 获取子状态定义
     */
    public func getChild(id: String): Option<StateDefinition<TState, TEvent, TContext>> {
        children.get(id)
    }
    
    /**
     * 获取所有转换定义
     */
    public func getTransitions(): Array<TransitionDefinition<TState, TEvent, TContext>> {
        transitions
    }
    
    /**
     * 查找匹配事件的转换
     */
    public func findTransitions(event: TEvent): Array<TransitionDefinition<TState, TEvent, TContext>> {
        let matchingTransitions = ArrayList<TransitionDefinition<TState, TEvent, TContext>>()
        
        for (transition in transitions) {
            if (transition.matchesEvent(event)) {
                matchingTransitions.add(transition)
            }
        }
        
        return matchingTransitions.toArray()
    }
}

/**
 * 状态配置 - 表示状态机的完整运行时状态
 * 包含当前状态、上下文和元数据
 */
public struct StateConfiguration<TState, TEvent, TContext> 
where TState <: StateMachineState, TEvent <: StateMachineEvent, TContext <: Equatable {
    public let states: HashSet<TState>
    public let context: TContext
    public let event: Option<TEvent>
    public let timestamp: Int64
    public let metadata: HashMap<String, String>
    
    public init(
        states: HashSet<TState>,
        context: TContext,
        event: Option<TEvent> = None,
        metadata: HashMap<String, String> = HashMap<String, String>()
    ) {
        this.states = states
        this.context = context
        this.event = event
        this.timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.metadata = metadata
    }
    
    /**
     * 检查是否包含指定状态
     */
    public func hasState(state: TState): Bool {
        for (s in states) {
            if (s.equals(state)) {
                return true
            }
        }
        return false
    }
    
    /**
     * 获取活跃状态列表
     */
    public func getActiveStates(): Array<TState> {
        let activeStates = ArrayList<TState>()
        for (state in states) {
            activeStates.add(state)
        }
        return activeStates.toArray()
    }
    
    /**
     * 检查是否是最终配置
     */
    public func isFinal(): Bool {
        // 如果任何活跃状态是最终状态，则整个配置是最终的
        for (state in states) {
            // 这里需要根据具体的状态实现来判断是否是最终状态
            // 暂时返回false，具体实现时需要完善
        }
        return false
    }
    
    /**
     * 更新上下文
     */
    public func withContext(newContext: TContext): StateConfiguration<TState, TEvent, TContext> {
        StateConfiguration(states, newContext, event, metadata)
    }
    
    /**
     * 添加元数据
     */
    public func withMetadata(key: String, value: String): StateConfiguration<TState, TEvent, TContext> {
        let newMetadata = metadata.clone()
        newMetadata.put(key, value)
        StateConfiguration(states, context, event, newMetadata)
    }
}
