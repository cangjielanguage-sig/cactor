/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 状态机定义
 * 基于仓颉0.60.5语言特性的状态机核心实现
 */
package cactor_xstate.core.machine

import std.collection.*
import std.time.*
import cactor_xstate.core.types.{StateMachineState, StateMachineEvent, TransitionDefinition, TransitionResult}

/**
 * 状态机定义
 * 定义状态机的结构和行为
 */
public struct StateMachineDefinition {
    public let id: String
    public let version: String
    public let initialStateId: String
    public let states: HashMap<String, StateNode>
    public let transitions: Array<TransitionDefinition>
    public let metadata: HashMap<String, String>
    
    public init(
        id: String,
        version: String,
        initialStateId: String,
        states: HashMap<String, StateNode>,
        transitions: Array<TransitionDefinition>,
        metadata: HashMap<String, String>
    ) {
        this.id = id
        this.version = version
        this.initialStateId = initialStateId
        this.states = states
        this.transitions = transitions
        this.metadata = metadata
    }
    
    /**
     * 验证状态机定义
     */
    public func validate(): Array<String> {
        let errors = ArrayList<String>()
        
        // 验证ID
        if (id.isEmpty()) {
            errors.add("状态机ID不能为空")
        }
        
        // 验证初始状态
        if (initialStateId.isEmpty()) {
            errors.add("初始状态ID不能为空")
        } else if (!states.contains(initialStateId)) {
            errors.add("初始状态'${initialStateId}'不存在于状态定义中")
        }
        
        // 验证转换定义
        for (transition in transitions) {
            if (!states.contains(transition.sourceStateId)) {
                errors.add("转换'${transition.id}'的源状态'${transition.sourceStateId}'不存在")
            }
            if (!states.contains(transition.targetStateId)) {
                errors.add("转换'${transition.id}'的目标状态'${transition.targetStateId}'不存在")
            }
        }
        
        return errors.toArray()
    }
    
    /**
     * 获取指定状态和事件的转换
     */
    public func getTransitionsForEvent(sourceStateId: String, eventType: String): Array<TransitionDefinition> {
        let matchingTransitions = ArrayList<TransitionDefinition>()
        
        for (transition in transitions) {
            if (transition.matches(sourceStateId, eventType)) {
                matchingTransitions.add(transition)
            }
        }
        
        return matchingTransitions.toArray()
    }
    
    /**
     * 检查状态是否存在
     */
    public func hasState(stateId: String): Bool {
        states.contains(stateId)
    }
    
    /**
     * 获取状态节点
     */
    public func getState(stateId: String): Option<StateNode> {
        states.get(stateId)
    }
    
    /**
     * 获取所有状态ID
     */
    public func getStateIds(): Array<String> {
        states.keys().toArray()
    }
    
    /**
     * 获取转换数量
     */
    public func getTransitionCount(): Int32 {
        Int32(transitions.size)
    }
}

/**
 * 状态节点
 * 表示状态机中的一个状态
 */
public struct StateNode {
    public let id: String
    public let displayName: String
    public let isFinal: Bool
    public let isInitial: Bool
    public let onEntry: Array<String>
    public let onExit: Array<String>
    public let metadata: HashMap<String, String>
    
    public init(
        id: String,
        displayName: String,
        isFinal: Bool,
        isInitial: Bool,
        onEntry: Array<String>,
        onExit: Array<String>,
        metadata: HashMap<String, String>
    ) {
        this.id = id
        this.displayName = displayName
        this.isFinal = isFinal
        this.isInitial = isInitial
        this.onEntry = onEntry
        this.onExit = onExit
        this.metadata = metadata
    }
    
    /**
     * 创建简单状态节点
     */
    public static func create(id: String): StateNode {
        StateNode(
            id,
            id,
            false,
            false,
            Array<String>(),
            Array<String>(),
            HashMap<String, String>()
        )
    }
    
    /**
     * 创建最终状态节点
     */
    public static func createFinal(id: String): StateNode {
        StateNode(
            id,
            id,
            true,
            false,
            Array<String>(),
            Array<String>(),
            HashMap<String, String>()
        )
    }
    
    /**
     * 检查是否有进入动作
     */
    public func hasEntryActions(): Bool {
        !onEntry.isEmpty()
    }
    
    /**
     * 检查是否有退出动作
     */
    public func hasExitActions(): Bool {
        !onExit.isEmpty()
    }
}

/**
 * 状态机构建器
 * 提供流式API构建状态机定义
 */
public struct StateMachineBuilder {
    private var id: String
    private var version: String
    private var initialStateId: String
    private var states: HashMap<String, StateNode>
    private var transitions: ArrayList<TransitionDefinition>
    private var metadata: HashMap<String, String>
    
    public init() {
        this.id = ""
        this.version = "1.0.0"
        this.initialStateId = ""
        this.states = HashMap<String, StateNode>()
        this.transitions = ArrayList<TransitionDefinition>()
        this.metadata = HashMap<String, String>()
    }
    
    /**
     * 设置状态机ID
     */
    public mut func withId(id: String): Unit {
        this.id = id
    }
    
    /**
     * 设置版本
     */
    public mut func withVersion(version: String): Unit {
        this.version = version
    }
    
    /**
     * 设置初始状态
     */
    public mut func withInitialState(stateId: String): Unit {
        this.initialStateId = stateId
    }
    
    /**
     * 添加状态
     */
    public mut func addState(state: StateNode): Unit {
        states.add(state.id, state)
    }
    
    /**
     * 添加转换
     */
    public mut func addTransition(transition: TransitionDefinition): Unit {
        transitions.add(transition)
    }
    
    /**
     * 添加多个转换
     */
    public mut func addTransitions(newTransitions: Array<TransitionDefinition>): Unit {
        for (transition in newTransitions) {
            transitions.add(transition)
        }
    }
    
    /**
     * 添加元数据
     */
    public mut func addMeta(key: String, value: String): Unit {
        metadata.add(key, value)
    }
    
    /**
     * 构建状态机定义
     */
    public func build(): StateMachineDefinition {
        StateMachineDefinition(
            id,
            version,
            initialStateId,
            states,
            transitions.toArray(),
            metadata
        )
    }
}
