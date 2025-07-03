/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 层次状态机实现
 * 基于CActor框架的层次状态机支持
 */
package cactor.xstate.features.hierarchical

import std.collection.*
import std.time.*
import cactor.api.{CActor, CActorSystem}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.context.{ActorContext}
import cactor.core.message.{Message, MessageResult}
import cactor.xstate.core.types.{StateMachineState, StateMachineEvent}
import cactor.xstate.core.machine.{StateMachineDefinition}
import cactor.xstate.core.actor.{StateMachineActor}

/**
 * 层次状态接口
 * 支持嵌套状态和状态继承
 */
public interface HierarchicalState <: StateMachineState {
    /**
     * 获取父状态ID
     */
    func getParentStateId(): Option<String>
    
    /**
     * 获取子状态列表
     */
    func getChildStates(): Array<String>
    
    /**
     * 是否为复合状态
     */
    func isCompoundState(): Bool
    
    /**
     * 获取初始子状态
     */
    func getInitialChildState(): Option<String>
}

/**
 * 层次状态实现
 */
public struct HierarchicalStateImpl <: HierarchicalState {
    private let stateId: String
    private let displayName: String
    private let parentStateId: Option<String>
    private let childStates: Array<String>
    private let isInitialState: Bool
    private let isCompound: Bool
    private let initialChildState: Option<String>

    public init(
        stateId: String,
        displayName: String,
        parentStateId: Option<String>,
        childStates: Array<String>,
        isInitialState: Bool,
        isCompound: Bool,
        initialChildState: Option<String>
    ) {
        this.stateId = stateId
        this.displayName = displayName
        this.parentStateId = parentStateId
        this.childStates = childStates
        this.isInitialState = isInitialState
        this.isCompound = isCompound
        this.initialChildState = initialChildState
    }

    public func getStateId(): String { stateId }
    public func getDisplayName(): String { displayName }
    public func isInitial(): Bool { isInitialState }
    public func getParentStateId(): Option<String> { parentStateId }
    public func getChildStates(): Array<String> { childStates }
    public func isCompoundState(): Bool { isCompound }
    public func getInitialChildState(): Option<String> { initialChildState }
    
    public func equals(other: StateMachineState): Bool {
        return stateId == other.getStateId()
    }
}

/**
 * 层次状态机Actor
 * 支持嵌套状态和状态层次管理
 */
public class HierarchicalStateMachineActor <: Actor {
    private let machineDefinition: StateMachineDefinition
    private var currentState: String
    private var stateHierarchy: HashMap<String, HierarchicalStateImpl>
    private var childActors: HashMap<String, ActorRef>
    private var parentActor: Option<ActorRef>
    
    public init(
        machineDefinition: StateMachineDefinition,
        parentActor: Option<ActorRef>
    ) {
        this.machineDefinition = machineDefinition
        this.currentState = machineDefinition.initialStateId
        this.stateHierarchy = HashMap<String, HierarchicalStateImpl>()
        this.childActors = HashMap<String, ActorRef>()
        this.parentActor = parentActor
        
        // 初始化状态层次结构
        initializeStateHierarchy()
    }
    
    /**
     * Actor名称
     */
    public prop name: String {
        get() { "HierarchicalStateMachineActor-${machineDefinition.id}" }
    }
    
    /**
     * Actor描述
     */
    public prop description: String {
        get() { "层次状态机Actor: ${machineDefinition.id}" }
    }
    
    /**
     * 处理消息
     */
    public func receive(message: Message, context: ActorContext): MessageResult {
        if (message.messageType() == "HierarchicalStateEvent") {
            return handleHierarchicalEvent(message, context)
        } else if (message.messageType() == "StateTransition") {
            return handleStateTransition(message, context)
        } else if (message.messageType() == "GetCurrentState") {
            println("当前层次状态: ${getCurrentHierarchicalState()}")
            return MessageResult.Handled
        } else {
            println("未知消息类型: ${message.messageType()}")
            return MessageResult.Unhandled
        }
    }
    
    /**
     * 初始化状态层次结构
     */
    private func initializeStateHierarchy(): Unit {
        // 创建示例层次状态
        let rootState = HierarchicalStateImpl(
            "root",
            "根状态",
            None,
            ["idle", "active"],
            true,
            true,
            Some("idle")
        )
        
        let idleState = HierarchicalStateImpl(
            "idle",
            "空闲状态",
            Some("root"),
            [],
            true,
            false,
            None
        )
        
        let activeState = HierarchicalStateImpl(
            "active",
            "活跃状态",
            Some("root"),
            ["processing", "completed"],
            false,
            true,
            Some("processing")
        )
        
        let processingState = HierarchicalStateImpl(
            "processing",
            "处理中状态",
            Some("active"),
            [],
            true,
            false,
            None
        )
        
        let completedState = HierarchicalStateImpl(
            "completed",
            "完成状态",
            Some("active"),
            [],
            false,
            false,
            None
        )
        
        stateHierarchy.add("root", rootState)
        stateHierarchy.add("idle", idleState)
        stateHierarchy.add("active", activeState)
        stateHierarchy.add("processing", processingState)
        stateHierarchy.add("completed", completedState)
    }
    
    /**
     * 处理层次状态事件
     */
    private func handleHierarchicalEvent(message: Message, context: ActorContext): MessageResult {
        println("处理层次状态事件: ${message.messageType()}")
        
        // 获取当前状态
        let currentStateOption = stateHierarchy.get(currentState)
        if (currentStateOption.isSome()) {
            let state = currentStateOption.getOrThrow()
            
            // 如果是复合状态，需要处理子状态
            if (state.isCompoundState()) {
                handleCompoundStateEvent(state, message, context)
            } else {
                handleSimpleStateEvent(state, message, context)
            }
        }
        
        return MessageResult.Handled
    }
    
    /**
     * 处理复合状态事件
     */
    private func handleCompoundStateEvent(
        state: HierarchicalStateImpl,
        message: Message,
        context: ActorContext
    ): Unit {
        println("处理复合状态事件: ${state.getStateId()}")
        
        // 将事件传递给子状态
        for (childStateId in state.getChildStates()) {
            if (childActors.contains(childStateId)) {
                let childActor = childActors.get(childStateId).getOrThrow()
                // 在实际实现中，这里会发送消息给子Actor
                println("转发事件到子状态: ${childStateId}")
            }
        }
    }
    
    /**
     * 处理简单状态事件
     */
    private func handleSimpleStateEvent(
        state: HierarchicalStateImpl,
        message: Message,
        context: ActorContext
    ): Unit {
        println("处理简单状态事件: ${state.getStateId()}")
        
        // 处理状态转换逻辑
        // 在实际实现中，这里会调用转换引擎
    }
    
    /**
     * 处理状态转换
     */
    private func handleStateTransition(message: Message, context: ActorContext): MessageResult {
        println("处理状态转换: ${currentState}")
        
        // 实现状态转换逻辑
        // 需要考虑层次状态的进入和退出语义
        
        return MessageResult.Handled
    }
    
    /**
     * 获取当前层次状态路径
     */
    public func getCurrentHierarchicalState(): String {
        let statePath = ArrayList<String>()
        var currentStateId = currentState
        
        while (true) {
            statePath.add(currentStateId)
            let stateOption = stateHierarchy.get(currentStateId)
            if (stateOption.isSome()) {
                let state = stateOption.getOrThrow()
                let parentOption = state.getParentStateId()
                if (parentOption.isSome()) {
                    currentStateId = parentOption.getOrThrow()
                } else {
                    break
                }
            } else {
                break
            }
        }
        
        // 反转路径，从根到叶
        let reversedPath = ArrayList<String>()
        for (i in (statePath.size - 1)..0) {
            reversedPath.add(statePath[i])
        }
        
        // 手动连接字符串
        var result = ""
        for (i in 0..reversedPath.size-1) {
            if (i > 0) {
                result = result + "."
            }
            result = result + reversedPath[i]
        }
        return result
    }
    
    /**
     * 创建子状态Actor
     */
    public func createChildActor(
        childStateId: String,
        childMachineDefinition: StateMachineDefinition,
        system: CActorSystem
    ): Unit {
        let childActor = HierarchicalStateMachineActor(childMachineDefinition, None)
        // 在实际实现中，这里会通过系统创建子Actor
        println("创建子状态Actor: ${childStateId}")
    }
}

/**
 * 层次状态机工厂
 */
public struct HierarchicalStateMachineFactory {
    /**
     * 创建层次状态机Actor
     */
    public static func create(
        system: CActorSystem,
        machineDefinition: StateMachineDefinition,
        actorName: String
    ): ActorRef {
        let actor = HierarchicalStateMachineActor(machineDefinition, None)
        return system.actorOf({ => actor }, actorName)
    }
}
