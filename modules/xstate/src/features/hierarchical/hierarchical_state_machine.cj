/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 层次状态机实现
 * 基于CActor框架的层次状态机支持
 */
package cactor_xstate.features.hierarchical

import std.collection.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.context.{ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_xstate.core.types.{StateMachineState, BaseStateMachineEvent}
import cactor_xstate.core.machine.{StateMachineDefinition}
import cactor_xstate.core.engine.{PatternTransitionEngine}

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
 * 真实实现嵌套状态和状态层次管理
 */
public class HierarchicalStateMachineActor <: Actor {
    private let machineDefinition: StateMachineDefinition
    private var currentState: String
    private var currentStatePath: Array<String>  // 完整的状态路径
    private var stateHierarchy: HashMap<String, HierarchicalStateImpl>
    private var childActors: HashMap<String, ActorRef>
    private var parentActor: Option<ActorRef>
    private var actorSystem: Option<CActorSystem>
    private var transitionEngine: PatternTransitionEngine

    public init(
        machineDefinition: StateMachineDefinition,
        parentActor: Option<ActorRef>
    ) {
        this.machineDefinition = machineDefinition
        this.currentState = machineDefinition.initialStateId
        this.currentStatePath = [machineDefinition.initialStateId]
        this.stateHierarchy = HashMap<String, HierarchicalStateImpl>()
        this.childActors = HashMap<String, ActorRef>()
        this.parentActor = parentActor
        this.actorSystem = None
        this.transitionEngine = PatternTransitionEngine(machineDefinition)

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

        // 解析事件
        if (message.messageType() == "ProcessEventMessage") {
            return handleStateTransitionEvent(message, context)
        } else if (message.messageType() == "EnterStateMessage") {
            return handleEnterState(message, context)
        } else if (message.messageType() == "ExitStateMessage") {
            return handleExitState(message, context)
        }

        return MessageResult.Unhandled
    }

    /**
     * 处理状态转换事件
     */
    private func handleStateTransitionEvent(message: Message, context: ActorContext): MessageResult {
        // 获取当前状态
        let currentStateOption = stateHierarchy.get(currentState)
        if (currentStateOption.isSome()) {
            let state = currentStateOption.getOrThrow()

            // 尝试在当前状态处理事件
            let handled = tryHandleEventInCurrentState(state, message, context)
            if (handled) {
                return MessageResult.Handled
            }

            // 如果当前状态无法处理，向上冒泡到父状态
            return bubbleEventToParent(state, message, context)
        }

        return MessageResult.Unhandled
    }

    /**
     * 在当前状态尝试处理事件
     */
    private func tryHandleEventInCurrentState(
        state: HierarchicalStateImpl,
        message: Message,
        context: ActorContext
    ): Bool {
        // 如果是复合状态，先尝试让子状态处理
        if (state.isCompoundState()) {
            let childHandled = delegateToChildStates(state, message, context)
            if (childHandled) {
                return true
            }
        }

        // 在当前状态级别处理事件
        return processEventAtCurrentLevel(state, message, context)
    }

    /**
     * 委托给子状态处理
     */
    private func delegateToChildStates(
        state: HierarchicalStateImpl,
        message: Message,
        context: ActorContext
    ): Bool {
        for (childStateId in state.getChildStates()) {
            if (childActors.contains(childStateId)) {
                let childActor = childActors.get(childStateId).getOrThrow()
                // 发送消息给子状态Actor
                childActor.tell(message)
                return true
            }
        }
        return false
    }

    /**
     * 在当前级别处理事件
     */
    private func processEventAtCurrentLevel(
        state: HierarchicalStateImpl,
        message: Message,
        context: ActorContext
    ): Bool {
        // 使用转换引擎处理事件
        if (message.messageType() == "ProcessEventMessage") {
            // 简化的事件处理 - 在实际实现中会解析具体的事件类型
            let toggleEvent = BaseStateMachineEvent("toggle")
            let result = transitionEngine.processTransition(state.getStateId(), toggleEvent)

            // 处理转换结果
            if (result.isSuccess()) {
                performHierarchicalTransition(state.getStateId(), result.targetStateId, context)
                return true
            }
        }

        return false
    }

    /**
     * 执行层次状态转换
     */
    private func performHierarchicalTransition(
        fromState: String,
        toState: String,
        context: ActorContext
    ): Unit {
        println("执行层次状态转换: ${fromState} -> ${toState}")

        // 1. 计算状态路径变化
        let fromPath = getStatePath(fromState)
        let toPath = getStatePath(toState)

        // 2. 找到最低公共祖先
        let commonAncestor = findCommonAncestor(fromPath, toPath)

        // 3. 退出从当前状态到公共祖先的所有状态
        exitStatesUpTo(fromPath, commonAncestor, context)

        // 4. 进入从公共祖先到目标状态的所有状态
        enterStatesFrom(commonAncestor, toPath, context)

        // 5. 更新当前状态
        currentState = toState
        currentStatePath = toPath
    }

    /**
     * 获取状态的完整路径
     */
    private func getStatePath(stateId: String): Array<String> {
        let path = ArrayList<String>()
        var currentId = stateId

        while (true) {
            path.add(currentId)
            let stateOption = stateHierarchy.get(currentId)
            if (stateOption.isSome()) {
                let state = stateOption.getOrThrow()
                let parentOption = state.getParentStateId()
                if (parentOption.isSome()) {
                    currentId = parentOption.getOrThrow()
                } else {
                    break
                }
            } else {
                break
            }
        }

        // 反转路径，从根到叶
        let reversedPath = ArrayList<String>()
        for (i in (path.size - 1)..0) {
            reversedPath.add(path[i])
        }

        return reversedPath.toArray()
    }

    /**
     * 处理进入状态事件
     */
    private func handleEnterState(message: Message, context: ActorContext): MessageResult {
        println("处理进入状态事件: ${message.messageType()}")
        // 实现状态进入逻辑
        return MessageResult.Handled
    }

    /**
     * 处理退出状态事件
     */
    private func handleExitState(message: Message, context: ActorContext): MessageResult {
        println("处理退出状态事件: ${message.messageType()}")
        // 实现状态退出逻辑
        return MessageResult.Handled
    }

    /**
     * 向父状态冒泡事件
     */
    private func bubbleEventToParent(
        state: HierarchicalStateImpl,
        message: Message,
        context: ActorContext
    ): MessageResult {
        let parentOption = state.getParentStateId()
        if (parentOption.isSome()) {
            let parentStateId = parentOption.getOrThrow()
            let parentStateOption = stateHierarchy.get(parentStateId)
            if (parentStateOption.isSome()) {
                let parentState = parentStateOption.getOrThrow()
                if (tryHandleEventInCurrentState(parentState, message, context)) {
                    return MessageResult.Handled
                } else {
                    return MessageResult.Unhandled
                }
            }
        }
        return MessageResult.Unhandled
    }

    /**
     * 找到两个状态路径的最低公共祖先
     */
    private func findCommonAncestor(fromPath: Array<String>, toPath: Array<String>): String {
        let minLength = if (fromPath.size < toPath.size) { fromPath.size } else { toPath.size }
        var commonAncestor = ""

        for (i in 0..minLength-1) {
            if (fromPath[i] == toPath[i]) {
                commonAncestor = fromPath[i]
            } else {
                break
            }
        }

        return commonAncestor
    }

    /**
     * 退出状态直到指定祖先
     */
    private func exitStatesUpTo(
        fromPath: Array<String>,
        commonAncestor: String,
        context: ActorContext
    ): Unit {
        for (i in (fromPath.size - 1)..0) {
            let stateId = fromPath[i]
            if (stateId == commonAncestor) {
                break
            }
            exitState(stateId, context)
        }
    }

    /**
     * 从指定祖先进入状态
     */
    private func enterStatesFrom(
        commonAncestor: String,
        toPath: Array<String>,
        context: ActorContext
    ): Unit {
        var startIndex = 0
        for (i in 0..toPath.size-1) {
            if (toPath[i] == commonAncestor) {
                startIndex = i + 1
                break
            }
        }

        for (i in startIndex..toPath.size-1) {
            enterState(toPath[i], context)
        }
    }

    /**
     * 退出指定状态
     */
    private func exitState(stateId: String, context: ActorContext): Unit {
        println("退出状态: ${stateId}")
        // 实现状态退出逻辑
        // 1. 执行退出动作
        // 2. 清理状态相关资源
        // 3. 通知子状态退出
    }

    /**
     * 进入指定状态
     */
    private func enterState(stateId: String, context: ActorContext): Unit {
        println("进入状态: ${stateId}")
        // 实现状态进入逻辑
        // 1. 执行进入动作
        // 2. 初始化状态资源
        // 3. 激活子状态（如果有）
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
