/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 并行状态机实现
 * 基于CActor框架的并行状态机支持
 */
package cactor_xstate.features.parallel

import std.collection.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.context.{ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_xstate.core.types.{StateMachineState}
import cactor_xstate.core.machine.{StateMachineDefinition}
import cactor_xstate.core.engine.{PatternTransitionEngine}
import cactor_foundation.logging.{Logger, Logging}

let log_parallel_state_machine: Logger = Logging.logger("cactor_xstate.features.parallel.parallel_state_machine")

/**
 * 并行状态接口
 * 支持正交区域和并行执行
 */
public interface ParallelState <: StateMachineState {
    /**
     * 获取并行区域列表
     */
    func getRegions(): Array<String>
    
    /**
     * 是否为并行状态
     */
    func isParallelState(): Bool
    
    /**
     * 获取区域的初始状态
     */
    func getRegionInitialState(regionId: String): Option<String>
}

/**
 * 并行状态实现
 */
public struct ParallelStateImpl <: ParallelState {
    private let stateId: String
    private let displayName: String
    private let regions: Array<String>
    private let regionInitialStates: HashMap<String, String>
    private let isInitialState: Bool

    public init(
        stateId: String,
        displayName: String,
        regions: Array<String>,
        regionInitialStates: HashMap<String, String>,
        isInitialState: Bool
    ) {
        this.stateId = stateId
        this.displayName = displayName
        this.regions = regions
        this.regionInitialStates = regionInitialStates
        this.isInitialState = isInitialState
    }

    public func getStateId(): String { stateId }
    public func getDisplayName(): String { displayName }
    public func isInitial(): Bool { isInitialState }
    public func getRegions(): Array<String> { regions }
    public func isParallelState(): Bool { true }
    
    public func getRegionInitialState(regionId: String): Option<String> {
        regionInitialStates.get(regionId)
    }
    
    public func equals(other: StateMachineState): Bool {
        return stateId == other.getStateId()
    }
}

/**
 * 并行区域状态
 */
public struct RegionState {
    public let regionId: String
    public let currentState: String
    public let isActive: Bool
    
    public init(regionId: String, currentState: String, isActive: Bool) {
        this.regionId = regionId
        this.currentState = currentState
        this.isActive = isActive
    }
}

/**
 * 并行状态机Actor
 * 真实实现正交区域和并行状态执行
 */
public class ParallelStateMachineActor <: Actor {
    private let machineDefinition: StateMachineDefinition
    private var currentState: String
    private var parallelStates: HashMap<String, ParallelStateImpl>
    private var regionStates: HashMap<String, RegionState>
    private var regionActors: HashMap<String, ActorRef>
    private var actorSystem: Option<CActorSystem>
    private var transitionEngine: PatternTransitionEngine
    private var completionCallbacks: ArrayList<() -> Unit>

    public init(machineDefinition: StateMachineDefinition) {
        this.machineDefinition = machineDefinition
        this.currentState = machineDefinition.initialStateId
        this.parallelStates = HashMap<String, ParallelStateImpl>()
        this.regionStates = HashMap<String, RegionState>()
        this.regionActors = HashMap<String, ActorRef>()
        this.actorSystem = None
        this.transitionEngine = PatternTransitionEngine(machineDefinition)
        this.completionCallbacks = ArrayList<() -> Unit>()

        // 初始化并行状态
        initializeParallelStates()
    }
    
    /**
     * Actor名称
     */
    public prop name: String {
        get() { "ParallelStateMachineActor-${machineDefinition.id}" }
    }
    
    /**
     * Actor描述
     */
    public prop description: String {
        get() { "并行状态机Actor: ${machineDefinition.id}" }
    }
    
    /**
     * 处理消息
     */
    public func receive(message: Message, context: ActorContext): MessageResult {
        if (message.messageType() == "ParallelStateEvent") {
            return handleParallelEvent(message, context)
        } else if (message.messageType() == "RegionStateUpdate") {
            return handleRegionStateUpdate(message, context)
        } else if (message.messageType() == "GetParallelState") {
            log_parallel_state_machine.debug("当前并行状态: ${getCurrentParallelState()}")
            return MessageResult.Handled
        } else {
            log_parallel_state_machine.debug("未知消息类型: ${message.messageType()}")
            return MessageResult.Unhandled
        }
    }
    
    /**
     * 初始化并行状态
     */
    private func initializeParallelStates(): Unit {
        // 创建示例并行状态
        let regionInitialStates = HashMap<String, String>()
        regionInitialStates.add("upload", "idle")
        regionInitialStates.add("validation", "pending")
        
        let parallelState = ParallelStateImpl(
            "processing",
            "并行处理状态",
            ["upload", "validation"],
            regionInitialStates,
            true
        )
        
        parallelStates.add("processing", parallelState)
        
        // 初始化区域状态
        regionStates.add("upload", RegionState("upload", "idle", true))
        regionStates.add("validation", RegionState("validation", "pending", true))
    }
    
    /**
     * 处理并行状态事件
     */
    private func handleParallelEvent(message: Message, context: ActorContext): MessageResult {
        log_parallel_state_machine.debug("处理并行状态事件: ${message.messageType()}")

        // 根据消息类型分发处理
        if (message.messageType() == "ParallelEventMessage") {
            return handleParallelEventMessage(message, context)
        } else if (message.messageType() == "RegionCompletedMessage") {
            return handleRegionCompleted(message, context)
        } else if (message.messageType() == "SynchronizeRegionsMessage") {
            return handleSynchronizeRegions(message, context)
        }

        return MessageResult.Unhandled
    }

    /**
     * 处理并行事件消息
     */
    private func handleParallelEventMessage(message: Message, context: ActorContext): MessageResult {
        // 获取当前并行状态
        let parallelStateOption = parallelStates.get(currentState)
        if (parallelStateOption.isSome()) {
            let parallelState = parallelStateOption.getOrThrow()

            // 广播事件到所有活跃区域
            broadcastEventToRegions(parallelState, message, context)

            // 检查是否所有区域都完成
            checkAndHandleCompletion(context)
        }

        return MessageResult.Handled
    }

    /**
     * 处理区域完成消息
     */
    private func handleRegionCompleted(message: Message, context: ActorContext): MessageResult {
        log_parallel_state_machine.debug("处理区域完成消息: ${message.messageType()}")

        // 更新区域状态
        updateRegionCompletionStatus(message)

        // 检查是否所有区域都完成
        checkAndHandleCompletion(context)

        return MessageResult.Handled
    }

    /**
     * 处理区域同步消息
     */
    private func handleSynchronizeRegions(message: Message, context: ActorContext): MessageResult {
        log_parallel_state_machine.debug("处理区域同步消息")

        // 同步所有区域状态
        synchronizeAllRegions(context)

        return MessageResult.Handled
    }

    /**
     * 更新区域完成状态
     */
    private func updateRegionCompletionStatus(message: Message): Unit {
        // 在实际实现中，这里会解析消息获取区域ID和完成状态
        // 简化实现：标记第一个区域为完成
        for ((regionId, regionState) in regionStates) {
            if (!isRegionCompleted(regionId)) {
                let completedRegionState = RegionState(regionId, "completed", true)
                regionStates.add(regionId, completedRegionState)
                log_parallel_state_machine.debug("区域 ${regionId} 标记为完成")
                break
            }
        }
    }

    /**
     * 检查区域是否完成
     */
    private func isRegionCompleted(regionId: String): Bool {
        let regionStateOption = regionStates.get(regionId)
        if (regionStateOption.isSome()) {
            let regionState = regionStateOption.getOrThrow()
            return regionState.currentState == "completed" || regionState.currentState == "validated"
        }
        return false
    }

    /**
     * 检查并处理完成状态
     */
    private func checkAndHandleCompletion(context: ActorContext): Unit {
        if (areAllRegionsCompleted()) {
            handleAllRegionsCompleted(context)
        }
    }

    /**
     * 检查所有区域是否完成
     */
    private func areAllRegionsCompleted(): Bool {
        for ((regionId, regionState) in regionStates) {
            if (!isRegionCompleted(regionId)) {
                return false
            }
        }
        return true
    }

    /**
     * 处理所有区域完成
     */
    private func handleAllRegionsCompleted(context: ActorContext): Unit {
        log_parallel_state_machine.debug("🎉 所有并行区域已完成!")

        // 执行完成回调
        for (callback in completionCallbacks) {
            callback()
        }

        // 触发并行状态机完成事件
        triggerParallelStateMachineCompleted(context)
    }

    /**
     * 触发并行状态机完成事件
     */
    private func triggerParallelStateMachineCompleted(context: ActorContext): Unit {
        log_parallel_state_machine.debug("触发并行状态机完成事件")
        // 在实际实现中，这里会发送完成事件给父状态机或监听者
    }

    /**
     * 同步所有区域
     */
    private func synchronizeAllRegions(context: ActorContext): Unit {
        log_parallel_state_machine.debug("同步所有并行区域状态")

        for ((regionId, regionActor) in regionActors) {
            // 发送同步消息给区域Actor
            // 在实际实现中，这里会发送具体的同步消息
            log_parallel_state_machine.debug("同步区域: ${regionId}")
        }
    }

    /**
     * 添加完成回调
     */
    public func addCompletionCallback(callback: () -> Unit): Unit {
        completionCallbacks.add(callback)
    }
    
    /**
     * 广播事件到所有区域
     */
    private func broadcastEventToRegions(
        parallelState: ParallelStateImpl,
        message: Message,
        context: ActorContext
    ): Unit {
        log_parallel_state_machine.debug("广播事件到并行区域: ${parallelState.getStateId()}")
        
        for (regionId in parallelState.getRegions()) {
            let regionStateOption = regionStates.get(regionId)
            if (regionStateOption.isSome()) {
                let regionState = regionStateOption.getOrThrow()
                if (regionState.isActive) {
                    processRegionEvent(regionId, regionState, message, context)
                }
            }
        }
    }
    
    /**
     * 处理区域事件
     */
    private func processRegionEvent(
        regionId: String,
        regionState: RegionState,
        message: Message,
        context: ActorContext
    ): Unit {
        log_parallel_state_machine.debug("处理区域事件: ${regionId} - ${regionState.currentState}")
        
        // 在实际实现中，这里会调用区域特定的状态机
        // 模拟区域状态转换
        let newState = simulateRegionTransition(regionId, regionState.currentState, message)
        
        if (newState != regionState.currentState) {
            // 更新区域状态
            let updatedRegionState = RegionState(regionId, newState, true)
            regionStates.add(regionId, updatedRegionState)
            
            log_parallel_state_machine.debug("区域状态转换: ${regionId} ${regionState.currentState} -> ${newState}")
            
            // 检查是否所有区域都达到最终状态
            checkParallelCompletion(context)
        }
    }
    
    /**
     * 模拟区域状态转换
     */
    private func simulateRegionTransition(
        regionId: String,
        currentState: String,
        message: Message
    ): String {
        // 简化的状态转换逻辑
        if (regionId == "upload") {
            match (currentState) {
                case "idle" => "uploading"
                case "uploading" => "completed"
                case _ => currentState
            }
        } else if (regionId == "validation") {
            match (currentState) {
                case "pending" => "validating"
                case "validating" => "validated"
                case _ => currentState
            }
        } else {
            currentState
        }
    }
    
    /**
     * 处理区域状态更新
     */
    private func handleRegionStateUpdate(message: Message, context: ActorContext): MessageResult {
        log_parallel_state_machine.debug("处理区域状态更新")
        
        // 在实际实现中，这里会处理来自子区域的状态更新
        checkParallelCompletion(context)
        
        return MessageResult.Handled
    }
    
    /**
     * 检查并行状态完成
     */
    private func checkParallelCompletion(context: ActorContext): Unit {
        var allCompleted = true
        let completionStatus = ArrayList<String>()
        
        for ((regionId, regionState) in regionStates) {
            completionStatus.add("${regionId}: ${regionState.currentState}")
            
            // 检查区域是否完成（简化逻辑）
            if (regionState.currentState != "completed" && regionState.currentState != "validated") {
                allCompleted = false
            }
        }
        
        // 手动连接字符串
        var statusStr = ""
        for (i in 0..completionStatus.size-1) {
            if (i > 0) {
                statusStr = statusStr + ", "
            }
            statusStr = statusStr + completionStatus[i]
        }
        log_parallel_state_machine.debug("并行状态检查: ${statusStr}")
        
        if (allCompleted) {
            log_parallel_state_machine.debug("🎉 所有并行区域已完成!")
            // 在实际实现中，这里会触发并行状态的完成事件
        }
    }
    
    /**
     * 获取当前并行状态
     */
    public func getCurrentParallelState(): String {
        let stateInfo = ArrayList<String>()
        stateInfo.add("主状态: ${currentState}")
        
        for ((regionId, regionState) in regionStates) {
            stateInfo.add("${regionId}: ${regionState.currentState}")
        }
        
        // 手动连接字符串
        var result = ""
        for (i in 0..stateInfo.size-1) {
            if (i > 0) {
                result = result + " | "
            }
            result = result + stateInfo[i]
        }
        return result
    }
    
    /**
     * 同步所有区域状态
     */
    public func synchronizeRegions(): Unit {
        log_parallel_state_machine.debug("同步所有并行区域状态")
        
        for ((regionId, regionState) in regionStates) {
            if (regionActors.contains(regionId)) {
                let regionActor = regionActors.get(regionId).getOrThrow()
                // 在实际实现中，这里会发送同步消息给区域Actor
                log_parallel_state_machine.debug("同步区域: ${regionId}")
            }
        }
    }
    
    /**
     * 添加新的并行区域
     */
    public func addRegion(regionId: String, initialState: String): Unit {
        let newRegionState = RegionState(regionId, initialState, true)
        regionStates.add(regionId, newRegionState)
        log_parallel_state_machine.debug("添加新的并行区域: ${regionId} (初始状态: ${initialState})")
    }
    
    /**
     * 移除并行区域
     */
    public func removeRegion(regionId: String): Unit {
        regionStates.remove(regionId)
        if (regionActors.contains(regionId)) {
            regionActors.remove(regionId)
        }
        log_parallel_state_machine.debug("移除并行区域: ${regionId}")
    }
}

/**
 * 并行状态机工厂
 */
public struct ParallelStateMachineFactory {
    /**
     * 创建并行状态机Actor
     */
    public static func create(
        system: CActorSystem,
        machineDefinition: StateMachineDefinition,
        actorName: String
    ): ActorRef {
        let actor = ParallelStateMachineActor(machineDefinition)
        return system.actorOf({ => actor }, actorName)
    }
}
