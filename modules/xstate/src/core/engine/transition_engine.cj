/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 转换引擎
 * 基于仓颉0.60.5模式匹配的高性能转换引擎
 */
package cactor_xstate.core.engine

import std.collection.*
import std.time.*
import cactor_xstate.core.types.{StateMachineEvent, TransitionDefinition, TransitionResult, TransitionEffect}
import cactor_xstate.core.machine.{StateMachineDefinition}
import cactor_foundation.logging.{Logger, Logging}

let log_transition_engine: Logger = Logging.logger("cactor_xstate.core.engine.transition_engine")

/**
 * 模式匹配转换引擎
 * 利用仓颉的模式匹配特性实现高效的状态转换
 */
public struct PatternTransitionEngine {
    private let machineDefinition: StateMachineDefinition
    private let transitionCache: HashMap<String, Array<TransitionDefinition>>
    
    public init(machineDefinition: StateMachineDefinition) {
        this.machineDefinition = machineDefinition
        this.transitionCache = HashMap<String, Array<TransitionDefinition>>()
        buildTransitionCache()
    }
    
    /**
     * 构建转换缓存以提高查找性能
     */
    private mut func buildTransitionCache(): Unit {
        for (transition in machineDefinition.transitions) {
            let key = "${transition.sourceStateId}:${transition.eventType}"

            if (transitionCache.contains(key)) {
                let existingTransitions = transitionCache.get(key).getOrThrow()
                let updatedTransitions = ArrayList<TransitionDefinition>()
                for (t in existingTransitions) {
                    updatedTransitions.add(t)
                }
                updatedTransitions.add(transition)
                transitionCache.add(key, updatedTransitions.toArray())
            } else {
                transitionCache.add(key, [transition])
            }
        }
    }
    
    /**
     * 处理状态转换
     * 使用模式匹配进行高效的转换处理
     */
    public func processTransition(
        currentStateId: String,
        event: StateMachineEvent
    ): TransitionExecutionResult {
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        // 查找匹配的转换
        let transitions = findMatchingTransitions(currentStateId, event.getEventType())
        
        if (transitions.isEmpty()) {
            return TransitionExecutionResult.createInvalid(
                "没有找到从状态'${currentStateId}'处理事件'${event.getEventType()}'的转换",
                startTime
            )
        }
        
        // 选择最高优先级的转换
        let selectedTransition = selectBestTransition(transitions)
        
        // 执行转换
        let result = executeTransition(selectedTransition, currentStateId, event, startTime)
        
        return result
    }
    
    /**
     * 查找匹配的转换
     */
    private func findMatchingTransitions(
        currentStateId: String,
        eventType: String
    ): Array<TransitionDefinition> {
        let key = "${currentStateId}:${eventType}"
        
        match (transitionCache.get(key)) {
            case Some(transitions) => transitions
            case None => Array<TransitionDefinition>()
        }
    }
    
    /**
     * 选择最佳转换（基于优先级）
     */
    private func selectBestTransition(
        transitions: Array<TransitionDefinition>
    ): TransitionDefinition {
        var bestTransition = transitions[0]
        
        for (i in 1..transitions.size) {
            let transition = transitions[i]
            if (transition.priority > bestTransition.priority) {
                bestTransition = transition
            }
        }
        
        return bestTransition
    }
    
    /**
     * 执行转换
     */
    private func executeTransition(
        transition: TransitionDefinition,
        currentStateId: String,
        event: StateMachineEvent,
        startTime: Int64
    ): TransitionExecutionResult {
        try {
            // 验证转换的有效性
            if (!validateTransition(transition, currentStateId, event)) {
                return TransitionExecutionResult.createInvalid(
                    "转换验证失败",
                    startTime
                )
            }
            
            // 执行退出动作
            executeExitActions(currentStateId)
            
            // 执行转换动作
            executeTransitionActions(transition, event)
            
            // 执行进入动作
            executeEntryActions(transition.targetStateId)
            
            // 生成副作用
            let effects = generateEffects(transition, currentStateId, event)
            
            let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
            
            return TransitionExecutionResult.createSuccess(
                transition.targetStateId,
                effects,
                endTime - startTime
            )
            
        } catch (e: Exception) {
            return TransitionExecutionResult.createError(
                "转换执行失败: ${e.message}",
                startTime
            )
        }
    }
    
    /**
     * 验证转换
     */
    private func validateTransition(
        transition: TransitionDefinition,
        currentStateId: String,
        event: StateMachineEvent
    ): Bool {
        // 验证源状态
        if (transition.sourceStateId != currentStateId) {
            return false
        }
        
        // 验证事件类型
        if (!event.matchesType(transition.eventType)) {
            return false
        }
        
        // 验证目标状态存在
        if (!machineDefinition.hasState(transition.targetStateId)) {
            return false
        }
        
        return true
    }
    
    /**
     * 执行退出动作
     */
    private func executeExitActions(stateId: String): Unit {
        let stateOption = machineDefinition.getState(stateId)
        if (stateOption.isSome()) {
            let state = stateOption.getOrThrow()
            if (state.hasExitActions()) {
                for (action in state.onExit) {
                    log_transition_engine.debug("执行退出动作: ${action}")
                }
            }
        } else {
            log_transition_engine.warn("警告: 状态'${stateId}'不存在")
        }
    }
    
    /**
     * 执行转换动作
     */
    private func executeTransitionActions(
        transition: TransitionDefinition,
        event: StateMachineEvent
    ): Unit {
        // 这里可以根据转换的元数据执行相应的动作
        log_transition_engine.debug("执行转换动作: ${transition.getDescription()}")
    }
    
    /**
     * 执行进入动作
     */
    private func executeEntryActions(stateId: String): Unit {
        let stateOption = machineDefinition.getState(stateId)
        if (stateOption.isSome()) {
            let state = stateOption.getOrThrow()
            if (state.hasEntryActions()) {
                for (action in state.onEntry) {
                    log_transition_engine.debug("执行进入动作: ${action}")
                }
            }
        } else {
            log_transition_engine.warn("警告: 状态'${stateId}'不存在")
        }
    }
    
    /**
     * 生成副作用
     */
    private func generateEffects(
        transition: TransitionDefinition,
        sourceStateId: String,
        event: StateMachineEvent
    ): Array<TransitionEffect> {
        let effects = ArrayList<TransitionEffect>()
        
        // 添加日志副作用
        effects.add(TransitionEffect.LogEffect)
        
        return effects.toArray()
    }
    
    /**
     * 获取引擎统计信息
     */
    public func getStatistics(): EngineStatistics {
        EngineStatistics(
            machineDefinition.getTransitionCount(),
            Int32(transitionCache.size),
            Int32(machineDefinition.getStateIds().size)
        )
    }
}

/**
 * 转换执行结果
 */
public struct TransitionExecutionResult {
    public let result: TransitionResult
    public let targetStateId: String
    public let effects: Array<TransitionEffect>
    public let executionTime: Int64
    public let errorMessage: String
    
    private init(
        result: TransitionResult,
        targetStateId: String,
        effects: Array<TransitionEffect>,
        executionTime: Int64,
        errorMessage: String
    ) {
        this.result = result
        this.targetStateId = targetStateId
        this.effects = effects
        this.executionTime = executionTime
        this.errorMessage = errorMessage
    }
    
    /**
     * 创建成功结果
     */
    public static func createSuccess(
        targetStateId: String,
        effects: Array<TransitionEffect>,
        executionTime: Int64
    ): TransitionExecutionResult {
        TransitionExecutionResult(
            TransitionResult.Success,
            targetStateId,
            effects,
            executionTime,
            ""
        )
    }
    
    /**
     * 创建无效结果
     */
    public static func createInvalid(
        errorMessage: String,
        startTime: Int64
    ): TransitionExecutionResult {
        let executionTime = DateTime.now().toUnixTimeStamp().toNanoseconds() - startTime
        TransitionExecutionResult(
            TransitionResult.Invalid,
            "",
            Array<TransitionEffect>(),
            executionTime,
            errorMessage
        )
    }
    
    /**
     * 创建错误结果
     */
    public static func createError(
        errorMessage: String,
        startTime: Int64
    ): TransitionExecutionResult {
        let executionTime = DateTime.now().toUnixTimeStamp().toNanoseconds() - startTime
        TransitionExecutionResult(
            TransitionResult.Forbidden,
            "",
            Array<TransitionEffect>(),
            executionTime,
            errorMessage
        )
    }
    
    /**
     * 检查是否成功
     */
    public func isSuccess(): Bool {
        result.isSuccess()
    }
    
    /**
     * 检查是否失败
     */
    public func isFailure(): Bool {
        result.isFailure()
    }
}

/**
 * 引擎统计信息
 */
public struct EngineStatistics {
    public let transitionCount: Int32
    public let cacheSize: Int32
    public let stateCount: Int32
    
    public init(transitionCount: Int32, cacheSize: Int32, stateCount: Int32) {
        this.transitionCount = transitionCount
        this.cacheSize = cacheSize
        this.stateCount = stateCount
    }
    
    /**
     * 获取缓存命中率（简化计算）
     */
    public func getCacheHitRatio(): Float64 {
        if (transitionCount == 0) {
            return 0.0
        }
        return Float64(cacheSize) / Float64(transitionCount)
    }
}
