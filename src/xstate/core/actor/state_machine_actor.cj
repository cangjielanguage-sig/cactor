/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 状态机Actor
 * 基于CActor框架的状态机Actor实现
 */
package cactor.xstate.core.actor

import std.collection.*
import std.time.*
import cactor.api.{CActor, CActorRef, CActorSystem}
import cactor.core.message.{Message}
import cactor.core.actor.{Actor, ActorContext}
import cactor.xstate.core.types.{StateMachineState, StateMachineEvent, TransitionResult}
import cactor.xstate.core.machine.{StateMachineDefinition}
import cactor.xstate.core.engine.{PatternTransitionEngine, TransitionExecutionResult}

/**
 * 状态机消息类型
 */
public enum StateMachineMessage <: Message {
    | ProcessEvent(event: StateMachineEvent)
    | GetCurrentState
    | GetStatistics
    | Reset
    
    public func messageType(): String {
        match (this) {
            case ProcessEvent(_) => "ProcessEvent"
            case GetCurrentState => "GetCurrentState"
            case GetStatistics => "GetStatistics"
            case Reset => "Reset"
        }
    }
    
    public func priority(): Int32 {
        match (this) {
            case ProcessEvent(_) => 0
            case GetCurrentState => 1
            case GetStatistics => 2
            case Reset => 10
        }
    }
    
    public func messageId(): String {
        "sm-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
    }
    
    public func timestamp(): Int64 {
        DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
}

/**
 * 状态机响应消息
 */
public enum StateMachineResponse <: Message {
    | StateChanged(oldState: String, newState: String, executionTime: Int64)
    | CurrentState(stateId: String)
    | Statistics(stats: StateMachineStatistics)
    | TransitionFailed(reason: String)
    | ResetComplete
    
    public func messageType(): String {
        match (this) {
            case StateChanged(_, _, _) => "StateChanged"
            case CurrentState(_) => "CurrentState"
            case Statistics(_) => "Statistics"
            case TransitionFailed(_) => "TransitionFailed"
            case ResetComplete => "ResetComplete"
        }
    }
    
    public func priority(): Int32 { 0 }
    
    public func messageId(): String {
        "sm-response-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
    }
    
    public func timestamp(): Int64 {
        DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
}

/**
 * 状态机Actor实现
 * 集成CActor框架的状态机Actor
 */
public class StateMachineActor <: Actor {
    private let machineDefinition: StateMachineDefinition
    private let transitionEngine: PatternTransitionEngine
    private var currentStateId: String
    private var statistics: StateMachineStatistics
    
    public init(machineDefinition: StateMachineDefinition) {
        this.machineDefinition = machineDefinition
        this.transitionEngine = PatternTransitionEngine(machineDefinition)
        this.currentStateId = machineDefinition.initialStateId
        this.statistics = StateMachineStatistics()
    }
    
    /**
     * 处理消息
     */
    public func receive(message: Message, context: ActorContext): Unit {
        match (message) {
            case msg: StateMachineMessage => handleStateMachineMessage(msg, context)
            case _ => {
                println("未知消息类型: ${message.messageType()}")
            }
        }
    }
    
    /**
     * 处理状态机消息
     */
    private mut func handleStateMachineMessage(
        message: StateMachineMessage,
        context: ActorContext
    ): Unit {
        match (message) {
            case ProcessEvent(event) => {
                handleProcessEvent(event, context)
            }
            case GetCurrentState => {
                let response = StateMachineResponse.CurrentState(currentStateId)
                context.sender().tell(response, context.self())
            }
            case GetStatistics => {
                let response = StateMachineResponse.Statistics(statistics)
                context.sender().tell(response, context.self())
            }
            case Reset => {
                handleReset(context)
            }
        }
    }
    
    /**
     * 处理事件
     */
    private mut func handleProcessEvent(
        event: StateMachineEvent,
        context: ActorContext
    ): Unit {
        let oldState = currentStateId
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        try {
            // 使用转换引擎处理事件
            let result = transitionEngine.processTransition(currentStateId, event)
            
            if (result.isSuccess()) {
                // 更新当前状态
                currentStateId = result.targetStateId
                
                // 更新统计信息
                statistics.recordSuccessfulTransition(result.executionTime)
                
                // 发送状态变更通知
                let response = StateMachineResponse.StateChanged(
                    oldState,
                    currentStateId,
                    result.executionTime
                )
                context.sender().tell(response, context.self())
                
                println("状态转换成功: ${oldState} -> ${currentStateId} (${result.executionTime}ns)")
                
            } else {
                // 记录失败的转换
                statistics.recordFailedTransition(result.executionTime)
                
                // 发送转换失败通知
                let response = StateMachineResponse.TransitionFailed(result.errorMessage)
                context.sender().tell(response, context.self())
                
                println("状态转换失败: ${result.errorMessage}")
            }
            
        } catch (e: Exception) {
            let executionTime = DateTime.now().toUnixTimeStamp().toNanoseconds() - startTime
            statistics.recordFailedTransition(executionTime)
            
            let response = StateMachineResponse.TransitionFailed("异常: ${e.message}")
            context.sender().tell(response, context.self())
            
            println("状态转换异常: ${e.message}")
        }
    }
    
    /**
     * 处理重置
     */
    private mut func handleReset(context: ActorContext): Unit {
        currentStateId = machineDefinition.initialStateId
        statistics = StateMachineStatistics()
        
        let response = StateMachineResponse.ResetComplete
        context.sender().tell(response, context.self())
        
        println("状态机已重置到初始状态: ${currentStateId}")
    }
    
    /**
     * Actor启动时调用
     */
    public func preStart(context: ActorContext): Unit {
        println("状态机Actor启动: ${machineDefinition.id}, 初始状态: ${currentStateId}")
    }
    
    /**
     * Actor停止时调用
     */
    public func postStop(context: ActorContext): Unit {
        println("状态机Actor停止: ${machineDefinition.id}")
        println("最终统计: ${statistics.getSummary()}")
    }
    
    /**
     * 获取当前状态
     */
    public func getCurrentState(): String {
        currentStateId
    }
    
    /**
     * 获取统计信息
     */
    public func getStatistics(): StateMachineStatistics {
        statistics
    }
}

/**
 * 状态机统计信息
 */
public struct StateMachineStatistics {
    private var totalTransitions: Int64
    private var successfulTransitions: Int64
    private var failedTransitions: Int64
    private var totalExecutionTime: Int64
    private var minExecutionTime: Int64
    private var maxExecutionTime: Int64
    
    public init() {
        this.totalTransitions = 0
        this.successfulTransitions = 0
        this.failedTransitions = 0
        this.totalExecutionTime = 0
        this.minExecutionTime = Int64.MAX_VALUE
        this.maxExecutionTime = 0
    }
    
    /**
     * 记录成功的转换
     */
    public mut func recordSuccessfulTransition(executionTime: Int64): Unit {
        totalTransitions += 1
        successfulTransitions += 1
        totalExecutionTime += executionTime
        
        if (executionTime < minExecutionTime) {
            minExecutionTime = executionTime
        }
        if (executionTime > maxExecutionTime) {
            maxExecutionTime = executionTime
        }
    }
    
    /**
     * 记录失败的转换
     */
    public mut func recordFailedTransition(executionTime: Int64): Unit {
        totalTransitions += 1
        failedTransitions += 1
        totalExecutionTime += executionTime
    }
    
    /**
     * 获取成功率
     */
    public func getSuccessRate(): Float64 {
        if (totalTransitions == 0) {
            return 0.0
        }
        return successfulTransitions.toFloat64() / totalTransitions.toFloat64()
    }
    
    /**
     * 获取平均执行时间
     */
    public func getAverageExecutionTime(): Int64 {
        if (totalTransitions == 0) {
            return 0
        }
        return totalExecutionTime / totalTransitions
    }
    
    /**
     * 获取统计摘要
     */
    public func getSummary(): String {
        "总转换: ${totalTransitions}, 成功: ${successfulTransitions}, 失败: ${failedTransitions}, " +
        "成功率: ${(getSuccessRate() * 100).toInt32()}%, " +
        "平均执行时间: ${getAverageExecutionTime()}ns"
    }
    
    /**
     * 获取详细统计信息
     */
    public func getDetailedStats(): HashMap<String, Any> {
        let stats = HashMap<String, Any>()
        stats.put("totalTransitions", totalTransitions)
        stats.put("successfulTransitions", successfulTransitions)
        stats.put("failedTransitions", failedTransitions)
        stats.put("successRate", getSuccessRate())
        stats.put("averageExecutionTime", getAverageExecutionTime())
        stats.put("minExecutionTime", minExecutionTime)
        stats.put("maxExecutionTime", maxExecutionTime)
        return stats
    }
}

/**
 * 状态机Actor工厂
 */
public struct StateMachineActorFactory {
    /**
     * 创建状态机Actor
     */
    public static func create(
        system: CActorSystem,
        machineDefinition: StateMachineDefinition,
        name: String
    ): CActorRef {
        let actor = StateMachineActor(machineDefinition)
        return system.actorOf(actor, name)
    }
    
    /**
     * 创建状态机Actor（自动生成名称）
     */
    public static func create(
        system: CActorSystem,
        machineDefinition: StateMachineDefinition
    ): CActorRef {
        let name = "state-machine-${machineDefinition.id}-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
        return create(system, machineDefinition, name)
    }
}
