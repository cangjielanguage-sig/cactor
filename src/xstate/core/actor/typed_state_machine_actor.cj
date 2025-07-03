/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 基于CActor TypedBehavior的类型安全状态机Actor
 * 充分利用CActor的类型安全Actor系统和仓颉语言特性
 */
package cactor.xstate.core.actor

import std.collection.*
import std.time.*
import cactor.core.typed.{TypedBehavior, TypedActorContext}
import cactor.core.message.{Message}
import cactor.xstate.core.types.{StateMachineState, StateMachineEvent, TransitionResult, TransitionEffect}
import cactor.xstate.core.machine.{StateMachineDefinition}
import cactor.xstate.core.engine.{PatternTransitionEngine}

/**
 * 状态机消息类型
 * 定义状态机Actor可以处理的消息类型
 */
public enum StateMachineMessage<TEvent> where TEvent <: StateMachineEvent {
    | EventMessage(event: TEvent)
    | QueryStateMessage
    | SnapshotMessage
    | RestoreMessage(snapshot: StateMachineSnapshot)
    | PauseMessage
    | ResumeMessage
    | StopMessage
    
    /**
     * 获取消息类型标识
     */
    public func getMessageType(): String {
        match (this) {
            case EventMessage(_) => "event"
            case QueryStateMessage => "query_state"
            case SnapshotMessage => "snapshot"
            case RestoreMessage(_) => "restore"
            case PauseMessage => "pause"
            case ResumeMessage => "resume"
            case StopMessage => "stop"
        }
    }
}

/**
 * 状态机快照
 * 用于状态机的持久化和恢复
 */
public struct StateMachineSnapshot {
    public let machineId: String
    public let currentStateId: String
    public let contextData: String  // 序列化的上下文数据
    public let timestamp: Int64
    public let version: String
    
    public init(
        machineId: String,
        currentStateId: String,
        contextData: String,
        version: String = "1.0.0"
    ) {
        this.machineId = machineId
        this.currentStateId = currentStateId
        this.contextData = contextData
        this.timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.version = version
    }
}

/**
 * 状态响应消息
 * 用于响应状态查询
 */
public struct StateResponseMessage<TState, TContext> 
where TState <: StateMachineState, TContext <: Equatable {
    public let currentState: TState
    public let context: TContext
    public let timestamp: Int64
    
    public init(currentState: TState, context: TContext) {
        this.currentState = currentState
        this.context = context
        this.timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
}

/**
 * 类型安全的状态机Actor
 * 基于CActor的TypedBehavior实现，提供完整的状态机功能
 */
public class TypedStateMachineActor<TState, TEvent, TContext> <: TypedBehavior<StateMachineMessage<TEvent>>
where TState <: StateMachineState, TEvent <: StateMachineEvent, TContext <: Equatable {
    
    private let machineDefinition: StateMachineDefinition<TState, TEvent, TContext>
    private let transitionEngine: PatternTransitionEngine<TState, TEvent, TContext>
    private var currentState: TState
    private var context: TContext
    private var isPaused: Bool = false
    private var eventQueue: ArrayList<TEvent> = ArrayList()
    private var statistics: StateMachineStatistics = StateMachineStatistics()
    
    public init(
        machineDefinition: StateMachineDefinition<TState, TEvent, TContext>
    ) {
        this.machineDefinition = machineDefinition
        this.transitionEngine = PatternTransitionEngine(machineDefinition)
        this.currentState = machineDefinition.initialState
        this.context = machineDefinition.initialContext
    }
    
    /**
     * 处理接收到的消息
     * 实现TypedBehavior接口
     */
    public func onMessage(
        message: StateMachineMessage<TEvent>,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): TypedBehavior<StateMachineMessage<TEvent>> {
        
        statistics.incrementMessageCount()
        
        match (message) {
            case EventMessage(event) =>
                handleEvent(event, actorContext)
            case QueryStateMessage =>
                handleStateQuery(actorContext)
            case SnapshotMessage =>
                handleSnapshot(actorContext)
            case RestoreMessage(snapshot) =>
                handleRestore(snapshot, actorContext)
            case PauseMessage =>
                handlePause(actorContext)
            case ResumeMessage =>
                handleResume(actorContext)
            case StopMessage =>
                handleStop(actorContext)
        }
        
        return this
    }
    
    /**
     * 处理事件消息
     */
    private func handleEvent(
        event: TEvent,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        
        if (isPaused) {
            // 如果暂停，将事件加入队列
            eventQueue.add(event)
            return
        }
        
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        try {
            // 执行状态转换
            let transitionResult = transitionEngine.processTransition(currentState, event, context)
            
            match (transitionResult) {
                case TransitionResult.Success(newState, newContext, effects) =>
                    handleSuccessfulTransition(newState, newContext, effects, event, actorContext)
                case TransitionResult.Invalid(reason) =>
                    handleInvalidTransition(reason, event, actorContext)
                case TransitionResult.Deferred =>
                    handleDeferredTransition(event, actorContext)
                case TransitionResult.Forbidden(reason) =>
                    handleForbiddenTransition(reason, event, actorContext)
            }
            
        } catch (e: Exception) {
            handleTransitionError(e, event, actorContext)
        }
        
        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        statistics.recordTransitionTime(endTime - startTime)
    }
    
    /**
     * 处理成功的状态转换
     */
    private func handleSuccessfulTransition(
        newState: TState,
        newContext: TContext,
        effects: Array<TransitionEffect>,
        event: TEvent,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        
        let oldState = currentState
        
        // 执行退出动作
        executeExitActions(oldState, event, context, actorContext)
        
        // 更新状态和上下文
        currentState = newState
        context = newContext
        
        // 执行进入动作
        executeEntryActions(newState, event, context, actorContext)
        
        // 处理副作用
        processEffects(effects, actorContext)
        
        // 更新统计信息
        statistics.incrementTransitionCount()
        statistics.recordStateChange(oldState.getStateId(), newState.getStateId())
        
        // 记录日志
        actorContext.log().info("状态转换: ${oldState.getStateId()} -> ${newState.getStateId()}")
    }
    
    /**
     * 处理状态查询
     */
    private func handleStateQuery(
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        let response = StateResponseMessage(currentState, context)
        
        match (actorContext.sender()) {
            case Some(senderPath) =>
                // 这里需要根据CActor的实际API发送响应
                actorContext.log().info("发送状态响应到: ${senderPath}")
            case None =>
                actorContext.log().warn("无法发送状态响应：没有发送者信息")
        }
    }
    
    /**
     * 处理快照请求
     */
    private func handleSnapshot(
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        let snapshot = createSnapshot()
        
        match (actorContext.sender()) {
            case Some(senderPath) =>
                actorContext.log().info("创建快照: ${snapshot.machineId}")
            case None =>
                actorContext.log().warn("无法发送快照：没有发送者信息")
        }
    }
    
    /**
     * 处理恢复请求
     */
    private func handleRestore(
        snapshot: StateMachineSnapshot,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        try {
            restoreFromSnapshot(snapshot)
            actorContext.log().info("从快照恢复状态机: ${snapshot.machineId}")
        } catch (e: Exception) {
            actorContext.log().error("恢复失败: ${e.message}")
        }
    }
    
    /**
     * 处理暂停请求
     */
    private func handlePause(
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        isPaused = true
        actorContext.log().info("状态机已暂停")
    }
    
    /**
     * 处理恢复请求
     */
    private func handleResume(
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        isPaused = false
        
        // 处理队列中的事件
        while (!eventQueue.isEmpty()) {
            let queuedEvent = eventQueue.removeAt(0)
            handleEvent(queuedEvent, actorContext)
        }
        
        actorContext.log().info("状态机已恢复")
    }
    
    /**
     * 处理停止请求
     */
    private func handleStop(
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        actorContext.log().info("状态机正在停止")
        actorContext.stopSelf()
    }
    
    /**
     * 执行退出动作
     */
    private func executeExitActions(
        state: TState,
        event: TEvent,
        context: TContext,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        // 获取状态定义并执行退出动作
        match (machineDefinition.getState(state.getStateId())) {
            case Some(stateDefinition) =>
                match (stateDefinition.exit) {
                    case Some(exitAction) =>
                        try {
                            exitAction(context, event)
                        } catch (e: Exception) {
                            actorContext.log().error("退出动作执行失败: ${e.message}")
                        }
                    case None => ()
                }
            case None => ()
        }
    }
    
    /**
     * 执行进入动作
     */
    private func executeEntryActions(
        state: TState,
        event: TEvent,
        context: TContext,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        // 获取状态定义并执行进入动作
        match (machineDefinition.getState(state.getStateId())) {
            case Some(stateDefinition) =>
                match (stateDefinition.entry) {
                    case Some(entryAction) =>
                        try {
                            entryAction(context, event)
                        } catch (e: Exception) {
                            actorContext.log().error("进入动作执行失败: ${e.message}")
                        }
                    case None => ()
                }
            case None => ()
        }
    }
    
    /**
     * 处理副作用
     */
    private func processEffects(
        effects: Array<TransitionEffect>,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        for (effect in effects) {
            match (effect) {
                case TransitionEffect.LogEffect(message, level) =>
                    match (level) {
                        case "info" => actorContext.log().info(message)
                        case "warn" => actorContext.log().warn(message)
                        case "error" => actorContext.log().error(message)
                        case _ => actorContext.log().debug(message)
                    }
                case TransitionEffect.TimerEffect(timerId, delay) =>
                    // 这里需要集成CActor的定时器系统
                    actorContext.log().info("设置定时器: ${timerId}, 延迟: ${delay}")
                case _ =>
                    actorContext.log().debug("处理副作用: ${effect.getEffectType()}")
            }
        }
    }
    
    /**
     * 创建状态机快照
     */
    private func createSnapshot(): StateMachineSnapshot {
        // 这里需要实现上下文的序列化
        let contextData = "serialized_context_data"  // 简化实现
        
        return StateMachineSnapshot(
            machineDefinition.id,
            currentState.getStateId(),
            contextData,
            machineDefinition.version
        )
    }
    
    /**
     * 从快照恢复状态机
     */
    private func restoreFromSnapshot(snapshot: StateMachineSnapshot): Unit {
        // 这里需要实现上下文的反序列化和状态恢复
        // 简化实现，实际需要根据具体的序列化方案
        throw Exception("快照恢复功能尚未完全实现")
    }
    
    /**
     * 处理无效转换
     */
    private func handleInvalidTransition(
        reason: String,
        event: TEvent,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        statistics.incrementInvalidTransitionCount()
        actorContext.log().warn("无效转换: ${reason}, 事件: ${event.getEventType()}")
    }
    
    /**
     * 处理延迟转换
     */
    private func handleDeferredTransition(
        event: TEvent,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        eventQueue.add(event)
        actorContext.log().debug("转换被延迟，事件已加入队列: ${event.getEventType()}")
    }
    
    /**
     * 处理被禁止的转换
     */
    private func handleForbiddenTransition(
        reason: String,
        event: TEvent,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        statistics.incrementForbiddenTransitionCount()
        actorContext.log().warn("转换被禁止: ${reason}, 事件: ${event.getEventType()}")
    }
    
    /**
     * 处理转换错误
     */
    private func handleTransitionError(
        error: Exception,
        event: TEvent,
        actorContext: TypedActorContext<StateMachineMessage<TEvent>>
    ): Unit {
        statistics.incrementErrorCount()
        actorContext.log().error("转换执行错误: ${error.message}, 事件: ${event.getEventType()}")
    }
    
    /**
     * 获取行为描述
     */
    public func getDescription(): String {
        "TypedStateMachineActor[${machineDefinition.id}]"
    }
    
    /**
     * 获取当前状态
     */
    public func getCurrentState(): TState {
        currentState
    }
    
    /**
     * 获取当前上下文
     */
    public func getContext(): TContext {
        context
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
public class StateMachineStatistics {
    private var messageCount: Int64 = 0
    private var transitionCount: Int64 = 0
    private var invalidTransitionCount: Int64 = 0
    private var forbiddenTransitionCount: Int64 = 0
    private var errorCount: Int64 = 0
    private var totalTransitionTime: Int64 = 0
    private let stateChangeHistory: ArrayList<String> = ArrayList()
    
    public func incrementMessageCount(): Unit { messageCount += 1 }
    public func incrementTransitionCount(): Unit { transitionCount += 1 }
    public func incrementInvalidTransitionCount(): Unit { invalidTransitionCount += 1 }
    public func incrementForbiddenTransitionCount(): Unit { forbiddenTransitionCount += 1 }
    public func incrementErrorCount(): Unit { errorCount += 1 }
    
    public func recordTransitionTime(time: Int64): Unit {
        totalTransitionTime += time
    }
    
    public func recordStateChange(fromState: String, toState: String): Unit {
        stateChangeHistory.add("${fromState} -> ${toState}")
    }
    
    public func getMessageCount(): Int64 { messageCount }
    public func getTransitionCount(): Int64 { transitionCount }
    public func getInvalidTransitionCount(): Int64 { invalidTransitionCount }
    public func getForbiddenTransitionCount(): Int64 { forbiddenTransitionCount }
    public func getErrorCount(): Int64 { errorCount }
    public func getAverageTransitionTime(): Int64 {
        if (transitionCount > 0) { totalTransitionTime / transitionCount } else { 0 }
    }
    
    public func getStateChangeHistory(): Array<String> {
        stateChangeHistory.toArray()
    }
}
