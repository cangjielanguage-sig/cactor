/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 分布式状态同步系统
 * 基于CActor集群的分布式状态机同步
 */
package cactor.xstate.enterprise.distributed

import std.collection.*
import std.time.*
import std.sync.*
import cactor.api.{CActor, CActorSystem}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.context.{ActorContext}
import cactor.core.message.{Message, MessageResult}
import cactor.distribution.cluster.{SimpleClusterManager, ClusterNode, NodeId}

/**
 * 分布式状态同步消息
 */
public class StateSync <: Message {
    public let stateMachineId: String
    public let currentState: String
    public let version: Int64
    public let eventTimestamp: Int64
    public let nodeId: String
    
    public init(
        stateMachineId: String,
        currentState: String,
        version: Int64,
        eventTimestamp: Int64,
        nodeId: String
    ) {
        this.stateMachineId = stateMachineId
        this.currentState = currentState
        this.version = version
        this.eventTimestamp = eventTimestamp
        this.nodeId = nodeId
    }
    
    public func getType(): String { "StateSync" }
    public func getPayload(): String { 
        "StateSync(${stateMachineId}, ${currentState}, v${version})"
    }
    public func messageType(): String { "StateSync" }
}

/**
 * 状态冲突解决消息
 */
public class StateConflictResolution <: Message {
    public let stateMachineId: String
    public let conflictingStates: Array<String>
    public let resolvedState: String
    public let resolutionStrategy: String
    
    public init(
        stateMachineId: String,
        conflictingStates: Array<String>,
        resolvedState: String,
        resolutionStrategy: String
    ) {
        this.stateMachineId = stateMachineId
        this.conflictingStates = conflictingStates
        this.resolvedState = resolvedState
        this.resolutionStrategy = resolutionStrategy
    }
    
    public func getType(): String { "StateConflictResolution" }
    public func getPayload(): String { 
        "ConflictResolution(${stateMachineId}, ${resolvedState})" 
    }
    public func messageType(): String { "StateConflictResolution" }
}

/**
 * 分布式状态信息
 */
public struct DistributedStateInfo {
    public let stateMachineId: String
    public let currentState: String
    public let version: Int64
    public let lastUpdated: Int64
    public let nodeId: String
    public let isConsistent: Bool
    
    public init(
        stateMachineId: String,
        currentState: String,
        version: Int64,
        lastUpdated: Int64,
        nodeId: String,
        isConsistent: Bool
    ) {
        this.stateMachineId = stateMachineId
        this.currentState = currentState
        this.version = version
        this.lastUpdated = lastUpdated
        this.nodeId = nodeId
        this.isConsistent = isConsistent
    }
}

/**
 * 冲突解决策略
 */
public enum ConflictResolutionStrategy {
    | LastWriterWins      // 最后写入者获胜
    | HighestVersion      // 最高版本获胜
    | NodePriority        // 节点优先级
    | CustomResolver      // 自定义解决器

    public func toString(): String {
        match (this) {
            case LastWriterWins => "LastWriterWins"
            case HighestVersion => "HighestVersion"
            case NodePriority => "NodePriority"
            case CustomResolver => "CustomResolver"
        }
    }
}

/**
 * 分布式状态同步管理器
 */
public class DistributedStateSyncManager <: Actor {
    private let clusterSupport: Option<SimpleClusterManager>
    private let localStates: HashMap<String, DistributedStateInfo>
    private let remoteStates: HashMap<String, HashMap<String, DistributedStateInfo>>
    private let conflictResolver: ConflictResolutionStrategy
    private let syncInterval: Duration
    private let mutex: Mutex
    private var lastSyncTime: Int64
    
    public init(
        clusterSupport: Option<SimpleClusterManager>,
        conflictResolver: ConflictResolutionStrategy,
        syncInterval: Duration
    ) {
        this.clusterSupport = clusterSupport
        this.localStates = HashMap<String, DistributedStateInfo>()
        this.remoteStates = HashMap<String, HashMap<String, DistributedStateInfo>>()
        this.conflictResolver = conflictResolver
        this.syncInterval = syncInterval
        this.mutex = Mutex()
        this.lastSyncTime = 0
    }
    
    /**
     * Actor名称
     */
    public prop name: String {
        get() { "DistributedStateSyncManager" }
    }
    
    /**
     * Actor描述
     */
    public prop description: String {
        get() { "分布式状态同步管理器" }
    }
    
    /**
     * 处理消息
     */
    public func receive(message: Message, context: ActorContext): MessageResult {
        if (message.messageType() == "StateSync") {
            return handleStateSync(message, context)
        } else if (message.messageType() == "StateConflictResolution") {
            return handleConflictResolution(message, context)
        } else if (message.messageType() == "SyncRequest") {
            return handleSyncRequest(message, context)
        } else if (message.messageType() == "PeriodicSync") {
            return handlePeriodicSync(message, context)
        }
        
        return MessageResult.Unhandled
    }
    
    /**
     * 处理状态同步消息
     */
    private func handleStateSync(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case syncMessage: StateSync =>
                mutex.lock()

                // 更新远程状态信息
                updateRemoteState(syncMessage)

                // 检查是否有冲突
                let conflicts = detectConflicts(syncMessage.stateMachineId)
                if (conflicts.size > 0) {
                    resolveConflicts(syncMessage.stateMachineId, conflicts, context)
                }

                mutex.unlock()

                println("处理状态同步: ${syncMessage.stateMachineId} -> ${syncMessage.currentState}")
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    /**
     * 处理冲突解决消息
     */
    private func handleConflictResolution(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case resolutionMessage: StateConflictResolution =>
                mutex.lock()

                // 应用冲突解决结果
                applyConflictResolution(resolutionMessage)

                mutex.unlock()

                println("应用冲突解决: ${resolutionMessage.stateMachineId} -> ${resolutionMessage.resolvedState}")
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    /**
     * 处理同步请求
     */
    private func handleSyncRequest(message: Message, context: ActorContext): MessageResult {
        println("处理同步请求")
        
        mutex.lock()
        
        // 广播本地状态到集群
        broadcastLocalStates(context)
        
        mutex.unlock()
        
        return MessageResult.Handled
    }
    
    /**
     * 处理定期同步
     */
    private func handlePeriodicSync(message: Message, context: ActorContext): MessageResult {
        let currentTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        if (currentTime - lastSyncTime >= syncInterval.toNanoseconds()) {
            performPeriodicSync(context)
            lastSyncTime = currentTime
        }
        
        return MessageResult.Handled
    }
    
    /**
     * 更新本地状态
     */
    public func updateLocalState(
        stateMachineId: String,
        currentState: String,
        version: Int64
    ): Unit {
        mutex.lock()
        
        let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let nodeId = getLocalNodeId()
        
        let stateInfo = DistributedStateInfo(
            stateMachineId,
            currentState,
            version,
            timestamp,
            nodeId,
            true
        )
        
        localStates.add(stateMachineId, stateInfo)
        
        mutex.unlock()
        
        println("更新本地状态: ${stateMachineId} -> ${currentState} (v${version})")
    }
    
    /**
     * 更新远程状态
     */
    private func updateRemoteState(syncMessage: StateSync): Unit {
        let nodeStatesOption = remoteStates.get(syncMessage.nodeId)
        let nodeStates = if (nodeStatesOption.isSome()) {
            nodeStatesOption.getOrThrow()
        } else {
            let newNodeStates = HashMap<String, DistributedStateInfo>()
            remoteStates.add(syncMessage.nodeId, newNodeStates)
            newNodeStates
        }
        
        let stateInfo = DistributedStateInfo(
            syncMessage.stateMachineId,
            syncMessage.currentState,
            syncMessage.version,
            syncMessage.eventTimestamp,
            syncMessage.nodeId,
            true
        )
        
        nodeStates.add(syncMessage.stateMachineId, stateInfo)
    }
    
    /**
     * 检测冲突
     */
    private func detectConflicts(stateMachineId: String): Array<DistributedStateInfo> {
        let conflicts = ArrayList<DistributedStateInfo>()
        
        // 获取本地状态
        let localStateOption = localStates.get(stateMachineId)
        if (localStateOption.isNone()) {
            return conflicts.toArray()
        }
        
        let localState = localStateOption.getOrThrow()
        
        // 检查与远程状态的冲突
        for ((nodeId, nodeStates) in remoteStates) {
            let remoteStateOption = nodeStates.get(stateMachineId)
            if (remoteStateOption.isSome()) {
                let remoteState = remoteStateOption.getOrThrow()
                
                // 检查状态不一致
                if (localState.currentState != remoteState.currentState) {
                    conflicts.add(remoteState)
                }
            }
        }
        
        return conflicts.toArray()
    }
    
    /**
     * 解决冲突
     */
    private func resolveConflicts(
        stateMachineId: String,
        conflicts: Array<DistributedStateInfo>,
        context: ActorContext
    ): Unit {
        println("解决状态冲突: ${stateMachineId}, ${conflicts.size} 个冲突")
        
        // 根据策略解决冲突
        let resolvedState = match (conflictResolver) {
            case ConflictResolutionStrategy.LastWriterWins => resolveByLastWriter(stateMachineId, conflicts)
            case ConflictResolutionStrategy.HighestVersion => resolveByHighestVersion(stateMachineId, conflicts)
            case ConflictResolutionStrategy.NodePriority => resolveByNodePriority(stateMachineId, conflicts)
            case ConflictResolutionStrategy.CustomResolver => resolveByCustomLogic(stateMachineId, conflicts)
        }
        
        // 广播解决结果
        broadcastConflictResolution(stateMachineId, conflicts, resolvedState, context)
    }
    
    /**
     * 按最后写入者解决冲突
     */
    private func resolveByLastWriter(stateMachineId: String, conflicts: Array<DistributedStateInfo>): String {
        var latestState = ""
        var latestTime = Int64(0)
        
        // 包含本地状态
        let localStateOption = localStates.get(stateMachineId)
        if (localStateOption.isSome()) {
            let localState = localStateOption.getOrThrow()
            if (localState.lastUpdated > latestTime) {
                latestTime = localState.lastUpdated
                latestState = localState.currentState
            }
        }
        
        // 检查冲突状态
        for (conflict in conflicts) {
            if (conflict.lastUpdated > latestTime) {
                latestTime = conflict.lastUpdated
                latestState = conflict.currentState
            }
        }
        
        return latestState
    }
    
    /**
     * 按最高版本解决冲突
     */
    private func resolveByHighestVersion(stateMachineId: String, conflicts: Array<DistributedStateInfo>): String {
        var highestState = ""
        var highestVersion = Int64(0)
        
        // 包含本地状态
        let localStateOption = localStates.get(stateMachineId)
        if (localStateOption.isSome()) {
            let localState = localStateOption.getOrThrow()
            if (localState.version > highestVersion) {
                highestVersion = localState.version
                highestState = localState.currentState
            }
        }
        
        // 检查冲突状态
        for (conflict in conflicts) {
            if (conflict.version > highestVersion) {
                highestVersion = conflict.version
                highestState = conflict.currentState
            }
        }
        
        return highestState
    }
    
    /**
     * 按节点优先级解决冲突
     */
    private func resolveByNodePriority(stateMachineId: String, conflicts: Array<DistributedStateInfo>): String {
        // 简化实现：选择节点ID最小的
        var priorityState = ""
        var priorityNodeId = "zzz"  // 最大值
        
        // 包含本地状态
        let localStateOption = localStates.get(stateMachineId)
        if (localStateOption.isSome()) {
            let localState = localStateOption.getOrThrow()
            if (localState.nodeId < priorityNodeId) {
                priorityNodeId = localState.nodeId
                priorityState = localState.currentState
            }
        }
        
        // 检查冲突状态
        for (conflict in conflicts) {
            if (conflict.nodeId < priorityNodeId) {
                priorityNodeId = conflict.nodeId
                priorityState = conflict.currentState
            }
        }
        
        return priorityState
    }
    
    /**
     * 自定义冲突解决逻辑
     */
    private func resolveByCustomLogic(stateMachineId: String, conflicts: Array<DistributedStateInfo>): String {
        // 简化实现：使用最后写入者策略
        return resolveByLastWriter(stateMachineId, conflicts)
    }
    
    /**
     * 广播冲突解决结果
     */
    private func broadcastConflictResolution(
        stateMachineId: String,
        conflicts: Array<DistributedStateInfo>,
        resolvedState: String,
        context: ActorContext
    ): Unit {
        let conflictingStates = ArrayList<String>()
        for (conflict in conflicts) {
            conflictingStates.add(conflict.currentState)
        }
        
        let resolutionMessage = StateConflictResolution(
            stateMachineId,
            conflictingStates.toArray(),
            resolvedState,
            conflictResolver.toString()
        )
        
        // 在实际实现中，这里会通过集群广播消息
        println("广播冲突解决结果: ${stateMachineId} -> ${resolvedState}")
    }
    
    /**
     * 应用冲突解决结果
     */
    private func applyConflictResolution(resolutionMessage: StateConflictResolution): Unit {
        // 更新本地状态为解决后的状态
        let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let nodeId = getLocalNodeId()
        
        let resolvedStateInfo = DistributedStateInfo(
            resolutionMessage.stateMachineId,
            resolutionMessage.resolvedState,
            timestamp,  // 使用时间戳作为版本
            timestamp,
            nodeId,
            true
        )
        
        localStates.add(resolutionMessage.stateMachineId, resolvedStateInfo)
    }
    
    /**
     * 广播本地状态
     */
    private func broadcastLocalStates(context: ActorContext): Unit {
        for ((stateMachineId, stateInfo) in localStates) {
            let syncMessage = StateSync(
                stateInfo.stateMachineId,
                stateInfo.currentState,
                stateInfo.version,
                stateInfo.lastUpdated,
                stateInfo.nodeId
            )
            
            // 在实际实现中，这里会通过集群广播消息
            println("广播本地状态: ${stateMachineId} -> ${stateInfo.currentState}")
        }
    }
    
    /**
     * 执行定期同步
     */
    private func performPeriodicSync(context: ActorContext): Unit {
        println("执行定期状态同步")
        broadcastLocalStates(context)
    }
    
    /**
     * 获取本地节点ID
     */
    private func getLocalNodeId(): String {
        if (clusterSupport.isSome()) {
            // 在实际实现中，这里会从集群支持获取节点ID
            return "node-local"
        }
        return "standalone"
    }
}
