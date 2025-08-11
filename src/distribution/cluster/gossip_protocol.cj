/*
 * Copyright (c) 2024. All rights reserved.
 * Distribution Layer - Gossip协议实现
 * 基于反熵机制的集群状态传播协议，参考Akka Cluster实现
 */
package cactor.distribution.cluster

import std.collection.*
import std.sync.*
import std.time.*
import cactor.foundation.network.{NetworkAddress, TransportProtocol}
import cactor.foundation.serialization.{Serializer, SerializationManager}
import cactor.core.actor.{ActorRef}

/**
 * Gossip消息类型
 */
public enum GossipMessageType <: ToString {
    | Gossip        // 常规Gossip消息
    | GossipStatus  // Gossip状态消息
    | GossipEnvelope // Gossip信封消息

    public func toString(): String {
        match (this) {
            case Gossip => "Gossip"
            case GossipStatus => "GossipStatus"
            case GossipEnvelope => "GossipEnvelope"
        }
    }
}

/**
 * Gossip集群节点信息
 */
public struct GossipClusterNode {
    public let nodeId: String
    public let address: NetworkAddress
    public let roles: Array<String>
    public let status: NodeStatus
    public let timestamp: Int64

    public init(nodeId: String, address: NetworkAddress, roles: Array<String>, 
                status: NodeStatus, timestamp: Int64) {
        this.nodeId = nodeId
        this.address = address
        this.roles = roles
        this.status = status
        this.timestamp = timestamp
    }

    public func withStatus(newStatus: NodeStatus): GossipClusterNode {
        GossipClusterNode(nodeId, address, roles, newStatus, getCurrentTimeNanos())
    }

    public func withTimestamp(newTimestamp: Int64): GossipClusterNode {
        GossipClusterNode(nodeId, address, roles, status, newTimestamp)
    }

    private func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds() * 1_000_000
    }
}

/**
 * 节点状态枚举
 */
public enum NodeStatus <: ToString {
    | Joining
    | Up
    | Leaving
    | Exiting
    | Down
    | Removed

    public func toString(): String {
        match (this) {
            case Joining => "Joining"
            case Up => "Up"
            case Leaving => "Leaving"
            case Exiting => "Exiting"
            case Down => "Down"
            case Removed => "Removed"
        }
    }

    public func isAvailable(): Bool {
        match (this) {
            case Up => true
            case _ => false
        }
    }
}

/**
 * Gossip状态
 */
public struct GossipState {
    public let members: HashMap<String, GossipClusterNode>
    public let overview: GossipOverview
    public let version: VectorClock

    public init(members: HashMap<String, GossipClusterNode>, overview: GossipOverview, version: VectorClock) {
        this.members = members
        this.overview = overview
        this.version = version
    }

    public static func empty(): GossipState {
        GossipState(
            HashMap<String, GossipClusterNode>(),
            GossipOverview.empty(),
            VectorClock.empty()
        )
    }

    /**
     * 添加或更新节点
     */
    public func addOrUpdateMember(node: GossipClusterNode): GossipState {
        let newMembers = HashMap<String, GossipClusterNode>()
        for ((nodeId, member) in members) {
            newMembers.add(nodeId, member)
        }
        newMembers.add(node.nodeId, node)

        let newVersion = version.increment(node.nodeId)
        let newOverview = overview  // 简化实现

        GossipState(newMembers, newOverview, newVersion)
    }

    /**
     * 移除节点
     */
    public func removeMember(nodeId: String): GossipState {
        let newMembers = HashMap<String, GossipClusterNode>()
        for ((id, member) in members) {
            if (id != nodeId) {
                newMembers.add(id, member)
            }
        }

        let newVersion = version.increment(nodeId)
        let newOverview = overview  // 简化实现

        GossipState(newMembers, newOverview, newVersion)
    }

    /**
     * 合并两个Gossip状态
     */
    public func merge(other: GossipState): GossipState {
        let mergedMembers = HashMap<String, GossipClusterNode>()

        // 合并成员信息，选择时间戳更新的版本
        for ((nodeId, member) in members) {
            mergedMembers.add(nodeId, member)
        }

        for ((nodeId, otherMember) in other.members) {
            match (mergedMembers.get(nodeId)) {
                case Some(existingMember) =>
                    if (otherMember.timestamp > existingMember.timestamp) {
                        mergedMembers.add(nodeId, otherMember)  // 使用add替代put
                    }
                case None =>
                    mergedMembers.add(nodeId, otherMember)
            }
        }

        let mergedVersion = version.merge(other.version)
        let mergedOverview = overview.merge(other.overview)

        GossipState(mergedMembers, mergedOverview, mergedVersion)
    }

    /**
     * 获取活跃节点列表
     */
    public func getActiveMembers(): Array<GossipClusterNode> {
        let activeMembers = ArrayList<GossipClusterNode>()
        for ((_, member) in members) {
            if (member.status.isAvailable()) {
                activeMembers.add(member)
            }
        }
        activeMembers.toArray()
    }
}

/**
 * Gossip概览信息
 */
public struct GossipOverview {
    public let seen: HashMap<String, VectorClock>
    public let reachability: ReachabilityMatrix

    public init(seen: HashMap<String, VectorClock>, reachability: ReachabilityMatrix) {
        this.seen = seen
        this.reachability = reachability
    }

    public static func empty(): GossipOverview {
        GossipOverview(HashMap<String, VectorClock>(), ReachabilityMatrix.empty())
    }

    public func withMember(node: ClusterNode): GossipOverview {
        let newSeen = HashMap<String, VectorClock>()
        for ((nodeId, clock) in seen) {
            newSeen.add(nodeId, clock)
        }
        
        let newReachability = reachability  // 简化实现
        GossipOverview(newSeen, newReachability)
    }

    public func withoutMember(nodeId: String): GossipOverview {
        let newSeen = HashMap<String, VectorClock>()
        for ((id, clock) in seen) {
            if (id != nodeId) {
                newSeen.add(id, clock)
            }
        }
        
        let newReachability = reachability.withoutNode(nodeId)
        GossipOverview(newSeen, newReachability)
    }

    public func merge(other: GossipOverview): GossipOverview {
        let mergedSeen = HashMap<String, VectorClock>()
        
        // 合并seen信息
        for ((nodeId, clock) in seen) {
            mergedSeen.add(nodeId, clock)
        }
        
        for ((nodeId, otherClock) in other.seen) {
            match (mergedSeen.get(nodeId)) {
                case Some(existingClock) =>
                    mergedSeen.add(nodeId, existingClock.merge(otherClock))
                case None =>
                    mergedSeen.add(nodeId, otherClock)
            }
        }
        
        let mergedReachability = reachability.merge(other.reachability)
        GossipOverview(mergedSeen, mergedReachability)
    }
}

/**
 * 向量时钟实现
 */
public struct VectorClock {
    public let versions: HashMap<String, Int64>

    public init(versions: HashMap<String, Int64>) {
        this.versions = versions
    }

    public static func empty(): VectorClock {
        VectorClock(HashMap<String, Int64>())
    }

    public func increment(nodeId: String): VectorClock {
        let newVersions = HashMap<String, Int64>()
        for ((id, version) in versions) {
            newVersions.add(id, version)
        }
        
        let currentVersion = match (versions.get(nodeId)) {
            case Some(version) => version
            case None => Int64(0)
        }
        newVersions.add(nodeId, currentVersion + 1)
        
        VectorClock(newVersions)
    }

    public func merge(other: VectorClock): VectorClock {
        let mergedVersions = HashMap<String, Int64>()
        
        // 合并版本信息，取最大值
        for ((nodeId, version) in versions) {
            mergedVersions.add(nodeId, version)
        }
        
        for ((nodeId, otherVersion) in other.versions) {
            match (mergedVersions.get(nodeId)) {
                case Some(existingVersion) =>
                    mergedVersions.add(nodeId, max(existingVersion, otherVersion))
                case None =>
                    mergedVersions.add(nodeId, otherVersion)
            }
        }
        
        VectorClock(mergedVersions)
    }

    public func compare(other: VectorClock): VectorClockOrdering {
        var thisGreater = false
        var otherGreater = false
        
        // 检查所有节点的版本
        let allNodes = HashSet<String>()
        for ((nodeId, _) in versions) {
            allNodes.add(nodeId)
        }
        for ((nodeId, _) in other.versions) {
            allNodes.add(nodeId)
        }
        
        for (nodeId in allNodes) {
            let thisVersion = match (versions.get(nodeId)) {
                case Some(version) => version
                case None => Int64(0)
            }
            let otherVersion = match (other.versions.get(nodeId)) {
                case Some(version) => version
                case None => Int64(0)
            }
            
            if (thisVersion > otherVersion) {
                thisGreater = true
            } else if (otherVersion > thisVersion) {
                otherGreater = true
            }
        }
        
        if (thisGreater && !otherGreater) {
            VectorClockOrdering.After
        } else if (otherGreater && !thisGreater) {
            VectorClockOrdering.Before
        } else if (!thisGreater && !otherGreater) {
            VectorClockOrdering.Same
        } else {
            VectorClockOrdering.Concurrent
        }
    }

    private func max(a: Int64, b: Int64): Int64 {
        if (a > b) { a } else { b }
    }
}

/**
 * 向量时钟排序枚举
 */
public enum VectorClockOrdering {
    | Before      // 此时钟在另一个之前
    | After       // 此时钟在另一个之后
    | Same        // 两个时钟相同
    | Concurrent  // 两个时钟并发
}

/**
 * 可达性矩阵
 */
public struct ReachabilityMatrix {
    public let records: HashMap<String, HashMap<String, ReachabilityRecord>>

    public init(records: HashMap<String, HashMap<String, ReachabilityRecord>>) {
        this.records = records
    }

    public static func empty(): ReachabilityMatrix {
        ReachabilityMatrix(HashMap<String, HashMap<String, ReachabilityRecord>>())
    }

    public func withNode(nodeId: String): ReachabilityMatrix {
        let newRecords = HashMap<String, HashMap<String, ReachabilityRecord>>()
        for ((fromNode, toNodes) in records) {
            newRecords.add(fromNode, toNodes)
        }
        
        match (newRecords.get(nodeId)) {
            case Some(_) => ()  // 已存在
            case None => newRecords.add(nodeId, HashMap<String, ReachabilityRecord>())
        }
        
        ReachabilityMatrix(newRecords)
    }

    public func withoutNode(nodeId: String): ReachabilityMatrix {
        let newRecords = HashMap<String, HashMap<String, ReachabilityRecord>>()
        for ((fromNode, toNodes) in records) {
            if (fromNode != nodeId) {
                let filteredToNodes = HashMap<String, ReachabilityRecord>()
                for ((toNode, record) in toNodes) {
                    if (toNode != nodeId) {
                        filteredToNodes.add(toNode, record)
                    }
                }
                newRecords.add(fromNode, filteredToNodes)
            }
        }
        
        ReachabilityMatrix(newRecords)
    }

    public func merge(other: ReachabilityMatrix): ReachabilityMatrix {
        let mergedRecords = HashMap<String, HashMap<String, ReachabilityRecord>>()
        
        // 复制当前记录
        for ((fromNode, toNodes) in records) {
            let copiedToNodes = HashMap<String, ReachabilityRecord>()
            for ((toNode, record) in toNodes) {
                copiedToNodes.add(toNode, record)
            }
            mergedRecords.add(fromNode, copiedToNodes)
        }
        
        // 合并其他记录
        for ((fromNode, otherToNodes) in other.records) {
            match (mergedRecords.get(fromNode)) {
                case Some(existingToNodes) =>
                    for ((toNode, otherRecord) in otherToNodes) {
                        match (existingToNodes.get(toNode)) {
                            case Some(existingRecord) =>
                                // 选择时间戳更新的记录
                                if (otherRecord.timestamp > existingRecord.timestamp) {
                                    existingToNodes.add(toNode, otherRecord)
                                }
                            case None =>
                                existingToNodes.add(toNode, otherRecord)
                        }
                    }
                case None =>
                    let copiedToNodes = HashMap<String, ReachabilityRecord>()
                    for ((toNode, record) in otherToNodes) {
                        copiedToNodes.add(toNode, record)
                    }
                    mergedRecords.add(fromNode, copiedToNodes)
            }
        }
        
        ReachabilityMatrix(mergedRecords)
    }

    public func isReachable(fromNode: String, toNode: String): Bool {
        match (records.get(fromNode)) {
            case Some(toNodes) =>
                match (toNodes.get(toNode)) {
                    case Some(record) =>
                        match (record.status) {
                            case ReachabilityStatus.Reachable => true
                            case _ => false
                        }
                    case None => true  // 默认可达
                }
            case None => true  // 默认可达
        }
    }
}

/**
 * 可达性记录
 */
public struct ReachabilityRecord {
    public let status: ReachabilityStatus
    public let timestamp: Int64

    public init(status: ReachabilityStatus, timestamp: Int64) {
        this.status = status
        this.timestamp = timestamp
    }
}

/**
 * 可达性状态
 */
public enum ReachabilityStatus {
    | Reachable
    | Unreachable
    | Terminated
}

/**
 * Gossip协议配置
 */
public struct GossipConfig {
    public let gossipInterval: Int64        // Gossip间隔，默认1000ms
    public let gossipTimeToLive: Int32      // Gossip TTL，默认2
    public let maxDeltaElements: Int32      // 最大增量元素数，默认3000
    public let convergenceTimeout: Int64    // 收敛超时，默认10000ms

    public init(gossipInterval: Int64, gossipTimeToLive: Int32,
                maxDeltaElements: Int32, convergenceTimeout: Int64) {
        this.gossipInterval = gossipInterval
        this.gossipTimeToLive = gossipTimeToLive
        this.maxDeltaElements = maxDeltaElements
        this.convergenceTimeout = convergenceTimeout
    }

    public static func createDefault(): GossipConfig {
        GossipConfig(1000, 2, 3000, 10000)
    }

    public static func createFast(): GossipConfig {
        GossipConfig(500, 2, 1000, 5000)
    }

    public static func createSlow(): GossipConfig {
        GossipConfig(2000, 3, 5000, 20000)
    }
}

/**
 * Gossip消息
 */
public struct GossipMessage {
    public let from: String
    public let to: String
    public let state: GossipState
    public let messageType: GossipMessageType
    public let timestamp: Int64

    public init(from: String, to: String, state: GossipState,
                messageType: GossipMessageType, timestamp: Int64) {
        this.from = from
        this.to = to
        this.state = state
        this.messageType = messageType
        this.timestamp = timestamp
    }
}

/**
 * Gossip协议管理器
 */
public class GossipProtocolManager {
    private let config: GossipConfig
    private let localNodeId: String
    private let localAddress: NetworkAddress
    private var currentState: GossipState
    private let mutex: Mutex
    private let gossipTargets: ArrayList<String>
    private let lastGossipTime: AtomicInt64
    private let isRunning: AtomicBool

    public init(config: GossipConfig, localNodeId: String, localAddress: NetworkAddress) {
        this.config = config
        this.localNodeId = localNodeId
        this.localAddress = localAddress
        this.currentState = GossipState.empty()
        this.mutex = Mutex()
        this.gossipTargets = ArrayList<String>()
        this.lastGossipTime = AtomicInt64(0)
        this.isRunning = AtomicBool(false)
    }

    /**
     * 启动Gossip协议
     */
    public func start(): Unit {
        if (isRunning.compareAndSwap(false, true)) {
            // 简化实现：不创建本地节点，直接启动

            // 简化实现：直接启动，不添加本地节点

            println("Gossip协议管理器已启动，节点ID: ${localNodeId}")
        }
    }

    /**
     * 停止Gossip协议
     */
    public func stop(): Unit {
        if (isRunning.compareAndSwap(true, false)) {
            println("Gossip协议管理器已停止")
        }
    }

    /**
     * 处理接收到的Gossip消息
     */
    public func handleGossipMessage(message: GossipMessage): Unit {
        if (!isRunning.load()) {
            return
        }

        mutex.lock()
        try {
            match (message.messageType) {
                case GossipMessageType.Gossip =>
                    // 合并接收到的状态
                    currentState = currentState.merge(message.state)

                    // 更新Gossip目标
                    updateGossipTargets()

                case GossipMessageType.GossipStatus =>
                    // 处理状态查询
                    handleGossipStatus(message)

                case GossipMessageType.GossipEnvelope =>
                    // 处理信封消息
                    handleGossipEnvelope(message)
            }
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 执行Gossip轮次
     */
    public func performGossipRound(): Unit {
        if (!isRunning.load()) {
            return
        }

        let currentTime = getCurrentTimeNanos()
        let lastTime = lastGossipTime.load()

        if (currentTime - lastTime >= config.gossipInterval * 1_000_000) {
            if (lastGossipTime.compareAndSwap(lastTime, currentTime)) {
                doGossip()
            }
        }
    }

    /**
     * 添加节点到集群
     */
    public func addNode(node: GossipClusterNode): Unit {
        mutex.lock()
        try {
            currentState = currentState.addOrUpdateMember(node)
            updateGossipTargets()
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 移除节点从集群
     */
    public func removeNode(nodeId: String): Unit {
        mutex.lock()
        try {
            currentState = currentState.removeMember(nodeId)
            updateGossipTargets()
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 获取当前集群状态
     */
    public func getCurrentState(): GossipState {
        mutex.lock()
        try {
            currentState
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 获取活跃节点列表
     */
    public func getActiveNodes(): Array<GossipClusterNode> {
        mutex.lock()
        try {
            currentState.getActiveMembers()
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 检查节点是否可达
     */
    public func isNodeReachable(nodeId: String): Bool {
        mutex.lock()
        try {
            currentState.overview.reachability.isReachable(localNodeId, nodeId)
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 执行实际的Gossip操作
     */
    private func doGossip(): Unit {
        mutex.lock()
        let targets = ArrayList<String>()
        for (target in gossipTargets) {
            targets.add(target)
        }
        let state = currentState
        mutex.unlock()

        // 选择Gossip目标并发送消息
        let selectedTargets = selectGossipTargets(targets.toArray())
        for (targetNodeId in selectedTargets) {
            let message = GossipMessage(
                localNodeId,
                targetNodeId,
                state,
                GossipMessageType.Gossip,
                getCurrentTimeNanos()
            )

            sendGossipMessage(targetNodeId, message)
        }
    }

    /**
     * 选择Gossip目标
     */
    private func selectGossipTargets(availableTargets: Array<String>): Array<String> {
        let maxTargets = min(Int32(3), Int32(availableTargets.size))  // 最多选择3个目标
        let selected = ArrayList<String>()

        // 简化的随机选择策略
        for (i in 0..maxTargets) {
            if (Int64(i) < availableTargets.size) {
                selected.add(availableTargets[Int64(i)])
            }
        }

        selected.toArray()
    }

    /**
     * 发送Gossip消息
     */
    private func sendGossipMessage(targetNodeId: String, message: GossipMessage): Unit {
        // 这里应该通过网络传输层发送消息
        // 简化实现，仅打印日志
        println("发送Gossip消息到节点 ${targetNodeId}: ${message.messageType.toString()}")
    }

    /**
     * 处理Gossip状态查询
     */
    private func handleGossipStatus(message: GossipMessage): Unit {
        // 比较版本并决定是否需要发送完整状态
        let comparison = currentState.version.compare(message.state.version)
        match (comparison) {
            case VectorClockOrdering.After =>
                // 我们的状态更新，发送完整状态
                let response = GossipMessage(
                    localNodeId,
                    message.from,
                    currentState,
                    GossipMessageType.Gossip,
                    getCurrentTimeNanos()
                )
                sendGossipMessage(message.from, response)

            case VectorClockOrdering.Before =>
                // 对方状态更新，请求完整状态
                let request = GossipMessage(
                    localNodeId,
                    message.from,
                    GossipState.empty(),
                    GossipMessageType.GossipStatus,
                    getCurrentTimeNanos()
                )
                sendGossipMessage(message.from, request)

            case _ => ()  // 相同或并发，无需操作
        }
    }

    /**
     * 处理Gossip信封消息
     */
    private func handleGossipEnvelope(message: GossipMessage): Unit {
        // 处理包装的消息
        currentState = currentState.merge(message.state)
    }

    /**
     * 更新Gossip目标列表
     */
    private func updateGossipTargets(): Unit {
        gossipTargets.clear()
        for ((nodeId, node) in currentState.members) {
            if (nodeId != localNodeId && node.status.isAvailable()) {
                gossipTargets.add(nodeId)
            }
        }
    }

    private func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds() * 1_000_000
    }

    private func min(a: Int32, b: Int32): Int32 {
        if (a < b) { a } else { b }
    }
}
