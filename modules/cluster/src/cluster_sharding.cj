/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 集群分片系统实现
 * Plan20阶段1：集群管理增强 - 分片、路由、故障转移
 */
package cactor_cluster

import std.collection.*
import std.sync.*
import std.time.*
import cactor_foundation.logging.{Logger, Logging}

let log_cluster_sharding: Logger = Logging.logger("cactor_cluster.cluster_sharding")

/**
 * 稳定字符串哈希（FNV-1a 的取模变体）。
 *
 * 不能直接连乘 Int64：仓颉整数溢出默认抛异常；这里每步取模保持在 31 位以内，
 * 结果非负、与插入顺序无关，可作为分片/哈希的稳定键。
 */
func stableShardHash(text: String): Int32 {
    var h: Int64 = 2166136261
    for (i in 0..text.size) {
        h = (h * 131 + Int64(text[i])) % 2147483647
    }
    Int32(h)
}

/**
 * 分片标识
 */
public struct ShardId {
    public let id: String

    public init(id: String) {
        this.id = id
    }

    public func toString(): String {
        "ShardId(${id})"
    }

    public func equals(other: ShardId): Bool {
        this.id == other.id
    }

    public func hashCode(): Int32 {
        // 用 id 的稳定哈希（旧实现固定返回 42，所有分片在哈希容器里会退化成同一条链）
        stableShardHash(id)
    }
}

/**
 * 实体标识
 */
public struct EntityId {
    public let id: String

    public init(id: String) {
        this.id = id
    }

    public func toString(): String {
        "EntityId(${id})"
    }

    /**
     * 按实体 id 的稳定哈希取模分片（旧实现固定返回 shard-0，所有实体挤在一个分片上：
     * 既失去分片水平扩展，也让"实体分散到多节点"的集群测试必然依赖协调器时序）。
     */
    public func extractShardId(numberOfShards: Int32): ShardId {
        if (numberOfShards <= 0) {
            return ShardId("shard-0")
        }
        let index = Int64(stableShardHash(id)) % Int64(numberOfShards)
        ShardId("shard-${index}")
    }
}

/**
 * 分片分配策略接口
 */
public interface ShardAllocationStrategy {
    /**
     * 为分片分配节点
     * @param shardId 分片ID
     * @param currentAllocations 当前分片分配情况
     * @param availableNodes 可用节点列表
     * @return 分配的节点ID
     */
    func allocateShard(shardId: ShardId, 
                      currentAllocations: HashMap<String, String>,
                      availableNodes: Array<NodeId>): Option<NodeId>
    
    /**
     * 重新平衡分片分配
     * @param currentAllocations 当前分片分配情况
     * @param availableNodes 可用节点列表
     * @return 新的分片分配方案
     */
    func rebalance(currentAllocations: HashMap<String, String>,
                  availableNodes: Array<NodeId>): HashMap<String, String>
}

/**
 * 最少分片优先分配策略
 */
public class LeastShardsAllocationStrategy <: ShardAllocationStrategy {
    public init() {}
    
    public func allocateShard(shardId: ShardId, 
                             currentAllocations: HashMap<String, String>,
                             availableNodes: Array<NodeId>): Option<NodeId> {
        if (availableNodes.size == 0) {
            return None
        }
        
        // 统计每个节点的分片数量
        let nodeShardCounts = HashMap<String, Int32>()
        for (node in availableNodes) {
            nodeShardCounts[node.id] = 0
        }
        
        for ((_, nodeId) in currentAllocations) {
            match (nodeShardCounts.get(nodeId)) {
                case Some(count) => nodeShardCounts[nodeId] = count + 1
                case None => () // 节点不在可用列表中
            }
        }
        
        // 找到分片数量最少的节点
        var minShards: Int32 = 2147483647  // Int32最大值
        var selectedNode: Option<NodeId> = None

        for (node in availableNodes) {
            match (nodeShardCounts.get(node.id)) {
                case Some(shardCount) =>
                    if (shardCount < minShards) {
                        minShards = shardCount
                        selectedNode = Some(node)
                    }
                case None =>
                    if (0 < minShards) {
                        minShards = 0
                        selectedNode = Some(node)
                    }
            }
        }
        
        selectedNode
    }
    
    public func rebalance(currentAllocations: HashMap<String, String>,
                         availableNodes: Array<NodeId>): HashMap<String, String> {
        // 简化的重平衡策略：保持现有分配，只处理失效节点
        let newAllocations = HashMap<String, String>()
        let availableNodeIds = HashSet<String>()
        
        for (node in availableNodes) {
            availableNodeIds.add(node.id)
        }
        
        for ((shardId, nodeId) in currentAllocations) {
            if (availableNodeIds.contains(nodeId)) {
                newAllocations[shardId] = nodeId
            } else {
                // 重新分配失效节点的分片
                let newNodeOption = allocateShard(ShardId(shardId), newAllocations, availableNodes)
                match (newNodeOption) {
                    case Some(newNode) => newAllocations[shardId] = newNode.id
                    case None => () // 无可用节点
                }
            }
        }
        
        newAllocations
    }
}

/**
 * 分片状态
 */
public enum ShardState {
    | Allocating(DateTime)    // 正在分配
    | Active(NodeId, DateTime) // 活跃状态
    | Rebalancing(NodeId, NodeId, DateTime) // 重平衡中
    | Failed(DateTime)        // 失败状态
    
    public func isActive(): Bool {
        match (this) {
            case Active(_, _) => true
            case _ => false
        }
    }
    
    public func getNodeId(): Option<NodeId> {
        match (this) {
            case Active(nodeId, _) => Some(nodeId)
            case Rebalancing(fromNode, _, _) => Some(fromNode)
            case _ => None
        }
    }
    
    public func toString(): String {
        match (this) {
            case Allocating(time) => "Allocating(${time})"
            case Active(nodeId, time) => "Active(${nodeId.toString()}, ${time})"
            case Rebalancing(from, to, time) => "Rebalancing(${from.toString()} -> ${to.toString()}, ${time})"
            case Failed(time) => "Failed(${time})"
        }
    }
}

/**
 * 分片信息
 */
public class ShardInfo {
    public let shardId: ShardId
    private var state: ShardState
    private let entities: HashSet<String>
    private let mutex: Mutex
    
    public init(shardId: ShardId) {
        this.shardId = shardId
        this.state = ShardState.Allocating(DateTime.now())
        this.entities = HashSet<String>()
        this.mutex = Mutex()
    }
    
    public func getState(): ShardState {
        mutex.lock()
        let currentState = state
        mutex.unlock()
        currentState
    }
    
    public func setState(newState: ShardState): Unit {
        mutex.lock()
        this.state = newState
        mutex.unlock()
        log_cluster_sharding.debug("分片 ${shardId.toString()} 状态变更为: ${newState.toString()}")
    }
    
    public func addEntity(entityId: EntityId): Unit {
        mutex.lock()
        entities.add(entityId.id)
        mutex.unlock()
    }
    
    public func removeEntity(entityId: EntityId): Unit {
        mutex.lock()
        entities.remove(entityId.id)
        mutex.unlock()
    }
    
    public func getEntityCount(): Int32 {
        mutex.lock()
        let count = Int32(entities.size)
        mutex.unlock()
        count
    }
    
    public func hasEntity(entityId: EntityId): Bool {
        mutex.lock()
        let exists = entities.contains(entityId.id)
        mutex.unlock()
        exists
    }
    
    public func toString(): String {
        "ShardInfo(${shardId.toString()}, ${getState().toString()}, entities=${getEntityCount()})"
    }
}

/**
 * 分片协调器 - 管理全局分片状态
 */
public class ShardCoordinator {
    private let clusterState: ClusterState
    private let allocationStrategy: ShardAllocationStrategy
    private let shardAllocations: HashMap<String, String>  // shardId -> nodeId
    private let shardInfos: HashMap<String, ShardInfo>
    private let numberOfShards: Int32
    private let mutex: Mutex
    
    public init(clusterState: ClusterState, numberOfShards: Int32, 
                allocationStrategy: ShardAllocationStrategy) {
        this.clusterState = clusterState
        this.numberOfShards = numberOfShards
        this.allocationStrategy = allocationStrategy
        this.shardAllocations = HashMap<String, String>()
        this.shardInfos = HashMap<String, ShardInfo>()
        this.mutex = Mutex()
        
        // 初始化所有分片
        for (i in 0..numberOfShards) {
            let shardId = ShardId("shard-${i}")
            let shardInfo = ShardInfo(shardId)
            shardInfos[shardId.id] = shardInfo
        }
    }
    
    public func allocateShard(shardId: ShardId): Option<NodeId> {
        mutex.lock()
        
        // 检查分片是否已分配
        match (shardAllocations.get(shardId.id)) {
            case Some(nodeId) => 
                mutex.unlock()
                return Some(NodeId(nodeId))
            case None => ()
        }
        
        // 获取可用节点
        let availableNodes = clusterState.getAvailableNodes()
        let nodeIds = Array<NodeId>(availableNodes.size, { i => availableNodes[i].nodeId })
        
        // 使用分配策略分配分片
        let selectedNode = allocationStrategy.allocateShard(shardId, shardAllocations, nodeIds)
        
        match (selectedNode) {
            case Some(nodeId) =>
                shardAllocations[shardId.id] = nodeId.id
                
                // 更新分片状态
                match (shardInfos.get(shardId.id)) {
                    case Some(shardInfo) => 
                        shardInfo.setState(ShardState.Active(nodeId, DateTime.now()))
                    case None => ()
                }
                
                log_cluster_sharding.debug("分片 ${shardId.toString()} 分配到节点 ${nodeId.toString()}")
                
            case None =>
                log_cluster_sharding.warn("警告: 无法为分片 ${shardId.toString()} 找到可用节点")
        }
        
        mutex.unlock()
        selectedNode
    }
    
    public func getShardLocation(shardId: ShardId): Option<NodeId> {
        mutex.lock()
        let result = match (shardAllocations.get(shardId.id)) {
            case Some(nodeId) => Some(NodeId(nodeId))
            case None => None<NodeId>
        }
        mutex.unlock()
        result
    }
    
    public func rebalanceShards(): Unit {
        mutex.lock()
        
        let availableNodes = clusterState.getAvailableNodes()
        let nodeIds = Array<NodeId>(availableNodes.size, { i => availableNodes[i].nodeId })
        
        let newAllocations = allocationStrategy.rebalance(shardAllocations, nodeIds)
        
        // 应用新的分配方案
        for ((shardId, newNodeId) in newAllocations) {
            let oldNodeId = shardAllocations.get(shardId)
            let shouldUpdate = match (oldNodeId) {
                case Some(oldId) => oldId != newNodeId
                case None => true
            }

            if (shouldUpdate) {
                shardAllocations[shardId] = newNodeId

                // 更新分片状态
                match (shardInfos.get(shardId)) {
                    case Some(shardInfo) =>
                        shardInfo.setState(ShardState.Active(NodeId(newNodeId), DateTime.now()))
                    case None => ()
                }

                log_cluster_sharding.debug("分片 ${shardId} 重新分配到节点 ${newNodeId}")
            }
        }
        
        mutex.unlock()
        log_cluster_sharding.debug("分片重平衡完成")
    }
    
    public func getShardInfo(shardId: ShardId): Option<ShardInfo> {
        mutex.lock()
        let result = shardInfos.get(shardId.id)
        mutex.unlock()
        result
    }
    
    public func getAllShardAllocations(): HashMap<String, String> {
        mutex.lock()
        let result = HashMap<String, String>()
        for ((shardId, nodeId) in shardAllocations) {
            result[shardId] = nodeId
        }
        mutex.unlock()
        result
    }
    
    public func getNumberOfShards(): Int32 {
        numberOfShards
    }
}
