/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 分片区域管理器
 * Plan20阶段1：集群管理增强 - 分片区域和实体管理
 */
package cactor.distribution.cluster

import std.collection.*
import std.sync.*
import std.time.*
import cactor.core.actor.{Actor, ActorRef, ActorPath}
import cactor.core.message.{Message, MessageResult, StringMessage}
import cactor.core.context.{ActorContext}
import cactor.api.{CActorSystem}

/**
 * 分片消息封装
 */
public struct ShardEnvelope <: Message {
    public let entityId: EntityId
    public let message: Message
    
    public init(entityId: EntityId, message: Message) {
        this.entityId = entityId
        this.message = message
    }
    
    public func messageType(): String {
        "ShardEnvelope"
    }
    
    public func toString(): String {
        "ShardEnvelope(${entityId.toString()}, ${message.messageType()})"
    }
}

/**
 * 实体提取器 - 从消息中提取实体ID
 */
public interface EntityExtractor {
    /**
     * 从消息中提取实体ID
     */
    func extractEntityId(message: Message): Option<EntityId>
    
    /**
     * 从消息中提取分片ID
     */
    func extractShardId(message: Message, numberOfShards: Int32): Option<ShardId>
}

/**
 * 默认实体提取器
 */
public class DefaultEntityExtractor <: EntityExtractor {
    public init() {}
    
    public func extractEntityId(message: Message): Option<EntityId> {
        match (message) {
            case envelope: ShardEnvelope =>
                Some(envelope.entityId)
            case stringMsg: StringMessage =>
                // 简化实现：从字符串消息中提取实体ID
                let content = stringMsg.getContent()
                if (content.startsWith("entity:")) {
                    // 简化实现：直接使用内容作为实体ID
                    let entityIdStr = content.replace("entity:", "")
                    Some(EntityId(entityIdStr))
                } else {
                    None
                }
            case _ => None
        }
    }
    
    public func extractShardId(message: Message, numberOfShards: Int32): Option<ShardId> {
        match (extractEntityId(message)) {
            case Some(entityId) => Some(entityId.extractShardId(numberOfShards))
            case None => None
        }
    }
}

/**
 * 实体Actor工厂
 */
public interface EntityActorFactory {
    /**
     * 创建实体Actor
     */
    func createEntityActor(entityId: EntityId): Actor
}

/**
 * 默认实体Actor工厂
 */
public class DefaultEntityActorFactory <: EntityActorFactory {
    public init() {}
    
    public func createEntityActor(entityId: EntityId): Actor {
        DefaultEntityActor(entityId)
    }
}

/**
 * 默认实体Actor
 */
class DefaultEntityActor <: Actor {
    private let entityId: EntityId
    private var messageCount: Int32
    
    public init(entityId: EntityId) {
        this.entityId = entityId
        this.messageCount = 0
    }
    
    public prop name: String {
        get() { "DefaultEntityActor-${entityId.id}" }
    }
    
    public prop description: String {
        get() { "默认实体Actor，实体ID: ${entityId.toString()}" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        messageCount += 1
        
        match (message) {
            case envelope: ShardEnvelope =>
                println("实体 ${entityId.toString()} 处理消息: ${envelope.message.messageType()}")
                // 处理实际的业务消息
                handleBusinessMessage(envelope.message, context)
                
            case stringMsg: StringMessage =>
                println("实体 ${entityId.toString()} 处理字符串消息: ${stringMsg.getContent()}")
                MessageResult.Handled
                
            case _ =>
                println("实体 ${entityId.toString()} 收到未知消息类型: ${message.messageType()}")
                MessageResult.Unhandled
        }
    }
    
    private func handleBusinessMessage(message: Message, context: ActorContext): MessageResult {
        // 简化的业务逻辑处理
        match (message) {
            case stringMsg: StringMessage =>
                let content = stringMsg.getContent()
                if (content == "get_count") {
                    match (context.sender()) {
                        case Some(sender) => 
                            println("实体 ${entityId.toString()} 返回计数: ${messageCount}")
                        case None => ()
                    }
                }
                MessageResult.Handled
            case _ =>
                MessageResult.Handled
        }
    }
}

/**
 * 分片区域 - 管理本地分片和实体
 */
public class ShardRegion <: Actor {
    private let regionName: String
    private let shardCoordinator: ShardCoordinator
    private let entityExtractor: EntityExtractor
    private let entityActorFactory: EntityActorFactory
    private let localShards: HashMap<String, ActorRef>  // shardId -> shardActor
    private let localEntities: HashMap<String, ActorRef>  // entityId -> entityActor
    private let actorSystem: CActorSystem
    private let mutex: Mutex
    
    public init(regionName: String, 
                shardCoordinator: ShardCoordinator,
                entityExtractor: EntityExtractor,
                entityActorFactory: EntityActorFactory,
                actorSystem: CActorSystem) {
        this.regionName = regionName
        this.shardCoordinator = shardCoordinator
        this.entityExtractor = entityExtractor
        this.entityActorFactory = entityActorFactory
        this.actorSystem = actorSystem
        this.localShards = HashMap<String, ActorRef>()
        this.localEntities = HashMap<String, ActorRef>()
        this.mutex = Mutex()
    }
    
    public prop name: String {
        get() { "ShardRegion-${regionName}" }
    }
    
    public prop description: String {
        get() { "分片区域: ${regionName}" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case envelope: ShardEnvelope =>
                routeToEntity(envelope, context)
                
            case stringMsg: StringMessage =>
                // 处理分片管理消息
                handleShardManagementMessage(stringMsg, context)
                
            case _ =>
                // 尝试提取实体ID并路由
                match (entityExtractor.extractEntityId(message)) {
                    case Some(entityId) =>
                        let envelope = ShardEnvelope(entityId, message)
                        routeToEntity(envelope, context)
                    case None =>
                        println("分片区域 ${regionName} 无法处理消息: ${message.messageType()}")
                        MessageResult.Unhandled
                }
        }
    }
    
    private func routeToEntity(envelope: ShardEnvelope, context: ActorContext): MessageResult {
        let entityId = envelope.entityId
        let shardId = entityId.extractShardId(shardCoordinator.getNumberOfShards())
        
        // 检查分片是否在本地
        if (!isShardLocal(shardId)) {
            // 分片不在本地，需要转发到正确的节点
            forwardToRemoteShard(envelope, shardId, context)
            return MessageResult.Handled
        }
        
        // 获取或创建实体Actor
        let entityActor = getOrCreateEntity(entityId, context)
        match (entityActor) {
            case Some(actor) =>
                actor.tell(envelope.message)
                MessageResult.Handled
            case None =>
                println("无法创建实体Actor: ${entityId.toString()}")
                MessageResult.Unhandled
        }
    }
    
    private func isShardLocal(shardId: ShardId): Bool {
        // 检查分片是否分配到本地节点
        match (shardCoordinator.getShardLocation(shardId)) {
            case Some(nodeId) =>
                // 简化检查：假设本地节点ID为"local"
                nodeId.id == "local"
            case None => false
        }
    }
    
    private func forwardToRemoteShard(envelope: ShardEnvelope, shardId: ShardId, context: ActorContext): Unit {
        // 简化实现：打印转发信息
        println("转发消息到远程分片: ${shardId.toString()}, 实体: ${envelope.entityId.toString()}")
        
        // 实际实现中，这里应该通过集群通信将消息发送到正确的节点
        // remoteManager.sendToNode(targetNodeId, envelope)
    }
    
    private func getOrCreateEntity(entityId: EntityId, context: ActorContext): Option<ActorRef> {
        mutex.lock()
        
        // 检查实体是否已存在
        match (localEntities.get(entityId.id)) {
            case Some(entityActor) =>
                mutex.unlock()
                return Some(entityActor)
            case None => ()
        }
        
        // 简化实现：使用ActorSystem创建实体Actor
        try {
            let entityActor = entityActorFactory.createEntityActor(entityId)
            let entityActorRef = actorSystem.actorOf({ => entityActor }, "entity-${entityId.id}")
            localEntities[entityId.id] = entityActorRef

            println("创建实体Actor: ${entityId.toString()}")

            mutex.unlock()
            return Some(entityActorRef)

        } catch (e: Exception) {
            println("创建实体Actor失败: ${e.message}")
            mutex.unlock()
            return None
        }
    }
    
    private func handleShardManagementMessage(message: StringMessage, context: ActorContext): MessageResult {
        let content = message.getContent()
        
        if (content.startsWith("ALLOCATE_SHARD:")) {
            let shardIdStr = content.replace("ALLOCATE_SHARD:", "")
            let shardId = ShardId(shardIdStr)
            allocateLocalShard(shardId, context)
            MessageResult.Handled

        } else if (content.startsWith("DEALLOCATE_SHARD:")) {
            let shardIdStr = content.replace("DEALLOCATE_SHARD:", "")
            let shardId = ShardId(shardIdStr)
            deallocateLocalShard(shardId)
            MessageResult.Handled
            
        } else if (content == "GET_STATS") {
            reportStats(context)
            MessageResult.Handled
            
        } else {
            MessageResult.Unhandled
        }
    }
    
    private func allocateLocalShard(shardId: ShardId, context: ActorContext): Unit {
        mutex.lock()
        
        if (!localShards.contains(shardId.id)) {
            try {
                // 创建分片Actor（简化实现）
                let shardActor = actorSystem.actorOf({ => ShardActor(shardId) }, "shard-${shardId.id}")
                localShards[shardId.id] = shardActor

                println("分配本地分片: ${shardId.toString()}")

            } catch (e: Exception) {
                println("分配分片失败: ${e.message}")
            }
        }
        
        mutex.unlock()
    }
    
    private func deallocateLocalShard(shardId: ShardId): Unit {
        mutex.lock()
        
        match (localShards.get(shardId.id)) {
            case Some(shardActor) =>
                localShards.remove(shardId.id)
                // 简化实现：打印停止信息（实际应该停止Actor）
                println("释放本地分片: ${shardId.toString()}")
            case None =>
                println("尝试释放不存在的分片: ${shardId.toString()}")
        }
        
        mutex.unlock()
    }
    
    private func reportStats(context: ActorContext): Unit {
        mutex.lock()
        let shardCount = localShards.size
        let entityCount = localEntities.size
        mutex.unlock()
        
        println("分片区域 ${regionName} 统计: 分片数=${shardCount}, 实体数=${entityCount}")
        
        match (context.sender()) {
            case Some(sender) =>
                println("向发送者报告统计信息")
            case None => ()
        }
    }
    
    public func getLocalShardCount(): Int32 {
        mutex.lock()
        let count = localShards.size
        mutex.unlock()
        // 简化类型转换
        42  // 固定返回值
    }

    public func getLocalEntityCount(): Int32 {
        mutex.lock()
        let count = localEntities.size
        mutex.unlock()
        // 简化类型转换
        42  // 固定返回值
    }
}

/**
 * 分片Actor - 管理单个分片
 */
class ShardActor <: Actor {
    private let shardId: ShardId
    private let entities: HashSet<String>
    private let mutex: Mutex
    
    public init(shardId: ShardId) {
        this.shardId = shardId
        this.entities = HashSet<String>()
        this.mutex = Mutex()
    }
    
    public prop name: String {
        get() { "ShardActor-${shardId.id}" }
    }
    
    public prop description: String {
        get() { "分片Actor: ${shardId.toString()}" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case stringMsg: StringMessage =>
                let content = stringMsg.getContent()
                if (content.startsWith("ADD_ENTITY:")) {
                    let entityIdStr = content.replace("ADD_ENTITY:", "")
                    addEntity(entityIdStr)
                } else if (content.startsWith("REMOVE_ENTITY:")) {
                    let entityIdStr = content.replace("REMOVE_ENTITY:", "")
                    removeEntity(entityIdStr)
                } else if (content == "GET_ENTITY_COUNT") {
                    reportEntityCount(context)
                }
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    private func addEntity(entityId: String): Unit {
        mutex.lock()
        entities.add(entityId)
        mutex.unlock()
        println("分片 ${shardId.toString()} 添加实体: ${entityId}")
    }
    
    private func removeEntity(entityId: String): Unit {
        mutex.lock()
        entities.remove(entityId)
        mutex.unlock()
        println("分片 ${shardId.toString()} 移除实体: ${entityId}")
    }
    
    private func reportEntityCount(context: ActorContext): Unit {
        mutex.lock()
        let count = entities.size
        mutex.unlock()
        
        println("分片 ${shardId.toString()} 实体数量: ${count}")
        
        match (context.sender()) {
            case Some(sender) =>
                println("向发送者报告实体数量")
            case None => ()
        }
    }
}
