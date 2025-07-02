/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 事件溯源系统增强实现
 * Plan20阶段1：持久化系统完善 - 事件溯源、CQRS支持
 */
package cactor.distribution.persistence

import std.collection.*
import std.time.*
import std.sync.*
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.message.{Message, MessageResult, StringMessage}
import cactor.core.context.{ActorContext}

/**
 * 事件载荷接口 - 所有事件数据必须实现此接口
 */
public interface EventPayload {
    func getEventType(): String
    func serialize(): String
    func getVersion(): Int32
}

/**
 * 增强的事件定义
 */
public struct PersistentEvent {
    public let persistenceId: String
    public let sequenceNr: Int64
    public let timestamp: DateTime
    public let payload: EventPayload
    public let metadata: HashMap<String, String>
    public let causationId: Option<String>  // 因果关系ID
    public let correlationId: Option<String>  // 关联ID
    
    public init(persistenceId: String, sequenceNr: Int64, payload: EventPayload) {
        this.persistenceId = persistenceId
        this.sequenceNr = sequenceNr
        this.timestamp = DateTime.now()
        this.payload = payload
        this.metadata = HashMap<String, String>()
        this.causationId = None<String>
        this.correlationId = None<String>
    }
    
    public init(persistenceId: String, sequenceNr: Int64, payload: EventPayload, 
                causationId: Option<String>, correlationId: Option<String>) {
        this.persistenceId = persistenceId
        this.sequenceNr = sequenceNr
        this.timestamp = DateTime.now()
        this.payload = payload
        this.metadata = HashMap<String, String>()
        this.causationId = causationId
        this.correlationId = correlationId
    }
    
    public func withMetadata(key: String, value: String): PersistentEvent {
        let newMetadata = HashMap<String, String>()
        for ((k, v) in metadata) {
            newMetadata[k] = v
        }
        newMetadata[key] = value
        
        PersistentEvent(persistenceId, sequenceNr, payload, causationId, correlationId)
    }
    
    public func toString(): String {
        "PersistentEvent(${persistenceId}, seq=${sequenceNr}, type=${payload.getEventType()})"
    }
}

/**
 * 增强的事件存储接口
 */
public interface EnhancedEventStore {
    /**
     * 保存事件批次（原子操作）
     */
    func saveEvents(persistenceId: String, events: Array<PersistentEvent>, expectedVersion: Int64): Future<Unit>
    
    /**
     * 加载事件流
     */
    func loadEventStream(persistenceId: String, fromSequenceNr: Int64, toSequenceNr: Int64): Future<Array<PersistentEvent>>
    
    /**
     * 获取最高序列号
     */
    func getHighestSequenceNr(persistenceId: String): Future<Int64>
    
    /**
     * 按事件类型查询
     */
    func queryEventsByType(eventType: String, fromTimestamp: DateTime, toTimestamp: DateTime): Future<Array<PersistentEvent>>
    
    /**
     * 删除事件流
     */
    func deleteEventStream(persistenceId: String, toSequenceNr: Int64): Future<Unit>
}

/**
 * 内存增强事件存储实现
 */
public class MemoryEnhancedEventStore <: EnhancedEventStore {
    private let events: HashMap<String, ArrayList<PersistentEvent>>
    private let sequenceNumbers: HashMap<String, Int64>
    private let mutex: Mutex
    
    public init() {
        this.events = HashMap<String, ArrayList<PersistentEvent>>()
        this.sequenceNumbers = HashMap<String, Int64>()
        this.mutex = Mutex()
    }
    
    public func saveEvents(persistenceId: String, events: Array<PersistentEvent>, expectedVersion: Int64): Future<Unit> {
        mutex.lock()
        
        try {
            // 检查版本冲突
            let currentVersion = match (sequenceNumbers.get(persistenceId)) {
                case Some(version) => version
                case None => 0
            }
            
            if (currentVersion != expectedVersion) {
                mutex.unlock()
                return FailedFuture<Unit>(Exception("版本冲突: 期望=${expectedVersion}, 当前=${currentVersion}"))
            }
            
            // 保存事件
            let eventList = match (this.events.get(persistenceId)) {
                case Some(list) => list
                case None => 
                    let newList = ArrayList<PersistentEvent>()
                    this.events[persistenceId] = newList
                    newList
            }
            
            for (event in events) {
                eventList.add(event)
            }
            
            // 更新序列号
            sequenceNumbers[persistenceId] = currentVersion + events.size
            
            mutex.unlock()
            return CompletedFuture<Unit>(())
            
        } catch (e: Exception) {
            mutex.unlock()
            return FailedFuture<Unit>(e)
        }
    }
    
    public func loadEventStream(persistenceId: String, fromSequenceNr: Int64, toSequenceNr: Int64): Future<Array<PersistentEvent>> {
        mutex.lock()
        
        try {
            let result = ArrayList<PersistentEvent>()
            
            match (events.get(persistenceId)) {
                case Some(eventList) =>
                    for (event in eventList) {
                        if (event.sequenceNr >= fromSequenceNr && event.sequenceNr <= toSequenceNr) {
                            result.add(event)
                        }
                    }
                case None => ()
            }
            
            mutex.unlock()
            return CompletedFuture<Array<PersistentEvent>>(result.toArray())
            
        } catch (e: Exception) {
            mutex.unlock()
            return FailedFuture<Array<PersistentEvent>>(e)
        }
    }
    
    public func getHighestSequenceNr(persistenceId: String): Future<Int64> {
        mutex.lock()
        let result = match (sequenceNumbers.get(persistenceId)) {
            case Some(seqNr) => seqNr
            case None => 0
        }
        mutex.unlock()
        CompletedFuture<Int64>(result)
    }
    
    public func queryEventsByType(eventType: String, fromTimestamp: DateTime, toTimestamp: DateTime): Future<Array<PersistentEvent>> {
        mutex.lock()
        
        try {
            let result = ArrayList<PersistentEvent>()
            
            for ((_, eventList) in events) {
                for (event in eventList) {
                    if (event.payload.getEventType() == eventType &&
                        event.timestamp >= fromTimestamp &&
                        event.timestamp <= toTimestamp) {
                        result.add(event)
                    }
                }
            }
            
            mutex.unlock()
            return CompletedFuture<Array<PersistentEvent>>(result.toArray())
            
        } catch (e: Exception) {
            mutex.unlock()
            return FailedFuture<Array<PersistentEvent>>(e)
        }
    }
    
    public func deleteEventStream(persistenceId: String, toSequenceNr: Int64): Future<Unit> {
        mutex.lock()
        
        try {
            match (events.get(persistenceId)) {
                case Some(eventList) =>
                    let newList = ArrayList<PersistentEvent>()
                    for (event in eventList) {
                        if (event.sequenceNr > toSequenceNr) {
                            newList.add(event)
                        }
                    }
                    events[persistenceId] = newList
                case None => ()
            }
            
            mutex.unlock()
            return CompletedFuture<Unit>(())
            
        } catch (e: Exception) {
            mutex.unlock()
            return FailedFuture<Unit>(e)
        }
    }
}

/**
 * 持久化Actor接口
 */
public interface PersistentActor <: Actor {
    /**
     * 持久化ID - 用于标识事件流
     */
    func persistenceId(): String
    
    /**
     * 恢复完成回调
     */
    func onRecoveryCompleted(): Unit
    
    /**
     * 持久化事件
     */
    func persist(event: EventPayload, handler: (EventPayload) -> Unit): Unit

    /**
     * 持久化并回复
     */
    func persistAndReply(event: EventPayload, replyTo: ActorRef, reply: Message): Unit

    /**
     * 批量持久化事件
     */
    func persistAll(events: Array<EventPayload>, handler: (EventPayload) -> Unit): Unit
    
    /**
     * 保存快照
     */
    func saveSnapshot(snapshot: Any): Unit
    
    /**
     * 删除快照
     */
    func deleteSnapshot(sequenceNr: Int64): Unit
}

/**
 * 简单事件载荷实现
 */
public class SimpleEventPayload <: EventPayload {
    private let eventType: String
    private let data: String
    private let version: Int32
    
    public init(eventType: String, data: String) {
        this.eventType = eventType
        this.data = data
        this.version = 1
    }
    
    public func getEventType(): String { eventType }
    public func serialize(): String { data }
    public func getVersion(): Int32 { version }
    
    public func toString(): String {
        "SimpleEventPayload(${eventType}, ${data})"
    }
}

/**
 * Future接口简化实现
 */
public interface Future<T> {
    func get(): T
    func isCompleted(): Bool
    func onComplete(callback: (T) -> Unit): Unit
}

public class CompletedFuture<T> <: Future<T> {
    private let value: T
    
    public init(value: T) {
        this.value = value
    }
    
    public func get(): T { value }
    public func isCompleted(): Bool { true }
    public func onComplete(callback: (T) -> Unit): Unit {
        callback(value)
    }
}

public class FailedFuture<T> <: Future<T> {
    private let exception: Exception
    
    public init(exception: Exception) {
        this.exception = exception
    }
    
    public func get(): T { 
        throw exception
    }
    
    public func isCompleted(): Bool { true }
    public func onComplete(callback: (T) -> Unit): Unit {
        // 失败的Future不调用回调
    }
}

/**
 * Future工厂
 */
public class FutureFactory {
    public static func successful(value: Unit): CompletedFuture<Unit> {
        CompletedFuture<Unit>(value)
    }

    public static func failed(exception: Exception): FailedFuture<Unit> {
        FailedFuture<Unit>(exception)
    }
}
