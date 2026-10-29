/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 企业级事件溯源系统
 * 基于CActor框架的高性能事件存储和重放
 */
package cactor_xstate.enterprise.event_sourcing

import std.collection.*
import std.time.*
import std.sync.*
import cactor_xstate.core.types.{StateMachineEvent}
import cactor_foundation.logging.{Logger, Logging}

let log_event_store: Logger = Logging.logger("cactor_xstate.enterprise.event_sourcing.event_store")

/**
 * 事件存储条目
 */
public struct EventStoreEntry {
    public let eventId: String
    public let stateMachineId: String
    public let eventType: String
    public let eventData: String
    public let timestamp: Int64
    public let sequenceNumber: Int64
    public let version: Int32
    
    public init(
        eventId: String,
        stateMachineId: String,
        eventType: String,
        eventData: String,
        timestamp: Int64,
        sequenceNumber: Int64,
        version: Int32
    ) {
        this.eventId = eventId
        this.stateMachineId = stateMachineId
        this.eventType = eventType
        this.eventData = eventData
        this.timestamp = timestamp
        this.sequenceNumber = sequenceNumber
        this.version = version
    }
}

/**
 * 事件存储接口
 */
public interface EventStore {
    /**
     * 存储事件
     */
    func storeEvent(entry: EventStoreEntry): Bool
    
    /**
     * 获取事件流
     */
    func getEventStream(stateMachineId: String): Array<EventStoreEntry>
    
    /**
     * 获取事件流（从指定序列号开始）
     */
    func getEventStreamFrom(stateMachineId: String, fromSequence: Int64): Array<EventStoreEntry>
    
    /**
     * 获取最新序列号
     */
    func getLatestSequenceNumber(stateMachineId: String): Int64
    
    /**
     * 创建快照
     */
    func createSnapshot(stateMachineId: String, state: String, sequenceNumber: Int64): Bool
    
    /**
     * 获取最新快照
     */
    func getLatestSnapshot(stateMachineId: String): Option<StateSnapshot>
}

/**
 * 状态快照
 */
public struct StateSnapshot {
    public let stateMachineId: String
    public let state: String
    public let sequenceNumber: Int64
    public let timestamp: Int64
    public let metadata: HashMap<String, String>
    
    public init(
        stateMachineId: String,
        state: String,
        sequenceNumber: Int64,
        timestamp: Int64,
        metadata: HashMap<String, String>
    ) {
        this.stateMachineId = stateMachineId
        this.state = state
        this.sequenceNumber = sequenceNumber
        this.timestamp = timestamp
        this.metadata = metadata
    }
}

/**
 * 内存事件存储实现
 */
public class InMemoryEventStore <: EventStore {
    private let events: HashMap<String, ArrayList<EventStoreEntry>>
    private let snapshots: HashMap<String, StateSnapshot>
    private let sequenceCounters: HashMap<String, Int64>
    private let mutex: Mutex
    
    public init() {
        this.events = HashMap<String, ArrayList<EventStoreEntry>>()
        this.snapshots = HashMap<String, StateSnapshot>()
        this.sequenceCounters = HashMap<String, Int64>()
        this.mutex = Mutex()
    }
    
    public func storeEvent(entry: EventStoreEntry): Bool {
        mutex.lock()
        
        try {
            // 获取或创建事件列表
            let eventList = getOrCreateEventList(entry.stateMachineId)
            
            // 添加事件
            eventList.add(entry)
            
            // 更新序列计数器
            sequenceCounters.add(entry.stateMachineId, entry.sequenceNumber)
            
            log_event_store.debug("存储事件: ${entry.eventType} for ${entry.stateMachineId}")
            
            mutex.unlock()
            return true
            
        } catch (e: Exception) {
            log_event_store.error("存储事件失败: ${e.message}")
            mutex.unlock()
            return false
        }
    }
    
    public func getEventStream(stateMachineId: String): Array<EventStoreEntry> {
        mutex.lock()
        
        let eventListOption = events.get(stateMachineId)
        if (eventListOption.isSome()) {
            let eventList = eventListOption.getOrThrow()
            let result = eventList.toArray()
            mutex.unlock()
            return result
        }
        
        mutex.unlock()
        return []
    }
    
    public func getEventStreamFrom(stateMachineId: String, fromSequence: Int64): Array<EventStoreEntry> {
        mutex.lock()
        
        let eventListOption = events.get(stateMachineId)
        if (eventListOption.isSome()) {
            let eventList = eventListOption.getOrThrow()
            let filteredEvents = ArrayList<EventStoreEntry>()
            
            for (event in eventList) {
                if (event.sequenceNumber >= fromSequence) {
                    filteredEvents.add(event)
                }
            }
            
            let result = filteredEvents.toArray()
            mutex.unlock()
            return result
        }
        
        mutex.unlock()
        return []
    }
    
    public func getLatestSequenceNumber(stateMachineId: String): Int64 {
        mutex.lock()
        
        let sequenceOption = sequenceCounters.get(stateMachineId)
        if (sequenceOption.isSome()) {
            let sequence = sequenceOption.getOrThrow()
            mutex.unlock()
            return sequence
        }
        
        mutex.unlock()
        return 0
    }
    
    public func createSnapshot(stateMachineId: String, state: String, sequenceNumber: Int64): Bool {
        mutex.lock()
        
        try {
            let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
            let metadata = HashMap<String, String>()
            metadata.add("created_by", "InMemoryEventStore")
            
            let snapshot = StateSnapshot(stateMachineId, state, sequenceNumber, timestamp, metadata)
            snapshots.add(stateMachineId, snapshot)
            
            log_event_store.debug("创建快照: ${stateMachineId} at sequence ${sequenceNumber}")
            
            mutex.unlock()
            return true
            
        } catch (e: Exception) {
            log_event_store.error("创建快照失败: ${e.message}")
            mutex.unlock()
            return false
        }
    }
    
    public func getLatestSnapshot(stateMachineId: String): Option<StateSnapshot> {
        mutex.lock()
        
        let snapshotOption = snapshots.get(stateMachineId)
        mutex.unlock()
        return snapshotOption
    }
    
    /**
     * 获取或创建事件列表
     */
    private func getOrCreateEventList(stateMachineId: String): ArrayList<EventStoreEntry> {
        let eventListOption = events.get(stateMachineId)
        if (eventListOption.isSome()) {
            return eventListOption.getOrThrow()
        } else {
            let newEventList = ArrayList<EventStoreEntry>()
            events.add(stateMachineId, newEventList)
            return newEventList
        }
    }
}

/**
 * 事件溯源管理器
 */
public class EventSourcingManager {
    private let eventStore: EventStore
    private let replayCallbacks: HashMap<String, (EventStoreEntry) -> Unit>
    
    public init(eventStore: EventStore) {
        this.eventStore = eventStore
        this.replayCallbacks = HashMap<String, (EventStoreEntry) -> Unit>()
    }
    
    /**
     * 记录状态机事件
     */
    public func recordEvent(
        stateMachineId: String,
        event: StateMachineEvent,
        eventData: String
    ): Bool {
        let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let sequenceNumber = eventStore.getLatestSequenceNumber(stateMachineId) + 1
        let eventId = "${stateMachineId}-${sequenceNumber}-${timestamp}"
        
        let entry = EventStoreEntry(
            eventId,
            stateMachineId,
            event.getEventType(),
            eventData,
            timestamp,
            sequenceNumber,
            1
        )
        
        return eventStore.storeEvent(entry)
    }
    
    /**
     * 重放事件流
     */
    public func replayEvents(stateMachineId: String): Bool {
        let events = eventStore.getEventStream(stateMachineId)
        let callbackOption = replayCallbacks.get(stateMachineId)
        
        if (callbackOption.isSome()) {
            let callback = callbackOption.getOrThrow()
            
            for (event in events) {
                callback(event)
            }
            
            log_event_store.debug("重放了 ${events.size} 个事件 for ${stateMachineId}")
            return true
        }
        
        return false
    }
    
    /**
     * 注册重放回调
     */
    public func registerReplayCallback(
        stateMachineId: String,
        callback: (EventStoreEntry) -> Unit
    ): Unit {
        replayCallbacks.add(stateMachineId, callback)
    }
    
    /**
     * 创建状态快照
     */
    public func createSnapshot(stateMachineId: String, currentState: String): Bool {
        let sequenceNumber = eventStore.getLatestSequenceNumber(stateMachineId)
        return eventStore.createSnapshot(stateMachineId, currentState, sequenceNumber)
    }
}
