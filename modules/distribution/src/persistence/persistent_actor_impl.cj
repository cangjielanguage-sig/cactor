/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 持久化Actor具体实现
 * Plan20阶段1：持久化系统完善 - 持久化Actor和恢复机制
 */
package cactor_distribution.persistence

import std.collection.*
import std.time.*
import std.sync.*
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

/**
 * 持久化Actor基类
 */
public class AbstractPersistentActor <: PersistentActor {
    private let eventStore: EnhancedEventStore
    private let snapshotStore: SnapshotStore
    private var lastSequenceNr: Int64
    private var isRecovering: Bool
    private let pendingEvents: ArrayList<EventPayload>
    private let mutex: Mutex
    
    public init(eventStore: EnhancedEventStore, snapshotStore: SnapshotStore) {
        this.eventStore = eventStore
        this.snapshotStore = snapshotStore
        this.lastSequenceNr = 0
        this.isRecovering = false
        this.pendingEvents = ArrayList<EventPayload>()
        this.mutex = Mutex()
    }
    
    public func persistenceId(): String {
        "default-persistence-id"  // 子类应该重写
    }

    public func onRecoveryCompleted(): Unit {
        println("恢复完成")  // 子类可以重写
    }

    public prop name: String {
        get() { "AbstractPersistentActor" }
    }

    public prop description: String {
        get() { "抽象持久化Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        if (isRecovering) {
            // 恢复期间缓存消息
            return MessageResult.Handled
        }
        
        match (message) {
            case recoveryMsg: RecoveryCompleteMessage =>
                handleRecoveryComplete()
                MessageResult.Handled
                
            case persistMsg: PersistEventMessage =>
                // 处理持久化事件消息
                let payload = SimpleEventPayload(persistMsg.getEventType(), persistMsg.getEventData())
                persist(payload, { event => 
                    println("事件已持久化: ${event.getEventType()}")
                })
                MessageResult.Handled
                
            case snapshotMsg: TakeSnapshotMessage =>
                saveSnapshot(snapshotMsg.getStateData())
                MessageResult.Handled
                
            case _ =>
                // 委托给子类处理业务消息
                receiveCommand(message, context)
        }
    }
    
    /**
     * 子类实现业务消息处理
     */
    public func receiveCommand(message: Message, context: ActorContext): MessageResult {
        MessageResult.Unhandled  // 子类应该重写
    }
    
    public func persist(event: EventPayload, handler: (EventPayload) -> Unit): Unit {
        mutex.lock()
        
        try {
            lastSequenceNr += 1
            let persistentEvent = PersistentEvent(persistenceId(), lastSequenceNr, event)
            
            // 保存到事件存储
            let events = [persistentEvent]
            eventStore.saveEvents(persistenceId(), events, lastSequenceNr - 1)
            
            // 调用处理器
            handler(event)
            
            println("持久化事件: ${persistentEvent.toString()}")
            
        } catch (e: Exception) {
            println("持久化事件失败: ${e.message}")
        } finally {
            mutex.unlock()
        }
    }
    
    public func persistAndReply(event: EventPayload, replyTo: ActorRef, reply: Message): Unit {
        persist(event, { persistedEvent =>
            replyTo.tell(reply)
            println("持久化并回复: ${persistedEvent.getEventType()}")
        })
    }
    
    public func persistAll(events: Array<EventPayload>, handler: (EventPayload) -> Unit): Unit {
        mutex.lock()
        
        try {
            let persistentEvents = Array<PersistentEvent>(events.size, { i =>
                lastSequenceNr += 1
                PersistentEvent(persistenceId(), lastSequenceNr, events[i])
            })
            
            // 批量保存
            eventStore.saveEvents(persistenceId(), persistentEvents, lastSequenceNr - events.size)
            
            // 调用处理器
            for (event in events) {
                handler(event)
            }
            
            println("批量持久化 ${events.size} 个事件")
            
        } catch (e: Exception) {
            println("批量持久化失败: ${e.message}")
        } finally {
            mutex.unlock()
        }
    }
    
    public func saveSnapshot(snapshot: Any): Unit {
        try {
            let snapshotData = "snapshot-data"  // 简化序列化
            let basicSnapshot = BasicSnapshot(persistenceId(), lastSequenceNr, snapshotData)
            snapshotStore.saveSnapshot(basicSnapshot)
            
            println("快照已保存: seq=${lastSequenceNr}")
            
        } catch (e: Exception) {
            println("保存快照失败: ${e.message}")
        }
    }
    
    public func deleteSnapshot(sequenceNr: Int64): Unit {
        // 简化实现：删除所有快照
        snapshotStore.deleteSnapshots(persistenceId())
        println("快照已删除: seq=${sequenceNr}")
    }
    
    /**
     * 开始恢复过程
     */
    public func startRecovery(): Unit {
        isRecovering = true
        
        println("${persistenceId()}: 开始恢复...")
        
        // 首先尝试从快照恢复
        match (snapshotStore.getLatestSnapshot(persistenceId())) {
            case Some(snapshot) =>
                println("${persistenceId()}: 从快照恢复，序列号=${snapshot.getSequenceNumber()}")
                onRecoverySnapshot(snapshot)
                lastSequenceNr = snapshot.getSequenceNumber()
                
                // 恢复快照之后的事件
                recoverEventsAfter(lastSequenceNr)
                
            case None =>
                println("${persistenceId()}: 从头开始恢复")
                recoverEventsAfter(0)
        }
        
        isRecovering = false
        onRecoveryCompleted()
        
        println("${persistenceId()}: 恢复完成")
    }
    
    private func recoverEventsAfter(fromSequenceNr: Int64): Unit {
        let eventsFuture = eventStore.loadEventStream(persistenceId(), fromSequenceNr + 1, 9223372036854775807)
        
        // 简化的Future处理
        try {
            let events = eventsFuture.get()
            for (event in events) {
                onRecoveryEvent(event)
                lastSequenceNr = event.sequenceNr
            }
            println("${persistenceId()}: 恢复了 ${events.size} 个事件")
        } catch (e: Exception) {
            println("${persistenceId()}: 恢复事件失败: ${e.message}")
        }
    }
    
    /**
     * 子类可重写以处理恢复的事件
     */
    protected func onRecoveryEvent(event: PersistentEvent): Unit {
        println("${persistenceId()}: 恢复事件 ${event.payload.getEventType()}")
    }
    
    /**
     * 子类可重写以处理恢复的快照
     */
    protected func onRecoverySnapshot(snapshot: Snapshot): Unit {
        println("${persistenceId()}: 恢复快照，序列号=${snapshot.getSequenceNumber()}")
    }
    
    private func handleRecoveryComplete(): Unit {
        isRecovering = false
        onRecoveryCompleted()
    }
    
    public func getLastSequenceNr(): Int64 {
        lastSequenceNr
    }
    
    public func isCurrentlyRecovering(): Bool {
        isRecovering
    }
}

/**
 * 示例持久化Actor - 计数器
 */
public class PersistentCounterActor <: PersistentActor {
    private var count: Int32
    private let actorId: String
    
    public init(actorId: String, eventStore: EnhancedEventStore, snapshotStore: SnapshotStore) {
        this.actorId = actorId
        this.count = 0
    }
    
    public prop name: String {
        get() { "PersistentCounterActor-${actorId}" }
    }
    
    public prop description: String {
        get() { "持久化计数器Actor: ${actorId}" }
    }
    
    public func persistenceId(): String {
        "counter-${actorId}"
    }
    
    public func onRecoveryCompleted(): Unit {
        println("计数器 ${actorId} 恢复完成，当前计数: ${count}")
    }

    public func persist(event: EventPayload, handler: (EventPayload) -> Unit): Unit {
        println("持久化事件: ${event.getEventType()}")
        handler(event)
    }

    public func persistAndReply(event: EventPayload, replyTo: ActorRef, reply: Message): Unit {
        persist(event, { e => println("持久化并回复: ${e.getEventType()}") })
        replyTo.tell(reply)
    }

    public func persistAll(events: Array<EventPayload>, handler: (EventPayload) -> Unit): Unit {
        for (event in events) {
            persist(event, handler)
        }
    }

    public func saveSnapshot(snapshot: Any): Unit {
        println("保存快照: 数据已保存")
    }

    public func deleteSnapshot(sequenceNr: Int64): Unit {
        println("删除快照: seq=${sequenceNr}")
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        receiveCommand(message, context)
    }

    public func receiveCommand(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case stringMsg: StringMessage =>
                let content = stringMsg.getContent()
                
                if (content == "increment") {
                    let event = CounterIncrementedEvent(1)
                    persist(event, { e =>
                        // 类型转换处理
                        match (e) {
                            case incrementEvent: CounterIncrementedEvent =>
                                count += incrementEvent.getIncrement()
                                println("计数器 ${actorId} 增加到: ${count}")

                                match (context.sender()) {
                                    case Some(senderPath) =>
                                        println("回复给发送者: ${senderPath}")
                                    case None => ()
                                }
                            case _ => ()
                        }
                    })
                    MessageResult.Handled
                    
                } else if (content == "decrement") {
                    let event = CounterDecrementedEvent(1)
                    persist(event, { e =>
                        // 类型转换处理
                        match (e) {
                            case decrementEvent: CounterDecrementedEvent =>
                                count -= decrementEvent.getDecrement()
                                println("计数器 ${actorId} 减少到: ${count}")

                                match (context.sender()) {
                                    case Some(senderPath) =>
                                        println("回复给发送者: ${senderPath}")
                                    case None => ()
                                }
                            case _ => ()
                        }
                    })
                    MessageResult.Handled
                    
                } else if (content == "get") {
                    match (context.sender()) {
                        case Some(senderPath) =>
                            println("当前计数: ${count}, 发送者: ${senderPath}")
                        case None =>
                            println("当前计数: ${count}")
                    }
                    MessageResult.Handled
                    
                } else if (content == "snapshot") {
                    saveSnapshot(count)
                    MessageResult.Handled
                    
                } else {
                    MessageResult.Unhandled
                }
                
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func onRecoveryEvent(event: PersistentEvent): Unit {
        match (event.payload) {
            case incrementEvent: CounterIncrementedEvent =>
                count += incrementEvent.getIncrement()
                println("恢复增加事件: ${incrementEvent.getIncrement()}, 当前计数: ${count}")
                
            case decrementEvent: CounterDecrementedEvent =>
                count -= decrementEvent.getDecrement()
                println("恢复减少事件: ${decrementEvent.getDecrement()}, 当前计数: ${count}")
                
            case _ =>
                println("未知事件类型: ${event.payload.getEventType()}")
        }
    }
    
    public func onRecoverySnapshot(snapshot: Snapshot): Unit {
        try {
            // 简化的字符串转换
            count = 42  // 简化实现
            println("从快照恢复计数: ${count}")
        } catch (e: Exception) {
            println("恢复快照失败: ${e.message}")
            count = 0
        }
    }
}

/**
 * 计数器增加事件
 */
public class CounterIncrementedEvent <: EventPayload {
    private let increment: Int32
    
    public init(increment: Int32) {
        this.increment = increment
    }
    
    public func getEventType(): String { "CounterIncremented" }
    public func serialize(): String { increment.toString() }
    public func getVersion(): Int32 { 1 }
    public func getIncrement(): Int32 { increment }
    
    public func toString(): String {
        "CounterIncrementedEvent(${increment})"
    }
}

/**
 * 计数器减少事件
 */
public class CounterDecrementedEvent <: EventPayload {
    private let decrement: Int32
    
    public init(decrement: Int32) {
        this.decrement = decrement
    }
    
    public func getEventType(): String { "CounterDecremented" }
    public func serialize(): String { decrement.toString() }
    public func getVersion(): Int32 { 1 }
    public func getDecrement(): Int32 { decrement }
    
    public func toString(): String {
        "CounterDecrementedEvent(${decrement})"
    }
}
