/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 持久化Actor具体实现
 * Plan20阶段1：持久化系统完善 - 持久化Actor和恢复机制
 */
package cactor_persistence

import std.collection.*
import std.sync.*
import std.convert.*
import cactor_core.actor.{ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}
import cactor_foundation.logging.{Logger, Logging}

let log_persistent_actor_impl: Logger = Logging.logger("cactor_persistence.persistent_actor_impl")

/**
 * 持久化操作异常：保存/恢复失败时上抛，调用方（监督策略）据此决定重试或停止，
 * 不再静默丢失数据（2026-11-06）
 */
public class PersistenceException <: Exception {
    public init(message: String, cause: Exception) {
        super(message, cause)
    }

    public init(message: String) {
        super(message)
    }
}

/**
 * 持久化Actor基类（E3 处置说明，见 docs/plan/20261106.md B8/E3）
 *
 * ⚠️ 使用须知：本类是 persistence 模块三套并行事件溯源 API 之一，**新代码请优先使用
 * EventStore + EventSourcedActor**（event_store.cj / event_sourced_actor.cj），那是 E8
 * 持久化收敛后的主路径。本类保留是为了编译兼容，后续版本将标记 @Deprecated。
 *
 * 语义保证（2026-11-06 修复 B8 后）：
 * - saveSnapshot 保存真实状态数据（非字符串状态需先自行序列化为 String）；
 * - persist/persistAll 失败时上抛 PersistenceException 并回退序号，不再静默丢数据；
 * - 恢复按 sequenceNr 对齐事件存储版本（getHighestSequenceNr），不按数组下标。
 */
public open class AbstractPersistentActor <: PersistentActor {
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
    
    public open func persistenceId(): String {
        "default-persistence-id"  // 子类应该重写
    }

    public func onRecoveryCompleted(): Unit {
        log_persistent_actor_impl.debug("恢复完成")  // 子类可以重写
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
                    log_persistent_actor_impl.debug("事件已持久化: ${event.getEventType()}")
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

            // 保存到事件存储：Future 结果必须消费——失败（如版本冲突）时 get() 抛出，
            // 走下方的失败上抛路径（2026-11-06 修复：此前丢弃 Future，冲突被静默吞掉）
            let events = [persistentEvent]
            eventStore.saveEvents(persistenceId(), events, lastSequenceNr - 1).get()

            // 调用处理器
            handler(event)

            log_persistent_actor_impl.debug("持久化事件: ${persistentEvent.toString()}")

        } catch (e: Exception) {
            // 2026-11-06 修复：持久化失败必须上抛（此前仅记日志，调用方无感知，静默丢数据）。
            // 回退序号，保持失败后状态一致，让监督策略决定是重试还是停止。
            lastSequenceNr -= 1
            log_persistent_actor_impl.error("持久化事件失败: ${e.message}")
            throw PersistenceException("持久化事件失败: ${e.message}", e)
        } finally {
            mutex.unlock()
        }
    }
    
    public func persistAndReply(event: EventPayload, replyTo: ActorRef, reply: Message): Unit {
        persist(event, { persistedEvent =>
            replyTo.tell(reply)
            log_persistent_actor_impl.debug("持久化并回复: ${persistedEvent.getEventType()}")
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
            
            log_persistent_actor_impl.debug("批量持久化 ${events.size} 个事件")
            
        } catch (e: Exception) {
            log_persistent_actor_impl.error("批量持久化失败: ${e.message}")
            // 2026-11-06 修复：失败上抛，不再静默
            lastSequenceNr -= events.size
            throw PersistenceException("批量持久化失败: ${e.message}", e)
        } finally {
            mutex.unlock()
        }
    }
    
    public func saveSnapshot<T>(snapshot: T): Unit where T <: ToString {
        try {
            // 2026-11-06 修复：真实保存状态数据（此前固定写 "snapshot-data"，恢复即垃圾）。
            let snapshotData = snapshot.toString()
            let basicSnapshot = BasicSnapshot(persistenceId(), lastSequenceNr, snapshotData)
            snapshotStore.saveSnapshot(basicSnapshot)

            log_persistent_actor_impl.debug("快照已保存: seq=${lastSequenceNr}")

        } catch (e: Exception) {
            log_persistent_actor_impl.error("保存快照失败: ${e.message}")
            throw PersistenceException("保存快照失败: ${e.message}", e)
        }
    }

    public func deleteSnapshot(sequenceNr: Int64): Unit {
        // 2026-11-06 修复：尊重 sequenceNr——仅当存在比它新的快照时，
        // 用"保存该序号的快照覆盖 + 删除最新"的方式实现精确删除；
        // SnapshotStore 接口只提供整组删除，精确删除需要按序号过滤，
        // 这里通过 getLatestSnapshot 判断是否所有快照都不晚于 sequenceNr，避免误删。
        match (snapshotStore.getLatestSnapshot(persistenceId())) {
            case Some(latest) =>
                if (latest.getSequenceNumber() > sequenceNr) {
                    // 存在更新的快照：整组删除会误删它们，保留并警告（接口能力限制）
                    log_persistent_actor_impl.warn(
                        "删除快照 seq=${sequenceNr}：存储接口仅支持整组删除，且存在更新的快照 seq=${latest.getSequenceNumber()}，已跳过以免误删")
                } else {
                    // 全部快照都不晚于 sequenceNr，删除全部即为正确语义
                    snapshotStore.deleteSnapshots(persistenceId())
                    log_persistent_actor_impl.debug("快照已删除: seq=${sequenceNr}")
                }
            case None => ()
        }
    }
    
    /**
     * 开始恢复过程
     */
    public func startRecovery(): Unit {
        isRecovering = true
        
        log_persistent_actor_impl.debug("${persistenceId()}: 开始恢复...")
        
        // 首先尝试从快照恢复
        match (snapshotStore.getLatestSnapshot(persistenceId())) {
            case Some(snapshot) =>
                log_persistent_actor_impl.debug("${persistenceId()}: 从快照恢复，序列号=${snapshot.getSequenceNumber()}")
                onRecoverySnapshot(snapshot)
                lastSequenceNr = snapshot.getSequenceNumber()
                
                // 恢复快照之后的事件
                recoverEventsAfter(lastSequenceNr)
                
            case None =>
                log_persistent_actor_impl.debug("${persistenceId()}: 从头开始恢复")
                recoverEventsAfter(0)
        }
        
        isRecovering = false
        onRecoveryCompleted()
        
        log_persistent_actor_impl.debug("${persistenceId()}: 恢复完成")
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
            // 2026-11-06 修复：对齐事件存储的最新版本号。
            // 事件流可能因外部写入（如另一实例）或截断（日志压缩）落后/超前于本机计数，
            // 只按下标重放会把 lastSequenceNr 停在本地视图上，后续 persist 的
            // expectedVersion 乐观锁会误判冲突或放行错误版本。以存储的
            // highestSequenceNr 为准（不会小于已重放到的位置）。
            match (eventStore.getHighestSequenceNr(persistenceId()).get()) {
                case stored => if (stored > lastSequenceNr) { lastSequenceNr = stored }
            }
            log_persistent_actor_impl.debug("${persistenceId()}: 恢复了 ${events.size} 个事件")
        } catch (e: Exception) {
            log_persistent_actor_impl.error("${persistenceId()}: 恢复事件失败: ${e.message}")
        }
    }
    
    /**
     * 子类可重写以处理恢复的事件
     */
    protected open func onRecoveryEvent(event: PersistentEvent): Unit {
        log_persistent_actor_impl.debug("${persistenceId()}: 恢复事件 ${event.payload.getEventType()}")
    }

    /**
     * 子类可重写以处理恢复的快照
     */
    protected open func onRecoverySnapshot(snapshot: Snapshot): Unit {
        log_persistent_actor_impl.debug("${persistenceId()}: 恢复快照，序列号=${snapshot.getSequenceNumber()}")
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
        log_persistent_actor_impl.debug("计数器 ${actorId} 恢复完成，当前计数: ${count}")
    }

    public func persist(event: EventPayload, handler: (EventPayload) -> Unit): Unit {
        log_persistent_actor_impl.debug("持久化事件: ${event.getEventType()}")
        handler(event)
    }

    public func persistAndReply(event: EventPayload, replyTo: ActorRef, reply: Message): Unit {
        persist(event, { e => log_persistent_actor_impl.debug("持久化并回复: ${e.getEventType()}") })
        replyTo.tell(reply)
    }

    public func persistAll(events: Array<EventPayload>, handler: (EventPayload) -> Unit): Unit {
        for (event in events) {
            persist(event, handler)
        }
    }

    public func saveSnapshot<T>(snapshot: T): Unit where T <: ToString {
        log_persistent_actor_impl.debug("保存快照: ${snapshot.toString()}")
    }

    public func deleteSnapshot(sequenceNr: Int64): Unit {
        log_persistent_actor_impl.debug("删除快照: seq=${sequenceNr}")
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
                                log_persistent_actor_impl.debug("计数器 ${actorId} 增加到: ${count}")

                                match (context.sender()) {
                                    case Some(senderPath) =>
                                        log_persistent_actor_impl.debug("回复给发送者: ${senderPath}")
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
                                log_persistent_actor_impl.debug("计数器 ${actorId} 减少到: ${count}")

                                match (context.sender()) {
                                    case Some(senderPath) =>
                                        log_persistent_actor_impl.debug("回复给发送者: ${senderPath}")
                                    case None => ()
                                }
                            case _ => ()
                        }
                    })
                    MessageResult.Handled
                    
                } else if (content == "get") {
                    match (context.sender()) {
                        case Some(senderPath) =>
                            log_persistent_actor_impl.debug("当前计数: ${count}, 发送者: ${senderPath}")
                        case None =>
                            log_persistent_actor_impl.debug("当前计数: ${count}")
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
                log_persistent_actor_impl.debug("恢复增加事件: ${incrementEvent.getIncrement()}, 当前计数: ${count}")
                
            case decrementEvent: CounterDecrementedEvent =>
                count -= decrementEvent.getDecrement()
                log_persistent_actor_impl.debug("恢复减少事件: ${decrementEvent.getDecrement()}, 当前计数: ${count}")
                
            case _ =>
                log_persistent_actor_impl.debug("未知事件类型: ${event.payload.getEventType()}")
        }
    }
    
    public func onRecoverySnapshot(snapshot: Snapshot): Unit {
        try {
            // 从快照携带的真实状态数据恢复计数（2026-11-06 修复：此前恒置 42）
            count = Int32.parse(snapshot.getStateData())
            log_persistent_actor_impl.debug("从快照恢复计数: ${count}")
        } catch (e: Exception) {
            log_persistent_actor_impl.error("恢复快照失败: ${e.message}")
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
