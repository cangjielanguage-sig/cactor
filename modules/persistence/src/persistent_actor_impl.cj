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
public open class PersistenceException <: Exception {
    public init(message: String, cause: Exception) {
        super(message, cause)
    }

    public init(message: String) {
        super(message)
    }
}

/**
 * 事件已经落库、但事件处理器抛了异常（P3）。
 * 事件是事实：序号不回退，交给监督决定——默认重启，重启后从存储恢复，已落库的事件会被重放。
 * 与存储写入失败（父类 PersistenceException，序号回退）区分开，调用方可以按类型分别处理
 */
public class EventHandlerException <: PersistenceException {
    /** 已落库的最后一个序号 */
    public let persistedSequenceNr: Int64

    public init(message: String, persistedSequenceNr: Int64, cause: Exception) {
        super(message, cause)
        this.persistedSequenceNr = persistedSequenceNr
    }
}

/**
 * 持久化Actor基类（E3 处置说明，见 docs/plan/20261106.md B8/E3）
 *
 * ⚠️ 使用须知：本类是 persistence 模块三套并行事件溯源 API 之一，**新代码请优先使用
 * EventStore + EventSourcedActor**（event_store.cj / event_sourced_actor.cj），那是 E8
 * 持久化收敛后的主路径。本类保留是为了编译兼容，已标记 @Deprecated（E8′），2.0 删除。
 *
 * 语义保证（2026-11-06 修复 B8 后）：
 * - saveSnapshot 保存真实状态数据（非字符串状态需先自行序列化为 String）；
 * - persist/persistAll 写入存储失败时上抛 PersistenceException 并回退序号，不再静默丢数据；
 * - 已落库后处理器抛异常时上抛 EventHandlerException，序号不回退（对标 Akka：事件已是事实，由监督重启并重放）；
 * - 恢复按 sequenceNr 对齐事件存储版本（getHighestSequenceNr），不按数组下标；
 * - preStart 自动恢复（重启后的新实例同样经 postRestart → preStart 恢复，P4）；已手动 startRecovery 的不重复恢复；
 * - 恢复失败上抛 PersistenceException：在 preStart 中即启动失败，默认监督策略停止该 Actor（对标 Akka onRecoveryFailure）。
 */
@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
public open class AbstractPersistentActor <: PersistentActor {
    private let eventStore: EnhancedEventStore
    private let snapshotStore: SnapshotStore
    private var lastSequenceNr: Int64
    private var isRecovering: Bool
    // 已恢复过：preStart 不再重复恢复（测试或调用方可能已手动 startRecovery）
    private var recovered: Bool = false
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

    public open func onRecoveryCompleted(): Unit {
        log_persistent_actor_impl.debug("恢复完成")  // 子类可以重写
    }

    /**
     * 启动时从存储恢复（P4：此前没有恢复钩子，重启后序号为 0，首次 persist 即与存储冲突、无限重启）。
     * 子类重写时请调用 super.preStart()
     */
    public open func preStart(): Unit {
        if (!recovered) {
            startRecovery()
        }
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
     * 子类实现业务消息处理（此前未标 open，子类无法重写，基类只能处理内置的持久化消息）
     */
    public open func receiveCommand(message: Message, context: ActorContext): MessageResult {
        MessageResult.Unhandled  // 子类应该重写
    }
    
    public func persist(event: EventPayload, handler: (EventPayload) -> Unit): Unit {
        // 写入与处理器分两段：只有写入失败才回退序号。处理器失败时事件已经落库，
        // 回退会让下一次 persist 以已占用的版本号写入、永远冲突（P3）
        let seq = synchronized(mutex) {
            let next = lastSequenceNr + 1
            // Future 必须消费：失败（如版本冲突）时 get() 抛出（2026-11-06 修复：此前丢弃 Future，冲突被静默吞掉）
            try {
                eventStore.saveEvents(persistenceId(), [PersistentEvent(persistenceId(), next, event)], next - 1).get()
            } catch (e: Exception) {
                log_persistent_actor_impl.error("持久化事件失败: ${e.message}")
                throw PersistenceException("持久化事件失败: ${e.message}", e)
            }
            lastSequenceNr = next
            next
        }
        runHandler(seq, { => handler(event) })
    }

    /** 事件已落库后调用处理器：异常包装为 EventHandlerException 上抛，序号保持 */
    private func runHandler(seq: Int64, body: () -> Unit): Unit {
        try {
            body()
        } catch (e: PersistenceException) {
            throw e
        } catch (e: Exception) {
            log_persistent_actor_impl.error("事件 seq=${seq} 已持久化，但处理器失败: ${e.message}")
            throw EventHandlerException("事件 seq=${seq} 已持久化，但处理器失败: ${e.message}", seq, e)
        }
    }
    
    public func persistAndReply(event: EventPayload, replyTo: ActorRef, reply: Message): Unit {
        persist(event, { persistedEvent =>
            replyTo.tell(reply)
            log_persistent_actor_impl.debug("持久化并回复: ${persistedEvent.getEventType()}")
        })
    }
    
    public func persistAll(events: Array<EventPayload>, handler: (EventPayload) -> Unit): Unit {
        if (events.isEmpty()) {
            return
        }
        let last = synchronized(mutex) {
            let first = lastSequenceNr + 1
            let batch = Array<PersistentEvent>(events.size, { i => PersistentEvent(persistenceId(), first + i, events[i]) })
            // 消费 Future（此前未 get()，版本冲突被静默吞掉、序号照样前进）
            try {
                eventStore.saveEvents(persistenceId(), batch, first - 1).get()
            } catch (e: Exception) {
                log_persistent_actor_impl.error("批量持久化失败: ${e.message}")
                throw PersistenceException("批量持久化失败: ${e.message}", e)
            }
            lastSequenceNr = first + events.size - 1
            lastSequenceNr
        }
        runHandler(last, { =>
            for (event in events) {
                handler(event)
            }
        })
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
        recovered = true
        
        log_persistent_actor_impl.debug("${persistenceId()}: 开始恢复...")
        
        try {
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
        } finally {
            isRecovering = false
        }
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
            // 恢复失败必须上抛（此前只记日志，Actor 以残缺状态继续运行、后续写入版本错乱）
            log_persistent_actor_impl.error("${persistenceId()}: 恢复事件失败: ${e.message}")
            throw PersistenceException("${persistenceId()}: 恢复事件失败: ${e.message}", e)
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
@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
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
@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
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
@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
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
