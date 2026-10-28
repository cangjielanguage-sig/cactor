/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 事件溯源 Actor
 *
 * 子类只需实现：
 *   persistenceId()            持久化标识
 *   handleCommand(msg, ctx)    处理命令，需要改变状态时调用 persist(...)
 *   applyEvent(event)          根据事件更新内存状态（必须是确定性的）
 * 可选实现：
 *   snapshotState()/restoreSnapshot(state)  快照序列化与恢复
 *
 * 语义：
 * - persist 先写日志再更新状态，写失败时抛出异常、状态不变
 * - 创建时（preStart）以及被监督重启后（postRestart）自动恢复：
 *   最新快照 → 重放快照之后的事件
 * - 每 snapshotEvery 个事件自动保存一次快照（<= 0 表示不自动快照）
 */
package cactor_persistence

import std.sync.*
import cactor_core.actor.{Actor}
import cactor_core.message.{Message, MessageResult}
import cactor_core.context.{ActorContext}

public abstract class EventSourcedActor <: Actor {
    private let eventStore: EventStore
    private let snapshotStore: Option<SnapshotStore>
    private let snapshotEvery: Int64
    private var sequenceNr: Int64 = 0
    private var recovered: Bool = false
    private var replayedEvents: Int64 = 0
    private static let eventSequence: AtomicInt64 = AtomicInt64(0)

    public init(eventStore: EventStore) {
        this.eventStore = eventStore
        this.snapshotStore = None
        this.snapshotEvery = 0
    }

    public init(eventStore: EventStore, snapshotStore: SnapshotStore, snapshotEvery: Int64) {
        this.eventStore = eventStore
        this.snapshotStore = Some(snapshotStore)
        this.snapshotEvery = snapshotEvery
    }

    /** 持久化标识，同一实体在多次运行之间必须保持不变 */
    public func persistenceId(): String

    /** 处理命令消息 */
    public func handleCommand(message: Message, context: ActorContext): MessageResult

    /** 应用事件到内存状态 */
    public func applyEvent(event: Event): Unit

    /** 快照：把当前状态序列化为字符串（默认不支持快照） */
    public open func snapshotState(): Option<String> {
        None
    }

    /** 从快照恢复状态 */
    public open func restoreSnapshot(state: String): Unit {}

    /** 恢复完成回调 */
    public open func onRecoveryCompleted(): Unit {}

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (!recovered) {
            recover()
        }
        handleCommand(message, context)
    }

    public open func preStart(): Unit {
        recover()
    }

    public open func postRestart(reason: Exception): Unit {
        recover()
    }

    /**
     * 持久化一个事件：写入日志 → 应用到状态 → 按需保存快照
     */
    protected func persist(eventType: String, eventData: String): Event {
        let event = BasicEvent("${persistenceId()}-${sequenceNr + 1}-${eventSequence.fetchAdd(1)}", persistenceId(), eventType, eventData)
        eventStore.saveEvent(event)
        sequenceNr += 1
        applyEvent(event)
        if (snapshotEvery > 0 && sequenceNr % snapshotEvery == 0) {
            saveSnapshot()
        }
        event
    }

    /** 立即保存快照（子类不支持快照时忽略） */
    protected func saveSnapshot(): Unit {
        if (let Some(store) <- snapshotStore) {
            if (let Some(state) <- snapshotState()) {
                store.saveSnapshot(BasicSnapshot(persistenceId(), sequenceNr, state))
            }
        }
    }

    /** 当前已持久化的事件序号 */
    public func lastSequenceNr(): Int64 {
        sequenceNr
    }

    /** 最近一次恢复时重放的事件数（不含快照覆盖的部分） */
    public func replayedEventCount(): Int64 {
        replayedEvents
    }

    private func recover(): Unit {
        sequenceNr = 0
        replayedEvents = 0
        if (let Some(store) <- snapshotStore) {
            if (let Some(sn) <- store.getLatestSnapshot(persistenceId())) {
                restoreSnapshot(sn.getStateData())
                sequenceNr = sn.getSequenceNumber()
            }
        }
        let events = eventStore.getEvents(persistenceId())
        for (i in sequenceNr..events.size) {
            applyEvent(events[i])
            replayedEvents += 1
        }
        // 日志可能已被截断（例如只保留快照），序号不能回退
        if (events.size > sequenceNr) {
            sequenceNr = events.size
        }
        recovered = true
        onRecoveryCompleted()
    }
}
