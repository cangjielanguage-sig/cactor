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
 * 语义（E8′，对标 Akka PersistentActor）：
 * - persist 先写日志（append 带期望序号，返回时已落盘）再更新状态；写失败抛 PersistenceException、状态不变；
 *   日志里已有别的写者写入的事件（同一标识两个实例）时抛 JournalConflictException，交给监督（重启后从日志恢复）；
 * - persistAll 一批事件一次写入、一次 fsync（组提交），全部写入后依次应用；
 * - 创建时（preStart）以及被监督重启后（postRestart）自动恢复：最新快照 → 按序号重放快照之后的事件
 *   （此前按数组位置重放，deleteTo 之后会错位）；
 * - 事件处理器（applyEvent）抛异常时事件已落库、序号不回退，异常交给监督，重启后重放；
 * - 每 snapshotEvery 个事件自动保存一次快照（<= 0 表示不自动快照）；deleteEventsTo 删除快照已覆盖的事件。
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
        persist(eventType, eventData, Array<String>())
    }

    /** 带标签持久化（ReadJournal.currentEventsByTag 可跨标识查询） */
    protected func persist(eventType: String, eventData: String, tags: Array<String>): Event {
        persistAll([(eventType, eventData)], tags)[0]
    }

    /** 一批事件：一次写入、一次 fsync，全部落盘后依次应用（要么全部写入，要么一条也不写） */
    protected func persistAll(events: Array<(String, String)>): Array<Event> {
        persistAll(events, Array<String>())
    }

    protected func persistAll(events: Array<(String, String)>, tags: Array<String>): Array<Event> {
        let id = persistenceId()
        let before = sequenceNr
        let batch = Array<Event>(events.size, { i =>
            let (eventType, eventData) = events[i]
            BasicEvent("${id}-${before + 1 + i}-${eventSequence.fetchAdd(1)}", id, eventType, eventData).withTags(tags)
        })
        sequenceNr = eventStore.append(id, batch, before)
        for (e in batch) {
            applyEvent(e)
        }
        if (snapshotEvery > 0 && sequenceNr / snapshotEvery > before / snapshotEvery) {
            saveSnapshot()
        }
        batch
    }

    /** 删除序号 ≤ toSequenceNr 的事件（通常在保存快照之后，对标 Akka deleteMessages）；最高序号保留 */
    protected func deleteEventsTo(toSequenceNr: Int64): Unit {
        eventStore.deleteTo(persistenceId(), toSequenceNr)
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
        let id = persistenceId()
        if (let Some(store) <- snapshotStore) {
            if (let Some(sn) <- store.getLatestSnapshot(id)) {
                restoreSnapshot(sn.getStateData())
                sequenceNr = sn.getSequenceNumber()
            }
        }
        for (env in eventStore.replay(id, sequenceNr + 1, Int64.Max)) {
            applyEvent(env.event)
            replayedEvents += 1
            sequenceNr = env.sequenceNr
        }
        // 快照之后的事件已被删除（deleteTo 越过了快照）时序号仍不能回退：下一次追加的期望序号是日志的最高序号
        let highest = eventStore.highestSequenceNr(id)
        if (highest > sequenceNr) {
            sequenceNr = highest
        }
        recovered = true
        onRecoveryCompleted()
    }
}
