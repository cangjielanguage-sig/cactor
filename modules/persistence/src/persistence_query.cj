/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Persistence - 持久化查询（对标 Akka Persistence Query 的 ReadJournal）
 *
 * 读侧（CQRS 的查询端、投影、审计）按持久化标识读取事件流，不经过写侧的 Actor：
 * - currentEventsByPersistenceId：当前已有的事件（有限流），序号从 1 开始，[fromSeq, toSeq] 闭区间；
 * - eventsByPersistenceId：实时流，先交付已有事件，之后按 pollInterval 轮询新事件（与 Akka 多数日志插件相同），
 *   每条事件恰好交付一次、按序号递增，返回的订阅可随时取消；
 * - currentPersistenceIds / currentEventsByType：全部持久化标识；按事件类型跨标识查询（按时间戳、标识、序号排序）。
 * 事件序号即它在该标识日志中的位置（第几条），与 EventSourcedActor 的 lastSequenceNr 一致。
 */
package cactor_persistence

import std.sync.*
import std.time.*
import std.sort.*
import std.collection.*

/** 带序号的事件 */
public class EventEnvelope {
    public let persistenceId: String
    public let sequenceNr: Int64
    public let event: Event

    public init(persistenceId: String, sequenceNr: Int64, event: Event) {
        this.persistenceId = persistenceId
        this.sequenceNr = sequenceNr
        this.event = event
    }
}

/** 实时查询的订阅：cancel 后不再回调（正在进行的回调执行完为止） */
public class QuerySubscription {
    private let cancelled = AtomicBool(false)
    private let deliveredCount = AtomicInt64(0)
    private let lastSeq = AtomicInt64(0)

    init(fromSeq: Int64) {
        lastSeq.store(fromSeq - 1)
    }

    public func cancel(): Unit {
        cancelled.store(true)
    }

    public func isCancelled(): Bool {
        cancelled.load()
    }

    /** 已交付的事件数 */
    public func delivered(): Int64 {
        deliveredCount.load()
    }

    /** 最后交付的序号（可作为偏移量保存，重启后从 lastSequenceNr + 1 继续） */
    public func lastSequenceNr(): Int64 {
        lastSeq.load()
    }

    func advance(seq: Int64): Unit {
        lastSeq.store(seq)
        deliveredCount.fetchAdd(1)
    }
}

public class ReadJournal {
    private let store: EventStore
    private let pollInterval: Duration

    public init(store: EventStore) {
        this(store, Duration.millisecond * 100)
    }

    public init(store: EventStore, pollInterval: Duration) {
        if (pollInterval <= Duration.Zero) {
            throw IllegalArgumentException("轮询间隔必须为正: ${pollInterval}")
        }
        this.store = store
        this.pollInterval = pollInterval
    }

    public func currentEventsByPersistenceId(persistenceId: String, fromSeq: Int64, toSeq: Int64): Array<EventEnvelope> {
        let events = store.getEvents(persistenceId)
        let out = ArrayList<EventEnvelope>()
        let from = max(1, fromSeq)
        let to = min(toSeq, events.size)
        for (seq in from..=to) {
            out.add(EventEnvelope(persistenceId, seq, events[seq - 1]))
        }
        out.toArray()
    }

    public func currentEventsByPersistenceId(persistenceId: String): Array<EventEnvelope> {
        currentEventsByPersistenceId(persistenceId, 1, Int64.Max)
    }

    /**
     * 实时事件流：在后台线程中按序回调 onEvent（同一订阅的回调不会并发），回调抛出的异常会结束订阅并交给 onError。
     */
    public func eventsByPersistenceId(persistenceId: String, fromSeq: Int64, onEvent: (EventEnvelope) -> Unit,
        onError: (Exception) -> Unit): QuerySubscription {
        let sub = QuerySubscription(max(1, fromSeq))
        let st = store
        let interval = pollInterval
        spawn {
            while (!sub.isCancelled()) {
                try {
                    let events = st.getEvents(persistenceId)
                    var next = sub.lastSequenceNr() + 1
                    while (next <= events.size && !sub.isCancelled()) {
                        onEvent(EventEnvelope(persistenceId, next, events[next - 1]))
                        sub.advance(next)
                        next++
                    }
                } catch (e: Exception) {
                    sub.cancel()
                    onError(e)
                    return
                }
                sleep(interval)
            }
        }
        sub
    }

    public func eventsByPersistenceId(persistenceId: String, fromSeq: Int64, onEvent: (EventEnvelope) -> Unit): QuerySubscription {
        eventsByPersistenceId(persistenceId, fromSeq, onEvent, { _ => () })
    }

    public func currentPersistenceIds(): Array<String> {
        let ids = store.persistenceIds()
        sort(ids)
        ids
    }

    /** 全部标识中类型为 eventType 的事件，按（时间戳，标识，序号）排序 */
    public func currentEventsByType(eventType: String): Array<EventEnvelope> {
        let out = ArrayList<EventEnvelope>()
        for (id in store.persistenceIds()) {
            let events = store.getEvents(id)
            for (i in 0..events.size) {
                if (events[i].getEventType() == eventType) {
                    out.add(EventEnvelope(id, i + 1, events[i]))
                }
            }
        }
        let arr = out.toArray()
        sort(arr, lessThan: { a: EventEnvelope, b: EventEnvelope =>
            if (a.event.getTimestamp() != b.event.getTimestamp()) {
                a.event.getTimestamp() < b.event.getTimestamp()
            } else if (a.persistenceId != b.persistenceId) {
                a.persistenceId < b.persistenceId
            } else {
                a.sequenceNr < b.sequenceNr
            }
        })
        arr
    }
}
