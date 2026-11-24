/*
 * CActor Platform.Session - 会话级事件存储隔离（对标 Rivet 的"每 actor 一个库"）
 *
 * 多个会话共用同一个 `EventStore` 时，用 `ScopedEventStore` 给所有持久化标识加一个作用域前缀：
 *   - 不同会话的 persistenceId / actorId 即使同名也互不可见（读写都带前缀）；
 *   - `persistenceIds()` 只返回本作用域内的标识并去掉前缀；
 *   - `deleteScope()` 一次删除本会话的全部事件（会话归档/清理）；
 *   - `close()` 是空操作：底层 store 由多个作用域共享，生命周期由创建者管理。
 *
 * 前缀采用"长度:值"编码，避免 scope 或 id 内含分隔符时不同会话对撞成同一个键。
 */
package cactor_platform.session

import std.collection.*
import std.convert.*
import cactor_persistence.{Event, EventEnvelope, EventStore}

/** 把底层事件按外来视角还原（getActorId 返回原标识，而不是带前缀的键） */
class ViewEvent <: Event {
    private let inner: Event
    private let actorId: String

    init(inner: Event, actorId: String) {
        this.inner = inner
        this.actorId = actorId
    }

    public func getEventId(): String { inner.getEventId() }
    public func getActorId(): String { actorId }
    public func getTimestamp(): Int64 { inner.getTimestamp() }
    public func getEventType(): String { inner.getEventType() }
    public func getEventData(): String { inner.getEventData() }
    public func getTags(): Array<String> { inner.getTags() }
    public func toString(): String { inner.toString() }
}

/** 写入前给事件的 actorId 加作用域前缀（底层 append 要求 event.actorId == persistenceId） */
class ScopedEvent <: Event {
    private let inner: Event
    private let scopedActorId: String

    init(inner: Event, scopedActorId: String) {
        this.inner = inner
        this.scopedActorId = scopedActorId
    }

    public func getEventId(): String { inner.getEventId() }
    public func getActorId(): String { scopedActorId }
    public func getTimestamp(): Int64 { inner.getTimestamp() }
    public func getEventType(): String { inner.getEventType() }
    public func getEventData(): String { inner.getEventData() }
    public func getTags(): Array<String> { inner.getTags() }
    public func toString(): String { inner.toString() }
}

/**
 * 一个会话/actor 的事件存储视图。可安全地与其他作用域共享同一个底层 `EventStore`。
 */
public class ScopedEventStore <: EventStore {
    private let inner: EventStore
    private let scope: String

    public init(inner: EventStore, scope: String) {
        if (scope.isEmpty()) {
            throw IllegalArgumentException("存储作用域不能为空")
        }
        this.inner = inner
        this.scope = scope
    }

    public prop scopeName: String {
        get() { scope }
    }

    private func prefix(): String {
        "${scope.size}:${scope}/"
    }

    private func keyOf(id: String): String {
        "${prefix()}${id.size}:${id}"
    }

    /** 从带前缀的键还原原标识；不属于本作用域或格式不符返回 None */
    private func originalOf(key: String): Option<String> {
        let p = prefix()
        if (!key.startsWith(p)) {
            return None<String>
        }
        let rest = key[p.size..]
        match (rest.indexOf(":")) {
            case Some(i) =>
                let declared = Int64.tryParse(rest[..i]) ?? -1
                let value = rest[(i + 1)..]
                if (declared == Int64(value.size)) {
                    Some<String>(value)
                } else {
                    None<String>
                }
            case None => None<String>
        }
    }

    /** 本作用域内的原标识列表 */
    public func scopedIds(): Array<String> {
        let out = ArrayList<String>()
        for (id in inner.persistenceIds()) {
            match (originalOf(id)) {
                case Some(original) => out.add(original)
                case None => ()
            }
        }
        out.toArray()
    }

    /** 删除本作用域的全部事件（会话归档/清理） */
    public func deleteScope(): Unit {
        for (id in scopedIds()) {
            inner.deleteEvents(keyOf(id))
        }
    }

    public func saveEvent(event: Event): Unit {
        inner.saveEvent(ScopedEvent(event, keyOf(event.getActorId())))
    }

    public func getEvents(actorId: String): Array<Event> {
        let out = ArrayList<Event>()
        for (e in inner.getEvents(keyOf(actorId))) {
            out.add(ViewEvent(e, actorId))
        }
        out.toArray()
    }

    public func getEventsAfter(actorId: String, timestamp: Int64): Array<Event> {
        let out = ArrayList<Event>()
        for (e in inner.getEventsAfter(keyOf(actorId), timestamp)) {
            out.add(ViewEvent(e, actorId))
        }
        out.toArray()
    }

    public func deleteEvents(actorId: String): Unit {
        inner.deleteEvents(keyOf(actorId))
    }

    public func persistenceIds(): Array<String> {
        scopedIds()
    }

    public func highestSequenceNr(persistenceId: String): Int64 {
        inner.highestSequenceNr(keyOf(persistenceId))
    }

    public func append(persistenceId: String, events: Array<Event>, expectedSeq: Int64): Int64 {
        let scopedId = keyOf(persistenceId)
        let wrapped = Array<Event>(events.size, { i => ScopedEvent(events[i], scopedId) })
        inner.append(scopedId, wrapped, expectedSeq)
    }

    public func replay(persistenceId: String, fromSeq: Int64, toSeq: Int64): Array<EventEnvelope> {
        let out = ArrayList<EventEnvelope>()
        for (env in inner.replay(keyOf(persistenceId), fromSeq, toSeq)) {
            out.add(EventEnvelope(persistenceId, env.sequenceNr, ViewEvent(env.event, persistenceId)))
        }
        out.toArray()
    }

    public func deleteTo(persistenceId: String, toSeq: Int64): Unit {
        inner.deleteTo(keyOf(persistenceId), toSeq)
    }

    /** 共享底层 store：作用域视图不负责关闭它 */
    public func close(): Unit {}
}
