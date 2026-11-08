/*
 * CActor Persistence - 事件与事件存储（日志）接口、内存实现、文件实现（日志 v2 见 journal_v2.cj）
 */
package cactor_persistence

import std.collection.*
import std.time.*
import std.sync.*
import std.fs.*
import std.sort.*

// 事件接口
public interface Event {
    func getEventId(): String
    func getActorId(): String
    func getTimestamp(): Int64
    func getEventType(): String
    func getEventData(): String
    func toString(): String

    /** 标签（ReadJournal.currentEventsByTag 按它跨标识查询，对标 Akka Tagged）；默认没有 */
    func getTags(): Array<String> {
        Array<String>()
    }
}

// 基础事件实现
public class BasicEvent <: Event {
    private let eventId: String
    private let actorId: String
    private let timestamp: Int64
    private let eventType: String
    private let eventData: String
    private let tags: Array<String>

    public init(eventId: String, actorId: String, eventType: String, eventData: String) {
        this(eventId, actorId, eventType, eventData, DateTime.now().toUnixTimeStamp().toMilliseconds())
    }

    /** 从存储恢复时使用：保留原始时间戳 */
    public init(eventId: String, actorId: String, eventType: String, eventData: String, timestamp: Int64) {
        this(eventId, actorId, eventType, eventData, timestamp, Array<String>())
    }

    public init(eventId: String, actorId: String, eventType: String, eventData: String, timestamp: Int64, tags: Array<String>) {
        this.eventId = eventId
        this.actorId = actorId
        this.timestamp = timestamp
        this.eventType = eventType
        this.eventData = eventData
        this.tags = tags
    }

    /** 带上标签的副本 */
    public func withTags(tags: Array<String>): BasicEvent {
        BasicEvent(eventId, actorId, eventType, eventData, timestamp, tags)
    }

    public func getEventId(): String { eventId }
    public func getActorId(): String { actorId }
    public func getTimestamp(): Int64 { timestamp }
    public func getEventType(): String { eventType }
    public func getEventData(): String { eventData }
    public func getTags(): Array<String> { tags }

    public func toString(): String {
        "Event(${eventId}, ${actorId}, ${eventType}, ${timestamp})"
    }
}

/**
 * 事件存储（日志）。新代码用 append / replay / highestSequenceNr / deleteTo（E8′，对标 Akka journal）：
 * - 序号从 1 连续递增，deleteTo 之后最高序号保留；
 * - append 带期望序号，同一标识出现第二个写者时被拒绝（JournalConflictException，P10）；
 * - FileEventStore 的 append 返回时事件已 fsync 落盘（可关闭）。
 * saveEvent / getEvents / getEventsAfter / deleteEvents 是旧接口，保留兼容（saveEvent 不校验序号）。
 * 只实现旧接口的自定义存储也能用新接口：默认实现按位置编号（不支持 deleteTo，append 的校验不是原子的）。
 */
public interface EventStore {
    func saveEvent(event: Event): Unit
    func getEvents(actorId: String): Array<Event>
    func getEventsAfter(actorId: String, timestamp: Int64): Array<Event>
    func deleteEvents(actorId: String): Unit
    func close(): Unit

    /** 有事件的全部持久化标识（供 Persistence Query 使用；默认不支持，返回空） */
    func persistenceIds(): Array<String> {
        Array<String>()
    }

    /** 当前最高序号（没有事件时为 0；deleteTo 之后保留） */
    func highestSequenceNr(persistenceId: String): Int64 {
        getEvents(persistenceId).size
    }

    /**
     * 追加一批事件：expectedSeq 必须等于当前最高序号，否则抛 JournalConflictException、一条也不写；
     * 返回追加后的最高序号。事件的 getActorId() 必须是 persistenceId
     */
    func append(persistenceId: String, events: Array<Event>, expectedSeq: Int64): Int64 {
        checkOwner(persistenceId, events)
        let current = highestSequenceNr(persistenceId)
        if (current != expectedSeq) {
            throw JournalConflictException(persistenceId, expectedSeq, current)
        }
        for (e in events) {
            saveEvent(e)
        }
        current + events.size
    }

    /** 序号在 [fromSeq, toSeq] 内的事件（闭区间，按序号递增） */
    func replay(persistenceId: String, fromSeq: Int64, toSeq: Int64): Array<EventEnvelope> {
        let events = getEvents(persistenceId)
        let out = ArrayList<EventEnvelope>()
        let from = max(1, fromSeq)
        let to = min(toSeq, events.size)
        for (seq in from..=to) {
            out.add(EventEnvelope(persistenceId, seq, events[seq - 1]))
        }
        out.toArray()
    }

    /** 删除序号 ≤ toSeq 的事件，最高序号保留（通常在保存快照之后调用） */
    func deleteTo(persistenceId: String, toSeq: Int64): Unit {
        throw UnsupportedException("该事件存储不支持 deleteTo（${persistenceId} ≤ ${toSeq}）")
    }
}

func checkOwner(persistenceId: String, events: Array<Event>): Unit {
    for (e in events) {
        if (e.getActorId() != persistenceId) {
            throw IllegalArgumentException("事件 ${e.getEventId()} 属于 ${e.getActorId()}，不能追加到 ${persistenceId}")
        }
    }
}

/** 内存日志：一个标识的事件与序号状态 */
class MemoryJournal {
    let entries = ArrayList<EventEnvelope>()
    var highest: Int64 = 0
    var deletedTo: Int64 = 0
}

// 内存事件存储（语义与 FileEventStore 相同，只是不落盘）
public class MemoryEventStore <: EventStore {
    private let journals = HashMap<String, MemoryJournal>()
    private let mutex = Mutex()

    public init() {}

    private func journalOf(id: String): MemoryJournal {
        match (journals.get(id)) {
            case Some(j) => j
            case None =>
                let j = MemoryJournal()
                journals[id] = j
                j
        }
    }

    public func saveEvent(event: Event): Unit {
        synchronized(mutex) {
            let j = journalOf(event.getActorId())
            j.highest++
            j.entries.add(EventEnvelope(event.getActorId(), j.highest, event))
        }
    }

    public func append(persistenceId: String, events: Array<Event>, expectedSeq: Int64): Int64 {
        checkOwner(persistenceId, events)
        synchronized(mutex) {
            let j = journalOf(persistenceId)
            if (j.highest != expectedSeq) {
                throw JournalConflictException(persistenceId, expectedSeq, j.highest)
            }
            for (e in events) {
                j.highest++
                j.entries.add(EventEnvelope(persistenceId, j.highest, e))
            }
            j.highest
        }
    }

    public func highestSequenceNr(persistenceId: String): Int64 {
        synchronized(mutex) {
            match (journals.get(persistenceId)) {
                case Some(j) => j.highest
                case None => 0
            }
        }
    }

    public func replay(persistenceId: String, fromSeq: Int64, toSeq: Int64): Array<EventEnvelope> {
        synchronized(mutex) {
            let out = ArrayList<EventEnvelope>()
            if (let Some(j) <- journals.get(persistenceId)) {
                for (env in j.entries) {
                    if (env.sequenceNr >= fromSeq && env.sequenceNr <= toSeq) {
                        out.add(env)
                    }
                }
            }
            out.toArray()
        }
    }

    public func deleteTo(persistenceId: String, toSeq: Int64): Unit {
        synchronized(mutex) {
            if (let Some(j) <- journals.get(persistenceId)) {
                let upTo = min(toSeq, j.highest)
                if (upTo > j.deletedTo) {
                    j.entries.removeIf({ env => env.sequenceNr <= upTo })
                    j.deletedTo = upTo
                }
            }
        }
    }

    public func getEvents(actorId: String): Array<Event> {
        synchronized(mutex) {
            match (journals.get(actorId)) {
                case Some(j) => Array<Event>(j.entries.size, { i => j.entries[i].event })
                case None => Array<Event>()
            }
        }
    }

    public func getEventsAfter(actorId: String, timestamp: Int64): Array<Event> {
        let result = ArrayList<Event>()
        for (e in getEvents(actorId)) {
            if (e.getTimestamp() > timestamp) {
                result.add(e)
            }
        }
        result.toArray()
    }

    public func deleteEvents(actorId: String): Unit {
        synchronized(mutex) {
            journals.remove(actorId)
        }
    }

    public func persistenceIds(): Array<String> {
        synchronized(mutex) {
            let ids = ArrayList<String>()
            for ((id, j) in journals) {
                if (j.highest > 0) {
                    ids.add(id)
                }
            }
            ids.toArray()
        }
    }

    public func close(): Unit {}
}

/**
 * 文件事件存储（日志 v2，格式与恢复规则见 journal_v2.cj）
 *
 * - 每个标识一个追加写的日志文件：<basePath>/<hex(标识)>.journal；v1 格式的旧文件首次打开时自动迁移
 * - fsync（默认开启）：append / saveEvent 返回时事件已落盘，并发写同一日志的写者合并为一次 fsync（组提交）；
 *   fsync = false 时只保证进程崩溃（kill -9）不丢，不保证断电不丢
 * - 常驻追加句柄，超过 maxOpenFiles 个时关闭最近最少使用的（关闭前把未落盘的写入 fsync）
 * - 同一进程里可以对同一目录建多个存储实例（例如模拟重启）：每次操作前比较文件大小，被别的实例改过就重新扫描
 */
public class FileEventStore <: EventStore {
    private let basePath: String
    private let sync: Bool
    private let maxOpenFiles: Int64
    private let mutex: Mutex
    private let journals = HashMap<String, JournalFile>()
    private let openHandles = AtomicInt64(0)

    public init(basePath: String) {
        this(basePath, fsync: true, maxOpenFiles: 256)
    }

    public init(basePath: String, fsync!: Bool, maxOpenFiles!: Int64 = 256) {
        this.basePath = basePath
        this.sync = fsync
        this.maxOpenFiles = max(1, maxOpenFiles)
        this.mutex = Mutex()
        ensureDirectory(basePath)
    }

    private func journalPath(actorId: String): String {
        "${basePath}/${safeFileName(actorId)}.journal"
    }

    private func journalOf(id: String): JournalFile {
        synchronized(mutex) {
            match (journals.get(id)) {
                case Some(j) => j
                case None =>
                    let j = JournalFile(id, journalPath(id), basePath, sync, openHandles)
                    journals[id] = j
                    j
            }
        }
    }

    /** 打开的句柄超过上限时关闭最近最少使用的（在存储锁之外关闭，不与日志锁嵌套） */
    private func evictIfNeeded(current: JournalFile): Unit {
        if (openHandles.load() <= maxOpenFiles) {
            return
        }
        let candidates = synchronized(mutex) {
            let l = ArrayList<JournalFile>()
            for ((_, j) in journals) {
                if (!refEq(j, current)) {
                    l.add(j)
                }
            }
            l.toArray()
        }
        sort(candidates, lessThan: { a: JournalFile, b: JournalFile => a.lastUse < b.lastUse })
        for (j in candidates) {
            if (openHandles.load() <= maxOpenFiles) {
                break
            }
            if (j.hasOpenHandle()) {
                j.release()
            }
        }
    }

    public func append(persistenceId: String, events: Array<Event>, expectedSeq: Int64): Int64 {
        checkOwner(persistenceId, events)
        let j = journalOf(persistenceId)
        let r = j.append(events, Some(expectedSeq))
        evictIfNeeded(j)
        r
    }

    public func saveEvent(event: Event): Unit {
        let j = journalOf(event.getActorId())
        j.append([event], None<Int64>)
        evictIfNeeded(j)
    }

    public func highestSequenceNr(persistenceId: String): Int64 {
        journalOf(persistenceId).highestSequenceNr()
    }

    public func replay(persistenceId: String, fromSeq: Int64, toSeq: Int64): Array<EventEnvelope> {
        journalOf(persistenceId).replay(fromSeq, toSeq)
    }

    public func deleteTo(persistenceId: String, toSeq: Int64): Unit {
        journalOf(persistenceId).deleteTo(toSeq)
    }

    public func getEvents(actorId: String): Array<Event> {
        let envs = replay(actorId, 1, Int64.Max)
        Array<Event>(envs.size, { i => envs[i].event })
    }

    public func getEventsAfter(actorId: String, timestamp: Int64): Array<Event> {
        let result = ArrayList<Event>()
        for (e in getEvents(actorId)) {
            if (e.getTimestamp() > timestamp) {
                result.add(e)
            }
        }
        result.toArray()
    }

    /** 事件条数（不含已 deleteTo 的） */
    public func countEvents(actorId: String): Int64 {
        replay(actorId, 1, Int64.Max).size
    }

    /** 当前打开的追加句柄数 */
    public func openFileCount(): Int64 {
        openHandles.load()
    }

    public func persistenceIds(): Array<String> {
        let ids = ArrayList<String>()
        for (info in Directory.readFrom(basePath)) {
            let name = info.name
            if (name.endsWith(".journal")) {
                let stem = name[0..(name.size - ".journal".size)]
                // 哈希文件名（标识过长）：原标识在文件头里
                let id = if (isHashedName(stem)) { journalIdOf("${basePath}/${name}") } else { unhexName(stem) }
                if (let Some(x) <- id) {
                    ids.add(x)
                }
            }
        }
        ids.toArray()
    }

    public func deleteEvents(actorId: String): Unit {
        journalOf(actorId).deleteAll()
    }

    /** 关闭全部追加句柄（未落盘的写入先 fsync）；之后仍可继续使用，句柄按需重新打开 */
    public func close(): Unit {
        let all = synchronized(mutex) {
            let l = ArrayList<JournalFile>()
            for ((_, j) in journals) {
                l.add(j)
            }
            l.toArray()
        }
        for (j in all) {
            j.release()
        }
    }
}

// 事件存储工厂
public class EventStoreFactory {
    public static func createMemoryStore(): EventStore {
        MemoryEventStore()
    }
    
    public static func createFileStore(basePath: String): EventStore {
        FileEventStore(basePath)
    }
}
