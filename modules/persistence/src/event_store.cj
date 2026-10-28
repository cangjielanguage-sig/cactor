package cactor_persistence

import std.collection.*
import std.time.*
import std.sync.*
import std.fs.*

// 事件接口
public interface Event {
    func getEventId(): String
    func getActorId(): String
    func getTimestamp(): Int64
    func getEventType(): String
    func getEventData(): String
    func toString(): String
}

// 基础事件实现
public class BasicEvent <: Event {
    private let eventId: String
    private let actorId: String
    private let timestamp: Int64
    private let eventType: String
    private let eventData: String
    
    public init(eventId: String, actorId: String, eventType: String, eventData: String) {
        this(eventId, actorId, eventType, eventData, DateTime.now().toUnixTimeStamp().toMilliseconds())
    }

    /** 从存储恢复时使用：保留原始时间戳 */
    public init(eventId: String, actorId: String, eventType: String, eventData: String, timestamp: Int64) {
        this.eventId = eventId
        this.actorId = actorId
        this.timestamp = timestamp
        this.eventType = eventType
        this.eventData = eventData
    }
    
    public func getEventId(): String { eventId }
    public func getActorId(): String { actorId }
    public func getTimestamp(): Int64 { timestamp }
    public func getEventType(): String { eventType }
    public func getEventData(): String { eventData }
    
    public func toString(): String {
        "Event(${eventId}, ${actorId}, ${eventType}, ${timestamp})"
    }
}

// 事件存储接口
public interface EventStore {
    func saveEvent(event: Event): Unit
    func getEvents(actorId: String): Array<Event>
    func getEventsAfter(actorId: String, timestamp: Int64): Array<Event>
    func deleteEvents(actorId: String): Unit
    func close(): Unit
}

// 内存事件存储
public class MemoryEventStore <: EventStore {
    private let events: HashMap<String, ArrayList<Event>>
    private let mutex: Mutex
    
    public init() {
        this.events = HashMap<String, ArrayList<Event>>()
        this.mutex = Mutex()
    }
    
    public func saveEvent(event: Event): Unit {
        mutex.lock()
        try {
            let actorId = event.getActorId()
            match (events.get(actorId)) {
                case Some(eventList) => 
                    eventList.add(event)
                case None =>
                    let newList = ArrayList<Event>()
                    newList.add(event)
                    events.add(actorId, newList)
            }
        } finally {
            mutex.unlock()
        }
    }
    
    public func getEvents(actorId: String): Array<Event> {
        mutex.lock()
        try {
            match (events.get(actorId)) {
                case Some(eventList) =>
                    let result = ArrayList<Event>()
                    for (event in eventList) {
                        result.add(event)
                    }
                    result.toArray()
                case None =>
                    Array<Event>()
            }
        } finally {
            mutex.unlock()
        }
    }
    
    public func getEventsAfter(actorId: String, timestamp: Int64): Array<Event> {
        mutex.lock()
        try {
            match (events.get(actorId)) {
                case Some(eventList) =>
                    let result = ArrayList<Event>()
                    for (event in eventList) {
                        if (event.getTimestamp() > timestamp) {
                            result.add(event)
                        }
                    }
                    result.toArray()
                case None =>
                    Array<Event>()
            }
        } finally {
            mutex.unlock()
        }
    }
    
    public func deleteEvents(actorId: String): Unit {
        mutex.lock()
        try {
            events.remove(actorId)
        } finally {
            mutex.unlock()
        }
    }
    
    public func close(): Unit {
        // 内存存储无需关闭操作
    }
}

/**
 * 文件事件存储
 *
 * - 每个 Actor 一个追加写的日志文件：<basePath>/<hex(actorId)>.journal
 * - 每条事件一条带长度前缀的二进制记录，写入后 flush
 * - 读取时忽略写入中断留下的残缺尾部，下一次写入前截断该尾部，保证日志可继续追加
 * - 进程重启后可以完整恢复事件
 */
public class FileEventStore <: EventStore {
    private let basePath: String
    private let mutex: Mutex

    public init(basePath: String) {
        this.basePath = basePath
        this.mutex = Mutex()
        ensureDirectory(basePath)
    }

    private func journalPath(actorId: String): String {
        "${basePath}/${safeFileName(actorId)}.journal"
    }

    public func saveEvent(event: Event): Unit {
        let record = RecordWriter()
            .putString(event.getEventId())
            .putString(event.getActorId())
            .putString(event.getEventType())
            .putString(event.getEventData())
            .putInt64(event.getTimestamp())
            .toRecord()
        synchronized(mutex) {
            let path = journalPath(event.getActorId())
            if (exists(path)) {
                truncateTornTail(path)
                let f = File(path, OpenMode.Append)
                try {
                    f.write(record)
                    f.flush()
                } finally {
                    f.close()
                }
            } else {
                File.writeTo(path, record)
            }
        }
    }

    public func getEvents(actorId: String): Array<Event> {
        synchronized(mutex) {
            readJournal(actorId)
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

    /** 事件条数 */
    public func countEvents(actorId: String): Int64 {
        getEvents(actorId).size
    }

    public func deleteEvents(actorId: String): Unit {
        synchronized(mutex) {
            let path = journalPath(actorId)
            if (exists(path)) {
                remove(path)
            }
        }
    }

    public func close(): Unit {}

    private func readJournal(actorId: String): Array<Event> {
        let path = journalPath(actorId)
        if (!exists(path)) {
            return Array<Event>()
        }
        let (records, _) = splitRecords(File.readFrom(path))
        let events = ArrayList<Event>()
        for (payload in records) {
            let r = RecordReader(payload)
            let eventId = r.getString()
            let owner = r.getString()
            let eventType = r.getString()
            let eventData = r.getString()
            let ts = r.getInt64()
            events.add(BasicEvent(eventId, owner, eventType, eventData, ts))
        }
        events.toArray()
    }

    /** 截断写入中断留下的残缺尾部 */
    private func truncateTornTail(path: String): Unit {
        let content = File.readFrom(path)
        let (_, validLength) = splitRecords(content)
        if (validLength < content.size) {
            let f = File(path, OpenMode.ReadWrite)
            try {
                f.setLength(validLength)
            } finally {
                f.close()
            }
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
