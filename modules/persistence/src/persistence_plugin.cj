package cactor_persistence

import cactor_core.message.*
import std.collection.*
import std.sync.*
import cactor_foundation.logging.{Logger, Logging}

let log_persistence_plugin: Logger = Logging.logger("cactor_persistence.persistence_plugin")

// 持久化配置
@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
public class PersistenceConfig {
    private let enabled: Bool
    private let eventStoreType: String
    private let snapshotStoreType: String
    private let eventStorePath: String
    private let snapshotStorePath: String
    private let snapshotInterval: Int64
    private let autoRecover: Bool
    
    public init(
        enabled: Bool,
        eventStoreType: String,
        snapshotStoreType: String,
        eventStorePath: String,
        snapshotStorePath: String,
        snapshotInterval: Int64,
        autoRecover: Bool
    ) {
        this.enabled = enabled
        this.eventStoreType = eventStoreType
        this.snapshotStoreType = snapshotStoreType
        this.eventStorePath = eventStorePath
        this.snapshotStorePath = snapshotStorePath
        this.snapshotInterval = snapshotInterval
        this.autoRecover = autoRecover
    }
    
    public func isEnabled(): Bool { enabled }
    public func getEventStoreType(): String { eventStoreType }
    public func getSnapshotStoreType(): String { snapshotStoreType }
    public func getEventStorePath(): String { eventStorePath }
    public func getSnapshotStorePath(): String { snapshotStorePath }
    public func getSnapshotInterval(): Int64 { snapshotInterval }
    public func isAutoRecover(): Bool { autoRecover }
    
    // 工厂方法
    public static func createDefault(): PersistenceConfig {
        PersistenceConfig(false, "memory", "memory", "./data/events", "./data/snapshots", 100, true)
    }
    
    public static func createEnabled(eventStoreType: String, snapshotStoreType: String): PersistenceConfig {
        PersistenceConfig(true, eventStoreType, snapshotStoreType, "./data/events", "./data/snapshots", 100, true)
    }
}

// 持久化插件接口
@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
public interface PersistencePlugin {
    func isEnabled(): Bool
    func persistEvent(actorName: String, eventType: String, eventData: String): Unit
    func recoverActor(actorName: String, onEvent: (Event) -> Unit, onSnapshot: (Snapshot) -> Unit): Unit
    func takeSnapshot(actorName: String, stateData: String): Unit
    func deleteData(actorName: String): Unit
    func close(): Unit
}

// 默认持久化插件实现
@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
public class DefaultPersistencePlugin <: PersistencePlugin {
    private let config: PersistenceConfig
    private let eventStore: EventStore
    private let snapshotStore: SnapshotStore
    private let sequenceNumbers: HashMap<String, Int64>
    private let mutex: Mutex
    
    public init(config: PersistenceConfig) {
        this.config = config
        this.eventStore = if (config.isEnabled()) {
            match (config.getEventStoreType()) {
                case "file" => EventStoreFactory.createFileStore(config.getEventStorePath())
                case _ => EventStoreFactory.createMemoryStore()
            }
        } else {
            EventStoreFactory.createMemoryStore()
        }
        this.snapshotStore = if (config.isEnabled()) {
            match (config.getSnapshotStoreType()) {
                case "file" => SnapshotStoreFactory.createFileStore(config.getSnapshotStorePath())
                case _ => SnapshotStoreFactory.createMemoryStore()
            }
        } else {
            SnapshotStoreFactory.createMemoryStore()
        }
        this.sequenceNumbers = HashMap<String, Int64>()
        this.mutex = Mutex()
    }
    
    public func isEnabled(): Bool {
        config.isEnabled()
    }
    
    public func persistEvent(actorName: String, eventType: String, eventData: String): Unit {
        if (!config.isEnabled()) {
            return
        }
        
        mutex.lock()
        try {
            // 期望序号取已知的最高序号（首次使用时从日志读取）：同一标识的另一个写者先写入时追加被拒绝（P10）
            let expected = match (sequenceNumbers.get(actorName)) {
                case Some(seq) => seq
                case None => eventStore.highestSequenceNr(actorName)
            }
            let currentSeq = expected + 1
            let event = BasicEvent("${actorName}_${currentSeq}", actorName, eventType, eventData)
            eventStore.append(actorName, [event], expected)
            sequenceNumbers[actorName] = currentSeq
            
            // 检查是否需要快照
            if (currentSeq % config.getSnapshotInterval() == 0) {
                log_persistence_plugin.debug("${actorName}: 达到快照间隔，序列号=${currentSeq}")
            }
        } finally {
            mutex.unlock()
        }
    }
    
    public func recoverActor(actorName: String, onEvent: (Event) -> Unit, onSnapshot: (Snapshot) -> Unit): Unit {
        if (!config.isEnabled() || !config.isAutoRecover()) {
            return
        }
        
        log_persistence_plugin.debug("${actorName}: 开始恢复...")
        
        // 首先尝试从快照恢复
        match (snapshotStore.getLatestSnapshot(actorName)) {
            case Some(snapshot) => 
                log_persistence_plugin.debug("${actorName}: 从快照恢复，序列号=${snapshot.getSequenceNumber()}")
                onSnapshot(snapshot)
                
                // 按序号重放快照之后的事件（P5：此前按时间戳，同一毫秒内的事件会重复或遗漏）
                for (env in eventStore.replay(actorName, snapshot.getSequenceNumber() + 1, Int64.Max)) {
                    onEvent(env.event)
                }
                
                mutex.lock()
                sequenceNumbers[actorName] = max(snapshot.getSequenceNumber(), eventStore.highestSequenceNr(actorName))
                mutex.unlock()
                
            case None => 
                log_persistence_plugin.debug("${actorName}: 从头开始恢复")
                // 恢复所有事件；最高序号取日志的（P5：此前恒为 1，之后的快照序号全错）
                for (env in eventStore.replay(actorName, 1, Int64.Max)) {
                    onEvent(env.event)
                }
                
                mutex.lock()
                sequenceNumbers[actorName] = eventStore.highestSequenceNr(actorName)
                mutex.unlock()
        }
        
        log_persistence_plugin.debug("${actorName}: 恢复完成")
    }
    
    public func takeSnapshot(actorName: String, stateData: String): Unit {
        if (!config.isEnabled()) {
            return
        }
        
        mutex.lock()
        let currentSeq = match (sequenceNumbers.get(actorName)) {
            case Some(seq) => seq
            case None => 0
        }
        mutex.unlock()
        
        let snapshot = BasicSnapshot(actorName, currentSeq, stateData)
        snapshotStore.saveSnapshot(snapshot)
        
        log_persistence_plugin.debug("${actorName}: 快照已保存，序列号=${currentSeq}")
    }
    
    public func deleteData(actorName: String): Unit {
        eventStore.deleteEvents(actorName)
        snapshotStore.deleteSnapshots(actorName)
        
        mutex.lock()
        sequenceNumbers.remove(actorName)
        mutex.unlock()
        
        log_persistence_plugin.debug("${actorName}: 持久化数据已清理")
    }
    
    public func close(): Unit {
        eventStore.close()
        snapshotStore.close()
    }
}

// 持久化插件工厂
@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
public class PersistencePluginFactory {
    public static func create(config: PersistenceConfig): PersistencePlugin {
        DefaultPersistencePlugin(config)
    }
    
    public static func createDisabled(): PersistencePlugin {
        DefaultPersistencePlugin(PersistenceConfig.createDefault())
    }
    
    public static func createMemoryEnabled(): PersistencePlugin {
        DefaultPersistencePlugin(PersistenceConfig.createEnabled("memory", "memory"))
    }
    
    public static func createFileEnabled(): PersistencePlugin {
        DefaultPersistencePlugin(PersistenceConfig.createEnabled("file", "file"))
    }
}

// 持久化消息类型
@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
public class PersistEventMessage <: Message {
    private let eventType: String
    private let eventData: String
    
    public init(eventType: String, eventData: String) {
        this.eventType = eventType
        this.eventData = eventData
    }
    
    public func getEventType(): String { eventType }
    public func getEventData(): String { eventData }
    public func messageType(): String { "PersistEventMessage" }

    public func toString(): String {
        "PersistEventMessage(${eventType}, ${eventData})"
    }
}

@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
public class TakeSnapshotMessage <: Message {
    private let stateData: String
    
    public init(stateData: String) {
        this.stateData = stateData
    }
    
    public func getStateData(): String { stateData }
    public func messageType(): String { "TakeSnapshotMessage" }

    public func toString(): String {
        "TakeSnapshotMessage"
    }
}

@Deprecated["请改用 EventSourcedActor + EventStore（append / replay / deleteTo，见 docs/zh/api-reference.md 第 14 节）；2.0 删除", since: "1.2.0"]
public class RecoveryCompleteMessage <: Message {
    public init() {}

    public func messageType(): String { "RecoveryCompleteMessage" }

    public func toString(): String {
        "RecoveryCompleteMessage"
    }
}
