package cactor_persistence

import std.collection.*
import std.time.*
import std.sync.*
import std.fs.*

// 快照接口
public interface Snapshot {
    func getActorId(): String
    func getTimestamp(): Int64
    func getSequenceNumber(): Int64
    func getStateData(): String
    func toString(): String
}

// 基础快照实现
public class BasicSnapshot <: Snapshot {
    private let actorId: String
    private let timestamp: Int64
    private let sequenceNumber: Int64
    private let stateData: String
    
    public init(actorId: String, sequenceNumber: Int64, stateData: String) {
        this(actorId, sequenceNumber, stateData, DateTime.now().toUnixTimeStamp().toMilliseconds())
    }

    /** 从存储恢复时使用：保留原始时间戳 */
    public init(actorId: String, sequenceNumber: Int64, stateData: String, timestamp: Int64) {
        this.actorId = actorId
        this.timestamp = timestamp
        this.sequenceNumber = sequenceNumber
        this.stateData = stateData
    }
    
    public func getActorId(): String { actorId }
    public func getTimestamp(): Int64 { timestamp }
    public func getSequenceNumber(): Int64 { sequenceNumber }
    public func getStateData(): String { stateData }
    
    public func toString(): String {
        "Snapshot(${actorId}, seq=${sequenceNumber}, ts=${timestamp})"
    }
}

// 快照存储接口
public interface SnapshotStore {
    func saveSnapshot(snapshot: Snapshot): Unit
    func getLatestSnapshot(actorId: String): Option<Snapshot>
    func getSnapshot(actorId: String, sequenceNumber: Int64): Option<Snapshot>
    func deleteSnapshots(actorId: String): Unit
    func close(): Unit
}

// 内存快照存储
public class MemorySnapshotStore <: SnapshotStore {
    private let snapshots: HashMap<String, ArrayList<Snapshot>>
    private let mutex: Mutex
    
    public init() {
        this.snapshots = HashMap<String, ArrayList<Snapshot>>()
        this.mutex = Mutex()
    }
    
    public func saveSnapshot(snapshot: Snapshot): Unit {
        mutex.lock()
        try {
            let actorId = snapshot.getActorId()
            match (snapshots.get(actorId)) {
                case Some(snapshotList) => 
                    snapshotList.add(snapshot)
                    // 保持最新的5个快照
                    if (snapshotList.size > 5) {
                        // 只保留最新的 5 个
                        let newList = ArrayList<Snapshot>()
                        for (i in (snapshotList.size - 5)..snapshotList.size) {
                            newList.add(snapshotList[i])
                        }
                        snapshots.add(actorId, newList)
                    }
                case None =>
                    let newList = ArrayList<Snapshot>()
                    newList.add(snapshot)
                    snapshots.add(actorId, newList)
            }
        } finally {
            mutex.unlock()
        }
    }
    
    public func getLatestSnapshot(actorId: String): Option<Snapshot> {
        mutex.lock()
        try {
            match (snapshots.get(actorId)) {
                case Some(snapshotList) => 
                    if (snapshotList.size > 0) {
                        Some(snapshotList[snapshotList.size - 1])
                    } else {
                        None<Snapshot>
                    }
                case None => 
                    None<Snapshot>
            }
        } finally {
            mutex.unlock()
        }
    }
    
    public func getSnapshot(actorId: String, sequenceNumber: Int64): Option<Snapshot> {
        mutex.lock()
        try {
            match (snapshots.get(actorId)) {
                case Some(snapshotList) => 
                    for (snapshot in snapshotList) {
                        if (snapshot.getSequenceNumber() == sequenceNumber) {
                            return Some(snapshot)
                        }
                    }
                    None<Snapshot>
                case None => 
                    None<Snapshot>
            }
        } finally {
            mutex.unlock()
        }
    }
    
    public func deleteSnapshots(actorId: String): Unit {
        mutex.lock()
        try {
            snapshots.remove(actorId)
        } finally {
            mutex.unlock()
        }
    }
    
    public func close(): Unit {
        // 内存存储无需关闭操作
    }
}

/**
 * 文件快照存储
 *
 * - 每个 Actor 一个快照文件：<basePath>/<hex(actorId)>.snapshots，保留最新 keep 个
 * - 写入采用“写临时文件 + rename 覆盖”，任何时刻文件要么是旧版本要么是新版本
 */
public class FileSnapshotStore <: SnapshotStore {
    private let basePath: String
    private let mutex: Mutex
    private let keep: Int64

    public init(basePath: String) {
        this(basePath, 5)
    }

    public init(basePath: String, keep: Int64) {
        this.basePath = basePath
        this.mutex = Mutex()
        this.keep = if (keep < 1) { 1 } else { keep }
        ensureDirectory(basePath)
    }

    private func snapshotPath(actorId: String): String {
        "${basePath}/${safeFileName(actorId)}.snapshots"
    }

    public func saveSnapshot(snapshot: Snapshot): Unit {
        synchronized(mutex) {
            let all = ArrayList<Snapshot>(readAll(snapshot.getActorId()))
            all.add(snapshot)
            let start = if (all.size > keep) { all.size - keep } else { 0 }
            let out = ArrayList<UInt8>()
            for (i in start..all.size) {
                let sn = all[i]
                out.add(all: RecordWriter()
                    .putString(sn.getActorId())
                    .putString(sn.getStateData())
                    .putInt64(sn.getSequenceNumber())
                    .putInt64(sn.getTimestamp())
                    .toRecord())
            }
            let path = snapshotPath(snapshot.getActorId())
            let tmp = path + ".tmp"
            File.writeTo(tmp, out.toArray())
            rename(tmp, to: path, overwrite: true)
        }
    }

    public func getLatestSnapshot(actorId: String): Option<Snapshot> {
        let all = synchronized(mutex) { readAll(actorId) }
        if (all.size == 0) { None<Snapshot> } else { Some(all[all.size - 1]) }
    }

    public func getSnapshot(actorId: String, sequenceNumber: Int64): Option<Snapshot> {
        let all = synchronized(mutex) { readAll(actorId) }
        for (sn in all) {
            if (sn.getSequenceNumber() == sequenceNumber) {
                return Some(sn)
            }
        }
        None<Snapshot>
    }

    public func deleteSnapshots(actorId: String): Unit {
        synchronized(mutex) {
            let path = snapshotPath(actorId)
            if (exists(path)) {
                remove(path)
            }
        }
    }

    public func close(): Unit {}

    private func readAll(actorId: String): Array<Snapshot> {
        let path = snapshotPath(actorId)
        if (!exists(path)) {
            return Array<Snapshot>()
        }
        let (records, _) = splitRecords(File.readFrom(path))
        let result = ArrayList<Snapshot>()
        for (payload in records) {
            let r = RecordReader(payload)
            let owner = r.getString()
            let state = r.getString()
            let seq = r.getInt64()
            let ts = r.getInt64()
            result.add(BasicSnapshot(owner, seq, state, ts))
        }
        result.toArray()
    }
}

// 快照存储工厂
public class SnapshotStoreFactory {
    public static func createMemoryStore(): SnapshotStore {
        MemorySnapshotStore()
    }
    
    public static func createFileStore(basePath: String): SnapshotStore {
        FileSnapshotStore(basePath)
    }
}
