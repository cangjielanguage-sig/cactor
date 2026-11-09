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

    /**
     * 删除序号 ≤ maxSequenceNr 的快照（类型化持久化的 RetentionCriteria 用它只保留最近 N 份）。
     * 只实现了上面五个方法的自定义存储不受影响：调用时抛 UnsupportedException
     */
    func deleteSnapshotsTo(actorId: String, maxSequenceNr: Int64): Unit {
        throw UnsupportedException("该快照存储不支持 deleteSnapshotsTo（${actorId} ≤ ${maxSequenceNr}）")
    }
}

/**
 * 按序号升序插入；同一序号的快照被替换（重复保存是幂等的）。
 * 为什么不直接追加：快照是异步保存的，Actor 重启前提交的旧快照可能比新实例的快照晚落盘；
 * 按位置取"最新"会把它当成最新，而它之后的事件可能已被保留策略删除，恢复出错误状态
 */
func insertSnapshotBySeq(list: ArrayList<Snapshot>, snapshot: Snapshot): Unit {
    let seq = snapshot.getSequenceNumber()
    var i = list.size
    while (i > 0 && list[i - 1].getSequenceNumber() > seq) {
        i--
    }
    if (i > 0 && list[i - 1].getSequenceNumber() == seq) {
        list[i - 1] = snapshot
    } else {
        list.add(snapshot, at: i)
    }
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
        synchronized(mutex) {
            let actorId = snapshot.getActorId()
            let list = match (snapshots.get(actorId)) {
                case Some(l) => l
                case None =>
                    let l = ArrayList<Snapshot>()
                    snapshots.add(actorId, l)
                    l
            }
            insertSnapshotBySeq(list, snapshot)
            // 只保留序号最大的 5 个
            if (list.size > 5) {
                let threshold = list[list.size - 5].getSequenceNumber()
                list.removeIf({ sn => sn.getSequenceNumber() < threshold })
            }
        }
    }

    public func getLatestSnapshot(actorId: String): Option<Snapshot> {
        synchronized(mutex) {
            match (snapshots.get(actorId)) {
                case Some(l) where l.size > 0 => Some(l[l.size - 1])
                case _ => None<Snapshot>
            }
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

    public func deleteSnapshotsTo(actorId: String, maxSequenceNr: Int64): Unit {
        synchronized(mutex) {
            if (let Some(l) <- snapshots.get(actorId)) {
                l.removeIf({ sn => sn.getSequenceNumber() <= maxSequenceNr })
            }
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
 * - 写入采用“写临时文件 + fsync + rename 覆盖 + 目录 fsync”，任何时刻文件要么是旧版本要么是新版本，返回时已落盘
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
            insertSnapshotBySeq(all, snapshot)
            let start = if (all.size > keep) { all.size - keep } else { 0 }
            writeAll(snapshot.getActorId(), all.toArray()[start..])
        }
    }

    public func deleteSnapshotsTo(actorId: String, maxSequenceNr: Int64): Unit {
        synchronized(mutex) {
            let all = readAll(actorId)
            let rest = ArrayList<Snapshot>()
            for (sn in all) {
                if (sn.getSequenceNumber() > maxSequenceNr) {
                    rest.add(sn)
                }
            }
            if (rest.size == all.size) {
                return
            }
            if (rest.isEmpty()) {
                let path = snapshotPath(actorId)
                if (exists(path)) {
                    remove(path)
                }
            } else {
                writeAll(actorId, rest.toArray())
            }
        }
    }

    private func writeAll(actorId: String, snapshots: Array<Snapshot>): Unit {
        let out = ArrayList<UInt8>()
        for (sn in snapshots) {
            out.add(all: RecordWriter()
                .putString(sn.getActorId())
                .putString(sn.getStateData())
                .putInt64(sn.getSequenceNumber())
                .putInt64(sn.getTimestamp())
                .toRecord())
        }
        // 临时文件 fsync → rename → 目录 fsync（B7：此前不 fsync，断电后可能丢掉新快照甚至留下空文件）
        writeFileAtomically(snapshotPath(actorId), out.toArray(), true)
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
            insertSnapshotBySeq(result, BasicSnapshot(owner, seq, state, ts))
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
