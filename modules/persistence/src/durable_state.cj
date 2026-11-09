/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Persistence - 持久化状态（对标 Akka Persistence 的 Durable State）
 *
 * 与事件溯源不同，只保存最新状态：适合状态小、不需要历史的实体（配置、会话、Agent 的长期记忆摘要）。
 * - 乐观并发：每次写入携带期望的新修订号（当前修订号 + 1），不相符时抛 DurableStateConflictException，
 *   因此同一实体被两个写入方（例如分片移交时短暂并存的新旧实例）同时修改时，后写的一方会失败而不是静默覆盖。
 * - FileDurableStateStore：每个实体一个文件，写临时文件后 rename 覆盖，任何时刻文件要么是旧版本要么是新版本。
 * - DurableStateActor<S>：启动和重启时读取最新状态；persistState 写入成功后才更新内存状态。
 */
package cactor_persistence

import std.fs.*
import std.sync.*
import std.collection.*
import cactor_core.actor.{Actor}
import cactor_core.message.{Message, MessageResult}
import cactor_core.context.{ActorContext}

/** 修订号不符（并发写入）时抛出 */
public class DurableStateConflictException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** 一个实体的最新状态：修订号从 1 开始，每次写入加一 */
public class DurableStateRecord {
    public let persistenceId: String
    public let revision: Int64
    public let state: String

    public init(persistenceId: String, revision: Int64, state: String) {
        this.persistenceId = persistenceId
        this.revision = revision
        this.state = state
    }
}

public interface DurableStateStore {
    /** 当前状态；从未写过或已删除时为 None（已删除的修订号见 revisionOf） */
    func get(persistenceId: String): Option<DurableStateRecord>

    /** 写入修订号为 revision 的状态；当前修订号（revisionOf）必须是 revision - 1 */
    func upsert(persistenceId: String, revision: Int64, state: String): Unit

    /**
     * 删除；当前修订号必须是 revision。删除本身也是一次写入：留下修订号为 revision + 1 的墓碑，
     * 之后的写入从 revision + 2 继续（对标 Akka DurableStateStore 删除后修订号不回退，P14）
     */
    func delete(persistenceId: String, revision: Int64): Unit

    /** 未被删除的标识 */
    func persistenceIds(): Array<String>

    /**
     * 当前修订号：有状态时是状态的修订号，已删除时是墓碑的修订号，从未写过为 0。
     * 写入方应据此决定下一次 upsert 的修订号——用 get 判断会在删除后把修订号算回 0（被拒绝）。
     * 默认实现基于 get（不认识墓碑的自定义存储）
     */
    func revisionOf(persistenceId: String): Int64 {
        match (get(persistenceId)) {
            case Some(r) => r.revision
            case None => 0
        }
    }
}

func checkRevision(persistenceId: String, current: Int64, expectedCurrent: Int64): Unit {
    if (current != expectedCurrent) {
        throw DurableStateConflictException(
            "${persistenceId} 的当前修订号是 ${current}，写入方期望 ${expectedCurrent}（存在并发写入）")
    }
}

public class MemoryDurableStateStore <: DurableStateStore {
    private let lock = Mutex()
    private let records = HashMap<String, DurableStateRecord>()
    // 已删除标识的墓碑修订号
    private let tombstones = HashMap<String, Int64>()

    public init() {}

    public func get(persistenceId: String): Option<DurableStateRecord> {
        synchronized(lock) {
            records.get(persistenceId)
        }
    }

    public func revisionOf(persistenceId: String): Int64 {
        synchronized(lock) {
            currentRevision(persistenceId)
        }
    }

    private func currentRevision(persistenceId: String): Int64 {
        match (records.get(persistenceId)) {
            case Some(r) => r.revision
            case None => tombstones.get(persistenceId) ?? 0
        }
    }

    public func upsert(persistenceId: String, revision: Int64, state: String): Unit {
        synchronized(lock) {
            checkRevision(persistenceId, currentRevision(persistenceId), revision - 1)
            records[persistenceId] = DurableStateRecord(persistenceId, revision, state)
            tombstones.remove(persistenceId)
        }
    }

    public func delete(persistenceId: String, revision: Int64): Unit {
        synchronized(lock) {
            checkRevision(persistenceId, currentRevision(persistenceId), revision)
            records.remove(persistenceId)
            tombstones[persistenceId] = revision + 1
        }
    }

    public func persistenceIds(): Array<String> {
        synchronized(lock) {
            let ids = ArrayList<String>()
            for ((id, _) in records) {
                ids.add(id)
            }
            ids.toArray()
        }
    }
}

/**
 * 文件持久化状态：<basePath>/<hex(标识)>.state，内容为一条带长度前缀的记录（标识、修订号、状态）。
 * 同一进程内的写入用互斥锁串行化；修订号检查与写入在同一把锁内完成。
 */
public class FileDurableStateStore <: DurableStateStore {
    private let basePath: String
    private let lock = Mutex()

    public init(basePath: String) {
        this.basePath = basePath
        ensureDirectory(basePath)
    }

    private func pathOf(persistenceId: String): String {
        "${basePath}/${safeFileName(persistenceId)}.state"
    }

    /** (记录, 是否墓碑)；文件格式：标识、修订号、状态[、墓碑标记 1]——旧文件没有第四个字段 */
    private func readFile(path: String): Option<(DurableStateRecord, Bool)> {
        if (!exists(path)) {
            return None
        }
        let (records, _) = splitRecords(File.readFrom(path))
        if (records.isEmpty()) {
            // 不可能出现（rename 保证原子性），按不存在处理
            return None
        }
        let r = RecordReader(records[0])
        let id = r.getString()
        let revision = r.getInt64()
        let state = r.getString()
        let deleted = r.hasMore() && r.getInt64() == 1
        Some((DurableStateRecord(id, revision, state), deleted))
    }

    private func currentRevision(persistenceId: String): Int64 {
        match (readFile(pathOf(persistenceId))) {
            case Some((r, _)) => r.revision
            case None => 0
        }
    }

    public func get(persistenceId: String): Option<DurableStateRecord> {
        synchronized(lock) {
            match (readFile(pathOf(persistenceId))) {
                case Some((r, false)) => Some(r)
                case _ => None<DurableStateRecord>
            }
        }
    }

    public func revisionOf(persistenceId: String): Int64 {
        synchronized(lock) {
            currentRevision(persistenceId)
        }
    }

    public func upsert(persistenceId: String, revision: Int64, state: String): Unit {
        synchronized(lock) {
            checkRevision(persistenceId, currentRevision(persistenceId), revision - 1)
            // 原子替换且返回时已落盘（B7）
            writeFileAtomically(pathOf(persistenceId),
                RecordWriter().putString(persistenceId).putInt64(revision).putString(state).toRecord(), true)
        }
    }

    public func delete(persistenceId: String, revision: Int64): Unit {
        synchronized(lock) {
            checkRevision(persistenceId, currentRevision(persistenceId), revision)
            // 墓碑而不是删文件：修订号不回退，过期的写者（还拿着删除前的修订号）会被拒绝（P14）
            writeFileAtomically(pathOf(persistenceId),
                RecordWriter().putString(persistenceId).putInt64(revision + 1).putString("").putInt64(1).toRecord(), true)
        }
    }

    public func persistenceIds(): Array<String> {
        synchronized(lock) {
            let ids = ArrayList<String>()
            for (info in Directory.readFrom(basePath)) {
                let name = info.name
                if (name.endsWith(".state")) {
                    // 原标识是记录的第一个字段（哈希文件名时只能从记录里取）；墓碑不算
                    if (let Some((r, false)) <- readFile("${basePath}/${name}")) {
                        ids.add(r.persistenceId)
                    }
                }
            }
            ids.toArray()
        }
    }
}

/**
 * 持久化状态 Actor：子类提供标识、空状态、状态与字符串的转换和命令处理；
 * 在 handleCommand 中调用 persistState(新状态) 保存，成功后 state 才更新。
 *
 * ```
 * class Counter <: DurableStateActor<Int64> {
 *     public func persistenceId(): String { "counter-1" }
 *     public func emptyState(): Int64 { 0 }
 *     public func encodeState(s: Int64): String { "${s}" }
 *     public func decodeState(s: String): Int64 { Int64.parse(s) }
 *     public func handleCommand(m: Message, ctx: ActorContext): MessageResult {
 *         persistState(state + 1); MessageResult.Handled
 *     }
 * }
 * ```
 */
public abstract class DurableStateActor<S> <: Actor {
    private let store: DurableStateStore
    private var currentState: Option<S> = None
    private var currentRevision: Int64 = 0
    private var loaded = false

    public init(store: DurableStateStore) {
        this.store = store
    }

    public func persistenceId(): String

    public func emptyState(): S

    public func encodeState(state: S): String

    public func decodeState(data: String): S

    public func handleCommand(message: Message, context: ActorContext): MessageResult

    /** 当前状态（已持久化的最新值） */
    public prop state: S {
        get() {
            ensureLoaded()
            currentState.getOrThrow()
        }
    }

    /** 当前修订号（0 表示还没有保存过） */
    public func revision(): Int64 {
        ensureLoaded()
        currentRevision
    }

    /** 保存新状态：写入成功后才更新内存中的状态；并发冲突时抛 DurableStateConflictException（交给监督，重启后重新读取） */
    protected func persistState(newState: S): Unit {
        ensureLoaded()
        store.upsert(persistenceId(), currentRevision + 1, encodeState(newState))
        currentRevision++
        currentState = Some(newState)
    }

    /** 删除已保存的状态，内存状态回到 emptyState()；修订号不回退（删除留下 revision + 1 的墓碑） */
    protected func deleteState(): Unit {
        ensureLoaded()
        if (currentRevision > 0) {
            store.delete(persistenceId(), currentRevision)
            currentRevision++
        }
        currentState = Some(emptyState())
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        ensureLoaded()
        handleCommand(message, context)
    }

    public open func preStart(): Unit {
        reload()
    }

    public open func postRestart(reason: Exception): Unit {
        reload()
    }

    private func ensureLoaded(): Unit {
        if (!loaded) {
            reload()
        }
    }

    private func reload(): Unit {
        match (store.get(persistenceId())) {
            case Some(r) =>
                currentRevision = r.revision
                currentState = Some(decodeState(r.state))
            case None =>
                // 已删除时修订号取墓碑的（不能回到 0，否则下一次写入被拒绝）
                currentRevision = store.revisionOf(persistenceId())
                currentState = Some(emptyState())
        }
        loaded = true
    }
}
