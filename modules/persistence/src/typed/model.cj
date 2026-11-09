/*
 * CActor Persistence.Typed - 标识、事件适配器、状态编解码、保留策略与信号（对标 Akka Typed persistence）
 */
package cactor_persistence.typed

import cactor_core.typed.*

// ======================= 持久化标识 =======================

/**
 * 持久化标识：日志、快照、持久状态都按它存取，必须全局唯一且稳定（同一实体重启后相同）。
 * 常用 PersistenceId.of("ShoppingCart", cartId) 得到 "ShoppingCart|<cartId>"（与 Akka 相同的分隔符）
 */
public struct PersistenceId <: ToString {
    public let id: String

    public init(id: String) {
        if (id.isEmpty()) {
            throw IllegalArgumentException("持久化标识不能为空")
        }
        this.id = id
    }

    /** 实体类型提示 + 实体标识；类型提示里不能含分隔符 "|"，否则不同类型的标识可能相撞 */
    public static func of(entityTypeHint: String, entityId: String): PersistenceId {
        if (entityTypeHint.contains("|")) {
            throw IllegalArgumentException("实体类型提示不能包含分隔符 |：${entityTypeHint}")
        }
        PersistenceId("${entityTypeHint}|${entityId}")
    }

    /** 已经全局唯一的标识（不加类型前缀） */
    public static func ofUniqueId(id: String): PersistenceId {
        PersistenceId(id)
    }

    public func toString(): String {
        id
    }
}

// ======================= 事件适配器 =======================

/**
 * 事件与日志记录之间的转换（对标 Akka EventAdapter + 序列化）
 *
 * 日志里每条记录存 manifest（事件类型名）与 data（字符串）。schema 演进时：
 * - 新增事件类型：写新的 manifest；
 * - 修改事件结构：读取时按 manifest / 数据里的版本号兼容旧格式；
 * - 废弃事件：fromJournal 返回空数组（重放时跳过）；拆分事件：返回多个事件（对标 Akka EventSeq）。
 * toJournal / fromJournal 在持久化 IO 线程或 Actor 线程中调用，必须是无副作用的纯函数。
 */
public interface EventAdapter<E> {
    /** 事件在日志中的类型名 */
    func manifest(event: E): String

    /** 写入日志的数据 */
    func toJournal(event: E): String

    /** 从日志读回 0..n 个事件；数据无法解析时抛异常（恢复失败，Actor 停止，不会带着错误状态继续运行） */
    func fromJournal(manifest: String, data: String): Array<E>

    /** 事件的标签（ReadJournal.currentEventsByTag 按它查询），默认没有 */
    func tags(event: E): Array<String> {
        Array<String>()
    }
}

/** 用函数构造的事件适配器 */
public class FunctionEventAdapter<E> <: EventAdapter<E> {
    private let manifestOf: (E) -> String
    private let encode: (E) -> String
    private let decode: (String, String) -> E
    private let tagsOf: (E) -> Array<String>

    public init(manifest!: (E) -> String, toJournal!: (E) -> String, fromJournal!: (String, String) -> E,
        tags!: (E) -> Array<String> = { _ => Array<String>() }) {
        this.manifestOf = manifest
        this.encode = toJournal
        this.decode = fromJournal
        this.tagsOf = tags
    }

    public func manifest(event: E): String {
        manifestOf(event)
    }

    public func toJournal(event: E): String {
        encode(event)
    }

    public func fromJournal(manifest: String, data: String): Array<E> {
        [decode(manifest, data)]
    }

    public func tags(event: E): Array<String> {
        tagsOf(event)
    }
}

// ======================= 状态编解码 =======================

/** 状态与字符串之间的转换：快照（EventSourcedBehavior）与持久状态（DurableStateBehavior）使用 */
public interface StateCodec<S> {
    func encode(state: S): String
    func decode(data: String): S
}

/** 用函数构造的状态编解码器 */
public class FunctionStateCodec<S> <: StateCodec<S> {
    private let enc: (S) -> String
    private let dec: (String) -> S

    public init(encode!: (S) -> String, decode!: (String) -> S) {
        this.enc = encode
        this.dec = decode
    }

    public func encode(state: S): String {
        enc(state)
    }

    public func decode(data: String): S {
        dec(data)
    }
}

// ======================= 保留策略 =======================

/**
 * 快照与日志的保留策略（对标 Akka RetentionCriteria）
 *
 * snapshotEvery(100, 2)：序号每跨过 100 的倍数保存一次快照，只保留最近 2 份；
 * withDeleteEventsOnSnapshot()：保存快照后同时删除最早保留的快照之前的事件（序号 ≤ 当前序号 − 2 × 100），
 * 恢复从快照开始，日志不再无限增长。
 */
public struct RetentionCriteria {
    public let snapshotEveryNEvents: Int64
    public let keepNSnapshots: Int64
    public let deleteEventsOnSnapshot: Bool

    init(snapshotEveryNEvents: Int64, keepNSnapshots: Int64, deleteEventsOnSnapshot: Bool) {
        this.snapshotEveryNEvents = snapshotEveryNEvents
        this.keepNSnapshots = keepNSnapshots
        this.deleteEventsOnSnapshot = deleteEventsOnSnapshot
    }

    /** 不自动保存快照（默认） */
    public static func disabled(): RetentionCriteria {
        RetentionCriteria(0, 0, false)
    }

    public static func snapshotEvery(numberOfEvents: Int64, keepNSnapshots: Int64): RetentionCriteria {
        if (numberOfEvents <= 0 || keepNSnapshots <= 0) {
            throw IllegalArgumentException("snapshotEvery 的事件数与保留份数必须为正：${numberOfEvents}、${keepNSnapshots}")
        }
        RetentionCriteria(numberOfEvents, keepNSnapshots, false)
    }

    public func withDeleteEventsOnSnapshot(): RetentionCriteria {
        RetentionCriteria(snapshotEveryNEvents, keepNSnapshots, true)
    }

    public func isEnabled(): Bool {
        snapshotEveryNEvents > 0
    }

    /** 序号从 fromExclusive 推进到 toInclusive 时是否跨过了快照点 */
    func crossesSnapshotPoint(fromExclusive: Int64, toInclusive: Int64): Bool {
        isEnabled() && toInclusive / snapshotEveryNEvents > fromExclusive / snapshotEveryNEvents
    }

    /** 在 lastSequenceNr 保存快照后，可以删除 ≤ 返回值的快照（与事件），0 表示不删 */
    func deleteUpperSequenceNr(lastSequenceNr: Int64): Int64 {
        let upper = lastSequenceNr - keepNSnapshots * snapshotEveryNEvents
        if (upper > 0) { upper } else { 0 }
    }
}

// ======================= 信号 =======================

/** 恢复完成（快照与事件都已应用，暂存的命令随后处理） */
public class RecoveryCompleted <: TypedSignal {
    /** 恢复后的最高序号（DurableStateBehavior 中是修订号） */
    public let highestSequenceNr: Int64

    public init(highestSequenceNr: Int64) {
        this.highestSequenceNr = highestSequenceNr
    }

    public func signalName(): String {
        "RecoveryCompleted"
    }
}

/** 恢复失败：信号处理完后 Actor 以该异常失败（默认停止） */
public class RecoveryFailed <: TypedSignal {
    public let cause: Exception

    public init(cause: Exception) {
        this.cause = cause
    }

    public func signalName(): String {
        "RecoveryFailed"
    }
}

/** 快照已保存 */
public class SnapshotCompleted <: TypedSignal {
    public let sequenceNr: Int64

    public init(sequenceNr: Int64) {
        this.sequenceNr = sequenceNr
    }

    public func signalName(): String {
        "SnapshotCompleted"
    }
}

/** 快照保存失败（不影响 Actor 运行：恢复时多重放一些事件而已） */
public class SnapshotFailed <: TypedSignal {
    public let sequenceNr: Int64
    public let cause: Exception

    public init(sequenceNr: Int64, cause: Exception) {
        this.sequenceNr = sequenceNr
        this.cause = cause
    }

    public func signalName(): String {
        "SnapshotFailed"
    }
}

/** 保留策略删除了旧快照（序号 ≤ toSequenceNr） */
public class DeleteSnapshotsCompleted <: TypedSignal {
    public let toSequenceNr: Int64

    public init(toSequenceNr: Int64) {
        this.toSequenceNr = toSequenceNr
    }

    public func signalName(): String {
        "DeleteSnapshotsCompleted"
    }
}

/** 保留策略删除了旧事件（序号 ≤ toSequenceNr，最高序号保留） */
public class DeleteEventsCompleted <: TypedSignal {
    public let toSequenceNr: Int64

    public init(toSequenceNr: Int64) {
        this.toSequenceNr = toSequenceNr
    }

    public func signalName(): String {
        "DeleteEventsCompleted"
    }
}

/** 删除旧快照或旧事件失败（不影响 Actor 运行） */
public class DeleteFailed <: TypedSignal {
    public let toSequenceNr: Int64
    public let cause: Exception

    public init(toSequenceNr: Int64, cause: Exception) {
        this.toSequenceNr = toSequenceNr
        this.cause = cause
    }

    public func signalName(): String {
        "DeleteFailed"
    }
}

/** 暂存缓冲区满时的处理方式 */
public enum StashOverflowStrategy {
    | DropCommand    // 丢弃新命令并记日志（Akka 默认）
    | FailActor      // Actor 以 StashOverflowException 失败
}
