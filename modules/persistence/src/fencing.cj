/*
 * CActor Persistence - 存储侧 fencing（DA4，配合 cactor_foundation.coordination.Lease）
 *
 * 为什么租约之外还要 fencing：租约只保证"持有者自认为持有"的时段互不重叠。旧持有者在检查租约之后、
 * 写入到达存储之前停顿（GC、调度、网络重传），恢复时新持有者可能已经写过——单靠租约无法拦住这条迟到的写入。
 * 做法（Kleppmann 的 fencing token）：每次成功获取租约得到严格递增的 token，写入携带 token；
 * 存储为每个受保护资源（= 租约名）记住见过的最大 token，小于它的写入一律拒绝，并原子地把检查与写入绑在一起。
 *
 * 保证范围（必须读）：
 *   - MemoryFencingTokenStore / FileFencingTokenStore 的"检查 token + 执行写入"在同一把进程内锁里完成：
 *     经由同一个 FencingTokenStore 实例的写者（同一进程内的新旧实例、或部署成单独存储服务时的所有客户端）严格互斥。
 *   - FileFencingTokenStore 把最大 token 持久化（原子替换 + fsync），进程重启后不回退；但多个进程直接共享同一目录时，
 *     文件系统不提供"比较 token 后写数据"的跨进程原子操作，检查与写入之间仍有窗口。此时仍能拒绝"新持有者写过之后
 *     才开始的旧写入"，但不能拒绝与新持有者写入交错的那一次；严格的跨宿主保证需要存储本身支持条件写
 *     （数据库事务、对象存储的条件 PUT 等），实现 FencingTokenStore 接到那里即可。
 *   - 读操作不受 fencing 约束。
 */
package cactor_persistence

import std.fs.*
import std.sync.*
import std.collection.*
import cactor_foundation.coordination.{Lease}

/** 写者的 fencing token 小于存储已见过的最大值：它的租约已被更新的持有者取代 */
public class StaleFencingTokenException <: PersistenceException {
    public let resource: String
    public let token: Int64
    public let highestSeen: Int64

    public init(resource: String, token: Int64, highestSeen: Int64) {
        super("资源 ${resource} 的写入被拒绝：fencing token ${token} 已过期（存储已见过 ${highestSeen}），写者已不是所有者")
        this.resource = resource
        this.token = token
        this.highestSeen = highestSeen
    }
}

/** 写入时本地已不持有租约（租约丢失或从未获取） */
public class LeaseNotHeldException <: PersistenceException {
    public init(resource: String) {
        super("资源 ${resource} 的写入被拒绝：本地未持有租约")
    }
}

/** 每个资源见过的最大 fencing token；fenced 在检查通过后、同一临界区内执行写入 */
public interface FencingTokenStore {
    /**
     * token 小于已见最大值时抛 StaleFencingTokenException 且不执行 write；
     * 否则记录 token（取较大者）并执行 write。write 抛出的异常原样传出（token 仍已记录：新持有者已开始写）。
     */
    func fenced(resource: String, token: Int64, write: () -> Unit): Unit

    /** 已见最大 token（从未写过为 0） */
    func highestToken(resource: String): Int64
}

/** 进程内 fencing 存储（单进程多实例、测试） */
public class MemoryFencingTokenStore <: FencingTokenStore {
    private let lock = Mutex()
    private let highest = HashMap<String, Int64>()

    public init() {}

    public func fenced(resource: String, token: Int64, write: () -> Unit): Unit {
        synchronized(lock) {
            let seen = highest.get(resource) ?? 0
            if (token < seen) {
                throw StaleFencingTokenException(resource, token, seen)
            }
            highest[resource] = token
            write()
        }
    }

    public func highestToken(resource: String): Int64 {
        synchronized(lock) {
            highest.get(resource) ?? 0
        }
    }
}

/**
 * 文件 fencing 存储：<dir>/<hex(资源)>.fence 记录最大 token（原子替换 + fsync），重启后不回退。
 * 跨进程直接共享目录时的保证范围见文件头。
 */
public class FileFencingTokenStore <: FencingTokenStore {
    private let dir: String
    private let lock = Mutex()
    private let sync: Bool

    public init(directory: String) {
        this(directory, sync: true)
    }

    public init(directory: String, sync!: Bool) {
        this.dir = directory
        this.sync = sync
        ensureDirectory(directory)
    }

    private func pathOf(resource: String): String {
        "${dir}/${safeFileName(resource)}.fence"
    }

    private func readHighest(resource: String): Int64 {
        let path = pathOf(resource)
        if (!exists(path)) {
            return 0
        }
        let (records, _) = splitRecords(File.readFrom(path))
        if (records.isEmpty()) {
            // 原子替换下不会出现；无法确认时拒绝一切写入比静默放行安全
            throw PersistenceCorruptionException("fencing 记录损坏：${path}")
        }
        let r = RecordReader(records[0])
        r.getString()
        r.getInt64()
    }

    public func fenced(resource: String, token: Int64, write: () -> Unit): Unit {
        synchronized(lock) {
            let seen = readHighest(resource)
            if (token < seen) {
                throw StaleFencingTokenException(resource, token, seen)
            }
            if (token > seen) {
                // 先落盘新的最大值再写数据：崩溃在两者之间只会多拒绝旧写者，不会放行
                writeFileAtomically(pathOf(resource), RecordWriter().putString(resource).putInt64(token).toRecord(), sync)
            }
            write()
        }
    }

    public func highestToken(resource: String): Int64 {
        synchronized(lock) {
            readHighest(resource)
        }
    }
}

/** 写者当前的 token 来源：默认取租约的 fencingToken（未持有即拒绝写入） */
func leaseTokenSource(lease: Lease): () -> Option<Int64> {
    { => lease.fencingToken() }
}

func currentToken(resource: String, source: () -> Option<Int64>): Int64 {
    match (source()) {
        case Some(t) => t
        case None => throw LeaseNotHeldException(resource)
    }
}

/**
 * 带 fencing 的事件存储：所有写操作（append / saveEvent / deleteTo / deleteEvents）先取当前 token，
 * 经 FencingTokenStore 检查后再委托给内层存储；读操作直接委托。resource 通常就是租约名，
 * 同一租约保护的所有持久化标识（例如一个分片里的实体）共享一个 token 序列。
 */
public class FencedEventStore <: EventStore {
    private let inner: EventStore
    private let fence: FencingTokenStore
    private let resource: String
    private let tokenSource: () -> Option<Int64>

    /** token 取自租约：租约丢失后的写入抛 LeaseNotHeldException，过期 token 的写入抛 StaleFencingTokenException */
    public init(inner: EventStore, fence: FencingTokenStore, lease: Lease) {
        this(inner, fence, lease.settings.leaseName, leaseTokenSource(lease))
    }

    /** 自定义 token 来源（例如 token 随命令一起传递的场景） */
    public init(inner: EventStore, fence: FencingTokenStore, resource: String, tokenSource: () -> Option<Int64>) {
        this.inner = inner
        this.fence = fence
        this.resource = resource
        this.tokenSource = tokenSource
    }

    private func guarded(write: () -> Unit): Unit {
        fence.fenced(resource, currentToken(resource, tokenSource), write)
    }

    public func saveEvent(event: Event): Unit {
        guarded({ => inner.saveEvent(event) })
    }

    public func append(persistenceId: String, events: Array<Event>, expectedSeq: Int64): Int64 {
        // 闭包不能捕获可变变量逃逸，结果放进容器带出
        let result = ArrayList<Int64>()
        guarded({ => result.add(inner.append(persistenceId, events, expectedSeq)) })
        result[0]
    }

    public func deleteTo(persistenceId: String, toSeq: Int64): Unit {
        guarded({ => inner.deleteTo(persistenceId, toSeq) })
    }

    public func deleteEvents(actorId: String): Unit {
        guarded({ => inner.deleteEvents(actorId) })
    }

    public func getEvents(actorId: String): Array<Event> {
        inner.getEvents(actorId)
    }

    public func getEventsAfter(actorId: String, timestamp: Int64): Array<Event> {
        inner.getEventsAfter(actorId, timestamp)
    }

    public func highestSequenceNr(persistenceId: String): Int64 {
        inner.highestSequenceNr(persistenceId)
    }

    public func replay(persistenceId: String, fromSeq: Int64, toSeq: Int64): Array<EventEnvelope> {
        inner.replay(persistenceId, fromSeq, toSeq)
    }

    public func persistenceIds(): Array<String> {
        inner.persistenceIds()
    }

    public func close(): Unit {
        inner.close()
    }
}

/** 带 fencing 的持久状态存储（提醒 reminders 也经由 DurableStateStore 写入，包一层即受保护） */
public class FencedDurableStateStore <: DurableStateStore {
    private let inner: DurableStateStore
    private let fence: FencingTokenStore
    private let resource: String
    private let tokenSource: () -> Option<Int64>

    public init(inner: DurableStateStore, fence: FencingTokenStore, lease: Lease) {
        this(inner, fence, lease.settings.leaseName, leaseTokenSource(lease))
    }

    public init(inner: DurableStateStore, fence: FencingTokenStore, resource: String,
        tokenSource: () -> Option<Int64>) {
        this.inner = inner
        this.fence = fence
        this.resource = resource
        this.tokenSource = tokenSource
    }

    public func get(persistenceId: String): Option<DurableStateRecord> {
        inner.get(persistenceId)
    }

    public func revisionOf(persistenceId: String): Int64 {
        inner.revisionOf(persistenceId)
    }

    public func upsert(persistenceId: String, revision: Int64, state: String): Unit {
        fence.fenced(resource, currentToken(resource, tokenSource), { => inner.upsert(persistenceId, revision, state) })
    }

    public func delete(persistenceId: String, revision: Int64): Unit {
        fence.fenced(resource, currentToken(resource, tokenSource), { => inner.delete(persistenceId, revision) })
    }

    public func persistenceIds(): Array<String> {
        inner.persistenceIds()
    }
}
