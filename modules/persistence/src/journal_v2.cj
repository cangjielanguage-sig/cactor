/*
 * CActor Persistence - 文件日志 v2（E8′，对标 Akka Persistence journal 的语义）
 *
 * 文件格式（每个持久化标识一个文件 <hex(标识)>.journal；标识过长时文件名为 h-<sha256>.journal，见 safeFileName）：
 *   文件头：魔数 "CAJRNL02" + deletedTo（8 字节大端，deleteTo 之后保留的最高已删除序号）+ 标识（4 字节长度 + UTF-8）。
 *   标识写进文件头：文件名是哈希时 persistenceIds() 据此取回原标识（P10：此前标识过长时文件名超过系统上限、写入失败）
 *   记录：[4 字节负载长度][4 字节 CRC32(负载)][负载]
 *   负载：seqNr、eventId、persistenceId、manifest（事件类型）、eventData、timestamp、tags（个数 + 各 tag）
 *
 * 为什么这样设计：
 * - 序号写进记录：deleteTo 之后位置不再等于序号；恢复与查询都按序号，不按时间戳（P5：同一毫秒的事件曾被重复或遗漏）。
 * - CRC + 长度 + 序号连续性：恢复时区分"写到一半的尾部"（含断电后的零填充，P6）与中间损坏。
 *   尾部残缺自动截断——它所在的那批写入从未被确认（确认发生在 fsync 之后）；
 *   中间损坏而其后仍有完整记录时拒绝启动，抛 PersistenceCorruptionException，不静默丢掉已确认的事件。
 * - append(expectedSeq)：期望序号与当前最高序号不符即拒绝（P10），同一标识出现两个写者时后写者失败，交给监督处理。
 * - 常驻追加句柄（数量超过上限时按最近最少使用关闭），追加不再整读文件（此前每次追加都读一遍全文件，O(n²)）。
 * - 组提交：写入在 lock 内完成，fsync 在 lock 外、syncLock 内执行；fsync 进行期间到达的写入会被下一次 fsync 一并覆盖，
 *   并发写者只付一次 fsync。persistAll 一批事件一次写入、一次 fsync。
 * - v1 日志（无文件头）首次打开时原子迁移为 v2，序号按位置从 1 编号。
 *
 * 锁顺序：syncLock → lock；存储级的 mutex 从不与日志的锁嵌套持有（LRU 关闭句柄在存储锁之外进行）。
 */
package cactor_persistence

import std.collection.*
import std.fs.*
import std.sync.*
import cactor_foundation.hashing.{crc32}
import cactor_foundation.logging.{Logger, Logging}

let log_journal: Logger = Logging.logger("cactor_persistence.journal")

let JOURNAL_MAGIC: Array<UInt8> = "CAJRNL02".toArray()
// 文件头的定长部分：魔数 8 + deletedTo 8 + 标识长度 4
let JOURNAL_HEADER_FIXED: Int64 = 20
let RECORD_HEADER_SIZE: Int64 = 8
// 负载至少包含 8 字节序号
let MIN_PAYLOAD_SIZE: Int64 = 8

/**
 * 追加时期望序号与日志当前最高序号不符（P10）：同一持久化标识有两个写者，或写者的状态已过期。
 * 事件没有写入；写者应停止或重启后从日志恢复
 */
public class JournalConflictException <: PersistenceException {
    public let persistenceId: String
    public let expectedSequenceNr: Int64
    public let actualSequenceNr: Int64

    public init(persistenceId: String, expectedSequenceNr: Int64, actualSequenceNr: Int64) {
        super("日志 ${persistenceId} 序号冲突：写者以为当前最高序号是 ${expectedSequenceNr}，实际是 ${actualSequenceNr}（同一标识有两个写者？）")
        this.persistenceId = persistenceId
        this.expectedSequenceNr = expectedSequenceNr
        this.actualSequenceNr = actualSequenceNr
    }
}

// ---------------- 编码 ----------------

func putU32(out: Array<UInt8>, pos: Int64, v: Int64): Unit {
    out[pos] = UInt8((v >> 24) & 0xFF)
    out[pos + 1] = UInt8((v >> 16) & 0xFF)
    out[pos + 2] = UInt8((v >> 8) & 0xFF)
    out[pos + 3] = UInt8(v & 0xFF)
}

func getU32(data: Array<UInt8>, pos: Int64): Int64 {
    (Int64(data[pos]) << 24) | (Int64(data[pos + 1]) << 16) | (Int64(data[pos + 2]) << 8) | Int64(data[pos + 3])
}

func getI64(data: Array<UInt8>, pos: Int64): Int64 {
    var v: Int64 = 0
    for (i in 0..8) {
        v = (v << 8) | Int64(data[pos + i])
    }
    v
}

func journalHeader(deletedTo: Int64, persistenceId: String): Array<UInt8> {
    let id = persistenceId.toArray()
    let out = Array<UInt8>(JOURNAL_HEADER_FIXED + id.size, repeat: 0)
    JOURNAL_MAGIC.copyTo(out, 0, 0, JOURNAL_MAGIC.size)
    for (i in 0..8) {
        out[8 + i] = UInt8((deletedTo >> (56 - 8 * i)) & 0xFF)
    }
    putU32(out, 16, id.size)
    id.copyTo(out, 0, JOURNAL_HEADER_FIXED, id.size)
    out
}

/** 文件头的结束位置；文件头不完整时为 None（调用方已确认魔数） */
func journalHeaderEnd(content: Array<UInt8>): Option<Int64> {
    if (content.size < JOURNAL_HEADER_FIXED) {
        return None
    }
    let end = JOURNAL_HEADER_FIXED + getU32(content, 16)
    if (end > content.size) { None } else { Some(end) }
}

/** 文件头里的持久化标识（文件名是哈希时用它取回原标识） */
func journalIdOf(path: String): Option<String> {
    try {
        let content = File.readFrom(path)
        if (!hasJournalMagic(content)) {
            return None
        }
        match (journalHeaderEnd(content)) {
            case Some(end) => Some(String.fromUtf8(content[JOURNAL_HEADER_FIXED..end]))
            case None => None
        }
    } catch (_: Exception) {
        None
    }
}

func hasJournalMagic(content: Array<UInt8>): Bool {
    if (content.size < JOURNAL_MAGIC.size) {
        return false
    }
    for (i in 0..JOURNAL_MAGIC.size) {
        if (content[i] != JOURNAL_MAGIC[i]) {
            return false
        }
    }
    true
}

/** 文件头没写完（创建文件的那次写入被中断）：内容是魔数的前缀，或魔数完整但文件头不完整 */
func isTornHeader(content: Array<UInt8>): Bool {
    if (hasJournalMagic(content)) {
        return journalHeaderEnd(content).isNone()
    }
    if (content.size >= JOURNAL_MAGIC.size) {
        return false
    }
    let n = content.size
    for (i in 0..n) {
        if (content[i] != JOURNAL_MAGIC[i]) {
            return false
        }
    }
    true
}

func encodeJournalRecord(seq: Int64, e: Event): Array<UInt8> {
    let w = RecordWriter()
        .putInt64(seq)
        .putString(e.getEventId())
        .putString(e.getActorId())
        .putString(e.getEventType())
        .putString(e.getEventData())
        .putInt64(e.getTimestamp())
    let tags = e.getTags()
    w.putInt64(tags.size)
    for (t in tags) {
        w.putString(t)
    }
    let payload = w.payload()
    let out = Array<UInt8>(RECORD_HEADER_SIZE + payload.size, repeat: 0)
    putU32(out, 0, payload.size)
    putU32(out, 4, Int64(crc32(payload)))
    payload.copyTo(out, 0, RECORD_HEADER_SIZE, payload.size)
    out
}

func decodeJournalPayload(payload: Array<UInt8>): EventEnvelope {
    let r = RecordReader(payload)
    let seq = r.getInt64()
    let eventId = r.getString()
    let owner = r.getString()
    let eventType = r.getString()
    let eventData = r.getString()
    let ts = r.getInt64()
    let n = r.getInt64()
    if (n < 0 || n > payload.size) {
        throw PersistenceCorruptionException("tags 个数非法：${n}")
    }
    let tags = Array<String>(n, { _ => r.getString() })
    EventEnvelope(owner, seq, BasicEvent(eventId, owner, eventType, eventData, ts, tags))
}

/** pos 处是否是一条完整且 CRC 正确的记录：Some((序号, 下一条的位置)) */
func recordAt(content: Array<UInt8>, pos: Int64, end: Int64): Option<(Int64, Int64)> {
    if (pos + RECORD_HEADER_SIZE > end) {
        return None
    }
    let len = getU32(content, pos)
    let start = pos + RECORD_HEADER_SIZE
    if (len < MIN_PAYLOAD_SIZE || start + len > end) {
        return None
    }
    if (Int64(crc32(content, start, start + len)) != getU32(content, pos + 4)) {
        return None
    }
    Some((getI64(content, start), start + len))
}

/** 扫描结果：deletedTo、最高序号、文件头结束位置、有效长度、是否有残缺尾部 */
struct JournalScan {
    let deletedTo: Int64
    let highestSeq: Int64
    let headerEnd: Int64
    let validLength: Int64
    let tornTail: Bool

    init(deletedTo: Int64, highestSeq: Int64, headerEnd: Int64, validLength: Int64, tornTail: Bool) {
        this.deletedTo = deletedTo
        this.highestSeq = highestSeq
        this.headerEnd = headerEnd
        this.validLength = validLength
        this.tornTail = tornTail
    }
}

/**
 * 校验整个 v2 日志。第一条无效记录之后若还能找到完整记录，说明是中间损坏而不是尾部残缺：抛异常，不截断
 */
func scanJournal(content: Array<UInt8>, path: String): JournalScan {
    let deletedTo = getI64(content, JOURNAL_MAGIC.size)
    let headerEnd = journalHeaderEnd(content).getOrThrow()
    var highest = deletedTo
    var pos = headerEnd
    let end = content.size
    while (pos < end) {
        match (recordAt(content, pos, end)) {
            case Some((seq, next)) =>
                if (seq != highest + 1) {
                    throw PersistenceCorruptionException("日志 ${path} 在偏移 ${pos} 处序号不连续：期望 ${highest + 1}，实际 ${seq}")
                }
                highest = seq
                pos = next
            case None =>
                var q = pos + 1
                while (q + RECORD_HEADER_SIZE <= end) {
                    if (recordAt(content, q, end).isSome()) {
                        throw PersistenceCorruptionException(
                            "日志 ${path} 在偏移 ${pos} 处损坏，偏移 ${q} 处仍有完整记录：拒绝截断，请人工处理（已确认的事件不能静默丢弃）")
                    }
                    q++
                }
                return JournalScan(deletedTo, highest, headerEnd, pos, true)
        }
    }
    JournalScan(deletedTo, highest, headerEnd, pos, false)
}

/** v1 日志（无文件头，[长度][eventId, actorId, eventType, eventData, ts]）转为 v2 内容，序号按位置从 1 编号 */
func migrateLegacyContent(content: Array<UInt8>, persistenceId: String): (Array<UInt8>, Int64) {
    let (records, _) = splitRecords(content)
    let out = ArrayList<UInt8>(journalHeader(0, persistenceId))
    var seq: Int64 = 0
    for (payload in records) {
        let r = RecordReader(payload)
        let eventId = r.getString()
        let owner = r.getString()
        let eventType = r.getString()
        let eventData = r.getString()
        let ts = r.getInt64()
        seq++
        out.add(all: encodeJournalRecord(seq, BasicEvent(eventId, owner, eventType, eventData, ts)))
    }
    (out.toArray(), seq)
}

/** 解码 [start, end) 中序号在 [fromSeq, toSeq] 的记录（重放时仍校验 CRC，发现磁盘静默损坏） */
func decodeRange(content: Array<UInt8>, start: Int64, end: Int64, fromSeq: Int64, toSeq: Int64, path: String): Array<EventEnvelope> {
    let out = ArrayList<EventEnvelope>()
    var pos = start
    while (pos < end) {
        match (recordAt(content, pos, end)) {
            case Some((seq, next)) =>
                if (seq > toSeq) {
                    break
                }
                if (seq >= fromSeq) {
                    out.add(decodeJournalPayload(content[pos + RECORD_HEADER_SIZE..next]))
                }
                pos = next
            case None => throw PersistenceCorruptionException("日志 ${path} 在偏移 ${pos} 处损坏（重放时 CRC 校验失败）")
        }
    }
    out.toArray()
}

// ---------------- 单个日志文件 ----------------

let journalUseTick = AtomicInt64(0)

class JournalFile {
    let persistenceId: String
    let path: String
    private let dir: String
    private let sync: Bool
    private let openHandles: AtomicInt64
    let lock = Mutex()
    private let syncLock = Mutex()
    private var handle: Option<File> = None
    // fsync 进行中被关闭的句柄：等 fsync 结束再真正关闭（fsync 用的 fd 不能被提前关掉或复用）
    private let retired = ArrayList<File>()
    private var syncing = false
    private var loaded = false
    private var highestSeq: Int64 = 0
    private var deletedTo: Int64 = 0
    private var headerEnd: Int64 = 0
    // 文件有效长度（含文件头）；0 表示文件不存在
    private var length: Int64 = 0
    // 组提交：写入代数与已落盘代数
    private var writeGen: Int64 = 0
    private var syncedGen: Int64 = 0
    var lastUse: Int64 = 0

    init(persistenceId: String, path: String, dir: String, sync: Bool, openHandles: AtomicInt64) {
        this.persistenceId = persistenceId
        this.path = path
        this.dir = dir
        this.sync = sync
        this.openHandles = openHandles
    }

    func hasOpenHandle(): Bool {
        synchronized(lock) { handle.isSome() }
    }

    // ---- 以下 private 函数都在 lock 内调用 ----

    private func diskSize(): Int64 {
        if (exists(path)) { FileInfo(path).size } else { 0 }
    }

    /** 首次使用，或文件被本对象之外改动过（另一个存储实例 / 进程写入、截断或替换）时重新扫描 */
    private func ensureLoaded(): Unit {
        let disk = diskSize()
        if (loaded && disk == length) {
            return
        }
        reload(disk)
    }

    private func reload(disk: Int64): Unit {
        closeHandle()
        loaded = false
        highestSeq = 0
        deletedTo = 0
        headerEnd = 0
        length = 0
        syncedGen = writeGen
        if (disk == 0) {
            loaded = true
            return
        }
        var content = File.readFrom(path)
        if (!hasJournalMagic(content)) {
            if (isTornHeader(content)) {
                // 创建文件的那次写入没完成：其中不可能有已确认的事件
                log_journal.warn("日志 ${path} 文件头残缺（${content.size} 字节），按空日志处理")
                remove(path)
                loaded = true
                return
            }
            let (migrated, n) = migrateLegacyContent(content, persistenceId)
            writeFileAtomically(path, migrated, sync)
            log_journal.info("日志 ${path} 由 v1 格式迁移为 v2：${n} 条事件")
            content = migrated
        }
        let scan = scanJournal(content, path)
        if (scan.tornTail) {
            log_journal.warn("日志 ${path} 尾部残缺 ${content.size - scan.validLength} 字节（未确认的写入），截断到 ${scan.validLength}")
            let f = File(path, OpenMode.ReadWrite)
            try {
                f.setLength(scan.validLength)
                if (sync) {
                    fsyncFile(f, path)
                }
            } finally {
                f.close()
            }
        }
        deletedTo = scan.deletedTo
        highestSeq = scan.highestSeq
        headerEnd = scan.headerEnd
        length = scan.validLength
        loaded = true
    }

    private func openHandle(): File {
        match (handle) {
            case Some(f) => f
            case None =>
                let f = File(path, OpenMode.Append)
                handle = Some(f)
                openHandles.fetchAdd(1)
                f
        }
    }

    /** 关闭追加句柄：还有未落盘的写入时先 fsync；fsync 进行中则推迟到它结束 */
    private func closeHandle(): Unit {
        if (let Some(f) <- handle) {
            if (sync && writeGen > syncedGen) {
                fsyncFile(f, path)
                syncedGen = writeGen
            }
            handle = None
            openHandles.fetchSub(1)
            if (syncing) {
                retired.add(f)
            } else {
                f.close()
            }
        }
    }

    // ---- 对外操作 ----

    func highestSequenceNr(): Int64 {
        synchronized(lock) {
            ensureLoaded()
            highestSeq
        }
    }

    /** 追加；expectedSeq 为 None 时不校验（旧的 saveEvent 语义）。返回追加后的最高序号，返回时已落盘（sync 时） */
    func append(events: Array<Event>, expectedSeq: Option<Int64>): Int64 {
        let (myGen, newHighest, created) = synchronized(lock) {
            ensureLoaded()
            if (let Some(exp) <- expectedSeq) {
                if (exp != highestSeq) {
                    throw JournalConflictException(persistenceId, exp, highestSeq)
                }
            }
            if (events.isEmpty()) {
                return highestSeq
            }
            let buf = ArrayList<UInt8>()
            let created = length == 0
            if (created) {
                let header = journalHeader(deletedTo, persistenceId)
                headerEnd = header.size
                buf.add(all: header)
            }
            var seq = highestSeq
            for (e in events) {
                seq++
                buf.add(all: encodeJournalRecord(seq, e))
            }
            let bytes = buf.toArray()
            try {
                let f = openHandle()
                f.write(bytes)
                f.flush()
            } catch (e: Exception) {
                // 可能留下半条记录：下次使用时按尾部残缺重新扫描
                loaded = false
                closeHandle()
                throw PersistenceException("写入日志 ${path} 失败", e)
            }
            highestSeq = seq
            length += bytes.size
            writeGen++
            lastUse = journalUseTick.fetchAdd(1)
            (writeGen, seq, created)
        }
        if (sync) {
            groupSync(myGen)
            if (created) {
                syncDirectory(dir)
            }
        }
        newHighest
    }

    /**
     * 组提交：myGen 已被某次 fsync 覆盖就直接返回；否则由本线程执行一次 fsync，覆盖到当前为止的全部写入。
     * fsync 在 lock 之外进行，期间其他写者可以继续写入（它们由下一次 fsync 覆盖）
     */
    private func groupSync(myGen: Int64): Unit {
        synchronized(syncLock) {
            let (target, h) = synchronized(lock) {
                if (syncedGen >= myGen) {
                    return
                }
                syncing = true
                (writeGen, handle)
            }
            var ok = false
            try {
                if (let Some(f) <- h) {
                    fsyncFile(f, path)
                }
                ok = true
            } finally {
                synchronized(lock) {
                    syncing = false
                    if (ok && target > syncedGen) {
                        syncedGen = target
                    }
                    if (!ok) {
                        // 不知道哪些写入已落盘：下次使用时重新扫描
                        loaded = false
                    }
                    for (f in retired) {
                        f.close()
                    }
                    retired.clear()
                }
            }
        }
    }

    func replay(fromSeq: Int64, toSeq: Int64): Array<EventEnvelope> {
        let (content, start, end) = synchronized(lock) {
            ensureLoaded()
            if (length == 0 || fromSeq > highestSeq || toSeq <= deletedTo || fromSeq > toSeq) {
                return Array<EventEnvelope>()
            }
            (File.readFrom(path), headerEnd, length)
        }
        decodeRange(content, start, min(end, content.size), max(fromSeq, 1), toSeq, path)
    }

    /** 删除序号 ≤ toSeq 的事件（最高序号保留，对标 Akka deleteMessages(toSequenceNr)）：原子改写文件 */
    func deleteTo(toSeq: Int64): Unit {
        synchronized(syncLock) {
            synchronized(lock) {
                ensureLoaded()
                let upTo = min(toSeq, highestSeq)
                if (upTo <= deletedTo) {
                    return
                }
                let content = File.readFrom(path)
                let header = journalHeader(upTo, persistenceId)
                let out = ArrayList<UInt8>(header)
                var pos = headerEnd
                while (pos < length) {
                    match (recordAt(content, pos, length)) {
                        case Some((seq, next)) =>
                            if (seq > upTo) {
                                out.add(all: content[pos..next])
                            }
                            pos = next
                        case None => throw PersistenceCorruptionException("日志 ${path} 在偏移 ${pos} 处损坏，deleteTo 中止")
                    }
                }
                closeHandle()
                let bytes = out.toArray()
                writeFileAtomically(path, bytes, sync)
                deletedTo = upTo
                headerEnd = header.size
                length = bytes.size
                syncedGen = writeGen
            }
        }
    }

    /** 删除整个日志（旧 deleteEvents 语义：连同最高序号一起清除） */
    func deleteAll(): Unit {
        synchronized(syncLock) {
            synchronized(lock) {
                closeHandle()
                if (exists(path)) {
                    remove(path)
                    if (sync) {
                        syncDirectory(dir)
                    }
                }
                highestSeq = 0
                deletedTo = 0
                length = 0
                loaded = true
            }
        }
    }

    /** 关闭追加句柄（LRU 淘汰或存储关闭）；未落盘的写入先 fsync */
    func release(): Unit {
        synchronized(lock) {
            closeHandle()
        }
    }
}
