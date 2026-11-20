/*
 * CActor Platform.Durable - Durable Streams（P4 / G3）
 *
 * 把持久化事件流暴露成 Rivet Durable Streams 风格的语义（模块内实现，不新建日志）：
 *   - 每个流内 offset 从 1 单调递增（协议里的 ZERO_OFFSET = 0 表示"从头读"）；
 *   - 读支持游标续读与长轮询（无新消息时最多等 2s）；
 *   - fork：从某条消息处分叉出一条独立新流（复制前缀，之后互不影响）；
 *   - TTL：流到期后读写都明确失败（可区分于 500）；
 *   - producer-id：同一条流可由多个生产者追加，序号仍连续。
 *
 * 存储复用 `DurableStateStore`：元数据一条记录、每条消息一条记录（写入后不再修改）。
 * 交叉进程并发追加不在本层保证（同一流对象内用锁串行）；需要多写者时由存储提供者做
 * 条件写或选主——这与 journal_v2 的单写者假设一致。
 *
 * `DurableStreamHttp` 只做"协议头 ↔ 语义"的映射，不绑定具体 HTTP 框架，便于复用与测试。
 */
package cactor_platform.durable

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import cactor_foundation.hashing.{toHex}
import cactor_persistence.{DurableStateStore}

/** 协议里的显式起始偏移（0 = 从头读，实际第一条消息的 offset 是 1） */
public let ZERO_OFFSET: Int64 = 0

/** 单次读的上限（对标 Rivet 的 MAX_READ_ROWS） */
public let MAX_READ_ROWS: Int64 = 128

/** 无新消息时的长轮询上限（对标 Rivet 的 LONG_POLL_TIMEOUT_MS） */
public let DEFAULT_LONG_POLL: Duration = Duration.millisecond * 2000

/** 流已过期（TTL 到期）；与"不存在"和"内部错误"区分 */
public class DurableStreamExpiredException <: Exception {
    public let path: String
    public let expiresAtMillis: Int64

    public init(path: String, expiresAtMillis: Int64) {
        super("流 ${path} 已过期（expires-at=${expiresAtMillis}）")
        this.path = path
        this.expiresAtMillis = expiresAtMillis
    }
}

/** 一条流记录 */
public struct StreamRecord {
    public let offset: Int64
    public let producerId: String
    public let payload: String

    public init(offset: Int64, producerId: String, payload: String) {
        this.offset = offset
        this.producerId = producerId
        this.payload = payload
    }
}

/** 一次读的结果；nextOffset 是下一次续读应带的游标 */
public struct StreamRead {
    public let records: Array<StreamRecord>
    public let nextOffset: Int64
    public let forkedFrom: Option<String>
    public let forkOffset: Int64
    public let ttlSeconds: Int64
    public let expiresAtMillis: Int64

    public init(records: Array<StreamRecord>, nextOffset: Int64, forkedFrom: Option<String>,
        forkOffset: Int64, ttlSeconds: Int64, expiresAtMillis: Int64) {
        this.records = records
        this.nextOffset = nextOffset
        this.forkedFrom = forkedFrom
        this.forkOffset = forkOffset
        this.ttlSeconds = ttlSeconds
        this.expiresAtMillis = expiresAtMillis
    }

    public func isEmpty(): Bool {
        records.size == 0
    }
}

/** 流元数据（只在本文件使用） */
class StreamMeta {
    var nextOffset: Int64 = 1
    var ttlSeconds: Int64 = 0
    var expiresAtMillis: Int64 = 0
    var forkedFrom: String = ""
    var forkOffset: Int64 = 0

    func encode(): String {
        "${nextOffset}|${ttlSeconds}|${expiresAtMillis}|${forkedFrom}|${forkOffset}"
    }

    static func decode(text: String): StreamMeta {
        let parts = text.split("|")
        let m = StreamMeta()
        if (parts.size >= 5) {
            m.nextOffset = Int64.tryParse(parts[0]) ?? 1
            m.ttlSeconds = Int64.tryParse(parts[1]) ?? 0
            m.expiresAtMillis = Int64.tryParse(parts[2]) ?? 0
            m.forkedFrom = parts[3]
            m.forkOffset = Int64.tryParse(parts[4]) ?? 0
        }
        m
    }
}

func nowMillis(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}

/**
 * Durable Streams 注册表：按路径创建/取得流。
 *
 * @param store 底层持久化；同一存储上的同名路径共享同一条流
 */
public class DurableStreamRegistry {
    private let store: DurableStateStore
    private let lock = Mutex()

    public init(store: DurableStateStore) {
        this.store = store
    }

    /** 取得（不存在则创建）某条流；ttlSeconds > 0 时在创建时设置过期时间 */
    public func stream(path: String, ttlSeconds!: Int64 = 0): DurableStream {
        validatePath(path)
        synchronized(lock) {
            let key = metaKey(path)
            match (store.get(key)) {
                case Some(_) => DurableStream(this, store, path)
                case None =>
                    let meta = StreamMeta()
                    if (ttlSeconds > 0) {
                        meta.ttlSeconds = ttlSeconds
                        meta.expiresAtMillis = nowMillis() + ttlSeconds * 1000
                    }
                    saveMeta(path, meta)
                    DurableStream(this, store, path)
            }
        }
    }

    /** 流是否存在（不检查过期） */
    public func exists(path: String): Bool {
        store.get(metaKey(path)).isSome()
    }

    /**
     * 从 source 的第 atOffset 条消息处分叉出 target（复制 1..atOffset 的前缀）。
     * 目标必须不存在；分叉后两条流独立追加。
     */
    public func fork(source: String, target: String, atOffset: Int64): DurableStream {
        validatePath(source)
        validatePath(target)
        if (atOffset < 0) {
            throw IllegalArgumentException("fork 偏移不能为负: ${atOffset}")
        }
        synchronized(lock) {
            if (store.get(metaKey(target)).isSome()) {
                throw IllegalArgumentException("fork 目标已存在: ${target}")
            }
            let src = DurableStream(this, store, source)
            let meta = StreamMeta()
            meta.forkedFrom = source
            meta.forkOffset = atOffset
            meta.nextOffset = 1
            // 按读上限分块复制 1..atOffset（read 单次最多 MAX_READ_ROWS 条）
            var copied: Int64 = 0
            var from: Int64 = ZERO_OFFSET
            while (copied < atOffset) {
                let chunk = src.read(fromOffset: from, maxRows: MAX_READ_ROWS)
                if (chunk.isEmpty()) {
                    break
                }
                for (r in chunk.records) {
                    if (r.offset > atOffset) {
                        break
                    }
                    writeRecord(target, meta.nextOffset, r.producerId, r.payload)
                    meta.nextOffset++
                    copied++
                }
                from = chunk.nextOffset
            }
            saveMeta(target, meta)
            DurableStream(this, store, target)
        }
    }

    func loadMeta(path: String): StreamMeta {
        match (store.get(metaKey(path))) {
            case Some(r) => StreamMeta.decode(r.state)
            case None => StreamMeta()
        }
    }

    func saveMeta(path: String, meta: StreamMeta): Unit {
        let key = metaKey(path)
        let revision = store.revisionOf(key) + 1
        store.upsert(key, revision, meta.encode())
    }

    func writeRecord(path: String, offset: Int64, producerId: String, payload: String): Unit {
        let key = recordKey(path, offset)
        let revision = store.revisionOf(key) + 1
        store.upsert(key, revision, "${producerId}\n${payload}")
    }

    func readRecord(path: String, offset: Int64): Option<StreamRecord> {
        match (store.get(recordKey(path, offset))) {
            case Some(r) =>
                let idx = r.state.indexOf("\n")
                match (idx) {
                    case Some(i) =>
                        Some<StreamRecord>(StreamRecord(offset, r.state[..i], r.state[(i + 1)..]))
                    case None => Some<StreamRecord>(StreamRecord(offset, "unknown", r.state))
                }
            case None => None<StreamRecord>
        }
    }
}

/** 单条流上的操作 */
public class DurableStream {
    private let registry: DurableStreamRegistry
    private let store: DurableStateStore
    private let pathValue: String
    private let appendLock = Mutex()

    init(registry: DurableStreamRegistry, store: DurableStateStore, path: String) {
        this.registry = registry
        this.store = store
        this.pathValue = path
    }

    public prop path: String {
        get() { pathValue }
    }

    /** 下一条消息的 offset（也等于当前消息数 + 1） */
    public func nextOffset(): Int64 {
        registry.loadMeta(pathValue).nextOffset
    }

    public func ttlSeconds(): Int64 {
        registry.loadMeta(pathValue).ttlSeconds
    }

    public func expiresAtMillis(): Int64 {
        registry.loadMeta(pathValue).expiresAtMillis
    }

    public func forkedFrom(): Option<String> {
        let m = registry.loadMeta(pathValue)
        if (m.forkedFrom.isEmpty()) { None<String> } else { Some<String>(m.forkedFrom) }
    }

    public func forkOffset(): Int64 {
        registry.loadMeta(pathValue).forkOffset
    }

    /** 追加一条消息，返回它的 offset（从 1 开始单调）。过期时抛 DurableStreamExpiredException */
    public func append(payload: String, producerId!: String = "anonymous"): Int64 {
        if (producerId.contains("\n")) {
            throw IllegalArgumentException("producer-id 不能包含换行")
        }
        synchronized(appendLock) {
            let meta = registry.loadMeta(pathValue)
            ensureNotExpired(meta)
            let offset = meta.nextOffset
            registry.writeRecord(pathValue, offset, producerId, payload)
            meta.nextOffset = offset + 1
            registry.saveMeta(pathValue, meta)
            offset
        }
    }

    /**
     * 从 fromOffset 续读（fromOffset <= 0 表示从头读），最多 maxRows 条。
     * nextOffset 在无新消息时保持为 fromOffset 的原值，便于客户端原地重试。
     */
    public func read(fromOffset!: Int64 = ZERO_OFFSET, maxRows!: Int64 = MAX_READ_ROWS): StreamRead {
        if (maxRows <= 0) {
            throw IllegalArgumentException("maxRows 必须为正数: ${maxRows}")
        }
        let meta = registry.loadMeta(pathValue)
        ensureNotExpired(meta)
        let start = if (fromOffset <= ZERO_OFFSET) { 1 } else { fromOffset }
        let limit = if (maxRows > MAX_READ_ROWS) { MAX_READ_ROWS } else { maxRows }
        let rows = ArrayList<StreamRecord>()
        var cursor = start
        while (cursor < meta.nextOffset && rows.size < limit) {
            match (registry.readRecord(pathValue, cursor)) {
                case Some(r) => rows.add(r)
                case None => ()  // 保留缺口（例如保留策略删除了旧消息），不伪装成连续
            }
            cursor++
        }
        let next = if (rows.size > 0) { rows[rows.size - 1].offset + 1 } else { start }
        StreamRead(rows.toArray(), next, forkedFrom(), meta.forkOffset, meta.ttlSeconds, meta.expiresAtMillis)
    }

    /**
     * 长轮询读：没有新消息时最多等 timeout（默认 2s），期间有新消息立即返回。
     * 通过轮询存储实现，跨进程写者也能被观察到（代价是无消息时每 20ms 读一次）。
     */
    public func readLongPoll(fromOffset!: Int64 = ZERO_OFFSET, timeout!: Duration = DEFAULT_LONG_POLL,
        maxRows!: Int64 = MAX_READ_ROWS): StreamRead {
        let deadline = MonoTime.now() + timeout
        while (MonoTime.now() < deadline) {
            let result = read(fromOffset: fromOffset, maxRows: maxRows)
            if (!result.isEmpty()) {
                return result
            }
            sleep(Duration.millisecond * 20)
        }
        // 超时：返回最后一次读的结果（通常为空），调用方按 nextOffset 原地重试
        read(fromOffset: fromOffset, maxRows: maxRows)
    }

    /** 从本流第 atOffset 条处分叉出 target（等价于 registry.fork(path, target, atOffset)） */
    public func fork(target: String, atOffset: Int64): DurableStream {
        registry.fork(pathValue, target, atOffset)
    }

    private func ensureNotExpired(meta: StreamMeta): Unit {
        if (meta.expiresAtMillis > 0 && nowMillis() > meta.expiresAtMillis) {
            throw DurableStreamExpiredException(pathValue, meta.expiresAtMillis)
        }
    }
}

/** 路径进入存储键前做 URL 安全化：拒绝分隔符与换行，键里用十六进制 */
func validatePath(path: String): Unit {
    if (path.isEmpty()) {
        throw IllegalArgumentException("流路径不能为空")
    }
    if (path.contains("|") || path.contains("\n")) {
        throw IllegalArgumentException("流路径不能包含 '|' 或换行: ${path}")
    }
}

func hexOf(path: String): String {
    toHex(path.toArray())
}

func metaKey(path: String): String {
    "ds-meta-${hexOf(path)}"
}

func recordKey(path: String, offset: Int64): String {
    "ds-msg-${hexOf(path)}-${offset}"
}

// ---------------- 协议适配层（头名与 Rivet Durable Streams 对齐） ----------------

public let HEADER_NEXT_OFFSET: String = "stream-next-offset"
public let HEADER_CURSOR: String = "stream-cursor"
public let HEADER_SEQ: String = "stream-seq"
public let HEADER_TTL: String = "stream-ttl"
public let HEADER_EXPIRES_AT: String = "stream-expires-at"
public let HEADER_FORKED_FROM: String = "stream-forked-from"
public let HEADER_FORK_OFFSET: String = "stream-fork-offset"
public let HEADER_PRODUCER_ID: String = "producer-id"

/** 与具体 HTTP 框架无关的请求/响应：由调用方把真实请求的 method/path/headers/body 填进来 */
public struct DurableRequest {
    public let method: String
    public let path: String
    public let headers: HashMap<String, String>
    public let body: String

    public init(method: String, path: String, headers!: HashMap<String, String> = HashMap<String, String>(),
        body!: String = "") {
        this.method = method
        this.path = path
        this.headers = headers
        this.body = body
    }
}

public struct DurableResponse {
    public let status: Int64
    public let headers: HashMap<String, String>
    public let body: String

    public init(status: Int64, headers: HashMap<String, String>, body: String) {
        this.status = status
        this.headers = headers
        this.body = body
    }
}

/**
 * 协议头映射：`GET /v1/stream/{path}`（长轮询读）、`POST /v1/stream/{path}`（追加或 fork）。
 *
 * 请求头：`stream-next-offset`（游标）、`producer-id`、`stream-ttl`（首次创建）、
 *        `stream-forked-from` + `stream-fork-offset`（fork）。
 * 响应头：`stream-next-offset`/`stream-cursor`/`stream-seq`/`stream-ttl`/`stream-expires-at`/
 *        `stream-forked-from`/`stream-fork-offset`。
 * 响应体：每条记录一行 `offset\tproducer-id\tpayload`（payload 内的换行原样保留）。
 * 过期返回 410（而不是 500）；未知方法 405。
 */
public class DurableStreamProtocol {
    private let registry: DurableStreamRegistry
    private let basePath: String

    public init(registry: DurableStreamRegistry, basePath!: String = "/v1/stream") {
        this.registry = registry
        this.basePath = basePath
    }

    public func handle(request: DurableRequest): DurableResponse {
        let tail = pathTail(request.path)
        if (tail.isEmpty()) {
            return DurableResponse(404, HashMap<String, String>(), "missing stream path")
        }
        match (request.method) {
            case "GET" =>
                return handleRead(tail, request)
            case "POST" =>
                return handleWrite(tail, request)
            case _ =>
                return DurableResponse(405, HashMap<String, String>(), "method not allowed")
        }
    }

    private func pathTail(fullPath: String): String {
        let prefix = if (basePath.endsWith("/")) { basePath } else { basePath + "/" }
        if (!fullPath.startsWith(prefix)) {
            return ""
        }
        fullPath[prefix.size..]
    }

    private func handleRead(path: String, request: DurableRequest): DurableResponse {
        let from = headerInt(request.headers, HEADER_NEXT_OFFSET, ZERO_OFFSET)
        let stream = registry.stream(path)
        try {
            let result = stream.readLongPoll(fromOffset: from)
            return buildReadResponse(result)
        } catch (e: DurableStreamExpiredException) {
            let headers = HashMap<String, String>()
            headers[HEADER_EXPIRES_AT] = e.expiresAtMillis.toString()
            return DurableResponse(410, headers, "stream expired")
        }
    }

    private func handleWrite(path: String, request: DurableRequest): DurableResponse {
        // fork：把源流的前缀复制到 path
        if (let Some(sourceHeader) <- request.headers.get(HEADER_FORKED_FROM)) {
            let at = headerInt(request.headers, HEADER_FORK_OFFSET, 0)
            try {
                let created = registry.fork(sourceHeader, path, at)
                let headers = streamHeaders(created)
                headers[HEADER_NEXT_OFFSET] = created.nextOffset().toString()
                headers[HEADER_CURSOR] = created.nextOffset().toString()
                return DurableResponse(201, headers, "")
            } catch (e: DurableStreamExpiredException) {
                let headers = HashMap<String, String>()
                headers[HEADER_EXPIRES_AT] = e.expiresAtMillis.toString()
                return DurableResponse(410, headers, "stream expired")
            } catch (e: IllegalArgumentException) {
                return DurableResponse(409, HashMap<String, String>(), e.message)
            }
        }
        let producer = request.headers.get(HEADER_PRODUCER_ID) ?? "anonymous"
        let ttl = headerInt(request.headers, HEADER_TTL, 0)
        let stream = registry.stream(path, ttlSeconds: ttl)
        try {
            let offset = stream.append(request.body, producerId: producer)
            let headers = streamHeaders(stream)
            headers[HEADER_SEQ] = offset.toString()
            headers[HEADER_NEXT_OFFSET] = (offset + 1).toString()
            headers[HEADER_CURSOR] = (offset + 1).toString()
            return DurableResponse(200, headers, "")
        } catch (e: DurableStreamExpiredException) {
            let headers = HashMap<String, String>()
            headers[HEADER_EXPIRES_AT] = e.expiresAtMillis.toString()
            return DurableResponse(410, headers, "stream expired")
        }
    }

    private func buildReadResponse(result: StreamRead): DurableResponse {
        let headers = HashMap<String, String>()
        headers[HEADER_NEXT_OFFSET] = result.nextOffset.toString()
        headers[HEADER_CURSOR] = result.nextOffset.toString()
        headers[HEADER_SEQ] = if (result.records.size > 0) { result.records[result.records.size - 1].offset.toString() } else { "0" }
        headers[HEADER_TTL] = result.ttlSeconds.toString()
        headers[HEADER_EXPIRES_AT] = result.expiresAtMillis.toString()
        match (result.forkedFrom) {
            case Some(src) =>
                headers[HEADER_FORKED_FROM] = src
                headers[HEADER_FORK_OFFSET] = result.forkOffset.toString()
            case None => ()
        }
        let sb = StringBuilder()
        for (r in result.records) {
            sb.append("${r.offset}\t${r.producerId}\t${r.payload}\n")
        }
        DurableResponse(200, headers, sb.toString())
    }

    private func streamHeaders(stream: DurableStream): HashMap<String, String> {
        let headers = HashMap<String, String>()
        headers[HEADER_TTL] = stream.ttlSeconds().toString()
        headers[HEADER_EXPIRES_AT] = stream.expiresAtMillis().toString()
        match (stream.forkedFrom()) {
            case Some(src) =>
                headers[HEADER_FORKED_FROM] = src
                headers[HEADER_FORK_OFFSET] = stream.forkOffset().toString()
            case None => ()
        }
        headers
    }

    private func headerInt(headers: HashMap<String, String>, name: String, fallback: Int64): Int64 {
        match (headers.get(name)) {
            case Some(v) => Int64.tryParse(v) ?? fallback
            case None => fallback
        }
    }
}
