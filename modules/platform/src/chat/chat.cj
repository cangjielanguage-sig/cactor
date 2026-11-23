/*
 * CActor Platform.Chat - 实时聊天（线程 / 历史 / 订阅 / 输入中 / 已读）（对标 rivet-chat）
 *
 * 线程模型与 Rivet 对齐：
 *   GetDirectThread   -> directThread(a, b)：两人直聊，线程 id 由双方 id 规范化生成（幂等、对称）；
 *   GetThreadHistory  -> history(threadId, fromOffset, maxRows)：按游标分页读历史；
 *   SendChatMessage   -> send(threadId, senderId, text)：追加一条消息；
 *   WatchThread       -> watch(threadId, fromOffset, timeout)：长轮询等待新消息（不占调用方锁）；
 *   GetThreadTopic    -> topic(threadId)；
 *   SetTypingStatus   -> setTyping(threadId, playerId, typing) / typingStatus(threadId)（带 TTL）；
 *   SetThreadRead     -> markRead(threadId, playerId, offset) / readState(threadId, playerId)。
 *
 * 历史复用 G3 `DurableStream`（offset 单调、游标续读、长轮询、可换持久化 store），不新造日志；
 * 输入中/已读是易失状态（进程内），重启后从 0 开始，符合其"瞬时"语义。
 *
 * 失败显式：未知线程、非线程成员、空消息一律抛 `IllegalArgumentException`，不静默丢消息。
 */
package cactor_platform.chat

import std.collection.*
import std.sync.*
import std.time.*
import stdx.encoding.json.*
import cactor_foundation.hashing.{toHex}
import cactor_persistence.{DurableStateStore, MemoryDurableStateStore}
import cactor_platform.durable.{DurableStream, DurableStreamRegistry}

/** 一条聊天消息 */
public class ChatMessage {
    public let id: String
    public let threadId: String
    public let senderId: String
    public let text: String
    public let sentAtMillis: Int64
    public let offset: Int64

    public init(threadId: String, senderId: String, text: String, sentAtMillis: Int64, offset: Int64) {
        this.threadId = threadId
        this.senderId = senderId
        this.text = text
        this.sentAtMillis = sentAtMillis
        this.offset = offset
        this.id = "${threadId}#${offset}"
    }
}

/** 线程元信息 */
public class ChatThreadInfo {
    public let id: String
    public let memberIds: Array<String>
    public let topic: String
    public let createdAtMillis: Int64

    public init(id: String, memberIds: Array<String>, topic: String, createdAtMillis: Int64) {
        this.id = id
        this.memberIds = memberIds
        this.topic = topic
        this.createdAtMillis = createdAtMillis
    }
}

/** 一次历史读取；nextOffset 是下一次续读的游标 */
public class ChatHistory {
    public let threadId: String
    public let messages: Array<ChatMessage>
    public let nextOffset: Int64

    public init(threadId: String, messages: Array<ChatMessage>, nextOffset: Int64) {
        this.threadId = threadId
        this.messages = messages
        this.nextOffset = nextOffset
    }

    public func isEmpty(): Bool {
        messages.size == 0
    }
}

/** 输入中状态（瞬时） */
public class ChatTypingState {
    public let playerId: String
    public let typing: Bool
    public let updatedAtMillis: Int64

    public init(playerId: String, typing: Bool, updatedAtMillis: Int64) {
        this.playerId = playerId
        this.typing = typing
        this.updatedAtMillis = updatedAtMillis
    }
}

/** 已读位置（瞬时） */
public class ChatReadState {
    public let threadId: String
    public let playerId: String
    public let lastReadOffset: Int64
    public let updatedAtMillis: Int64

    public init(threadId: String, playerId: String, lastReadOffset: Int64, updatedAtMillis: Int64) {
        this.threadId = threadId
        this.playerId = playerId
        this.lastReadOffset = lastReadOffset
        this.updatedAtMillis = updatedAtMillis
    }
}

/** 内部线程条目：元信息 + 复用的流对象（同一实例才能保证追加串行） */
class ChatThreadEntry {
    let info: ChatThreadInfo
    let stream: DurableStream

    init(info: ChatThreadInfo, stream: DurableStream) {
        this.info = info
        this.stream = stream
    }
}

/** 聊天服务：单节点、线程安全；历史可随 DurableStream 换成持久化存储 */
public class ChatService {
    private let registry: DurableStreamRegistry
    private let lock = Mutex()
    private let threads = HashMap<String, ChatThreadEntry>()
    private let typingStates = HashMap<String, HashMap<String, ChatTypingState>>()
    private let reads = HashMap<String, HashMap<String, ChatReadState>>()
    private let typingTtlMillis: Int64

    private init(store: DurableStateStore, typingTtl: Duration) {
        if (typingTtl <= Duration.Zero) {
            throw IllegalArgumentException("输入中 TTL 必须为正")
        }
        this.registry = DurableStreamRegistry(store)
        this.typingTtlMillis = typingTtl.toMilliseconds()
    }

    /** 创建服务；默认内存存储（进程内），生产可传文件/数据库实现 */
    public static func create(store!: DurableStateStore = MemoryDurableStateStore(),
        typingTtl!: Duration = Duration.second * 10): ChatService {
        ChatService(store, typingTtl)
    }

    private func nowMillis(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds()
    }

    private func isMember(info: ChatThreadInfo, playerId: String): Bool {
        for (id in info.memberIds) {
            if (id == playerId) {
                return true
            }
        }
        false
    }

    private func threadPath(a: String, b: String): String {
        if (a.isEmpty() || b.isEmpty() || a == b) {
            throw IllegalArgumentException("直聊线程需要两个不同的非空玩家")
        }
        // 规范化顺序 + 十六进制转义，避免 id 内的分隔符导致不同玩家对撞成同一线程
        if (a < b) {
            "chat/direct/${toHex(a.toArray())}-${toHex(b.toArray())}"
        } else {
            "chat/direct/${toHex(b.toArray())}-${toHex(a.toArray())}"
        }
    }

    /** 取得（不存在则创建）两人的直聊线程（GetDirectThread）；topic 只在首次创建时生效 */
    public func directThread(a: String, b: String, topic!: String = ""): ChatThreadInfo {
        let path = threadPath(a, b)
        var info = ChatThreadInfo("", Array<String>(), "", 0)
        synchronized(lock) {
            match (threads.get(path)) {
                case Some(entry) => info = entry.info
                case None =>
                    let members = ArrayList<String>()
                    if (a < b) {
                        members.add(a)
                        members.add(b)
                    } else {
                        members.add(b)
                        members.add(a)
                    }
                    info = ChatThreadInfo(path, members.toArray(), topic, nowMillis())
                    threads[path] = ChatThreadEntry(info, registry.stream(path))
            }
        }
        info
    }

    /** 线程元信息；未知线程返回 None */
    public func threadInfo(threadId: String): Option<ChatThreadInfo> {
        var out = None<ChatThreadInfo>
        synchronized(lock) {
            match (threads.get(threadId)) {
                case Some(entry) => out = Some<ChatThreadInfo>(entry.info)
                case None => ()
            }
        }
        out
    }

    private func entryOf(threadId: String): ChatThreadEntry {
        var out = None<ChatThreadEntry>
        synchronized(lock) {
            out = threads.get(threadId)
        }
        match (out) {
            case Some(entry) => entry
            case None => throw IllegalArgumentException("thread not found: ${threadId}")
        }
    }

    private func requireMember(entry: ChatThreadEntry, playerId: String): Unit {
        if (!isMember(entry.info, playerId)) {
            throw IllegalArgumentException("player not in thread: ${playerId}")
        }
    }

    private func encodePayload(senderId: String, text: String, sentAtMillis: Int64): String {
        let obj = JsonObject()
        obj.put("sender", JsonString(senderId))
        obj.put("text", JsonString(text))
        obj.put("sentAt", JsonInt(sentAtMillis))
        obj.toString()
    }

    private func decodeMessage(threadId: String, offset: Int64, payload: String): ChatMessage {
        var sender = "unknown"
        var text = ""
        var sentAt: Int64 = 0
        try {
            let obj = JsonValue.fromStr(payload).asObject()
            match (obj.get("sender")) {
                case Some(v: JsonString) => sender = v.getValue()
                case _ => ()
            }
            match (obj.get("text")) {
                case Some(v: JsonString) => text = v.getValue()
                case _ => ()
            }
            match (obj.get("sentAt")) {
                case Some(v: JsonInt) => sentAt = v.getValue()
                case _ => ()
            }
        } catch (e: Exception) {
            // 载荷不是本服务写的：保留原文，sender 标记 unknown，不丢消息
            text = payload
        }
        ChatMessage(threadId, sender, text, sentAt, offset)
    }

    /** 发送消息（SendChatMessage）：返回带 offset 的消息；同时清除发送者的"输入中" */
    public func send(threadId: String, senderId: String, text: String): ChatMessage {
        if (text.isEmpty()) {
            throw IllegalArgumentException("消息内容不能为空")
        }
        let entry = entryOf(threadId)
        requireMember(entry, senderId)
        let sentAt = nowMillis()
        let offset = entry.stream.append(encodePayload(senderId, text, sentAt), producerId: senderId)
        synchronized(lock) {
            match (typingStates.get(threadId)) {
                case Some(m) => m.remove(senderId)
                case None => ()
            }
        }
        ChatMessage(threadId, senderId, text, sentAt, offset)
    }

    /** 读历史（GetThreadHistory）：按 offset 游标分页，nextOffset 用于续读 */
    public func history(threadId: String, fromOffset!: Int64 = 0, maxRows!: Int64 = 50): ChatHistory {
        let entry = entryOf(threadId)
        let read = entry.stream.read(fromOffset: fromOffset, maxRows: maxRows)
        let messages = ArrayList<ChatMessage>()
        for (r in read.records) {
            messages.add(decodeMessage(threadId, r.offset, r.payload))
        }
        ChatHistory(threadId, messages.toArray(), read.nextOffset)
    }

    /**
     * 等待新消息（WatchThread）：没有新消息时最多等 timeout，期间到达立即返回。
     * 长轮询期间不持有服务锁，不影响其他线程发送。
     */
    public func watch(threadId: String, fromOffset!: Int64 = 0,
        timeout!: Duration = Duration.second * 2, maxRows!: Int64 = 50): ChatHistory {
        let entry = entryOf(threadId)
        let read = entry.stream.readLongPoll(fromOffset: fromOffset, timeout: timeout, maxRows: maxRows)
        let messages = ArrayList<ChatMessage>()
        for (r in read.records) {
            messages.add(decodeMessage(threadId, r.offset, r.payload))
        }
        ChatHistory(threadId, messages.toArray(), read.nextOffset)
    }

    /** 线程主题（GetThreadTopic） */
    public func topic(threadId: String): String {
        entryOf(threadId).info.topic
    }

    /** 设置输入中状态（SetTypingStatus）；typing=false 表示停止输入 */
    public func setTyping(threadId: String, playerId: String, typing!: Bool = true): ChatTypingState {
        let entry = entryOf(threadId)
        requireMember(entry, playerId)
        let state = ChatTypingState(playerId, typing, nowMillis())
        synchronized(lock) {
            match (typingStates.get(threadId)) {
                case Some(m) => m[playerId] = state
                case None =>
                    let m = HashMap<String, ChatTypingState>()
                    m[playerId] = state
                    typingStates[threadId] = m
            }
        }
        state
    }

    /** 当前仍在输入中的成员（typing=true 且未超过 TTL） */
    public func typingStatus(threadId: String): Array<ChatTypingState> {
        let now = nowMillis()
        let out = ArrayList<ChatTypingState>()
        synchronized(lock) {
            match (typingStates.get(threadId)) {
                case Some(m) =>
                    for ((_, s) in m) {
                        if (s.typing && now - s.updatedAtMillis <= typingTtlMillis) {
                            out.add(s)
                        }
                    }
                case None => ()
            }
        }
        out.toArray()
    }

    /**
     * 记录已读位置（SetThreadRead）。offset < 0 表示"读到最新"；
     * 超出当前末尾的 offset 会被夹到最新一条，避免客户端声称读到不存在的消息。
     */
    public func markRead(threadId: String, playerId: String, offset!: Int64 = -1): ChatReadState {
        let entry = entryOf(threadId)
        requireMember(entry, playerId)
        let latest = entry.stream.nextOffset() - 1
        let target = if (offset < 0 || offset > latest) { latest } else { offset }
        let state = ChatReadState(threadId, playerId, target, nowMillis())
        synchronized(lock) {
            match (reads.get(threadId)) {
                case Some(m) => m[playerId] = state
                case None =>
                    let m = HashMap<String, ChatReadState>()
                    m[playerId] = state
                    reads[threadId] = m
            }
        }
        state
    }

    /** 读取已读位置；从未标记时返回 lastReadOffset=0、updatedAtMillis=0 */
    public func readState(threadId: String, playerId: String): ChatReadState {
        let entry = entryOf(threadId)
        requireMember(entry, playerId)
        var out = ChatReadState(threadId, playerId, 0, 0)
        synchronized(lock) {
            match (reads.get(threadId)) {
                case Some(m) =>
                    match (m.get(playerId)) {
                        case Some(s) => out = s
                        case None => ()
                    }
                case None => ()
            }
        }
        out
    }
}
