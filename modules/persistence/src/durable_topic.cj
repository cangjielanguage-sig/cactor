/*
 * CActor Persistence.DurableTopic - 持久化主题：发布后订阅者离线也不丢消息（差距 4.8 的 Durable Subscriber）
 *
 * 与 `DistributedPubSub` 的区别：后者是内存扇出，订阅者离线期间的消息直接丢弃；
 * 这里把每条消息按序号写入 `DurableStateStore`，订阅者用持久游标续读：
 * - `publish` 返回全局递增序号（同一主题内单调）；
 * - `subscribe(name, offsets, handler)` 先补齐游标之后的历史，再接实时投递；
 *   handler 成功返回后才推进游标，崩溃/异常时下次重连会重放（至少一次）；
 * - `retention` 只保留最近 N 条，旧消息按序号删除（游标落后太多时会出现缺口并被明确跳过）。
 *
 * 不承担跨节点复制：存储提供者负责持久性与一致性（例如共享目录/数据库），
 * 主题本身只是"日志 + 游标"的语义层。
 */
package cactor_persistence

import std.collection.*
import std.convert.*
import std.sync.*
import cactor_foundation.hashing.{toHex}

/** 持久订阅句柄：`unsubscribe` 只停止实时投递，已保存的游标保留 */
public class DurableSubscription {
    private let topic: DurableTopic
    private let name: String
    private let offsets: DurableStateStore
    let handler: (Int64, String) -> Unit
    private var active = true

    init(topic: DurableTopic, name: String, offsets: DurableStateStore, handler: (Int64, String) -> Unit) {
        this.topic = topic
        this.name = name
        this.offsets = offsets
        this.handler = handler
    }

    /** 业务处理成功后推进游标 */
    func mark(seq: Int64): Unit {
        topic.saveOffset(name, offsets, seq)
    }

    public prop subscriptionName: String {
        get() { name }
    }

    public func unsubscribe(): Unit {
        if (active) {
            active = false
            topic.removeSubscription(name)
        }
    }

    public func isActive(): Bool {
        active
    }
}

public class DurableTopic {
    private let topic: String
    private let store: DurableStateStore
    private let retention: Int64
    private let lock = Mutex()
    // 主题名可能含任意字符：键里用十六进制，避免与分隔符冲突
    private let prefix: String
    private let metaKey: String
    private let subscriptions = HashMap<String, DurableSubscription>()

    /**
     * @param retention > 0 时只保留最近 N 条消息（发布时按序号删除更早的）
     */
    public init(topic: String, store: DurableStateStore, retention!: Int64 = 0) {
        if (topic.isEmpty()) {
            throw IllegalArgumentException("主题名不能为空")
        }
        if (retention < 0) {
            throw IllegalArgumentException("保留条数不能为负")
        }
        this.topic = topic
        this.store = store
        this.retention = retention
        this.prefix = "topic-msg-${toHex(topic.toArray())}-"
        this.metaKey = "topic-meta-${toHex(topic.toArray())}"
    }

    public prop topicName: String {
        get() { topic }
    }

    /** 发布一条消息，返回其在主题内的序号（从 1 开始单调递增） */
    public func publish(payload: String): Int64 {
        synchronized(lock) {
            let seq = currentSeq() + 1
            store.upsert(metaKey, store.revisionOf(metaKey) + 1, "${seq}")
            store.upsert(messageKey(seq), 1, payload)
            prune(seq)
            for ((_, sub) in subscriptions) {
                deliver(sub, seq, payload)
            }
            seq
        }
    }

    /** 从 fromSeq（含）开始的历史消息，按序号升序 */
    public func history(fromSeq: Int64): Array<(Int64, String)> {
        synchronized(lock) {
            let seqs = retainedSequences()
            let out = ArrayList<(Int64, String)>()
            for (seq in seqs) {
                if (seq >= fromSeq) {
                    match (store.get(messageKey(seq))) {
                        case Some(r) => out.add((seq, r.state))
                        case None => ()
                    }
                }
            }
            out.toArray()
        }
    }

    /**
     * 只回放历史（不注册实时投递）：把游标之后的消息按序号交给 handler，成功一条推进一条。
     * 供"在线扇出 + 持久补齐"组合使用（例如 DistributedPubSub）：回放与随后的在线订阅之间
     * 可能有重叠，业务应按序号/业务键幂等。返回最后一条成功处理的序号（没有则为起始游标）。
     */
    public func replay(name: String, offsets: DurableStateStore, handler: (Int64, String) -> Unit,
        fromSeq!: Int64 = 0): Int64 {
        if (name.isEmpty()) {
            throw IllegalArgumentException("订阅者名称不能为空")
        }
        synchronized(lock) {
            let start = if (fromSeq > 0) { fromSeq } else { offsetOf(name, offsets) + 1 }
            var last = start - 1
            for ((seq, payload) in history(start)) {
                try {
                    handler(seq, payload)
                    saveOffset(name, offsets, seq)
                    last = seq
                } catch (_: Exception) {
                    break
                }
            }
            last
        }
    }

    /**
     * 持久订阅：fromSeq > 0 时从该序号开始，否则从已保存游标的下一条开始（首次为 1）。
     * 先补齐历史再接实时；handler 抛异常时该条不推进游标，断开后重连会重放。
     */
    public func subscribe(name: String, offsets: DurableStateStore, handler: (Int64, String) -> Unit,
        fromSeq!: Int64 = 0): DurableSubscription {
        if (name.isEmpty()) {
            throw IllegalArgumentException("订阅者名称不能为空")
        }
        synchronized(lock) {
            if (subscriptions.contains(name)) {
                throw IllegalStateException("订阅者 ${name} 已在主题 ${topic} 上")
            }
            let start = if (fromSeq > 0) { fromSeq } else { offsetOf(name, offsets) + 1 }
            // 回放期间持有锁：与并发的 publish 串行，避免历史与实时之间出现缺口/重复
            for ((seq, payload) in history(start)) {
                try {
                    handler(seq, payload)
                    saveOffset(name, offsets, seq)
                } catch (_: Exception) {
                    // 回放中处理失败：停在该条之前，仍注册实时订阅（故障恢复后重连重放）
                    break
                }
            }
            let sub = DurableSubscription(this, name, offsets, handler)
            subscriptions[name] = sub
            sub
        }
    }

    /** 已保存的游标（0 = 从未投递过） */
    public func offsetOf(name: String, offsets: DurableStateStore): Int64 {
        match (offsets.get(offsetKey(name))) {
            case Some(r) => match (Int64.tryParse(r.state)) {
                case Some(v) => v
                case None => 0
            }
            case None => 0
        }
    }

    /** 当前最高序号（0 = 空主题） */
    public func highestSequenceNr(): Int64 {
        synchronized(lock) {
            currentSeq()
        }
    }

    func removeSubscription(name: String): Unit {
        synchronized(lock) {
            subscriptions.remove(name)
        }
    }

    private func deliver(sub: DurableSubscription, seq: Int64, payload: String): Unit {
        // 单一订阅者的处理失败不应影响其他订阅者；游标不推进，重连后重放
        try {
            sub.handler(seq, payload)
            sub.mark(seq)
        } catch (_: Exception) {
            ()
        }
    }

    private func currentSeq(): Int64 {
        match (store.get(metaKey)) {
            case Some(r) => match (Int64.tryParse(r.state)) {
                case Some(v) => v
                case None => 0
            }
            case None => 0
        }
    }

    private func messageKey(seq: Int64): String {
        "${prefix}${seq}"
    }

    private func offsetKey(name: String): String {
        "topic-offset-${toHex(topic.toArray())}-${toHex(name.toArray())}"
    }

    func saveOffset(name: String, offsets: DurableStateStore, seq: Int64): Unit {
        let key = offsetKey(name)
        offsets.upsert(key, offsets.revisionOf(key) + 1, "${seq}")
    }

    /** 现存消息序号（升序；被保留策略删除的会出现缺口） */
    private func retainedSequences(): Array<Int64> {
        let seqs = ArrayList<Int64>()
        for (id in store.persistenceIds()) {
            if (id.startsWith(prefix)) {
                let tail = id[prefix.size..]
                match (Int64.tryParse(tail)) {
                    case Some(seq) => seqs.add(seq)
                    case None => ()
                }
            }
        }
        // 插入排序：序号数量受 retention 约束，且避免额外依赖 std.sort
        for (i in 1..seqs.size) {
            let v = seqs[i]
            var j = i - 1
            while (j >= 0 && seqs[j] > v) {
                seqs[j + 1] = seqs[j]
                j--
            }
            seqs[j + 1] = v
        }
        seqs.toArray()
    }

    private func prune(seq: Int64): Unit {
        if (retention <= 0 || seq <= retention) {
            return
        }
        let old = seq - retention
        let key = messageKey(old)
        let revision = store.revisionOf(key)
        if (revision > 0) {
            store.delete(key, revision)
        }
    }
}
