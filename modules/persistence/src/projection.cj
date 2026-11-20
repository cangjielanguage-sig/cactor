/*
 * CActor Persistence - 投影（差距 4.12：把事件流投影成可查询读模型）
 *
 * 对标 Akka Projection 的最小语义：
 * - 输入：`ReadJournal.currentEventsByTag(tag)`（跨 persistenceId，按时间戳/标识/序号排序）；
 * - 处理：对每个 persistenceId 记录**已处理序号**，只把新事件交给 `apply`；
 * - 进度：偏移量写入 `DurableStateStore`（复用现有存储，一处键存全部标识的偏移），重启后从断点续跑；
 * - 幂等：同一段事件重复 `runOnce` 不会重复应用；需要全量重建时 `reset()` 后重放，
 *   由调用方保证读模型可重建（例如先清空读模型）。
 *
 * 不复活已弃用的 `CQRSBus`：这里只做"事件 → 读模型"的驱动与进度，不绑定具体读模型类型。
 */
package cactor_persistence

import std.collection.*
import std.sync.*
import std.convert.*

public class Projection {
    private let name: String
    private let journal: ReadJournal
    private let offsetStore: DurableStateStore
    private let applyFn: (EventEnvelope) -> Unit
    private let offsetKey: String
    private let lock = Mutex()

    public init(name: String, journal: ReadJournal, offsetStore: DurableStateStore,
        apply: (EventEnvelope) -> Unit) {
        if (name.isEmpty()) {
            throw IllegalArgumentException("投影名称不能为空")
        }
        this.name = name
        this.journal = journal
        this.offsetStore = offsetStore
        this.applyFn = apply
        this.offsetKey = "projection-offset-${name}"
    }

    public prop projectionName: String {
        get() { name }
    }

    /**
     * 应用 tag 下尚未处理的事件；返回本次实际应用的条数。
     * 每个 persistenceId 只应用序号大于已记录偏移的事件；没有新事件时不写存储。
     */
    public func runOnce(tag: String): Int64 {
        synchronized(lock) {
            let offsets = loadOffsets()
            var applied: Int64 = 0
            for (env in journal.currentEventsByTag(tag)) {
                let seen = offsets.get(env.persistenceId) ?? 0
                if (env.sequenceNr > seen) {
                    applyFn(env)
                    offsets[env.persistenceId] = env.sequenceNr
                    applied++
                }
            }
            if (applied > 0) {
                saveOffsets(offsets)
            }
            applied
        }
    }

    /** 清除全部偏移（读模型由调用方清空/重建），之后 `runOnce` 会从头重放 */
    public func reset(): Unit {
        synchronized(lock) {
            saveOffsets(HashMap<String, Int64>())
        }
    }

    /** 某持久化标识已处理到的序号（0 表示从未处理） */
    public func offsetOf(persistenceId: String): Int64 {
        synchronized(lock) {
            loadOffsets().get(persistenceId) ?? 0
        }
    }

    private func loadOffsets(): HashMap<String, Int64> {
        let out = HashMap<String, Int64>()
        match (offsetStore.get(offsetKey)) {
            case Some(r) =>
                for (line in r.state.split("\n")) {
                    if (line.isEmpty()) {
                        continue
                    }
                    match (line.indexOf("|")) {
                        case Some(i) =>
                            match (Int64.tryParse(line[0..i])) {
                                case Some(seq) => out[line[(i + 1)..]] = seq
                                case None => ()
                            }
                        case None => ()
                    }
                }
            case None => ()
        }
        out
    }

    private func saveOffsets(offsets: HashMap<String, Int64>): Unit {
        let sb = StringBuilder()
        for ((pid, seq) in offsets) {
            sb.append("${seq}|${pid}\n")
        }
        let revision = offsetStore.revisionOf(offsetKey) + 1
        offsetStore.upsert(offsetKey, revision, sb.toString())
    }
}
