/*
 * CActor Platform.KV - 共享键值（F6 / AR9）
 *
 * 两种实现共用 `KvStore` 接口：
 *   - `DDataKvStore`：值存 LWWMap（最后写入者胜，按 (时间戳, 节点) 决胜负），计数存 PNCounter；
 *     本地读写与既有 DData 一致，`getMajority` 走 `ReadConsistency.ReadMajority`，
 *     拿不到足够副本时明确抛 `KvReadConsistencyException`（不返回可能过期的旧值冒充成功）；
 *   - `MemoryKvStore`：单节点进程内实现，便于测试与不需要跨节点的场景。
 *
 * 语义边界：DData 是多主最终一致，不提供线性一致；需要强一致时实现同一接口接外部 KV。
 * LWW 的写时间戳取本机毫秒，跨节点同一毫秒写入由节点 id 决胜负（与 CRDT 内部一致）。
 */
package cactor_platform.kv

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import cactor_cluster.ddata.*

/** 读多数超时/副本不足：读到失败必须明确，不能拿本地旧值当成功 */
public class KvReadConsistencyException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** 共享键值接口（生产接外部 KV 时实现同一接口即可） */
public interface KvStore {
    func get(key: String): Option<String>
    func getMajority(key: String, timeout!: Duration): Option<String>
    func put(key: String, value: String): Unit
    func delete(key: String): Bool
    func keys(): Array<String>
    func list(prefix: String): Array<String>
    /** 原子加：返回加完之后的值（跨节点为 PNCounter 合并值） */
    func incr(key: String, delta: Int64): Int64
}

func kvNowMillis(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}

/**
 * 基于集群 DData 的共享 KV。
 *
 * @param replicator 本节点的 Replicator（与集群其他节点共用同一个 DDoc key 即可互见）
 * @param namespace DDoc 命名空间；不同业务用不同 namespace 互不干扰
 */
public class DDataKvStore <: KvStore {
    private let replicator: Replicator
    private let namespace: String

    public init(replicator: Replicator, namespace!: String = "kv") {
        if (namespace.isEmpty() || namespace.contains("|")) {
            throw IllegalArgumentException("KV namespace 不能为空或包含 '|'")
        }
        this.replicator = replicator
        this.namespace = namespace
    }

    private func mapKey(): String {
        "kv-map/${namespace}"
    }

    private func counterKey(key: String): String {
        "kv-counter/${namespace}/${key}"
    }

    public func get(key: String): Option<String> {
        replicator.lwwMapGet(mapKey(), key)
    }

    public func getMajority(key: String, timeout!: Duration): Option<String> {
        let merged = try {
            replicator.getWith(mapKey(), ReadConsistency.ReadMajority(timeout)).get(timeout)
        } catch (e: Exception) {
            throw KvReadConsistencyException("KV 读多数失败（${timeout.toMilliseconds()}ms）：${e.message}")
        }
        match (merged) {
            case Some(LWWMapValue(m)) => m.get(key)
            case _ => None<String>
        }
    }

    public func put(key: String, value: String): Unit {
        replicator.putInLWWMap(mapKey(), key, value, kvNowMillis())
    }

    public func delete(key: String): Bool {
        let existed = get(key).isSome()
        replicator.removeFromLWWMap(mapKey(), key, kvNowMillis())
        existed
    }

    public func keys(): Array<String> {
        match (replicator.get(mapKey())) {
            case Some(LWWMapValue(m)) => m.keys()
            case _ => Array<String>()
        }
    }

    public func list(prefix: String): Array<String> {
        let out = ArrayList<String>()
        for (k in keys()) {
            if (k.startsWith(prefix)) {
                out.add(k)
            }
        }
        out.toArray()
    }

    public func incr(key: String, delta: Int64): Int64 {
        replicator.incrementCounter(counterKey(key), delta)
        replicator.counterValue(counterKey(key))
    }
}

/** 单节点进程内 KV：与 DData 语义尽量一致（LWW 不需要，直接覆盖），并发安全 */
public class MemoryKvStore <: KvStore {
    private let lock = Mutex()
    private let values = HashMap<String, String>()

    public init() {}

    public func get(key: String): Option<String> {
        synchronized(lock) {
            values.get(key)
        }
    }

    public func getMajority(key: String, timeout!: Duration): Option<String> {
        get(key)
    }

    public func put(key: String, value: String): Unit {
        synchronized(lock) {
            values[key] = value
        }
    }

    public func delete(key: String): Bool {
        synchronized(lock) {
            values.remove(key).isSome()
        }
    }

    public func keys(): Array<String> {
        synchronized(lock) {
            let out = ArrayList<String>()
            for ((k, _) in values) {
                out.add(k)
            }
            out.toArray()
        }
    }

    public func list(prefix: String): Array<String> {
        let out = ArrayList<String>()
        for (k in keys()) {
            if (k.startsWith(prefix)) {
                out.add(k)
            }
        }
        out.toArray()
    }

    public func incr(key: String, delta: Int64): Int64 {
        synchronized(lock) {
            let current = match (values.get(key)) {
                case Some(v) => Int64.tryParse(v) ?? 0
                case None => 0
            }
            let next = current + delta
            values[key] = next.toString()
            next
        }
    }
}
