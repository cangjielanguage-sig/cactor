/*
 * CActor Cluster - 无冲突复制数据类型（CRDT，对标 Akka Distributed Data）
 *
 * 所有类型都是不可变值，merge 满足交换律、结合律、幂等律，因此副本之间以任意顺序、
 * 任意次数交换状态后都会收敛到同一个值（状态型 CRDT / CvRDT）。
 *
 * - GCounter：只增计数器
 * - PNCounter：可增可减计数器
 * - GSet：只增集合
 * - ORSet：可增删集合（观察-删除，add 优先）
 * - LWWRegister：最后写入胜出的寄存器
 *
 * 增量（delta-CRDT，E64）：一次本地修改相对修改前的增量。计数器、GSet、寄存器的增量就是同类型的"部分状态"，
 * 与任意副本 merge 即可；ORSet 的增量是"来源节点新增的点 + 删除的点"（ORSetDelta），必须按来源保序应用。
 * 无法表示为增量的修改（例如合并了别的副本、被 pruning 改写）返回 None，由 Replicator 改发整值。
 *
 * pruning（E64，对标 Akka）：被移除节点 removed 的数据由一个节点 collapseInto 并入自己（prune），
 * 之后合并任何副本前先剥掉 removed 的数据（cleanup），计数不会重复、ORSet 元素不丢也不复活。
 * 何时可以 prune 由 Replicator 的两阶段协议保证（所有成员都见过"准备清理"标记之后）。
 */
package cactor_cluster.ddata

import std.collection.*
import std.convert.*
import std.sort.*

/** 可合并的复制数据 */
public interface ReplicatedData<T> {
    func merge(other: T): T
}

// ======================= GCounter =======================

/** 只增计数器：每个节点一个分量，值为各分量之和，合并取各分量最大值 */
public class GCounter <: ReplicatedData<GCounter> & Equatable<GCounter> {
    let state: HashMap<String, Int64>

    public init() {
        this.state = HashMap<String, Int64>()
    }

    init(state: HashMap<String, Int64>) {
        this.state = state
    }

    public func value(): Int64 {
        var sum: Int64 = 0
        for ((_, v) in state) {
            sum += v
        }
        sum
    }

    /** 本节点增加 delta（delta 必须非负） */
    public func increment(node: String, delta: Int64): GCounter {
        if (delta < 0) {
            throw IllegalArgumentException("GCounter 只能增加，delta = ${delta}")
        }
        let next = HashMap<String, Int64>(state)
        next[node] = (state.get(node) ?? 0) + delta
        GCounter(next)
    }

    public func increment(node: String): GCounter {
        increment(node, 1)
    }

    /** 相对 before 的增量：变化了的分量（计数器只增，出现减少或分量消失说明不是普通修改，返回 None） */
    public func deltaFrom(before: GCounter): Option<GCounter> {
        let d = HashMap<String, Int64>()
        for ((k, v) in state) {
            let old = before.state.get(k) ?? 0
            if (v < old) {
                return None
            }
            if (v != old) {
                d[k] = v
            }
        }
        for ((k, _) in before.state) {
            if (!state.contains(k)) {
                return None
            }
        }
        Some(GCounter(d))
    }

    /** 分量个数（pruning 的观测指标） */
    public func componentCount(): Int64 {
        state.size
    }

    public func needsPruning(removed: String): Bool {
        state.contains(removed)
    }

    /** 有分量的节点 */
    public func modifiedByNodes(): Array<String> {
        let out = ArrayList<String>(state.size)
        for ((k, _) in state) {
            out.add(k)
        }
        out.toArray()
    }

    /** removed 的分量加到 collapseInto 的分量上，总值不变 */
    public func prune(removed: String, collapseInto: String): GCounter {
        match (state.get(removed)) {
            case Some(v) =>
                let next = HashMap<String, Int64>(state)
                next.remove(removed)
                next[collapseInto] = (state.get(collapseInto) ?? 0) + v
                GCounter(next)
            case None => this
        }
    }

    /** 去掉 removed 的分量（它已被并入其他分量） */
    public func cleanup(removed: String): GCounter {
        if (!state.contains(removed)) {
            return this
        }
        let next = HashMap<String, Int64>(state)
        next.remove(removed)
        GCounter(next)
    }

    public func merge(other: GCounter): GCounter {
        let next = HashMap<String, Int64>(state)
        for ((k, v) in other.state) {
            let mine = next.get(k) ?? 0
            if (v > mine) {
                next[k] = v
            }
        }
        GCounter(next)
    }

    public operator func ==(other: GCounter): Bool {
        sameEntries(state, other.state)
    }

    public operator func !=(other: GCounter): Bool {
        !(this == other)
    }

    public func toString(): String {
        "GCounter(${value()})"
    }
}

func sameEntries(a: HashMap<String, Int64>, b: HashMap<String, Int64>): Bool {
    // 值为 0 的分量与不存在等价
    for ((k, v) in a) {
        if ((b.get(k) ?? 0) != v) {
            return false
        }
    }
    for ((k, v) in b) {
        if ((a.get(k) ?? 0) != v) {
            return false
        }
    }
    true
}

// ======================= PNCounter =======================

/** 可增可减计数器：一个 GCounter 记增加，一个记减少 */
public class PNCounter <: ReplicatedData<PNCounter> & Equatable<PNCounter> {
    let increments: GCounter
    let decrements: GCounter

    public init() {
        this(GCounter(), GCounter())
    }

    init(increments: GCounter, decrements: GCounter) {
        this.increments = increments
        this.decrements = decrements
    }

    public func value(): Int64 {
        increments.value() - decrements.value()
    }

    public func increment(node: String, delta: Int64): PNCounter {
        if (delta >= 0) {
            PNCounter(increments.increment(node, delta), decrements)
        } else {
            PNCounter(increments, decrements.increment(node, -delta))
        }
    }

    public func decrement(node: String, delta: Int64): PNCounter {
        increment(node, -delta)
    }

    public func deltaFrom(before: PNCounter): Option<PNCounter> {
        match ((increments.deltaFrom(before.increments), decrements.deltaFrom(before.decrements))) {
            case (Some(i), Some(d)) => Some(PNCounter(i, d))
            case _ => None
        }
    }

    public func componentCount(): Int64 {
        increments.componentCount() + decrements.componentCount()
    }

    public func needsPruning(removed: String): Bool {
        increments.needsPruning(removed) || decrements.needsPruning(removed)
    }

    public func modifiedByNodes(): Array<String> {
        let out = HashSet<String>(increments.modifiedByNodes())
        out.add(all: decrements.modifiedByNodes())
        out.toArray()
    }

    public func prune(removed: String, collapseInto: String): PNCounter {
        PNCounter(increments.prune(removed, collapseInto), decrements.prune(removed, collapseInto))
    }

    public func cleanup(removed: String): PNCounter {
        PNCounter(increments.cleanup(removed), decrements.cleanup(removed))
    }

    public func merge(other: PNCounter): PNCounter {
        PNCounter(increments.merge(other.increments), decrements.merge(other.decrements))
    }

    public operator func ==(other: PNCounter): Bool {
        increments == other.increments && decrements == other.decrements
    }

    public operator func !=(other: PNCounter): Bool {
        !(this == other)
    }

    public func toString(): String {
        "PNCounter(${value()})"
    }
}

// ======================= GSet =======================

/** 只增集合：合并取并集 */
public class GSet <: ReplicatedData<GSet> & Equatable<GSet> {
    let elements: HashSet<String>

    public init() {
        this.elements = HashSet<String>()
    }

    init(elements: HashSet<String>) {
        this.elements = elements
    }

    public func add(element: String): GSet {
        let next = HashSet<String>(elements)
        next.add(element)
        GSet(next)
    }

    public func contains(element: String): Bool {
        elements.contains(element)
    }

    public func size(): Int64 {
        elements.size
    }

    public func toArray(): Array<String> {
        elements.toArray()
    }

    /** 相对 before 的增量：新增的元素（GSet 只增，元素消失说明不是普通修改） */
    public func deltaFrom(before: GSet): Option<GSet> {
        for (e in before.elements) {
            if (!elements.contains(e)) {
                return None
            }
        }
        let d = HashSet<String>()
        for (e in elements) {
            if (!before.elements.contains(e)) {
                d.add(e)
            }
        }
        Some(GSet(d))
    }

    public func merge(other: GSet): GSet {
        let next = HashSet<String>(elements)
        next.add(all: other.elements)
        GSet(next)
    }

    public operator func ==(other: GSet): Bool {
        if (elements.size != other.elements.size) {
            return false
        }
        for (e in other.elements) {
            if (!elements.contains(e)) {
                return false
            }
        }
        true
    }

    public operator func !=(other: GSet): Bool {
        !(this == other)
    }
}

// ======================= ORSet =======================

/**
 * 观察-删除集合（Observed-Remove Set）
 *
 * 每次 add 生成唯一的“点”（节点, 序号）；remove 删除当前观察到的全部点。
 * 并发的 add 与 remove：add 产生的新点没有被 remove 观察到，因此元素保留（add 优先）。
 * 为了让 merge 能区分“对方没见过”和“对方删除了”，每个副本维护版本向量（各节点已见过的最大序号）。
 */
public class ORSet <: ReplicatedData<ORSet> & Equatable<ORSet> {
    // 元素 -> 点集合（"node#seq"）
    let entries: HashMap<String, HashSet<String>>
    // 版本向量：节点 -> 已见过的最大序号
    let vv: HashMap<String, Int64>

    public init() {
        this.entries = HashMap<String, HashSet<String>>()
        this.vv = HashMap<String, Int64>()
    }

    init(entries: HashMap<String, HashSet<String>>, vv: HashMap<String, Int64>) {
        this.entries = entries
        this.vv = vv
    }

    private func copyEntries(): HashMap<String, HashSet<String>> {
        let m = HashMap<String, HashSet<String>>()
        for ((k, dots) in entries) {
            m[k] = HashSet<String>(dots)
        }
        m
    }

    public func add(node: String, element: String): ORSet {
        let seq = (vv.get(node) ?? 0) + 1
        let nextVv = HashMap<String, Int64>(vv)
        nextVv[node] = seq
        let next = copyEntries()
        let dots = HashSet<String>()
        // 同一节点重复 add：用新点替换旧点
        dots.add("${node}#${seq}")
        if (let Some(old) <- next.get(element)) {
            for (d in old) {
                if (!d.startsWith("${node}#")) {
                    dots.add(d)
                }
            }
        }
        next[element] = dots
        ORSet(next, nextVv)
    }

    public func remove(element: String): ORSet {
        if (!entries.contains(element)) {
            return this
        }
        let next = copyEntries()
        next.remove(element)
        ORSet(next, HashMap<String, Int64>(vv))
    }

    public func contains(element: String): Bool {
        entries.contains(element)
    }

    public func size(): Int64 {
        entries.size
    }

    public func elements(): Array<String> {
        let result = ArrayList<String>()
        for ((k, _) in entries) {
            result.add(k)
        }
        result.toArray()
    }

    private static func dotNode(dot: String): String {
        match (dot.lastIndexOf("#")) {
            case Some(i) => dot[0..i]
            case None => dot
        }
    }

    private static func dotSeq(dot: String): Int64 {
        match (dot.lastIndexOf("#")) {
            case Some(i) => Int64.parse(dot[(i + 1)..])
            case None => 0
        }
    }

    /** 点 dot 是否已被版本向量 v 覆盖（即对方见过它） */
    private static func covered(dot: String, v: HashMap<String, Int64>): Bool {
        dotSeq(dot) <= (v.get(dotNode(dot)) ?? 0)
    }

    public func merge(other: ORSet): ORSet {
        let next = HashMap<String, HashSet<String>>()
        let keys = HashSet<String>()
        for ((k, _) in entries) {
            keys.add(k)
        }
        for ((k, _) in other.entries) {
            keys.add(k)
        }
        for (k in keys) {
            // 不用 `?? HashSet()` 的空集回退：合并是只读操作，缺键直接按"空集合"处理，
            // 避免为每个缺失键临时分配对象（百万级 key 的 gossip 同步时是真实的分配压力）
            let mine = entries.get(k)
            let theirs = other.entries.get(k)
            let dots = HashSet<String>()
            if (let Some(m) <- mine) {
                for (d in m) {
                    // 双方都有，或对方没见过（对方没删）
                    let seen = match (theirs) {
                        case Some(t) => t.contains(d)
                        case None => false
                    }
                    if (seen || !covered(d, other.vv)) {
                        dots.add(d)
                    }
                }
            }
            if (let Some(t) <- theirs) {
                for (d in t) {
                    let seen = match (mine) {
                        case Some(m) => m.contains(d)
                        case None => false
                    }
                    if (seen || !covered(d, vv)) {
                        dots.add(d)
                    }
                }
            }
            if (!dots.isEmpty()) {
                next[k] = dots
            }
        }
        let nextVv = HashMap<String, Int64>(vv)
        for ((n, s) in other.vv) {
            if (s > (nextVv.get(n) ?? 0)) {
                nextVv[n] = s
            }
        }
        ORSet(next, nextVv)
    }

    /**
     * 相对 before 的增量（来源 origin 的一次本地修改）：新增的点、删除的点、origin 在版本向量中的新值。
     * 新增的点只能是 origin 自己的点、版本向量只能变 origin 一项，否则（合并了其他副本、pruning 改写）返回 None
     */
    public func deltaFrom(before: ORSet, origin: String): Option<ORSetDelta> {
        let added = HashMap<String, HashSet<String>>()
        let removed = HashMap<String, HashSet<String>>()
        for ((e, dots) in entries) {
            let old = before.entries.get(e)
            let fresh = HashSet<String>()
            for (d in dots) {
                let had = match (old) {
                    case Some(o) => o.contains(d)
                    case None => false
                }
                if (!had) {
                    if (dotNode(d) != origin) {
                        return None
                    }
                    fresh.add(d)
                }
            }
            if (!fresh.isEmpty()) {
                added[e] = fresh
            }
        }
        for ((e, dots) in before.entries) {
            let now = entries.get(e)
            let gone = HashSet<String>()
            for (d in dots) {
                let has = match (now) {
                    case Some(n) => n.contains(d)
                    case None => false
                }
                if (!has) {
                    gone.add(d)
                }
            }
            if (!gone.isEmpty()) {
                removed[e] = gone
            }
        }
        for ((n, v) in vv) {
            if (n != origin && (before.vv.get(n) ?? 0) != v) {
                return None
            }
        }
        for ((n, _) in before.vv) {
            if (!vv.contains(n)) {
                return None
            }
        }
        Some(ORSetDelta(origin, vv.get(origin) ?? 0, added, removed))
    }

    /**
     * 应用来源节点的增量。前提：本副本已应用该来源此前的全部增量（Replicator 按序号保证），
     * 所以把版本向量中来源的一项前移到 originSeq 是真实的（不会把没见过的点当成已删除）。
     * - 先删后加：同一节点重复 add 时，旧点在 removed、新点在 added；
     * - 删除的点本副本还没见过（来自第三个节点、尚未送达）：无从删除，之后送达会短暂出现，
     *   与删除方的整值合并时（对方版本向量覆盖它而又没有它）即被去掉——最终一致；
     * - 新增的点已被版本向量覆盖而本副本没有：说明已见过并删除了（迟到的增量），跳过
     */
    public func applyDelta(d: ORSetDelta): ORSet {
        let next = copyEntries()
        for ((e, dots) in d.removed) {
            if (let Some(cur) <- next.get(e)) {
                for (x in dots) {
                    cur.remove(x)
                }
                if (cur.isEmpty()) {
                    next.remove(e)
                }
            }
        }
        for ((e, dots) in d.added) {
            for (x in dots) {
                let has = match (next.get(e)) {
                    case Some(cur) => cur.contains(x)
                    case None => false
                }
                if (!has && covered(x, vv)) {
                    continue
                }
                match (next.get(e)) {
                    case Some(cur) => cur.add(x)
                    case None =>
                        let fresh = HashSet<String>()
                        fresh.add(x)
                        next[e] = fresh
                }
            }
        }
        let nextVv = HashMap<String, Int64>(vv)
        if (d.originSeq > (nextVv.get(d.origin) ?? 0)) {
            nextVv[d.origin] = d.originSeq
        }
        ORSet(next, nextVv)
    }

    /** 版本向量的条目数（pruning 的观测指标） */
    public func componentCount(): Int64 {
        vv.size
    }

    /** 版本向量里的节点与点的节点（点总被版本向量覆盖，这里兼顾合并中途的不完整状态） */
    public func modifiedByNodes(): Array<String> {
        let out = HashSet<String>()
        for ((n, _) in vv) {
            out.add(n)
        }
        for ((_, dots) in entries) {
            for (d in dots) {
                out.add(dotNode(d))
            }
        }
        out.toArray()
    }

    public func needsPruning(removed: String): Bool {
        if (vv.contains(removed)) {
            return true
        }
        for ((_, dots) in entries) {
            for (d in dots) {
                if (dotNode(d) == removed) {
                    return true
                }
            }
        }
        false
    }

    /**
     * removed 的点换成 collapseInto 的新点（每个元素一个，相当于 collapseInto 重新 add，同 Akka 的 VersionVector.prune），
     * 删掉版本向量中 removed 一项：元素一个不丢，之后由 collapseInto 的点代表
     */
    public func prune(removed: String, collapseInto: String): ORSet {
        var seq = vv.get(collapseInto) ?? 0
        let next = copyEntries()
        for ((_, dots) in next) {
            var had = false
            var hasOwn = false
            for (d in dots.toArray()) {
                let n = dotNode(d)
                if (n == removed) {
                    dots.remove(d)
                    had = true
                } else if (n == collapseInto) {
                    hasOwn = true
                }
            }
            // 已有 collapseInto 的点就由它代表，不必再加
            if (had && !hasOwn) {
                seq++
                dots.add("${collapseInto}#${seq}")
            }
        }
        let nextVv = HashMap<String, Int64>(vv)
        nextVv.remove(removed)
        if (seq > 0) {
            nextVv[collapseInto] = seq
        }
        ORSet(next, nextVv)
    }

    /** 去掉 removed 的点与版本向量条目（它们已由 pruning 节点的新点代表）；只剩 removed 的点的元素随之消失 */
    public func cleanup(removed: String): ORSet {
        if (!needsPruning(removed)) {
            return this
        }
        let next = HashMap<String, HashSet<String>>()
        for ((e, dots) in entries) {
            let keep = HashSet<String>()
            for (d in dots) {
                if (dotNode(d) != removed) {
                    keep.add(d)
                }
            }
            if (!keep.isEmpty()) {
                next[e] = keep
            }
        }
        let nextVv = HashMap<String, Int64>(vv)
        nextVv.remove(removed)
        ORSet(next, nextVv)
    }

    /** 元素集合相同即相等 */
    public operator func ==(other: ORSet): Bool {
        if (entries.size != other.entries.size) {
            return false
        }
        for ((k, _) in entries) {
            if (!other.entries.contains(k)) {
                return false
            }
        }
        true
    }

    public operator func !=(other: ORSet): Bool {
        !(this == other)
    }
}

/** ORSet 的增量：来源节点一次本地修改新增的点与删除的点（元素 -> 点集合），及来源在版本向量中的新值 */
public class ORSetDelta {
    public let origin: String
    public let originSeq: Int64
    let added: HashMap<String, HashSet<String>>
    let removed: HashMap<String, HashSet<String>>

    init(origin: String, originSeq: Int64, added: HashMap<String, HashSet<String>>,
        removed: HashMap<String, HashSet<String>>) {
        this.origin = origin
        this.originSeq = originSeq
        this.added = added
        this.removed = removed
    }

    public func isEmpty(): Bool {
        added.isEmpty() && removed.isEmpty()
    }
}

// ======================= LWWRegister =======================

/**
 * 最后写入胜出的寄存器：时间戳大的胜出，时间戳相同时按节点名决定，保证确定性
 */
public class LWWRegister<T> <: ReplicatedData<LWWRegister<T>> {
    public let value: T
    public let timestamp: Int64
    public let node: String

    public init(value: T, timestamp: Int64, node: String) {
        this.value = value
        this.timestamp = timestamp
        this.node = node
    }

    /** 写入新值；时间戳至少比当前大 1，保证同一副本上的写入有序 */
    public func set(value: T, timestamp: Int64, node: String): LWWRegister<T> {
        let ts = if (timestamp > this.timestamp) { timestamp } else { this.timestamp + 1 }
        LWWRegister<T>(value, ts, node)
    }

    public func merge(other: LWWRegister<T>): LWWRegister<T> {
        if (other.timestamp > timestamp || (other.timestamp == timestamp && other.node > node)) {
            other
        } else {
            this
        }
    }
}

// ======================= ORMap（对标 Akka ORMap：观察-删除键值映射） =======================

/**
 * 可增删键值映射，表示为"键值对集合"：每个 (key, value) 是 [ORSet] 的一个元素（长度前缀编码）。
 *
 * 为什么不是"每个 key 一个 ORSet"：一侧删掉整个 key 后，该 key 在它那里不存在；合并时
 * 仅凭"对方没有这个 key"无法区分"对方没见过"与"对方删了"（缺 key 的一侧没有版本向量可依），
 * 会复活已删除的值。把 (key,value) 都放进**同一个** ORSet，删除就是删点，
 * ORSet 的版本向量天然表达"我见过并删除了"，add-wins 语义也随之继承。
 *
 * 同一 key 下并发 put 不同值：两个对都保留（add-wins）；并发 put 与删除同一个值：add 胜出。
 * 不表示增量（deltaFrom 返回 None），由 Replicator 改发整值。
 */
public class ORMap <: ReplicatedData<ORMap> & Equatable<ORMap> {
    let pairs: ORSet

    public init() {
        this.pairs = ORSet()
    }

    init(pairs: ORSet) {
        this.pairs = pairs
    }

    /** 把 (key, value) 编码成一个元素："<key 字节数>:<key><value>"，长度前缀避免分隔符冲突 */
    static func encodePair(key: String, value: String): String {
        "${key.size}:${key}${value}"
    }

    static func decodePair(pair: String): Option<(String, String)> {
        match (pair.indexOf(":")) {
            case Some(i) =>
                match (Int64.tryParse(pair[0..i])) {
                    case Some(len) =>
                        let start = i + 1
                        if (len < 0 || start + len > pair.size) {
                            None<(String, String)>
                        } else {
                            Some((pair[start..start + len], pair[(start + len)..]))
                        }
                    case None => None<(String, String)>
                }
            case None => None<(String, String)>
        }
    }

    /** 在 key 下放入 value（add-wins：并发 put 的不同值都会保留） */
    public func put(node: String, key: String, value: String): ORMap {
        ORMap(pairs.add(node, encodePair(key, value)))
    }

    /** 移除 key 下的某个值 */
    public func removeValue(key: String, value: String): ORMap {
        ORMap(pairs.remove(encodePair(key, value)))
    }

    /** 移除整个键（移除该键当前的全部值；并发的 put 仍是 add-wins） */
    public func removeKey(key: String): ORMap {
        var next = pairs
        for (p in pairs.elements()) {
            match (decodePair(p)) {
                case Some((k, _)) where k == key => next = next.remove(p)
                case _ => ()
            }
        }
        ORMap(next)
    }

    /** key 下的全部值（排序后返回） */
    public func values(key: String): Array<String> {
        let out = ArrayList<String>()
        for (p in pairs.elements()) {
            match (decodePair(p)) {
                case Some((k, v)) where k == key => out.add(v)
                case _ => ()
            }
        }
        let arr = out.toArray()
        sortStrings(arr)
        arr
    }

    public func containsKey(key: String): Bool {
        for (p in pairs.elements()) {
            match (decodePair(p)) {
                case Some((k, _)) where k == key => return true
                case _ => ()
            }
        }
        false
    }

    /** 键列表（去重、排序） */
    public func keys(): Array<String> {
        let set = HashSet<String>()
        for (p in pairs.elements()) {
            match (decodePair(p)) {
                case Some((k, _)) => set.add(k)
                case None => ()
            }
        }
        let arr = ArrayList<String>(set).toArray()
        sortStrings(arr)
        arr
    }

    public func size(): Int64 {
        keys().size
    }

    public func merge(other: ORMap): ORMap {
        ORMap(pairs.merge(other.pairs))
    }

    public func componentCount(): Int64 {
        pairs.componentCount()
    }

    public func needsPruning(removed: String): Bool {
        pairs.needsPruning(removed)
    }

    public func modifiedByNodes(): Array<String> {
        pairs.modifiedByNodes()
    }

    public func prune(removed: String, collapseInto: String): ORMap {
        ORMap(pairs.prune(removed, collapseInto))
    }

    public func cleanup(removed: String): ORMap {
        ORMap(pairs.cleanup(removed))
    }

    public operator func ==(other: ORMap): Bool {
        pairs == other.pairs
    }
}

// ======================= LWWMap（每个键一个最后写入胜出的寄存器） =======================

/** 删除墓碑：写入它是删除，读取时视为不存在；墓碑本身参与合并，防止已删键被旧副本复活 */
let LWW_MAP_TOMBSTONE: String = "\u{0001}"

/**
 * 最后写入胜出的映射：每个 key 独立的 [LWWRegister]，按 key 合并。
 * 删除用墓碑表示（[remove]），墓碑与普通值一样按时间戳/节点决胜，因此合并后删除不会回退。
 */
public class LWWMap <: ReplicatedData<LWWMap> & Equatable<LWWMap> {
    // 键 -> 寄存器（值可能是墓碑）
    let entries: HashMap<String, LWWRegister<String>>

    public init() {
        this.entries = HashMap<String, LWWRegister<String>>()
    }

    init(entries: HashMap<String, LWWRegister<String>>) {
        this.entries = entries
    }

    /** 写入 key；同一副本上重复写入时时间戳至少 +1（与 LWWRegister.set 一致） */
    public func put(key: String, value: String, timestamp: Int64, node: String): LWWMap {
        let next = HashMap<String, LWWRegister<String>>(entries)
        let reg = match (entries.get(key)) {
            case Some(r) => r.set(value, timestamp, node)
            case None => LWWRegister<String>(value, timestamp, node)
        }
        next[key] = reg
        LWWMap(next)
    }

    /** 删除 key（写入墓碑）；墓碑的删除时间戳参与合并，旧副本的值不会复活 */
    public func remove(key: String, timestamp: Int64, node: String): LWWMap {
        put(key, LWW_MAP_TOMBSTONE, timestamp, node)
    }

    public func get(key: String): Option<String> {
        match (entries.get(key)) {
            case Some(r) =>
                if (r.value == LWW_MAP_TOMBSTONE) {
                    None<String>
                } else {
                    Some(r.value)
                }
            case None => None<String>
        }
    }

    public func containsKey(key: String): Bool {
        get(key).isSome()
    }

    /** 键列表（不含墓碑；排序后返回） */
    public func keys(): Array<String> {
        let out = ArrayList<String>()
        for ((k, r) in entries) {
            if (r.value != LWW_MAP_TOMBSTONE) {
                out.add(k)
            }
        }
        let arr = out.toArray()
        sortStrings(arr)
        arr
    }

    /** 存活的键值对数（不含墓碑） */
    public func size(): Int64 {
        keys().size
    }

    public func merge(other: LWWMap): LWWMap {
        let next = HashMap<String, LWWRegister<String>>()
        let keys = HashSet<String>()
        for ((k, _) in entries) {
            keys.add(k)
        }
        for ((k, _) in other.entries) {
            keys.add(k)
        }
        for (k in keys) {
            let mine = entries.get(k)
            let theirs = other.entries.get(k)
            if (let Some(a) <- mine) {
                next[k] = match (theirs) {
                    case Some(b) => a.merge(b)
                    case None => a
                }
            } else if (let Some(b) <- theirs) {
                next[k] = b
            }
        }
        LWWMap(next)
    }

    public func componentCount(): Int64 {
        0
    }

    public func needsPruning(removed: String): Bool {
        false
    }

    public func modifiedByNodes(): Array<String> {
        Array<String>()
    }

    public func prune(removed: String, collapseInto: String): LWWMap {
        this
    }

    public func cleanup(removed: String): LWWMap {
        this
    }

    public operator func ==(other: LWWMap): Bool {
        let a = keys()
        let b = other.keys()
        if (a.size != b.size) {
            return false
        }
        for (i in 0..a.size) {
            if (a[i] != b[i]) {
                return false
            }
            let ra = entries[a[i]]
            let rb = other.entries[b[i]]
            if (ra.value != rb.value || ra.timestamp != rb.timestamp || ra.node != rb.node) {
                return false
            }
        }
        true
    }
}

/** 排序字符串数组（CRDT 内部用于稳定输出/编码） */
func sortStrings(arr: Array<String>): Unit {
    sort(arr)
}
