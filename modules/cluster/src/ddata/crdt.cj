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
 */
package cactor_cluster.ddata

import std.collection.*
import std.convert.*

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
            let mine = entries.get(k) ?? HashSet<String>()
            let theirs = other.entries.get(k) ?? HashSet<String>()
            let dots = HashSet<String>()
            for (d in mine) {
                // 双方都有，或对方没见过（对方没删）
                if (theirs.contains(d) || !covered(d, other.vv)) {
                    dots.add(d)
                }
            }
            for (d in theirs) {
                if (mine.contains(d) || !covered(d, vv)) {
                    dots.add(d)
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
