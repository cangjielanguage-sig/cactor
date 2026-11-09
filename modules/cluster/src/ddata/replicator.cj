/*
 * CActor Cluster - Replicator：在节点间同步 CRDT（对标 Akka Distributed Data 的 Replicator）
 *
 * 一致性模型（与 Akka 的 WriteLocal + gossip 相同）：
 * - 更新只在本地执行并立即生效（不等待其他节点），随后把该键的增量推送给所有对等节点（delta 传播，E64）：
 *   每个来源节点对每个键的增量带递增序号，接收方只按序应用；发现断档（连接重建时丢了消息）就向来源要整值，
 *   不会把 ORSet 的增量用错（版本向量只能在连续见过来源的全部点后前移）。无法表示为增量的修改改发整值，同样带序号；
 * - 每隔 gossipInterval 再把全部键的状态发给所有对等节点（反熵），丢失的推送由下一轮补上；
 * - 收到状态后与本地值 merge。CRDT 的 merge 满足交换律、结合律、幂等律，
 *   因此只要网络最终连通，所有节点最终收敛到同一个值，与消息顺序、重复、丢失无关。
 *
 * 传输复用 cactor_remote：每个节点把一个 Replicator Actor 以名称 "replicator" 暴露出去，
 * 状态编码为带长度前缀的文本放在 StringMessage 中。
 */
package cactor_cluster.ddata

import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import std.sort.*
import std.random.*
import cactor.api.{CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress}
import cactor_foundation.logging.{Logger, Logging}
import cactor_remote.{SimpleRemoteManager}
import cactor_core.typed.{TypedPromise}

// ======================= 文本编码 =======================

/** 带长度前缀的字段编码："<字节数>:<内容>"，内容可以包含任意字符（ddata 与集群成员协议共用） */
public class FieldWriter {
    let sb = StringBuilder()

    public init() {}

    public func put(s: String): FieldWriter {
        sb.append(s.size)
        sb.append(":")
        sb.append(s)
        this
    }

    public func put(n: Int64): FieldWriter {
        put(n.toString())
    }

    public func done(): String {
        sb.toString()
    }
}

/** FieldWriter 的读取端；格式错误时抛出 IllegalArgumentException */
public class FieldReader {
    let s: String
    var pos: Int64 = 0

    public init(s: String) {
        this.s = s
    }

    public func hasMore(): Bool {
        pos < s.size
    }

    public func next(): String {
        var i = pos
        while (i < s.size && s[i] != UInt8(UInt32(r':'))) {
            i++
        }
        if (i >= s.size) {
            throw IllegalArgumentException("编码损坏：缺少长度分隔符")
        }
        let len = Int64.tryParse(s[pos..i]) ?? -1
        let start = i + 1
        if (len < 0 || start + len > s.size) {
            throw IllegalArgumentException("编码损坏：长度越界")
        }
        pos = start + len
        s[start..pos]
    }

    public func nextInt(): Int64 {
        match (Int64.tryParse(next())) {
            case Some(n) => n
            case None => throw IllegalArgumentException("编码损坏：不是整数")
        }
    }
}

// ======================= 可复制的值 =======================

/** Replicator 中保存的值：五种 CRDT 之一 */
public enum ReplicatedValue {
    | Counter(GCounter)
    | PNCounterValue(PNCounter)
    | GSetValue(GSet)
    | ORSetValue(ORSet)
    | Register(LWWRegister<String>)

    public func kind(): String {
        match (this) {
            case Counter(_) => "GCounter"
            case PNCounterValue(_) => "PNCounter"
            case GSetValue(_) => "GSet"
            case ORSetValue(_) => "ORSet"
            case Register(_) => "LWWRegister"
        }
    }

    /** 与同类型的值合并；类型不同时抛出 IllegalArgumentException */
    public func merge(other: ReplicatedValue): ReplicatedValue {
        match ((this, other)) {
            case (Counter(a), Counter(b)) => Counter(a.merge(b))
            case (PNCounterValue(a), PNCounterValue(b)) => PNCounterValue(a.merge(b))
            case (GSetValue(a), GSetValue(b)) => GSetValue(a.merge(b))
            case (ORSetValue(a), ORSetValue(b)) => ORSetValue(a.merge(b))
            case (Register(a), Register(b)) => Register(a.merge(b))
            case _ => throw IllegalArgumentException("不能合并 ${kind()} 与 ${other.kind()}")
        }
    }

    /**
     * 相对 before 的增量（来源 origin 的一次本地修改）；无法表示为增量时返回 None（改发整值）。
     * 计数器、GSet、寄存器的增量是同类型的部分状态，ORSet 的是 ORSetDelta
     */
    func deltaFrom(before: ReplicatedValue, origin: String): Option<ReplicatedDelta> {
        match ((this, before)) {
            case (Counter(a), Counter(b)) => match (a.deltaFrom(b)) {
                case Some(d) => Some(ReplicatedDelta.Partial(Counter(d)))
                case None => None
            }
            case (PNCounterValue(a), PNCounterValue(b)) => match (a.deltaFrom(b)) {
                case Some(d) => Some(ReplicatedDelta.Partial(PNCounterValue(d)))
                case None => None
            }
            case (GSetValue(a), GSetValue(b)) => match (a.deltaFrom(b)) {
                case Some(d) => Some(ReplicatedDelta.Partial(GSetValue(d)))
                case None => None
            }
            case (ORSetValue(a), ORSetValue(b)) => match (a.deltaFrom(b, origin)) {
                case Some(d) => Some(ReplicatedDelta.OrSet(d))
                case None => None
            }
            case (Register(_), Register(_)) => Some(ReplicatedDelta.Partial(this))
            case _ => None
        }
    }

    /** 是否还含被移除节点 removed 的数据（计数器分量、ORSet 的点或版本向量条目） */
    public func needsPruning(removed: String): Bool {
        match (this) {
            case Counter(c) => c.needsPruning(removed)
            case PNCounterValue(c) => c.needsPruning(removed)
            case ORSetValue(o) => o.needsPruning(removed)
            case _ => false
        }
    }

    /** 在值里留下数据的节点（计数器分量、ORSet 的版本向量与点；GSet / 寄存器为空——它们不按节点区分，不需要 pruning） */
    public func modifiedByNodes(): Array<String> {
        match (this) {
            case Counter(c) => c.modifiedByNodes()
            case PNCounterValue(c) => c.modifiedByNodes()
            case ORSetValue(o) => o.modifiedByNodes()
            case _ => Array<String>()
        }
    }

    func prune(removed: String, collapseInto: String): ReplicatedValue {
        match (this) {
            case Counter(c) => Counter(c.prune(removed, collapseInto))
            case PNCounterValue(c) => PNCounterValue(c.prune(removed, collapseInto))
            case ORSetValue(o) => ORSetValue(o.prune(removed, collapseInto))
            case _ => this
        }
    }

    func cleanup(removed: String): ReplicatedValue {
        match (this) {
            case Counter(c) => Counter(c.cleanup(removed))
            case PNCounterValue(c) => PNCounterValue(c.cleanup(removed))
            case ORSetValue(o) => ORSetValue(o.cleanup(removed))
            case _ => this
        }
    }

    /** 按节点区分的分量个数（计数器的分量、ORSet 的版本向量条目；GSet / 寄存器为 0）：pruning 的观测指标 */
    public func nodeComponents(): Int64 {
        match (this) {
            case Counter(c) => c.componentCount()
            case PNCounterValue(c) => c.componentCount()
            case ORSetValue(o) => o.componentCount()
            case _ => 0
        }
    }

    /** 用户可见的值是否相同（用于决定是否通知订阅者） */
    public func sameValue(other: ReplicatedValue): Bool {
        match ((this, other)) {
            case (Counter(a), Counter(b)) => a == b
            case (PNCounterValue(a), PNCounterValue(b)) => a == b
            case (GSetValue(a), GSetValue(b)) => a == b
            case (ORSetValue(a), ORSetValue(b)) => a == b
            case (Register(a), Register(b)) => a.timestamp == b.timestamp && a.node == b.node && a.value == b.value
            case _ => false
        }
    }

    public func encode(): String {
        let w = FieldWriter()
        w.put(kind())
        match (this) {
            case Counter(c) => encodeCounter(w, c)
            case PNCounterValue(c) =>
                encodeCounter(w, c.increments)
                encodeCounter(w, c.decrements)
            case GSetValue(g) =>
                let es = g.toArray()
                sort(es)
                w.put(es.size)
                for (e in es) {
                    w.put(e)
                }
            case ORSetValue(o) =>
                // 按键排序：相同内容在任何节点上编码相同，摘要才能比较
                let es = sortedKeys(o.entries)
                w.put(es.size)
                for (e in es) {
                    let dots = o.entries[e].toArray()
                    sort(dots)
                    w.put(e)
                    w.put(dots.size)
                    for (d in dots) {
                        w.put(d)
                    }
                }
                let ns = sortedKeys(o.vv)
                w.put(ns.size)
                for (n in ns) {
                    w.put(n)
                    w.put(o.vv[n])
                }
            case Register(r) =>
                w.put(r.value)
                w.put(r.timestamp)
                w.put(r.node)
        }
        w.done()
    }

    private static func encodeCounter(w: FieldWriter, c: GCounter): Unit {
        let ns = sortedKeys(c.state)
        w.put(ns.size)
        for (n in ns) {
            w.put(n)
            w.put(c.state[n])
        }
    }

    private static func sortedKeys<V>(m: HashMap<String, V>): Array<String> {
        let ks = ArrayList<String>()
        for ((k, _) in m) {
            ks.add(k)
        }
        let arr = ks.toArray()
        sort(arr)
        arr
    }

    /** 内容摘要：规范编码的 64 位 FNV-1a，相同内容在任何节点上相同 */
    public func digest(): String {
        fnv64(encode()).toString()
    }

    public static func decode(text: String): ReplicatedValue {
        decodeFrom(FieldReader(text))
    }

    static func decodeFrom(r: FieldReader): ReplicatedValue {
        match (r.next()) {
            case "GCounter" => Counter(decodeCounter(r))
            case "PNCounter" =>
                let inc = decodeCounter(r)
                let dec = decodeCounter(r)
                PNCounterValue(PNCounter(inc, dec))
            case "GSet" =>
                let n = r.nextInt()
                let set = HashSet<String>()
                for (_ in 0..n) {
                    set.add(r.next())
                }
                GSetValue(GSet(set))
            case "ORSet" =>
                let n = r.nextInt()
                let entries = HashMap<String, HashSet<String>>()
                for (_ in 0..n) {
                    let e = r.next()
                    let m = r.nextInt()
                    let dots = HashSet<String>()
                    for (_ in 0..m) {
                        dots.add(r.next())
                    }
                    entries[e] = dots
                }
                let k = r.nextInt()
                let vv = HashMap<String, Int64>()
                for (_ in 0..k) {
                    let node = r.next()
                    vv[node] = r.nextInt()
                }
                ORSetValue(ORSet(entries, vv))
            case "LWWRegister" =>
                let value = r.next()
                let ts = r.nextInt()
                let node = r.next()
                Register(LWWRegister<String>(value, ts, node))
            case other => throw IllegalArgumentException("未知的 CRDT 类型: ${other}")
        }
    }

    private static func decodeCounter(r: FieldReader): GCounter {
        let n = r.nextInt()
        let m = HashMap<String, Int64>()
        for (_ in 0..n) {
            let node = r.next()
            m[node] = r.nextInt()
        }
        GCounter(m)
    }
}

// ======================= pruning 标记（E64，对标 Akka PruningInitialized / PruningPerformed） =======================

/**
 * 一个键上针对某个被移除节点的 pruning 标记（不可变）。
 * - 准备中（performed = false）：owner（发起时的 Leader）等全部成员把自己登记进 seen——
 *   成员登记时以整值推送，它手里该节点的数据随之回到 owner，所以执行时 owner 持有的是该节点数据的最终值；
 * - 已执行（performed = true）：owner 已把该节点的数据并入自己的分量；任何节点合并前先剥掉该节点的数据，
 *   陈旧副本里的数据不会被重复计数、也不会让已删除的元素复活。performedAt（墙钟毫秒）后 TTL 到期即删除标记
 */
class PruningMarker {
    let owner: String
    let seen: HashSet<String>
    let performed: Bool
    let performedAt: Int64

    init(owner: String, seen: HashSet<String>, performed: Bool, performedAt: Int64) {
        this.owner = owner
        this.seen = seen
        this.performed = performed
        this.performedAt = performedAt
    }

    static func initialized(owner: String): PruningMarker {
        let s = HashSet<String>()
        s.add(owner)
        PruningMarker(owner, s, false, 0)
    }

    func withSeen(node: String): PruningMarker {
        let s = HashSet<String>(seen)
        s.add(node)
        PruningMarker(owner, s, false, 0)
    }

    /** 已执行的胜出（取较晚的执行时间）；都在准备中时同一发起者合并 seen，不同发起者取标识较小的（确定性） */
    func merge(other: PruningMarker): PruningMarker {
        if (performed || other.performed) {
            let at = if (performed && other.performed) {
                max(performedAt, other.performedAt)
            } else if (performed) {
                performedAt
            } else {
                other.performedAt
            }
            return PruningMarker(if (performed) { owner } else { other.owner }, HashSet<String>(), true, at)
        }
        if (owner == other.owner) {
            let s = HashSet<String>(seen)
            s.add(all: other.seen)
            return PruningMarker(owner, s, false, 0)
        }
        if (owner < other.owner) { this } else { other }
    }

    func same(other: PruningMarker): Bool {
        if (owner != other.owner || performed != other.performed || performedAt != other.performedAt ||
            seen.size != other.seen.size) {
            return false
        }
        for (n in seen) {
            if (!other.seen.contains(n)) {
                return false
            }
        }
        true
    }
}

func mergeMarkers(a: Option<HashMap<String, PruningMarker>>, b: HashMap<String, PruningMarker>): HashMap<String, PruningMarker> {
    let out = HashMap<String, PruningMarker>()
    if (let Some(m) <- a) {
        for ((r, x) in m) {
            out[r] = x
        }
    }
    for ((r, x) in b) {
        out[r] = match (out.get(r)) {
            case Some(y) => y.merge(x)
            case None => x
        }
    }
    out
}

func sameMarkers(a: Option<HashMap<String, PruningMarker>>, b: HashMap<String, PruningMarker>): Bool {
    let m = a ?? HashMap<String, PruningMarker>()
    if (m.size != b.size) {
        return false
    }
    for ((r, x) in b) {
        match (m.get(r)) {
            case Some(y) where y.same(x) => ()
            case _ => return false
        }
    }
    true
}

/** 规范编码（按被移除节点、seen 排序）：相同标记在任何节点上编码相同，摘要才能比较 */
func encodeMarkers(w: FieldWriter, markers: Option<HashMap<String, PruningMarker>>): Unit {
    let m = markers ?? HashMap<String, PruningMarker>()
    let rs = ArrayList<String>()
    for ((r, _) in m) {
        rs.add(r)
    }
    let arr = rs.toArray()
    sort(arr)
    w.put(arr.size)
    for (r in arr) {
        let x = m[r]
        w.put(r)
        w.put(x.owner)
        w.put(if (x.performed) { 1 } else { 0 })
        w.put(x.performedAt)
        let seen = x.seen.toArray()
        sort(seen)
        w.put(seen.size)
        for (n in seen) {
            w.put(n)
        }
    }
}

func decodeMarkers(r: FieldReader): HashMap<String, PruningMarker> {
    let n = r.nextInt()
    let m = HashMap<String, PruningMarker>()
    for (_ in 0..n) {
        let removed = r.next()
        let owner = r.next()
        let performed = r.nextInt() == 1
        let at = r.nextInt()
        let k = r.nextInt()
        let seen = HashSet<String>()
        for (_ in 0..k) {
            seen.add(r.next())
        }
        m[removed] = PruningMarker(owner, seen, performed, at)
    }
    m
}

/** "地址#uid" -> (地址, uid)；不是这种形式时为 None */
func splitNode(node: String): Option<(String, String)> {
    match (node.lastIndexOf("#")) {
        case Some(i) where i > 0 && i + 1 < node.size => Some((node[..i], node[i + 1..]))
        case _ => None
    }
}

func wallNowMillis(): Int64 {
    DateTime.now().toUnixTimeStamp().toMilliseconds()
}

/** pruning 需要的成员视图：本节点是否 Leader、当前成员（未 Down）的节点标识 */
class PruningMembership {
    let isLeader: () -> Bool
    let members: () -> Array<String>

    init(isLeader: () -> Bool, members: () -> Array<String>) {
        this.isLeader = isLeader
        this.members = members
    }
}

/** pruning 标记默认保留 6 小时（同 Akka pruning-marker-time-to-live） */
public let DEFAULT_PRUNING_MARKER_TTL: Duration = Duration.hour * 6

/**
 * Replicator 协议版本：2 = 增量 / 整值报文带（来源, 键）序号（DL / DQ），一致性读写与反熵报文附带 pruning 标记（E64）。
 * 作为内置兼容键 cactor.ddata.protocol 参与集群加入检查（E42）：报文格式不同的节点不能混部
 */
public let REPLICATOR_PROTOCOL_VERSION: String = "2"

/** 默认每隔多久检查一次 pruning（扫描全部键；Akka 默认 120 s，这里的扫描只看节点集合，代价小得多） */
public let DEFAULT_PRUNING_INTERVAL: Duration = Duration.second * 5

/**
 * 节点被移除后等多久才发起 pruning（Akka max-pruning-dissemination 默认 300 s）：
 * 让它生前发出、还在途中的更新先传遍集群，否则这些更新会在 pruning 后被丢弃
 */
public let DEFAULT_PRUNING_DISSEMINATION: Duration = Duration.second * 60

/** 推送给对等节点的一次更新：部分状态（merge 即可）、ORSet 增量（按序应用）、整值（merge 并补齐序号） */
enum ReplicatedDelta {
    | Partial(ReplicatedValue)
    | OrSet(ORSetDelta)
    | Full(ReplicatedValue)

    func encode(w: FieldWriter): Unit {
        match (this) {
            case Partial(v) =>
                w.put("P")
                w.sb.append(v.encode())
            case Full(v) =>
                w.put("F")
                w.sb.append(v.encode())
            case OrSet(d) =>
                w.put("O")
                w.put(d.origin)
                w.put(d.originSeq)
                encodeDots(w, d.added)
                encodeDots(w, d.removed)
        }
    }

    private static func encodeDots(w: FieldWriter, m: HashMap<String, HashSet<String>>): Unit {
        w.put(m.size)
        for ((e, dots) in m) {
            w.put(e)
            w.put(dots.size)
            for (d in dots) {
                w.put(d)
            }
        }
    }

    private static func decodeDots(r: FieldReader): HashMap<String, HashSet<String>> {
        let n = r.nextInt()
        let m = HashMap<String, HashSet<String>>()
        for (_ in 0..n) {
            let e = r.next()
            let k = r.nextInt()
            let dots = HashSet<String>()
            for (_ in 0..k) {
                dots.add(r.next())
            }
            m[e] = dots
        }
        m
    }

    static func decodeFrom(r: FieldReader): ReplicatedDelta {
        match (r.next()) {
            case "P" => Partial(ReplicatedValue.decodeFrom(r))
            case "F" => Full(ReplicatedValue.decodeFrom(r))
            case "O" =>
                let origin = r.next()
                let seq = r.nextInt()
                let added = decodeDots(r)
                let removed = decodeDots(r)
                OrSet(ORSetDelta(origin, seq, added, removed))
            case other => throw IllegalArgumentException("未知的增量类型: ${other}")
        }
    }
}

@OverflowWrapping
func fnv64(s: String): UInt64 {
    var h: UInt64 = 0xcbf29ce484222325
    for (b in s) {
        h = h ^ UInt64(b)
        h = h * 0x100000001b3
    }
    h
}

let log_replicator: Logger = Logging.logger("cactor_cluster.ddata.replicator")

// ======================= Replicator Actor =======================

/** 本地更新请求（只在本节点内传递，不经过网络） */
class LocalUpdate <: Message {
    let key: String
    let initial: ReplicatedValue
    let modify: (ReplicatedValue) -> ReplicatedValue
    let done: Option<SyncSignal>

    init(key: String, initial: ReplicatedValue, modify: (ReplicatedValue) -> ReplicatedValue, done: Option<SyncSignal>) {
        this.key = key
        this.initial = initial
        this.modify = modify
        this.done = done
    }

    public func messageType(): String {
        "ReplicatorLocalUpdate"
    }
}

// ======================= 一致性级别（E64，对标 Akka WriteMajority / ReadMajority） =======================

/**
 * 写一致性：本地应用后，等多少个副本（含本节点）确认收到新值才算成功。
 * 过半 = ⌊n/2⌋ + 1，n 为本节点 + 当前对等节点数；时限内达不到时以 ReplicatorConsistencyException 失败
 * （本地值已更新，之后仍经推送与反熵传播，同 Akka）
 */
public enum WriteConsistency {
    | WriteLocal
    | WriteMajority(Duration)
    | WriteAll(Duration)
}

/**
 * 读一致性：向对等节点取值并与本地值合并，收到多少个副本（含本节点）的回应才返回；
 * 合并结果写回本地（同 Akka）。过半写 + 过半读必然相交，读得到之前成功的过半写
 */
public enum ReadConsistency {
    | ReadLocal
    | ReadMajority(Duration)
    | ReadAll(Duration)
}

/** 一致性级别要求的副本数没有在时限内达到 */
public class ReplicatorConsistencyException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** 需要的副本数（含本节点）："majority" = ⌊n/2⌋ + 1，"all" = n，"local" = 1 */
func requiredReplicas(total: Int64, mode: String): Int64 {
    match (mode) {
        case "majority" => total / 2 + 1
        case "all" => total
        case _ => 1
    }
}

class ConsistentUpdate <: Message {
    let key: String
    let initial: ReplicatedValue
    let modify: (ReplicatedValue) -> ReplicatedValue
    let mode: String
    let timeout: Duration
    let promise: TypedPromise<Unit>

    init(key: String, initial: ReplicatedValue, modify: (ReplicatedValue) -> ReplicatedValue, mode: String,
        timeout: Duration, promise: TypedPromise<Unit>) {
        this.key = key
        this.initial = initial
        this.modify = modify
        this.mode = mode
        this.timeout = timeout
        this.promise = promise
    }

    public func messageType(): String {
        "ReplicatorConsistentUpdate"
    }
}

class ConsistentRead <: Message {
    let key: String
    let mode: String
    let timeout: Duration
    let promise: TypedPromise<Option<ReplicatedValue>>

    init(key: String, mode: String, timeout: Duration, promise: TypedPromise<Option<ReplicatedValue>>) {
        this.key = key
        this.mode = mode
        this.timeout = timeout
        this.promise = promise
    }

    public func messageType(): String {
        "ReplicatorConsistentRead"
    }
}

class OpTimeout <: Message {
    let id: Int64

    init(id: Int64) {
        this.id = id
    }

    public func messageType(): String {
        "ReplicatorOpTimeout"
    }
}

/** 进行中的一致性读写：已回应的副本、读到的合并值 */
class PendingOp {
    let id: Int64
    let key: String
    let needed: Int64
    let total: Int64
    let replied = HashSet<String>()
    var merged: Option<ReplicatedValue>
    let writePromise: Option<TypedPromise<Unit>>
    let readPromise: Option<TypedPromise<Option<ReplicatedValue>>>

    init(id: Int64, key: String, needed: Int64, total: Int64, merged: Option<ReplicatedValue>,
        writePromise: Option<TypedPromise<Unit>>, readPromise: Option<TypedPromise<Option<ReplicatedValue>>>) {
        this.id = id
        this.key = key
        this.needed = needed
        this.total = total
        this.merged = merged
        this.writePromise = writePromise
        this.readPromise = readPromise
    }
}

func ignoreOutcome(_: Bool): Unit {}

let MONO_EPOCH = MonoTime.now()

func monotonicNow(): Int64 {
    (MonoTime.now() - MONO_EPOCH).toMilliseconds()
}

/** 屏障：Replicator 按顺序处理到这里时回调（之前提交的本地更新都已生效） */
class UpdateBarrier <: Message {
    let callback: () -> Unit

    init(callback: () -> Unit) {
        this.callback = callback
    }

    public func messageType(): String {
        "ReplicatorUpdateBarrier"
    }
}

class GossipTick <: Message {
    public func messageType(): String {
        "ReplicatorGossipTick"
    }
}

/** 断档请求的重试：请求或回应丢失、来源之后又没有新增量时，不必等下一轮反熵 */
class GapRetry <: Message {
    let gapKey: String
    let originAddr: String
    let key: String

    init(gapKey: String, originAddr: String, key: String) {
        this.gapKey = gapKey
        this.originAddr = originAddr
        this.key = key
    }

    public func messageType(): String {
        "ReplicatorGapRetry"
    }
}

/** 断档请求最多重试几次（来源可能已宕机；之后交给反熵） */
let GAP_RETRY_LIMIT = 5

/** 更新完成信号（update 的同步版本使用） */
class SyncSignal {
    let lock = Mutex()
    let cond: Condition
    var done = false
    var error: Option<Exception> = None

    init() {
        cond = synchronized(lock) { lock.condition() }
    }

    func complete(e: Option<Exception>): Unit {
        synchronized(lock) {
            done = true
            error = e
            cond.notifyAll()
        }
    }

    func await(timeout: Duration): Bool {
        synchronized(lock) {
            let deadline = MonoTime.now() + timeout
            while (!done) {
                let left = deadline - MonoTime.now()
                if (left <= Duration.Zero) {
                    return false
                }
                cond.wait(timeout: left)
            }
            if (let Some(e) <- error) {
                throw e
            }
            true
        }
    }
}

/**
 * Replicator 的共享状态：Actor 写入，任意线程读取。
 *
 * 锁只保护这里的容器（键 → 值的引用、订阅表、对等节点、统计），持锁时间与值的大小无关：
 * 存进 values 的 ReplicatedValue 是深度不可变的（CRDT 的 add / remove / merge 都返回新实例，
 * 内部 HashMap 构造完后不再写），经这把锁发布后，任意线程在锁外并发读取、遍历、合并、编码
 * 都没有数据竞争。
 *
 * 历史（N0 / E43）：3e48028、4157d01 把 merge、encode、digest 全部挪进锁内、get() 改为编码再解码的副本，
 * 认为"锁外并发遍历 HashMap"导致 SIGSEGV。E43 复核：同一批对象 9 线程并发 merge / 编码、
 * 叠加四种 GC 配置连跑 50 轮无一次崩溃，旧写法也一样；锁内遍历反而让读线程长时间占锁、
 * 压住 Actor 的写入（并行跑集群套件时 testConcurrentReadsDuringLocalUpdates 因此超时）。见计划 §3.7 第 13 批。
 */
class ReplicaStore {
    let lock = Mutex()
    let values = HashMap<String, ReplicatedValue>()
    let subscribers = HashMap<String, ArrayList<(ReplicatedValue) -> Unit>>()
    let peers = ArrayList<NetworkAddress>()
    var gossipsSent: Int64 = 0
    var mergesReceived: Int64 = 0
    var decodeErrors: Int64 = 0
    var bytesSent: Int64 = 0
    var digestRounds: Int64 = 0
    // delta 传播（E64）：关闭时每次更新推送整值（对照与排障用）
    var deltaEnabled: Bool = true
    var deltaPushes: Int64 = 0
    var fullPushes: Int64 = 0
    var deltaGaps: Int64 = 0
    var deltasApplied: Int64 = 0
    // pruning（E64）：每个键的标记（被移除节点 -> 标记，标记不可变）；已移除节点 -> 得知的单调时间（毫秒）
    let pruning = HashMap<String, HashMap<String, PruningMarker>>()
    let removedNodes = HashMap<String, Int64>()
    var membership: Option<PruningMembership> = None
    var pruningTtl: Duration = DEFAULT_PRUNING_MARKER_TTL
    var pruningInterval: Duration = DEFAULT_PRUNING_INTERVAL
    var pruningDissemination: Duration = DEFAULT_PRUNING_DISSEMINATION
    var pruningInitialized: Int64 = 0
    var pruningPerformed: Int64 = 0
}

class ReplicatorActor <: Actor {
    let store: ReplicaStore
    let manager: SimpleRemoteManager
    let gossipInterval: Duration
    let rnd = Random()
    var round: Int64 = 0
    // 每个键的摘要缓存（值变化时在 apply 中清除）。只在本 Actor 线程访问，不需要锁
    let digests = HashMap<String, String>()
    // 进行中的一致性读写（只在本 Actor 线程访问）
    let pendingOps = HashMap<Int64, PendingOp>()
    var nextOpId: Int64 = 0
    // delta 传播（只在本 Actor 线程访问）：本节点每个键的增量序号；已按序应用到的各来源序号；断档请求的发出时间
    let selfNode: String
    let localSeq = HashMap<String, Int64>()
    let deltaSeen = HashMap<String, HashMap<String, Int64>>()
    let gapRequested = HashMap<String, Int64>()
    let gapRetries = HashMap<String, Int64>()
    // 下一次 pruning 检查的单调时间（毫秒）
    var pruningDue: Int64 = 0

    init(store: ReplicaStore, manager: SimpleRemoteManager, gossipInterval: Duration, selfNode: String) {
        this.store = store
        this.manager = manager
        this.gossipInterval = gossipInterval
        this.selfNode = selfNode
    }

    public prop name: String {
        get() { "replicator" }
    }

    public prop description: String {
        get() { "CRDT replicator" }
    }

    public func onStarted(context: ActorContext): Unit {
        context.timers.startTimerWithFixedDelay("gossip", GossipTick(), gossipInterval)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case u: LocalUpdate => onLocalUpdate(u)
            case c: ConsistentUpdate => onConsistentUpdate(c, context)
            case c: ConsistentRead => onConsistentRead(c, context)
            case t: OpTimeout => onOpTimeout(t.id)
            case b: UpdateBarrier =>
                try {
                    b.callback()
                } catch (e: Exception) {
                    log_replicator.warn("更新屏障回调失败: ${e}")
                }
            case _: GossipTick =>
                pruningRound()
                gossipAll()
            case g: GapRetry => onGapRetry(g, context)
            case s: StringMessage => onGossip(s.getContent(), context)
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }

    private func onLocalUpdate(u: LocalUpdate): Unit {
        let result: Option<Exception> = try {
            // 只有本 Actor 写 values，取出的值在本次处理期间不会被替换；值本身不可变，
            // modify / merge 在锁外执行（用户的 modify 可能很慢，不能让读线程等它）
            let existing = synchronized(store.lock) { store.values.get(u.key) }
            let current = existing ?? u.initial
            let next = u.modify(current)
            // 防止 modify 返回其他类型的值（此前用一次完整 merge 来触发类型检查，白白复制整个值）
            if (current.kind() != next.kind()) {
                throw IllegalArgumentException("不能把 ${u.key} 从 ${current.kind()} 改为 ${next.kind()}")
            }
            apply(u.key, next)
            pushDelta(u.key, existing, next)
            None<Exception>
        } catch (e: Exception) {
            Some(e)
        }
        match (u.done) {
            case Some(d) => d.complete(result)
            // updateAsync 的调用方不等结果，异常只能记在这里
            case None => if (let Some(e) <- result) { log_replicator.warn("更新 ${u.key} 失败: ${e}") }
        }
    }

    private func onGossip(text: String, context: ActorContext): Unit {
        try {
            let r = FieldReader(text)
            match (r.next()) {
                case "S" => onState(r)
                case "D" => onDigest(r)
                case "Q" => onRequest(r)
                case "W" => onReplicaWrite(r)
                case "WA" => onReplicaAck(r)
                case "R" => onReplicaRead(r)
                case "RA" => onReplicaReadAck(r)
                case "DL" => onDelta(r, context)
                case "DQ" => onDeltaRequest(r)
                case _ => ()
            }
        } catch (_: Exception) {
            synchronized(store.lock) {
                store.decodeErrors++
            }
        }
    }

    // ---------- 一致性读写（E64） ----------

    private func selfAddr(): String {
        manager.getLocalAddress().toString()
    }

    private func onConsistentUpdate(c: ConsistentUpdate, context: ActorContext): Unit {
        let existing = synchronized(store.lock) { store.values.get(c.key) }
        let next = try {
            let current = existing ?? c.initial
            let n = c.modify(current)
            if (current.kind() != n.kind()) {
                throw IllegalArgumentException("不能把 ${c.key} 从 ${current.kind()} 改为 ${n.kind()}")
            }
            n
        } catch (e: Exception) {
            c.promise.tryFailure(e) |> ignoreOutcome
            return
        }
        apply(c.key, next)
        // 增量照常推送（保持各节点的序号连续）；需要确认时另发带整值的 W，对方合并后回 WA
        pushDelta(c.key, existing, next)
        let peers = synchronized(store.lock) { store.peers.toArray() }
        let total = peers.size + 1
        let needed = requiredReplicas(total, c.mode)
        if (needed <= 1) {
            c.promise.trySuccess(()) |> ignoreOutcome
            return
        }
        nextOpId++
        let op = PendingOp(nextOpId, c.key, needed, total, Some(next), Some(c.promise), None)
        op.replied.add(selfAddr())
        pendingOps[op.id] = op
        let w = FieldWriter()
        w.put("W")
        w.put(selfAddr())
        w.put(op.id)
        w.put(c.key)
        w.sb.append(next.encode())
        encodeMarkers(w, markersOf(c.key))
        let text = w.done()
        for (p in peers) {
            sendTo(p, text)
        }
        context.timers.startSingleTimer("op-${op.id}", OpTimeout(op.id), c.timeout)
    }

    private func onConsistentRead(c: ConsistentRead, context: ActorContext): Unit {
        let local = synchronized(store.lock) { store.values.get(c.key) }
        let peers = synchronized(store.lock) { store.peers.toArray() }
        let total = peers.size + 1
        let needed = requiredReplicas(total, c.mode)
        if (needed <= 1) {
            c.promise.trySuccess(local) |> ignoreOutcome
            return
        }
        nextOpId++
        let op = PendingOp(nextOpId, c.key, needed, total, local, None, Some(c.promise))
        op.replied.add(selfAddr())
        pendingOps[op.id] = op
        let text = FieldWriter().put("R").put(selfAddr()).put(op.id).put(c.key).done()
        for (p in peers) {
            sendTo(p, text)
        }
        context.timers.startSingleTimer("op-${op.id}", OpTimeout(op.id), c.timeout)
    }

    /** 对方的一致性写：合并、应用，回确认 */
    private func onReplicaWrite(r: FieldReader): Unit {
        let from = NetworkAddress.parse(r.next())
        let id = r.nextInt()
        let key = r.next()
        let incoming = ReplicatedValue.decodeFrom(r)
        mergeEnvelope(key, incoming, decodeMarkers(r))
        sendTo(from, FieldWriter().put("WA").put(selfAddr()).put(id).done())
    }

    private func onReplicaAck(r: FieldReader): Unit {
        let from = r.next()
        let id = r.nextInt()
        if (let Some(op) <- pendingOps.get(id)) {
            op.replied.add(from)
            if (op.replied.size >= op.needed) {
                pendingOps.remove(id)
                if (let Some(p) <- op.writePromise) {
                    p.trySuccess(()) |> ignoreOutcome
                }
            }
        }
    }

    /** 对方的一致性读：回送本地值（没有时回空） */
    private func onReplicaRead(r: FieldReader): Unit {
        let from = NetworkAddress.parse(r.next())
        let id = r.nextInt()
        let key = r.next()
        let w = FieldWriter()
        w.put("RA")
        w.put(selfAddr())
        w.put(id)
        match (synchronized(store.lock) { store.values.get(key) }) {
            case Some(v) =>
                w.put(1)
                w.sb.append(v.encode())
                encodeMarkers(w, markersOf(key))
            case None => w.put(0)
        }
        sendTo(from, w.done())
    }

    private func onReplicaReadAck(r: FieldReader): Unit {
        let from = r.next()
        let id = r.nextInt()
        let op = match (pendingOps.get(id)) {
            case Some(o) => o
            case None => return
        }
        if (r.nextInt() == 1) {
            // 回应的值连同 pruning 标记直接合并进本地（之后的本地读与推送都包含它）
            let v = ReplicatedValue.decodeFrom(r)
            mergeEnvelope(op.key, v, decodeMarkers(r))
        }
        op.replied.add(from)
        if (op.replied.size >= op.needed) {
            pendingOps.remove(id)
            op.merged = synchronized(store.lock) { store.values.get(op.key) }
            if (let Some(p) <- op.readPromise) {
                p.trySuccess(op.merged) |> ignoreOutcome
            }
        }
    }

    private func onOpTimeout(id: Int64): Unit {
        if (let Some(op) <- pendingOps.remove(id)) {
            let ex = ReplicatorConsistencyException(
                "${op.key}：时限内只有 ${op.replied.size}/${op.total} 个副本回应，需要 ${op.needed}")
            if (let Some(p) <- op.writePromise) {
                p.tryFailure(ex) |> ignoreOutcome
            }
            if (let Some(p) <- op.readPromise) {
                p.tryFailure(ex) |> ignoreOutcome
            }
        }
    }

    /** 对方的摘要：把对方缺少或不同的键的值发给它，并请求本节点缺少或不同的键 */
    private func onDigest(r: FieldReader): Unit {
        let from = NetworkAddress.parse(r.next())
        let n = r.nextInt()
        let theirs = HashMap<String, String>()
        for (_ in 0..n) {
            let k = r.next()
            theirs[k] = r.next()
        }
        let mine = digestsNow()
        let send = ArrayList<(String, ReplicatedValue)>()
        let want = ArrayList<String>()
        for ((k, d) in mine) {
            match (theirs.get(k)) {
                case Some(td) where td == d => ()
                case _ =>
                    if (let Some(v) <- synchronized(store.lock) { store.values.get(k) }) {
                        send.add((k, v))
                    }
            }
        }
        for ((k, td) in theirs) {
            match (mine.get(k)) {
                case Some(d) where d == td => ()
                case _ => want.add(k)
            }
        }
        if (!send.isEmpty()) {
            sendTo(from, encodeState(send.toArray()))
        }
        if (!want.isEmpty()) {
            let w = FieldWriter()
            w.put("Q")
            w.put(manager.getLocalAddress().toString())
            w.put(want.size)
            for (k in want) {
                w.put(k)
            }
            sendTo(from, w.done())
        }
    }

    /** 对方请求的键：回送当前值 */
    private func onRequest(r: FieldReader): Unit {
        let from = NetworkAddress.parse(r.next())
        let n = r.nextInt()
        let out = ArrayList<(String, ReplicatedValue)>()
        for (_ in 0..n) {
            let k = r.next()
            if (let Some(v) <- synchronized(store.lock) { store.values.get(k) }) {
                out.add((k, v))
            }
        }
        if (!out.isEmpty()) {
            sendTo(from, encodeState(out.toArray()))
        }
    }

    /** 全部键的当前摘要：锁内只取键与值的引用，编码与哈希在锁外（值不可变） */
    private func digestsNow(): HashMap<String, String> {
        let snapshot = synchronized(store.lock) {
            let l = ArrayList<(String, ReplicatedValue)>(store.values.size)
            for ((k, v) in store.values) {
                l.add((k, v))
            }
            l
        }
        let out = HashMap<String, String>(snapshot.size)
        for ((k, v) in snapshot) {
            let d = match (digests.get(k)) {
                case Some(c) => c
                case None =>
                    // 摘要包含 pruning 标记：标记不同（例如一方已执行）也要交换
                    let mw = FieldWriter()
                    encodeMarkers(mw, markersOf(k))
                    let c = "${v.digest()}.${fnv64(mw.done())}"
                    digests[k] = c
                    c
            }
            out[k] = d
        }
        out
    }

    private func onState(r: FieldReader): Unit {
        try {
            let n = r.nextInt()
            for (_ in 0..n) {
                let key = r.next()
                let incoming = ReplicatedValue.decodeFrom(r)
                mergeEnvelope(key, incoming, decodeMarkers(r))
            }
        } catch (_: Exception) {
            synchronized(store.lock) {
                store.decodeErrors++
            }
        }
    }

    /** 保存新值；用户可见的值变化时通知订阅者（在 Replicator Actor 线程中调用） */
    private func apply(key: String, next: ReplicatedValue, remote!: Bool = false): Unit {
        let (before, subs) = synchronized(store.lock) {
            let before = store.values.get(key)
            store.values[key] = next
            if (remote) {
                store.mergesReceived++
            }
            let subs = match (store.subscribers.get(key)) {
                case Some(l) => l.toArray()
                case None => Array<(ReplicatedValue) -> Unit>()
            }
            (before, subs)
        }
        digests.remove(key)
        let changed = match (before) {
            case Some(b) => !b.sameValue(next)
            case None => true
        }
        if (changed) {
            for (cb in subs) {
                try {
                    cb(next)
                } catch (_: Exception) {}
            }
        }
    }

    /** 编码一组值（锁外：值不可变，见 ReplicaStore 注释） */
    private func encodeState(entries: Array<(String, ReplicatedValue)>): String {
        let w = FieldWriter()
        w.put("S")
        w.put(entries.size)
        for ((k, v) in entries) {
            w.put(k)
            // 值本身已是字段序列，直接拼接
            w.sb.append(v.encode())
            encodeMarkers(w, markersOf(k))
        }
        w.done()
    }

    private func sendToPeers(text: String): Unit {
        let peers = synchronized(store.lock) { store.peers.toArray() }
        for (p in peers) {
            sendTo(p, text)
        }
    }

    private func sendTo(p: NetworkAddress, text: String): Unit {
        try {
            manager.remoteActorOf(p, "replicator").tell(StringMessage(text))
            synchronized(store.lock) {
                store.gossipsSent++
                store.bytesSent += text.size
            }
        } catch (_: Exception) {
            // 对等节点暂不可达：下一轮反熵重试
        }
    }

    // ---------- pruning（E64，对标 Akka） ----------

    private func markersOf(key: String): Option<HashMap<String, PruningMarker>> {
        synchronized(store.lock) { store.pruning.get(key) }
    }

    private func setMarkers(key: String, markers: HashMap<String, PruningMarker>): Unit {
        synchronized(store.lock) {
            if (markers.isEmpty()) {
                store.pruning.remove(key)
            } else {
                store.pruning[key] = markers
            }
        }
        digests.remove(key)
    }

    /**
     * 合并带 pruning 标记的整值：先合并标记；对已执行 pruning 的节点，本地值与来值都先剥掉它的数据
     * （它已由执行者的分量代表），再合并值
     */
    private func mergeEnvelope(key: String, incoming: ReplicatedValue, inMarkers: HashMap<String, PruningMarker>): Unit {
        let (cur, curMarkers) = synchronized(store.lock) { (store.values.get(key), store.pruning.get(key)) }
        let markers = mergeMarkers(curMarkers, inMarkers)
        var a = cur
        var b = incoming
        for ((removed, m) in markers) {
            if (m.performed) {
                a = match (a) {
                    case Some(x) => Some(x.cleanup(removed))
                    case None => None
                }
                b = b.cleanup(removed)
            }
        }
        if (!sameMarkers(curMarkers, markers)) {
            setMarkers(key, markers)
        }
        apply(key, match (a) {
            case Some(x) => x.merge(b)
            case None => b
        }, remote: true)
    }

    /** 以整值（连同标记）推送，序号照常递增：pruning 的标记与改写不能表示为增量 */
    private func pushFull(key: String, value: ReplicatedValue): Unit {
        let seq = (localSeq.get(key) ?? 0) + 1
        localSeq[key] = seq
        synchronized(store.lock) {
            store.fullPushes++
        }
        sendToPeers(encodeDelta(key, seq, ReplicatedDelta.Full(value)))
    }

    /**
     * pruning 的一轮（每个 gossip 周期检查，按 pruningInterval 执行；需要 setMembership）。对标 Akka Replicator 的
     * collectRemovedNodes / initRemovedNodePruning / performRemovedNodePruning / deleteObsoletePruningPerformed：
     * 1. Leader 扫描数据里出现、却不是成员的节点（同址重启后新实例可能成为 Leader，它收不到旧实例的 MemberRemoved），
     *    与 nodeRemoved 通知的一起记为已移除；
     * 2. 移除满 pruningDissemination 后，Leader 对还含它数据、没有标记的键发起（发起者已不在成员中时接管）；
     * 3. 每个节点把自己登记进准备中的标记并以整值推送——手里该节点的数据随标记一起回到发起者；
     * 4. 发起者（不要求仍是 Leader，否则 Leader 中途变更会让标记永远停在准备中）看到全部成员都已登记，
     *    把该节点的数据并入自己的分量，标记改为已执行；
     * 5. 已执行的标记超过 TTL 后删除（期间所有合并都会先剥掉该节点的数据）
     */
    private func pruningRound(): Unit {
        let (ctxOpt, ttl, interval, dissemination) = synchronized(store.lock) {
            (store.membership, store.pruningTtl, store.pruningInterval, store.pruningDissemination)
        }
        let ctx = match (ctxOpt) {
            case Some(c) => c
            case None => return
        }
        let mono = monotonicNow()
        if (mono < pruningDue) {
            return
        }
        pruningDue = mono + interval.toMilliseconds()
        let leader = try {
            ctx.isLeader()
        } catch (_: Exception) {
            false
        }
        let members = HashSet<String>()
        try {
            members.add(all: ctx.members())
        } catch (_: Exception) {
            return
        }
        members.add(selfNode)
        let snapshot = synchronized(store.lock) {
            let l = ArrayList<(String, ReplicatedValue, Option<HashMap<String, PruningMarker>>)>()
            for ((k, v) in store.values) {
                l.add((k, v, store.pruning.get(k)))
            }
            l
        }
        if (leader) {
            collectRemovedNodes(snapshot, members, mono)
        }
        let horizon = mono - ttl.toMilliseconds()
        let ready = HashSet<String>()
        synchronized(store.lock) {
            let drop = ArrayList<String>()
            for ((n, t) in store.removedNodes) {
                if (members.contains(n) || t < horizon) {
                    // 误判（其实是成员）或已过 TTL（它的数据都已清理）：不再跟踪
                    drop.add(n)
                } else if (mono - t >= dissemination.toMilliseconds()) {
                    ready.add(n)
                }
            }
            for (n in drop) {
                store.removedNodes.remove(n)
                if (!members.contains(n)) {
                    deltaSeen.remove(n)
                }
            }
        }
        let now = wallNowMillis()
        for ((key, value, current) in snapshot) {
            let markers = HashMap<String, PruningMarker>()
            if (let Some(m) <- current) {
                for ((r, x) in m) {
                    markers[r] = x
                }
            }
            var next = value
            var changed = false
            if (leader) {
                for (r in ready) {
                    match (markers.get(r)) {
                        case None where next.needsPruning(r) =>
                            markers[r] = PruningMarker.initialized(selfNode)
                            changed = true
                            synchronized(store.lock) {
                                store.pruningInitialized++
                            }
                        case Some(m) where !m.performed && !members.contains(m.owner) =>
                            // 发起者已离开：由现任 Leader 重新发起
                            markers[r] = PruningMarker.initialized(selfNode)
                            changed = true
                            synchronized(store.lock) {
                                store.pruningInitialized++
                            }
                        case _ => ()
                    }
                }
            }
            for ((r, m) in markers.toArray()) {
                if (!m.performed && !m.seen.contains(selfNode)) {
                    markers[r] = m.withSeen(selfNode)
                    changed = true
                }
            }
            for ((r, m) in markers.toArray()) {
                if (m.performed || m.owner != selfNode || members.contains(r)) {
                    continue
                }
                var all = true
                for (n in members) {
                    if (!m.seen.contains(n)) {
                        all = false
                        break
                    }
                }
                if (all) {
                    next = next.prune(r, selfNode)
                    markers[r] = PruningMarker(selfNode, HashSet<String>(), true, now)
                    changed = true
                    synchronized(store.lock) {
                        store.pruningPerformed++
                    }
                    log_replicator.info("ddata pruning：${key} 中 ${r} 的数据已并入 ${selfNode}")
                }
            }
            for ((r, m) in markers.toArray()) {
                if (m.performed && now - m.performedAt > ttl.toMilliseconds()) {
                    markers.remove(r)
                    next = next.cleanup(r)
                    changed = true
                }
            }
            if (changed) {
                setMarkers(key, markers)
                apply(key, next)
                pushFull(key, next)
            }
        }
    }

    /**
     * Leader 扫描数据中出现的节点：既不是成员、也没有登记为已移除的，记为已移除（从现在起计算传播等待）。
     * 只认 "地址#uid" 形式、uid 非 0 的标识，且该地址上没有 uid 未知（0）的成员——宁可漏掉也不能把活着的节点当成已移除
     */
    private func collectRemovedNodes(snapshot: ArrayList<(String, ReplicatedValue, Option<HashMap<String, PruningMarker>>)>,
        members: HashSet<String>, mono: Int64): Unit {
        let unknownUidAt = HashSet<String>()
        for (m in members) {
            if (let Some((addr, uid)) <- splitNode(m)) {
                if (uid == "0") {
                    unknownUidAt.add(addr)
                }
            }
        }
        let known = synchronized(store.lock) { HashSet<String>(store.removedNodes.keys()) }
        let found = HashSet<String>()
        for ((_, v, _) in snapshot) {
            for (n in v.modifiedByNodes()) {
                if (n == selfNode || members.contains(n) || known.contains(n) || found.contains(n)) {
                    continue
                }
                match (splitNode(n)) {
                    case Some((addr, uid)) where uid != "0" && !unknownUidAt.contains(addr) => found.add(n)
                    case _ => ()
                }
            }
        }
        if (found.isEmpty()) {
            return
        }
        synchronized(store.lock) {
            for (n in found) {
                if (!store.removedNodes.contains(n)) {
                    store.removedNodes[n] = mono
                }
            }
        }
        log_replicator.info("ddata pruning：数据中发现已不在集群的节点 ${String.join(found.toArray(), delimiter: ", ")}")
    }

    // ---------- delta 传播（E64，对标 Akka delta-CRDT） ----------

    /**
     * 推送一次本地更新：增量（能表示时）或整值，都带本节点该键的下一个序号。
     * 键第一次出现时发整值：initial 可能不是空值，相对 initial 的增量会漏掉 initial 本身
     */
    private func pushDelta(key: String, before: Option<ReplicatedValue>, next: ReplicatedValue): Unit {
        let seq = (localSeq.get(key) ?? 0) + 1
        localSeq[key] = seq
        let enabled = synchronized(store.lock) { store.deltaEnabled }
        let payload = match (before) {
            case Some(b) where enabled => next.deltaFrom(b, selfNode) ?? ReplicatedDelta.Full(next)
            case _ => ReplicatedDelta.Full(next)
        }
        synchronized(store.lock) {
            match (payload) {
                case ReplicatedDelta.Full(_) => store.fullPushes++
                case _ => store.deltaPushes++
            }
        }
        sendToPeers(encodeDelta(key, seq, payload))
    }

    private func encodeDelta(key: String, seq: Int64, payload: ReplicatedDelta): String {
        let w = FieldWriter()
        w.put("DL")
        w.put(selfNode)
        w.put(selfAddr())
        w.put(key)
        w.put(seq)
        payload.encode(w)
        match (payload) {
            case ReplicatedDelta.Full(_) => encodeMarkers(w, markersOf(key))
            case _ => ()
        }
        w.done()
    }

    private func seenSeq(origin: String, key: String): Int64 {
        match (deltaSeen.get(origin)) {
            case Some(m) => m.get(key) ?? 0
            case None => 0
        }
    }

    private func setSeen(origin: String, key: String, seq: Int64): Unit {
        match (deltaSeen.get(origin)) {
            case Some(m) => m[key] = seq
            case None =>
                let m = HashMap<String, Int64>()
                m[key] = seq
                deltaSeen[origin] = m
        }
    }

    /**
     * 来源的增量：整值随时可合并（并把序号补齐到它）；增量只在序号恰好是下一个时应用，
     * 重复的丢弃，断档的不应用、向来源请求整值（1 s 内不重复请求，期间反熵也会补上）
     */
    private func onDelta(r: FieldReader, context: ActorContext): Unit {
        let origin = r.next()
        let originAddr = r.next()
        let key = r.next()
        let seq = r.nextInt()
        let payload = ReplicatedDelta.decodeFrom(r)
        let seen = seenSeq(origin, key)
        let cur = synchronized(store.lock) { store.values.get(key) }
        match (payload) {
            case ReplicatedDelta.Full(v) =>
                mergeEnvelope(key, v, decodeMarkers(r))
                if (seq > seen) {
                    setSeen(origin, key, seq)
                }
                let gk = "${origin}\n${key}"
                gapRequested.remove(gk)
                gapRetries.remove(gk)
                return
            case _ => ()
        }
        // 来源的数据已被 pruning 并入执行者的分量：它迟到的增量不能再加回来（否则计数器重复计数）
        if (let Some(ms) <- markersOf(key)) {
            if (let Some(m) <- ms.get(origin)) {
                if (m.performed) {
                    return
                }
            }
        }
        if (seq <= seen) {
            return
        }
        if (seq > seen + 1) {
            synchronized(store.lock) {
                store.deltaGaps++
            }
            let gapKey = "${origin}\n${key}"
            match (gapRequested.get(gapKey)) {
                // 已有未完成的请求：来源按序回送的整值会覆盖这之前的全部增量，不重复请求（避免一次丢包引发一串整值回送）
                case Some(t) where monotonicNow() - t < 1000 => ()
                case _ =>
                    gapRetries[gapKey] = 0
                    requestFull(gapKey, originAddr, key, context)
            }
            return
        }
        let next = match ((payload, cur)) {
            case (ReplicatedDelta.Partial(v), Some(c)) => c.merge(v)
            case (ReplicatedDelta.Partial(v), None) => v
            case (ReplicatedDelta.OrSet(d), Some(ReplicatedValue.ORSetValue(o))) => ReplicatedValue.ORSetValue(o.applyDelta(d))
            case (ReplicatedDelta.OrSet(d), None) => ReplicatedValue.ORSetValue(ORSet().applyDelta(d))
            case _ => throw IllegalArgumentException("${key} 的增量与本地值类型不符")
        }
        apply(key, next, remote: true)
        setSeen(origin, key, seq)
        synchronized(store.lock) {
            store.deltasApplied++
        }
    }

    private func requestFull(gapKey: String, originAddr: String, key: String, context: ActorContext): Unit {
        gapRequested[gapKey] = monotonicNow()
        sendTo(NetworkAddress.parse(originAddr), FieldWriter().put("DQ").put(selfAddr()).put(key).done())
        context.timers.startSingleTimer("gap-${gapKey}", GapRetry(gapKey, originAddr, key), Duration.second)
    }

    /** 1 s 内没收到来源的整值（请求或回应丢失）：重发请求 */
    private func onGapRetry(g: GapRetry, context: ActorContext): Unit {
        if (!gapRequested.contains(g.gapKey)) {
            return
        }
        let n = (gapRetries.get(g.gapKey) ?? 0) + 1
        if (n > GAP_RETRY_LIMIT) {
            gapRequested.remove(g.gapKey)
            gapRetries.remove(g.gapKey)
            return
        }
        gapRetries[g.gapKey] = n
        requestFull(g.gapKey, g.originAddr, g.key, context)
    }

    /** 对方发现了本节点该键的增量断档：回送整值与当前序号 */
    private func onDeltaRequest(r: FieldReader): Unit {
        let from = NetworkAddress.parse(r.next())
        let key = r.next()
        if (let Some(v) <- synchronized(store.lock) { store.values.get(key) }) {
            sendTo(from, encodeDelta(key, localSeq.get(key) ?? 0, ReplicatedDelta.Full(v)))
        }
    }

    /**
     * 反熵：每轮向一个随机对等节点发送全部键的摘要（键 + 64 位内容摘要），
     * 对方只回送不同或缺少的键的值，并请求本节点不同或缺少的键。状态一致时每轮只有摘要在网络上传输。
     * 更新本身仍立即推送给全部对等节点（只含变化的键），反熵只负责补上丢失的推送。
     */
    private func gossipAll(): Unit {
        let peers = synchronized(store.lock) { store.peers.toArray() }
        if (peers.isEmpty()) {
            return
        }
        let mine = digestsNow()
        if (mine.isEmpty()) {
            return
        }
        round++
        let target = peers[Int64(rnd.nextUInt64() % UInt64(peers.size))]
        let w = FieldWriter()
        w.put("D")
        w.put(manager.getLocalAddress().toString())
        w.put(mine.size)
        for ((k, d) in mine) {
            w.put(k)
            w.put(d)
        }
        synchronized(store.lock) {
            store.digestRounds++
        }
        sendTo(target, w.done())
    }
}

// ======================= 对外接口 =======================

/** delta 传播统计（单调递增） */
public struct ReplicatorDeltaStats {
    /** 以增量推送的本地更新数 */
    public let deltaPushes: Int64
    /** 以整值推送的本地更新数（键第一次出现、无法表示为增量、或关闭了 delta 传播） */
    public let fullPushes: Int64
    /** 已按序应用的远程增量数 */
    public let deltasApplied: Int64
    /** 发现的序号断档数（每次向来源请求整值补齐） */
    public let gaps: Int64

    public init(deltaPushes: Int64, fullPushes: Int64, deltasApplied: Int64, gaps: Int64) {
        this.deltaPushes = deltaPushes
        this.fullPushes = fullPushes
        this.deltasApplied = deltasApplied
        this.gaps = gaps
    }
}

/**
 * 本节点的 Replicator
 *
 * ```
 * let r = Replicator(system, remoteManager, Duration.millisecond * 200)
 * r.join(otherNodeAddress)
 * r.incrementCounter("visits", 1)
 * r.counterValue("visits")      // 最终等于所有节点增量之和
 * ```
 */
public class Replicator {
    private let store = ReplicaStore()
    private let actor: ActorRef
    private let system: CActorSystem
    /** 本节点在 CRDT 中使用的节点标识（默认是"远程地址#启动标识"） */
    public let selfNode: String

    /**
     * 节点标识默认取"地址#启动标识"（对标 Akka 以 UniqueAddress 作为 CRDT 的节点键，N4）：
     * 只用地址时，同一地址重启的新进程从 0 重新计数，会被其他副本里旧进程的分量吞掉——
     * GCounter / PNCounter 的增量在新值超过旧值之前全部丢失，ORSet 的新 add 被当成"已见过又删除"而丢失
     */
    public init(system: CActorSystem, manager: SimpleRemoteManager, gossipInterval: Duration) {
        this(system, manager, gossipInterval, "${manager.getLocalAddress().toString()}#${manager.selfUid()}")
    }

    public init(system: CActorSystem, manager: SimpleRemoteManager, gossipInterval: Duration, selfNode: String) {
        this.system = system
        this.selfNode = selfNode
        let s = store
        let actorName = "replicator-${selfNode.replace("/", "_").replace(":", "_").replace("#", "_")}"
        let id = selfNode
        this.actor = system.actorOf({ => ReplicatorActor(s, manager, gossipInterval, id) }, actorName)
        manager.exposeActor("replicator", actor)
    }

    /** 加入对等节点（双方都应 join 对方；只单向 join 时数据只向对方推送） */
    public func join(peer: NetworkAddress): Unit {
        synchronized(store.lock) {
            for (p in store.peers) {
                if (p.toString() == peer.toString()) {
                    return
                }
            }
            store.peers.add(peer)
        }
    }

    public func leave(peer: NetworkAddress): Unit {
        synchronized(store.lock) {
            store.peers.removeIf({ p => p.toString() == peer.toString() })
        }
    }

    /**
     * 本地更新：key 不存在时以 initial 为当前值，modify 返回新值。
     * 等待本地生效后返回（不等待其他节点）；modify 抛出的异常传给调用方。
     * 不要在 Actor 的 receive 里调用：同步等待 Replicator Actor 会占住当前工作线程，请用 updateAsync
     */
    public func update(key: String, initial: ReplicatedValue, modify: (ReplicatedValue) -> ReplicatedValue): Unit {
        let done = SyncSignal()
        actor.tell(LocalUpdate(key, initial, modify, Some(done)))
        if (!done.await(Duration.second * 10)) {
            throw IllegalStateException("Replicator 更新 ${key} 超时")
        }
    }

    /**
     * 本地当前值（不可变快照，对标 Akka 的 Get 返回不可变数据）。
     *
     * 直接返回存储里的引用，O(1)：CRDT 值深度不可变，之后的本地更新或远程合并生成新实例、
     * 不会改动已返回的这一份，调用方可以在任意线程遍历、合并、编码。
     * （此前锁内编码 + 锁外解码复制一份，读线程长时间占锁，见 ReplicaStore 注释）
     */
    public func get(key: String): Option<ReplicatedValue> {
        synchronized(store.lock) {
            store.values.get(key)
        }
    }

    public func keys(): Array<String> {
        synchronized(store.lock) {
            let l = ArrayList<String>()
            for ((k, _) in store.values) {
                l.add(k)
            }
            l.toArray()
        }
    }

    /** 值发生变化（本地更新或从其他节点合并）时回调；回调在 Replicator Actor 线程中执行 */
    public func subscribe(key: String, callback: (ReplicatedValue) -> Unit): Unit {
        synchronized(store.lock) {
            match (store.subscribers.get(key)) {
                case Some(l) => l.add(callback)
                case None =>
                    let l = ArrayList<(ReplicatedValue) -> Unit>()
                    l.add(callback)
                    store.subscribers[key] = l
            }
        }
    }

    /** 发出的字节数（推送、摘要与差异回送合计） */
    public func bytesSent(): Int64 {
        synchronized(store.lock) {
            store.bytesSent
        }
    }

    /**
     * delta 传播开关（默认开启）。关闭后每次更新推送整值（对照与排障用）；
     * 两种推送都带序号，同一集群中开关不同的节点照常互通
     */
    public func setDeltaPropagation(enabled: Bool): Unit {
        synchronized(store.lock) {
            store.deltaEnabled = enabled
        }
    }

    /**
     * 接上集群成员视图以启用 pruning（ClusterReplicatorBinding 自动调用）：
     * isLeader —— 本节点是否 Leader（由它发起与执行）；memberNodes —— 当前成员（未 Down）的节点标识（"地址#uid"）
     */
    public func setMembership(isLeader: () -> Bool, memberNodes: () -> Array<String>): Unit {
        synchronized(store.lock) {
            store.membership = Some(PruningMembership(isLeader, memberNodes))
        }
    }

    /** 节点（"地址#uid"）已从集群移除：之后由 Leader 清理它在各个键中的数据（ClusterReplicatorBinding 自动调用） */
    public func nodeRemoved(node: String): Unit {
        if (node == selfNode) {
            return
        }
        synchronized(store.lock) {
            if (!store.removedNodes.contains(node)) {
                store.removedNodes[node] = monotonicNow()
            }
        }
    }

    /**
     * pruning 的时间参数（只改传入的项）：
     * interval —— 检查间隔（默认 5 s）；maxDissemination —— 节点移除后等多久才发起（默认 60 s，让它在途的更新先传遍）；
     * markerTimeToLive —— 已执行标记的保留时间（默认 6 h，期间陈旧副本里该节点的数据都会被剥掉；须大于副本可能离线的时长）
     */
    public func configurePruning(interval!: ?Duration = None, maxDissemination!: ?Duration = None,
        markerTimeToLive!: ?Duration = None): Unit {
        synchronized(store.lock) {
            if (let Some(d) <- interval) {
                store.pruningInterval = d
            }
            if (let Some(d) <- maxDissemination) {
                store.pruningDissemination = d
            }
            if (let Some(d) <- markerTimeToLive) {
                store.pruningTtl = d
            }
        }
    }

    /** pruning 统计：(本节点发起的次数, 本节点执行的次数, 当前带标记的键数) */
    public func pruningStats(): (Int64, Int64, Int64) {
        synchronized(store.lock) {
            (store.pruningInitialized, store.pruningPerformed, store.pruning.size)
        }
    }

    /** delta 传播统计 */
    public func deltaStats(): ReplicatorDeltaStats {
        synchronized(store.lock) {
            ReplicatorDeltaStats(store.deltaPushes, store.fullPushes, store.deltasApplied, store.deltaGaps)
        }
    }

    /** (已发送的状态消息数, 已合并的远程状态数, 解码失败数) */
    public func stats(): (Int64, Int64, Int64) {
        synchronized(store.lock) {
            (store.gossipsSent, store.mergesReceived, store.decodeErrors)
        }
    }

    public func stop(): Unit {
        system.stop(actor)
    }

    // ---------- 常用类型的便捷方法 ----------

    public func incrementCounter(key: String, delta: Int64): Unit {
        let node = selfNode
        update(key, ReplicatedValue.PNCounterValue(PNCounter()), { v =>
            match (v) {
                case PNCounterValue(c) => ReplicatedValue.PNCounterValue(c.increment(node, delta))
                case _ => throw IllegalArgumentException("${key} 不是 PNCounter")
            }
        })
    }

    public func counterValue(key: String): Int64 {
        match (get(key)) {
            case Some(PNCounterValue(c)) => c.value()
            case Some(Counter(c)) => c.value()
            case _ => 0
        }
    }

    /**
     * 与 update 相同，但不等待本地生效；modify 抛出的异常记录为警告，调用方收不到。
     * Actor 在 receive 里更新时用这个版本：Replicator Actor 按顺序处理，之后经它的读写都能看到这次更新
     */
    public func updateAsync(key: String, initial: ReplicatedValue, modify: (ReplicatedValue) -> ReplicatedValue): Unit {
        actor.tell(LocalUpdate(key, initial, modify, None<SyncSignal>))
    }

    public func addToSet(key: String, element: String): Unit {
        update(key, ReplicatedValue.ORSetValue(ORSet()), orSetAdd(key, element))
    }

    /**
     * 之前（在同一线程上）提交的本地更新全部生效后回调（E12′）：回调在 Replicator Actor 线程执行，应当很快。
     * 例如 pubsub 的 subscribeAsync 在节点集合的添加生效后才完成
     */
    public func afterPendingUpdates(callback: () -> Unit): Unit {
        actor.tell(UpdateBarrier(callback))
    }

    /**
     * 按一致性级别更新（E64，对标 Akka `Update(key, initial, WriteMajority(timeout))`）：本地应用后把新值发给对等节点，
     * 等足够多的副本确认。WriteLocal 与 updateAsync 相同（推送、不等确认）。不阻塞，可在 Actor 内使用
     *
     * ```
     * replicator.updateWith("inventory", initial, modify, WriteConsistency.WriteMajority(3.seconds))
     *     .onComplete({ o => ... })
     * ```
     */
    public func updateWith(key: String, initial: ReplicatedValue, modify: (ReplicatedValue) -> ReplicatedValue,
        consistency: WriteConsistency): TypedPromise<Unit> {
        let p = TypedPromise<Unit>()
        let (mode, timeout) = match (consistency) {
            case WriteLocal => ("local", Duration.second)
            case WriteMajority(t) => ("majority", t)
            case WriteAll(t) => ("all", t)
        }
        actor.tell(ConsistentUpdate(key, initial, modify, mode, timeout, p))
        p
    }

    /**
     * 按一致性级别读取（E64，对标 Akka `Get(key, ReadMajority(timeout))`）：向对等节点取值并与本地值合并，
     * 收到足够多副本的回应后返回合并结果（并写回本地）。ReadLocal 等同 get。不阻塞
     */
    public func getWith(key: String, consistency: ReadConsistency): TypedPromise<Option<ReplicatedValue>> {
        let p = TypedPromise<Option<ReplicatedValue>>()
        let (mode, timeout) = match (consistency) {
            case ReadLocal => ("local", Duration.second)
            case ReadMajority(t) => ("majority", t)
            case ReadAll(t) => ("all", t)
        }
        actor.tell(ConsistentRead(key, mode, timeout, p))
        p
    }

    /** ORSet 加入元素，按一致性级别确认 */
    public func addToSetWith(key: String, element: String, consistency: WriteConsistency): TypedPromise<Unit> {
        updateWith(key, ReplicatedValue.ORSetValue(ORSet()), orSetAdd(key, element), consistency)
    }

    /** PNCounter 加 delta，按一致性级别确认 */
    public func incrementCounterWith(key: String, delta: Int64, consistency: WriteConsistency): TypedPromise<Unit> {
        let node = selfNode
        updateWith(key, ReplicatedValue.PNCounterValue(PNCounter()), { v =>
            match (v) {
                case PNCounterValue(c) => ReplicatedValue.PNCounterValue(c.increment(node, delta))
                case _ => throw IllegalArgumentException("${key} 不是 PNCounter")
            }
        }, consistency)
    }

    /** addToSet 的不等待版本（见 updateAsync） */
    public func addToSetAsync(key: String, element: String): Unit {
        updateAsync(key, ReplicatedValue.ORSetValue(ORSet()), orSetAdd(key, element))
    }

    public func removeFromSet(key: String, element: String): Unit {
        update(key, ReplicatedValue.ORSetValue(ORSet()), orSetRemove(key, element))
    }

    /** removeFromSet 的不等待版本（见 updateAsync） */
    public func removeFromSetAsync(key: String, element: String): Unit {
        updateAsync(key, ReplicatedValue.ORSetValue(ORSet()), orSetRemove(key, element))
    }

    private func orSetAdd(key: String, element: String): (ReplicatedValue) -> ReplicatedValue {
        let node = selfNode
        { v =>
            match (v) {
                case ORSetValue(s) => ReplicatedValue.ORSetValue(s.add(node, element))
                case _ => throw IllegalArgumentException("${key} 不是 ORSet")
            }
        }
    }

    private func orSetRemove(key: String, element: String): (ReplicatedValue) -> ReplicatedValue {
        { v =>
            match (v) {
                case ORSetValue(s) => ReplicatedValue.ORSetValue(s.remove(element))
                case _ => throw IllegalArgumentException("${key} 不是 ORSet")
            }
        }
    }

    /** ORSet 的元素（按字典序） */
    public func setElements(key: String): Array<String> {
        match (get(key)) {
            case Some(ORSetValue(s)) =>
                let es = s.elements()
                sort(es)
                es
            case Some(GSetValue(s)) =>
                let es = s.toArray()
                sort(es)
                es
            case _ => Array<String>()
        }
    }

    /** 写入 LWW 寄存器，时间戳取当前系统时间（毫秒） */
    public func setRegister(key: String, value: String): Unit {
        let node = selfNode
        let ts = DateTime.now().toUnixTimeStamp().toMilliseconds()
        update(key, ReplicatedValue.Register(LWWRegister<String>(value, 0, node)), { v =>
            match (v) {
                case Register(r) => ReplicatedValue.Register(r.set(value, ts, node))
                case _ => throw IllegalArgumentException("${key} 不是 LWWRegister")
            }
        })
    }

    public func registerValue(key: String): Option<String> {
        match (get(key)) {
            case Some(Register(r)) => Some(r.value)
            case _ => None<String>
        }
    }
}
