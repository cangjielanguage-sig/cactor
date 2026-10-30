/*
 * CActor Cluster - Replicator：在节点间同步 CRDT（对标 Akka Distributed Data 的 Replicator）
 *
 * 一致性模型（与 Akka 的 WriteLocal + gossip 相同）：
 * - 更新只在本地执行并立即生效（不等待其他节点），随后把该键的完整状态推送给所有对等节点；
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
import cactor_remote.{SimpleRemoteManager}

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

@OverflowWrapping
func fnv64(s: String): UInt64 {
    var h: UInt64 = 0xcbf29ce484222325
    for (b in s) {
        h = h ^ UInt64(b)
        h = h * 0x100000001b3
    }
    h
}

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

class GossipTick <: Message {
    public func messageType(): String {
        "ReplicatorGossipTick"
    }
}

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

/** Replicator 的共享状态：Actor 写入，任意线程读取 */
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
    // 缓存每个键的摘要（值变化时清除）
    let digests = HashMap<String, String>()
}

class ReplicatorActor <: Actor {
    let store: ReplicaStore
    let manager: SimpleRemoteManager
    let gossipInterval: Duration
    let rnd = Random()
    var round: Int64 = 0

    init(store: ReplicaStore, manager: SimpleRemoteManager, gossipInterval: Duration) {
        this.store = store
        this.manager = manager
        this.gossipInterval = gossipInterval
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
            case _: GossipTick => gossipAll()
            case s: StringMessage => onGossip(s.getContent())
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }

    private func onLocalUpdate(u: LocalUpdate): Unit {
        let result: Option<Exception> = try {
            let current = synchronized(store.lock) { store.values.get(u.key) ?? u.initial }
            let next = u.modify(current)
            // 防止 modify 返回其他类型的值
            let _ = current.merge(next)
            apply(u.key, next)
            push(u.key, next)
            None<Exception>
        } catch (e: Exception) {
            Some(e)
        }
        if (let Some(d) <- u.done) {
            d.complete(result)
        }
    }

    private func onGossip(text: String): Unit {
        try {
            let r = FieldReader(text)
            match (r.next()) {
                case "S" => onState(r)
                case "D" => onDigest(r)
                case "Q" => onRequest(r)
                case _ => ()
            }
        } catch (_: Exception) {
            synchronized(store.lock) {
                store.decodeErrors++
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

    private func digestsNow(): HashMap<String, String> {
        synchronized(store.lock) {
            let out = HashMap<String, String>()
            for ((k, v) in store.values) {
                let d = match (store.digests.get(k)) {
                    case Some(c) => c
                    case None =>
                        let c = v.digest()
                        store.digests[k] = c
                        c
                }
                out[k] = d
            }
            out
        }
    }

    private func onState(r: FieldReader): Unit {
        try {
            let n = r.nextInt()
            for (_ in 0..n) {
                let key = r.next()
                let incoming = ReplicatedValue.decodeFrom(r)
                let merged = match (synchronized(store.lock) { store.values.get(key) }) {
                    case Some(cur) => cur.merge(incoming)
                    case None => incoming
                }
                synchronized(store.lock) {
                    store.mergesReceived++
                }
                apply(key, merged)
            }
        } catch (_: Exception) {
            synchronized(store.lock) {
                store.decodeErrors++
            }
        }
    }

    /** 保存新值；用户可见的值变化时通知订阅者（在 Replicator Actor 线程中调用） */
    private func apply(key: String, next: ReplicatedValue): Unit {
        let (changed, subs) = synchronized(store.lock) {
            let before = store.values.get(key)
            store.values[key] = next
            store.digests.remove(key)
            let changed = match (before) {
                case Some(b) => !b.sameValue(next)
                case None => true
            }
            let subs = match (store.subscribers.get(key)) {
                case Some(l) => l.toArray()
                case None => Array<(ReplicatedValue) -> Unit>()
            }
            (changed, subs)
        }
        if (changed) {
            for (cb in subs) {
                try {
                    cb(next)
                } catch (_: Exception) {}
            }
        }
    }

    private func encodeState(entries: Array<(String, ReplicatedValue)>): String {
        let w = FieldWriter()
        w.put("S")
        w.put(entries.size)
        for ((k, v) in entries) {
            w.put(k)
            // 值本身已是字段序列，直接拼接
            w.sb.append(v.encode())
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

    private func push(key: String, value: ReplicatedValue): Unit {
        sendToPeers(encodeState([(key, value)]))
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
    /** 本节点在 CRDT 中使用的节点标识（默认是远程地址） */
    public let selfNode: String

    public init(system: CActorSystem, manager: SimpleRemoteManager, gossipInterval: Duration) {
        this(system, manager, gossipInterval, manager.getLocalAddress().toString())
    }

    public init(system: CActorSystem, manager: SimpleRemoteManager, gossipInterval: Duration, selfNode: String) {
        this.system = system
        this.selfNode = selfNode
        let s = store
        let actorName = "replicator-${selfNode.replace("/", "_").replace(":", "_")}"
        this.actor = system.actorOf({ => ReplicatorActor(s, manager, gossipInterval) }, actorName)
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
     */
    public func update(key: String, initial: ReplicatedValue, modify: (ReplicatedValue) -> ReplicatedValue): Unit {
        let done = SyncSignal()
        actor.tell(LocalUpdate(key, initial, modify, Some(done)))
        if (!done.await(Duration.second * 10)) {
            throw IllegalStateException("Replicator 更新 ${key} 超时")
        }
    }

    /** 本地当前值 */
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

    /** (已发送的状态消息数, 已合并的远程状态数, 解码失败数) */
    /** 发出的字节数（推送、摘要与差异回送合计） */
    public func bytesSent(): Int64 {
        synchronized(store.lock) {
            store.bytesSent
        }
    }

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

    public func addToSet(key: String, element: String): Unit {
        let node = selfNode
        update(key, ReplicatedValue.ORSetValue(ORSet()), { v =>
            match (v) {
                case ORSetValue(s) => ReplicatedValue.ORSetValue(s.add(node, element))
                case _ => throw IllegalArgumentException("${key} 不是 ORSet")
            }
        })
    }

    public func removeFromSet(key: String, element: String): Unit {
        update(key, ReplicatedValue.ORSetValue(ORSet()), { v =>
            match (v) {
                case ORSetValue(s) => ReplicatedValue.ORSetValue(s.remove(element))
                case _ => throw IllegalArgumentException("${key} 不是 ORSet")
            }
        })
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
