/*
 * E43：N0（CRDT 偶发 SIGSEGV）最小复现尝试——只依赖标准库，用 tools/repro/n0_crdt_stress.sh 编译并连跑。
 *
 * 复刻 3e48028 / 4157d01 之前 Replicator 的访问模式：
 *   - 一个"Actor"线程反复：锁内取引用 → 锁外 merge（旧）/ 锁内 merge → 锁内写回；
 *   - 若干读线程：锁内取引用 → 锁外遍历（编码 / 摘要 / merge）。
 * 共享的 ORSet 发布后从不修改（经 Mutex 发布，存在 happens-before），按语言内存模型是无竞争的只读共享。
 * mergeOld 是当年被认为导致崩溃的写法（`?? HashSet()` 临时空集），原样保留。
 *
 * 参数：n0_crdt_stress <模式> <秒数> <读线程数>
 *   模式 old-out   旧 merge + 锁外遍历（3e48028 之前）
 *        old-in    旧 merge + 全部在锁内
 *        new-out   新 merge（Option 分支）+ 锁外遍历（E43 之后的 Replicator）
 *        new-in    新 merge + 全部在锁内（3e48028 / 4157d01 之后）
 *        old-solo  旧 merge，单线程（无共享），只看 GC 压力
 * 结果（2026-11-07，负载 40–55）：old-out / new-out 各 50 轮 × 5 s，四种 GC 配置轮换，100/100 无崩溃。
 */
package crdtstress

import std.collection.*
import std.sync.*
import std.sort.*
import std.time.*
import std.random.*
import std.convert.*

class ORSet {
    let entries: HashMap<String, HashSet<String>>
    let vv: HashMap<String, Int64>

    init() {
        entries = HashMap<String, HashSet<String>>()
        vv = HashMap<String, Int64>()
    }

    init(entries: HashMap<String, HashSet<String>>, vv: HashMap<String, Int64>) {
        this.entries = entries
        this.vv = vv
    }

    func copyEntries(): HashMap<String, HashSet<String>> {
        let m = HashMap<String, HashSet<String>>()
        for ((k, dots) in entries) {
            m[k] = HashSet<String>(dots)
        }
        m
    }

    func add(node: String, element: String): ORSet {
        let seq = (vv.get(node) ?? 0) + 1
        let nextVv = HashMap<String, Int64>(vv)
        nextVv[node] = seq
        let next = copyEntries()
        let dots = HashSet<String>()
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

    func remove(element: String): ORSet {
        if (!entries.contains(element)) {
            return this
        }
        let next = copyEntries()
        next.remove(element)
        ORSet(next, HashMap<String, Int64>(vv))
    }

    static func dotNode(dot: String): String {
        match (dot.lastIndexOf("#")) {
            case Some(i) => dot[0..i]
            case None => dot
        }
    }

    static func dotSeq(dot: String): Int64 {
        match (dot.lastIndexOf("#")) {
            case Some(i) => Int64.parse(dot[(i + 1)..])
            case None => 0
        }
    }

    static func covered(dot: String, v: HashMap<String, Int64>): Bool {
        dotSeq(dot) <= (v.get(dotNode(dot)) ?? 0)
    }

    // 4157d01 之前的写法
    func mergeOld(other: ORSet): ORSet {
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

    // 当前写法
    func mergeNew(other: ORSet): ORSet {
        let next = HashMap<String, HashSet<String>>()
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
            let dots = HashSet<String>()
            if (let Some(m) <- mine) {
                for (d in m) {
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

    func merge(other: ORSet, old: Bool): ORSet {
        if (old) { mergeOld(other) } else { mergeNew(other) }
    }

    func encode(): String {
        let sb = StringBuilder()
        let ks = ArrayList<String>()
        for ((k, _) in entries) {
            ks.add(k)
        }
        let arr = ks.toArray()
        sort(arr)
        for (e in arr) {
            let dots = entries[e].toArray()
            sort(dots)
            sb.append(e)
            sb.append(":")
            for (d in dots) {
                sb.append(d)
                sb.append(",")
            }
            sb.append(";")
        }
        let ns = ArrayList<String>()
        for ((n, _) in vv) {
            ns.add(n)
        }
        let narr = ns.toArray()
        sort(narr)
        for (n in narr) {
            sb.append(n)
            sb.append("=")
            sb.append(vv[n])
        }
        sb.toString()
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

class Store {
    let lock = Mutex()
    let values = HashMap<String, ORSet>()
    let digests = HashMap<String, String>()
}

main(args: Array<String>): Int64 {
    let mode = if (args.size > 0) { args[0] } else { "old-out" }
    let secs = if (args.size > 1) { Int64.parse(args[1]) } else { 10 }
    let readers = if (args.size > 2) { Int64.parse(args[2]) } else { 4 }
    let old = mode.startsWith("old")
    let outside = mode.endsWith("out")
    let keys = 16
    let deadline = MonoTime.now() + Duration.second * secs
    let ops = AtomicInt64(0)

    if (mode == "old-solo" || mode == "new-solo") {
        var a = ORSet()
        var b = ORSet()
        var i = 0
        while (MonoTime.now() < deadline) {
            a = a.add("n1", "e${i % 64}")
            b = b.add("n2", "f${i % 64}")
            if (i % 7 == 0) {
                a = a.remove("e${(i / 7) % 64}")
            }
            let m = a.merge(b, old)
            if (fnv64(m.encode()) == 0) {
                println("?")
            }
            i++
        }
        println("${mode}: ${i} 轮，未崩溃")
        return 0
    }

    let store = Store()
    let stop = AtomicBool(false)
    // "Actor" 线程：本地更新（锁内 / 锁外 merge）+ 模拟收到其他节点状态后合并
    let actor = spawn {
        let rnd = Random(42)
        var i: Int64 = 0
        while (!stop.load()) {
            let k = "set-${i % keys}"
            let current = synchronized(store.lock) { store.values.get(k) ?? ORSet() }
            let incoming = ORSet().add("remote-${rnd.nextUInt64() % 3}", "x-${rnd.nextUInt64() % 50}")
                .add("remote-9", "y-${i % 40}")
            let next = if (outside) {
                let n = current.add("n1", "e-${i % 60}")
                let n2 = if (i % 5 == 0) { n.remove("e-${(i / 5) % 60}") } else { n }
                let _ = current.merge(n2, old)
                n2.merge(incoming, old)
            } else {
                synchronized(store.lock) {
                    let n = current.add("n1", "e-${i % 60}")
                    let n2 = if (i % 5 == 0) { n.remove("e-${(i / 5) % 60}") } else { n }
                    let _ = current.merge(n2, old)
                    n2.merge(incoming, old)
                }
            }
            synchronized(store.lock) {
                store.values[k] = next
                store.digests.remove(k)
            }
            ops.fetchAdd(1)
            i++
        }
    }
    let futures = ArrayList<Future<Unit>>()
    for (r in 0..readers) {
        futures.add(spawn {
            var i: Int64 = r
            while (!stop.load()) {
                let k = "set-${i % keys}"
                let k2 = "set-${(i + 3) % keys}"
                match (i % 3) {
                    // 摘要：锁内（与旧 digestsNow 相同）
                    case 0 => synchronized(store.lock) {
                        for ((key, v) in store.values) {
                            if (store.digests.get(key).isNone()) {
                                store.digests[key] = fnv64(v.encode()).toString()
                            }
                        }
                    }
                    // 编码 / 合并：旧实现在锁外
                    case 1 =>
                        let (a, b) = synchronized(store.lock) { (store.values.get(k), store.values.get(k2)) }
                        if (let (Some(x), Some(y)) <- (a, b)) {
                            if (outside) {
                                let _ = x.merge(y, old).encode()
                            } else {
                                synchronized(store.lock) {
                                    let _ = x.merge(y, old).encode()
                                }
                            }
                        }
                    case _ =>
                        let a = synchronized(store.lock) { store.values.get(k) }
                        if (let Some(x) <- a) {
                            if (outside) {
                                let _ = x.encode()
                            } else {
                                synchronized(store.lock) {
                                    let _ = x.encode()
                                }
                            }
                        }
                }
                ops.fetchAdd(1)
                i += readers
            }
        })
    }
    while (MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 100)
    }
    stop.store(true)
    actor.get()
    for (f in futures) {
        f.get()
    }
    println("${mode}: ${ops.load()} 次操作，未崩溃")
    0
}
