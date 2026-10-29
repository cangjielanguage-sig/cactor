/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 仓颉原语微基准
 *
 * 测量运行时热路径用到的语言与标准库原语的单次开销，作为优化依据
 * （结论见 docs/plan/20261028-performance.md）。
 * 运行：./target/release/bin/cactor_examples.micro_benchmark
 *
 * 注意：
 * - 工作区以 -O2 编译。不逃逸的对象分配、接口调用、类型匹配会被逃逸分析和去虚化消除，
 *   所以分配测试分为“不逃逸”和“逃逸”两种。
 * - 结果单位为 ns/次（队列为 ns/条）。
 */
package cactor_examples.micro_benchmark

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*

open class Msg {
    let v: Int64
    init(v: Int64) { this.v = v }
}

class Msg2 <: Msg {
    init(v: Int64) { super(v) }
}

interface Handler {
    func handle(m: Msg): Int64
}

class H1 <: Handler {
    public func handle(m: Msg): Int64 { m.v }
}

/** 精简 Vyukov MPSC：与运行时 CellMailbox 结构相同 */
class Node<T> {
    var value: Option<T>
    let next: AtomicOptionReference<Node<T>> = AtomicOptionReference<Node<T>>()
    init(v: Option<T>) { value = v }
}

class LeanMPSC<T> {
    private let head: AtomicReference<Node<T>>
    private var tail: Node<T>

    init() {
        let s = Node<T>(None<T>)
        head = AtomicReference<Node<T>>(s)
        tail = s
    }

    func add(v: T): Unit {
        let n = Node<T>(Some(v))
        let p = head.swap(n)
        p.next.store(n)
    }

    func remove(): Option<T> {
        match (tail.next.load()) {
            case Some(n) =>
                tail = n
                let v = n.value
                n.value = None<T>
                v
            case None => None<T>
        }
    }
}

/** 旧 MPSCQueue 的成本结构：额外的容量检查、统计计数和消费者锁 */
class CountedMPSC<T> {
    private let q = LeanMPSC<T>()
    private let enq = AtomicInt64(0)
    private let deq = AtomicInt64(0)
    private let lock = Mutex()

    func add(v: T): Unit {
        if (enq.load() - deq.load() < 16777216) {
            q.add(v)
            enq.fetchAdd(1)
        }
    }

    func remove(): Option<T> {
        synchronized(lock) {
            let r = q.remove()
            if (r.isSome()) {
                deq.fetchAdd(1)
            }
            r
        }
    }
}

func per(t: MonoTime, n: Int64): Float64 {
    Float64((MonoTime.now() - t).toNanoseconds()) / Float64(n)
}

func line(name: String, v: Float64): Unit {
    println("${name}\t${v}")
}

/** P 个生产者、1 个消费者，返回每条消息的平均耗时 */
func multiProducer(name: String, producers: Int64, each: Int64, add: (Msg) -> Unit, remove: () -> Option<Msg>): Unit {
    let m = Msg(1)
    let start = MonoTime.now()
    let fs = ArrayList<Future<Unit>>()
    for (_ in 0..producers) {
        fs.add(spawn {
            for (_ in 0..each) {
                add(m)
            }
        })
    }
    var got: Int64 = 0
    while (got < producers * each) {
        if (remove().isSome()) {
            got++
        }
    }
    for (f in fs) {
        f.get()
    }
    line("${name} ${producers}P1C", per(start, producers * each))
}

func benchLanguage(sinkIn: Int64): Int64 {
    let N = 2_000_000
    var sink = sinkIn
    println("## 语言原语（ns/次）")
    var t = MonoTime.now()
    for (i in 0..N) {
        let m = Msg(i)
        sink += m.v
    }
    line("对象分配（不逃逸）", per(t, N))

    let keep = Array<Option<Msg>>(1024, { _ => None<Msg> })
    t = MonoTime.now()
    for (i in 0..N) {
        if (let Some(o) <- keep[(i + 7) & 1023]) {
            sink += o.v
        }
        keep[i & 1023] = Msg(i)
    }
    line("对象分配（逃逸）", per(t, N))

    let keep2 = Array<Option<(Msg, Option<String>)>>(1024, { _ => None })
    t = MonoTime.now()
    for (i in 0..N) {
        if (let Some(o) <- keep2[(i + 7) & 1023]) {
            sink += o[0].v
        }
        keep2[i & 1023] = (Msg(i), None<String>)
    }
    line("元组 (Msg, Option<String>) 分配（逃逸）", per(t, N))

    let keep3 = Array<Option<AtomicOptionReference<Msg>>>(1024, { _ => None })
    t = MonoTime.now()
    for (i in 0..N) {
        if (let Some(o) <- keep3[(i + 7) & 1023]) {
            if (o.load().isNone()) {
                sink += 1
            }
        }
        keep3[i & 1023] = AtomicOptionReference<Msg>()
    }
    line("AtomicOptionReference 分配（逃逸）", per(t, N))

    let h: Handler = H1()
    let m0: Msg = Msg2(1)
    t = MonoTime.now()
    for (_ in 0..N) {
        sink += h.handle(m0)
    }
    line("接口调用", per(t, N))
    t = MonoTime.now()
    for (_ in 0..N) {
        match (m0) {
            case _: Msg2 => sink += 1
            case _ => ()
        }
    }
    line("match 类型匹配", per(t, N))

    t = MonoTime.now()
    for (i in 0..200_000) {
        let s = "actor-${i} msg"
        sink += s.size
    }
    line("字符串插值", per(t, 200_000))
    sink
}

func benchSync(sinkIn: Int64): Int64 {
    let N = 2_000_000
    var sink = sinkIn
    println("## 同步原语（ns/次）")
    let a = AtomicInt64(0)
    var t = MonoTime.now()
    for (_ in 0..N) {
        a.fetchAdd(1)
    }
    line("AtomicInt64.fetchAdd", per(t, N))
    let b = AtomicBool(true)
    t = MonoTime.now()
    for (_ in 0..N) {
        b.compareAndSwap(false, true)
    }
    line("AtomicBool CAS（失败）", per(t, N))
    t = MonoTime.now()
    for (_ in 0..N) {
        if (!b.load()) {
            b.compareAndSwap(false, true)
        }
    }
    line("AtomicBool 先读后 CAS（已设置）", per(t, N))
    let mu = Mutex()
    t = MonoTime.now()
    for (_ in 0..N) {
        synchronized(mu) {
            sink += 1
        }
    }
    line("Mutex 加解锁（无竞争）", per(t, N))
    sink
}

func benchQueues(): Unit {
    let N = 2_000_000
    let m = Msg(1)
    println("## 队列（ns/条，单线程为一次入队+一次出队）")
    let clq = ConcurrentLinkedQueue<Msg>()
    var t = MonoTime.now()
    for (_ in 0..N) {
        clq.add(m)
        clq.remove()
    }
    line("std ConcurrentLinkedQueue 单线程", per(t, N))
    let lbq = LinkedBlockingQueue<Msg>()
    t = MonoTime.now()
    for (_ in 0..N) {
        lbq.add(m)
        lbq.remove()
    }
    line("std LinkedBlockingQueue 单线程", per(t, N))
    let lean = LeanMPSC<Msg>()
    t = MonoTime.now()
    for (_ in 0..N) {
        lean.add(m)
        lean.remove()
    }
    line("Vyukov MPSC 单线程", per(t, N))
    let counted = CountedMPSC<Msg>()
    t = MonoTime.now()
    for (_ in 0..N) {
        counted.add(m)
        counted.remove()
    }
    line("带统计和消费者锁的 MPSC 单线程", per(t, N))

    let clqB = ConcurrentLinkedQueue<Msg>()
    t = MonoTime.now()
    for (_ in 0..2000) {
        for (_ in 0..1000) {
            clqB.add(m)
        }
        for (_ in 0..1000) {
            clqB.remove()
        }
    }
    line("std ConcurrentLinkedQueue 批量 1000 入/出", per(t, 2_000_000))
    let leanB = LeanMPSC<Msg>()
    t = MonoTime.now()
    for (_ in 0..2000) {
        for (_ in 0..1000) {
            leanB.add(m)
        }
        for (_ in 0..1000) {
            leanB.remove()
        }
    }
    line("Vyukov MPSC 批量 1000 入/出", per(t, 2_000_000))

    let c2 = ConcurrentLinkedQueue<Msg>()
    multiProducer("std ConcurrentLinkedQueue", 8, 250_000, { x => c2.add(x) }, { => c2.remove() })
    let l2 = LeanMPSC<Msg>()
    multiProducer("Vyukov MPSC", 8, 250_000, { x => l2.add(x) }, { => l2.remove() })
    let b2 = LinkedBlockingQueue<Msg>()
    multiProducer("std LinkedBlockingQueue", 8, 250_000, { x => b2.add(x) }, { => b2.tryRemove() })
}

func benchThreads(): Unit {
    println("## 线程与唤醒（ns/次）")
    let S = 50_000
    var t = MonoTime.now()
    for (_ in 0..S) {
        spawn { 1 }.get()
    }
    line("spawn + get（串行）", per(t, S))
    t = MonoTime.now()
    let fs = ArrayList<Future<Int64>>()
    for (_ in 0..S) {
        fs.add(spawn { 1 })
    }
    for (f in fs) {
        f.get()
    }
    line("spawn（批量创建后 get）", per(t, S))

    let R = 50_000
    let req = LinkedBlockingQueue<Int64>()
    let rsp = LinkedBlockingQueue<Int64>()
    spawn {
        while (true) {
            let v = req.remove()
            rsp.add(v)
            if (v < 0) {
                break
            }
        }
    }
    t = MonoTime.now()
    for (i in 0..R) {
        req.add(i)
        rsp.remove()
    }
    line("LinkedBlockingQueue 阻塞往返（2 次唤醒）", per(t, R))
    req.add(-1)
    rsp.remove()

    let x = AtomicInt64(0)
    let y = AtomicInt64(0)
    spawn {
        var last: Int64 = 0
        while (last >= 0) {
            let v = x.load()
            if (v != last) {
                last = v
                y.store(v)
            }
        }
    }
    t = MonoTime.now()
    for (i in 1..=R) {
        x.store(i)
        while (y.load() != i) {}
    }
    line("跨线程自旋往返（原子变量）", per(t, R))
    x.store(-1)

    let cm = Mutex()
    let cond = synchronized(cm) { cm.condition() }
    let turn = AtomicInt64(0)
    spawn {
        for (_ in 0..R) {
            synchronized(cm) {
                while (turn.load() != 1) {
                    cond.wait()
                }
                turn.store(0)
                cond.notifyAll()
            }
        }
    }
    t = MonoTime.now()
    for (_ in 0..R) {
        synchronized(cm) {
            turn.store(1)
            cond.notifyAll()
            while (turn.load() != 0) {
                cond.wait()
            }
        }
    }
    line("Mutex + Condition 往返", per(t, R))
}

main(): Int64 {
    println("=== 仓颉原语微基准 ===")
    var sink: Int64 = 0
    sink = benchLanguage(sink)
    sink = benchSync(sink)
    benchQueues()
    benchThreads()
    println("(sink=${sink})")
    return 0
}
