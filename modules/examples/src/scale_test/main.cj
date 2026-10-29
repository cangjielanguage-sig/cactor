/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 规模测试（R4）：创建大量 Actor，每个处理一条消息，再全部停止
 *
 * 输出：创建耗时、首条消息全部处理完的耗时、停止耗时、每个 Actor 占用的内存：
 *   - 存活堆：完整 GC 后的已用堆增量 / Actor 数（不含垃圾，最能反映每个 Actor 的真实开销）
 *   - RSS：进程常驻内存增量 / Actor 数（含尚未回收的垃圾和 GC 预留空间）
 * 程序自检：全部消息都被处理、全部 Actor 都停止，否则以退出码 1 结束。
 *
 * 运行：cjpm build && ./target/release/bin/cactor_examples.scale_test [Actor 数，默认 1000000] [top|child]
 *   top：全部创建为顶层 Actor（经过 /user Guardian 与运行时登记）
 *   child（默认）：全部创建为同一个父 Actor 的子 Actor（Akka 应用的常见形态）
 * Actor 很多时需要调大仓颉堆上限，例如 cjHeapSize=8GB。
 */
package cactor_examples.scale_test

import std.sync.*
import std.time.*
import std.fs.*
import std.env.*
import std.convert.*
import std.runtime.{gc, getUsedHeapSize}
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}

class Cell <: Actor {
    let processed: AtomicInt64
    let stopped: AtomicInt64

    init(processed: AtomicInt64, stopped: AtomicInt64) {
        this.processed = processed
        this.stopped = stopped
    }

    public prop name: String {
        get() { "cell" }
    }

    public prop description: String {
        get() { "cell" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        processed.fetchAdd(1)
        MessageResult.Handled
    }

    public func postStop(): Unit {
        stopped.fetchAdd(1)
    }
}

/** 收到 Int64 n 时创建 n 个子 Actor，并把引用交给 sink */
class Parent <: Actor {
    let n: Int64
    let processed: AtomicInt64
    let stopped: AtomicInt64
    let sink: (Array<ActorRef>) -> Unit

    init(n: Int64, processed: AtomicInt64, stopped: AtomicInt64, sink: (Array<ActorRef>) -> Unit) {
        this.n = n
        this.processed = processed
        this.stopped = stopped
        this.sink = sink
    }

    public prop name: String {
        get() { "parent" }
    }

    public prop description: String {
        get() { "parent" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let p = processed
        let s = stopped
        sink(Array<ActorRef>(n, { i => context.actorOf({ => Cell(p, s) }, "c${i}") }))
        MessageResult.Handled
    }
}

class RefsBox {
    var refs: Option<Array<ActorRef>> = None
}

/** 当前进程的常驻内存（KB），读取 /proc/self/status 的 VmRSS */
func rssKb(): Int64 {
    try {
        let text = String.fromUtf8(File.readFrom(Path("/proc/self/status")))
        for (line in text.split("\n")) {
            if (line.startsWith("VmRSS:")) {
                let digits = StringBuilder()
                for (r in line.toRuneArray()) {
                    if (r >= r'0' && r <= r'9') {
                        digits.append(r)
                    }
                }
                return Int64.parse(digits.toString())
            }
        }
    } catch (_: Exception) {}
    0
}

func awaitCount(counter: AtomicInt64, target: Int64, timeout: Duration): Bool {
    let deadline = MonoTime.now() + timeout
    while (counter.load() < target) {
        if (MonoTime.now() > deadline) {
            return false
        }
        sleep(Duration.millisecond)
    }
    true
}

main(args: Array<String>): Int64 {
    let n = if (args.size > 0) { Int64.parse(args[0]) } else { 1_000_000 }
    let asChildren = args.size < 2 || args[1] != "top"
    let system = CActorSystemFactory.create("scale")
    let processed = AtomicInt64(0)
    let stopped = AtomicInt64(0)
    let msg = StringMessage("hi")

    gc(heavy: true)
    let heap0 = getUsedHeapSize()
    let rss0 = rssKb()
    let t0 = MonoTime.now()
    let parentRef: Option<ActorRef>
    let refs = if (asChildren) {
        let box = RefsBox()
        let done = AtomicBool(false)
        let p = system.actorOf({ => Parent(n, processed, stopped, { r =>
            box.refs = Some(r)
            done.store(true)
        }) }, "parent")
        parentRef = Some(p)
        p.tell(msg)
        while (!done.load()) {
            sleep(Duration.millisecond)
        }
        box.refs.getOrThrow()
    } else {
        parentRef = None
        Array<ActorRef>(n, { i => system.actorOf({ => Cell(processed, stopped) }, "c${i}") })
    }
    let created = MonoTime.now() - t0
    let rss1 = rssKb()

    let t1 = MonoTime.now()
    for (r in refs) {
        r.tell(msg)
    }
    let allProcessed = awaitCount(processed, n, Duration.second * 120)
    let delivered = MonoTime.now() - t1
    let rss2 = rssKb()
    gc(heavy: true)
    let heap2 = getUsedHeapSize()

    let t2 = MonoTime.now()
    match (parentRef) {
        // 停止父 Actor 会先停止全部子 Actor
        case Some(p) => system.stop(p)
        case None =>
            for (r in refs) {
                system.stop(r)
            }
    }
    let allStopped = awaitCount(stopped, n, Duration.second * 120)
    let stopping = MonoTime.now() - t2
    system.terminate()

    let perActor = Float64(rss2 - rss0) * 1024.0 / Float64(n)
    println("Actor 数：${n}（${if (asChildren) { "子 Actor" } else { "顶层 Actor" }}）")
    println("创建：${created.toMilliseconds()} ms（${Int64(Float64(n) / (Float64(created.toNanoseconds()) / 1e9))} 个/秒）")
    println("每个处理一条消息：${delivered.toMilliseconds()} ms，已处理 ${processed.load()}")
    println("停止：${stopping.toMilliseconds()} ms，已停止 ${stopped.load()}")
    println("RSS：起始 ${rss0 / 1024} MB，创建后 ${rss1 / 1024} MB，处理后 ${rss2 / 1024} MB")
    println("每个 Actor：存活堆 ${(heap2 - heap0) / n} 字节，RSS ${Int64(perActor)} 字节（均含测试自身保存的 ActorRef 数组）")
    if (allProcessed && allStopped) {
        println("规模测试：通过")
        0
    } else {
        eprintln("规模测试：失败")
        1
    }
}
