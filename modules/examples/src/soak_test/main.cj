/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 长时间稳定性测试（soak）
 *
 * 同时运行三类负载，持续给定的秒数：
 *   1. 收发：4 个生产者线程向 200 个顶层 Worker 持续发送消息（在途消息超过 2 万时等待，避免无界积压）；
 *   2. 创建 / 停止：Churn Actor 每 10ms 创建 50 个子 Actor、各发一条消息、再全部停止；
 *   3. 失败重启：约千分之一的消息让 Worker 抛异常，由默认监督策略重启。
 * 按固定间隔在完整 GC 后记录存活堆。
 *
 * 自检（不满足时退出码 1）：
 *   - 普通消息零丢失：处理数 == 发送数；
 *   - 每条故障消息都触发一次重启；
 *   - 子 Actor 全部停止（创建数 == postStop 数）；
 *   - 没有结构性泄漏：预热（前 1/4 时间）后与结束时各暂停一次负载、等在途工作全部完成，
 *     两次静止状态下存活的 Actor Cell 数、登记的顶层 Actor 数必须相同；
 *   - 存活堆没有严重增长：结束时 < 2 × 预热后 + 64MB（只防明显泄漏）。
 *     完整 GC 后的已用堆读数在繁忙机器上波动很大（实测同一程序静止状态 14MB–155MB），不能做精确判定；
 *     长时间运行（tools/ci.sh --soak）时观察采样表的趋势。
 *
 * 运行：cjpm build && ./target/release/bin/cactor_examples.soak_test [秒数，默认 60]
 */
package cactor_examples.soak_test

import std.sync.*
import std.time.*
import std.env.*
import std.convert.*
import std.collection.*
import std.runtime.{gc, getUsedHeapSize}
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}

class Counters {
    let sent = AtomicInt64(0)
    let processed = AtomicInt64(0)
    let boomsSent = AtomicInt64(0)
    let restarts = AtomicInt64(0)
    let childrenCreated = AtomicInt64(0)
    let childrenStopped = AtomicInt64(0)
    let childMessages = AtomicInt64(0)
}

class Worker <: Actor {
    let c: Counters
    var handled: Int64 = 0

    init(c: Counters) {
        this.c = c
    }

    public prop name: String { get() { "worker" } }
    public prop description: String { get() { "worker" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            if (s.getContent() == "boom") {
                throw Exception("soak: 故障注入")
            }
            handled++
            c.processed.fetchAdd(1)
        }
        MessageResult.Handled
    }

    public func postRestart(reason: Exception): Unit {
        c.restarts.fetchAdd(1)
    }
}

class Child <: Actor {
    let c: Counters

    init(c: Counters) {
        this.c = c
    }

    public prop name: String { get() { "child" } }
    public prop description: String { get() { "child" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        c.childMessages.fetchAdd(1)
        MessageResult.Handled
    }

    public func postStop(): Unit {
        c.childrenStopped.fetchAdd(1)
    }
}

class Churn <: Actor {
    let c: Counters
    let paused: AtomicBool
    var round: Int64 = 0

    init(c: Counters, paused: AtomicBool) {
        this.c = c
        this.paused = paused
    }

    public prop name: String { get() { "churn" } }
    public prop description: String { get() { "churn" } }

    public func onStarted(context: ActorContext): Unit {
        context.timers.startTimerWithFixedDelay("churn", StringMessage("tick"), Duration.millisecond * 10)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (paused.load()) {
            return MessageResult.Handled
        }
        round++
        let counters = c
        let kids = Array<ActorRef>(50, { i => context.actorOf({ => Child(counters) }, "r${round}-${i}") })
        c.childrenCreated.fetchAdd(50)
        for (k in kids) {
            k.tell(StringMessage("hi"))
            context.stop(k)
        }
        MessageResult.Handled
    }
}

func liveHeapBytes(): Int64 {
    gc(heavy: true)
    gc(heavy: true)
    Int64(getUsedHeapSize())
}

main(args: Array<String>): Int64 {
    let seconds = if (args.size > 0) { Int64.parse(args[0]) } else { 60 }
    let samples = 12
    let interval = Duration.millisecond * max(500, seconds * 1000 / samples)
    let system = CActorSystemFactory.create("soak")
    let c = Counters()
    let workers = Array<ActorRef>(200, { i => system.actorOf({ => Worker(c) }, "w${i}") })
    let paused = AtomicBool(false)
    let churn = system.actorOf({ => Churn(c, paused) }, "churn")
    let running = AtomicBool(true)
    let producers = ArrayList<Future<Unit>>()
    for (p in 0..4) {
        producers.add(spawn {
            var i: Int64 = p
            while (running.load()) {
                if (paused.load() || c.sent.load() - c.processed.load() > 20000) {
                    sleep(Duration.millisecond)
                    continue
                }
                let w = workers[i % workers.size]
                if (i % 997 == 0) {
                    c.boomsSent.fetchAdd(1)
                    w.tell(StringMessage("boom"))
                } else {
                    c.sent.fetchAdd(1)
                    w.tell(StringMessage("m"))
                }
                i += 4
            }
        })
    }

    let guardian = system.getRuntime().getUserGuardian()
    // 暂停负载并等在途工作全部完成后测存活堆，同时记录存活 Cell 数与顶层 Actor 数
    let cellsAtRest = AtomicInt64(0)
    let topAtRest = AtomicInt64(0)
    let quiescentHeap = { =>
        paused.store(true)
        sleep(Duration.millisecond * 50)
        let until = MonoTime.now() + Duration.second * 30
        while (MonoTime.now() < until &&
            (c.processed.load() < c.sent.load() || c.restarts.load() < c.boomsSent.load() ||
            c.childrenStopped.load() < c.childrenCreated.load())) {
            sleep(Duration.millisecond * 10)
        }
        var h = liveHeapBytes()
        for (_ in 0..2) {
            sleep(Duration.millisecond * 100)
            h = min(h, liveHeapBytes())
        }
        cellsAtRest.store(guardian.liveCellCount())
        topAtRest.store(guardian.getAllUserActors().size)
        paused.store(false)
        h
    }
    let heap = ArrayList<Int64>()
    let start = MonoTime.now()
    println("soak: ${seconds}s, 采样间隔 ${interval.toMilliseconds()}ms")
    println("时间(s)\t存活堆(MB)\t已处理\t子Actor创建\t重启")
    var baseline: Int64 = -1
    while (MonoTime.now() - start < Duration.second * seconds) {
        sleep(interval)
        if (baseline < 0 && MonoTime.now() - start >= Duration.second * seconds / 4) {
            // 前 1/4 时间作为预热：堆在此期间上升到稳定平台（实测 20 秒运行在 2 秒时仍未到平台），之后应保持不变
            baseline = quiescentHeap()
            println("预热后静止状态：存活堆 ${baseline / 1024 / 1024}MB，存活 Cell ${cellsAtRest.load()}，顶层 Actor ${topAtRest.load()}")
        }
        let h = liveHeapBytes()
        heap.add(h)
        println("${(MonoTime.now() - start).toSeconds()}\t${h / 1024 / 1024}\t${c.processed.load()}\t${c.childrenCreated.load()}\t${c.restarts.load()}")
    }
    let cellsBefore = cellsAtRest.load()
    let topBefore = topAtRest.load()
    let finalHeap = quiescentHeap()
    running.store(false)
    for (f in producers) {
        f.get()
    }
    system.stop(churn)
    // 等待在途消息与停止中的子 Actor 处理完
    let deadline = MonoTime.now() + Duration.second * 30
    while (MonoTime.now() < deadline &&
        (c.processed.load() < c.sent.load() || c.restarts.load() < c.boomsSent.load() ||
        c.childrenStopped.load() < c.childrenCreated.load())) {
        sleep(Duration.millisecond * 10)
    }
    let elapsed = (MonoTime.now() - start).toMilliseconds()
    let failures = AtomicInt64(0)
    let check = { cond: Bool, label: String =>
        println("${if (cond) { "通过" } else { "失败" }}：${label}")
        if (!cond) {
            failures.fetchAdd(1)
        }
    }
    check(c.processed.load() == c.sent.load(), "普通消息零丢失（发送 ${c.sent.load()}，处理 ${c.processed.load()}）")
    check(c.restarts.load() == c.boomsSent.load(), "每条故障消息一次重启（故障 ${c.boomsSent.load()}，重启 ${c.restarts.load()}）")
    check(c.childrenStopped.load() == c.childrenCreated.load(),
        "子 Actor 全部停止（创建 ${c.childrenCreated.load()}，停止 ${c.childrenStopped.load()}）")
    check(cellsAtRest.load() == cellsBefore && topAtRest.load() == topBefore,
        "没有结构性泄漏（静止时存活 Cell ${cellsBefore} → ${cellsAtRest.load()}，顶层 Actor ${topBefore} → ${topAtRest.load()}）")
    check(finalHeap < baseline * 2 + 64 * 1024 * 1024,
        "存活堆没有严重增长（预热后 ${baseline / 1024 / 1024}MB，结束时 ${finalHeap / 1024 / 1024}MB）")
    let rate = c.processed.load() * 1000 / max(1, elapsed)
    println("吞吐：${rate} 条/秒；子 Actor 创建+停止：${c.childrenCreated.load() * 1000 / max(1, elapsed)} 个/秒")
    system.terminate()
    if (failures.load() == 0) { 0 } else { 1 }
}
