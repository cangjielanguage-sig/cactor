/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 规模延迟基准：N 个空闲 Actor 常驻时，一对 Actor 的 ping-pong 延迟
 *
 * 与 real_benchmark [3]（空系统）对照，回答“大量 Actor 常驻是否拖慢消息路径”。
 * 对照程序：bench/protoactor（spawn 模式）、bench/akka（spawn 模式），三者顺序一致：
 *   创建 N 个 Actor → 常驻状态下 ping-pong 5000 次 → 结束
 * 运行：cjpm build && ./target/release/bin/cactor_examples.scale_latency [Actor 数，默认 1000000]
 * Actor 很多时需要调大仓颉堆上限，例如 cjHeapSize=8GB。
 */
package cactor_examples.scale_latency

import std.sync.*
import std.time.*
import std.sort.*
import std.convert.*
import std.collection.*
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, PingMessage, PongMessage}

/** 空闲 Actor：常驻占内存，不参与消息路径 */
class Cell <: Actor {
    public prop name: String { get() { "cell" } }
    public prop description: String { get() { "空闲占位" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

class PongActor <: Actor {
    var peer: Option<ActorRef> = None
    public prop name: String { get() { "PongActor" } }
    public prop description: String { get() { "pong" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case p: PingMessage =>
                if (let Some(r) <- peer) {
                    r.tell(PongMessage(p.getId()))
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

class PingActor <: Actor {
    let done: AtomicInt64
    init(done: AtomicInt64) { this.done = done }
    public prop name: String { get() { "PingActor" } }
    public prop description: String { get() { "ping" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case p: PongMessage =>
                done.store(p.getOriginalId())
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

class RefsBox {
    var refs: Option<Array<ActorRef>> = None
}

/** 收到消息时创建 n 个子 Actor，全部常驻 */
class Parent <: Actor {
    let n: Int64
    let box: RefsBox
    let done: AtomicBool
    init(n: Int64, box: RefsBox, done: AtomicBool) {
        this.n = n
        this.box = box
        this.done = done
    }
    public prop name: String { get() { "parent" } }
    public prop description: String { get() { "批量创建" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        box.refs = Some(Array<ActorRef>(n, { i => context.actorOf({ => Cell() }, "c${i}") }))
        done.store(true)
        MessageResult.Handled
    }
}

func percentile(sorted: Array<Int64>, p: Float64): Float64 {
    let idx = Int64(Float64(sorted.size - 1) * p)
    Float64(sorted[idx]) / 1000.0
}

func report(title: String, samples: ArrayList<Int64>): Unit {
    let arr = samples.toArray()
    sort(arr)
    var sum: Int64 = 0
    for (v in arr) { sum += v }
    println("  ${title}: n=${arr.size} 平均=${Float64(sum) / Float64(arr.size) / 1000.0}µs P50=${percentile(arr, 0.5)}µs P99=${percentile(arr, 0.99)}µs P99.9=${percentile(arr, 0.999)}µs 最大=${Float64(arr[arr.size - 1]) / 1000.0}µs")
}

main(args: Array<String>): Int64 {
    let n = if (args.size > 0) { Int64.parse(args[0]) } else { 1_000_000 }
    println("=== CActor 规模延迟基准（常驻 ${n} 个 Actor）===")
    let system = CActorSystemFactory.create("scale-latency")
    let box = RefsBox()
    let done = AtomicBool(false)
    let parent = system.actorOf({ => Parent(n, box, done) }, "parent")
    let t0 = MonoTime.now()
    parent.tell(StringMessage0())
    while (!done.load()) {
        sleep(Duration.millisecond)
    }
    let created = MonoTime.now() - t0
    println("创建：${created.toMilliseconds()} ms（${Int64(Float64(n) / (Float64(created.toNanoseconds()) / 1e9))} 个/秒）")

    // 常驻状态下的一对 ping-pong（与 real_benchmark [3] 同协议）
    let finished = AtomicInt64(-1)
    let pong = PongActor()
    let pongRef = system.actorOf({ => pong }, "pong")
    let pingRef = system.actorOf({ => PingActor(finished) }, "ping")
    pong.peer = Some(pingRef)
    let samples = ArrayList<Int64>()
    let rounds: Int64 = 5000
    for (i in 0..rounds) {
        let t1 = MonoTime.now()
        pongRef.tell(PingMessage(i))
        while (finished.load() != i) {}
        if (i >= rounds / 10) {
            samples.add((MonoTime.now() - t1).toNanoseconds())
        }
    }
    report("[3s] 常驻 ${n} Actor 时 ping-pong 往返延迟", samples)

    system.stop(parent)
    system.terminate()
    return 0
}

/** 触发 Parent 创建的空消息 */
class StringMessage0 <: Message {
    public func messageType(): String { "StringMessage0" }
}
