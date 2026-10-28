/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 真实基准测试
 *
 * 全部使用单调时钟 MonoTime 计时，只统计“消息被 Actor 实际处理完成”的时间：
 *   1. 单 Actor tell 吞吐
 *   2. 多发送方 → 多 Actor 扇出吞吐
 *   3. 两个 Actor 之间 ping-pong 往返延迟（P50 / P99 / P99.9）
 *   4. Ask 往返延迟
 *   5. 远程 TCP 吞吐（本机回环）
 * 运行：cjpm run --name cactor_examples.real_benchmark
 */
package cactor_examples.real_benchmark

import std.sync.*
import std.time.*
import std.sort.*
import std.collection.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage, PingMessage, PongMessage}
import cactor_core.context.{ActorContext}
import cactor_patterns.ask.{AskPatternManager, AskMessage, AskResponse}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}

class CountActor <: Actor {
    let counter: AtomicInt64
    init(counter: AtomicInt64) { this.counter = counter }
    public prop name: String { get() { "CountActor" } }
    public prop description: String { get() { "计数" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        counter.fetchAdd(1)
        MessageResult.Handled
    }
}

/** 收到 Ping 后回 Pong 给 peer */
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

/** 收到 Pong 后通知等待方 */
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

class EchoActor <: Actor {
    public prop name: String { get() { "EchoActor" } }
    public prop description: String { get() { "echo" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case ask: AskMessage =>
                ask.getReplyTo().tell(AskResponse(ask.getRequestId(), ask.getOriginalMessage(), true))
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

func waitFor(cond: () -> Bool): Unit {
    while (!cond()) {
        sleep(Duration.microsecond * 50)
    }
}

/** 自旋等待（延迟测量用，避免 sleep 粒度干扰结果） */
func spinFor(cond: () -> Bool): Unit {
    while (!cond()) {}
}

func perSecond(count: Int64, elapsed: Duration): Int64 {
    let ns = elapsed.toNanoseconds()
    if (ns <= 0) { 0 } else { count * 1_000_000_000 / ns }
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

func benchSingleActor(system: CActorSystem, n: Int64): Unit {
    let counter = AtomicInt64(0)
    let ref = system.actorOf({ => CountActor(counter) }, "single")
    let msg = StringMessage("x")
    let start = MonoTime.now()
    for (_ in 0..n) {
        ref.tell(msg)
    }
    let sent = MonoTime.now() - start
    waitFor({ => counter.load() >= n })
    let total = MonoTime.now() - start
    println("[1] 单 Actor 吞吐：${n} 条，发送耗时 ${sent.toMilliseconds()}ms，处理完成 ${total.toMilliseconds()}ms → ${perSecond(n, total)} msg/s")
}

func benchFanOut(system: CActorSystem, senders: Int64, actors: Int64, perSender: Int64): Unit {
    // 每个 Actor 独立计数，避免所有 Actor 争用同一个原子变量
    let counters = Array<AtomicInt64>(actors, { _ => AtomicInt64(0) })
    let refs = Array<ActorRef>(actors, { i => system.actorOf({ => CountActor(counters[i]) }, "fan-${i}") })
    let total = senders * perSender
    let msg = StringMessage("x")
    let start = MonoTime.now()
    let futures = ArrayList<Future<Unit>>()
    for (s in 0..senders) {
        futures.add(spawn {
            for (i in 0..perSender) {
                refs[(s + i) % actors].tell(msg)
            }
        })
    }
    for (f in futures) { f.get() }
    waitFor({ =>
        var sum: Int64 = 0
        for (c in counters) { sum += c.load() }
        sum >= total
    })
    let elapsed = MonoTime.now() - start
    println("[2] 扇出吞吐：${senders} 发送方 × ${actors} Actor，共 ${total} 条，${elapsed.toMilliseconds()}ms → ${perSecond(total, elapsed)} msg/s")
}

func benchPingPong(system: CActorSystem, rounds: Int64): Unit {
    let done = AtomicInt64(-1)
    let pong = PongActor()
    let pongRef = system.actorOf({ => pong }, "pong")
    let pingRef = system.actorOf({ => PingActor(done) }, "ping")
    pong.peer = Some(pingRef)
    let samples = ArrayList<Int64>()
    for (i in 0..rounds) {
        let t0 = MonoTime.now()
        pongRef.tell(PingMessage(i))
        spinFor({ => done.load() == i })
        if (i >= rounds / 10) {   // 前 10% 作为预热
            samples.add((MonoTime.now() - t0).toNanoseconds())
        }
    }
    report("[3] ping-pong 往返延迟", samples)
}

func benchAsk(system: CActorSystem, rounds: Int64): Unit {
    let echo = system.actorOf({ => EchoActor() }, "echo")
    let manager = AskPatternManager(Duration.second * 5)
    let samples = ArrayList<Int64>()
    for (i in 0..rounds) {
        let t0 = MonoTime.now()
        manager.ask(echo, StringMessage("q")).get()
        if (i >= rounds / 10) {
            samples.add((MonoTime.now() - t0).toNanoseconds())
        }
    }
    report("[4] Ask 往返延迟", samples)
}

func benchRemote(n: Int64): Unit {
    let a = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    let b = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    a.start()
    b.start()
    let system = CActorSystemFactory.create("bench-remote")
    let counter = AtomicInt64(0)
    let sink = system.actorOf({ => CountActor(counter) }, "sink")
    b.exposeActor("sink", sink)
    let remote = a.remoteActorOf(b.getLocalAddress(), "sink")
    let msg = StringMessage("payload-0123456789")
    let start = MonoTime.now()
    for (_ in 0..n) {
        remote.tell(msg)
    }
    waitFor({ => counter.load() >= n })
    let elapsed = MonoTime.now() - start
    println("[5] 远程 TCP 吞吐（回环）：${n} 条，${elapsed.toMilliseconds()}ms → ${perSecond(n, elapsed)} msg/s")
    system.terminate()
    a.stop()
    b.stop()
}

main(): Int64 {
    println("=== CActor 真实基准（MonoTime 计时）===")
    let system = CActorSystemFactory.create("bench")
    benchSingleActor(system, 1_000_000)
    benchFanOut(system, 8, 100, 250_000)
    benchPingPong(system, 20_000)
    benchAsk(system, 20_000)
    system.terminate()
    benchRemote(200_000)
    return 0
}
