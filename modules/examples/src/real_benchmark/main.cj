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
 *   6. 多对 ping-pong 吞吐：protoactor-go 官方 inprocessbenchmark 的写法（对照程序 bench/protoactor）
 *   7. 类型化多对互发（枚举协议，推荐写法；CActor 独有，用来跟踪类型化路径的开销）
 * 场景 6、7 另外输出测量窗口内的进程 CPU 时间（每条消息多少 ns）：
 * 机器上有其他负载时墙钟吞吐波动很大，每条消息的 CPU 时间稳定得多，适合前后对比。
 * 运行：cjpm run --name cactor_examples.real_benchmark
 */
package cactor_examples.real_benchmark

import std.sync.*
import std.time.*
import std.sort.*
import std.collection.*
import std.fs.{File}
import std.convert.*
import std.env.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage, PingMessage, PongMessage}
import cactor_core.context.{ActorContext}
import cactor_core.typed.*
import cactor_core.dsl.{AskInbox}
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

// 经典 ask（E38）：AskInbox.ask 以收件箱为发送方，目标照常 context.reply，不再解包 AskMessage
class EchoActor <: Actor {
    public prop name: String { get() { "EchoActor" } }
    public prop description: String { get() { "echo" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        context.reply(message)
        MessageResult.Handled
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

func envInt(name: String, def: Int64): Int64 {
    match (getVariable(name)) {
        case Some(s) => match (Int64.tryParse(s)) {
            case Some(v) => v
            case None => def
        }
        case None => def
    }
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
    let samples = ArrayList<Int64>()
    for (i in 0..rounds) {
        let t0 = MonoTime.now()
        AskInbox.ask(echo, StringMessage("q")).get(Duration.second * 5) |> ignoreReply
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

// ---------- [6] 多对 ping-pong（与 bench/protoactor 的 [6] 一致）----------

/** 一批消息共用一个对象（与 protoactor 官方写法一致），echo 把它原样发回 sender */
class BatchMsg <: Message {
    let sender: ActorRef
    init(sender: ActorRef) { this.sender = sender }
    public func messageType(): String { "BatchMsg" }
}

class StartMsg <: Message {
    let echo: ActorRef
    init(echo: ActorRef) { this.echo = echo }
    public func messageType(): String { "StartMsg" }
}

class EchoBack <: Actor {
    public prop name: String { get() { "EchoBack" } }
    public prop description: String { get() { "原样发回" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: BatchMsg =>
                m.sender.tell(m)
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

/** 发出一批后等全部回信，再发下一批 */
class PingClient <: Actor {
    var echo: Option<ActorRef> = None
    var remaining: Int64
    let batchSize: Int64
    var inFlight: Int64 = 0
    let finished: AtomicInt64
    init(perClient: Int64, batchSize: Int64, finished: AtomicInt64) {
        this.remaining = perClient
        this.batchSize = batchSize
        this.finished = finished
    }
    public prop name: String { get() { "PingClient" } }
    public prop description: String { get() { "批量往返" } }

    func sendBatch(context: ActorContext): Bool {
        if (remaining == 0) {
            return false
        }
        let e = echo.getOrThrow()
        let m = BatchMsg(context.self)
        for (_ in 0..batchSize) {
            e.tell(m)
        }
        remaining -= batchSize
        inFlight = batchSize
        true
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StartMsg =>
                echo = Some(s.echo)
                sendBatch(context)
                MessageResult.Handled
            case _: BatchMsg =>
                inFlight--
                if (inFlight == 0 && !sendBatch(context)) {
                    finished.fetchAdd(1)
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

/** 本进程累计 CPU 时间（user + sys，毫秒）；读不到 /proc 时为 None */
func processCpuMillis(): Option<Int64> {
    try {
        let text = String.fromUtf8(File.readFrom("/proc/self/stat"))
        // 进程名可能含空格，从最后一个 ')' 之后开始数：utime、stime 是第 14、15 个字段
        let rest = text[(text.lastIndexOf(")").getOrThrow() + 2)..]
        let fields = rest.split(" ")
        Some((Int64.parse(fields[11]) + Int64.parse(fields[12])) * 10)
    } catch (_: Exception) {
        None<Int64>
    }
}

func cpuPerMessage(cpu0: Option<Int64>, total: Int64): String {
    match ((cpu0, processCpuMillis())) {
        case (Some(a), Some(b)) => "，窗口 CPU ${b - a}ms（${(b - a) * 1000000 / total} ns/条）"
        case _ => ""
    }
}

func benchInProc(system: CActorSystem, clients: Int64, perClient: Int64, batch: Int64): Unit {
    let finished = AtomicInt64(0)
    let pairs = Array<(ActorRef, ActorRef)>(clients, { i =>
        (system.actorOf({ => PingClient(perClient, batch, finished) }, "client-${i}"),
         system.actorOf({ => EchoBack() }, "echo-${i}"))
    })
    let cpu0 = processCpuMillis()
    let start = MonoTime.now()
    for ((c, e) in pairs) {
        c.tell(StartMsg(e))
    }
    waitFor({ => finished.load() >= clients })
    let elapsed = MonoTime.now() - start
    let total = clients * perClient * 2
    println("[6] 多对 ping-pong：${clients} 对 × ${perClient} 条（批量 ${batch}），共 ${total} 条，${elapsed.toMilliseconds()}ms → ${perSecond(total, elapsed)} msg/s${cpuPerMessage(cpu0, total)}")
}

// ---------- [7] 类型化多对互发 ----------

enum Volley {
    | Partner(TypedActorRef<Volley>)
    | Ball
}

class VolleyState {
    var partner: Option<TypedActorRef<Volley>> = None
    var left: Int64
    let finished: AtomicInt64
    init(left: Int64, finished: AtomicInt64) {
        this.left = left
        this.finished = finished
    }
}

func volley(st: VolleyState): TypedBehavior<Volley> {
    TypedBehaviors.receiveMessage<Volley>({ m =>
        match (m) {
            case Partner(p) => st.partner = Some(p)
            case Ball =>
                st.left--
                if (st.left > 0) {
                    st.partner.getOrThrow().tell(Volley.Ball)
                } else if (st.left == 0) {
                    st.finished.fetchAdd(1)
                }
        }
        TypedBehaviors.same<Volley>()
    })
}

/** 每对 inFlight 个球来回传；A 收到 perActor 个球后不再回传 */
func benchTypedPairs(system: CActorSystem, pairs: Int64, perActor: Int64, inFlight: Int64): Unit {
    let finished = AtomicInt64(0)
    let as_ = Array<TypedActorRef<Volley>>(pairs, { i => system.typedActorOf<Volley>(volley(VolleyState(perActor, finished)), "volley-a-${i}") })
    // 只有 A 侧计完成：A 收满 perActor 个球时 B 也收到了约 perActor 个
    let ignored = AtomicInt64(0)
    let bs = Array<TypedActorRef<Volley>>(pairs, { i => system.typedActorOf<Volley>(volley(VolleyState(perActor, ignored)), "volley-b-${i}") })
    for (i in 0..pairs) {
        as_[i] << Volley.Partner(bs[i])
        bs[i] << Volley.Partner(as_[i])
    }
    // 等两边都记住对方（Partner 先于 Ball 送达同一个邮箱，但对方的 Partner 可能还没处理）
    sleep(Duration.millisecond * 100)
    let cpu0 = processCpuMillis()
    let start = MonoTime.now()
    for (a in as_) {
        for (_ in 0..inFlight) {
            a << Volley.Ball
        }
    }
    waitFor({ => finished.load() >= pairs })
    let elapsed = MonoTime.now() - start
    let total = pairs * perActor * 2
    println("[7] 类型化多对互发：${pairs} 对 × 每对 ${inFlight} 个球，约 ${total} 条，${elapsed.toMilliseconds()}ms → ${perSecond(total, elapsed)} msg/s${cpuPerMessage(cpu0, total)}")
}

// ---------- [8] 类型化批量往返：与 [6] 完全同结构，仅消息走 TypedActorRef ----------

enum TypedBatch {
    | Start(TypedActorRef<TypedBatch>)
    | Ball
}

class TypedEcho {
    var client: Option<TypedActorRef<TypedBatch>> = None
}

func typedEchoBehavior(st: TypedEcho): TypedBehavior<TypedBatch> {
    TypedBehaviors.receiveMessage<TypedBatch>({ m =>
        match (m) {
            case Start(c) => st.client = Some(c)
            case Ball => if (let Some(c) <- st.client) { c.tell(TypedBatch.Ball) }
        }
        TypedBehaviors.same<TypedBatch>()
    })
}

class TypedClient {
    var echo: Option<TypedActorRef<TypedBatch>> = None
    var remaining: Int64
    var inFlight: Int64 = 0
    let batchSize: Int64
    let finished: AtomicInt64
    init(remaining: Int64, batchSize: Int64, finished: AtomicInt64) {
        this.remaining = remaining
        this.batchSize = batchSize
        this.finished = finished
    }
}

func typedClientSend(st: TypedClient): Bool {
    if (st.remaining <= 0) {
        return false
    }
    if (let Some(e) <- st.echo) {
        for (_ in 0..st.batchSize) { e.tell(TypedBatch.Ball) }
        st.remaining -= st.batchSize
        st.inFlight = st.batchSize
        true
    } else {
        false
    }
}

func typedClientBehavior(st: TypedClient): TypedBehavior<TypedBatch> {
    TypedBehaviors.receiveMessage<TypedBatch>({ m =>
        match (m) {
            case Start(e) =>
                st.echo = Some(e)
                let _ = typedClientSend(st)
            case Ball =>
                st.inFlight--
                if (st.inFlight == 0 && !typedClientSend(st)) {
                    st.finished.fetchAdd(1)
                }
        }
        TypedBehaviors.same<TypedBatch>()
    })
}

func benchTypedBatch(system: CActorSystem, clients: Int64, perClient: Int64, batch: Int64): Unit {
    let finished = AtomicInt64(0)
    let pairs = Array<(TypedActorRef<TypedBatch>, TypedActorRef<TypedBatch>)>(clients, { i =>
        let c = system.typedActorOf<TypedBatch>(typedClientBehavior(TypedClient(perClient, batch, finished)), "tc-${i}")
        let e = system.typedActorOf<TypedBatch>(typedEchoBehavior(TypedEcho()), "te-${i}")
        (c, e)
    })
    let start = MonoTime.now()
    for ((c, e) in pairs) {
        c.tell(TypedBatch.Start(e))
        e.tell(TypedBatch.Start(c))
    }
    let deadline = MonoTime.now() + Duration.second * 30
    while (finished.load() < clients && MonoTime.now() < deadline) { sleep(Duration.microsecond * 50) }
    let elapsed = MonoTime.now() - start
    let total = clients * perClient * 2
    println("[8] 类型化批量往返：${clients} 对 × 批量 ${batch}，共 ${total} 条，${elapsed.toMilliseconds()}ms → ${perSecond(total, elapsed)} msg/s")
}

// ---------- [9] 经典批量往返：与 [8] 完全同结构，Actor 是经典 Actor（对照组）----------

class ClassicEcho <: Actor {
    var client: Option<ActorRef> = None
    public prop name: String { get() { "ClassicEcho" } }
    public prop description: String { get() { "echo" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: Start => client = Some(s.c)
            case _: Ball => if (let Some(c) <- client) { c.tell(Ball()) }
            case _ => ()
        }
        MessageResult.Handled
    }
}

class Ball <: Message {
    public func messageType(): String { "Ball" }
}

class Start <: Message {
    let c: ActorRef
    init(c: ActorRef) { this.c = c }
    public func messageType(): String { "Start" }
}

class ClassicClient <: Actor {
    var echo: Option<ActorRef> = None
    var remaining: Int64
    var inFlight: Int64 = 0
    let batchSize: Int64
    let finished: AtomicInt64
    init(remaining: Int64, batchSize: Int64, finished: AtomicInt64) {
        this.remaining = remaining
        this.batchSize = batchSize
        this.finished = finished
    }
    public prop name: String { get() { "ClassicClient" } }
    public prop description: String { get() { "client" } }
    func send(): Bool {
        if (remaining <= 0) { return false }
        if (let Some(e) <- echo) {
            for (_ in 0..batchSize) { e.tell(Ball()) }
            remaining -= batchSize
            inFlight = batchSize
            true
        } else { false }
    }
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: Start =>
                echo = Some(s.c)
                let _ = send()
            case _: Ball =>
                inFlight--
                if (inFlight == 0 && !send()) { finished.fetchAdd(1) }
            case _ => ()
        }
        MessageResult.Handled
    }
}

func benchClassicBatch(system: CActorSystem, clients: Int64, perClient: Int64, batch: Int64): Unit {
    let finished = AtomicInt64(0)
    let pairs = Array<(ActorRef, ActorRef)>(clients, { i =>
        (system.actorOf({ => ClassicClient(perClient, batch, finished) }, "cc-${i}"),
         system.actorOf({ => ClassicEcho() }, "ce-${i}"))
    })
    let start = MonoTime.now()
    for ((c, e) in pairs) {
        c.tell(Start(e))
        e.tell(Start(c))
    }
    let deadline = MonoTime.now() + Duration.second * 30
    while (finished.load() < clients && MonoTime.now() < deadline) { sleep(Duration.microsecond * 50) }
    let elapsed = MonoTime.now() - start
    let total = clients * perClient * 2
    println("[9] 经典批量往返：${clients} 对 × 批量 ${batch}，共 ${total} 条，${elapsed.toMilliseconds()}ms → ${perSecond(total, elapsed)} msg/s")
}

main(): Int64 {
    println("=== CActor 真实基准（MonoTime 计时）===")
    let system = CActorSystemFactory.create("bench")
    benchSingleActor(system, 1_000_000)
    // 发送方数可用 CACTOR_BENCH_SENDERS 覆盖：用于区分"运行时扇出效率"与"基准自身线程数超过核数"
    // 造成的 CPU 超额订阅（T14 时 8 发送方 + 14 worker = 22 线程 > 14 核）。
    benchFanOut(system, envInt("CACTOR_BENCH_SENDERS", 8), 100, 250_000)
    benchPingPong(system, 20_000)
    benchAsk(system, 20_000)
    benchInProc(system, 64, 100_000, 100)
    benchTypedPairs(system, 64, 50_000, 64)
    benchTypedBatch(system, 64, 100_000, 100)
    benchClassicBatch(system, 64, 100_000, 100)
    system.terminate()
    benchRemote(200_000)
    return 0
}

func ignoreReply(_: Message): Unit {}
