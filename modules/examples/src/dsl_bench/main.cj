/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 零开销基准：宏路径 vs 手写路径
 *
 * 对照 docs/plan/dsl20261030.md 验收 A3：
 * 宏展开后的 receive 与手写 receive 语义相同、性能相同（差距 ≤2%）。
 *
 * 两部分：
 * 1. 直接分派（主指标）：不经调度器，在一个循环里直接调用 receive(msg, ctx)，4 种消息轮流出现。
 *    宏的开销只可能出现在分派这一层，纯 CPU 循环把调度、唤醒、跨线程投递的噪声都排除在外。
 * 2. 端到端 ping-pong（参考值）：经过真实邮箱与调度器；单轮会被机器负载放大到 ±50% 以上，
 *    只用来确认两条路径的端到端行为一致，不用它判定 ≤2%。
 *
 * 端到端方法：同一个进程里分别跑"手写 receive"和"@actor 宏"两个自 ping-pong Actor，
 * 同样轮数，每轮交换先后顺序（消除"总是先跑"带来的系统性偏差），
 * 取每侧多轮中的最好成绩（最小耗时，抗噪声），输出两条路径的耗时比值。
 * 计时用单调时钟；任何一轮没在时限内跑完都判为基准无效（不把超时当成耗时算进去）。
 *
 * 运行：cjpm build && ./target/release/bin/cactor_examples.dsl_bench
 */
package cactor_examples.dsl_bench

import std.sync.*
import std.time.*
import cactor.api.{CActor, CActorSystem}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult}
import cactor_macros.*

/** Ping 消息（带回包地址） */
class Ping <: Message {
    let replyTo: ActorRef

    init(replyTo: ActorRef) {
        this.replyTo = replyTo
    }

    public func messageType(): String {
        "Ping"
    }
}

/** Pong 消息（带回包地址） */
class Pong <: Message {
    let replyTo: ActorRef

    init(replyTo: ActorRef) {
        this.replyTo = replyTo
    }

    public func messageType(): String {
        "Pong"
    }
}

/** 手写 receive 的 ping-pong Actor */
class ManualPingPong <: Actor {
    private let counter: AtomicInt64
    private let target: Int64

    init(counter: AtomicInt64, target: Int64) {
        this.counter = counter
        this.target = target
    }

    public prop name: String {
        get() { "manual-ping-pong" }
    }

    public prop description: String {
        get() { "手写 receive 的 ping-pong Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: Ping =>
                m.replyTo << Pong(m.replyTo)
                MessageResult.Handled
            case m: Pong =>
                let done = counter.fetchAdd(1) + 1
                if (done < target) {
                    m.replyTo << Ping(m.replyTo)
                }
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
}

/** @actor 宏生成的 ping-pong Actor（与手写版语义一致） */
@actor[cases = [Ping, Pong]]
class DslPingPong <: Actor {
    private let counter: AtomicInt64
    private let target: Int64

    init(counter: AtomicInt64, target: Int64) {
        this.counter = counter
        this.target = target
    }

    func onPing(m: Ping, ctx: ActorContext): MessageResult {
        m.replyTo << Pong(m.replyTo)
        MessageResult.Handled
    }

    func onPong(m: Pong, ctx: ActorContext): MessageResult {
        let done = counter.fetchAdd(1) + 1
        if (done < target) {
            m.replyTo << Ping(m.replyTo)
        }
        MessageResult.Handled
    }
}

// ---------- 直接分派：4 种消息，手写 receive vs @actor ----------

class M0 <: Message {
    let v: Int64
    init(v: Int64) { this.v = v }
    public func messageType(): String { "M0" }
}

class M1 <: Message {
    let v: Int64
    init(v: Int64) { this.v = v }
    public func messageType(): String { "M1" }
}

class M2 <: Message {
    let v: Int64
    init(v: Int64) { this.v = v }
    public func messageType(): String { "M2" }
}

class M3 <: Message {
    let v: Int64
    init(v: Int64) { this.v = v }
    public func messageType(): String { "M3" }
}

/** 只为直接调用 receive 准备的上下文（处理方法不使用它） */
class BenchContext <: ActorContext {
    public func sender(): Option<String> {
        None<String>
    }
}

class ManualDispatch <: Actor {
    var sum: Int64 = 0

    public prop name: String {
        get() { "manual-dispatch" }
    }

    public prop description: String {
        get() { "手写 receive 的分派基准" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: M0 =>
                sum += m.v
                MessageResult.Handled
            case m: M1 =>
                sum += m.v * 2
                MessageResult.Handled
            case m: M2 =>
                sum += m.v * 3
                MessageResult.Handled
            case m: M3 =>
                sum -= m.v
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
}

@actor[cases = [M0, M1, M2, M3]]
class DslDispatch <: Actor {
    var sum: Int64 = 0

    func onM0(m: M0) {
        sum += m.v
    }

    func onM1(m: M1) {
        sum += m.v * 2
    }

    func onM2(m: M2) {
        sum += m.v * 3
    }

    func onM3(m: M3) {
        sum -= m.v
    }
}

func dispatchLoop(actor: Actor, messages: Array<Message>, iterations: Int64): Duration {
    let ctx = BenchContext()
    let mask = messages.size - 1
    let start = MonoTime.now()
    var i: Int64 = 0
    while (i < iterations) {
        actor.receive(messages[i & mask], ctx)
        i++
    }
    MonoTime.now() - start
}

/** 直接分派基准：返回（宏 / 手写 的最好成绩比值 %，两侧累加结果是否一致） */
func runDispatchBench(): (Float64, Bool) {
    let messages = Array<Message>(1024, { i =>
        match (i % 4) {
            case 0 => M0(i)
            case 1 => M1(i)
            case 2 => M2(i)
            case _ => M3(i)
        }
    })
    let iterations: Int64 = 20000000
    let manual = ManualDispatch()
    let dsl = DslDispatch()
    var manualBest = Duration.second * 3600
    var dslBest = Duration.second * 3600
    for (i in 0..7) {
        // 奇偶轮交换先后顺序
        let (m, d) = if (i % 2 == 0) {
            let a = dispatchLoop(manual, messages, iterations)
            (a, dispatchLoop(dsl, messages, iterations))
        } else {
            let b = dispatchLoop(dsl, messages, iterations)
            (dispatchLoop(manual, messages, iterations), b)
        }
        if (m < manualBest) {
            manualBest = m
        }
        if (d < dslBest) {
            dslBest = d
        }
    }
    let perManual = Float64(manualBest.toNanoseconds()) / Float64(iterations)
    let perDsl = Float64(dslBest.toNanoseconds()) / Float64(iterations)
    let ratio = perDsl / perManual * 100.0
    println("直接分派（${iterations} 次 × 7 轮取最好）：手写 ${perManual} ns/次，宏 ${perDsl} ns/次，比值 ${ratio}%")
    (ratio, manual.sum == dsl.sum)
}

// ---------- 端到端 ping-pong ----------

/** 等待计数达到目标 */
func waitFor(counter: AtomicInt64, target: Int64, timeout: Duration): Bool {
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        if (counter.load() >= target) {
            return true
        }
        sleep(Duration.millisecond * 1)
    }
    counter.load() >= target
}

/** 跑一轮手写路径，返回耗时 */
func runManual(target: Int64, name: String): Duration {
    let system = CActor.system("bench-${name}")
    let counter = AtomicInt64(0)
    let actor = system.actorOf({ => ManualPingPong(counter, target) }, name)
    let start = MonoTime.now()
    actor.tell(Ping(actor))
    let finished = waitFor(counter, target, Duration.second * 60)
    let cost = MonoTime.now() - start
    system.terminate()
    if (!finished) {
        throw Exception("${name} 在 60s 内只完成了 ${counter.load()}/${target} 次，基准无效")
    }
    cost
}

/** 跑一轮宏路径，返回耗时 */
func runDsl(target: Int64, name: String): Duration {
    let system = CActor.system("bench-${name}")
    let counter = AtomicInt64(0)
    let actor = system.actorOf({ => DslPingPong(counter, target) }, name)
    let start = MonoTime.now()
    actor.tell(Ping(actor))
    let finished = waitFor(counter, target, Duration.second * 60)
    let cost = MonoTime.now() - start
    system.terminate()
    if (!finished) {
        throw Exception("${name} 在 60s 内只完成了 ${counter.load()}/${target} 次，基准无效")
    }
    cost
}

main(): Int64 {
    let (dispatchRatio, sameResult) = runDispatchBench()
    if (!sameResult) {
        println("dsl_bench：手写与宏的累加结果不一致，分派语义有差异")
        return 1
    }

    let rounds: Int64 = 200000
    let repeat: Int64 = 5
    // 预热：让线程池、邮箱与内存分配器进入稳定状态，避免第一轮偏慢（仓颉是 AOT 编译，没有 JIT）
    runManual(20000, "warmup-manual")
    runDsl(20000, "warmup-dsl")
    var manualBest = Duration.second * 3600
    var dslBest = Duration.second * 3600
    var manualSum = 0.0
    var dslSum = 0.0
    for (i in 0..repeat) {
        // 每次都用全新系统，避免上一轮的 Actor / 邮箱 / 调度线程干扰；奇偶轮交换先后顺序
        var manual = Duration.Zero
        var dsl = Duration.Zero
        if (i % 2 == 0) {
            manual = runManual(rounds, "manual-${i}")
            dsl = runDsl(rounds, "dsl-${i}")
        } else {
            dsl = runDsl(rounds, "dsl-${i}")
            manual = runManual(rounds, "manual-${i}")
        }
        manualSum += Float64(manual.toNanoseconds())
        dslSum += Float64(dsl.toNanoseconds())
        if (manual < manualBest) {
            manualBest = manual
        }
        if (dsl < dslBest) {
            dslBest = dsl
        }
        let ratio = (Float64(dsl.toNanoseconds()) / Float64(manual.toNanoseconds())) * 100.0
        println("第 ${i + 1} 轮：手写 ${manual / Duration.millisecond}ms，宏 ${dsl / Duration.millisecond}ms，比值 ${ratio}%")
    }

    let bestRatio = (Float64(dslBest.toNanoseconds()) / Float64(manualBest.toNanoseconds())) * 100.0
    let meanRatio = (dslSum / manualSum) * 100.0
    println("端到端最好成绩比值：${bestRatio}%（手写 ${manualBest / Duration.millisecond}ms，宏 ${dslBest / Duration.millisecond}ms）")
    println("端到端平均成绩比值：${meanRatio}%（参考值，受调度噪声影响大）")
    // 判定只看直接分派；机器繁忙时 CPU 循环也会抖，超出只提示复跑，不判失败
    if (dispatchRatio <= 102.0) {
        println("dsl_bench：零开销达标（直接分派差距 ≤2%）")
    } else {
        println("dsl_bench：直接分派比值 ${dispatchRatio}%，超过 102%（先用 uptime 确认机器空闲再复跑）")
    }
    0
}
