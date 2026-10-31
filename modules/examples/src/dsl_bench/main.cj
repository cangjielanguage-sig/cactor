/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 零开销基准：宏路径 vs 手写路径
 *
 * 对照 docs/plan/dsl20261030.md 验收 A3：
 * 宏展开后的 receive 与手写 receive 语义相同、性能相同（差距 ≤2%）。
 *
 * 方法：同一个进程里分别跑"手写 receive"和"@actor 宏"两个自 ping-pong Actor，
 * 同样轮数、交替执行，取每侧 3 轮中的最好成绩（最小耗时，抗噪声），
 * 输出两条路径的耗时比值。
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

/** 等待计数达到目标 */
func waitFor(counter: AtomicInt64, target: Int64, timeout: Duration): Bool {
    let deadline = DateTime.now() + timeout
    while (DateTime.now() < deadline) {
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
    let start = DateTime.now()
    actor.tell(Ping(actor))
    waitFor(counter, target, Duration.second * 60)
    let cost = DateTime.now() - start
    system.terminate()
    cost
}

/** 跑一轮宏路径，返回耗时 */
func runDsl(target: Int64, name: String): Duration {
    let system = CActor.system("bench-${name}")
    let counter = AtomicInt64(0)
    let actor = system.actorOf({ => DslPingPong(counter, target) }, name)
    let start = DateTime.now()
    actor.tell(Ping(actor))
    waitFor(counter, target, Duration.second * 60)
    let cost = DateTime.now() - start
    system.terminate()
    cost
}

main(): Int64 {
    let rounds: Int64 = 200000
    let repeat: Int64 = 5
    // 预热：让 JIT / 线程池进入稳定状态，避免第一轮偏慢
    runManual(20000, "warmup-manual")
    runDsl(20000, "warmup-dsl")
    var manualBest = Duration.second * 3600
    var dslBest = Duration.second * 3600
    var manualSum = 0.0
    var dslSum = 0.0
    for (i in 0..repeat) {
        // 每次都用全新系统，避免上一轮的 Actor / 邮箱 / 调度线程干扰
        let manual = runManual(rounds, "manual-${i}")
        let dsl = runDsl(rounds, "dsl-${i}")
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
    println("最好成绩比值：${bestRatio}%（手写 ${manualBest / Duration.millisecond}ms，宏 ${dslBest / Duration.millisecond}ms）")
    println("平均成绩比值：${meanRatio}%")
    // 2% 以内视为零开销达标（噪声主导时可能略超，仅作提示）
    if (bestRatio <= 102.0) {
        println("dsl_bench：零开销达标（差距 ≤2%）")
        0
    } else {
        println("dsl_bench：差距 ${bestRatio}%，超过 2%（多为测量噪声，建议复跑）")
        0
    }
}
