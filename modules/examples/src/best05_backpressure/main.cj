/*
 * CActor Best 05 - 有界邮箱与背压：DropNew / Fail / Block 三种溢出策略
 *
 * 演示（经典 API：Props 上的邮箱配置）：
 * - TypedProps().withBoundedMailbox(n) + blocking 调度器
 * - MailboxOverflow.DropNew：满后丢新消息并计死信（默认）
 * - MailboxOverflow.Fail：满后向发送方抛 MailboxFullException
 * - MailboxOverflow.Block：发送方限速等待（真实背压）
 *
 * 自检：三种策略的丢弃数、死信数、限速效果符合语义。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.best05_backpressure_pipeline
 */
package cactor_examples.best05_backpressure

import std.sync.*
import std.time.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.*
import cactor_core.actor.{MailboxOverflow}

/** 慢消费者：处理一条睡一段时间，并卡在 gate 上以便填满邮箱 */
func slow(delay: Duration, gate: AtomicBool, processed: AtomicInt64): TypedBehavior<Int64> {
    TypedBehaviors.receiveMessage<Int64>({ n =>
        while (!gate.load()) {
            sleep(Duration.millisecond)
        }
        sleep(delay)
        processed.fetchAdd(1)
        TypedBehaviors.same<Int64>()
    })
}

main(): Int64 {
    let system: CActorSystem = CActorSystemFactory.create("best05")
    var ok = true

    // ---------- 1) DropNew：默认策略，满后丢新 ----------
    let gate1 = AtomicBool(false)
    let p1 = AtomicInt64(0)
    let dropRef = system.typedActorOf<Int64>(
        slow(Duration.Zero, gate1, p1), "drop",
        TypedProps().withBoundedMailbox(4).withDispatcher("blocking"))
    for (i in 0..20) {
        dropRef << i
    }
    let deadDrop = system.deadLetterCount()
    gate1.store(true)
    sleep(Duration.millisecond * 100)
    println("[DropNew] 死信 ${deadDrop} 条（容量 4 + 在途，约 15–16 条被丢）")
    ok = ok && deadDrop >= 15 && p1.load() <= 6

    // ---------- 2) Fail：满后向发送方抛异常 ----------
    let gate2 = AtomicBool(false)
    let p2 = AtomicInt64(0)
    let failRef = system.typedActorOf<Int64>(
        slow(Duration.Zero, gate2, p2), "fail",
        TypedProps().withBoundedMailbox(4, MailboxOverflow.Fail).withDispatcher("blocking"))
    var failures = 0
    for (i in 0..20) {
        try {
            failRef << i
        } catch (_: Exception) {
            failures++
        }
    }
    gate2.store(true)
    sleep(Duration.millisecond * 100)
    println("[Fail] 发送方感知失败 ${failures} 次")
    ok = ok && failures >= 15

    // ---------- 3) Block：发送方被限速，零丢失 ----------
    let gate3 = AtomicBool(true)
    let p3 = AtomicInt64(0)
    let blockRef = system.typedActorOf<Int64>(
        slow(Duration.millisecond * 2, gate3, p3), "block",
        TypedProps().withBoundedMailbox(4, MailboxOverflow.Block(Duration.second * 5)).withDispatcher("blocking"))
    let start = MonoTime.now()
    for (i in 0..60) {
        blockRef << i
    }
    let elapsed = MonoTime.now() - start
    println("[Block] 60 条发送耗时 ${elapsed.toMilliseconds()}ms（被限速），死信 ${system.deadLetterCount() - deadDrop} 条增量")
    ok = ok && elapsed >= Duration.millisecond * 80
    let deadline = MonoTime.now() + Duration.second * 5
    while (p3.load() < 60 && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 5)
    }
    ok = ok && p3.load() == 60

    system.terminate()
    println(if (ok) { "best05：通过" } else { "best05：失败" })
    if (ok) { 0 } else { 1 }
}
