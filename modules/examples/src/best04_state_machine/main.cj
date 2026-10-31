/*
 * CActor Best 04 - 行为状态机：Stash + Timers + 返回新行为（热身/暂停）
 *
 * 演示：
 * - withStash：初始化完成前暂存消息，完成后 unstashAll 依次处理
 * - withTimers：startTimerWithFixedDelay 周期唤醒
 * - 状态机：idle ⇄ running 两个状态，状态即行为
 *
 * 自检：暂存的消息在解锁后按原顺序全部处理，定时器触发暂停/恢复。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.best04_state_machine
 */
package cactor_examples.best04_state_machine

import std.time.*
import std.collection.*
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.*

enum JobMsg {
    | Submit(String, TypedActorRef<String>)
    | Unlock          // 初始化完成
    | Pause           // 定时器：暂停
    | Resume          // 定时器：恢复
}

func machine(): TypedBehavior<JobMsg> {
    TypedBehaviors.withStash<JobMsg>(100, { stash =>
        // 初始状态：locked——只接受 Unlock，其余暂存
        TypedBehaviors.receive<JobMsg>({ ctx, m =>
            match (m) {
                case JobMsg.Unlock =>
                    println("初始化完成，释放暂存消息")
                    stash.unstashAll(running(false, ctx))
                case other =>
                    stash.stash(other)
                    TypedBehaviors.same<JobMsg>()
            }
        })
    })
}

func running(paused: Bool, ctx: TypedActorContext<JobMsg>): TypedBehavior<JobMsg> {
    TypedBehaviors.withTimers<JobMsg>({ timers =>
        timers.startSingleTimer("pause", JobMsg.Pause, Duration.millisecond * 30)
        TypedBehaviors.receive<JobMsg>({ c, m =>
            match (m) {
                case JobMsg.Submit(job, replyTo) =>
                    if (paused) {
                        println("（暂停中，丢弃 ${job}）")
                        replyTo.tell("paused:${job}")
                    } else {
                        println("处理 ${job}")
                        replyTo.tell("done:${job}")
                    }
                    TypedBehaviors.same<JobMsg>()
                case JobMsg.Pause =>
                    if (!paused) {
                        println("定时器：暂停")
                        timers.startSingleTimer("resume", JobMsg.Resume, Duration.millisecond * 30)
                        return running(true, c)
                    }
                    TypedBehaviors.same<JobMsg>()
                case JobMsg.Resume =>
                    if (paused) {
                        println("定时器：恢复")
                        timers.startSingleTimer("pause", JobMsg.Pause, Duration.millisecond * 30)
                        return running(false, c)
                    }
                    TypedBehaviors.same<JobMsg>()
                case JobMsg.Unlock => TypedBehaviors.same<JobMsg>()
            }
        })
    })
}

main(): Int64 {
    let system = CActorSystemFactory.create("best04")
    let ref = system.typedActorOf<JobMsg>(machine(), "machine")

    // Unlock 之前的提交被暂存
    let probe = TypedAsk.ask<JobMsg, String>(ref, { r => JobMsg.Submit("job-1", r) }, Duration.second * 3)
    let probe2 = TypedAsk.ask<JobMsg, String>(ref, { r => JobMsg.Submit("job-2", r) }, Duration.second * 3)
    ref << JobMsg.Unlock

    var ok = true
    try {
        let r1 = probe.get(Duration.second * 3)
        let r2 = probe2.get(Duration.second * 3)
        println("回复：${r1} / ${r2}")
        ok = ok && r1 == "done:job-1" && r2 == "done:job-2"
    } catch (e: Exception) {
        println("ask 失败：${e.message}")
        ok = false
    }

    // 等待定时器完成一轮 暂停→恢复
    sleep(Duration.millisecond * 150)
    system.terminate()
    println(if (ok) { "best04：通过" } else { "best04：失败" })
    if (ok) { 0 } else { 1 }
}
