/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 综合示例：@Message 事件 + @StateMachine 状态机 + @actor
 *
 * 演示（P3.3 / P4.1）：
 * - 事件类型用 @Message 定义：既能当 Actor 消息传，也能被状态机消费（事件类型复用）；
 * - 状态与转换表用 @StateMachine 宏声明：漏状态、写错状态名、非法转换都在编译期报错；
 * - Actor 用 @actor 处理事件，非法状态迁移在运行时被拒绝；
 * - 宏生成的转换表可以直接喂给 xstate 的 StateMachineBuilder（宏 ⇄ 运行时对接）。
 *
 * 自检：合法迁移成功（2 次）、非法迁移被拒（1 次）、最终状态为 Stopped、xstate 定义里的状态与转换数一致。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.dsl_state_machine
 */
package cactor_examples.dsl_state_machine

import std.collection.*
import std.sync.*
import std.time.*
import cactor.api.CActor
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.*
import cactor_macros.*
import cactor_xstate.core.machine.{StateMachineBuilder, StateMachineDefinition, StateNode}
import cactor_xstate.core.types.TransitionDefinition

/** 事件：开始一个任务 */
@Message
class StartJob <: Message {
    let jobId: String

    init(jobId: String) {
        this.jobId = jobId
    }
}

/** 事件：结束一个任务 */
@Message
class FinishJob <: Message {
    let jobId: String

    init(jobId: String) {
        this.jobId = jobId
    }
}

/** 任务状态 */
enum JobState {
    | Idle
    | Running
    | Stopped
}

/**
 * 状态机骨架：状态清单 + 转换表（编译期核对）+ guard/action 覆写（P4.1）
 *
 * 宏为每个转换生成 guard<From><To>()（默认放行）与 onTransition<From><To>()（默认空），
 * 在类体里写同名方法即可覆写（宏检测到已存在就跳过生成）。
 */
@StateMachine[
    stateType = JobState,
    states = [Idle, Running, Stopped],
    initial = Idle,
    transitions = [(Idle, Running), (Running, Stopped), (Stopped, Idle)]
]
class JobMachine {
    var processed: Int64 = 0
    var acceptingJobs: Bool = true

    func onEvent(name: String): Unit {
        processed += 1
        println("[状态机] 处理事件 ${name}，累计 ${processed} 次")
    }

    // 守卫：Idle -> Running 由业务条件决定（false 则迁移被拒，状态不变）
    public func guardIdleRunning(): Bool {
        acceptingJobs
    }

    // 动作：迁移成功后执行
    public func onTransitionRunningStopped(): Unit {
        println("[状态机] 任务收尾")
    }
}

/** 用宏生成的转换表构造 xstate 的状态机定义 */
func buildXStateDefinition(): StateMachineDefinition {
    var builder = StateMachineBuilder()
    builder.withId("job-machine")
    builder.withInitialState(JobMachine.stateName(JobMachine.initialState))
    for (name in JobMachine.stateNames) {
        builder.addState(StateNode.create(name))
    }
    for (source in JobMachine.stateNames) {
        for (target in JobMachine.allowedTargetNames(source)) {
            builder.addTransition(TransitionDefinition.create(source, target, "job-event"))
        }
    }
    builder.build()
}

/** 迁移结果统计（Actor 内写、main 里读，用于自检） */
class JobStats {
    let accepted = AtomicInt64(0)
    let rejected = AtomicInt64(0)
    let endedStopped = AtomicBool(false)
}

/** Actor：收到事件后按状态机规则迁移 */
@actor[cases = [StartJob, FinishJob]]
class JobActor <: Actor {
    let machine = JobMachine()
    let stats: JobStats

    init(stats: JobStats) {
        this.stats = stats
    }

    func onStartJob(m: StartJob, ctx: ActorContext): MessageResult {
        if (!machine.transitionTo(JobState.Running)) {
            println("[Actor] 迁移被拒：当前状态 ${JobMachine.stateName(machine.currentState())} 不能开始任务")
            return reject()
        }
        machine.onEvent("StartJob(${m.jobId})")
        accept()
    }

    func onFinishJob(m: FinishJob, ctx: ActorContext): MessageResult {
        if (!machine.transitionTo(JobState.Stopped)) {
            println("[Actor] 非法迁移：当前状态 ${JobMachine.stateName(machine.currentState())} 不能结束任务")
            return reject()
        }
        machine.onEvent("FinishJob(${m.jobId})")
        accept()
    }

    // 先写最终状态再计数：main 看到计数达标时状态一定已经写好
    private func accept(): MessageResult {
        stats.endedStopped.store(JobMachine.stateName(machine.currentState()) == "Stopped")
        stats.accepted.fetchAdd(1)
        MessageResult.Handled
    }

    private func reject(): MessageResult {
        stats.endedStopped.store(JobMachine.stateName(machine.currentState()) == "Stopped")
        stats.rejected.fetchAdd(1)
        MessageResult.Unhandled
    }
}

/** 等 Actor 处理完 n 个事件（有上限，不会卡死） */
func awaitEvents(stats: JobStats, n: Int64, timeout: Duration): Bool {
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        if (stats.accepted.load() + stats.rejected.load() >= n) {
            return true
        }
        sleep(Duration.millisecond * 5)
    }
    false
}

main(): Int64 {
    let system = CActor.system("dsl-state-machine")
    let stats = JobStats()
    let actor = system.actorOf({ => JobActor(stats) }, "job")

    // 合法迁移：Idle -> Running -> Stopped（guardIdleRunning 放行）
    actor.tell(StartJob("job-1"))
    actor.tell(FinishJob("job-1"))
    // 非法迁移：Stopped -> Running 不在转换表里，在查表这一步就被拒绝（不会走到守卫）
    actor.tell(StartJob("job-2"))
    let processed = awaitEvents(stats, 3, Duration.second * 3)
    println("Actor：迁移成功 ${stats.accepted.load()} 次，被拒 ${stats.rejected.load()} 次，最终停在 Stopped：${stats.endedStopped.load()}")

    // 宏生成的转换表与 xstate 运行时定义对齐
    let definition = buildXStateDefinition()
    let transitionCount = definition.getTransitionCount()
    println("xstate 定义：状态 ${definition.getStateIds().size} 个，转换 ${transitionCount} 条")

    var ok = processed
    ok = ok && stats.accepted.load() == 2
    ok = ok && stats.rejected.load() == 1
    ok = ok && stats.endedStopped.load()
    ok = ok && definition.getStateIds().size == 3
    ok = ok && transitionCount == 3
    ok = ok && definition.getStateIds().contains("Idle")
    system.terminate()
    println(if (ok) { "dsl_state_machine：通过" } else { "dsl_state_machine：失败" })
    if (ok) { 0 } else { 1 }
}
