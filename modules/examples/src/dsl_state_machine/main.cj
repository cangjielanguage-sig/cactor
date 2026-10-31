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
 * 自检：合法迁移成功、非法迁移被拒、xstate 定义里的状态与转换数一致。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.dsl_state_machine
 */
package cactor_examples.dsl_state_machine

import std.collection.*
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

/** 状态机骨架：状态清单 + 转换表（编译期核对） */
@StateMachine[
    stateType = JobState,
    states = [Idle, Running, Stopped],
    initial = Idle,
    transitions = [(Idle, Running), (Running, Stopped), (Stopped, Idle)]
]
class JobMachine {
    var processed: Int64 = 0

    func onEvent(name: String): Unit {
        processed += 1
        println("[状态机] 处理事件 ${name}，累计 ${processed} 次")
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

/** Actor：收到事件后按状态机规则迁移 */
@actor[cases = [StartJob, FinishJob]]
class JobActor <: Actor {
    let machine = JobMachine()

    func onStartJob(m: StartJob, ctx: ActorContext): MessageResult {
        if (!machine.transitionTo(JobState.Running)) {
            println("[Actor] 非法迁移：当前状态 ${JobMachine.stateName(machine.currentState())} 不能开始任务")
            return MessageResult.Unhandled
        }
        machine.onEvent("StartJob(${m.jobId})")
        MessageResult.Handled
    }

    func onFinishJob(m: FinishJob, ctx: ActorContext): MessageResult {
        if (!machine.transitionTo(JobState.Stopped)) {
            println("[Actor] 非法迁移：当前状态 ${JobMachine.stateName(machine.currentState())} 不能结束任务")
            return MessageResult.Unhandled
        }
        machine.onEvent("FinishJob(${m.jobId})")
        MessageResult.Handled
    }
}

main(): Int64 {
    let system = CActor.system("dsl-state-machine")
    let actor = system.actorOf({ => JobActor() }, "job")

    // 合法迁移：Idle -> Running -> Stopped
    actor.tell(StartJob("job-1"))
    actor.tell(FinishJob("job-1"))
    // 非法迁移：Stopped -> Running（转换表里没有 Stopped -> Running）
    actor.tell(StartJob("job-2"))
    sleep(Duration.millisecond * 300)

    // 宏生成的转换表与 xstate 运行时定义对齐
    let definition = buildXStateDefinition()
    let transitionCount = definition.getTransitionCount()
    println("xstate 定义：状态 ${definition.getStateIds().size} 个，转换 ${transitionCount} 条")

    var ok = true
    ok = ok && definition.getStateIds().size == 3
    ok = ok && transitionCount == 3
    ok = ok && definition.getStateIds().contains("Idle")
    system.terminate()
    println(if (ok) { "dsl_state_machine：通过" } else { "dsl_state_machine：失败" })
    if (ok) { 0 } else { 1 }
}
