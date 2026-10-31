/*
 * CActor Best 10 - AI Agent 工作流：Agent + 工具 Actor + Ask + 监督
 *
 * 演示（CActor 作为 Agent 运行时的典型形态）：
 * - AgentActor：串行邮箱保证一次只做一个决策，上下文通过行为参数携带
 * - ToolActor：把外部调用包成 Actor，失败抛异常交给监督策略（重启恢复）
 * - ctx.ask：等待工具结果（不阻塞邮箱），结果经 mapResponse 回到自己
 * - 长时间工具调用由 blocking 调度器承载，不占住默认线程池
 *
 * 自检：三轮对话完成、工具故障被恢复、最终回复正确。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.best10_agent_workflow
 */
package cactor_examples.best10_agent_workflow

import std.sync.*
import std.time.*
import std.collection.*
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.*

// ======================= 协议 =======================

/** 用户 → Agent */
enum AgentMsg {
    | UserInput(String, TypedActorRef<String>)
    | ToolResult(Int64)                  // 工具返回的字符数
    | ToolFailed
}

/** Agent → 工具 */
enum ToolMsg {
    | Call(String, TypedActorRef<Int64>)
}

// ======================= 工具 Actor：前 2 次调用失败，之后成功 =======================

class FlakyTool <: Exception {
    init() { super("工具暂时不可用") }
}

/** 包级计数：Actor 重启（setup 重新执行）后仍累计，模拟“前两次调用失败”的外部依赖 */
let TOOL_CALLS = AtomicInt64(0)

func flakyTool(): TypedBehavior<ToolMsg> {
    TypedBehaviors.supervise(
        TypedBehaviors.receive<ToolMsg>({ ctx, m =>
            match (m) {
                case ToolMsg.Call(job, replyTo) =>
                    let calls = TOOL_CALLS.fetchAdd(1) + 1
                    if (calls <= 2) {
                        println("  [tool] 第 ${calls} 次调用失败（监督重启）")
                        throw FlakyTool()
                    }
                    println("  [tool] 第 ${calls} 次调用成功")
                    replyTo.tell(Int64(job.size))
                    TypedBehaviors.same<ToolMsg>()
            }
        })
    ).onFailure(TypedSupervisorStrategy.restart().withLimit(5, Duration.second * 10))
}

// ======================= Agent：决策循环 =======================

/** 工具引用（main 中创建后注入，避免 Agent 内重复创建子 Actor） */
class ToolBox {
    var tool: TypedActorRef<ToolMsg>
    init(t: TypedActorRef<ToolMsg>) { this.tool = t }
}

class PendingBox {
    var asker: TypedActorRef<String>
    init(a: TypedActorRef<String>) { this.asker = a }
}

/** pending：Some 表示正在等待工具应答，此间忽略新输入 */
func agent(tool: ToolBox, pending: Option<PendingBox>, done: AtomicInt64): TypedBehavior<AgentMsg> {
    TypedBehaviors.receive<AgentMsg>({ ctx, m =>
        match (m) {
            case AgentMsg.UserInput(q, replyTo) =>
                if (pending.isSome()) {
                    println("  [agent] 忙碌，丢弃输入（生产环境应改为有界邮箱或暂存）")
                    TypedBehaviors.same<AgentMsg>()
                } else {
                    println("  [agent] 收到输入「${q}」，调用工具 analyze")
                    ctx.ask<ToolMsg, Int64>(tool.tool, { r => ToolMsg.Call(q, r) }, Duration.second * 2, { o =>
                        match (o) {
                            case AskOutcome.Replied(n) => AgentMsg.ToolResult(n)
                            case AskOutcome.Failed(_) => AgentMsg.ToolFailed
                        }
                    })
                    agent(tool, Some(PendingBox(replyTo)), done)
                }
            case AgentMsg.ToolResult(n) =>
                let answer = "「analyze」结果：${n} 个字符"
                println("  [agent] ${answer}")
                if (let Some(p) <- pending) {
                    p.asker.tell(answer)
                }
                done.fetchAdd(1)
                agent(tool, None<PendingBox>, done)
            case AgentMsg.ToolFailed =>
                println("  [agent] 工具失败，降级回答")
                if (let Some(p) <- pending) {
                    p.asker.tell("工具暂不可用，请稍后再试")
                }
                agent(tool, None<PendingBox>, done)
        }
    })
}

// ======================= 主程序 =======================

main(): Int64 {
    let system = CActorSystemFactory.create("best10")
    let done = AtomicInt64(0)

    // 工具在系统层创建：独立于 Agent 的生命周期，失败由 supervise 重启
    let toolRef = system.typedActorOf<ToolMsg>(flakyTool(), "tool")
    let box = ToolBox(toolRef)

    let agentRef = system.typedActorOf<AgentMsg>(agent(box, None<PendingBox>, done), "agent")

    // 三轮对话：前两轮工具失败被监督恢复，第三轮成功
    let answers = ArrayList<String>()
    let lock = Mutex()
    for (i in 1..=3) {
        let p = TypedAsk.ask<AgentMsg, String>(agentRef, { r => AgentMsg.UserInput("问题${i}", r) }, Duration.second * 5)
        try {
            let a = p.get(Duration.second * 5)
            synchronized(lock) { answers.add(a) }
        } catch (e: Exception) {
            synchronized(lock) { answers.add("超时：${e.message}") }
        }
    }
    for (a in answers) {
        println("回复：${a}")
    }

    let ok = answers.size == 3 &&
        answers[0] == "工具暂不可用，请稍后再试" &&
        answers[1] == "工具暂不可用，请稍后再试" &&
        answers[2].startsWith("「analyze」结果") &&
        done.load() == 1

    system.terminate()
    println(if (ok) { "best10：通过" } else { "best10：失败" })
    if (ok) { 0 } else { 1 }
}
