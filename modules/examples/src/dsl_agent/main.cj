/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 综合示例：AI Agent 消息协议一屏写完（对照 docs/plan/dsl20261030.md §1.2 G5）
 *
 * 目标：Agent 场景的标准协议（用户输入 / 工具调用 / 工具结果 / 流式增量 / 最终答复）
 * 用 @Message + @actor + askInbox 同一套 DSL 写完，不需要手写 receive / match 分派。
 *
 * 关键点：Agent 的处理方法从不阻塞。调工具时把 replyTo 设为 ctx.self，工具结果作为普通消息
 * 回到 Agent 的邮箱，按 requestId 找回那一轮对话再继续。默认调度器是亲和调度且没有工作窃取，
 * 若在处理方法里阻塞等工具（askInbox().get），与工具分在同一工作线程的 Agent 会和工具互相卡死
 * ——Agent 数一多就必然出现；askInbox 在 Actor 线程上阻塞会直接报错，正是为了挡住这种写法。
 * askInbox 只用在 Actor 之外（这里是 main 线程）收集回复。
 *
 * 自检：4 个 Agent 共用 1 个工具，并发 16 轮对话，每轮都按顺序收到
 *       流式增量（思考中）→ 流式增量（工具结果）→ 最终答复；用 CACTOR_DISPATCHER_THREADS=1 跑也能通过。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.dsl_agent
 */
package cactor_examples.dsl_agent

import std.collection.*
import std.time.*
import cactor.api.CActor
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.*
import cactor_core.dsl.*
import cactor_macros.*

// ---------- Agent 消息协议 ----------
// 规则（docs/dsl.md §2.2）：带本地 replyTo 的请求消息用普通类（ActorRef 不可序列化）；
// 可远程的载荷消息用 @Message（自动生成 messageType 与编解码器）。

class UserInput <: Message {
    let text: String
    let replyTo: ActorRef

    init(text: String, replyTo: ActorRef) {
        this.text = text
        this.replyTo = replyTo
    }

    public func messageType(): String {
        "UserInput"
    }
}

class ToolCall <: Message {
    let requestId: Int64
    let tool: String
    let input: String
    let replyTo: ActorRef

    init(requestId: Int64, tool: String, input: String, replyTo: ActorRef) {
        this.requestId = requestId
        this.tool = tool
        this.input = input
        this.replyTo = replyTo
    }

    public func messageType(): String {
        "ToolCall"
    }
}

@Message
class ToolResult <: Message {
    let requestId: Int64
    let output: String

    init(requestId: Int64, output: String) {
        this.requestId = requestId
        this.output = output
    }
}

@Message
class StreamChunk <: Message {
    let delta: String

    init(delta: String) {
        this.delta = delta
    }
}

@Message
class FinalAnswer <: Message {
    let text: String

    init(text: String) {
        this.text = text
    }
}

// ---------- 工具 Actor：收到 ToolCall 就回 ToolResult ----------

@actor[cases = [ToolCall]]
class ToolActor <: Actor {
    func onToolCall(m: ToolCall) {
        m.replyTo << ToolResult(m.requestId, "${m.tool}(${m.input}) = 42")
    }
}

// ---------- Agent Actor：串行邮箱，一次只做一个决策，从不阻塞 ----------

/** 一轮对话在等工具结果期间需要记住的上下文 */
class PendingTurn {
    let question: String
    let user: ActorRef

    init(question: String, user: ActorRef) {
        this.question = question
        this.user = user
    }
}

@actor[cases = [UserInput, ToolResult]]
class AgentActor <: Actor {
    let tool: ActorRef
    let pending = HashMap<Int64, PendingTurn>()
    var nextRequestId: Int64 = 0

    init(tool: ActorRef) {
        this.tool = tool
    }

    func onUserInput(m: UserInput, ctx: ActorContext) {
        nextRequestId++
        pending[nextRequestId] = PendingTurn(m.text, m.replyTo)
        m.replyTo << StreamChunk("思考中…")
        // 工具结果回到自己的邮箱，由 onToolResult 接着处理
        tool << ToolCall(nextRequestId, "search", m.text, ctx.self)
    }

    func onToolResult(m: ToolResult, ctx: ActorContext): MessageResult {
        match (pending.remove(m.requestId)) {
            case Some(turn) =>
                turn.user << StreamChunk(m.output)
                turn.user << FinalAnswer("答案：${turn.question} → ${m.output}")
                MessageResult.Handled
            // 未知或重复的工具结果：交给死信，不影响其他对话
            case None => MessageResult.Unhandled
        }
    }
}

// ---------- 自检（main 线程不是 Actor 线程，可以用 askInbox 阻塞收集回复） ----------

func deltaOf(m: Option<Message>): String {
    match (m) {
        case Some(x) => match (x as StreamChunk) {
            case Some(c) => c.delta
            case None => ""
        }
        case None => ""
    }
}

func answerOf(m: Option<Message>): String {
    match (m) {
        case Some(x) => match (x as FinalAnswer) {
            case Some(a) => a.text
            case None => ""
        }
        case None => ""
    }
}

/** 一轮对话必须按顺序收到：思考中 → 工具结果 → 最终答复 */
func checkTurn(inbox: AskInbox, question: String): Bool {
    let first = deltaOf(inbox.tryGet(Duration.second * 3))
    let second = deltaOf(inbox.tryGet(Duration.second * 3))
    let answer = answerOf(inbox.tryGet(Duration.second * 3))
    first == "思考中…" && second == "search(${question}) = 42" && answer == "答案：${question} → search(${question}) = 42"
}

main(): Int64 {
    let system = CActor.system("dsl-agent")
    let tool = system.actorOf({ => ToolActor() }, "tool")
    let agents = Array<ActorRef>(4, { i => system.actorOf({ => AgentActor(tool) }, "agent-${i}") })

    // 16 轮对话并发进行：每轮一个收件箱，轮流分给 4 个 Agent
    let turns = 16
    let inboxes = Array<AskInbox>(turns, { _ => askInbox() })
    for (i in 0..turns) {
        agents[i % agents.size] << UserInput("问题${i}", inboxes[i])
    }

    var passed = 0
    for (i in 0..turns) {
        if (checkTurn(inboxes[i], "问题${i}")) {
            passed++
        }
    }
    system.terminate()
    println("dsl_agent：${passed}/${turns} 轮对话按顺序收到 流式增量 → 工具结果 → 最终答复")
    let ok = passed == turns
    println(if (ok) { "dsl_agent：通过" } else { "dsl_agent：失败" })
    if (ok) { 0 } else { 1 }
}
