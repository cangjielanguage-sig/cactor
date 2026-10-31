/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 综合示例：AI Agent 消息协议一屏写完（对照 docs/plan/dsl20261030.md §1.2 G5）
 *
 * 目标：Agent 场景的标准协议（用户输入 / 工具调用 / 工具结果 / 流式增量）
 * 用 @Message + @actor + TypedAsk 同一套 DSL 写完，不需要手写 receive / match 分派。
 *
 * 自检：一轮对话内完成 工具调用 → 流式增量 → 最终答复，结果逐项校验。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.dsl_agent
 */
package cactor_examples.dsl_agent

import std.collection.*
import std.time.*
import cactor.api.CActor
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.*
import cactor_core.dsl.*
import cactor_core.typed.{TypedActorRef}
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
    let tool: String
    let input: String
    let replyTo: ActorRef

    init(tool: String, input: String, replyTo: ActorRef) {
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
    let output: String

    init(output: String) {
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

// ---------- 工具 Actor：收到 ToolCall 就回 ToolResult ----------

@actor[cases = [ToolCall]]
class ToolActor <: Actor {
    func onToolCall(m: ToolCall, ctx: ActorContext): MessageResult {
        m.replyTo << ToolResult("${m.tool}(${m.input}) = 42")
        MessageResult.Handled
    }
}

// ---------- Agent Actor：串行邮箱，一次只做一个决策 ----------

@actor[cases = [UserInput, ToolResult, StreamChunk]]
class AgentActor <: Actor {
    let tool: ActorRef
    let chunks = ArrayList<String>()

    init(tool: ActorRef) {
        this.tool = tool
    }

    func onUserInput(m: UserInput, ctx: ActorContext): MessageResult {
        // 调工具：把收件箱当 replyTo 发出去
        let inbox = askInbox()
        tool << ToolCall("search", m.text, inbox)
        let result = (inbox.get(3.seconds) as ToolResult).getOrThrow()
        // 流式输出增量，再给最终答复
        m.replyTo << StreamChunk("思考中…")
        m.replyTo << StreamChunk(result.output)
        m.replyTo << ToolResult("答案：${m.text} → ${result.output}")
        MessageResult.Handled
    }

    func onToolResult(m: ToolResult, ctx: ActorContext): MessageResult {
        println("[Agent] 最终答复：${m.output}")
        MessageResult.Handled
    }

    func onStreamChunk(m: StreamChunk, ctx: ActorContext): MessageResult {
        chunks.add(m.delta)
        MessageResult.Handled
    }
}

main(): Int64 {
    let system = CActor.system("dsl-agent")
    let tool = system.actorOf({ => ToolActor() }, "tool")
    let agent = system.actorOf({ => AgentActor(tool) }, "agent")

    // 用户 → Agent：收件箱收集流式增量与最终答复
    let inbox = askInbox()
    agent << UserInput("仓颉是什么", inbox)
    sleep(Duration.millisecond * 300)

    var ok = true
    // 收件箱里应是 3 条：StreamChunk("思考中…")、StreamChunk(工具结果)、ToolResult(最终答复)
    ok = ok && inbox.received() == 3
    let c1 = inbox.get(Duration.Zero)
    let c2 = inbox.get(Duration.Zero)
    let c3 = inbox.get(Duration.Zero)
    ok = ok && (c1 as StreamChunk).isSome() && (c1 as StreamChunk).getOrThrow().delta == "思考中…"
    ok = ok && (c2 as StreamChunk).isSome() && (c2 as StreamChunk).getOrThrow().delta.contains("= 42")
    ok = ok && (c3 as ToolResult).isSome() && (c3 as ToolResult).getOrThrow().output.contains("答案：仓颉是什么")
    system.terminate()
    println(if (ok) { "dsl_agent：通过" } else { "dsl_agent：失败" })
    if (ok) { 0 } else { 1 }
}
