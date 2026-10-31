/*
 * CActor Best 02 - 请求-响应：外部 TypedAsk、Actor 内 ctx.ask、超时
 *
 * 演示：
 * - 外部线程：TypedAsk.ask<Cmd, R>(ref, factory, timeout) 阻塞取结果
 * - Actor 内部：ctx.ask 把响应经 mapResponse 转成自己的消息，不阻塞邮箱
 * - 超时语义：不回复的目标在超时后以 TypedAskTimeoutException 失败
 *
 * 自检：正常应答、超时分支都符合预期。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.best02_ask_roundtrip
 */
package cactor_examples.best02_ask_roundtrip

import std.collection.*
import std.time.*
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.*

enum QueryMsg {
    | Query(String, TypedActorRef<String>)
    | Ignore
}

enum RunnerMsg {
    | Done(String)
    | Timeout
}

/** 大小写反转应答 */
func echo(): TypedBehavior<QueryMsg> {
    TypedBehaviors.receiveMessage<QueryMsg>({ m =>
        match (m) {
            case Query(text, replyTo) =>
                replyTo.tell(text)
                TypedBehaviors.same<QueryMsg>()
            case Ignore => TypedBehaviors.same<QueryMsg>()
        }
    })
}

/** 沉默的 Actor：从不回复，用于验证超时 */
func silent(): TypedBehavior<QueryMsg> {
    TypedBehaviors.receiveMessage<QueryMsg>({ _ => TypedBehaviors.same<QueryMsg>() })
}

main(): Int64 {
    let system = CActorSystemFactory.create("best02")
    let echoRef = system.typedActorOf<QueryMsg>(echo(), "echo")
    let silentRef = system.typedActorOf<QueryMsg>(silent(), "silent")

    // 1) 外部 ask：阻塞等待
    var ok = true
    try {
        let r1 = TypedAsk.ask<QueryMsg, String>(echoRef, { r => Query("ping", r) }, Duration.second * 3)
            .get(Duration.second * 3)
        println("外部 ask：${r1}")
        ok = ok && r1 == "ping"
    } catch (e: Exception) {
        println("外部 ask 失败：${e.message}")
        ok = false
    }

    // 2) Actor 内 ask：响应转换为自己的消息
    let results = ArrayList<String>()
    system.typedActorOf<RunnerMsg>(TypedBehaviors.setup<RunnerMsg>({ ctx =>
        ctx.ask<QueryMsg, String>(echoRef, { r => Query("hello", r) }, Duration.second * 3, { o =>
            match (o) {
                case AskOutcome.Replied(text) => RunnerMsg.Done("ctx-ask:${text}")
                case AskOutcome.Failed(_) => RunnerMsg.Timeout
            }
        })
        ctx.ask<QueryMsg, String>(silentRef, { r => Query("x", r) }, Duration.millisecond * 50, { o =>
            match (o) {
                case AskOutcome.Replied(_) => RunnerMsg.Done("不应到达")
                case AskOutcome.Failed(_) => RunnerMsg.Timeout
            }
        })
        TypedBehaviors.receiveMessage<RunnerMsg>({ m =>
            results.add(match (m) {
                case RunnerMsg.Done(s) => s
                case RunnerMsg.Timeout => "超时"
            })
            TypedBehaviors.same<RunnerMsg>()
        })
    }), "runner")

    // 3) 外部 ask 超时
    var timedOut = false
    try {
        TypedAsk.ask<QueryMsg, String>(silentRef, { r => Query("x", r) }, Duration.millisecond * 50)
            .get(Duration.second * 3)
    } catch (_: Exception) {
        timedOut = true
    }
    println("外部 ask 超时：${timedOut}")
    ok = ok && timedOut

    // 等待 ctx.ask 的两个结果
    let deadline = MonoTime.now() + Duration.second * 3
    while (results.size < 2 && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 5)
    }
    println("ctx.ask 结果：${results.size} 条")
    ok = ok && results.contains("ctx-ask:hello") && results.contains("超时")

    system.terminate()
    println(if (ok) { "best02：通过" } else { "best02：失败" })
    if (ok) { 0 } else { 1 }
}
