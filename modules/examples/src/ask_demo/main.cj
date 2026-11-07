/*
 * CActor - 经典 ActorRef 的 ask（E38 收敛后的唯一经典入口：AskInbox）
 *
 * ask 只有三个入口，各管一段：
 *   - 类型化、Actor 外：TypedAsk.ask<Req, Res>(ref, { r => Req(.., r) }, timeout)   —— 见 best02_ask_roundtrip
 *   - 类型化、Actor 内：ctx.ask(target, factory, timeout) { outcome => ... }        —— 见 best02_ask_roundtrip
 *   - 经典 ActorRef：AskInbox.ask(target, message).get(timeout)                     —— 本示例
 *
 * 经典 ask 不需要目标做任何配合：收件箱就是发送方，目标照常 context.reply(...)。
 * （旧的 cactor_patterns.ask.Ask 要求目标解包 AskMessage、回 AskResponse，已 @Deprecated，2.0.0 移除。）
 *
 * 演示与自检：
 *   1. 请求-回复：AskInbox.ask(...).get(timeout)
 *   2. 超时：tryGet(timeout) 返回 None；get(timeout) 抛 TypedAskTimeoutException
 *   3. 请求消息自带 replyTo（DSL 写法）：askInbox() 填进消息
 *   4. 多线程并发 ask：每个请求一个收件箱，回复不会串
 *
 * 运行：cjpm build && ./target/release/bin/cactor_examples.ask_demo
 */
package cactor_examples.ask_demo

import std.sync.*
import std.time.*
import std.collection.*
import cactor.api.{CActor}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.dsl.{AskInbox, askInbox}
import cactor_core.typed.{TypedAskTimeoutException}

/** 请求消息自带回复地址（DSL 的标准写法） */
class Square <: Message {
    let n: Int64
    let replyTo: ActorRef

    init(n: Int64, replyTo: ActorRef) {
        this.n = n
        this.replyTo = replyTo
    }

    public func messageType(): String {
        "Square"
    }
}

/** 普通经典 Actor：用 context.reply 回复发送方；"silence" 不回复（演示超时） */
class Calculator <: Actor {
    public prop name: String {
        get() { "calculator" }
    }

    public prop description: String {
        get() { "calculator" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage =>
                if (s.getContent() != "silence") {
                    context.reply(StringMessage(s.getContent().toAsciiUpper()))
                }
            case q: Square => q.replyTo << StringMessage("${q.n * q.n}")
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }
}

func textOf(m: Message): String {
    match (m as StringMessage) {
        case Some(s) => s.getContent()
        case None => m.messageType()
    }
}

main(): Int64 {
    let system = CActor.system("ask-demo")
    let calc = system.actorOf({ => Calculator() }, "calculator")
    // 闭包不能捕获可变局部变量再作为值传递，失败计数用原子量
    let failures = AtomicInt64(0)
    let check = { ok: Bool, what: String =>
        println("${if (ok) { "✓" } else { "✗" }} ${what}")
        if (!ok) {
            failures.fetchAdd(1)
        }
        ()
    }

    // 1. 请求-回复
    let reply = AskInbox.ask(calc, StringMessage("hello")).get(Duration.second * 3)
    check(textOf(reply) == "HELLO", "AskInbox.ask 收到回复: ${textOf(reply)}")

    // 2. 超时
    let silent = AskInbox.ask(calc, StringMessage("silence"))
    check(silent.tryGet(Duration.millisecond * 100).isNone(), "不回复的请求 tryGet 超时返回 None")
    var timedOut = false
    try {
        AskInbox.ask(calc, StringMessage("silence")).get(Duration.millisecond * 100) |> ignoreMsg
    } catch (e: TypedAskTimeoutException) {
        timedOut = true
    }
    check(timedOut, "get 超时抛 TypedAskTimeoutException")

    // 3. 请求自带 replyTo
    let inbox = askInbox()
    calc << Square(12, inbox)
    check(textOf(inbox.get(Duration.second * 3)) == "144", "replyTo 写法：12² = 144")

    // 4. 多线程并发 ask
    let threads = 4
    let perThread = 250
    let wrong = AtomicInt64(0)
    let futures = ArrayList<Future<Unit>>()
    for (t in 0..threads) {
        futures.add(spawn {
            for (i in 0..perThread) {
                let word = "w${t}-${i}"
                let r = AskInbox.ask(calc, StringMessage(word)).get(Duration.second * 3)
                if (textOf(r) != word.toAsciiUpper()) {
                    wrong.fetchAdd(1)
                }
            }
        })
    }
    for (f in futures) {
        f.get()
    }
    check(wrong.load() == 0, "${threads} 线程 × ${perThread} 次并发 ask，回复全部对应各自的请求")

    system.terminate()
    if (failures.load() == 0) {
        println("ask 示例：通过")
        0
    } else {
        println("ask 示例：${failures.load()} 项失败")
        1
    }
}

func ignoreMsg(_: Message): Unit {}
