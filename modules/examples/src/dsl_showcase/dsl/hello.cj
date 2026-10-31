/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 对照示例（DSL 写法）：Hello World
 *
 * 对照对象：dsl_showcase/classic/hello.cj（经典写法）
 * 枚举协议：一个枚举表达整个协议；protocol 清单在编译期核对；
 * 枚举内部的穷尽性由仓颉 match 在编译期保证。
 */
package cactor_examples.dsl_showcase.dsl

import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_macros.*

/** 问候协议：问候 / 告别 */
enum GreetingMsg <: Message {
    | Hi(String)
    | Bye(String)

    public func messageType(): String {
        "GreetingMsg"
    }
}

/** DSL 写法：@actor 生成 receive，协议清单在编译期核对 */
@actor[protocol = GreetingMsg]
class HelloActor <: Actor {
    private var count: Int64 = 0

    func onGreetingMsg(m: GreetingMsg, ctx: ActorContext): MessageResult {
        count += 1
        match (m) {
            case GreetingMsg.Hi(text) =>
                println("[dsl] 收到 ${text}（第 ${count} 次）")
            case GreetingMsg.Bye(text) =>
                println("[dsl] 收到 ${text}（第 ${count} 次）")
        }
        MessageResult.Handled
    }
}
