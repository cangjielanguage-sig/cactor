/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 对照示例（DSL 写法）：计数器
 *
 * 对照对象：dsl_showcase/classic/counter.cj（经典写法）
 * cases 清单列出协议里的两种消息，漏掉任何一种都会在编译期报错。
 */
package cactor_examples.dsl_showcase.dsl

import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_macros.*

/** 计数器协议：自增 / 读取 */
enum CounterMsg <: Message {
    | Increment(Int64)
    | GetCount

    public func messageType(): String {
        "CounterMsg"
    }
}

/** DSL 写法：@actor 生成 receive，协议清单在编译期核对 */
@actor[protocol = CounterMsg]
class CounterActor <: Actor {
    private var count: Int64 = 0

    func onCounterMsg(m: CounterMsg, ctx: ActorContext): MessageResult {
        match (m) {
            case CounterMsg.Increment(n) =>
                count += n
            case CounterMsg.GetCount =>
                println("[dsl] 计数器当前值 = ${count}")
        }
        MessageResult.Handled
    }
}
