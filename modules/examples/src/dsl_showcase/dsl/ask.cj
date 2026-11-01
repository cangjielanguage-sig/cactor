/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 对照示例（DSL 写法）：请求-应答
 *
 * 对照对象：dsl_showcase/classic/ask.cj（经典写法）
 */
package cactor_examples.dsl_showcase.dsl

import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult}
import cactor_macros.*

/** 请求-应答协议：Ask 带回复地址，Answer 是应答 */
enum QueryMsg <: Message {
    | Ask(String, ActorRef)
    | Answer(String)

    public func messageType(): String {
        "QueryMsg"
    }
}

/** 收集应答的 Actor */
@actor[protocol = QueryMsg]
class Collector <: Actor {
    func onQueryMsg(m: QueryMsg, ctx: ActorContext): MessageResult {
        match (m) {
            case QueryMsg.Answer(text) =>
                println("[dsl] 收到应答：${text}")
            case QueryMsg.Ask(_, _) =>
                ()
        }
        MessageResult.Handled
    }
}

/** DSL 写法：@actor 生成 receive，协议清单在编译期核对 */
@actor[protocol = QueryMsg]
class ServiceActor <: Actor {
    func onQueryMsg(m: QueryMsg, ctx: ActorContext): MessageResult {
        match (m) {
            case QueryMsg.Ask(text, replyTo) =>
                replyTo << QueryMsg.Answer("答复：${text}")
            case QueryMsg.Answer(_) =>
                ()
        }
        MessageResult.Handled
    }
}
