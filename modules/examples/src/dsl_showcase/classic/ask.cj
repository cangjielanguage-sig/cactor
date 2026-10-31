/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 对照示例（经典写法）：请求-应答
 *
 * 对照对象：dsl_showcase/dsl/ask.cj（@actor + 枚举协议）
 */
package cactor_examples.dsl_showcase.classic

import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult}

/** 请求消息 */
class Query <: Message {
    let text: String
    let replyTo: ActorRef

    init(text: String, replyTo: ActorRef) {
        this.text = text
        this.replyTo = replyTo
    }

    public func messageType(): String {
        "Query"
    }
}

/** 应答消息 */
class Answer <: Message {
    let text: String

    init(text: String) {
        this.text = text
    }

    public func messageType(): String {
        "Answer"
    }
}

/** 收集应答的 Actor */
class Collector <: Actor {
    public prop name: String {
        get() { "Collector" }
    }

    public prop description: String {
        get() { "打印收到的应答" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: Answer =>
                println("[classic] 收到应答：${m.text}")
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
}

/** 经典写法：手写 receive + match */
class ServiceActor <: Actor {
    public prop name: String {
        get() { "ServiceActor" }
    }

    public prop description: String {
        get() { "经典写法的请求-应答服务" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: Query =>
                m.replyTo << Answer("答复：${m.text}")
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
}
