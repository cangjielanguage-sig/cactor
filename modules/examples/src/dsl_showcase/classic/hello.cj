/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 对照示例（经典写法）：Hello World
 *
 * 对照对象：dsl_showcase/dsl/hello.cj（@actor + 枚举协议）
 * 经典写法：每种消息一个类 + 手写 receive + match + name/description 样板。
 */
package cactor_examples.dsl_showcase.classic

import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}

/** 问候消息 */
class Greeting <: Message {
    let text: String

    init(text: String) {
        this.text = text
    }

    public func messageType(): String {
        "Greeting"
    }
}

/** 告别消息 */
class Farewell <: Message {
    let text: String

    init(text: String) {
        this.text = text
    }

    public func messageType(): String {
        "Farewell"
    }
}

/** 经典写法：手写 receive + match */
class HelloActor <: Actor {
    private var count: Int64 = 0

    public prop name: String {
        get() { "HelloActor" }
    }

    public prop description: String {
        get() { "经典写法的 Hello Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: Greeting =>
                count += 1
                println("[classic] 收到 ${m.text}（第 ${count} 次）")
                MessageResult.Handled
            case m: Farewell =>
                count += 1
                println("[classic] 收到 ${m.text}（第 ${count} 次）")
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
}
