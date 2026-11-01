/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 对照示例（经典写法）：计数器
 *
 * 对照对象：dsl_showcase/dsl/counter.cj（@actor + 枚举协议）
 */
package cactor_examples.dsl_showcase.classic

import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}

/** 自增消息 */
class Increment <: Message {
    let n: Int64

    init(n: Int64) {
        this.n = n
    }

    public func messageType(): String {
        "Increment"
    }
}

/** 读取计数值消息 */
class GetCount <: Message {
    public func messageType(): String {
        "GetCount"
    }
}

/** 经典写法：手写 receive + match */
class CounterActor <: Actor {
    private var count: Int64 = 0

    public prop name: String {
        get() { "CounterActor" }
    }

    public prop description: String {
        get() { "经典写法的计数器" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: Increment =>
                count += m.n
                MessageResult.Handled
            case _: GetCount =>
                println("[classic] 计数器当前值 = ${count}")
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
}
