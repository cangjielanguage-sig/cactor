/*
 * 正例对照：协议清单与处理方法一致 —— 必须编译通过。
 */
package neg_actor_ok

import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_macros.*

class Inc <: Message {
    public func messageType(): String { "Inc" }
}

class Query <: Message {
    public func messageType(): String { "Query" }
}

@actor[cases = [Inc, Query]]
class OkAgent <: Actor {
    func onInc(m: Inc, ctx: ActorContext): MessageResult {
        MessageResult.Handled
    }

    func onQuery(m: Query, ctx: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

main(): Int64 {
    println("编译通过")
    return 0
}
