/*
 * 负例：协议清单只列了 Inc，却多实现了 onQuery —— 必须在编译期报错。
 * 期望错误包含："未在协议清单中声明的类型 Query"
 */
package neg_actor_extra_handler

import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_macros.*

class Inc <: Message {
    public func messageType(): String { "Inc" }
}

class Query <: Message {
    public func messageType(): String { "Query" }
}

@actor[cases = [Inc]]
class ExtraHandlerAgent <: Actor {
    func onInc(m: Inc, ctx: ActorContext): MessageResult {
        MessageResult.Handled
    }

    func onQuery(m: Query, ctx: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
