/*
 * 负例：同一消息类型写了两个处理方法 —— 必须在编译期报错。
 * 期望错误包含："多个处理 Inc"
 */
package neg_actor_duplicate

import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_macros.*

class Inc <: Message {
    public func messageType(): String { "Inc" }
}

@actor[cases = [Inc]]
class DuplicateAgent <: Actor {
    func onInc(m: Inc, ctx: ActorContext): MessageResult {
        MessageResult.Handled
    }

    func onIncAgain(m: Inc, ctx: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
