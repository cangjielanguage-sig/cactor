/*
 * 负例：属性键不认识 —— 必须在编译期报错。
 * 期望错误包含："不认识属性 protocolx"
 */
package neg_actor_unknown_attr

import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_macros.*

class Inc <: Message {
    public func messageType(): String { "Inc" }
}

@actor[protocolx = Inc]
class UnknownAttrAgent <: Actor {
    func onInc(m: Inc, ctx: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
