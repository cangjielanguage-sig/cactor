/*
 * 负例：协议清单里列了 Inc、Query，但只实现了 onInc —— 必须在编译期报错。
 * 期望错误包含："未处理 Query"
 */
package neg_actor_missing_case

import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_macros.*

class Inc <: Message {
    let n: Int64
    init(n: Int64) { this.n = n }
    public func messageType(): String { "Inc" }
}

class Query <: Message {
    public func messageType(): String { "Query" }
}

@actor[cases = [Inc, Query]]
class MissingCaseAgent <: Actor {
    func onInc(m: Inc, ctx: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
