/*
 * 正例（T13）：@actor 类里的生命周期钩子 onStarted(context: ActorContext) 不是消息处理方法，
 * 不能被计入协议清单核对，必须编译通过。
 * 修复前：宏把 onStarted 当成"处理 ActorContext 类型消息"的处理器，报"处理了未在协议清单中声明的类型 ActorContext"
 */
package neg_actor_lifecycle_ok

import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult}
import cactor_macros.*

class Inc <: Message {
    public func messageType(): String { "Inc" }
}

@actor[cases = [Inc]]
class LifecycleAgent <: Actor {
    var started = false

    public func onStarted(context: ActorContext): Unit {
        started = true
    }

    func onInc(m: Inc, ctx: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

main(): Int64 {
    println("编译通过")
    return 0
}
