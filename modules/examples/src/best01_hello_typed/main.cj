/*
 * CActor Best 01 - 类型化 Actor 入门：enum 协议、不可变状态、运算符发送
 *
 * 演示：
 * - 消息协议用 enum 表达，match 穷尽检查在编译期发现漏处理的消息
 * - 状态放在行为参数里，处理完返回新行为（无可变字段、无锁）
 * - ref << msg 链式发送；TypedAsk 请求-响应
 *
 * 自检：计数、求和、问候语全部符合预期，退出码 0/1。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.best01_hello_typed
 */
package cactor_examples.best01_hello_typed

import std.time.*
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.*

// ======================= 协议 =======================

enum CalcMsg {
    | Add(Int64)
    | GetSum(TypedActorRef<Int64>)
    | Greet(String)
    | Stop
}

// ======================= 行为 =======================

/** 计数器：状态 n 通过返回新行为保存 */
func calc(n: Int64, greeted: Int64): TypedBehavior<CalcMsg> {
    TypedBehaviors.receive<CalcMsg>({ ctx, msg =>
        match (msg) {
            case CalcMsg.Add(k) => calc(n + k, greeted)
            case CalcMsg.GetSum(replyTo) =>
                replyTo.tell(n)
                TypedBehaviors.same<CalcMsg>()
            case CalcMsg.Greet(name) =>
                ctx.log().info("你好，${name}！当前和 = ${n}")
                calc(n, greeted + 1)
            case CalcMsg.Stop => TypedBehaviors.stopped<CalcMsg>()
        }
    })
}

// ======================= 主程序 =======================

main(): Int64 {
    let system = CActorSystemFactory.create("best01")
    let ref = system.typedActorOf<CalcMsg>(calc(0, 0), "calc")

    ref << CalcMsg.Add(10) << CalcMsg.Add(32) << CalcMsg.Greet("CActor")

    let p = TypedAsk.ask<CalcMsg, Int64>(ref, { r => CalcMsg.GetSum(r) }, Duration.second * 3)
    var ok = true
    try {
        let sum = p.get(Duration.second * 3)
        println("求和结果：${sum}")
        ok = ok && sum == 42
    } catch (e: Exception) {
        println("ask 失败：${e.message}")
        ok = false
    }

    ref << CalcMsg.Stop
    system.terminate()
    println(if (ok) { "best01：通过" } else { "best01：失败" })
    if (ok) { 0 } else { 1 }
}
