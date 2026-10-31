/*
 * CActor Core.DSL - 消息处理器运行时组合子（对照 docs/plan/dsl20261030.md §7.3 / §8.2 降级方案）
 *
 * P0 实验结论：cjc 1.2.0 属性宏的 attr 为表达式时（如 3.seconds）在成员声明位置
 * 存在解析歧义，@within/@retry 修饰宏不可行，因此超时监控与重试改为运行时组合子：
 *
 *   // 处理器内部组合使用，语义与手写等价：
 *   func onAskModel(m: AskModel, ctx: ActorContext): MessageResult {
 *       within(3.seconds, "onAskModel") {        // 超时监控：超过阈值打日志，不改返回值
 *           retry(3) {                            // 失败重试：Unhandled/异常时重试，耗尽返回 Escalate
 *               callModel(m)
 *           }
 *       }
 *   }
 *
 *   // 更贴近"一句英语"的链式形态（推荐）：
 *   onAskModel => handle(ctx) { callModel(m) }
 *       |> within(3.seconds)
 *       |> retry(3)
 *
 * 实现为泛型高阶函数，无装箱、无反射；retry 只针对 MessageResult 语义。
 */
package cactor_core.dsl

import std.time.*
import cactor_core.message.MessageResult

/**
 * 超时监控组合子：执行 body 并计时，超过 limit 打印警告日志。
 * 返回值原样透传，不改变处理语义（监控用，非抢占式超时——Actor 模型中
 * 抢占式取消需要信封层支持，属 20261029 P 系列范围）。
 */
public func within(limit: Duration, name: String, body: () -> MessageResult): MessageResult {
    let start = DateTime.now()
    let r = body()
    let cost = DateTime.now() - start
    if (cost > limit) {
        println("[within] ${name} 耗时 ${cost / Duration.millisecond}ms 超过 ${limit / Duration.millisecond}ms")
    }
    r
}

/**
 * 重试组合子：body 返回 Unhandled 或抛异常视为失败，最多 attempts 次尝试；
 * 全部失败时：若曾抛异常返回 Escalate，否则返回最后一次的 Unhandled。
 */
public func retry(attempts: Int64, body: () -> MessageResult): MessageResult {
    var lastException: ?Exception = None
    var attempt: Int64 = 0
    var lastResult = MessageResult.Unhandled
    var escalated = false
    while (attempt < attempts) {
        attempt++
        try {
            let r = body()
            lastResult = r
            match (r) {
                case MessageResult.Unhandled => ()
                case _ => return r
            }
        } catch (e: Exception) {
            lastException = Some(e)
            escalated = true
        }
    }
    if (escalated) {
        return MessageResult.Escalate
    }
    return lastResult
}
