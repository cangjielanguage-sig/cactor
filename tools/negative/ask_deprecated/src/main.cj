/*
 * 检查：已废弃的经典 ask 入口（AskPatternManager / Ask，E38 收敛后由 AskInbox.ask 取代）仍可编译，
 * 但必须给出 deprecated 告警。早期恒超时的模拟实现 TypedAskPattern 已删除（C10），不再出现在这里。
 */
package neg_ask_deprecated

import std.time.*
import cactor_patterns.ask.*

main(): Int64 {
    let m = AskPatternManager(Duration.second)
    println("deprecated check ${m.getPendingRequestCount()}")
    return 0
}
