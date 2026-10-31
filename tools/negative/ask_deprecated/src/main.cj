/*
 * 检查：已废弃的 ask 模拟实现仍可编译，但必须给出 deprecated 告警。
 */
package neg_ask_deprecated

import cactor_patterns.ask.*

main(): Int64 {
    let p = TypedAskPattern()
    println("deprecated check")
    return 0
}
