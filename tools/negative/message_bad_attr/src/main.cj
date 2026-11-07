/*
 * 负例（T13）：@Message 的属性只支持 name = "线上名称"，写错键名必须在编译期报错。
 * 期望错误包含："的属性只支持 name"
 */
package neg_message_bad_attr

import cactor_core.message.*
import cactor_macros.*

@Message[nmae = "orders.Ping"]
class Ping <: Message {
    var n: Int64 = 0

    init(n: Int64) {
        this.n = n
    }
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
