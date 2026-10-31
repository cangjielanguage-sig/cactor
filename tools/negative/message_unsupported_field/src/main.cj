/*
 * 负例：@Message 字段类型不受支持 —— 必须在编译期报错，且定位到具体字段。
 * 期望错误包含："的类型 Int32 不支持"
 */
package neg_message_unsupported_field

import cactor_core.message.*
import cactor_macros.*

@Message
class BadField <: Message {
    let payload: Int32

    init(payload: Int32) {
        this.payload = payload
    }
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
