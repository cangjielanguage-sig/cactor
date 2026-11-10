/*
 * CActor - 类型化消息的跨进程编解码（E63）
 *
 * `TypedPayloadBox` 的载荷在编译期是 `Any`：能过线的只有实现了 Message 的协议类型。
 * 这里断言三件事：
 *   - 协议消息（StringMessage）装箱后能原样还原，且还原出的仍是"载荷载体"；
 *   - 载荷不是 Message 时发送侧立即抛 SerializationException（而不是让网络层丢包）；
 *   - 重复调用注册函数是幂等的，不会把已有 codec 顶掉。
 */
package cactor_tests.unit.runtime.typed_semantics

import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{Message, StringMessage, MessageCodecRegistry, SerializationException}
import cactor_core.typed.*

@Test
public class TypedPayloadCodecTest {
    @TestCase
    public func roundTripsMessagePayload(): Unit {
        @Expect(ensureTypedPayloadCodecRegistered(), true)
        let codec = match (MessageCodecRegistry.get(TYPED_PAYLOAD_BOX_TYPE)) {
            case Some(c) => c
            case None => throw Exception("类型化消息 codec 没有注册")
        }
        let encoded = codec.encode(TypedPayloadBox(StringMessage("跨进程")))
        let decoded = codec.decode(encoded)
        let payload = match (decoded as TypedPayloadBox) {
            case Some(b) => b.payload
            case None => throw Exception("解码结果不是 TypedPayloadBox")
        }
        let text = match (payload as StringMessage) {
            case Some(m) => m.getContent()
            case None => "<不是 StringMessage>"
        }
        @Expect(text, "跨进程")
        // 还原出的信封仍可被经典接收侧当作载荷载体解包
        let carrier = match (decoded as PayloadCarrier) {
            case Some(c) => c
            case None => throw Exception("TypedPayloadBox 应当实现 PayloadCarrier")
        }
        let again = match (carrier.anyPayload() as StringMessage) {
            case Some(m) => m.getContent()
            case None => "<不是 StringMessage>"
        }
        @Expect(again, "跨进程")
    }

    @TestCase
    public func rejectsPayloadThatIsNotMessage(): Unit {
        ensureTypedPayloadCodecRegistered()
        let codec = match (MessageCodecRegistry.get(TYPED_PAYLOAD_BOX_TYPE)) {
            case Some(c) => c
            case None => throw Exception("类型化消息 codec 没有注册")
        }
        // 闭包/基础类型这类"不可能过线"的载荷必须在发送侧失败
        var failed = false
        try {
            codec.encode(TypedPayloadBox({ => 1 }))
        } catch (e: SerializationException) {
            failed = true
        }
        @Expect(failed, true)
    }

    @TestCase
    public func registrationIsIdempotent(): Unit {
        // 不依赖用例执行顺序：先确保已注册，再断言"重复注册不覆盖"
        ensureTypedPayloadCodecRegistered()
        @Expect(MessageCodecRegistry.isRegistered(TYPED_PAYLOAD_BOX_TYPE), true)
        @Expect(ensureTypedPayloadCodecRegistered(), false)
    }
}
