/*
 * CActor - 类型化消息的跨进程编解码（E63）
 *
 * `TypedPayloadBox` 的载荷在编译期是 `Any`：要过线，要么实现 Message（嵌套消息帧），
 * 要么用 TypedPayloadCodecs 注册命名编解码器（普通 enum 的常见做法）。这里断言：
 *   - 协议消息（StringMessage）装箱后能原样还原，且还原出的仍是"载荷载体"；
 *   - 普通 enum 经命名编解码器往返后回到同一分支；
 *   - 两端编解码器配置不一致时解码侧明确失败；
 *   - 载荷两条路都走不通时发送侧立即抛 SerializationException（而不是让网络层丢包）；
 *   - 类型名重复注册立刻拒绝；重复调用注册函数是幂等的。
 */
package cactor_tests.unit.runtime.typed_semantics

import std.collection.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{Message, StringMessage, MessageCodecRegistry, SerializationException, DeserializationException}
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
        let decoded = codec.decode(encoded, codec.version())
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
    public func roundTripsRegisteredEnumPayload(): Unit {
        ensureTypedPayloadCodecRegistered()
        let codec = match (MessageCodecRegistry.get(TYPED_PAYLOAD_BOX_TYPE)) {
            case Some(c) => c
            case None => throw Exception("类型化消息 codec 没有注册")
        }
        // 普通 enum 不是 Message：只能靠命名编解码器过线
        TypedPayloadCodecs.register<Shape>(
            typeName: "tests.Shape",
            encode: { s => encodeShape(s) },
            decode: { b => decodeShape(b) })
        let encoded = codec.encode(TypedPayloadBox(Shape.Circle(2.5)))
        let decoded = codec.decode(encoded, codec.version())
        let shape = match (decoded as TypedPayloadBox) {
            case Some(b) => match (b.payload as Shape) {
                case Some(s) => s
                case None => throw Exception("解码结果不是 Shape")
            }
            case None => throw Exception("解码结果不是 TypedPayloadBox")
        }
        match (shape) {
            case Shape.Circle(r) => @Expect(r, 2.5)
            case Shape.Named(_) => @Fail("应当是 Circle")
        }
        TypedPayloadCodecs.unregister("tests.Shape")
    }

    @TestCase
    public func rejectsUnknownPayloadCodecAtDecode(): Unit {
        ensureTypedPayloadCodecRegistered()
        let codec = match (MessageCodecRegistry.get(TYPED_PAYLOAD_BOX_TYPE)) {
            case Some(c) => c
            case None => throw Exception("类型化消息 codec 没有注册")
        }
        TypedPayloadCodecs.register<Shape>(
            typeName: "tests.Shape.Unknown",
            encode: { s => encodeShape(s) },
            decode: { b => decodeShape(b) })
        let encoded = codec.encode(TypedPayloadBox(Shape.Named("发给没有该编解码器的节点")))
        TypedPayloadCodecs.unregister("tests.Shape.Unknown")
        var failed = false
        try {
            codec.decode(encoded, 2)
        } catch (e: DeserializationException) {
            failed = true
        }
        @Expect(failed, true)
    }

    @TestCase
    public func rejectsDuplicatePayloadTypeName(): Unit {
        TypedPayloadCodecs.register<Shape>(
            typeName: "tests.Shape.Dup",
            encode: { s => encodeShape(s) },
            decode: { b => decodeShape(b) })
        var failed = false
        try {
            TypedPayloadCodecs.register<Shape>(
                typeName: "tests.Shape.Dup",
                encode: { s => encodeShape(s) },
                decode: { b => decodeShape(b) })
        } catch (e: IllegalStateException) {
            failed = true
        }
        TypedPayloadCodecs.unregister("tests.Shape.Dup")
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

/** 类型化协议最常见的形态：不实现 Message 的普通 enum */
enum Shape {
    | Circle(Float64)
    | Named(String)
}

func encodeShape(s: Shape): Array<UInt8> {
    let out = ArrayList<UInt8>()
    match (s) {
        case Shape.Circle(r) =>
            out.add(1u8)
            for (b in r.toString().toArray()) {
                out.add(b)
            }
        case Shape.Named(text) =>
            out.add(2u8)
            for (b in text.toArray()) {
                out.add(b)
            }
    }
    out.toArray()
}

func decodeShape(b: Array<UInt8>): Shape {
    if (b.size < 1) {
        throw DeserializationException("Shape 载荷为空")
    }
    let text = String.fromUtf8(b.slice(1, b.size - 1))
    match (b[0]) {
        case 1u8 => Shape.Circle(Float64.parse(text))
        case 2u8 => Shape.Named(text)
        case _ => throw DeserializationException("未知 Shape 种类")
    }
}
