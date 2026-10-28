/*
 * 消息序列化测试：往返一致性、自定义编解码器、损坏数据处理
 */
package cactor_core.message

import std.random.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*

/** 自定义消息：订单 */
struct OrderMessage <: UserMessage {
    let orderId: Int64
    let item: String

    init(orderId: Int64, item: String) {
        this.orderId = orderId
        this.item = item
    }

    public func messageType(): String {
        "OrderMessage"
    }
}

class OrderCodec <: MessageCodec {
    public func encode(message: Message): Array<UInt8> {
        match (message as OrderMessage) {
            case Some(o) => "${o.orderId}|${o.item}".toArray()
            case None => throw SerializationException("不是 OrderMessage")
        }
    }

    public func decode(payload: Array<UInt8>): Message {
        let text = String.fromUtf8(payload)
        let parts = text.split("|", 2)
        OrderMessage(Int64.parse(parts[0]), parts[1])
    }
}

func roundTrip(m: Message): Message {
    let s = MessageSerializer()
    s.deserialize(s.serialize(m))
}

@Test
public class SerializationTest {
    @TestCase
    func builtInMessagesRoundTrip(): Unit {
        for (text in ["hello", "", "中文消息 ✓ emoji 🚀", "a|b|c"]) {
            match (roundTrip(StringMessage(text)) as StringMessage) {
                case Some(m) => @Expect(m.getContent(), text)
                case None => @Fail("类型不匹配")
            }
        }
        match (roundTrip(PingMessage(123456789)) as PingMessage) {
            case Some(m) => @Expect(m.getId(), 123456789)
            case None => @Fail("类型不匹配")
        }
        match (roundTrip(PongMessage(-42)) as PongMessage) {
            case Some(m) => @Expect(m.getOriginalId(), -42)
            case None => @Fail("类型不匹配")
        }
        match (roundTrip(StopMessage("关闭")) as StopMessage) {
            case Some(m) => @Expect(m.getReason(), "关闭")
            case None => @Fail("类型不匹配")
        }
    }

    @TestCase
    func networkMessagePreservesEnvelope(): Unit {
        let s = NetworkMessageSerializer()
        let original = NetworkMessage("user/目标", StringMessage("payload"), Some("cactor://node-a/user/sender"))
        let decoded = s.deserialize(s.serialize(original))
        @Expect(decoded.targetActor, "user/目标")
        @Expect(decoded.sender, Some("cactor://node-a/user/sender"))
        @Expect(decoded.messageId, original.messageId)
        @Expect(decoded.timestamp, original.timestamp)
        match (decoded.message as StringMessage) {
            case Some(m) => @Expect(m.getContent(), "payload")
            case None => @Fail("类型不匹配")
        }
        // 无发送者
        let noSender = s.deserialize(s.serialize(NetworkMessage("t", PingMessage(1), None)))
        @Expect(noSender.sender.isNone(), true)
        // 每条消息 ID 唯一
        @Expect(NetworkMessage("t", PingMessage(1), None).messageId != NetworkMessage("t", PingMessage(1), None).messageId, true)
    }

    @TestCase
    func customCodecRegistry(): Unit {
        @ExpectThrows[SerializationException](MessageSerializer().serialize(OrderMessage(1, "x")))
        MessageCodecRegistry.register("OrderMessage", OrderCodec())
        @Expect(MessageCodecRegistry.isRegistered("OrderMessage"), true)
        match (roundTrip(OrderMessage(77, "键盘|红色")) as OrderMessage) {
            case Some(o) =>
                @Expect(o.orderId, 77)
                @Expect(o.item, "键盘|红色")
            case None => @Fail("类型不匹配")
        }
        // 经由网络信封
        let s = NetworkMessageSerializer()
        let decoded = s.deserialize(s.serialize(NetworkMessage("orders", OrderMessage(5, "鼠标"), None)))
        @Expect((decoded.message as OrderMessage).isSome(), true)
        MessageCodecRegistry.unregister("OrderMessage")
        @Expect(MessageCodecRegistry.isRegistered("OrderMessage"), false)
    }

    @TestCase
    func truncatedDataIsRejected(): Unit {
        let s = NetworkMessageSerializer()
        let bytes = s.serialize(NetworkMessage("target", StringMessage("some content"), Some("from")))
        var rejected = 0
        for (cut in 0..bytes.size) {
            try {
                s.deserialize(bytes[0..cut])
            } catch (_: Exception) {
                rejected++
            }
        }
        // 任意截断都必须以异常结束，而不是返回错误数据或崩溃
        @Expect(rejected, bytes.size)
    }

    @TestCase
    func randomBytesNeverCrash(): Unit {
        let rnd = Random(20261028)
        let s = NetworkMessageSerializer()
        var decodedOrRejected = 0
        for (_ in 0..2000) {
            let n = Int64(rnd.nextUInt8() % 64)
            let data = Array<UInt8>(n, { _ => rnd.nextUInt8() })
            try {
                s.deserialize(data)
            } catch (_: Exception) {
                ()
            }
            decodedOrRejected++
        }
        @Expect(decodedOrRejected, 2000)
    }
}
