/*
 * CActor - R5 测试：@Message 宏生成 messageType、编解码器与 registerCodec，经真实 TCP 远程往返
 */
package cactor_tests.unit.dsl

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.*
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_macros.*
import cactor_testkit.*

@Message
public class PlaceOrder <: Message {
    let orderId: String
    let quantity: Int64
    let price: Float64
    let urgent: Bool
    let tags: Array<String>
    let lines: Array<Int64>
    let weights: Array<Float64>
    let flags: Array<Bool>
    var note: String = ""
    static let created = AtomicInt64(0)

    public init(orderId: String, quantity: Int64, price: Float64, urgent: Bool, tags: Array<String>,
        lines: Array<Int64>, weights: Array<Float64>, flags: Array<Bool>) {
        this.orderId = orderId
        this.quantity = quantity
        this.price = price
        this.urgent = urgent
        this.tags = tags
        this.lines = lines
        this.weights = weights
        this.flags = flags
        created.fetchAdd(1)
    }

    func summary(): String {
        "${orderId}|${quantity}|${price}|${urgent}|${tags}|${lines}|${weights}|${flags}|${note}"
    }
}

@Message
class Empty <: Message {
    init() {}
}

func sampleOrder(): PlaceOrder {
    let o = PlaceOrder("订单-1/α", -42, 3.25, true, ["a", "", "中文"], [Int64.Min, 0, Int64.Max],
        [-0.5, 1.0e300, 0.0], [true, false])
    o.note = "备注"
    o
}

@Test
public class MessageMacroTest {
    @TestCase
    public func testGeneratedMessageTypeAndRoundTrip(): Unit {
        PlaceOrder.registerCodec()
        Empty.registerCodec()
        let o = sampleOrder()
        @Expect(o.messageType(), "PlaceOrder")
        @Expect(MessageCodecRegistry.isRegistered("PlaceOrder"), true)
        let ser = MessageSerializer()
        let back = (ser.deserialize(ser.serialize(o)) as PlaceOrder).getOrThrow()
        @Expect(back.summary(), o.summary())
        let e = ser.deserialize(ser.serialize(Empty()))
        @Expect(e.messageType(), "Empty")
    }

    @TestCase
    public func testCorruptPayloadIsRejected(): Unit {
        PlaceOrder.registerCodec()
        let ser = MessageSerializer()
        let bytes = ser.serialize(sampleOrder())
        // 截断：字段不完整
        var truncated = false
        try {
            ser.deserialize(bytes[0..(bytes.size - 3)])
        } catch (e: DeserializationException) {
            truncated = true
        }
        @Expect(truncated, true)
        // 多余字节：直接调用编解码器
        let codec = MessageCodecRegistry.get("PlaceOrder").getOrThrow()
        let payload = codec.encode(sampleOrder())
        let extra = Array<UInt8>(payload.size + 1, { i => if (i < payload.size) { payload[i] } else { 7 } })
        var trailing = false
        try {
            codec.decode(extra)
        } catch (e: DeserializationException) {
            trailing = true
        }
        @Expect(trailing, true)
    }

    @TestCase
    public func testRemoteRoundTripBetweenTwoNodes(): Unit {
        PlaceOrder.registerCodec()
        let a = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        let b = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        a.start()
        b.start()
        let rec = EventRecorder()
        b.registerHandler("orders", { m: Message =>
            if (let Some(o) <- (m as PlaceOrder)) {
                rec.record(o.summary())
            }
        })
        let o = sampleOrder()
        for (_ in 0..100) {
            a.sendMessage(b.getLocalAddress(), "orders", o)
        }
        @Expect(rec.awaitSize(100, Duration.second * 5), true)
        @Expect(rec.count(o.summary()), 100)
        a.stop()
        b.stop()
    }
}
