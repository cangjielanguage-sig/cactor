/*
 * CActor - R5 测试：@Message 宏生成 messageType、编解码器与 registerCodec，经真实 TCP 远程往返
 */
package cactor_tests.unit.dsl

import std.collection.*
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

// P2 增强：Option<T> 与 HashMap<String, T> 字段
@Message
public class RichOrder <: Message {
    let orderId: String
    let note: Option<String>
    let discount: Option<Int64>
    let ratio: Option<Float64>
    let expedited: Option<Bool>
    let extras: HashMap<String, String>
    let weights: HashMap<String, Float64>
    let flags: HashMap<String, Bool>

    public init(
        orderId: String,
        note: Option<String>,
        discount: Option<Int64>,
        ratio: Option<Float64>,
        expedited: Option<Bool>,
        extras: HashMap<String, String>,
        weights: HashMap<String, Float64>,
        flags: HashMap<String, Bool>
    ) {
        this.orderId = orderId
        this.note = note
        this.discount = discount
        this.ratio = ratio
        this.expedited = expedited
        this.extras = extras
        this.weights = weights
        this.flags = flags
    }

    public func summary(): String {
        "${orderId}|${note ?? "-"}|${discount ?? -1}|${ratio ?? -1.0}|${expedited ?? false}|" +
        "${extras.size}|${weights.size}|${flags.size}"
    }
}

func sampleOrder(): PlaceOrder {
    let o = PlaceOrder("订单-1/α", -42, 3.25, true, ["a", "", "中文"], [Int64.Min, 0, Int64.Max],
        [-0.5, 1.0e300, 0.0], [true, false])
    o.note = "备注"
    o
}

// T13：线上名称与重名检测。两个不同的类故意用同一个线上名称，模拟"不同包里的同名消息类"
@Message[name = "t13.orders.Ping"]
class NamedPing <: Message {
    var n: Int64 = 0

    init(n: Int64) {
        this.n = n
    }
}

@Message[name = "t13.Clash"]
class ClashA <: Message {
    var a: String = ""

    init(a: String) {
        this.a = a
    }
}

@Message[name = "t13.Clash"]
class ClashB <: Message {
    var b: Int64 = 0

    init(b: Int64) {
        this.b = b
    }
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
    public func testOptionAndMapFieldsRoundTrip(): Unit {
        RichOrder.registerCodec()
        let extras = HashMap<String, String>()
        extras["gift"] = "包装"
        let weights = HashMap<String, Float64>()
        weights["kg"] = 1.5
        let flags = HashMap<String, Bool>()
        flags["fragile"] = true
        let o = RichOrder("O-1", Some("加急"), None, Some(0.25), None, extras, weights, flags)
        let ser = MessageSerializer()
        let back = (ser.deserialize(ser.serialize(o)) as RichOrder).getOrThrow()
        @Expect(back.summary(), o.summary())
        @Expect(back.note ?? "-", "加急")
        @Expect(back.discount ?? -1, -1)
        @Expect(back.extras.get("gift") ?? "", "包装")
        @Expect(back.weights.get("kg") ?? 0.0, 1.5)
        @Expect(back.flags.get("fragile") ?? false, true)
    }

    @TestCase
    public func testWireFormatIsStable(): Unit {
        // 线上格式锁定：写入器换实现（ArrayList → 数组缓冲）后字节必须与原格式逐字节一致，
        // 否则新旧节点之间无法互通
        let m = HashMap<String, Bool>()
        m["k"] = true
        let bytes = MessageFieldWriter()
            .writeInt64(-2)
            .writeString("ab")
            .writeBool(true)
            .writeOptionInt64(None)
            .writeOptionString(Some("é"))
            .writeBoolMap(m)
            .toArray()
        let expected: Array<UInt8> = [
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE,
            0, 0, 0, 2, 97, 98,
            1,
            0,
            1, 0, 0, 0, 2, 0xC3, 0xA9,
            0, 0, 0, 1, 0, 0, 0, 1, 107, 1
        ]
        @Expect(bytes, expected)
    }

    @TestCase
    public func testWriterGrowsBeyondInitialBuffer(): Unit {
        // 远大于初始 64 字节缓冲的消息：多次扩容后仍能完整往返
        let big = String(Array<Rune>(10000, repeat: r'字'))
        let w = MessageFieldWriter()
        for (i in 0..100) {
            w.writeInt64(i)
        }
        w.writeString(big)
        let r = MessageFieldReader(w.toArray())
        var sum: Int64 = 0
        for (_ in 0..100) {
            sum += r.readInt64()
        }
        @Expect(sum, 4950)
        @Expect(r.readString(), big)
        @Expect(r.isAtEnd(), true)
    }

    @TestCase
    public func testCorruptOptionAndMapAreRejected(): Unit {
        // Option 标记只能是 0 / 1
        var badTag = false
        try {
            MessageFieldReader([2, 0, 0, 0, 0, 0, 0, 0, 1]).readOptionInt64()
        } catch (e: DeserializationException) {
            badTag = true
        }
        @Expect(badTag, true)
        // 伪造的超大条目数：在预分配之前就被拒绝
        var hugeCount = false
        try {
            MessageFieldReader([0x7F, 0xFF, 0xFF, 0xFF, 0, 0, 0, 1, 107, 1]).readBoolMap()
        } catch (e: DeserializationException) {
            hugeCount = true
        }
        @Expect(hugeCount, true)
        // 重复键：合法编码不会出现
        var duplicate = false
        try {
            MessageFieldReader([0, 0, 0, 2, 0, 0, 0, 1, 107, 1, 0, 0, 0, 1, 107, 0]).readBoolMap()
        } catch (e: DeserializationException) {
            duplicate = true
        }
        @Expect(duplicate, true)
        // 非法 UTF-8：同样按数据损坏报 DeserializationException
        var badUtf8 = false
        try {
            MessageFieldReader([0, 0, 0, 2, 0xC3, 0x28]).readString()
        } catch (e: DeserializationException) {
            badUtf8 = true
        }
        @Expect(badUtf8, true)
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

    @TestCase
    public func explicitWireNameIsUsedForTypeAndRegistry(): Unit {
        NamedPing.registerCodec()
        // 同一个类重复注册无害
        NamedPing.registerCodec()
        @Expect(NamedPing(1).messageType(), "t13.orders.Ping")
        @Expect(MessageCodecRegistry.isRegistered("t13.orders.Ping"), true)
        @Expect(MessageCodecRegistry.isRegistered("NamedPing"), false)
        let ser = MessageSerializer()
        let back = (ser.deserialize(ser.serialize(NamedPing(42))) as NamedPing).getOrThrow()
        @Expect(back.n, 42)
    }

    @TestCase
    public func differentClassesWithSameWireNameFailFast(): Unit {
        ClashA.registerCodec()
        var clash = ""
        try {
            ClashB.registerCodec()
        } catch (e: IllegalStateException) {
            clash = e.message
        }
        // 修复前：ClashB 静默覆盖 ClashA 的编解码器，远程收到的 ClashA 被解成 ClashB
        @Expect(clash.contains("t13.Clash"), true)
        let ser = MessageSerializer()
        let back = (ser.deserialize(ser.serialize(ClashA("still-a"))) as ClashA).getOrThrow()
        @Expect(back.a, "still-a")
        MessageCodecRegistry.unregister("t13.Clash")
    }
}
