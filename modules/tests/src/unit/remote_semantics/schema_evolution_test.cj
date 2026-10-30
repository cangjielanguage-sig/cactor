/*
 * CActor - R6 测试：消息序列化版本与 schema 演进
 * - 版本 1 的帧与旧格式逐字节相同（旧节点可读）
 * - 新节点读取旧版本数据（新增字段取默认值）
 * - 旧节点收到更新版本的数据时明确拒绝；经真实 TCP 时计入接收方的解码失败，后续消息不受影响
 * - 与分片信封等嵌套编解码器组合
 */
package cactor_tests.unit.remote_semantics

import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.*
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterSharding, ShardingEnvelope}
import cactor_testkit.*

/** 订单消息：v1 只有 id、qty；v2 增加 currency */
class Order <: Message {
    let id: String
    let qty: Int64
    let currency: String

    init(id: String, qty: Int64, currency: String) {
        this.id = id
        this.qty = qty
        this.currency = currency
    }

    public func messageType(): String {
        "test.Order"
    }
}

/** 旧版本（v1）编解码器 */
class OrderCodecV1 <: MessageCodec {
    public func encode(message: Message): Array<UInt8> {
        let o = (message as Order).getOrThrow()
        "${o.id}|${o.qty}".toArray()
    }

    public func decode(payload: Array<UInt8>): Message {
        let parts = String.fromUtf8(payload).split("|")
        Order(parts[0], Int64.parse(parts[1]), "CNY")
    }
}

/** 新版本（v2）编解码器：读 v1 时 currency 取默认值 */
class OrderCodecV2 <: MessageCodec {
    public func encode(message: Message): Array<UInt8> {
        let o = (message as Order).getOrThrow()
        "${o.id}|${o.qty}|${o.currency}".toArray()
    }

    public func decode(payload: Array<UInt8>): Message {
        decode(payload, 2)
    }

    public func version(): Int64 {
        2
    }

    public func decode(payload: Array<UInt8>, version: Int64): Message {
        let parts = String.fromUtf8(payload).split("|")
        if (version == 1) {
            Order(parts[0], Int64.parse(parts[1]), "CNY")
        } else {
            Order(parts[0], Int64.parse(parts[1]), parts[2])
        }
    }
}

/** 同一进程内模拟“发送方是新版本、接收方是旧版本”：只有设置了标记的线程按 v2 写出 */
let NEWER_WRITER = ThreadLocal<Bool>()

class MixedVersionCodec <: MessageCodec {
    let v2 = OrderCodecV2()
    let v1 = OrderCodecV1()

    private func newer(): Bool {
        NEWER_WRITER.get() ?? false
    }

    public func encode(message: Message): Array<UInt8> {
        if (newer()) { v2.encode(message) } else { v1.encode(message) }
    }

    public func decode(payload: Array<UInt8>): Message {
        v1.decode(payload)
    }

    public func version(): Int64 {
        if (newer()) { 2 } else { 1 }
    }
}

func legacyFrame(messageType: String, payload: Array<UInt8>): Array<UInt8> {
    let out = ArrayList<UInt8>()
    let put32 = { v: Int64 =>
        out.add(UInt8((v >> 24) & 0xFF))
        out.add(UInt8((v >> 16) & 0xFF))
        out.add(UInt8((v >> 8) & 0xFF))
        out.add(UInt8(v & 0xFF))
    }
    let t = messageType.toArray()
    put32(t.size)
    out.add(all: t)
    put32(payload.size)
    out.add(all: payload)
    out.toArray()
}

func asOrder(m: Message): Order {
    (m as Order).getOrThrow()
}

@Test
public class SchemaEvolutionTest {
    let ser = MessageSerializer()

    @TestCase
    public func testVersionOneFrameIsByteIdenticalToLegacy(): Unit {
        MessageCodecRegistry.register("test.Order", OrderCodecV1())
        let bytes = ser.serialize(Order("o-1", 3, "USD"))
        @Expect(bytes, legacyFrame("test.Order", "o-1|3".toArray()))
        let back = asOrder(ser.deserialize(bytes))
        @Expect(back.id, "o-1")
        @Expect(back.qty, 3)
        @Expect(back.currency, "CNY")
    }

    @TestCase
    public func testNewReaderUnderstandsOldData(): Unit {
        // 旧节点写出的帧（无版本头）
        let old = legacyFrame("test.Order", "o-2|7".toArray())
        MessageCodecRegistry.register("test.Order", OrderCodecV2())
        let o = asOrder(ser.deserialize(old))
        @Expect(o.id, "o-2")
        @Expect(o.qty, 7)
        @Expect(o.currency, "CNY")
        // v2 自己的往返
        let o2 = asOrder(ser.deserialize(ser.serialize(Order("o-3", 1, "EUR"))))
        @Expect(o2.currency, "EUR")
    }

    @TestCase
    public func testOldReaderRejectsNewerVersion(): Unit {
        MessageCodecRegistry.register("test.Order", OrderCodecV2())
        let newer = ser.serialize(Order("o-4", 2, "JPY"))
        // 版本帧：类型之后是 -1 标记与版本号 2
        let typeEnd = 4 + "test.Order".size
        @Expect(newer[typeEnd..(typeEnd + 4)], [0xFFu8, 0xFFu8, 0xFFu8, 0xFFu8])
        @Expect(newer[(typeEnd + 4)..(typeEnd + 8)], [0u8, 0u8, 0u8, 2u8])
        MessageCodecRegistry.register("test.Order", OrderCodecV1())
        var error = ""
        try {
            ser.deserialize(newer)
        } catch (e: DeserializationException) {
            error = e.message
        }
        @Expect(error.contains("版本 2") && error.contains("支持的版本 1"), true)
        // 截断的版本头
        var truncated = false
        try {
            ser.deserialize(newer[0..(typeEnd + 6)])
        } catch (e: DeserializationException) {
            truncated = true
        }
        @Expect(truncated, true)
    }

    @TestCase
    public func testNestedInShardingEnvelope(): Unit {
        // 构造 ClusterSharding 会注册分片信封编解码器
        let kit = ActorTestKit("schema-nested")
        let r = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        r.start()
        let c = Cluster(kit.system, r, [r.getLocalAddress()], ClusterSettings.defaults())
        let sharding = ClusterSharding(c, kit.system, r, "schema", 4, { _ => Silent() })
        MessageCodecRegistry.register("test.Order", OrderCodecV2())
        // 分片信封内嵌 v2 消息：内层同样带版本头并正确还原
        let env = ShardingEnvelope("e-1", Order("o-5", 9, "GBP"))
        let bytes = ser.serialize(env)
        let back = (ser.deserialize(bytes) as ShardingEnvelope).getOrThrow()
        @Expect(back.entityId, "e-1")
        @Expect(asOrder(back.payload).currency, "GBP")
        // 内层版本更新时，外层信封整体被拒绝
        MessageCodecRegistry.register("test.Order", OrderCodecV1())
        var rejected = false
        try {
            ser.deserialize(bytes)
        } catch (e: DeserializationException) {
            rejected = true
        }
        @Expect(rejected, true)
        sharding.stop()
        c.shutdown()
        r.stop()
        kit.shutdown()
    }

    @TestCase
    public func testRemoteRejectsNewerVersionAndKeepsWorking(): Unit {
        MessageCodecRegistry.register("test.Order", MixedVersionCodec())
        let a = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        let b = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        a.start()
        b.start()
        let rec = EventRecorder()
        b.registerHandler("orders", { m: Message =>
            let o = asOrder(m)
            rec.record("${o.id}:${o.currency}")
        })
        let failuresBefore = b.getReceiver().getDecodeFailures()
        // 新版本发送方（本线程按 v2 写出），旧版本接收方（传输线程只认 v1）
        NEWER_WRITER.set(Some(true))
        a.sendMessage(b.getLocalAddress(), "orders", Order("new-1", 1, "USD"))
        NEWER_WRITER.set(Some(false))
        @Expect(awaitCondition(T5S, { => b.getReceiver().getDecodeFailures() == failuresBefore + 1 }), true)
        // 连接没有受影响：随后的 v1 消息正常到达
        a.sendMessage(b.getLocalAddress(), "orders", Order("old-1", 2, "USD"))
        @Expect(rec.await("old-1:CNY", T5S), true)
        @Expect(rec.count("new-1:USD") + rec.count("new-1:CNY"), 0)
        a.stop()
        b.stop()
        MessageCodecRegistry.unregister("test.Order")
    }
}
