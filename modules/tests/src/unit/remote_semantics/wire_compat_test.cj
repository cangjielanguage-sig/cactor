/*
 * CActor - DA6 线协议能力协商与滚动升级兼容矩阵
 *
 * 混合版本的组合（"旧" = 引入协商之前的节点：握手不带能力扩展；用 WireProtocolSettings.legacyEmulation 模拟，
 * 并用抓下来的旧格式握手字节直接走真实 TCP）：
 *   新 ↔ 新：协商出 typed-codec-v3，kind 3（带协议标记的嵌套消息）照常往返；
 *   新 → 旧：kind 1/2 写成 v2 版本头（无损降级，v1.2.x 读得懂）；kind 3 在发送侧明确失败——
 *            tell 抛 WireIncompatibleException、ask 以它失败、可靠投递不入队，计数 + 监听（死信），不算成功；
 *   旧 → 新：旧节点照常握手、照常发 v3 帧，新节点读得懂；
 *   读不懂的帧（未知 kind / 更高版本）：不投递、单独计数、通知监听，连接与后续消息不受影响；
 *   协议下限：低于 minPeerProtocolVersion 的握手被拒并给出原因。
 * 等待一律用 awaitCondition，不睡固定时长。
 */
package cactor_tests.unit.remote_semantics

import std.sync.*
import std.time.*
import std.net.{TcpSocket}
import std.collection.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.*
import cactor_core.typed.{TypedPayloadBox, TypedTypeTag, TypedPayloadBoxCodec, TypedPayloadCodecs, TYPED_PAYLOAD_BOX_TYPE}
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TransportHandshake, TransportProtocol, TcpTransport,
    encodeFrame}
import cactor_foundation.hashing.{hmacSha256, toHex}
import cactor_remote.*
import cactor_testkit.*

/** 只抓握手对象的假传输：直接驱动 accept / confirm，比对逐字节的握手帧 */
class WireHandshakeWire <: NetworkTransport {
    let port: Int32
    var handshake: Option<TransportHandshake> = None<TransportHandshake>
    var active = false
    init(port: Int32) { this.port = port }
    public func start(): Unit { active = true }
    public func stop(): Unit { active = false }
    public func isActive(): Bool { active }
    public func getLocalAddress(): NetworkAddress { NetworkAddress("127.0.0.1", port) }
    public func setDataHandler(_: (Array<UInt8>) -> Unit): Unit { () }
    public func send(_: NetworkAddress, _: Array<UInt8>): Unit { () }
    public func setHandshake(h: TransportHandshake): Unit { handshake = Some(h) }
    func hs(): TransportHandshake { handshake.getOrThrow() }
}

func wireNode(port: Int32, wire: WireProtocolSettings, cookie: Option<String>): (SimpleRemoteManager, WireHandshakeWire) {
    let t = WireHandshakeWire(port)
    let m = SimpleRemoteManager(t)
    if (let Some(c) <- cookie) {
        m.configure(RemotingSettings.defaults().withSecureCookie(c))
    }
    m.configureWire(wire)
    m.start()
    (m, t)
}

func tcpNode(wire: WireProtocolSettings): SimpleRemoteManager {
    let m = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
    m.configureWire(wire)
    m.start()
    m
}

/** 引入协商之前（HEAD 714d897）的 HELLO 解析：严格 7 段、版本 "1"，第 5 段只参与 MAC 原文 */
func legacyParserAccepts(frame: String, cookie: Option<String>): Bool {
    let parts = frame.split("|")
    if (parts.size != 7 || parts[1] != "1") {
        return false
    }
    match (cookie) {
        case Some(c) =>
            let body = "${parts[0]}|${parts[1]}|${parts[2]}|${parts[3]}|${parts[4]}|${parts[5]}"
            toHex(hmacSha256(c.toArray(), body.toArray())) == parts[6]
        case None => true
    }
}

func put32(out: ArrayList<UInt8>, v: Int64): Unit {
    out.add(UInt8((v >> 24) & 0xFF))
    out.add(UInt8((v >> 16) & 0xFF))
    out.add(UInt8((v >> 8) & 0xFF))
    out.add(UInt8(v & 0xFF))
}

/** 手工拼一帧 TypedPayloadBox：版本头 + 负载（模拟更新版本的节点写出的帧） */
func typedFrame(version: Int64, body: Array<UInt8>): Array<UInt8> {
    let out = ArrayList<UInt8>()
    let t = TYPED_PAYLOAD_BOX_TYPE.toArray()
    put32(out, t.size)
    out.add(all: t)
    put32(out, -1)
    put32(out, version)
    put32(out, body.size)
    out.add(all: body)
    out.toArray()
}

/** 用真实序列化器拼网络消息外层，再把最后的消息块换成手工帧 */
func networkFrame(target: String, rawMessage: Array<UInt8>): Array<UInt8> {
    let placeholder = StringMessage("x")
    let full = NetworkMessageSerializer().serialize(NetworkMessage(target, placeholder, None<String>))
    let inner = MessageSerializer().serialize(placeholder)
    let out = ArrayList<UInt8>()
    out.add(all: full[0..(full.size - 4 - inner.size)])
    put32(out, rawMessage.size)
    out.add(all: rawMessage)
    out.toArray()
}

/** 帧里版本头的版本号（类型名之后：-1 标记、版本） */
func frameVersion(frame: Array<UInt8>): Int64 {
    let typeLen = (Int64(frame[0]) << 24) | (Int64(frame[1]) << 16) | (Int64(frame[2]) << 8) | Int64(frame[3])
    let at = 4 + typeLen + 4
    (Int64(frame[at]) << 24) | (Int64(frame[at + 1]) << 16) | (Int64(frame[at + 2]) << 8) | Int64(frame[at + 3])
}

func readFrame(s: TcpSocket): Array<UInt8> {
    let header = Array<UInt8>(4, repeat: 0)
    readFully(s, header)
    let n = (Int64(header[0]) << 24) | (Int64(header[1]) << 16) | (Int64(header[2]) << 8) | Int64(header[3])
    let body = Array<UInt8>(n, repeat: 0)
    readFully(s, body)
    body
}

func readFully(s: TcpSocket, buf: Array<UInt8>): Unit {
    var off = 0
    while (off < buf.size) {
        let n = s.read(buf[off..])
        if (n <= 0) {
            throw Exception("连接已关闭")
        }
        off += n
    }
}

/** 收集一个处理器收到的类型化消息："内容@协议标记" */
class TypedSink {
    let rec = EventRecorder()
    func handler(): (Message) -> Unit {
        let r = rec
        { m: Message =>
            match (m as TypedPayloadBox) {
                case Some(b) =>
                    let tag = match (b.typeTag) {
                        case Some(t) => t.typeName
                        case None => "-"
                    }
                    let text = match (b.payload as StringMessage) {
                        case Some(s) => s.getContent()
                        case None => match (b.payload as Da6Num) {
                            case Some(n) => "num:${n.value}"
                            case None => "?"
                        }
                    }
                    r.record("${text}@${tag}")
                case None =>
                    if (let Some(s) <- (m as StringMessage)) {
                        r.record("plain:${s.getContent()}")
                    }
            }
        }
    }
}

/** 命名载荷（kind 2）用的非 Message 类型 */
class Da6Num {
    let value: Int64
    init(value: Int64) { this.value = value }
}

let DA6_NUM_TYPE = "tests.da6.Num"

func ensureDa6NumCodec(): Unit {
    if (TypedPayloadCodecs.find(DA6_NUM_TYPE).isNone()) {
        TypedPayloadCodecs.register<Da6Num>(typeName: DA6_NUM_TYPE, encode: { n => "${n.value}".toArray() },
            decode: { b => Da6Num(Int64.parse(String.fromUtf8(b))) })
    }
}

let TAG = Some(TypedTypeTag("tests.da6.Echo.v1"))

/**
 * 模拟 v1.2.x 的接收方：解码时只认 codec v2（发送线程标记为"新节点"时按 v3 报告版本，同 schema_evolution_test 的手法）
 */
let DA6_NEW_WRITER = ThreadLocal<Bool>()

class V2ReaderTypedCodec <: MessageCodec {
    let real = TypedPayloadBoxCodec()
    public func encode(message: Message): Array<UInt8> { real.encode(message) }
    public func decode(payload: Array<UInt8>): Message { real.decode(payload) }
    public func decode(payload: Array<UInt8>, version: Int64): Message { real.decode(payload, version) }
    public func version(): Int64 {
        if (DA6_NEW_WRITER.get() ?? false) { 3 } else { 2 }
    }
    public func requiredCapabilities(message: Message): UInt64 { real.requiredCapabilities(message) }
    public func encodeFor(message: Message, caps: UInt64): (Int64, Array<UInt8>) {
        // 模拟"未升级的发送方"（旧节点）：无论对端能力都按 v3 写出
        if (DA6_NEW_WRITER.get() ?? false) { (3, real.encode(message)) } else { real.encodeFor(message, caps) }
    }
}

@Test
public class WireCompatTest {
    // ---------------- 编码层：能力决定写出格式 ----------------

    @TestCase
    public func encodingDowngradesLosslesslyOrRejects(): Unit {
        ensureTypedPayloadCodecRegisteredForTest()
        ensureDa6NumCodec()
        let ser = MessageSerializer()
        // kind 1：对端无 v3 能力时写 v2 版本头，字节与 v3 相同之外的部分不变，读回无损
        let k1 = TypedPayloadBox(StringMessage("k1"))
        let v3 = ser.serialize(k1, WireProtocol.LOCAL_CAPABILITIES)
        let v2 = ser.serialize(k1, WireProtocol.CAP_NONE)
        @Expect(frameVersion(v3), 3)
        @Expect(frameVersion(v2), 2)
        let back = (ser.deserialize(v2) as TypedPayloadBox).getOrThrow()
        @Expect((back.payload as StringMessage).getOrThrow().getContent(), "k1")
        // kind 2：命名载荷 + 类型期望尾部，同样降级为 v2（v1.2.x 的解码器按长度切片、忽略尾部）
        let k2 = ser.serialize(TypedPayloadBox(Da6Num(7), typeTag: TAG), WireProtocol.CAP_NONE)
        @Expect(frameVersion(k2), 2)
        let back2 = (ser.deserialize(k2) as TypedPayloadBox).getOrThrow()
        @Expect((back2.payload as Da6Num).getOrThrow().value, 7)
        // kind 3：没有旧格式可退，发送侧抛 WireIncompatibleException（不悄悄丢掉协议标记）
        let k3 = TypedPayloadBox(StringMessage("k3"), typeTag: TAG)
        @Expect(MessageSerializer.requiredCapabilities(k3), WireProtocol.CAP_TYPED_CODEC_V3)
        @Expect(MessageSerializer.requiredCapabilities(k1), WireProtocol.CAP_NONE)
        var missing: UInt64 = 0
        try {
            ser.serialize(k3, WireProtocol.CAP_NONE)
        } catch (e: WireIncompatibleException) {
            missing = e.missingCapabilities
        }
        @Expect(missing, WireProtocol.CAP_TYPED_CODEC_V3)
        let ok = (ser.deserialize(ser.serialize(k3, WireProtocol.LOCAL_CAPABILITIES)) as TypedPayloadBox).getOrThrow()
        @Expect(ok.typeTag.getOrThrow().typeName, "tests.da6.Echo.v1")
        // 嵌套：外层信封里的类型化消息沿用外层的对端能力（可靠投递信封、分片信封等无需改签名）
        let mgr = SimpleRemoteManager(WireHandshakeWire(1)) // 构造即注册可靠投递信封的编解码器
        let env = ReliableEnvelope("c#1", 1, "127.0.0.1:1", "", k3)
        @ExpectThrows[WireIncompatibleException](ser.serialize(env, WireProtocol.CAP_NONE))
        @Expect(ser.serialize(env, WireProtocol.LOCAL_CAPABILITIES).size > 0, true)
        @Expect(mgr.isActive(), false)
    }

    @TestCase
    public func unknownKindAndNewerVersionAreUnsupportedNotCorrupt(): Unit {
        ensureTypedPayloadCodecRegisteredForTest()
        let ser = MessageSerializer()
        var kindErr = ""
        try {
            ser.deserialize(typedFrame(3, [99u8, 0u8]))
        } catch (e: UnsupportedWireFormatException) {
            kindErr = "${e.messageType}:${e.message}"
        }
        @Expect(kindErr.contains("TypedPayloadBox") && kindErr.contains("99"), true)
        var versionErr = ""
        try {
            ser.deserialize(typedFrame(9, [1u8]))
        } catch (e: UnsupportedWireFormatException) {
            versionErr = e.message
        }
        @Expect(versionErr.contains("版本 9"), true)
        // 外层网络消息补上目标：远程层据此通知死信监听
        var target = ""
        try {
            NetworkMessageSerializer().deserialize(networkFrame("sink", typedFrame(3, [99u8])))
        } catch (e: UnsupportedWireFormatException) {
            target = e.targetActor ?? ""
        }
        @Expect(target, "sink")
    }

    // ---------------- 握手：扩展对旧解析器透明 ----------------

    @TestCase
    public func handshakeExtensionIsTransparentToLegacyParsers(): Unit {
        for (cookie in [None<String>, Some("da6-secret")]) {
            let (n, nt) = wireNode(7101, WireProtocolSettings.defaults(), cookie)
            let (o, ot) = wireNode(7102, WireProtocolSettings.legacyEmulation(), cookie)
            try {
                // 新 HELLO：能力扩展追加在第 5 段末尾，仍是 7 段、版本 "1"，旧解析器（含 MAC 校验）照常通过
                let hello = String.fromUtf8(nt.hs().hello(NetworkAddress("127.0.0.1", 7102)))
                @Expect(hello.split("|").size, 7)
                @Expect(hello.split("|")[5], "127.0.0.1:7102;wp=2;wc=1")
                @Expect(legacyParserAccepts(hello, cookie), true)
                // 旧节点（模拟）接受新 HELLO，回旧格式 ACK：第 5 段只有回显的 mac
                let ack = ot.hs().accept(hello.toArray()).getOrThrow()[0]
                let ackParts = String.fromUtf8(ack).split("|")
                @Expect(ackParts.size, 7)
                @Expect(ackParts[5].contains(";"), false)
                // 新节点据旧 ACK 把对端记为旧版本，按保守能力编码
                @Expect(nt.hs().confirm(NetworkAddress("127.0.0.1", 7102), ack), true)
                let p = n.peerWireProfile(NetworkAddress("127.0.0.1", 7102)).getOrThrow()
                @Expect(p.negotiated, false)
                @Expect(p.protocolVersion, WireProtocol.LEGACY_VERSION)

                // 抓下来的旧格式 HELLO（HEAD 714d897 写出的字节）：新节点接受，并回**不带扩展**的 ACK——
                // 旧连接方在有密钥时逐字比较回显的 mac，带扩展会让它拒绝
                let body = "HELLO|1|127.0.0.1:7103|4242|${DateTime.now().toUnixTimeStamp().toMilliseconds()}|127.0.0.1:7101"
                let mac = match (cookie) {
                    case Some(c) => toHex(hmacSha256(c.toArray(), body.toArray()))
                    case None => "-"
                }
                let legacyAck = String.fromUtf8(nt.hs().accept("${body}|${mac}".toArray()).getOrThrow()[0])
                @Expect(legacyAck.split("|").size, 7)
                @Expect(legacyAck.split("|")[5], mac)
                let lp = n.peerWireProfile(NetworkAddress("127.0.0.1", 7103)).getOrThrow()
                @Expect(lp.negotiated, false)
                @Expect(lp.uid, 4242)

                // 新 ↔ 新：ACK 带扩展，双方都记下对端能力
                let (n2, nt2) = wireNode(7104, WireProtocolSettings.defaults(), cookie)
                try {
                    let h2 = nt.hs().hello(NetworkAddress("127.0.0.1", 7104))
                    let a2 = nt2.hs().accept(h2).getOrThrow()[0]
                    @Expect(String.fromUtf8(a2).split("|")[5].contains(";wp=2;wc=1"), true)
                    @Expect(nt.hs().confirm(NetworkAddress("127.0.0.1", 7104), a2), true)
                    let p2 = n.peerWireProfile(NetworkAddress("127.0.0.1", 7104)).getOrThrow()
                    @Expect(p2.negotiated, true)
                    @Expect(p2.capabilities, WireProtocol.CAP_TYPED_CODEC_V3)
                    @Expect(n2.peerWireProfile(NetworkAddress("127.0.0.1", 7101)).getOrThrow().negotiated, true)
                } finally { n2.stop() }
            } finally {
                n.stop()
                o.stop()
            }
        }
    }

    @TestCase
    public func handshakeBelowMinimumOrMalformedIsRejectedWithReason(): Unit {
        let (strict, st) = wireNode(7111, WireProtocolSettings.defaults().withMinPeerProtocol(WireProtocol.CURRENT_VERSION), None<String>)
        try {
            // 旧格式 HELLO（协议 1）低于下限 2：拒绝，单独计数，不登记身份
            @Expect(st.hs().accept("HELLO|1|127.0.0.1:7112|7|0|127.0.0.1:7111|-".toArray()).isNone(), true)
            @Expect(strict.wireProtocolRejected(), 1)
            @Expect(strict.rejectedHandshakes(), 1)
            @Expect(strict.peerWireProfile(NetworkAddress("127.0.0.1", 7112)).isNone(), true)
            // 宣告协议 0：同样按"低于下限"拒绝
            @Expect(st.hs().accept("HELLO|1|127.0.0.1:7113|8|0|127.0.0.1:7111;wp=0;wc=0|-".toArray()).isNone(), true)
            @Expect(strict.wireProtocolRejected(), 2)
            // 扩展非法：不猜，拒绝
            @Expect(st.hs().accept("HELLO|1|127.0.0.1:7114|9|0|127.0.0.1:7111;wp=x|-".toArray()).isNone(), true)
            @Expect(strict.wireProtocolRejected(), 3)
            // 未知键忽略（向前兼容），宣告协议 2 通过
            @Expect(st.hs().accept("HELLO|1|127.0.0.1:7115|10|0|127.0.0.1:7111;wp=2;wc=3;zz=1|-".toArray()).isSome(), true)
            let p = strict.peerWireProfile(NetworkAddress("127.0.0.1", 7115)).getOrThrow()
            @Expect(p.capabilities, 3)
            @ExpectThrows[IllegalArgumentException](WireProtocolSettings.defaults().withMinPeerProtocol(0))
            @ExpectThrows[IllegalArgumentException](WireProtocolSettings.defaults().withMinPeerProtocol(WireProtocol.CURRENT_VERSION + 1))
        } finally { strict.stop() }
    }

    // ---------------- 真实 TCP 的兼容矩阵 ----------------

    /** 新 ↔ 新：协商出 v3，kind 3 带协议标记往返；没有不兼容计数 */
    @TestCase
    public func newToNewKeepsFullFeatures(): Unit {
        let a = tcpNode(WireProtocolSettings.defaults())
        let b = tcpNode(WireProtocolSettings.defaults())
        try {
            let sink = TypedSink()
            b.registerHandler("sink", sink.handler())
            let ref = a.remoteActorOf(b.getLocalAddress(), "sink")
            ref.tell(TypedPayloadBox(StringMessage("hello")))
            @Expect(sink.rec.await("hello@-", T5S), true)
            @Expect(awaitCondition(T5S, { => a.peerWireProfile(b.getLocalAddress()).isSome() }), true)
            @Expect(a.peerWireProfile(b.getLocalAddress()).getOrThrow().capabilities, WireProtocol.CAP_TYPED_CODEC_V3)
            ref.tell(TypedPayloadBox(StringMessage("tagged"), typeTag: TAG))
            @Expect(sink.rec.await("tagged@tests.da6.Echo.v1", T5S), true)
            @Expect(a.wireIncompatibleSent(), 0)
            @Expect(b.wireIncompatibleReceived(), 0)
        } finally {
            a.stop()
            b.stop()
        }
    }

    /** 新 → 旧：kind 1/2 降级送达；kind 3 的 tell / ask / 可靠投递都在发送侧明确失败，计数并通知监听 */
    @TestCase
    public func newToLegacyFallsBackOrFailsLoudly(): Unit {
        ensureDa6NumCodec()
        let a = tcpNode(WireProtocolSettings.defaults())
        let old = tcpNode(WireProtocolSettings.legacyEmulation())
        try {
            let events = EventRecorder()
            a.onWireIncompatible({ e =>
                events.record("${e.direction}|${e.targetActor}|${e.messageType}|${e.message.isSome()}")
            })
            let sink = TypedSink()
            old.registerHandler("sink", sink.handler())
            let ref = a.remoteActorOf(old.getLocalAddress(), "sink")
            ref.tell(TypedPayloadBox(StringMessage("k1")))
            ref.tell(TypedPayloadBox(Da6Num(5), typeTag: TAG))
            @Expect(sink.rec.await("k1@-", T5S), true)
            @Expect(sink.rec.await("num:5@tests.da6.Echo.v1", T5S), true)
            let p = a.peerWireProfile(old.getLocalAddress()).getOrThrow()
            @Expect(p.negotiated, false)

            // kind 3：tell 抛出（不是"发送成功"），计数 + 监听拿到原消息（可转死信）
            @ExpectThrows[WireIncompatibleException](ref.tell(TypedPayloadBox(StringMessage("k3"), typeTag: TAG)))
            @Expect(a.wireIncompatibleSent(), 1)
            @Expect(events.await("outbound|sink|TypedPayloadBox|true", T5S), true)
            // ask：Promise 以 WireIncompatibleException 失败，不等超时
            let promise = a.ask(ref, TypedPayloadBox(StringMessage("ask"), typeTag: TAG), T5S)
            @Expect(promise.isCompleted(), true)
            var askFailed = false
            try {
                promise.get(T5S)
            } catch (e: WireIncompatibleException) {
                askFailed = true
            }
            @Expect(askFailed, true)
            @Expect(a.pendingAskCount(), 0)
            // 可靠投递：入队前拒绝，不占未确认额度、不会被重发到放弃
            @ExpectThrows[WireIncompatibleException](a.tellReliable(ref, TypedPayloadBox(StringMessage("rel"), typeTag: TAG)))
            @Expect(a.reliableStats()[2], 0)
            @Expect(a.wireIncompatibleSent(), 3)
            // 连接未受影响：随后的兼容消息照常送达，失败的那几条一条也没漏过去
            ref.tell(TypedPayloadBox(StringMessage("after")))
            @Expect(sink.rec.await("after@-", T5S), true)
            @Expect(sink.rec.count("k3@tests.da6.Echo.v1") + sink.rec.count("ask@tests.da6.Echo.v1") +
                sink.rec.count("rel@tests.da6.Echo.v1"), 0)
            @Expect(old.wireIncompatibleReceived(), 0)
        } finally {
            a.stop()
            old.stop()
        }
    }

    /** 旧 → 新：旧节点照常握手、照常写 v3（含 kind 3），新节点读得懂并保留协议标记 */
    @TestCase
    public func legacyToNewInteroperates(): Unit {
        let old = tcpNode(WireProtocolSettings.legacyEmulation())
        let b = tcpNode(WireProtocolSettings.defaults())
        try {
            let sink = TypedSink()
            b.registerHandler("sink", sink.handler())
            let ref = old.remoteActorOf(b.getLocalAddress(), "sink")
            ref.tell(TypedPayloadBox(StringMessage("from-old")))
            ref.tell(TypedPayloadBox(StringMessage("from-old-tagged"), typeTag: TAG))
            @Expect(sink.rec.await("from-old@-", T5S), true)
            @Expect(sink.rec.await("from-old-tagged@tests.da6.Echo.v1", T5S), true)
            @Expect(old.wireIncompatibleSent(), 0)
            @Expect(b.peerWireProfile(old.getLocalAddress()).getOrThrow().negotiated, false)
        } finally {
            old.stop()
            b.stop()
        }
    }

    /**
     * v1.2.x 接收方（只认 codec v2）：新节点降级写 v2 → 送达；
     * 对照组：未升级的发送方照旧写 v3 → 接收方读不懂，计入 wireIncompatibleReceived、不投递（这正是协商之前的静默失败）
     */
    @TestCase
    public func v2OnlyReceiverGetsDowngradedFramesAndCountsNewerOnes(): Unit {
        let fresh = tcpNode(WireProtocolSettings.defaults())
        let unupgraded = tcpNode(WireProtocolSettings.legacyEmulation())
        let v12 = tcpNode(WireProtocolSettings.legacyEmulation())
        MessageCodecRegistry.register(TYPED_PAYLOAD_BOX_TYPE, V2ReaderTypedCodec())
        try {
            let inbound = EventRecorder()
            v12.onWireIncompatible({ e => inbound.record("${e.direction}|${e.targetActor}") })
            let sink = TypedSink()
            v12.registerHandler("sink", sink.handler())
            fresh.remoteActorOf(v12.getLocalAddress(), "sink").tell(TypedPayloadBox(StringMessage("downgraded")))
            @Expect(sink.rec.await("downgraded@-", T5S), true)
            @Expect(v12.wireIncompatibleReceived(), 0)

            DA6_NEW_WRITER.set(Some(true))
            try {
                unupgraded.remoteActorOf(v12.getLocalAddress(), "sink").tell(TypedPayloadBox(StringMessage("v3-frame")))
            } finally {
                DA6_NEW_WRITER.set(Some(false))
            }
            @Expect(awaitCondition(T5S, { => v12.wireIncompatibleReceived() == 1 }), true)
            @Expect(inbound.await("inbound|sink", T5S), true)
            // 连接仍可用：之后的兼容帧照常送达，读不懂的那条没有被投递
            fresh.remoteActorOf(v12.getLocalAddress(), "sink").tell(TypedPayloadBox(StringMessage("still-ok")))
            @Expect(sink.rec.await("still-ok@-", T5S), true)
            @Expect(sink.rec.count("v3-frame@-"), 0)
        } finally {
            MessageCodecRegistry.register(TYPED_PAYLOAD_BOX_TYPE, TypedPayloadBoxCodec())
            fresh.stop()
            unupgraded.stop()
            v12.stop()
        }
    }

    /** 抓包字节直连：旧格式 HELLO 经真实 TCP 握手成功，随后的未知 kind 帧被计数、不投递、不断连 */
    @TestCase
    public func rawLegacyPeerOverTcpAndUnknownKindIsCounted(): Unit {
        let b = tcpNode(WireProtocolSettings.defaults())
        let raw = TcpSocket("127.0.0.1", UInt16(b.getLocalAddress().port))
        try {
            let sink = TypedSink()
            b.registerHandler("sink", sink.handler())
            raw.connect()
            raw.readTimeout = T5S
            raw.write(encodeFrame("HELLO|1|127.0.0.1:7121|5151|0|${b.getLocalAddress().toString()}|-".toArray()))
            let ack = String.fromUtf8(readFrame(raw)).split("|")
            @Expect(ack.size, 7)
            @Expect(ack[0], "HELLO-ACK")
            @Expect(ack[5], "-") // 旧连接方：ACK 不带扩展
            @Expect(b.peerWireProfile(NetworkAddress("127.0.0.1", 7121)).getOrThrow().negotiated, false)
            // 未知 kind 99：计数、不投递
            raw.write(encodeFrame(networkFrame("sink", typedFrame(3, [99u8, 1u8, 2u8]))))
            @Expect(awaitCondition(T5S, { => b.wireIncompatibleReceived() == 1 }), true)
            // 同一连接上随后的正常帧照常送达（读不懂的帧没有毁掉连接）
            let okFrame = NetworkMessageSerializer().serialize(
                NetworkMessage("sink", TypedPayloadBox(StringMessage("after-unknown")), None<String>))
            raw.write(encodeFrame(okFrame))
            @Expect(sink.rec.await("after-unknown@-", T5S), true)
            @Expect(sink.rec.size(), 1)
        } finally {
            try { raw.close() } catch (_: Exception) {}
            b.stop()
        }
    }

    /** 滚动升级收尾：调高协议下限后，未升级节点的真实 TCP 握手被拒，消息到不了 */
    @TestCase
    public func minimumProtocolRejectsLegacyNodeOverTcp(): Unit {
        let strict = tcpNode(WireProtocolSettings.defaults().withMinPeerProtocol(WireProtocol.CURRENT_VERSION))
        let old = tcpNode(WireProtocolSettings.legacyEmulation())
        let fresh = tcpNode(WireProtocolSettings.defaults())
        try {
            let sink = TypedSink()
            strict.registerHandler("sink", sink.handler())
            old.remoteActorOf(strict.getLocalAddress(), "sink").tell(StringMessage("legacy"))
            @Expect(awaitCondition(T5S, { => strict.wireProtocolRejected() >= 1 }), true)
            fresh.remoteActorOf(strict.getLocalAddress(), "sink").tell(StringMessage("fresh"))
            @Expect(sink.rec.await("plain:fresh", T5S), true)
            @Expect(sink.rec.count("plain:legacy"), 0)
        } finally {
            strict.stop()
            old.stop()
            fresh.stop()
        }
    }
}

func ensureTypedPayloadCodecRegisteredForTest(): Unit {
    // 其它用例可能换过注册的实现，这里显式恢复标准编解码器
    MessageCodecRegistry.register(TYPED_PAYLOAD_BOX_TYPE, TypedPayloadBoxCodec())
}
