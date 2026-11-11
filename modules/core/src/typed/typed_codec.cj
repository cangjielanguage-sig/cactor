/*
 * CActor Core.Typed - 类型化消息的跨进程编解码
 *
 * 类型化 Actor 之间用 `TypedPayloadBox` 传递消息（非泛型载体，见 typed_adapter.cj 的性能说明）。
 * 本机投递无所谓：载荷就是一个 `Any`。但跨进程必须把载荷变成字节，而 `Any` 在编译期已经擦除，
 * 默认序列化器只会报"未知消息类型"。
 *
 * 两条路，优先级从高到低：
 * 1. 载荷本身实现 `Message`：嵌套一帧完整消息（类型名 + 版本 + 负载），复用载荷自己的 codec，
 *    用户不需要为类型化通道再写一套编解码。
 * 2. 载荷不是 `Message`（类型化协议最常见的就是普通 `enum`）：用户用
 *    `TypedPayloadCodecs.register<T>(typeName:, encode:, decode:)` 注册一对闭包。注册时按 `v is T`
 *    记录匹配函数，发送时按注册顺序找第一个匹配的类型。编解码本身仍是用户写的显式代码——
 *    core 层没有反射式序列化，也不该为了让消息过线而在热路径上做运行期类型查找。
 *
 * 都不满足时在**发送侧**抛 SerializationException：这不是限制，而是把"这条消息能不能跨节点"
 * 变成用户看得见的选择——闭包、连接、ActorRef 本来就不该被悄悄送去网络。
 *
 * 线上格式：v2 的 kind 1 = 嵌套消息帧，kind 2 = 命名载荷；v3 增加 kind 3 = 带协议标记的嵌套消息。
 * 新解码器保留 v1/v2 支持。显式协议标记的 Message 载荷要求两端均升级到 v3，不支持旧节点读取 kind 3。
 */
package cactor_core.typed

import std.collection.*
import std.sync.*
import cactor_core.message.{Message, MessageSerializer, MessageCodec, MessageCodecRegistry, SerializationException, DeserializationException}

/** 线上名称：与 TypedPayloadBox.messageType() 保持一致 */
public let TYPED_PAYLOAD_BOX_TYPE: String = "TypedPayloadBox"

/**
 * 类型期望（type expectation）：一条消息声明的"我本来该是什么类型"。
 *
 * 类型化载荷仍由接收侧 `as T` 校验；协议名为显式声明的分布式边界额外提供诊断依据。
 * 声明不匹配时拒收（不投递、计入未处理、限速告警）。协议名不能替代载荷类型检查或身份认证。
 *
 * 类型名从哪来（约定）：
 * - 命名载荷（注册了编解码器的类型）：编解码器的 typeName；
 * - Message 载荷：messageType()（MessageSerializer 本来就把类型名写进了线上帧）；
 * - 显式声明：引用上挂的 TypedTypeTag（发布方声明；跨节点时随命名载荷帧的尾部一起过线）。
 *
 * 类型期望是**可选**的：没有声明就没有校验，行为与引入本机制之前完全一致。
 */
public class TypedTypeTag {
    /** 协议类型名（双方约定的字符串，建议带包名） */
    public let typeName: String

    public init(typeName: String) {
        if (typeName.isEmpty()) {
            throw IllegalArgumentException("类型期望的类型名不能为空")
        }
        this.typeName = typeName
    }

    public func toString(): String {
        "TypedTypeTag(${typeName})"
    }

    public func equals(other: TypedTypeTag): Bool {
        typeName == other.typeName
    }
}

/** 由协议类型名构造类型期望（空名 = 不声明，沿用旧行为） */
public func typedTypeTagOf(typeName: Option<String>): Option<TypedTypeTag> {
    match (typeName) {
        case Some(n) =>
            if (n.isEmpty()) {
                None<TypedTypeTag>
            } else {
                Some(TypedTypeTag(n))
            }
        case None => None<TypedTypeTag>
    }
}

/**
 * 类型期望的可观测计数（进程级）。
 *
 * - checked：按类型名校验通过的消息数；
 * - mismatch：类型名对不上、被拒收的消息数（这些消息同时计入 Actor 的未处理消息数，会进死信）；
 * - unverified：声明了期望但消息没有可比对的名字（无从校验，放行）。
 *
 * 拒收的原文放在 lastMismatchText()，供测试与运维定位。
 */
public class TypedTypeExpectations {
    private static let lock: Mutex = Mutex()
    private static var checkedValue: Int64 = 0
    private static var mismatchValue: Int64 = 0
    private static var unverifiedValue: Int64 = 0
    private static var lastMismatchTextValue: String = ""

    /** 校验通过一条 */
    public static func recordChecked(): Unit {
        synchronized(lock) {
            checkedValue++
        }
    }

    /** 类型不符拒收一条（detail 记入 lastMismatchText，供观测） */
    public static func recordMismatch(detail: String): Unit {
        synchronized(lock) {
            mismatchValue++
            lastMismatchTextValue = detail
        }
    }

    /** 声明了期望但消息没有类型名：无从校验，放行 */
    public static func recordUnverified(): Unit {
        synchronized(lock) {
            unverifiedValue++
        }
    }

    public static func checkedCount(): Int64 {
        synchronized(lock) {
            checkedValue
        }
    }

    public static func mismatchCount(): Int64 {
        synchronized(lock) {
            mismatchValue
        }
    }

    public static func unverifiedCount(): Int64 {
        synchronized(lock) {
            unverifiedValue
        }
    }

    /** 最近一次类型不符的描述（没有则为空串） */
    public static func lastMismatchText(): String {
        synchronized(lock) {
            lastMismatchTextValue
        }
    }

    /** 清零（仅测试用） */
    public static func resetForTest(): Unit {
        synchronized(lock) {
            checkedValue = 0
            mismatchValue = 0
            unverifiedValue = 0
            lastMismatchTextValue = ""
        }
    }
}

/**
 * 命名载荷的编解码器：把"不是 Message 的类型"变成字节。
 *
 * 只描述编码方式，不承担类型安全——类型名是双方约定的字符串，写错了只会得到解码失败，
 * 不会得到"看起来能跑但语义错位"的消息。
 */
public interface TypedPayloadCodec {
    /** 线上类型名（双方必须一致，建议带包名） */
    func typeName(): String

    /** 判断某个载荷是否由本编解码器处理 */
    func matches(value: Any): Bool

    func encode(value: Any): Array<UInt8>

    func decode(bytes: Array<UInt8>): Any
}

/** 由 `TypedPayloadCodecs.register<T>` 生成的实现：`v is T` 判定，`v as T` 转换 */
class TypedPayloadCodecImpl<T> <: TypedPayloadCodec {
    private let name: String
    private let encodeTyped: (T) -> Array<UInt8>
    private let decodeTyped: (Array<UInt8>) -> T

    init(name: String, encodeTyped: (T) -> Array<UInt8>, decodeTyped: (Array<UInt8>) -> T) {
        this.name = name
        this.encodeTyped = encodeTyped
        this.decodeTyped = decodeTyped
    }

    public func typeName(): String {
        name
    }

    public func matches(value: Any): Bool {
        value is T
    }

    public func encode(value: Any): Array<UInt8> {
        match (value as T) {
            case Some(v) => encodeTyped(v)
            // matches() 已经保证过，走到这里说明有人绕过 matches 直接调 encode
            case None => throw SerializationException("载荷与编解码器 ${name} 声明的类型不符")
        }
    }

    public func decode(bytes: Array<UInt8>): Any {
        decodeTyped(bytes)
    }
}

/**
 * 命名载荷编解码器注册表（进程级，线程安全）。
 *
 * 注册顺序即匹配顺序：同一父类型注册多个子类型时，先注册的优先。类型名重复直接拒绝——
 * 与 `MessageCodecRegistry.register(name, codec)` 的静默覆盖不同，这里覆盖会让两个节点对同一
 * 字节串得出不同结论，属于必须立刻暴露的错误。
 */
public class TypedPayloadCodecs {
    private static let codecs: ArrayList<TypedPayloadCodec> = ArrayList<TypedPayloadCodec>()
    // 仅声明类型名（不提供编解码器）的条目：与编解码器共享同一份类型名空间
    private static let declared: ArrayList<DeclaredTypeName> = ArrayList<DeclaredTypeName>()
    private static let lock: Mutex = Mutex()

    public static func register(codec: TypedPayloadCodec): Unit {
        synchronized(lock) {
            if (codecs.any({ c => c.typeName() == codec.typeName() })) {
                throw IllegalStateException("载荷类型 ${codec.typeName()} 已注册过编解码器")
            }
            codecs.add(codec)
        }
    }

    /**
     * 为类型 T 注册编解码器。
     *
     * 例：`TypedPayloadCodecs.register<CalcMsg>(typeName: "demo.CalcMsg", encode: { ... }, decode: { ... })`
     */
    public static func register<T>(typeName!: String, encode!: (T) -> Array<UInt8>, decode!: (Array<UInt8>) -> T): Unit {
        if (typeName.isEmpty()) {
            throw IllegalArgumentException("载荷类型名不能为空")
        }
        register(TypedPayloadCodecImpl<T>(typeName, encode, decode))
    }

    /**
     * 只声明一个类型名（不提供编解码器）：类型名与编解码器共用同一份命名空间，
     * 重复声明直接拒绝（与编解码器重名同样抛 IllegalStateException）。
     *
     * 用途：本节点内的类型化协议也需要一个稳定的名字（例如集群 Receptionist 登记服务时记录的类型名），
     * 但它不需要跨节点传输，因此不必写编解码闭包。
     */
    public static func declareTypeName<T>(typeName!: String): Unit {
        if (typeName.isEmpty()) {
            throw IllegalArgumentException("类型名不能为空")
        }
        let matcher: (Any) -> Bool = { v => v is T }
        synchronized(lock) {
            if (codecs.any({ c => c.typeName() == typeName })) {
                throw IllegalStateException("类型名 ${typeName} 已被编解码器占用")
            }
            for (d in declared) {
                if (d.name == typeName) {
                    throw IllegalStateException("类型名 ${typeName} 已声明过")
                }
            }
            declared.add(DeclaredTypeName(typeName, matcher))
        }
    }

    /** 按值查协议类型名：先找编解码器，再找仅声明的类型名（都没命中则没有类型名） */
    public static func typeNameOfValue(value: Any): Option<String> {
        synchronized(lock) {
            for (c in codecs) {
                if (c.matches(value)) {
                    return Some(c.typeName())
                }
            }
            for (d in declared) {
                if (d.matcher(value)) {
                    return Some(d.name)
                }
            }
        }
        None<String>
    }

    public static func unregister(typeName: String): Bool {
        synchronized(lock) {
            for (i in 0..codecs.size) {
                if (codecs[i].typeName() == typeName) {
                    codecs.remove(at: i)
                    return true
                }
            }
        }
        false
    }

    public static func find(typeName: String): Option<TypedPayloadCodec> {
        synchronized(lock) {
            for (c in codecs) {
                if (c.typeName() == typeName) {
                    return Some(c)
                }
            }
        }
        None<TypedPayloadCodec>
    }

    public static func findFor(value: Any): Option<TypedPayloadCodec> {
        synchronized(lock) {
            for (c in codecs) {
                if (c.matches(value)) {
                    return Some(c)
                }
            }
        }
        None<TypedPayloadCodec>
    }

    public static func count(): Int64 {
        synchronized(lock) {
            codecs.size
        }
    }

    /** 已注册的线上类型名（编解码器 + 仅声明的，按注册顺序），用于诊断两端配置是否一致 */
    public static func typeNames(): Array<String> {
        synchronized(lock) {
            let out = ArrayList<String>()
            for (c in codecs) {
                out.add(c.typeName())
            }
            for (d in declared) {
                out.add(d.name)
            }
            out.toArray()
        }
    }

    /** 清空（仅测试用：进程级注册表在用例之间会互相影响） */
    public static func clearForTest(): Unit {
        synchronized(lock) {
            codecs.clear()
            declared.clear()
        }
    }
}

/** 仅声明类型名的条目（没有编解码器，只用于类型名的匹配与查询） */
class DeclaredTypeName {
    let name: String
    let matcher: (Any) -> Bool

    init(name: String, matcher: (Any) -> Bool) {
        this.name = name
        this.matcher = matcher
    }
}

private let PAYLOAD_KIND_MESSAGE: UInt8 = 1
private let PAYLOAD_KIND_REGISTERED: UInt8 = 2

/**
 * `TypedPayloadBox` 的编解码器。
 *
 * 注意：解码方只还原载荷与信封，`as T` 的转换仍由接收侧用户包里生成的桥接闭包完成
 * （core 泛型代码里做 `as T` 会在运行时查类型信息，是热路径的主要开销）。
 */
public class TypedPayloadBoxCodec <: MessageCodec {
    public init() {}

    public func version(): Int64 {
        3
    }

    public func encode(message: Message): Array<UInt8> {
        let box = match (message as TypedPayloadBox) {
            case Some(b) => b
            case None => throw SerializationException("TypedPayloadBoxCodec 只能编码 TypedPayloadBox")
        }
        match (box.payload as Message) {
            case Some(payload) =>
                let out = ArrayList<UInt8>()
                let frame = MessageSerializer().serialize(payload)
                match (box.typeTag) {
                    case Some(tag) =>
                        // v3 的 kind 3 用长度隔离内层帧，避免把协议标记交给载荷的解码器。
                        out.add(3u8)
                        putInt32(out, Int32(frame.size))
                        for (b in frame) { out.add(b) }
                        let tagBytes = tag.typeName.toArray()
                        putInt32(out, Int32(tagBytes.size))
                        for (b in tagBytes) { out.add(b) }
                    case None =>
                        out.add(PAYLOAD_KIND_MESSAGE)
                        for (b in frame) { out.add(b) }
                }
                out.toArray()
            case None =>
                match (TypedPayloadCodecs.findFor(box.payload)) {
                    case Some(codec) =>
                        let bytes = codec.encode(box.payload)
                        let name = codec.typeName().toArray()
                        let out = ArrayList<UInt8>()
                        out.add(PAYLOAD_KIND_REGISTERED)
                        putInt32(out, Int32(name.size))
                        for (b in name) {
                            out.add(b)
                        }
                        putInt32(out, Int32(bytes.size))
                        for (b in bytes) {
                            out.add(b)
                        }
                        // 类型期望（可选）：发布方声明的类型名随消息过线，接收端据此拒收协议不符的消息。
                        // 旧解码器按长度切片取 body，会忽略这段尾部，因此滚动升级期间新旧节点可以互读。
                        match (box.typeTag) {
                            case Some(tag) =>
                                let tagBytes = tag.typeName.toArray()
                                putInt32(out, Int32(tagBytes.size))
                                for (b in tagBytes) {
                                    out.add(b)
                                }
                            case None => ()
                        }
                        out.toArray()
                    case None =>
                        throw SerializationException(
                            "类型化消息的载荷无法跨进程传输：载荷既不是 Message，也没有注册命名编解码器。" +
                            "请让协议类型实现 Message，或用 TypedPayloadCodecs.register<T>(typeName:, encode:, decode:) " +
                            "注册一对编解码闭包；不想跨节点的消息请留在本节点发送")
                }
        }
    }

    public func decode(payload: Array<UInt8>): Message {
        // 版本 1 的帧没有 kind 字节，整段就是嵌套消息帧
        TypedPayloadBox(MessageSerializer().deserialize(payload))
    }

    public func decode(payload: Array<UInt8>, version: Int64): Message {
        if (version < 2) {
            return decode(payload)
        }
        if (payload.size < 1) {
            throw DeserializationException("类型化信封为空")
        }
        // 注意：这里必须写字面量。仓颉 match 里的小写/非枚举名字会当成"绑定变量"，
        // 写成 case PAYLOAD_KIND_MESSAGE 会把所有取值都吃掉（踩过一次）。
        match (payload[0]) {
            case 1u8 => decode(payload.slice(1, payload.size - 1))
            case 3u8 =>
                if (version < 3) {
                    throw DeserializationException("带协议标记的嵌套消息需要 codec v3")
                }
                let bodyLength = Int64(readInt32(payload, 1))
                if (bodyLength < 0 || bodyLength > payload.size - 5) {
                    throw DeserializationException("类型化信封的嵌套消息不完整")
                }
                let tagOffset = 5 + bodyLength
                let tagLength = Int64(readInt32(payload, tagOffset))
                if (tagLength <= 0 || tagLength != payload.size - tagOffset - 4) {
                    throw DeserializationException("类型化信封的协议标记不完整")
                }
                let tag = TypedTypeTag(String.fromUtf8(payload.slice(tagOffset + 4, tagLength)))
                TypedPayloadBox(MessageSerializer().deserialize(payload.slice(5, bodyLength)), typeTag: Some(tag))
            case 2u8 =>
                var offset = 1
                let nameLength = Int64(readInt32(payload, offset))
                offset += 4
                if (nameLength < 0 || payload.size < offset + nameLength) {
                    throw DeserializationException("类型化信封的载荷类型名不完整")
                }
                let name = String.fromUtf8(payload.slice(offset, nameLength))
                offset += nameLength
                let bodyLength = Int64(readInt32(payload, offset))
                offset += 4
                if (bodyLength < 0 || payload.size < offset + bodyLength) {
                    throw DeserializationException("类型化信封的载荷数据不完整：类型 ${name}")
                }
                let body = payload.slice(offset, bodyLength)
                offset += bodyLength
                // 可选的类型期望（发送方声明的类型名）：没有就用帧里的载荷类型名（它同样随消息过线）
                var tag: Option<TypedTypeTag> = None<TypedTypeTag>
                if (payload.size > offset) {
                    let tagLength = Int64(readInt32(payload, offset))
                    offset += 4
                    if (tagLength < 0 || payload.size < offset + tagLength) {
                        throw DeserializationException("类型化信封的类型期望标记不完整：类型 ${name}")
                    }
                    let tagName = String.fromUtf8(payload.slice(offset, tagLength))
                    if (!tagName.isEmpty()) {
                        tag = Some(TypedTypeTag(tagName))
                    }
                }
                let effectiveTag = match (tag) {
                    case Some(t) => t
                    case None => TypedTypeTag(name)
                }
                match (TypedPayloadCodecs.find(name)) {
                    case Some(codec) => TypedPayloadBox(codec.decode(body), typeTag: Some(effectiveTag))
                    case None => throw DeserializationException(
                        "没有注册载荷类型 ${name} 的编解码器：两端 TypedPayloadCodecs 配置不一致")
                }
            case other =>
                throw DeserializationException("未知的类型化信封种类：${other}")
        }
    }
}

private func putInt32(out: ArrayList<UInt8>, value: Int32): Unit {
    out.add(UInt8((value >> 24) & 0xFF))
    out.add(UInt8((value >> 16) & 0xFF))
    out.add(UInt8((value >> 8) & 0xFF))
    out.add(UInt8(value & 0xFF))
}

private func readInt32(data: Array<UInt8>, offset: Int64): Int32 {
    if (data.size < offset + 4) {
        throw DeserializationException("类型化信封数据不完整")
    }
    (Int32(data[offset]) << 24) |
    (Int32(data[offset + 1]) << 16) |
    (Int32(data[offset + 2]) << 8) |
    Int32(data[offset + 3])
}

/**
 * 注册类型化消息编解码器（幂等）。远程层启动时调用一次即可。
 *
 * 已存在同名 codec 时不覆盖：用户可能显式注册了兼容旧版本的实现，启动流程不该把它顶掉。
 */
public func ensureTypedPayloadCodecRegistered(): Bool {
    if (MessageCodecRegistry.isRegistered(TYPED_PAYLOAD_BOX_TYPE)) {
        return false
    }
    MessageCodecRegistry.register(TYPED_PAYLOAD_BOX_TYPE, TypedPayloadBoxCodec())
    true
}
