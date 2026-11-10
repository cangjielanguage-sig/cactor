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
 * 线上格式（codec 版本 2）：首字节 kind，1 = 嵌套消息帧，2 = 命名载荷（[i32 名字长度][名字][负载]）。
 * 版本 1 的旧帧没有 kind 字节，解码时按 kind 1 处理，因此滚动升级期间新旧节点可以互读。
 */
package cactor_core.typed

import std.collection.*
import std.sync.*
import cactor_core.message.{Message, MessageSerializer, MessageCodec, MessageCodecRegistry, SerializationException, DeserializationException}

/** 线上名称：与 TypedPayloadBox.messageType() 保持一致 */
public let TYPED_PAYLOAD_BOX_TYPE: String = "TypedPayloadBox"

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

    /** 已注册的线上类型名（按注册顺序），用于诊断两端配置是否一致 */
    public static func typeNames(): Array<String> {
        synchronized(lock) {
            codecs.map { c => c.typeName() }.toArray()
        }
    }

    /** 清空（仅测试用：进程级注册表在用例之间会互相影响） */
    public static func clearForTest(): Unit {
        synchronized(lock) {
            codecs.clear()
        }
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
        2
    }

    public func encode(message: Message): Array<UInt8> {
        let box = match (message as TypedPayloadBox) {
            case Some(b) => b
            case None => throw SerializationException("TypedPayloadBoxCodec 只能编码 TypedPayloadBox")
        }
        match (box.payload as Message) {
            case Some(payload) =>
                let out = ArrayList<UInt8>()
                out.add(PAYLOAD_KIND_MESSAGE)
                for (b in MessageSerializer().serialize(payload)) {
                    out.add(b)
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
                match (TypedPayloadCodecs.find(name)) {
                    case Some(codec) => TypedPayloadBox(codec.decode(body))
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
