/*
 * Copyright (c) 2024. All rights reserved.
 * Core Layer - 消息序列化器
 * 基于Foundation层序列化框架的消息序列化实现
 */
package cactor_core.message

import std.collection.*
import std.sync.*
import cactor_foundation.serialization.{Serializer, SerializationManager}

/** 版本帧标记：写在负载长度的位置（合法长度不会为负） */
const VERSIONED_FRAME: Int32 = -1

/** 当前线程正在为哪种能力的对端编码（嵌套编解码器据此沿用外层的对端能力，见 serialize） */
let AMBIENT_PEER_CAPABILITIES = ThreadLocal<UInt64>()

/**
 * 消息序列化器
 * 基于Foundation层的通用序列化框架实现消息序列化
 */
public class MessageSerializer <: Serializer<Message> {
    private let serializationManager: SerializationManager

    public init() {
        this.serializationManager = SerializationManager()
    }

    /**
     * 序列化。处在某次 serialize(message, peerCapabilities) 内部时（嵌套信封的编解码器在 encode 里
     * 再调 MessageSerializer().serialize(内层)）沿用外层的对端能力，因此分片信封、可靠投递信封里
     * 嵌套的类型化消息同样按对端能力编码，编解码器不需要逐个改签名。
     */
    public func serialize(message: Message): Array<UInt8> {
        serialize(message, AMBIENT_PEER_CAPABILITIES.get() ?? WireProtocol.LOCAL_CAPABILITIES)
    }

    /**
     * 按对端能力序列化（DA6）：peerCapabilities 是对端在握手里宣告的能力位。
     * 编解码器能降级时写对端读得懂的旧版本；不能降级时抛 WireIncompatibleException，帧不会发出。
     */
    public func serialize(message: Message, peerCapabilities: UInt64): Array<UInt8> {
        let previous = AMBIENT_PEER_CAPABILITIES.get()
        AMBIENT_PEER_CAPABILITIES.set(Some(peerCapabilities))
        try {
            serializeFrame(message, peerCapabilities)
        } finally {
            AMBIENT_PEER_CAPABILITIES.set(previous)
        }
    }

    private func serializeFrame(message: Message, peerCapabilities: UInt64): Array<UInt8> {
        let messageType = message.messageType()
        let result = ArrayList<UInt8>()
        
        // 写入消息类型长度和类型
        let typeBytes = serializationManager.serializeString(messageType)
        writeInt32(result, Int32(typeBytes.size))
        // 替代appendAll：逐个添加字节
        for (byte in typeBytes) {
            result.add(byte)
        }
        
        // 根据消息类型序列化具体内容
        match (messageType) {
            case "StringMessage" =>
                match (message as StringMessage) {
                    case Some(msg) =>
                        let contentBytes = serializationManager.serializeString(msg.getContent())
                        writeInt32(result, Int32(contentBytes.size))
                        // 替代appendAll：逐个添加字节
                        for (byte in contentBytes) {
                            result.add(byte)
                        }
                    case None => throw SerializationException("消息类型转换失败")
                }
            case "PingMessage" =>
                match (message as PingMessage) {
                    case Some(msg) =>
                        let idBytes = serializationManager.serializeInt(msg.getId())
                        // 替代appendAll：逐个添加字节
                        for (byte in idBytes) {
                            result.add(byte)
                        }
                        let timestampBytes = serializationManager.serializeInt(msg.timestamp())
                        for (byte in timestampBytes) {
                            result.add(byte)
                        }
                    case None => throw SerializationException("消息类型转换失败")
                }
            case "PongMessage" =>
                match (message as PongMessage) {
                    case Some(msg) =>
                        let idBytes = serializationManager.serializeInt(msg.getOriginalId())
                        // 替代appendAll：逐个添加字节
                        for (byte in idBytes) {
                            result.add(byte)
                        }
                    case None => throw SerializationException("消息类型转换失败")
                }
            case "StopMessage" =>
                match (message as StopMessage) {
                    case Some(msg) =>
                        let reasonBytes = serializationManager.serializeString(msg.getReason())
                        writeInt32(result, Int32(reasonBytes.size))
                        // 替代appendAll：逐个添加字节
                        for (byte in reasonBytes) {
                            result.add(byte)
                        }
                    case None => throw SerializationException("消息类型转换失败")
                }
            case _ =>
                match (MessageCodecRegistry.get(messageType)) {
                    case Some(codec) =>
                        let (version, payload) = codec.encodeFor(message, peerCapabilities)
                        if (version < 1 || version > Int64(Int32.Max)) {
                            throw SerializationException("${messageType} 的编解码器版本无效: ${version}")
                        }
                        if (version > 1) {
                            // 版本帧：长度位置写 -1，随后是版本号与真实长度。版本 1 保持旧格式，旧节点仍可读取
                            writeInt32(result, VERSIONED_FRAME)
                            writeInt32(result, Int32(version))
                        }
                        writeInt32(result, Int32(payload.size))
                        for (byte in payload) {
                            result.add(byte)
                        }
                    case None =>
                        throw SerializationException("不支持的消息类型: ${messageType}（可通过 MessageCodecRegistry.register 注册编解码器）")
                }
        }
        result.toArray()
    }

    public func deserialize(data: Array<UInt8>): Message {
        if (data.size < 4) {
            throw DeserializationException("数据不完整")
        }
        
        var offset = 0
        
        // 读取消息类型
        let typeLength = readInt32(data, offset)
        offset += 4
        
        if (data.size < offset + Int64(typeLength)) {
            throw DeserializationException("消息类型数据不完整")
        }
        
        let typeBytes = data.slice(Int64(offset), Int64(typeLength))
        let rawMessageType = serializationManager.deserializeString(typeBytes)

        let messageType = trimString(rawMessageType)  // 去除首尾空格和控制字符

        offset += Int64(typeLength)

        // 根据消息类型反序列化具体内容
        match (messageType) {
            case "StringMessage" =>
                if (data.size < offset + 4) {
                    throw DeserializationException("StringMessage内容长度数据不完整")
                }
                let contentLength = readInt32(data, offset)
                offset += 4



                if (data.size < offset + Int64(contentLength)) {
                    throw DeserializationException("StringMessage内容数据不完整: 需要${offset + Int64(contentLength)}字节，但只有${data.size}字节")
                }

                let contentBytes = data.slice(Int64(offset), Int64(contentLength))
                let content = serializationManager.deserializeString(contentBytes)
                StringMessage(content)
                
            case "PingMessage" =>
                if (data.size < offset + 16) { // 8字节ID + 8字节时间戳
                    throw DeserializationException("PingMessage数据不完整")
                }
                
                let idBytes = data.slice(Int64(offset), 8)
                let id = serializationManager.deserializeInt(idBytes)
                offset += 8

                let timestampBytes = data.slice(Int64(offset), 8)
                let timestamp = serializationManager.deserializeInt(timestampBytes)
                
                // 创建PingMessage，但需要设置时间戳
                // 简化实现：创建新的PingMessage
                PingMessage(id)
                
            case "PongMessage" =>
                if (data.size < offset + 8) {
                    throw DeserializationException("PongMessage数据不完整")
                }
                
                let idBytes = data.slice(Int64(offset), 8)
                let originalId = serializationManager.deserializeInt(idBytes)
                PongMessage(originalId)
                
            case "StopMessage" =>
                if (data.size < offset + 4) {
                    throw DeserializationException("StopMessage原因长度数据不完整")
                }
                let reasonLength = readInt32(data, offset)
                offset += 4
                
                if (data.size < offset + Int64(reasonLength)) {
                    throw DeserializationException("StopMessage原因数据不完整")
                }
                
                let reasonBytes = data.slice(Int64(offset), Int64(reasonLength))
                let reason = serializationManager.deserializeString(reasonBytes)
                StopMessage(reason)
                
            case _ =>
                match (MessageCodecRegistry.get(messageType)) {
                    case Some(codec) =>
                        if (data.size < offset + 4) {
                            throw DeserializationException("${messageType} 负载长度数据不完整")
                        }
                        var payloadLength = Int64(readInt32(data, offset))
                        offset += 4
                        var version: Int64 = 1
                        if (payloadLength == Int64(VERSIONED_FRAME)) {
                            if (data.size < offset + 8) {
                                throw DeserializationException("${messageType} 版本头不完整")
                            }
                            version = Int64(readInt32(data, offset))
                            payloadLength = Int64(readInt32(data, offset + 4))
                            offset += 8
                            if (version < 1) {
                                throw DeserializationException("${messageType} 版本号无效: ${version}")
                            }
                            if (version > codec.version()) {
                                // 单独的异常类型：远程层据此区分"混合版本不兼容"与数据损坏（DA6）
                                throw UnsupportedWireFormatException(messageType,
                                    "${messageType} 的版本 ${version} 高于本节点支持的版本 ${codec.version()}，请先升级接收方")
                            }
                        }
                        if (payloadLength < 0 || data.size < offset + payloadLength) {
                            throw DeserializationException("${messageType} 负载数据不完整")
                        }
                        codec.decode(data.slice(Int64(offset), payloadLength), version)
                    case None =>
                        throw DeserializationException("不支持的消息类型: ${messageType}")
                }
        }
    }

    public func getName(): String {
        "MessageSerializer"
    }

    /** 编码这条消息时对端必须具备的能力位（内置类型与未注册类型为 CAP_NONE） */
    public static func requiredCapabilities(message: Message): UInt64 {
        match (MessageCodecRegistry.get(message.messageType())) {
            case Some(codec) => codec.requiredCapabilities(message)
            case None => WireProtocol.CAP_NONE
        }
    }

    private func writeInt32(buffer: ArrayList<UInt8>, value: Int32): Unit {
        buffer.add(UInt8((value >> 24) & 0xFF))
        buffer.add(UInt8((value >> 16) & 0xFF))
        buffer.add(UInt8((value >> 8) & 0xFF))
        buffer.add(UInt8(value & 0xFF))
    }

    private func readInt32(data: Array<UInt8>, offset: Int64): Int32 {
        (Int32(data[offset]) << 24) |
        (Int32(data[offset + 1]) << 16) |
        (Int32(data[offset + 2]) << 8) |
        Int32(data[offset + 3])
    }

    /**
     * 去除字符串首尾空格
     */
    private func trimString(str: String): String {
        if (str.size == 0) {
            return str
        }

        var start = 0
        var end = Int64(str.size) - 1

        // 找到第一个非空格字符
        while (start <= end && isSpace(str[start])) {
            start += 1
        }

        // 找到最后一个非空格字符
        while (end >= start && isSpace(str[end])) {
            end -= 1
        }

        if (start > end) {
            return ""
        }

        // 提取子字符串
        let chars = ArrayList<Rune>()
        for (i in start..end + 1) {
            chars.add(Rune(Int32(str[i])))
        }
        String(chars.toArray())
    }

    /**
     * 检查字符是否为空格或控制字符
     */
    private func isSpace(byte: UInt8): Bool {
        // 空格、制表符、换行符、回车符，以及其他控制字符(0-31)
        byte <= UInt8(32) || byte == UInt8(127)
    }
}

/**
 * 自定义消息编解码器
 * 用于让用户定义的消息类型可以经由 MessageSerializer（以及远程传输）序列化
 *
 * schema 演进：
 * - version() 返回当前写出的格式版本（默认 1）。版本 1 的帧与旧格式完全相同，旧节点可以读取；
 *   版本 > 1 的帧带版本头，旧节点（或版本更低的编解码器）读取时明确拒绝，远程层计入解码失败。
 * - decode(payload, version) 收到旧版本数据时按旧格式解析（例如新增字段取默认值），默认转交 decode(payload)。
 * - 推荐的滚动升级顺序：先让全部节点能读新版本（decode 支持 v2，version() 仍返回 1），再切换写出版本。
 */
public interface MessageCodec {
    func encode(message: Message): Array<UInt8>
    func decode(payload: Array<UInt8>): Message

    /** 当前写出的格式版本，>= 1 */
    func version(): Int64 {
        1
    }

    /** 解析给定版本（<= version()）的数据 */
    func decode(payload: Array<UInt8>, version: Int64): Message {
        decode(payload)
    }

    /**
     * 这条消息要求对端具备的能力位（DA6）。默认不要求：version() 高于对端能读的版本时由接收方拒绝，
     * 与引入能力协商之前的行为一致。新格式需要对端升级时覆盖它，并在 encodeFor 里给出降级或拒绝。
     */
    func requiredCapabilities(message: Message): UInt64 {
        WireProtocol.CAP_NONE
    }

    /**
     * 按对端能力编码，返回 (写出的版本, 负载)。默认忽略能力、按 version() 编码。
     * 覆盖时：能降级就返回旧版本与旧格式；不能降级抛 WireIncompatibleException（不得悄悄丢字段）。
     */
    func encodeFor(message: Message, peerCapabilities: UInt64): (Int64, Array<UInt8>) {
        (version(), encode(message))
    }
}

/**
 * 全局消息编解码器注册表（按 messageType() 区分，线程安全）
 */
public class MessageCodecRegistry {
    private static let codecs: HashMap<String, MessageCodec> = HashMap<String, MessageCodec>()
    // 由 @Message 生成的 registerCodec 登记的"所属消息类"标识（每个类一个静态对象）
    private static let owners: HashMap<String, Object> = HashMap<String, Object>()
    private static let lock: Mutex = Mutex()

    /** 注册（或显式替换）编解码器：同名已存在时覆盖——用于手写编解码器与模式演进（换版本） */
    public static func register(messageType: String, codec: MessageCodec): Unit {
        synchronized(lock) {
            codecs[messageType] = codec
            owners.remove(messageType)
        }
    }

    /**
     * @Message 生成的 registerCodec 使用的入口（T13）：owner 是消息类自己的静态对象，同一个类重复注册无害；
     * 另一个类已用同一个名字注册时立即抛 IllegalStateException。
     * 此前按简单类名注册、后注册者静默覆盖：不同包里同名的消息类会互相顶掉，远程收到的消息被解成另一个类
     */
    public static func register(messageType: String, codec: MessageCodec, owner: Object): Unit {
        synchronized(lock) {
            if (let Some(existing) <- owners.get(messageType)) {
                if (!refEq(existing, owner)) {
                    throw IllegalStateException(
                        "消息名 ${messageType} 已被另一个 @Message 类注册：不同包里有同名的消息类。" +
                        "请用 @Message[name = \"包名.${messageType}\"] 给其中一个指定唯一的线上名称")
                }
            }
            codecs[messageType] = codec
            owners[messageType] = owner
        }
    }

    public static func unregister(messageType: String): Unit {
        synchronized(lock) {
            codecs.remove(messageType)
            owners.remove(messageType)
        }
    }

    public static func get(messageType: String): Option<MessageCodec> {
        synchronized(lock) {
            codecs.get(messageType)
        }
    }

    public static func isRegistered(messageType: String): Bool {
        get(messageType).isSome()
    }
}

/**
 * 序列化异常
 */
public open class SerializationException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 反序列化异常
 */
public open class DeserializationException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 消息序列化管理器
 * 提供消息序列化的统一接口
 */
public class MessageSerializationManager {
    private let messageSerializer: MessageSerializer

    public init() {
        this.messageSerializer = MessageSerializer()
    }

    /**
     * 序列化消息
     */
    public func serialize(message: Message): Array<UInt8> {
        messageSerializer.serialize(message)
    }

    /**
     * 反序列化消息
     */
    public func deserialize(data: Array<UInt8>): Message {
        messageSerializer.deserialize(data)
    }

    /**
     * 获取序列化器名称
     */
    public func getSerializerName(): String {
        messageSerializer.getName()
    }
}

/**
 * 全局消息序列化管理器
 */
public class GlobalMessageSerializationManager {
    private static var instance: Option<MessageSerializationManager> = None

    public static func getInstance(): MessageSerializationManager {
        match (instance) {
            case Some(manager) => manager
            case None =>
                let manager = MessageSerializationManager()
                instance = Some(manager)
                manager
        }
    }
}
