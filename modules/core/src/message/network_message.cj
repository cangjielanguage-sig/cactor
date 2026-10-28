/*
 * Copyright (c) 2024. All rights reserved.
 * Core Layer - 网络消息传输
 * 基于Foundation层网络传输的消息网络传输实现
 */
package cactor_core.message

import std.collection.*
import std.sync.*
import std.time.*
import cactor_foundation.network.{NetworkTransport, TcpTransport, UdpTransport, NetworkAddress}
import cactor_foundation.serialization.SerializationManager

/**
 * 网络消息包装器
 * 包装要通过网络发送的消息，添加Actor相关信息
 */
public struct NetworkMessage {
    public let targetActor: String
    public let message: Message
    public let sender: Option<String>
    public let messageId: String
    public let timestamp: Int64

    public init(targetActor: String, message: Message, sender: Option<String>) {
        this(targetActor, message, sender, NetworkMessage.generateMessageId(), NetworkMessage.getCurrentTimeNanos())
    }

    /** 反序列化时使用：保留发送方的消息 ID 与时间戳 */
    public init(targetActor: String, message: Message, sender: Option<String>, messageId: String, timestamp: Int64) {
        this.targetActor = targetActor
        this.message = message
        this.sender = sender
        this.messageId = messageId
        this.timestamp = timestamp
    }

    private static let idSequence: AtomicInt64 = AtomicInt64(0)

    private static func generateMessageId(): String {
        "msg-${getCurrentTimeNanos()}-${idSequence.fetchAdd(1)}"
    }

    private static func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toNanoseconds()
    }
}

/**
 * 网络消息序列化器
 * 将NetworkMessage序列化为字节流，用于网络传输
 */
public class NetworkMessageSerializer {
    private let serializationManager: SerializationManager
    private let messageSerializer: MessageSerializer

    public init() {
        this.serializationManager = SerializationManager()
        this.messageSerializer = MessageSerializer()
    }

    /**
     * 序列化网络消息
     */
    public func serialize(networkMessage: NetworkMessage): Array<UInt8> {
        let result = ArrayList<UInt8>()
        
        // 序列化目标Actor
        let targetBytes = serializationManager.serializeString(networkMessage.targetActor)
        writeInt32(result, Int32(targetBytes.size))
        // 替代appendAll：逐个添加字节
        for (byte in targetBytes) {
            result.add(byte)
        }

        // 序列化发送者
        let senderStr = match (networkMessage.sender) {
            case Some(s) => s
            case None => ""
        }
        let senderBytes = serializationManager.serializeString(senderStr)
        writeInt32(result, Int32(senderBytes.size))
        for (byte in senderBytes) {
            result.add(byte)
        }

        // 序列化消息ID
        let messageIdBytes = serializationManager.serializeString(networkMessage.messageId)
        writeInt32(result, Int32(messageIdBytes.size))
        for (byte in messageIdBytes) {
            result.add(byte)
        }

        // 序列化时间戳
        let timestampBytes = serializationManager.serializeInt(networkMessage.timestamp)
        for (byte in timestampBytes) {
            result.add(byte)
        }

        // 序列化消息内容
        let messageBytes = messageSerializer.serialize(networkMessage.message)
        writeInt32(result, Int32(messageBytes.size))
        for (byte in messageBytes) {
            result.add(byte)
        }
        
        result.toArray()
    }

    /**
     * 反序列化网络消息
     */
    public func deserialize(data: Array<UInt8>): NetworkMessage {
        var offset: Int64 = 0
        let targetActor = serializationManager.deserializeString(readBlock(data, offset))
        offset += 4 + Int64(readInt32(data, offset))
        let senderStr = serializationManager.deserializeString(readBlock(data, offset))
        offset += 4 + Int64(readInt32(data, offset))
        let sender: Option<String> = if (senderStr.isEmpty()) { None } else { Some(senderStr) }
        let messageId = serializationManager.deserializeString(readBlock(data, offset))
        offset += 4 + Int64(readInt32(data, offset))
        if (data.size < offset + 8) {
            throw NetworkMessageException("时间戳数据不完整")
        }
        let timestamp = serializationManager.deserializeInt(data.slice(offset, 8))
        offset += 8
        let message = messageSerializer.deserialize(readBlock(data, offset))
        NetworkMessage(targetActor, message, sender, messageId, timestamp)
    }

    /** 读取 offset 处的 [4 字节长度 + 数据] 块，越界时抛出异常 */
    private func readBlock(data: Array<UInt8>, offset: Int64): Array<UInt8> {
        if (data.size < offset + 4) {
            throw NetworkMessageException("数据不完整：缺少长度字段（offset=${offset}）")
        }
        let len = Int64(readInt32(data, offset))
        if (len < 0 || data.size < offset + 4 + len) {
            throw NetworkMessageException("数据不完整：需要 ${len} 字节（offset=${offset}，总长 ${data.size}）")
        }
        data.slice(offset + 4, len)
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
}

/**
 * 网络消息传输器
 * 基于Foundation层网络传输实现消息的网络传输
 */
public class NetworkMessageTransport {
    private let transport: NetworkTransport
    private let serializer: NetworkMessageSerializer
    private var messageHandler: Option<(NetworkMessage) -> Unit>

    public init(transport: NetworkTransport) {
        this.transport = transport
        this.serializer = NetworkMessageSerializer()
        this.messageHandler = None
        
        // 设置字节流处理器
        transport.setDataHandler({ data => this.handleReceivedData(data) })
    }

    /**
     * 发送网络消息
     */
    public func send(address: NetworkAddress, networkMessage: NetworkMessage): Unit {
        let data = serializer.serialize(networkMessage)
        transport.send(address, data)
    }

    /**
     * 设置消息处理器
     */
    public func setMessageHandler(handler: (NetworkMessage) -> Unit): Unit {
        this.messageHandler = Some(handler)
    }

    /**
     * 启动传输器
     */
    public func start(): Unit {
        transport.start()
    }

    /**
     * 停止传输器
     */
    public func stop(): Unit {
        transport.stop()
    }

    /**
     * 检查是否活跃
     */
    public func isActive(): Bool {
        transport.isActive()
    }

    /**
     * 获取本地地址
     */
    public func getLocalAddress(): NetworkAddress {
        transport.getLocalAddress()
    }

    /**
     * 处理接收到的字节数据
     */
    private func handleReceivedData(data: Array<UInt8>): Unit {
        try {
            let networkMessage = serializer.deserialize(data)
            match (messageHandler) {
                case Some(handler) => handler(networkMessage)
                case None => println("警告：没有设置消息处理器")
            }
        } catch (e: Exception) {
            println("网络消息反序列化失败: ${e.message}")
        }
    }
}

/**
 * 网络消息传输工厂
 * 创建不同类型的网络消息传输器
 */
public class NetworkMessageTransportFactory {
    /**
     * 创建TCP网络消息传输器
     */
    public static func createTcpTransport(localAddress: NetworkAddress): NetworkMessageTransport {
        let tcpTransport = TcpTransport(localAddress)
        NetworkMessageTransport(tcpTransport)
    }

    /**
     * 创建UDP网络消息传输器
     */
    public static func createUdpTransport(localAddress: NetworkAddress): NetworkMessageTransport {
        let udpTransport = UdpTransport(localAddress)
        NetworkMessageTransport(udpTransport)
    }
}

/**
 * 网络消息异常
 */
public class NetworkMessageException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 远程消息发送器
 * 提供简化的远程消息发送接口
 */
public class RemoteMessageSender {
    private let transport: NetworkMessageTransport

    public init(transport: NetworkMessageTransport) {
        this.transport = transport
    }

    /**
     * 发送消息到远程Actor
     */
    public func sendToRemoteActor(remoteAddress: NetworkAddress, targetActor: String, 
                                  message: Message, sender: Option<String>): Unit {
        let networkMessage = NetworkMessage(targetActor, message, sender)
        transport.send(remoteAddress, networkMessage)
    }

    /**
     * 启动发送器
     */
    public func start(): Unit {
        transport.start()
    }

    /**
     * 停止发送器
     */
    public func stop(): Unit {
        transport.stop()
    }
}
