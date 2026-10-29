/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 远程通信
 *
 * 一个 SimpleRemoteManager 对应一个节点：持有一个 NetworkTransport（同时用于收发），
 * 通过 NetworkMessageSerializer 编解码消息，按目标 Actor 名称分发到本地处理器。
 * RemoteActorRef 把远程 Actor 包装成普通 ActorRef，实现位置透明。
 */
package cactor_remote

import std.collection.*
import std.sync.*
import cactor_core.actor.{ActorRef, ActorPath}
import cactor_core.message.{Message, NetworkMessage, NetworkMessageSerializer}
import cactor_foundation.network.{NetworkTransport, NetworkAddress, TransportFactory, TransportProtocol}
import cactor_foundation.logging.{Logger, Logging}

let log_simple_remote: Logger = Logging.logger("cactor_remote.simple_remote")

/**
 * 远程消息发送器
 */
public class SimpleRemoteSender {
    private let transport: NetworkTransport
    private let ownsTransport: Bool
    private let networkMessageSerializer: NetworkMessageSerializer
    private let isStarted: AtomicBool
    private let sentCount: AtomicInt64

    public init(localAddress: NetworkAddress, protocol: TransportProtocol) {
        this(TransportFactory.createTransport(protocol, localAddress), true)
    }

    /**
     * 使用共享传输创建（ownsTransport 为 false 时不负责启动/停止传输）
     */
    public init(transport: NetworkTransport, ownsTransport: Bool) {
        this.transport = transport
        this.ownsTransport = ownsTransport
        this.networkMessageSerializer = NetworkMessageSerializer()
        this.isStarted = AtomicBool(false)
        this.sentCount = AtomicInt64(0)
    }

    public func start(): Unit {
        if (isStarted.compareAndSwap(false, true) && ownsTransport) {
            transport.start()
        }
    }

    public func stop(): Unit {
        if (isStarted.compareAndSwap(true, false) && ownsTransport) {
            transport.stop()
        }
    }

    /**
     * 发送消息到远程节点上的指定 Actor；网络失败时抛出异常
     */
    public func sendMessage(targetAddress: NetworkAddress, targetActor: String, message: Message): Unit {
        sendMessage(targetAddress, targetActor, message, None<String>)
    }

    public func sendMessage(targetAddress: NetworkAddress, targetActor: String, message: Message, sender: Option<String>): Unit {
        if (!isStarted.load()) {
            throw IllegalStateException("发送器未启动")
        }
        let networkMessage = NetworkMessage(targetActor, message, sender)
        transport.send(targetAddress, networkMessageSerializer.serialize(networkMessage))
        sentCount.fetchAdd(1)
    }

    public func getSentCount(): Int64 {
        sentCount.load()
    }

    public func isActive(): Bool {
        isStarted.load()
    }

    public func getLocalAddress(): NetworkAddress {
        transport.getLocalAddress()
    }
}

/**
 * 远程消息接收器
 */
public class SimpleRemoteReceiver {
    private let transport: NetworkTransport
    private let ownsTransport: Bool
    private let networkMessageSerializer: NetworkMessageSerializer
    private let isStarted: AtomicBool
    private let messageHandlers: HashMap<String, (Message, Option<String>) -> Unit>
    private let mutex: Mutex
    private let receivedCount: AtomicInt64
    private let undeliverableCount: AtomicInt64
    private let decodeFailures: AtomicInt64

    public init(localAddress: NetworkAddress, protocol: TransportProtocol) {
        this(TransportFactory.createTransport(protocol, localAddress), true)
    }

    public init(transport: NetworkTransport, ownsTransport: Bool) {
        this.transport = transport
        this.ownsTransport = ownsTransport
        this.networkMessageSerializer = NetworkMessageSerializer()
        this.isStarted = AtomicBool(false)
        this.messageHandlers = HashMap<String, (Message, Option<String>) -> Unit>()
        this.mutex = Mutex()
        this.receivedCount = AtomicInt64(0)
        this.undeliverableCount = AtomicInt64(0)
        this.decodeFailures = AtomicInt64(0)
        transport.setDataHandler(handleReceivedData)
    }

    public func start(): Unit {
        if (isStarted.compareAndSwap(false, true) && ownsTransport) {
            transport.start()
        }
    }

    public func stop(): Unit {
        if (isStarted.compareAndSwap(true, false) && ownsTransport) {
            transport.stop()
        }
    }

    /**
     * 注册消息处理器（同名覆盖）
     */
    public func registerHandler(actorName: String, handler: (Message) -> Unit): Unit {
        registerHandler(actorName, { m: Message, _: Option<String> => handler(m) })
    }

    public func registerHandler(actorName: String, handler: (Message, Option<String>) -> Unit): Unit {
        synchronized(mutex) {
            messageHandlers[actorName] = handler
        }
    }

    public func unregisterHandler(actorName: String): Unit {
        synchronized(mutex) {
            messageHandlers.remove(actorName)
        }
    }

    private func handleReceivedData(data: Array<UInt8>): Unit {
        let networkMessage = try {
            networkMessageSerializer.deserialize(data)
        } catch (e: Exception) {
            decodeFailures.fetchAdd(1)
            log_simple_remote.error("反序列化网络消息失败: ${e.message}")
            return
        }
        receivedCount.fetchAdd(1)
        let handler = synchronized(mutex) { messageHandlers.get(networkMessage.targetActor) }
        match (handler) {
            case Some(h) =>
                try {
                    h(networkMessage.message, networkMessage.sender)
                } catch (e: Exception) {
                    log_simple_remote.error("远程消息处理失败(${networkMessage.targetActor}): ${e.message}")
                }
            case None =>
                undeliverableCount.fetchAdd(1)
        }
    }

    public func getReceivedCount(): Int64 {
        receivedCount.load()
    }

    /** 找不到目标 Actor 的消息数（死信） */
    public func getUndeliverableCount(): Int64 {
        undeliverableCount.load()
    }

    public func getDecodeFailures(): Int64 {
        decodeFailures.load()
    }

    public func isActive(): Bool {
        isStarted.load()
    }

    public func getLocalAddress(): NetworkAddress {
        transport.getLocalAddress()
    }
}

/**
 * 远程通信管理器（一个节点）
 */
public class SimpleRemoteManager {
    private let transport: NetworkTransport
    private let sender: SimpleRemoteSender
    private let receiver: SimpleRemoteReceiver
    private let started: AtomicBool

    public init(localAddress: NetworkAddress, protocol: TransportProtocol) {
        this.transport = TransportFactory.createTransport(protocol, localAddress)
        this.sender = SimpleRemoteSender(transport, false)
        this.receiver = SimpleRemoteReceiver(transport, false)
        this.started = AtomicBool(false)
    }

    public func start(): Unit {
        if (started.compareAndSwap(false, true)) {
            transport.start()
            sender.start()
            receiver.start()
        }
    }

    public func stop(): Unit {
        if (started.compareAndSwap(true, false)) {
            receiver.stop()
            sender.stop()
            transport.stop()
        }
    }

    public func sendMessage(targetAddress: NetworkAddress, targetActor: String, message: Message): Unit {
        sender.sendMessage(targetAddress, targetActor, message)
    }

    public func sendMessage(targetAddress: NetworkAddress, targetActor: String, message: Message, senderPath: Option<String>): Unit {
        sender.sendMessage(targetAddress, targetActor, message, senderPath)
    }

    public func registerHandler(actorName: String, handler: (Message) -> Unit): Unit {
        receiver.registerHandler(actorName, handler)
    }

    /**
     * 把本地 Actor 以指定名称暴露给远程节点：收到的消息投递到该 Actor 的邮箱
     */
    public func exposeActor(actorName: String, localRef: ActorRef): Unit {
        receiver.registerHandler(actorName, { m: Message, from: Option<String> => localRef.tell(m, from) })
    }

    public func unexposeActor(actorName: String): Unit {
        receiver.unregisterHandler(actorName)
    }

    /**
     * 获取远程 Actor 的引用
     */
    public func remoteActorOf(targetAddress: NetworkAddress, actorName: String): RemoteActorRef {
        RemoteActorRef(this, targetAddress, actorName)
    }

    public func isActive(): Bool {
        started.load() && transport.isActive()
    }

    /** 实际监听地址（绑定端口 0 时为系统分配的端口） */
    public func getLocalAddress(): NetworkAddress {
        transport.getLocalAddress()
    }

    public func getSender(): SimpleRemoteSender {
        sender
    }

    public func getReceiver(): SimpleRemoteReceiver {
        receiver
    }
}

/**
 * 远程 Actor 引用：tell 通过网络发送到远程节点
 * 网络失败时 tell 抛出异常（调用方可决定重试或记录死信）
 */
public class RemoteActorRef <: ActorRef {
    private let manager: SimpleRemoteManager
    private let targetAddress: NetworkAddress
    private let actorName: String
    private let actorPath: ActorPath

    public init(manager: SimpleRemoteManager, targetAddress: NetworkAddress, actorName: String) {
        this.manager = manager
        this.targetAddress = targetAddress
        this.actorName = actorName
        this.actorPath = ActorPath("remote@${targetAddress.toString()}", "/user/${actorName}")
    }

    public prop path: ActorPath {
        get() { actorPath }
    }

    public func tell(message: Message): Unit {
        manager.sendMessage(targetAddress, actorName, message)
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        manager.sendMessage(targetAddress, actorName, message, sender)
    }

    public func equals(other: ActorRef): Bool {
        other.path.toString() == actorPath.toString()
    }

    public func hashCode(): Int32 {
        Int32(actorPath.toString().hashCode() & 0x7FFFFFFF)
    }

    public func getTargetAddress(): NetworkAddress {
        targetAddress
    }

    public func getActorName(): String {
        actorName
    }
}

/**
 * 远程通信工厂
 */
public class RemoteFactory {
    public static func createSender(address: NetworkAddress, protocol: TransportProtocol): SimpleRemoteSender {
        SimpleRemoteSender(address, protocol)
    }

    public static func createReceiver(address: NetworkAddress, protocol: TransportProtocol): SimpleRemoteReceiver {
        SimpleRemoteReceiver(address, protocol)
    }

    public static func createManager(address: NetworkAddress, protocol: TransportProtocol): SimpleRemoteManager {
        SimpleRemoteManager(address, protocol)
    }

    public static func createDefaultManager(port: Int32): SimpleRemoteManager {
        SimpleRemoteManager(NetworkAddress("127.0.0.1", port), TransportProtocol.TCP)
    }
}
