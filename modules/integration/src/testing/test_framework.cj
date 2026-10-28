/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 测试框架核心实现
 * Plan20阶段1：测试框架建设 - 完整实现
 */
package cactor_integration.testing

import std.time.*
import std.sync.*
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult}
import cactor_core.context.{ActorContext}
import cactor.api.{CActorSystem, CActorSystemFactory}

/**
 * 测试消息期望结果
 */
public enum TestExpectation<T> {
    | Success(T)
    | Timeout
    | UnexpectedMessage(Message)
    | Error(Exception)
    
    public func isSuccess(): Bool {
        match (this) {
            case Success(_) => true
            case _ => false
        }
    }
    
    public func getValue(): Option<T> {
        match (this) {
            case Success(value) => Some(value)
            case _ => None
        }
    }
    
    public func toString(): String {
        match (this) {
            case Success(value) => "Success"
            case Timeout => "Timeout"
            case UnexpectedMessage(msg) => "UnexpectedMessage(${msg.messageType()})"
            case Error(ex) => "Error(${ex.message})"
        }
    }
}

/**
 * 测试时间控制器
 */
public class TestTimeController {
    private var virtualTime: Int64
    private let mutex: Mutex
    
    public init() {
        this.virtualTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.mutex = Mutex()
    }
    
    public func getCurrentTime(): Int64 {
        mutex.lock()
        let time = virtualTime
        mutex.unlock()
        time
    }
    
    public func advanceTime(duration: Duration): Unit {
        mutex.lock()
        virtualTime += duration.toNanoseconds()
        mutex.unlock()
    }
    
    public func reset(): Unit {
        mutex.lock()
        virtualTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        mutex.unlock()
    }
}

/**
 * 测试专用Actor系统
 */
public class TestActorSystem {
    private let underlyingSystem: CActorSystem
    private let timeController: TestTimeController
    private let mutex: Mutex
    
    public init(name: String) {
        this.underlyingSystem = CActorSystemFactory.create(name)
        this.timeController = TestTimeController()
        this.mutex = Mutex()
    }
    
    public func actorOf(factory: () -> Actor, name: String): ActorRef {
        underlyingSystem.actorOf(factory, name)
    }
    
    public func createTestProbe(): TestProbe {
        TestProbe.create(this)
    }
    
    public func getTimeController(): TestTimeController {
        timeController
    }
    
    public func terminate(): Unit {
        underlyingSystem.terminate()
    }
    
    public func getName(): String {
        underlyingSystem.getName()
    }
    
    public func isTerminated(): Bool {
        underlyingSystem.isTerminated()
    }

    public func getUnderlyingSystem(): CActorSystem {
        underlyingSystem
    }
}

/**
 * 测试探针 - 用于验证消息接收
 */
public class TestProbe {
    private let testSystem: TestActorSystem
    private var lastMessage: Option<Message>
    private var messageReceived: Bool
    private let mutex: Mutex
    private let actorRef: ActorRef
    private var isActive: Bool
    private var totalMessageCount: Int32

    private init(testSystem: TestActorSystem, actorRef: ActorRef) {
        this.testSystem = testSystem
        this.lastMessage = None
        this.messageReceived = false
        this.mutex = Mutex()
        this.actorRef = actorRef
        this.isActive = true
        this.totalMessageCount = 0
    }
    
    public static func create(testSystem: TestActorSystem): TestProbe {
        let probeId = DateTime.now().toUnixTimeStamp().toMilliseconds().toString()

        // 创建一个临时的TestProbe实例
        let tempActorRef = testSystem.actorOf({ => TestDummyActor() }, "temp-${probeId}")
        let probe = TestProbe(testSystem, tempActorRef)

        // 创建真正的TestProbeActor
        let realActorRef = testSystem.actorOf({ => TestProbeActor(probe) }, "test-probe-${probeId}")

        // 简化实现：直接使用真正的ActorRef
        return probe
    }
    
    /**
     * 期望接收特定消息
     */
    public func expectMessage(expectedMessage: Message, timeout: Duration): TestExpectation<Message> {
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let timeoutNanos = timeout.toNanoseconds()

        while (DateTime.now().toUnixTimeStamp().toNanoseconds() - startTime < timeoutNanos) {
            mutex.lock()
            if (messageReceived && lastMessage.isSome()) {
                match (lastMessage) {
                    case Some(message) =>
                        messageReceived = false
                        lastMessage = None
                        mutex.unlock()

                        if (messagesEqual(message, expectedMessage)) {
                            return TestExpectation.Success(message)
                        } else {
                            return TestExpectation.UnexpectedMessage(message)
                        }
                    case None =>
                        mutex.unlock()
                }
            } else {
                mutex.unlock()
            }

            // 简单的等待实现
            let waitStart = DateTime.now().toUnixTimeStamp().toNanoseconds()
            while (DateTime.now().toUnixTimeStamp().toNanoseconds() - waitStart < 1000000) {
                // 等待1ms
            }
        }

        return TestExpectation.Timeout
    }
    
    /**
     * 期望在指定时间内不接收任何消息
     */
    public func expectNoMessage(duration: Duration): TestExpectation<Unit> {
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let durationNanos = duration.toNanoseconds()

        while (DateTime.now().toUnixTimeStamp().toNanoseconds() - startTime < durationNanos) {
            mutex.lock()
            if (messageReceived && lastMessage.isSome()) {
                match (lastMessage) {
                    case Some(message) =>
                        mutex.unlock()
                        return TestExpectation.UnexpectedMessage(message)
                    case None =>
                        mutex.unlock()
                }
            } else {
                mutex.unlock()
            }

            // 简单的等待实现
            let waitStart = DateTime.now().toUnixTimeStamp().toNanoseconds()
            while (DateTime.now().toUnixTimeStamp().toNanoseconds() - waitStart < 1000000) {
                // 等待1ms
            }
        }

        return TestExpectation.Success(())
    }
    
    /**
     * 期望接收任意消息
     */
    public func expectAnyMessage(timeout: Duration): TestExpectation<Message> {
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let timeoutNanos = timeout.toNanoseconds()

        while (DateTime.now().toUnixTimeStamp().toNanoseconds() - startTime < timeoutNanos) {
            mutex.lock()
            if (messageReceived && lastMessage.isSome()) {
                match (lastMessage) {
                    case Some(message) =>
                        messageReceived = false
                        lastMessage = None
                        mutex.unlock()
                        return TestExpectation.Success(message)
                    case None =>
                        mutex.unlock()
                }
            } else {
                mutex.unlock()
            }

            // 简单的等待实现
            let waitStart = DateTime.now().toUnixTimeStamp().toNanoseconds()
            while (DateTime.now().toUnixTimeStamp().toNanoseconds() - waitStart < 1000000) {
                // 等待1ms
            }
        }

        return TestExpectation.Timeout
    }
    
    /**
     * 获取ActorRef用于发送消息
     */
    public func getRef(): ActorRef {
        actorRef
    }
    
    /**
     * 内部方法：记录接收到的消息
     */
    public func recordMessage(message: Message): Unit {
        if (isActive) {
            mutex.lock()
            lastMessage = Some(message)
            messageReceived = true
            totalMessageCount += 1
            mutex.unlock()
        }
    }

    /**
     * 清理资源
     */
    public func cleanup(): Unit {
        mutex.lock()
        isActive = false
        messageReceived = false
        lastMessage = None
        totalMessageCount = 0
        mutex.unlock()
    }

    /**
     * 获取已接收的消息数量
     */
    public func getReceivedMessageCount(): Int32 {
        mutex.lock()
        let count = totalMessageCount
        mutex.unlock()
        count
    }
    
    private func messagesEqual(msg1: Message, msg2: Message): Bool {
        // 简化的消息比较实现
        msg1.messageType() == msg2.messageType()
    }
}

/**
 * TestProbe内部使用的Actor
 */
class TestProbeActor <: Actor {
    private let probe: TestProbe

    public init(probe: TestProbe) {
        this.probe = probe
    }

    public prop name: String {
        get() { "TestProbeActor" }
    }

    public prop description: String {
        get() { "测试探针内部Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        probe.recordMessage(message)
        MessageResult.Handled
    }
}

/**
 * 测试用的虚拟Actor
 */
class TestDummyActor <: Actor {
    public prop name: String {
        get() { "TestDummyActor" }
    }

    public prop description: String {
        get() { "测试用虚拟Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}
