/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 测试框架核心组件
 * Plan20阶段1：测试框架建设 - 核心实现
 */
package cactor.integration.testing

import std.time.*
import std.sync.*
import std.collection.*
import cactor.core.actor.{Actor, ActorRef, ActorPath}
import cactor.core.message.{Message, MessageResult}
import cactor.core.context.{ActorContext}
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
}

/**
 * 测试时间控制器
 */
public class TestTimeController {
    private var virtualTime: Int64
    private let mutex: Mutex
    private var timeAdvanceListeners: Array<(Int64) -> Unit>

    public init() {
        this.virtualTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.mutex = Mutex()
        this.timeAdvanceListeners = Array<(Int64) -> Unit>(0, { _ => { _ => () } })
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
        let newTime = virtualTime
        mutex.unlock()

        // 通知监听器
        for (listener in timeAdvanceListeners) {
            listener(newTime)
        }
    }

    public func addTimeAdvanceListener(listener: (Int64) -> Unit): Unit {
        mutex.lock()
        let newArray = Array<(Int64) -> Unit>(timeAdvanceListeners.size + 1, { i =>
            if (i < timeAdvanceListeners.size) {
                timeAdvanceListeners[i]
            } else {
                listener
            }
        })
        timeAdvanceListeners = newArray
        mutex.unlock()
    }
}

/**
 * 测试专用Actor系统
 */
public class TestActorSystem {
    private let underlyingSystem: CActorSystem
    private let timeController: TestTimeController
    private let testProbes: ArrayList<TestProbe>
    private let mutex: Mutex

    public init(name: String) {
        this.underlyingSystem = CActorSystemFactory.create(name)
        this.timeController = TestTimeController()
        this.testProbes = ArrayList<TestProbe>()
        this.mutex = Mutex()
    }

    public func actorOf(factory: () -> Actor, name: String): ActorRef {
        underlyingSystem.actorOf(factory, name)
    }

    public func createTestProbe(): TestProbe {
        let probe = TestProbe(this)
        mutex.lock()
        testProbes.add(probe)
        mutex.unlock()
        probe
    }

    public func getTimeController(): TestTimeController {
        timeController
    }

    public func terminate(): Unit {
        // 清理所有测试探针
        mutex.lock()
        for (probe in testProbes) {
            probe.cleanup()
        }
        testProbes.clear()
        mutex.unlock()

        underlyingSystem.terminate()
    }

    public func getName(): String {
        underlyingSystem.getName()
    }

    public func isTerminated(): Bool {
        underlyingSystem.isTerminated()
    }
}

/**
 * 测试探针 - 用于验证消息接收
 */
public class TestProbe {
    private let testSystem: TestActorSystem
    private let receivedMessages: ArrayList<Message>
    private let mutex: Mutex
    private let actorRef: ActorRef
    private var isActive: Bool
    private let probeId: String

    public init(testSystem: TestActorSystem) {
        this.testSystem = testSystem
        this.receivedMessages = ArrayList<Message>()
        this.mutex = Mutex()
        this.isActive = true
        this.probeId = DateTime.now().toUnixTimeStamp().toMilliseconds().toString()

        // 创建内部Actor来接收消息
        this.actorRef = testSystem.actorOf({ => TestProbeActor(this) }, "test-probe-${probeId}")
    }

    /**
     * 期望接收特定消息
     */
    public func expectMessage(expectedMessage: Message, timeout: Duration): TestExpectation<Message> {
        let startTime = testSystem.getTimeController().getCurrentTime()
        let timeoutNanos = timeout.toNanoseconds()

        while (testSystem.getTimeController().getCurrentTime() - startTime < timeoutNanos) {
            mutex.lock()
            if (receivedMessages.size > 0) {
                let message = receivedMessages.removeAt(0)
                mutex.unlock()

                if (messagesEqual(message, expectedMessage)) {
                    return TestExpectation.Success(message)
                } else {
                    return TestExpectation.UnexpectedMessage(message)
                }
            }
            mutex.unlock()

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
        let startTime = testSystem.getTimeController().getCurrentTime()
        let durationNanos = duration.toNanoseconds()

        while (testSystem.getTimeController().getCurrentTime() - startTime < durationNanos) {
            mutex.lock()
            if (receivedMessages.size > 0) {
                let message = receivedMessages.removeAt(0)
                mutex.unlock()
                return TestExpectation.UnexpectedMessage(message)
            }
            mutex.unlock()

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
        let startTime = testSystem.getTimeController().getCurrentTime()
        let timeoutNanos = timeout.toNanoseconds()

        while (testSystem.getTimeController().getCurrentTime() - startTime < timeoutNanos) {
            mutex.lock()
            if (receivedMessages.size > 0) {
                let message = receivedMessages.removeAt(0)
                mutex.unlock()
                return TestExpectation.Success(message)
            }
            mutex.unlock()

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
            receivedMessages.add(message)
            mutex.unlock()
        }
    }

    /**
     * 清理资源
     */
    public func cleanup(): Unit {
        mutex.lock()
        isActive = false
        receivedMessages.clear()
        mutex.unlock()
    }

    /**
     * 获取已接收的消息数量
     */
    public func getReceivedMessageCount(): Int32 {
        mutex.lock()
        let count = receivedMessages.size
        mutex.unlock()
        count
    }

    private func messagesEqual(msg1: Message, msg2: Message): Bool {
        // 简化的消息比较实现
        msg1.messageType() == msg2.messageType()
    }

    private func generateProbeId(): String {
        DateTime.now().toUnixTimeStamp().toMilliseconds().toString()
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
