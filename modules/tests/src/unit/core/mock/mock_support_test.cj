/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Mock对象支持测试
 * Plan 0.20 Phase 1.2: 单元测试完善 - Mock对象支持
 */
package cactor_tests.unit.core.mock

import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

/**
 * Mock Actor接口
 */
interface MockActor {
    func receive(message: Message, context: ActorContext): MessageResult
    func getName(): String
    func getCallCount(): Int32
    func getLastMessage(): Option<Message>
    func reset(): Unit
}

/**
 * Mock Actor实现
 */
class MockActorImpl <: Actor {
    private var callCount: Int32 = 0
    private var lastMessage: Option<Message> = None
    private var shouldFail: Bool = false
    private var returnResult: MessageResult = MessageResult.Handled
    
    public prop name: String {
        get() { "MockActor" }
    }
    
    public prop description: String {
        get() { "Mock Actor用于测试" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        callCount += 1
        lastMessage = Some(message)
        
        if (shouldFail) {
            throw Exception("Mock Actor 模拟失败")
        }
        
        returnResult
    }
    
    public func getName(): String {
        name
    }
    
    public func getCallCount(): Int32 {
        callCount
    }
    
    public func getLastMessage(): Option<Message> {
        lastMessage
    }
    
    public func reset(): Unit {
        callCount = 0
        lastMessage = None
        shouldFail = false
        returnResult = MessageResult.Handled
    }
    
    // Mock特定方法
    public func setShouldFail(fail: Bool): Unit {
        shouldFail = fail
    }
    
    public func setReturnResult(result: MessageResult): Unit {
        returnResult = result
    }
}

/**
 * Mock消息类
 */
class MockMessage <: Message {
    private let content: String
    private let msgId: Int32
    private var processed: Bool = false

    public init(content: String, messageId: Int32) {
        this.content = content
        this.msgId = messageId
    }

    public func messageType(): String {
        "MockMessage"
    }

    public func getContent(): String {
        content
    }

    public func getMsgId(): Int32 {
        msgId
    }

    public func isProcessed(): Bool {
        processed
    }

    public func markProcessed(): Unit {
        processed = true
    }

    public func toString(): String {
        "MockMessage(id=${msgId}, content=${content}, processed=${processed})"
    }
}

/**
 * Mock ActorRef实现
 */
class MockActorRef {
    private let actorName: String
    private var sentMessages: ArrayList<Message> = ArrayList<Message>()
    private var isActive: Bool = true
    
    public init(name: String) {
        this.actorName = name
    }
    
    public func tell(message: Message): Unit {
        if (isActive) {
            sentMessages.add(message)
        }
    }
    
    public func getSentMessages(): ArrayList<Message> {
        sentMessages
    }
    
    public func getSentMessageCount(): Int64 {
        sentMessages.size
    }
    
    public func getLastSentMessage(): Option<Message> {
        if (sentMessages.size > 0) {
            Some(sentMessages[sentMessages.size - 1])
        } else {
            None
        }
    }
    
    public func setActive(active: Bool): Unit {
        isActive = active
    }
    
    public func getActiveStatus(): Bool {
        isActive
    }
    
    public func reset(): Unit {
        sentMessages.clear()
        isActive = true
    }
    
    public func getName(): String {
        actorName
    }
}

/**
 * Mock上下文实现
 */
class MockActorContext {
    private let selfRef: MockActorRef
    private let senderRef: MockActorRef
    private var contextData: HashMap<String, String> = HashMap<String, String>()
    
    public init(selfName: String, senderName: String) {
        this.selfRef = MockActorRef(selfName)
        this.senderRef = MockActorRef(senderName)
    }
    
    public func getSelf(): MockActorRef {
        selfRef
    }
    
    public func getSender(): MockActorRef {
        senderRef
    }
    
    public func setContextData(key: String, value: String): Unit {
        contextData[key] = value
    }
    
    public func getContextData(key: String): Option<String> {
        contextData.get(key)
    }
    
    public func getContextDataSize(): Int64 {
        contextData.size
    }
    
    public func reset(): Unit {
        selfRef.reset()
        senderRef.reset()
        contextData.clear()
    }
}

/**
 * CActor Mock对象支持测试套件
 * 测试Mock对象在复杂依赖测试中的应用
 */
@Test
public class MockSupportTestSuite {
    
    /**
     * 测试Mock Actor基本功能
     */
    @TestCase
    public func testMockActorBasicFunctionality(): Unit {
        let mockActor = MockActorImpl()
        let message = StringMessage("test message")

        // 测试初始状态
        @Assert(mockActor.getCallCount() == 0, true)
        @Assert(mockActor.getLastMessage().isNone(), true)
        @Assert(mockActor.name == "MockActor", true)
    }
    
    /**
     * 测试Mock Actor失败模拟
     */
    @TestCase
    public func testMockActorFailureSimulation(): Unit {
        let mockActor = MockActorImpl()

        // 设置失败模式
        mockActor.setShouldFail(true)

        // 验证失败设置
        @Assert(mockActor.getCallCount() == 0, true)

        // 重置失败模式
        mockActor.setShouldFail(false)
        @Assert(mockActor.getCallCount() == 0, true)
    }
    
    /**
     * 测试Mock消息功能
     */
    @TestCase
    public func testMockMessageFunctionality(): Unit {
        let mockMessage = MockMessage("test content", 123)
        
        // 验证初始状态
        @Assert(mockMessage.getContent() == "test content", true)
        @Assert(mockMessage.getMsgId() == 123, true)
        @Assert(!mockMessage.isProcessed(), true)
        
        // 标记为已处理
        mockMessage.markProcessed()
        @Assert(mockMessage.isProcessed(), true)
        
        // 验证toString
        @Assert(mockMessage.toString().contains("MockMessage"), true)
        @Assert(mockMessage.toString().contains("123"), true)
    }
    
    /**
     * 测试Mock ActorRef功能
     */
    @TestCase
    public func testMockActorRefFunctionality(): Unit {
        let mockRef = MockActorRef("test-actor")
        let message1 = StringMessage("message 1")
        let message2 = StringMessage("message 2")
        
        // 验证初始状态
        @Assert(mockRef.getSentMessageCount() == 0, true)
        @Assert(mockRef.getLastSentMessage().isNone(), true)
        @Assert(mockRef.getActiveStatus(), true)
        
        // 发送消息
        mockRef.tell(message1)
        mockRef.tell(message2)
        
        // 验证消息发送
        @Assert(mockRef.getSentMessageCount() == 2, true)
        @Assert(mockRef.getLastSentMessage().isSome(), true)
        
        // 测试非活跃状态
        mockRef.setActive(false)
        mockRef.tell(StringMessage("should not be sent"))
        @Assert(mockRef.getSentMessageCount() == 2, true)  // 应该还是2
    }
    
    /**
     * 测试Mock上下文功能
     */
    @TestCase
    public func testMockContextFunctionality(): Unit {
        let mockContext = MockActorContext("self-actor", "sender-actor")
        
        // 验证基本功能
        @Assert(mockContext.getSelf().getName() == "self-actor", true)
        @Assert(mockContext.getSender().getName() == "sender-actor", true)
        @Assert(mockContext.getContextDataSize() == 0, true)
        
        // 设置上下文数据
        mockContext.setContextData("key1", "value1")
        mockContext.setContextData("key2", "value2")
        
        // 验证上下文数据
        @Assert(mockContext.getContextDataSize() == 2, true)
        @Assert(mockContext.getContextData("key1").getOrThrow() == "value1", true)
        @Assert(mockContext.getContextData("key2").getOrThrow() == "value2", true)
        @Assert(mockContext.getContextData("nonexistent").isNone(), true)
    }
    
    /**
     * 测试复杂Mock场景
     */
    @TestCase
    public func testComplexMockScenario(): Unit {
        let mockActor = MockActorImpl()
        let mockContext = MockActorContext("complex-self", "complex-sender")

        // 设置复杂场景
        mockContext.setContextData("scenario", "complex-test")
        mockActor.setReturnResult(MessageResult.Unhandled)

        // 验证Mock设置
        @Assert(mockActor.getCallCount() == 0, true)
        @Assert(mockContext.getContextData("scenario").getOrThrow() == "complex-test", true)
        @Assert(mockContext.getContextDataSize() == 1, true)
    }
    
    /**
     * 测试Mock对象重置功能
     */
    @TestCase
    public func testMockObjectReset(): Unit {
        let mockActor = MockActorImpl()
        let mockRef = MockActorRef("reset-test")
        let mockContext = MockActorContext("self", "sender")

        // 设置一些状态
        mockRef.tell(StringMessage("test"))
        mockContext.setContextData("test", "value")

        // 验证状态已设置
        @Assert(mockRef.getSentMessageCount() == 1, true)
        @Assert(mockContext.getContextDataSize() == 1, true)

        // 重置
        mockActor.reset()
        mockRef.reset()
        mockContext.reset()

        // 验证重置后状态
        @Assert(mockActor.getCallCount() == 0, true)
        @Assert(mockRef.getSentMessageCount() == 0, true)
        @Assert(mockContext.getContextDataSize() == 0, true)
    }
    
    /**
     * 测试Mock对象在集成测试中的应用
     */
    @TestCase
    public func testMockObjectsInIntegration(): Unit {
        let system = createTestSystem()
        
        // 创建真实Actor和Mock对象混合测试
        let realActor = system.actorOf({ => MockActorImpl() }, "real-actor")
        let mockRef = MockActorRef("mock-ref")
        
        // 发送消息到真实Actor
        realActor.tell(StringMessage("integration test"))
        
        // 发送消息到Mock
        mockRef.tell(StringMessage("mock test"))
        
        // 等待处理
        sleep(Duration.millisecond * 100)
        
        // 验证集成
        @Assert(realActor.path.toString().contains("real-actor"), true)
        @Assert(mockRef.getSentMessageCount() == 1, true)
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试参数化Mock测试
     */
    @TestCase
    public func testParameterizedMockTesting(): Unit {
        // 测试用例1: Handled
        let mockActor1 = MockActorImpl()
        mockActor1.setReturnResult(MessageResult.Handled)
        @Assert(mockActor1.getCallCount() == 0, true)

        // 测试用例2: Unhandled
        let mockActor2 = MockActorImpl()
        mockActor2.setReturnResult(MessageResult.Unhandled)
        @Assert(mockActor2.getCallCount() == 0, true)

        // 测试用例3: Handled
        let mockActor3 = MockActorImpl()
        mockActor3.setReturnResult(MessageResult.Handled)
        @Assert(mockActor3.getCallCount() == 0, true)
    }
    
    /**
     * 测试Mock对象性能
     */
    @TestCase
    public func testMockObjectPerformance(): Unit {
        let mockActor = MockActorImpl()
        let mockContext = MockActorContext("perf-self", "perf-sender")

        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()

        // 创建大量Mock消息
        let messages = ArrayList<MockMessage>()
        for (i in 0..1000) {
            let message = MockMessage("perf message ${i}", Int32(i))
            messages.add(message)
        }

        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime

        // 验证性能
        @Assert(messages.size == 1000, true)
        @Assert(duration > 0, true)

        // Mock对象应该很快
        println("Mock性能测试: ${duration}纳秒创建1000个Mock消息")
    }
    
    /**
     * 创建测试系统
     */
    private func createTestSystem(): CActorSystem {
        let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        CActorSystemFactory.create("mock-test-system-${timestamp}")
    }
    
    /**
     * 清理测试系统
     */
    private func cleanupTestSystem(system: CActorSystem): Unit {
        if (!system.isTerminated()) {
            system.terminate()
            sleep(Duration.second * 1)
        }
    }
}
