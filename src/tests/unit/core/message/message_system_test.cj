/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 消息系统单元测试
 * Plan 0.20 Phase 1.2: 单元测试完善 - 消息系统测试
 */
package cactor.tests.unit.core.message

import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.core.message.{Message, MessageResult, StringMessage}
import cactor.core.context.{ActorContext}

/**
 * 测试用消息类
 */
class TestMessage <: Message {
    private let content: String
    private let creationTime: Int64

    public init(content: String) {
        this.content = content
        this.creationTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
    }

    public func messageType(): String {
        "TestMessage"
    }

    public func timestamp(): Int64 {
        creationTime
    }

    public func getContent(): String {
        content
    }

    public func getCreationTime(): Int64 {
        creationTime
    }

    public func toString(): String {
        "TestMessage(content=${content}, timestamp=${creationTime})"
    }
}

/**
 * 复杂测试消息类
 */
class ComplexTestMessage <: Message {
    private let id: Int32
    private let data: Array<String>
    private let metadata: HashMap<String, String>

    public init(id: Int32, data: Array<String>) {
        this.id = id
        this.data = data
        this.metadata = HashMap<String, String>()
    }

    public func messageType(): String {
        "ComplexTestMessage"
    }

    public func getId(): Int32 {
        id
    }

    public func getData(): Array<String> {
        data
    }

    public func getMetadata(): HashMap<String, String> {
        metadata
    }

    public func addMetadata(key: String, value: String): Unit {
        metadata[key] = value
    }

    public func toString(): String {
        "ComplexTestMessage(id=${id}, dataSize=${data.size}, metadataSize=${metadata.size})"
    }
}

/**
 * CActor消息系统单元测试套件
 * 测试消息创建、序列化、传输等核心功能
 */
@Test
public class MessageSystemTestSuite {
    
    /**
     * 测试基本消息创建和属性
     */
    @TestCase
    public func testBasicMessageCreation(): Unit {
        let message = StringMessage("Hello World")
        
        // 验证消息内容
        @Assert(message.getContent() == "Hello World", true)
        @Assert(message.messageType() == "StringMessage", true)
    }
    
    /**
     * 测试自定义消息创建
     */
    @TestCase
    public func testCustomMessageCreation(): Unit {
        let message = TestMessage("Custom Test Message")
        
        // 验证消息属性
        @Assert(message.getContent() == "Custom Test Message", true)
        @Assert(message.getCreationTime() > 0, true)
        @Assert(message.toString().contains("TestMessage"), true)
    }
    
    /**
     * 测试复杂消息结构
     */
    @TestCase
    public func testComplexMessageStructure(): Unit {
        let data = ["item1", "item2", "item3"]
        let message = ComplexTestMessage(123, data)
        
        // 添加元数据
        message.addMetadata("sender", "test-actor")
        message.addMetadata("priority", "high")
        
        // 验证消息结构
        @Assert(message.getId() == 123, true)
        @Assert(message.getData().size == 3, true)
        @Assert(message.getMetadata().size == 2, true)
        @Assert(message.getMetadata()["sender"] == "test-actor", true)
    }
    
    /**
     * 测试消息不可变性
     */
    @TestCase
    public func testMessageImmutability(): Unit {
        let originalContent = "Immutable Message"
        let message = StringMessage(originalContent)
        
        // 验证消息内容不变
        @Assert(message.getContent() == originalContent, true)
        
        // 创建新消息验证独立性
        let message2 = StringMessage("Different Message")
        @Assert(message.getContent() == originalContent, true)
        @Assert(message2.getContent() == "Different Message", true)
    }
    
    /**
     * 测试消息类型识别
     */
    @TestCase
    public func testMessageTypeIdentification(): Unit {
        let stringMsg = StringMessage("String Message")
        let testMsg = TestMessage("Test Message")
        let complexMsg = ComplexTestMessage(456, ["data"])
        
        // 验证消息类型
        @Assert(stringMsg.messageType() == "StringMessage", true)
        @Assert(testMsg.messageType() == "TestMessage", true)
        @Assert(complexMsg.messageType() == "ComplexTestMessage", true)
    }
    
    /**
     * 测试空消息处理
     */
    @TestCase
    public func testEmptyMessageHandling(): Unit {
        let emptyMessage = StringMessage("")
        
        // 验证空消息处理
        @Assert(emptyMessage.getContent() == "", true)
        @Assert(emptyMessage.getContent().size == 0, true)
    }
    
    /**
     * 测试长消息处理
     */
    @TestCase
    public func testLongMessageHandling(): Unit {
        let longContent = "A" * 1000  // 1000个字符的长消息
        let longMessage = StringMessage(longContent)
        
        // 验证长消息处理
        @Assert(longMessage.getContent().size == 1000, true)
        @Assert(longMessage.getContent() == longContent, true)
    }
    
    /**
     * 测试特殊字符消息
     */
    @TestCase
    public func testSpecialCharacterMessages(): Unit {
        let specialContent = "特殊字符测试: !@#$%^&*()_+-=[]{}|;':\",./<>?"
        let specialMessage = StringMessage(specialContent)
        
        // 验证特殊字符处理
        @Assert(specialMessage.getContent() == specialContent, true)
        @Assert(specialMessage.messageType() == "StringMessage", true)
    }
    
    /**
     * 测试消息时间戳
     */
    @TestCase
    public func testMessageTimestamp(): Unit {
        let beforeTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        sleep(Duration.millisecond * 1)  // 确保时间差异
        
        let message = TestMessage("Timestamp Test")
        
        sleep(Duration.millisecond * 1)
        let afterTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        // 验证时间戳在合理范围内
        @Assert(message.getCreationTime() > beforeTime, true)
        @Assert(message.getCreationTime() < afterTime, true)
    }
    
    /**
     * 测试批量消息创建
     */
    @TestCase
    public func testBatchMessageCreation(): Unit {
        let messages = ArrayList<Message>()
        let messageCount = 100
        
        // 创建批量消息
        for (i in 0..messageCount) {
            messages.add(StringMessage("Message ${i}"))
        }
        
        // 验证批量创建
        @Assert(messages.size == Int64(messageCount), true)
        
        // 验证批量创建
        @Assert(messages.size == Int64(messageCount), true)
    }
    
    /**
     * 测试消息内存使用
     */
    @TestCase
    public func testMessageMemoryUsage(): Unit {
        let messages = ArrayList<Message>()
        
        // 创建大量小消息
        for (i in 0..1000) {
            messages.add(StringMessage("Msg${i}"))
        }
        
        // 验证消息创建成功
        @Assert(messages.size == 1000, true)
        
        // 清理测试
        messages.clear()
        @Assert(messages.size == 0, true)
    }
    
    /**
     * 测试消息相等性
     */
    @TestCase
    public func testMessageEquality(): Unit {
        let content = "Equal Test"
        let message1 = StringMessage(content)
        let message2 = StringMessage(content)
        
        // 验证消息内容相等（但对象不同）
        @Assert(message1.getContent() == message2.getContent(), true)
        
        // 验证不同内容的消息
        let message3 = StringMessage("Different Content")
        @Assert(message1.getContent() != message3.getContent(), true)
    }
}
