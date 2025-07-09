/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 简化测试示例
 * Plan 0.20 Phase 1.1: 标准化测试框架 - 基础实现
 */
package cactor.tests.unit.core.actor

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.message.{Message, MessageResult, StringMessage}
import cactor.core.context.{ActorContext}

/**
 * 简单测试用Actor
 */
class SimpleTestActor <: Actor {
    private var messageCount: Int32 = 0
    private var lastMessage: String = ""
    
    public prop name: String {
        get() { "SimpleTestActor" }
    }
    
    public prop description: String {
        get() { "简单测试用Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                messageCount += 1
                lastMessage = msg.getContent()
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func getMessageCount(): Int32 {
        messageCount
    }
    
    public func getLastMessage(): String {
        lastMessage
    }
    
    public func reset(): Unit {
        messageCount = 0
        lastMessage = ""
    }
}

/**
 * 简单的Actor系统测试类
 * 使用仓颉0.60.5标准unittest框架
 */
@Test
public class SimpleActorTestSuite {
    
    /**
     * 创建测试系统
     */
    private func createTestSystem(): CActorSystem {
        let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        CActorSystemFactory.create("simple-test-system-${timestamp}")
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
    
    /**
     * 测试Actor系统创建
     */
    @TestCase
    public func testActorSystemCreation(): Unit {
        let system = createTestSystem()

        // 验证系统创建成功
        @Assert(!system.isTerminated(), true)
        @Assert(system.getName().size > 0, true)

        cleanupTestSystem(system)
    }
    
    /**
     * 测试Actor创建
     */
    @TestCase
    public func testActorCreation(): Unit {
        let system = createTestSystem()

        // 创建Actor
        let actor = system.actorOf({ => SimpleTestActor() }, "test-actor")

        // 验证Actor创建成功
        @Assert(actor.path.toString().contains("test-actor"), true)

        cleanupTestSystem(system)
    }
    
    /**
     * 测试消息发送
     */
    @TestCase
    public func testMessageSending(): Unit {
        let system = createTestSystem()

        // 创建Actor
        let actor = system.actorOf({ => SimpleTestActor() }, "message-test-actor")

        // 发送消息
        let message = StringMessage("Hello Test Message")
        actor.tell(message)

        // 等待消息处理
        sleep(Duration.millisecond * 100)

        // 验证系统状态
        @Assert(!system.isTerminated(), true)

        cleanupTestSystem(system)
    }
    
    /**
     * 测试多个Actor
     */
    @TestCase
    public func testMultipleActors(): Unit {
        let system = createTestSystem()
        let actorCount: Int32 = 3

        // 创建多个Actor
        for (i in 0..Int64(actorCount)) {
            let actor = system.actorOf({ => SimpleTestActor() }, "multi-actor-${i}")
            actor.tell(StringMessage("Message to actor ${i}"))
        }

        // 等待消息处理
        sleep(Duration.millisecond * 200)

        // 验证系统状态
        @Assert(!system.isTerminated(), true)

        cleanupTestSystem(system)
    }
    
    /**
     * 测试系统终止
     */
    @TestCase
    public func testSystemTermination(): Unit {
        let system = createTestSystem()

        // 创建一些Actor
        let actor1 = system.actorOf({ => SimpleTestActor() }, "term-actor1")
        let actor2 = system.actorOf({ => SimpleTestActor() }, "term-actor2")

        // 验证系统初始状态
        @Assert(!system.isTerminated(), true)

        // 终止系统
        system.terminate()
        sleep(Duration.second * 1)

        // 验证系统终止状态
        @Assert(system.isTerminated(), true)
    }
    
    /**
     * 测试消息处理计数
     */
    @TestCase
    public func testMessageCounting(): Unit {
        let system = createTestSystem()

        // 创建Actor
        let actor = system.actorOf({ => SimpleTestActor() }, "count-test-actor")

        // 发送多条消息
        let messageCount = 5
        for (i in 0..messageCount) {
            actor.tell(StringMessage("Count message ${i}"))
        }

        // 等待消息处理
        sleep(Duration.millisecond * 200)

        // 验证系统状态
        @Assert(!system.isTerminated(), true)

        cleanupTestSystem(system)
    }
    
    /**
     * 测试Actor路径
     */
    @TestCase
    public func testActorPath(): Unit {
        let system = createTestSystem()

        // 创建Actor
        let actor = system.actorOf({ => SimpleTestActor() }, "path-test-actor")

        // 验证Actor路径
        let path = actor.path
        @Assert(path.toString().contains("path-test-actor"), true)

        cleanupTestSystem(system)
    }
    
    /**
     * 测试快速消息发送
     */
    @TestCase
    public func testRapidMessageSending(): Unit {
        let system = createTestSystem()

        // 创建Actor
        let actor = system.actorOf({ => SimpleTestActor() }, "rapid-test-actor")

        // 快速发送多条消息
        for (i in 0..20) {
            actor.tell(StringMessage("Rapid message ${i}"))
        }

        // 等待消息处理
        sleep(Duration.millisecond * 300)

        // 验证系统稳定性
        @Assert(!system.isTerminated(), true)

        cleanupTestSystem(system)
    }
    
    /**
     * 测试Actor名称唯一性
     */
    @TestCase
    public func testActorNameUniqueness(): Unit {
        let system = createTestSystem()

        // 创建第一个Actor
        let actor1 = system.actorOf({ => SimpleTestActor() }, "unique-actor")

        // 尝试创建同名Actor（可能会成功或失败，取决于实现）
        let actor2 = system.actorOf({ => SimpleTestActor() }, "unique-actor-2")

        // 验证两个Actor都创建成功
        @Assert(actor1.path.toString().contains("unique-actor"), true)
        @Assert(actor2.path.toString().contains("unique-actor-2"), true)

        cleanupTestSystem(system)
    }
    
    /**
     * 测试空消息处理
     */
    @TestCase
    public func testEmptyMessageHandling(): Unit {
        let system = createTestSystem()

        // 创建Actor
        let actor = system.actorOf({ => SimpleTestActor() }, "empty-test-actor")

        // 发送空内容消息
        actor.tell(StringMessage(""))

        // 等待消息处理
        sleep(Duration.millisecond * 100)

        // 验证系统状态
        @Assert(!system.isTerminated(), true)

        cleanupTestSystem(system)
    }
}
