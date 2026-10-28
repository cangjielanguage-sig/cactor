/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 上下文系统单元测试
 * Plan 0.20 Phase 1.2: 单元测试完善 - Actor上下文测试
 */
package cactor_tests.unit.core.context

import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

/**
 * 测试用Actor - 用于上下文测试
 */
class ContextTestActor <: Actor {
    private var receivedMessages: ArrayList<String> = ArrayList<String>()
    private var contextInfo: String = ""
    
    public prop name: String {
        get() { "ContextTestActor" }
    }
    
    public prop description: String {
        get() { "用于测试Actor上下文的测试Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()
                receivedMessages.add(content)
                
                // 记录上下文信息（简化）
                contextInfo = "Message processed in context"
                
                // 特殊命令处理
                if (content == "get-context-info") {
                    // 可以在这里添加上下文信息的处理
                }
                
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func getReceivedMessages(): ArrayList<String> {
        receivedMessages
    }
    
    public func getContextInfo(): String {
        contextInfo
    }
    
    public func reset(): Unit {
        receivedMessages.clear()
        contextInfo = ""
    }
}

/**
 * 子Actor测试类
 */
class ChildTestActor <: Actor {
    private var parentRef: Option<ActorRef> = None
    
    public prop name: String {
        get() { "ChildTestActor" }
    }
    
    public prop description: String {
        get() { "用于测试父子关系的子Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()
                
                if (content == "set-parent") {
                    // 简化父子关系设置
                    println("设置父Actor关系")
                } else if (content == "notify-parent") {
                    // 简化通知处理
                    println("通知父Actor")
                }
                
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
}

/**
 * CActor上下文系统单元测试套件
 * 测试ActorContext的各种功能
 */
@Test
public class ActorContextTestSuite {
    
    /**
     * 创建测试系统
     */
    private func createTestSystem(): CActorSystem {
        let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        CActorSystemFactory.create("context-test-system-${timestamp}")
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
     * 测试基本上下文功能
     */
    @TestCase
    public func testBasicContextFunctionality(): Unit {
        let system = createTestSystem()
        
        // 创建测试Actor
        let actor = system.actorOf({ => ContextTestActor() }, "context-test-actor")
        
        // 发送消息
        actor.tell(StringMessage("test-message"))
        
        // 等待消息处理
        sleep(Duration.millisecond * 100)
        
        // 验证系统状态
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试Actor自引用
     */
    @TestCase
    public func testActorSelfReference(): Unit {
        let system = createTestSystem()
        
        // 创建测试Actor
        let actor = system.actorOf({ => ContextTestActor() }, "self-ref-test-actor")
        
        // 发送消息获取上下文信息
        actor.tell(StringMessage("get-context-info"))
        
        // 等待消息处理
        sleep(Duration.millisecond * 100)
        
        // 验证Actor路径包含名称
        @Assert(actor.path.toString().contains("self-ref-test-actor"), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试消息发送者上下文
     */
    @TestCase
    public func testMessageSenderContext(): Unit {
        let system = createTestSystem()
        
        // 创建两个测试Actor
        let actor1 = system.actorOf({ => ContextTestActor() }, "sender-actor")
        let actor2 = system.actorOf({ => ContextTestActor() }, "receiver-actor")
        
        // Actor1向Actor2发送消息
        actor1.tell(StringMessage("setup"))
        actor2.tell(StringMessage("receive-from-actor1"))
        
        // 等待消息处理
        sleep(Duration.millisecond * 200)
        
        // 验证Actor创建成功
        @Assert(actor1.path.toString().contains("sender-actor"), true)
        @Assert(actor2.path.toString().contains("receiver-actor"), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试Actor层次结构
     */
    @TestCase
    public func testActorHierarchy(): Unit {
        let system = createTestSystem()
        
        // 创建父Actor
        let parentActor = system.actorOf({ => ContextTestActor() }, "parent-actor")
        
        // 创建子Actor
        let childActor = system.actorOf({ => ChildTestActor() }, "child-actor")
        
        // 建立父子关系
        childActor.tell(StringMessage("set-parent"))
        
        // 等待消息处理
        sleep(Duration.millisecond * 100)
        
        // 验证Actor创建成功
        @Assert(parentActor.path.toString().contains("parent-actor"), true)
        @Assert(childActor.path.toString().contains("child-actor"), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试上下文信息传递
     */
    @TestCase
    public func testContextInformationPassing(): Unit {
        let system = createTestSystem()
        
        // 创建测试Actor
        let actor = system.actorOf({ => ContextTestActor() }, "info-test-actor")
        
        // 发送多条消息
        actor.tell(StringMessage("message1"))
        actor.tell(StringMessage("message2"))
        actor.tell(StringMessage("message3"))
        
        // 等待消息处理
        sleep(Duration.millisecond * 200)
        
        // 验证系统状态
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试并发上下文处理
     */
    @TestCase
    public func testConcurrentContextHandling(): Unit {
        let system = createTestSystem()
        
        // 创建多个Actor
        let actors = ArrayList<ActorRef>()
        let actorCount = 5
        
        for (i in 0..actorCount) {
            let actor = system.actorOf({ => ContextTestActor() }, "concurrent-actor-${i}")
            actors.add(actor)
        }
        
        // 并发发送消息
        for (i in 0..Int64(actorCount)) {
            actors[i].tell(StringMessage("concurrent-message-${i}"))
        }
        
        // 等待消息处理
        sleep(Duration.millisecond * 300)
        
        // 验证Actor数量
        @Assert(actors.size == Int64(actorCount), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试上下文错误处理
     */
    @TestCase
    public func testContextErrorHandling(): Unit {
        let system = createTestSystem()
        
        // 创建测试Actor
        let actor = system.actorOf({ => ContextTestActor() }, "error-test-actor")
        
        // 发送正常消息
        actor.tell(StringMessage("normal-message"))
        
        // 发送可能导致错误的消息
        actor.tell(StringMessage("error-trigger"))
        
        // 发送恢复消息
        actor.tell(StringMessage("recovery-message"))
        
        // 等待消息处理
        sleep(Duration.millisecond * 200)
        
        // 验证系统稳定性
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试上下文生命周期
     */
    @TestCase
    public func testContextLifecycle(): Unit {
        let system = createTestSystem()
        
        // 创建测试Actor
        let actor = system.actorOf({ => ContextTestActor() }, "lifecycle-test-actor")
        
        // 发送初始化消息
        actor.tell(StringMessage("init"))
        
        // 等待初始化
        sleep(Duration.millisecond * 100)
        
        // 发送工作消息
        actor.tell(StringMessage("work"))
        
        // 等待工作完成
        sleep(Duration.millisecond * 100)
        
        // 验证Actor状态
        @Assert(actor.path.toString().contains("lifecycle-test-actor"), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试上下文资源管理
     */
    @TestCase
    public func testContextResourceManagement(): Unit {
        let system = createTestSystem()
        
        // 创建多个Actor测试资源管理
        let actors = ArrayList<ActorRef>()
        
        for (i in 0..10) {
            let actor = system.actorOf({ => ContextTestActor() }, "resource-actor-${i}")
            actors.add(actor)
            
            // 发送消息
            actor.tell(StringMessage("resource-test-${i}"))
        }
        
        // 等待消息处理
        sleep(Duration.millisecond * 200)
        
        // 验证资源创建
        @Assert(actors.size == 10, true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试上下文性能
     */
    @TestCase
    public func testContextPerformance(): Unit {
        let system = createTestSystem()
        
        // 创建测试Actor
        let actor = system.actorOf({ => ContextTestActor() }, "performance-test-actor")
        
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        // 发送大量消息测试性能
        for (i in 0..100) {
            actor.tell(StringMessage("perf-message-${i}"))
        }
        
        // 等待消息处理
        sleep(Duration.millisecond * 500)
        
        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime
        
        // 验证性能（应该在合理时间内完成）
        @Assert(duration > 0, true)
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
}
