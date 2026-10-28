/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 测试框架 - TestActorRef实现
 * Plan20阶段1：测试框架建设 - 同步测试支持
 */
package cactor_integration.testing

import std.time.*
import std.sync.*
import cactor_core.actor.{Actor, ActorRef, ActorPath}
import cactor_core.message.{Message, MessageResult}
import cactor_core.context.{ActorContext}
import cactor.api.{CActorSystem}

/**
 * 测试专用ActorRef - 提供同步测试能力
 */
public class TestActorRef {
    private let underlyingActor: Actor
    private let actorRef: ActorRef
    private let testSystem: TestActorSystem
    private let mutex: Mutex
    private var lastResult: MessageResult
    private var lastException: Option<Exception>
    
    public init(actor: Actor, actorRef: ActorRef, testSystem: TestActorSystem) {
        this.underlyingActor = actor
        this.actorRef = actorRef
        this.testSystem = testSystem
        this.mutex = Mutex()
        this.lastResult = MessageResult.Handled
        this.lastException = None
    }
    
    /**
     * 同步发送消息并等待处理完成
     */
    public func tellAndWait(message: Message): MessageResult {
        return tellAndWait(message, Duration.second * 5)
    }
    
    /**
     * 同步发送消息并等待处理完成（指定超时）
     */
    public func tellAndWait(message: Message, timeout: Duration): MessageResult {
        mutex.lock()
        try {
            // 创建测试上下文
            let testContext = TestActorContext(testSystem)
            
            // 直接调用Actor的receive方法
            let result = underlyingActor.receive(message, testContext)
            lastResult = result
            lastException = None
            
            mutex.unlock()
            return result
        } catch (e: Exception) {
            lastException = Some(e)
            mutex.unlock()
            return MessageResult.Unhandled  // 使用Unhandled代替Failed
        }
    }
    
    /**
     * 异步发送消息（委托给底层ActorRef）
     */
    public func tell(message: Message): Unit {
        actorRef.tell(message)
    }
    
    /**
     * 获取底层ActorRef
     */
    public func getActorRef(): ActorRef {
        actorRef
    }
    
    /**
     * 获取底层Actor实例（用于状态检查）
     */
    public func getUnderlyingActor(): Actor {
        underlyingActor
    }
    
    /**
     * 获取最后一次消息处理结果
     */
    public func getLastResult(): MessageResult {
        mutex.lock()
        let result = lastResult
        mutex.unlock()
        result
    }
    
    /**
     * 获取最后一次异常
     */
    public func getLastException(): Option<Exception> {
        mutex.lock()
        let exception = lastException
        mutex.unlock()
        exception
    }
    
    /**
     * 检查Actor是否处于正常状态
     */
    public func isHealthy(): Bool {
        mutex.lock()
        let healthy = match (lastException) {
            case Some(_) => false
            case None => true
        }
        mutex.unlock()
        healthy
    }
    
    /**
     * 重置测试状态
     */
    public func reset(): Unit {
        mutex.lock()
        lastResult = MessageResult.Handled
        lastException = None
        mutex.unlock()
    }
}

/**
 * 测试专用ActorContext
 */
public class TestActorContext <: ActorContext {
    private let testSystem: TestActorSystem
    private var senderPath: Option<String>

    public init(testSystem: TestActorSystem) {
        this.testSystem = testSystem
        this.senderPath = None
    }

    public func sender(): Option<String> {
        senderPath
    }
    
    public func setSender(sender: ActorRef): Unit {
        senderPath = Some(sender.path.toString())
    }
    
    public func self(): ActorRef {
        // 返回一个测试用的ActorRef
        testSystem.actorOf({ => TestDummyActor() }, "test-self")
    }
    
    public func system(): CActorSystem {
        // 返回测试系统的底层系统
        testSystem.getUnderlyingSystem()
    }
    
    public func actorOf(factory: () -> Actor, name: String): ActorRef {
        testSystem.actorOf(factory, name)
    }
    
    public func actorSelection(path: String): Option<ActorRef> {
        // 简化实现
        None
    }
    
    public func stop(actor: ActorRef): Unit {
        // 简化实现：在测试环境中不实际停止Actor
        println("测试环境中停止Actor")
    }
    
    public func watch(actor: ActorRef): Unit {
        // 简化实现
        println("测试环境中监视Actor")
    }

    public func unwatch(actor: ActorRef): Unit {
        // 简化实现
        println("测试环境中取消监视Actor")
    }
    
    public func become(behavior: (Message, ActorContext) -> MessageResult): Unit {
        // 简化实现：在测试环境中不支持行为切换
        println("测试环境中不支持行为切换")
    }
    
    public func unbecome(): Unit {
        // 简化实现
        println("测试环境中不支持行为恢复")
    }
    
    public func schedule(delay: Duration, message: Message, target: ActorRef): Unit {
        // 简化实现：立即发送消息
        target.tell(message)
    }
    
    public func scheduleOnce(delay: Duration, message: Message, target: ActorRef): Unit {
        // 简化实现：立即发送消息
        target.tell(message)
    }
}

// TestDummyActor已在test_framework.cj中定义

/**
 * TestActorRef工厂
 */
public class TestActorRefFactory {
    public static func create(actor: Actor, testSystem: TestActorSystem): TestActorRef {
        let actorName = "test-actor-${DateTime.now().toUnixTimeStamp().toMilliseconds()}"
        let actorRef = testSystem.actorOf({ => actor }, actorName)
        TestActorRef(actor, actorRef, testSystem)
    }
    
    public static func createWithName(actor: Actor, name: String, testSystem: TestActorSystem): TestActorRef {
        let actorRef = testSystem.actorOf({ => actor }, name)
        TestActorRef(actor, actorRef, testSystem)
    }
}

/**
 * 测试断言工具
 */
public class TestAssertions {
    public static func assertMessageHandled(result: MessageResult): Unit {
        match (result) {
            case MessageResult.Handled =>
                println("✅ 消息处理成功")
            case MessageResult.Unhandled =>
                throw Exception("断言失败: 消息未被处理")
            case MessageResult.Stop =>
                throw Exception("断言失败: 消息处理导致停止")
            case MessageResult.Restart =>
                throw Exception("断言失败: 消息处理导致重启")
            case MessageResult.Escalate =>
                throw Exception("断言失败: 消息处理导致升级")
        }
    }

    public static func assertMessageUnhandled(result: MessageResult): Unit {
        match (result) {
            case MessageResult.Unhandled =>
                println("✅ 消息正确未被处理")
            case MessageResult.Handled =>
                throw Exception("断言失败: 消息被意外处理")
            case MessageResult.Stop =>
                throw Exception("断言失败: 消息处理导致停止")
            case MessageResult.Restart =>
                throw Exception("断言失败: 消息处理导致重启")
            case MessageResult.Escalate =>
                throw Exception("断言失败: 消息处理导致升级")
        }
    }
    
    public static func assertExpectationSuccess<T>(expectation: TestExpectation<T>): Unit {
        match (expectation) {
            case TestExpectation.Success(_) => 
                println("✅ 期望验证成功")
            case TestExpectation.Timeout => 
                throw Exception("断言失败: 期望超时")
            case TestExpectation.UnexpectedMessage(msg) => 
                throw Exception("断言失败: 收到意外消息 ${msg.messageType()}")
            case TestExpectation.Error(ex) => 
                throw Exception("断言失败: 发生错误 ${ex.message}")
        }
    }
    
    public static func assertExpectationTimeout<T>(expectation: TestExpectation<T>): Unit {
        match (expectation) {
            case TestExpectation.Timeout => 
                println("✅ 期望超时验证成功")
            case TestExpectation.Success(_) => 
                throw Exception("断言失败: 期望意外成功")
            case TestExpectation.UnexpectedMessage(msg) => 
                throw Exception("断言失败: 收到意外消息 ${msg.messageType()}")
            case TestExpectation.Error(ex) => 
                throw Exception("断言失败: 发生错误 ${ex.message}")
        }
    }
}
