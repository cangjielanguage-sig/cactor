/*
 * Copyright (c) 2024. All rights reserved.
 * CActor TestKit - 测试支撑类型（TestActorRef / TestActorContext / TestAssertions）
 *
 * 为什么迁到这里：
 * 这组类型原先放在 modules/integration（Plan20 的测试框架）里，而 integration 模块即将整体下线。
 * testkit 是仓库唯一的测试工具模块（TestProbe / ActorTestKit / EventRecorder / awaitCondition），
 * 测试支撑（同步测试引用 + 断言）应当和它们放在一起，integration 删除后测试能力不出现缺口。
 *
 * 迁移后的依赖方向：testkit -> core / runtime / foundation / cactor，
 * testkit 不反向依赖 integration（该模块要删）。
 *
 * 与原版的差异（都是 integration 内部类型被移除后的必然调整）：
 * - TestActorContext / TestActorRef / TestActorRefFactory 的"测试系统"参数由
 *   integration 的 TestActorSystem 改为标准的 cactor.api.CActorSystem
 *   （testkit 已有 ActorTestKit.system 就是 CActorSystem），避免再造一套测试系统；
 * - TestExpectation 一并迁入（TestAssertions 的入参类型），语义与原版一致；
 * - 新增 expectMessage / expectNoMessage 两个自由函数，把 testkit 的 TestProbe
 *   （阻塞队列语义）包装成原版的期望式返回值，让断言工具与探针能配合使用。
 */
package cactor_testkit

import std.sync.*
import std.time.*
import cactor_core.actor.{Actor, ActorRef, ActorContext, Receive}
import cactor_core.message.{Message, MessageResult}
import cactor.api.{CActorSystem}

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
            case Success(_) => "Success"
            case Timeout => "Timeout"
            case UnexpectedMessage(msg) => "UnexpectedMessage(${msg.messageType()})"
            case Error(ex) => "Error(${ex.message})"
        }
    }
}

/**
 * 测试用的虚拟 Actor（TestActorContext.self 的替身，不处理任何业务消息）
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

/**
 * 测试专用ActorContext
 *
 * 经典 Actor 的 receive(message, context) 可以脱离运行时直接调用（同步测试），
 * 此时需要一个自造的上下文。这里基于 cactor_core 的 ActorContext 接口实现，
 * 子 Actor 创建走真实的 CActorSystem，其余在测试环境里退化为空实现。
 */
public class TestActorContext <: ActorContext {
    private let testSystem: CActorSystem
    private var senderPath: Option<String>
    private var senderRefOpt: Option<ActorRef>
    private var selfRef: Option<ActorRef>

    public init(system: CActorSystem) {
        this.testSystem = system
        this.senderPath = None
        this.senderRefOpt = None
        this.selfRef = None
    }

    public func sender(): Option<String> {
        senderPath
    }

    /** 同步测试里没有真实邮箱，用 setSender 显式设置发送方 */
    public func setSender(sender: ActorRef): Unit {
        senderPath = Some(sender.path.toString())
        senderRefOpt = Some(sender)
    }

    public func senderRef(): Option<ActorRef> {
        senderRefOpt
    }

    /** 同步测试里没有真实的回复通道：只有显式设置过发送方才真正回信 */
    public func reply(message: Message): Unit {
        match (senderRefOpt) {
            case Some(s) => s.tell(message)
            case None => ()
        }
    }

    public prop self: ActorRef {
        get() {
            match (selfRef) {
                case Some(r) => r
                case None =>
                    // 返回一个测试用的ActorRef（懒创建，避免每次访问都新建 Actor）
                    let r = testSystem.actorOf({ => TestDummyActor() }, "test-self-${DateTime.now().toUnixTimeStamp().toNanoseconds()}")
                    selfRef = Some(r)
                    r
            }
        }
    }

    /** 测试系统的底层系统 */
    public func underlyingSystem(): CActorSystem {
        testSystem
    }

    public func actorOf(factory: () -> Actor, name: String): ActorRef {
        testSystem.actorOf(factory, name)
    }

    public func actorSelection(path: String): Option<ActorRef> {
        throw IllegalStateException("同步上下文不支持 actorSelection，请使用 ActorTestKit")
    }

    public func stop(actor: ActorRef): Unit {
        testSystem.stop(actor)
    }

    public func watch(actor: ActorRef): Unit {
        throw IllegalStateException("同步上下文不支持 watch，请使用 ActorTestKit")
    }

    public func unwatch(actor: ActorRef): Unit {
        throw IllegalStateException("同步上下文不支持 unwatch，请使用 ActorTestKit")
    }

    public func become(behavior: Receive): Unit {
        throw IllegalStateException("同步上下文不支持 become，请使用 ActorTestKit")
    }

    public func unbecome(): Unit {
        throw IllegalStateException("同步上下文不支持 unbecome，请使用 ActorTestKit")
    }

    public func schedule(delay: Duration, message: Message, target: ActorRef): Unit {
        throw IllegalStateException("同步上下文不支持定时器，请使用 ActorTestKit")
    }

    public func scheduleOnce(delay: Duration, message: Message, target: ActorRef): Unit {
        throw IllegalStateException("同步上下文不支持定时器，请使用 ActorTestKit")
    }
}

/**
 * 测试专用ActorRef - 提供同步测试能力
 */
public class TestActorRef {
    private let underlyingActor: Actor
    private let actorRef: ActorRef
    private let system: CActorSystem
    private let mutex: Mutex
    private var lastResult: MessageResult
    private var lastException: Option<Exception>

    public init(actor: Actor, actorRef: ActorRef, system: CActorSystem) {
        this.underlyingActor = actor
        this.actorRef = actorRef
        this.system = system
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
        var result = MessageResult.Handled
        try {
            // 创建测试上下文，直接调用Actor的receive方法
            let testContext = TestActorContext(system)
            result = underlyingActor.receive(message, testContext)
            lastResult = result
            lastException = None
        } catch (e: Exception) {
            lastException = Some(e)
            result = MessageResult.Unhandled  // 使用Unhandled代替Failed
        } finally {
            mutex.unlock()
        }
        return result
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
 * TestActorRef工厂
 */
public class TestActorRefFactory {
    public static func create(actor: Actor, system: CActorSystem): TestActorRef {
        let actorName = "test-actor-${DateTime.now().toUnixTimeStamp().toMilliseconds()}"
        createWithName(actor, actorName, system)
    }

    public static func createWithName(actor: Actor, name: String, system: CActorSystem): TestActorRef {
        let actorRef = system.actorOf({ => actor }, name)
        TestActorRef(actor, actorRef, system)
    }
}

/**
 * 测试断言工具
 */
public class TestAssertions {
    public static func assertMessageHandled(result: MessageResult): Unit {
        match (result) {
            case MessageResult.Handled =>
                ()
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
                ()
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
                ()
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
                ()
            case TestExpectation.Success(_) =>
                throw Exception("断言失败: 期望意外成功")
            case TestExpectation.UnexpectedMessage(msg) =>
                throw Exception("断言失败: 收到意外消息 ${msg.messageType()}")
            case TestExpectation.Error(ex) =>
                throw Exception("断言失败: 发生错误 ${ex.message}")
        }
    }
}

/**
 * 期望探针在 timeout 内收到指定类型的消息（用 messageType 比较，与原测试框架一致）。
 * 超时返回 TestExpectation.Timeout，收到别的消息返回 UnexpectedMessage。
 */
public func expectMessage(probe: TestProbe, expected: Message, timeout: Duration): TestExpectation<Message> {
    try {
        let m = probe.expectMsg(timeout)
        if (m.messageType() == expected.messageType()) {
            return TestExpectation.Success(m)
        }
        TestExpectation.UnexpectedMessage(m)
    } catch (_: TestKitAssertionException) {
        TestExpectation.Timeout
    }
}

/**
 * 期望探针在 duration 内不收到任何消息；没收到返回 Success(())，收到则返回 UnexpectedMessage。
 */
public func expectNoMessage(probe: TestProbe, duration: Duration): TestExpectation<Unit> {
    try {
        let m = probe.expectMsg(duration)
        return TestExpectation.UnexpectedMessage(m)
    } catch (_: TestKitAssertionException) {
        return TestExpectation.Success(())
    }
}
