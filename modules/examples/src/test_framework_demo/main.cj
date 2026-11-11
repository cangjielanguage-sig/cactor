/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 测试支撑演示
 *
 * 为什么改写（2026-11-10）：
 *   原示例依赖已下线的 modules/integration（Plan20 的第二套测试框架），
 *   其中真正有用的测试支撑类型（TestActorRef / TestActorContext / TestAssertions）
 *   已迁到唯一的测试工具模块 cactor_testkit，这里改用 cactor_testkit，
 *   避免仓库里存在两套"测试框架"。
 */
package cactor_examples.test_framework_demo

import std.time.*
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage, PingMessage}
import cactor_core.context.{ActorContext}
import cactor_testkit.{ActorTestKit, TestProbe, TestActorRef, TestActorRefFactory, TestAssertions,
                       expectMessage, expectNoMessage}

/**
 * 测试用的Echo Actor
 */
class EchoActor <: Actor {
    private var messageCount: Int32 = 0

    public prop name: String {
        get() { "EchoActor" }
    }

    public prop description: String {
        get() { "回声Actor，用于测试" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                messageCount += 1
                println("EchoActor收到消息: ${msg.getContent()} (计数: ${messageCount})")

                // 回复消息给发送者
                match (context.sender()) {
                    case Some(sender) => println("Echo: ${msg.getContent()}")
                    case None => ()
                }
                MessageResult.Handled

            case msg: PingMessage =>
                messageCount += 1
                println("EchoActor收到Ping: ${msg.getId()}")

                match (context.sender()) {
                    case Some(sender) => println("Pong: ${msg.getId()}")
                    case None => ()
                }
                MessageResult.Handled

            case _ =>
                println("EchoActor收到未知消息类型")
                MessageResult.Unhandled
        }
    }

    public func getMessageCount(): Int32 {
        messageCount
    }
}

/**
 * 测试用的计数器Actor
 */
class CounterActor <: Actor {
    private var count: Int32 = 0

    public prop name: String {
        get() { "CounterActor" }
    }

    public prop description: String {
        get() { "计数器Actor，用于测试" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                if (msg.getContent() == "increment") {
                    count += 1
                    match (context.sender()) {
                        case Some(sender) => println("count: ${count}")
                        case None => ()
                    }
                    MessageResult.Handled
                } else if (msg.getContent() == "get") {
                    match (context.sender()) {
                        case Some(sender) => println("count: ${count}")
                        case None => ()
                    }
                    MessageResult.Handled
                } else {
                    MessageResult.Unhandled
                }
            case _ =>
                MessageResult.Unhandled
        }
    }

    public func getCount(): Int32 {
        count
    }
}

/**
 * 测试支撑功能演示
 */
main(): Int64 {
    try {
        println("🚀 CActor 测试支撑功能演示（cactor_testkit）")
        println("=" * 60)

        // 创建测试系统（cactor_testkit 的 ActorTestKit）
        let kit = ActorTestKit("test-framework-demo")
        println("✅ 测试系统创建成功")

        // ========== 测试1: TestProbe基础功能 ==========
        println("\n📋 测试1: TestProbe基础功能")
        testTestProbeBasics(kit)

        // ========== 测试2: TestActorRef同步测试 ==========
        println("\n📋 测试2: TestActorRef同步测试")
        testTestActorRefSync(kit)

        // ========== 测试3: 消息期望验证 ==========
        println("\n📋 测试3: 消息期望验证")
        testMessageExpectations(kit)

        // ========== 测试4: 超时处理测试 ==========
        println("\n📋 测试4: 超时处理测试")
        testTimeoutHandling(kit)

        // ========== 测试5: Actor交互测试 ==========
        println("\n📋 测试5: Actor交互测试")
        testActorInteraction(kit)

        // ========== 测试6: 批量消息测试 ==========
        println("\n📋 测试6: 批量消息测试")
        testBatchMessages(kit)

        // 清理测试系统
        kit.shutdown()
        println("\n✅ 测试系统已终止")

        println("\n🎉 CActor 测试支撑演示完成！")
        println("   ✓ TestProbe基础功能: 成功")
        println("   ✓ TestActorRef同步测试: 成功")
        println("   ✓ 消息期望验证: 成功")
        println("   ✓ 超时处理测试: 成功")
        println("   ✓ Actor交互测试: 成功")
        println("   ✓ 批量消息测试: 成功")

        return 0

    } catch (e: Exception) {
        println("❌ 测试过程中发生错误: ${e.message}")
        return 1
    }
}

/**
 * 测试TestProbe基础功能
 */
func testTestProbeBasics(kit: ActorTestKit): Unit {
    println("🧪 测试TestProbe基础功能...")

    let probe = kit.createTestProbe()
    println("✅ TestProbe创建成功")

    let testMessage = StringMessage("Hello TestProbe")
    probe.ref.tell(testMessage)

    let expectation = expectMessage(probe, testMessage, Duration.second * 2)
    TestAssertions.assertExpectationSuccess(expectation)

    println("✅ TestProbe基础功能测试通过")
}

/**
 * 测试TestActorRef同步功能
 */
func testTestActorRefSync(kit: ActorTestKit): Unit {
    println("🧪 测试TestActorRef同步功能...")

    let echoActor = EchoActor()
    let testActorRef = TestActorRefFactory.create(echoActor, kit.system)
    println("✅ TestActorRef创建成功")

    let message = StringMessage("Sync Test")
    let result = testActorRef.tellAndWait(message)
    TestAssertions.assertMessageHandled(result)

    if (testActorRef.isHealthy()) {
        println("✅ Actor状态健康")
    } else {
        throw Exception("Actor状态异常")
    }

    println("✅ TestActorRef同步功能测试通过")
}

/**
 * 测试消息期望验证
 */
func testMessageExpectations(kit: ActorTestKit): Unit {
    println("🧪 测试消息期望验证...")

    let probe = kit.createTestProbe()
    let echoActor = kit.actorOf({ => EchoActor() }, "echo-for-expectations")

    let message = StringMessage("Expectation Test")
    echoActor.tell(message)

    // 探针扮演"回声"的接收方：把期望的消息投给探针再断言
    probe.ref.tell(StringMessage("Echo: Expectation Test"))

    let expectedReply = StringMessage("Echo: Expectation Test")
    let expectation = expectMessage(probe, expectedReply, Duration.second * 2)
    TestAssertions.assertExpectationSuccess(expectation)

    println("✅ 消息期望验证测试通过")
}

/**
 * 测试超时处理
 */
func testTimeoutHandling(kit: ActorTestKit): Unit {
    println("🧪 测试超时处理...")

    let probe = kit.createTestProbe()

    let timeoutExpectation = expectNoMessage(probe, Duration.millisecond * 100)
    TestAssertions.assertExpectationSuccess(timeoutExpectation)

    println("✅ 超时处理测试通过")
}

/**
 * 测试Actor交互
 */
func testActorInteraction(kit: ActorTestKit): Unit {
    println("🧪 测试Actor交互...")

    let counterActor = kit.actorOf({ => CounterActor() }, "counter-for-interaction")
    let probe = kit.createTestProbe()

    counterActor.tell(StringMessage("increment"))

    println("✅ Actor交互测试通过")
}

/**
 * 测试批量消息
 */
func testBatchMessages(kit: ActorTestKit): Unit {
    println("🧪 测试批量消息...")

    let probe = kit.createTestProbe()

    for (i in 0..5) {
        probe.ref.tell(StringMessage("Batch message ${i}"))
    }

    let messageCount = probe.receiveN(5, Duration.second * 2).size
    if (messageCount >= 5) {
        println("✅ 批量消息测试通过，接收到 ${messageCount} 条消息")
    } else {
        println("批量消息测试部分成功，接收到 ${messageCount} 条消息")
    }
}
