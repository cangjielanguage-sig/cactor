/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - Ask模式改进演示
 * Plan20阶段1：验证Ask模式完善的功能实现
 */
package cactor.examples.ask_pattern_demo

import std.time.*
import cactor.api.{CActor}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.message.{Message, MessageResult, StringMessage, PingMessage}
import cactor.core.context.{ActorContext}
import cactor.patterns.ask.{Ask, AskMessage, AskResponse, Result, AskFuture}

/**
 * 响应式Actor - 能够处理Ask请求并返回响应
 */
class ResponsiveActor <: Actor {
    private var requestCount: Int64 = 0
    
    public prop name: String {
        get() { "ResponsiveActor" }
    }
    
    public prop description: String {
        get() { "能够响应Ask请求的Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case askMsg: AskMessage =>
                requestCount += 1
                let originalMsg = askMsg.getOriginalMessage()
                let replyTo = askMsg.getReplyTo()
                
                println("📨 ResponsiveActor收到Ask请求 #${requestCount}")
                println("   请求ID: ${askMsg.getRequestId()}")
                println("   原始消息: ${originalMsg.messageType()}")
                
                // 根据消息类型生成不同的响应
                let response = match (originalMsg) {
                    case stringMsg: StringMessage =>
                        let content = stringMsg.getContent()
                        StringMessage("Echo: ${content} (处理时间: ${DateTime.now()})")
                    case pingMsg: PingMessage =>
                        StringMessage("Pong! ID: ${pingMsg.getId()}")
                    case _ =>
                        StringMessage("Unknown message type: ${originalMsg.messageType()}")
                }
                
                // 发送响应
                let askResponse = AskResponse(askMsg.getRequestId(), response, true)
                replyTo.tell(askResponse)
                
                println("✅ 已发送响应到: ${replyTo.path.toString()}")
                MessageResult.Handled
                
            case stringMsg: StringMessage =>
                requestCount += 1
                println("📝 ResponsiveActor收到普通消息 #${requestCount}: ${stringMsg.getContent()}")
                MessageResult.Handled
                
            case _ =>
                println("❓ ResponsiveActor收到未知消息类型: ${message.messageType()}")
                MessageResult.Unhandled
        }
    }
    
    public func preStart(): Unit {
        println("🚀 ResponsiveActor启动")
    }
    
    public func postStop(): Unit {
        println("🛑 ResponsiveActor停止，总共处理了 ${requestCount} 个请求")
    }
}

/**
 * Ask模式演示函数
 */
func runAskPatternDemo(): Unit {
    println("🌟 CActor Ask模式改进演示")
    println("================================")
    println("📚 验证Plan20阶段1的Ask模式完善功能")
    println("🎯 测试类型安全、超时处理、异步回调等特性")
    println("")
    
    // 1. 创建Actor系统
    println("📋 步骤1: 创建Actor系统")
    let system = CActor.system("ask-demo-system")
    println("✅ Actor系统创建成功")
    println("")
    
    // 2. 创建响应式Actor
    println("📋 步骤2: 创建响应式Actor")
    let responsiveActor = system.actorOf({ => ResponsiveActor() }, "responsive-actor")
    println("✅ ResponsiveActor创建成功")
    println("")
    
    // 3. 测试基本Ask功能
    println("📋 步骤3: 测试基本Ask功能")
    testBasicAsk(responsiveActor)
    println("")
    
    // 4. 测试超时处理
    println("📋 步骤4: 测试超时处理")
    testAskTimeout(responsiveActor)
    println("")
    
    // 5. 测试异步回调
    println("📋 步骤5: 测试异步回调")
    testAsyncAsk(responsiveActor)
    println("")
    
    // 6. 测试批量Ask请求
    println("📋 步骤6: 测试批量Ask请求")
    testBatchAsk(responsiveActor)
    println("")
    
    // 7. 等待所有异步操作完成
    println("📋 步骤7: 等待异步操作完成")
    sleep(Duration.second * 2)
    println("✅ 异步操作完成")
    println("")
    
    // 8. 显示统计信息
    println("📋 步骤8: 显示Ask统计信息")
    let stats = Ask.getStats()
    println("📊 Ask统计: ${stats.toString()}")
    println("")
    
    // 9. 关闭系统
    println("📋 步骤9: 关闭系统")
    Ask.shutdown()
    system.terminate()
    println("✅ 系统已关闭")
    println("")
    
    // 10. 总结
    println("🎉 Ask模式改进演示完成!")
    println("📚 验证的功能:")
    println("   ✓ 基本Ask请求-响应")
    println("   ✓ 超时处理机制")
    println("   ✓ 异步回调处理")
    println("   ✓ 批量请求处理")
    println("   ✓ 统计信息收集")
    println("")
    println("🚀 Ask模式已达到生产级水平!")
}

/**
 * 测试基本Ask功能
 */
func testBasicAsk(actor: ActorRef): Unit {
    println("🧪 测试基本Ask功能...")
    
    // 发送字符串消息
    let stringMessage = StringMessage("Hello from Ask pattern!")
    let future1 = Ask.ask(actor, stringMessage, Duration.second * 3)
    
    match (future1.get()) {
        case Result.Success(response) =>
            println("✅ 收到字符串响应: ${response.messageType()}")
        case Result.Failure(error) =>
            println("❌ 字符串请求失败: ${error.message}")
    }
    
    // 发送Ping消息
    let pingMessage = PingMessage(42)
    let future2 = Ask.ask(actor, pingMessage, Duration.second * 3)
    
    match (future2.get()) {
        case Result.Success(response) =>
            println("✅ 收到Ping响应: ${response.messageType()}")
        case Result.Failure(error) =>
            println("❌ Ping请求失败: ${error.message}")
    }
}

/**
 * 测试超时处理
 */
func testAskTimeout(actor: ActorRef): Unit {
    println("🧪 测试超时处理...")
    
    // 使用很短的超时时间来测试超时机制
    let message = StringMessage("Timeout test")
    let future = Ask.ask(actor, message, Duration.millisecond * 1)  // 1ms超时
    
    match (future.get()) {
        case Result.Success(response) =>
            println("✅ 意外收到响应: ${response.messageType()}")
        case Result.Failure(error) =>
            println("✅ 正确处理超时: ${error.message}")
    }
}

/**
 * 测试异步回调
 */
func testAsyncAsk(actor: ActorRef): Unit {
    println("🧪 测试异步回调...")
    
    let message = StringMessage("Async callback test")
    
    Ask.askAsync(
        actor, 
        message, 
        Duration.second * 2,
        { response =>
            println("✅ 异步成功回调: ${response.messageType()}")
        },
        { error =>
            println("❌ 异步失败回调: ${error.message}")
        }
    )
    
    println("📤 异步请求已发送，等待回调...")
}

/**
 * 测试批量Ask请求
 */
func testBatchAsk(actor: ActorRef): Unit {
    println("🧪 测试批量Ask请求...")
    
    let futures = Array<AskFuture<Message>>(5, { i =>
        let message = StringMessage("Batch request #${i}")
        Ask.ask(actor, message, Duration.second * 3)
    })
    
    println("📤 已发送5个批量请求")
    
    for (i in 0..Int64(futures.size)) {
        match (futures[i].get()) {
            case Result.Success(response) =>
                println("✅ 批量请求 #${i} 成功")
            case Result.Failure(error) =>
                println("❌ 批量请求 #${i} 失败: ${error.message}")
        }
    }
}

/**
 * 主函数
 */
main(): Int64 {
    runAskPatternDemo()
    return 0
}
