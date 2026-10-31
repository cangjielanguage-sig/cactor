/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - Hello World Demo（DSL 写法）
 * 最简单的CActor入门示例，展示基本的Actor创建和消息传递
 *
 * P2 示例改造：Actor 定义从"手写 receive + match"改为 @actor 宏，
 * 协议清单（cases）在编译期核对；行数对照见 docs/dsl.md（A1）。
 */
package cactor_examples.hello_world_demo

import std.time.*
import cactor.api.{CActor}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, StringMessage, PingMessage, MessageResult}
import cactor_core.context.{ActorContext}
import cactor_macros.*

/**
 * Hello World Actor - 最简单的Actor实现（DSL 写法）
 */
@actor[cases = [StringMessage, PingMessage]]
class HelloWorldActor <: Actor {
    private var messageCount: Int64 = 0

    public prop name: String {
        get() { "HelloWorldActor" }
    }

    public prop description: String {
        get() { "最简单的Hello World Actor示例" }
    }

    // "on StringMessage：记一次消息并回复"
    func onStringMessage(msg: StringMessage, context: ActorContext): MessageResult {
        messageCount += 1
        let content = msg.getContent()
        println("👋 Hello World Actor收到消息 #${messageCount}: ${content}")

        // 根据消息内容做不同响应
        if (content.contains("hello")) {
            println("🎉 Hello World Actor回复: Hello there!")
        } else if (content.contains("goodbye")) {
            println("👋 Hello World Actor回复: Goodbye!")
            return MessageResult.Stop
        } else {
            println("💬 Hello World Actor回复: I received your message!")
        }

        MessageResult.Handled
    }

    // "on PingMessage：回 Pong"
    func onPingMessage(msg: PingMessage, context: ActorContext): MessageResult {
        messageCount += 1
        println("🏓 Hello World Actor收到Ping消息 #${messageCount}, ID: ${msg.getId()}")
        println("🏓 Hello World Actor回复: Pong!")
        MessageResult.Handled
    }

    public func preStart(): Unit {
        println("🚀 Hello World Actor启动了!")
        println("📝 准备接收消息...")
    }

    public func postStop(): Unit {
        println("🛑 Hello World Actor停止了!")
        println("📊 总共处理了 ${messageCount} 条消息")
    }

    public func getMessageCount(): Int64 {
        messageCount
    }
}

/**
 * 问候Actor - 演示Actor之间的交互（DSL 写法）
 */
@actor[cases = [StringMessage]]
class GreeterActor <: Actor {
    private var greetingCount: Int64 = 0

    public prop name: String {
        get() { "GreeterActor" }
    }

    public prop description: String {
        get() { "专门负责问候的Actor" }
    }

    func onStringMessage(msg: StringMessage, context: ActorContext): MessageResult {
        greetingCount += 1
        let content = msg.getContent()

        if (content.startsWith("greet:")) {
            // 简化处理：直接使用整个消息内容
            println("🤝 Greeter Actor问候 #${greetingCount}: Hello! Welcome to CActor!")
            println("🤝 收到问候请求: ${content}")
        } else {
            println("🤝 Greeter Actor说: Hello there! (消息 #${greetingCount})")
        }

        MessageResult.Handled
    }

    public func preStart(): Unit {
        println("🤝 Greeter Actor准备开始问候!")
    }

    public func postStop(): Unit {
        println("🤝 Greeter Actor完成了 ${greetingCount} 次问候")
    }
}

/**
 * Hello World演示函数
 */
func runHelloWorldDemo(): Unit {
    println("🌟 CActor Hello World Demo")
    println("==========================")
    println("📚 这是最简单的CActor入门示例")
    println("🎯 展示基本的Actor创建、消息发送和生命周期管理")
    println("")

    // 1. 创建Actor系统
    println("📋 步骤1: 创建Actor系统")
    let system = CActor.system("hello-world-system")
    println("✅ Actor系统创建成功: hello-world-system")
    println("")

    // 2. 创建Hello World Actor
    println("📋 步骤2: 创建Hello World Actor")
    let helloActor = system.actorOf({ => HelloWorldActor() }, "hello-actor")
    println("✅ Hello World Actor创建成功")
    println("")

    // 3. 创建Greeter Actor
    println("📋 步骤3: 创建Greeter Actor")
    let greeterActor = system.actorOf({ => GreeterActor() }, "greeter-actor")
    println("✅ Greeter Actor创建成功")
    println("")

    // 4. 发送基本消息
    println("📋 步骤4: 发送基本消息")
    helloActor.tell(StringMessage("hello world"))
    helloActor.tell(StringMessage("这是我的第一条CActor消息"))
    helloActor.tell(PingMessage(42))
    println("✅ 基本消息发送完成")
    println("")

    // 5. 发送问候消息
    println("📋 步骤5: 发送问候消息")
    greeterActor.tell(StringMessage("greet:Alice"))
    greeterActor.tell(StringMessage("greet:Bob"))
    greeterActor.tell(StringMessage("greet:CActor用户"))
    println("✅ 问候消息发送完成")
    println("")

    // 6. 等待消息处理
    println("📋 步骤6: 等待消息处理...")
    // 简单的等待实现
    let startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
    while (DateTime.now().toUnixTimeStamp().toMilliseconds() - startTime < 1000) {
        // 等待1秒让消息处理完成
    }
    println("✅ 消息处理完成")
    println("")

    // 7. 发送更多交互消息
    println("📋 步骤7: 发送更多交互消息")
    helloActor.tell(StringMessage("CActor真棒!"))
    greeterActor.tell(StringMessage("普通问候"))
    helloActor.tell(PingMessage(100))
    println("✅ 交互消息发送完成")
    println("")

    // 8. 等待处理并停止
    println("📋 步骤8: 准备停止Actor")
    // 再等待一下
    let waitStart = DateTime.now().toUnixTimeStamp().toMilliseconds()
    while (DateTime.now().toUnixTimeStamp().toMilliseconds() - waitStart < 500) {
        // 等待0.5秒
    }

    // 发送停止消息
    helloActor.tell(StringMessage("goodbye"))
    println("✅ 停止消息发送完成")
    println("")

    // 9. 关闭系统
    println("📋 步骤9: 关闭Actor系统")
    // 等待一下让停止消息处理完成
    let stopStart = DateTime.now().toUnixTimeStamp().toMilliseconds()
    while (DateTime.now().toUnixTimeStamp().toMilliseconds() - stopStart < 500) {
        // 等待0.5秒
    }

    system.terminate()
    println("✅ Actor系统已关闭")
    println("")

    // 10. 总结
    println("🎉 Hello World Demo完成!")
    println("📚 你已经学会了:")
    println("   ✓ 如何创建Actor系统")
    println("   ✓ 如何定义和创建Actor")
    println("   ✓ 如何发送和处理消息")
    println("   ✓ 如何管理Actor生命周期")
    println("   ✓ 如何优雅地关闭系统")
    println("")
    println("🚀 现在你可以开始构建更复杂的CActor应用了!")
}

/**
 * 主函数
 */
main(): Int64 {
    runHelloWorldDemo()
    return 0
}
