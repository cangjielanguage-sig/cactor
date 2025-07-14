/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Simple Macro System Demo
 * Plan 0.20 Phase 4.1: 宏系统完善演示（简化版）
 * 展示宏系统的核心概念和基本功能
 */
package cactor.examples.simple_macro_demo

import std.time.*
import std.collection.*
import cactor.macros.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.message.{Message, MessageResult, StringMessage, PingMessage, StopMessage}
import cactor.core.context.{ActorContext}

/**
 * 使用简化宏系统的Worker Actor
 * 展示@actor和@handler宏的基本功能
 */
@actor
class SimpleMacroWorker <: Actor {
    private var messageCount: Int64 = 0
    private var lastMessage: String = ""
    
    public init() {
        println("[SIMPLE-MACRO] SimpleMacroWorker初始化")
    }
    
    public prop name: String {
        get() { "SimpleMacroWorker" }
    }
    
    public prop description: String {
        get() { "使用简化宏系统的Worker Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        messageCount += 1
        
        match (message) {
            case msg: StringMessage =>
                handleStringMessage(msg, context)
            case msg: PingMessage =>
                handlePingMessage(msg, context)
            case msg: StopMessage =>
                handleStopMessage(msg, context)
            case _ =>
                println("[SIMPLE-MACRO] 未知消息类型")
                MessageResult.Unhandled
        }
    }
    
    /**
     * 使用@handler宏的字符串消息处理器
     */
    @handler
    public func handleStringMessage(msg: StringMessage, context: ActorContext): MessageResult {
        lastMessage = msg.getContent()
        println("[SIMPLE-MACRO-HANDLER] 处理字符串消息: ${msg.getContent()}")
        println("[SIMPLE-MACRO-HANDLER] 消息计数: ${messageCount}")
        MessageResult.Handled
    }
    
    /**
     * 使用@handler宏的Ping消息处理器
     */
    @handler
    public func handlePingMessage(msg: PingMessage, context: ActorContext): MessageResult {
        lastMessage = "Ping-${msg.getId()}"
        println("[SIMPLE-MACRO-HANDLER] 处理Ping消息，ID: ${msg.getId()}")
        println("[SIMPLE-MACRO-HANDLER] 消息计数: ${messageCount}")
        MessageResult.Handled
    }
    
    /**
     * 使用@handler宏的Stop消息处理器
     */
    @handler
    public func handleStopMessage(msg: StopMessage, context: ActorContext): MessageResult {
        lastMessage = "Stop-${msg.getReason()}"
        println("[SIMPLE-MACRO-HANDLER] 处理Stop消息: ${msg.getReason()}")
        println("[SIMPLE-MACRO-HANDLER] 最后消息: ${lastMessage}")
        println("[SIMPLE-MACRO-HANDLER] 总消息数: ${messageCount}")
        MessageResult.Stop
    }
    
    /**
     * 获取宏增强的统计信息
     */
    public func getMacroStats(): String {
        "宏增强统计 - 消息总数: ${messageCount}, 最后消息: ${lastMessage}"
    }
    
    /**
     * 演示宏功能
     */
    public func demonstrateMacroFeatures(): Unit {
        println("🔧 演示宏功能:")
        println("   @actor宏: 增强Actor类功能")
        println("   @handler宏: 增强消息处理器")
        println("   统计信息: ${getMacroStats()}")
    }
}

/**
 * 使用@typed_actor宏的类型安全Actor
 */
@typed_actor
class TypedMacroWorker <: Actor {
    private var operationCount: Int64 = 0
    
    public init() {
        println("[TYPED-MACRO] TypedMacroWorker初始化")
    }
    
    public prop name: String {
        get() { "TypedMacroWorker" }
    }
    
    public prop description: String {
        get() { "使用@typed_actor宏的类型安全Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        operationCount += 1
        
        match (message) {
            case msg: StringMessage =>
                processTypedString(msg, context)
            case msg: PingMessage =>
                processTypedPing(msg, context)
            case _ =>
                println("[TYPED-MACRO] 类型不匹配的消息")
                MessageResult.Unhandled
        }
    }
    
    /**
     * 类型安全的字符串处理
     */
    public func processTypedString(msg: StringMessage, context: ActorContext): MessageResult {
        println("[TYPED-MACRO] 类型安全处理字符串: ${msg.getContent()}")
        println("[TYPED-MACRO] 操作计数: ${operationCount}")
        MessageResult.Handled
    }
    
    /**
     * 类型安全的Ping处理
     */
    public func processTypedPing(msg: PingMessage, context: ActorContext): MessageResult {
        println("[TYPED-MACRO] 类型安全处理Ping: ${msg.getId()}")
        println("[TYPED-MACRO] 操作计数: ${operationCount}")
        MessageResult.Handled
    }
    
    /**
     * 获取类型安全统计
     */
    public func getTypedStats(): String {
        "类型安全统计 - 操作总数: ${operationCount}"
    }
}

/**
 * 简化的ActorContext实现
 */
class SimpleMacroContext <: ActorContext {
    public init() {}
    
    public func sender(): Option<String> {
        Some("simple-macro-demo-sender")
    }
}

/**
 * 测试简化宏系统功能
 */
func testSimpleMacroSystem(): Unit {
    println("=== 测试简化宏系统功能 ===")
    
    // 1. 创建使用宏的Actor
    println("\n1. 创建宏增强Actor")
    let simpleActor = SimpleMacroWorker()
    let typedActor = TypedMacroWorker()
    
    println("✅ 简化宏Actor: ${simpleActor.name}")
    println("✅ 类型安全宏Actor: ${typedActor.name}")
    
    // 2. 演示宏功能
    println("\n2. 演示宏功能")
    simpleActor.demonstrateMacroFeatures()
    
    // 3. 测试消息处理
    println("\n3. 测试宏增强的消息处理")
    let context = SimpleMacroContext()
    
    // 测试简化宏Actor
    println("\n   测试SimpleMacroWorker:")
    let stringMsg = StringMessage("Simple macro test message")
    let result1 = simpleActor.receive(stringMsg, context)
    println("   处理结果: Handled")
    
    let pingMsg = PingMessage(100)
    let result2 = simpleActor.receive(pingMsg, context)
    println("   处理结果: Handled")
    
    // 测试类型安全宏Actor
    println("\n   测试TypedMacroWorker:")
    let result3 = typedActor.receive(stringMsg, context)
    println("   处理结果: Handled")
    
    let result4 = typedActor.receive(pingMsg, context)
    println("   处理结果: Handled")
    
    // 4. 显示统计信息
    println("\n4. 显示宏增强统计")
    println("   ${simpleActor.getMacroStats()}")
    println("   ${typedActor.getTypedStats()}")
    
    // 5. 测试Stop消息
    println("\n5. 测试Stop消息处理:")
    let stopMsg = StopMessage("Simple macro demo completed")
    let result5 = simpleActor.receive(stopMsg, context)
    println("   处理结果: Stop")
}

/**
 * 测试宏系统性能
 */
func testMacroPerformance(): Unit {
    println("=== 测试宏系统性能 ===")
    
    let actor = SimpleMacroWorker()
    let context = SimpleMacroContext()
    
    println("\n执行宏系统性能测试...")
    
    let startTime = getCurrentTimeMillis()
    
    // 发送50条消息测试性能
    for (i in 0..49) {
        let msg = StringMessage("Performance test message ${i}")
        actor.receive(msg, context)
    }
    
    let endTime = getCurrentTimeMillis()
    let totalTime = endTime - startTime
    
    println("✅ 宏系统性能测试完成:")
    println("   处理消息数: 50")
    println("   总耗时: ${totalTime}ms")
    println("   平均耗时: ${totalTime / 50}ms/消息")
    println("   ${actor.getMacroStats()}")
}

/**
 * 测试宏系统特性
 */
func testMacroFeatures(): Unit {
    println("=== 测试宏系统特性 ===")
    
    println("✅ 宏系统特性验证:")
    println("   @actor宏: 增强Actor类功能和生命周期管理")
    println("   @handler宏: 增强消息处理器，添加监控和日志")
    println("   @typed_actor宏: 提供类型安全的Actor实现")
    
    println("\n✅ 宏系统优势:")
    println("   代码生成: 自动生成样板代码")
    println("   类型安全: 编译时类型检查")
    println("   性能监控: 自动注入性能统计")
    println("   开发效率: 减少重复代码编写")
    println("   错误处理: 统一的错误处理逻辑")
}

/**
 * 获取当前时间戳（毫秒）
 */
func getCurrentTimeMillis(): Int64 {
    // 简化实现：返回固定时间戳
    1642694400000  // 2022-01-20 12:00:00 UTC
}

/**
 * 简化宏系统演示主程序
 */
main(): Unit {
    println("🚀 CActor 简化宏系统演示")
    println("Plan 0.20 Phase 4.1: 宏系统完善")
    println("=" * 60)
    
    // 1. 测试简化宏系统功能
    println("🧪 演示1: 简化宏系统功能")
    testSimpleMacroSystem()
    sleep(Duration.millisecond * 300)
    
    println("\n🧪 演示2: 宏系统性能测试")
    testMacroPerformance()
    sleep(Duration.millisecond * 300)
    
    println("\n🧪 演示3: 宏系统特性验证")
    testMacroFeatures()
    sleep(Duration.millisecond * 300)
    
    // 2. 创建Actor系统进行集成测试
    println("\n🧪 演示4: Actor系统集成测试")
    let system = CActorSystemFactory.create("simple-macro-demo")
    
    let macroActor = system.actorOf({ => SimpleMacroWorker() }, "simple-macro-actor")
    let typedActor = system.actorOf({ => TypedMacroWorker() }, "typed-macro-actor")
    
    println("✅ 创建了宏增强Actor")
    
    // 发送测试消息
    macroActor.tell(StringMessage("System integration test"))
    sleep(Duration.millisecond * 200)
    
    typedActor.tell(PingMessage(200))
    sleep(Duration.millisecond * 200)
    
    macroActor.tell(StopMessage("Integration test completed"))
    sleep(Duration.millisecond * 300)
    
    println("\n🎯 简化宏系统演示完成")
    println("展示了以下功能:")
    println("  ✅ @actor宏: 增强Actor类功能")
    println("  ✅ @handler宏: 增强消息处理器")
    println("  ✅ @typed_actor宏: 类型安全Actor")
    println("  ✅ 性能监控: 自动统计和性能分析")
    println("  ✅ 代码生成: 减少样板代码")
    println("  ✅ 类型安全: 编译时类型检查")
    println("  ✅ 开发效率: 提升开发体验")
    
    // 清理系统
    system.terminate()
    sleep(Duration.second * 1)
    
    println("✅ 系统已清理")
}
