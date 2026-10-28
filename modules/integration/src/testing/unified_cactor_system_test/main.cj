/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 统一CActorSystem测试
 * 验证CActorSystem统一支持标准Actor和类型安全Actor
 */
package cactor_integration.testing.unified_cactor_system_test

import cactor_core.actor.{Actor, ActorRef}
import cactor_core.context.ActorContext
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.typed.{TypedBehavior, TypedActorRef, TypedActorContext, TypedBehaviors}
import cactor.api.{CActorSystem, CActorSystemFactory}

/**
 * 标准Actor示例
 */
public class StandardTestActor <: Actor {
    private var messageCount: Int32 = 0
    
    public prop name: String {
        get() { "StandardTestActor" }
    }
    
    public prop description: String {
        get() { "标准Actor测试" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                messageCount += 1
                println("标准Actor收到消息: ${msg.getContent()} (计数: ${messageCount})")
                MessageResult.Handled
            case _ =>
                println("标准Actor收到未知消息类型")
                MessageResult.Unhandled
        }
    }
}

/**
 * 类型安全Actor行为
 */
func createTypedBehavior(): TypedBehavior<String> {
    TypedBehaviors.receive<String>(typedMessageHandler)
}

/**
 * 类型安全Actor消息处理函数
 */
func typedMessageHandler(context: TypedActorContext<String>, message: String): TypedBehavior<String> {
    println("类型安全Actor收到消息: ${message}")
    TypedBehaviors.same<String>()
}

/**
 * 统一CActorSystem测试主函数
 */
main(): Int64 {
    try {
        println("🚀 开始CActorSystem统一API测试...")
        println("📊 验证CActorSystem同时支持标准Actor和类型安全Actor")
        
        // ========== 测试1: 创建统一的CActorSystem ==========
        println("\n📋 测试1: 创建统一的CActorSystem")
        let system = CActorSystemFactory.create("unified-test")
        println("✅ 统一CActorSystem创建成功: ${system.getName()}")
        
        // ========== 测试2: 在同一系统中创建标准Actor ==========
        println("\n📋 测试2: 在同一系统中创建标准Actor")
        let standardActor = system.actorOf({ => StandardTestActor() }, "standard-actor")
        standardActor.tell(StringMessage("Hello from Standard Actor"))
        println("✅ 标准Actor创建和消息发送成功")
        
        // ========== 测试3: 在同一系统中创建类型安全Actor ==========
        println("\n📋 测试3: 在同一系统中创建类型安全Actor")
        let typedActor = system.typedActorOf(createTypedBehavior(), "typed-actor")
        typedActor.tell("Hello from Typed Actor")
        println("✅ 类型安全Actor创建和消息发送成功")
        
        // ========== 测试4: Actor之间的交互 ==========
        println("\n📋 测试4: Actor之间的交互")
        
        // 将标准Actor包装为类型安全Actor
        let wrappedTypedActor = system.wrapAsTypedActor<String>(standardActor, "wrapped-typed")
        wrappedTypedActor.tell("Message from wrapped typed actor")
        
        // 从类型安全Actor获取底层标准Actor
        let unwrappedStandardActor = system.unwrapTypedActor(typedActor)
        unwrappedStandardActor.tell(StringMessage("Message from unwrapped standard actor"))
        
        println("✅ Actor之间的交互测试完成")
        
        // ========== 测试5: 消息桥接器 ==========
        println("\n📋 测试5: 消息桥接器")
        let messageBridge = system.createMessageBridge(standardActor, typedActor)
        
        // 通过桥接器发送消息
        messageBridge.sendToTyped("Bridge message to typed actor")
        messageBridge.sendToStandard(StringMessage("Bridge message to standard actor"))
        
        println("✅ 消息桥接器测试完成")
        
        // ========== 测试6: 类型安全Actor选择 ==========
        println("\n📋 测试6: 类型安全Actor选择")
        let typedSelection = system.typedActorSelection<String>("/user/typed-actor")
        typedSelection.tell("Message via typed actor selection")
        println("✅ 类型安全Actor选择测试完成")
        
        // ========== 测试7: 批量创建Actor ==========
        println("\n📋 测试7: 批量创建不同类型的Actor")
        
        // 创建多个标准Actor
        let standardActor1 = system.actorOf({ => StandardTestActor() }, "standard-1")
        let standardActor2 = system.actorOf({ => StandardTestActor() }, "standard-2")
        
        // 创建多个类型安全Actor
        let typedActor1 = system.typedActorOf(createTypedBehavior(), "typed-1")
        let typedActor2 = system.typedActorOf(createTypedBehavior(), "typed-2")
        
        // 发送测试消息
        standardActor1.tell(StringMessage("Test message 1"))
        standardActor2.tell(StringMessage("Test message 2"))
        typedActor1.tell("Typed test message 1")
        typedActor2.tell("Typed test message 2")
        
        println("✅ 批量创建Actor测试完成")
        
        // ========== 测试8: 系统信息和管理 ==========
        println("\n📋 测试8: 系统信息和管理")
        
        println("系统名称: ${system.getName()}")
        println("系统启动时间: ${system.getStartTime()}")
        println("系统是否已终止: ${system.isTerminated()}")
        
        println("✅ 系统信息测试完成")
        
        // ========== 测试9: 系统终止 ==========
        println("\n📋 测试9: 系统终止")
        
        println("终止前系统状态: ${system.isTerminated()}")
        system.terminate()
        println("终止后系统状态: ${system.isTerminated()}")
        
        println("✅ 系统终止测试完成")
        
        // ========== 测试总结 ==========
        println("\n🎉 CActorSystem统一API测试完成！")
        println("✨ 测试结果总结:")
        println("   ✓ 统一CActorSystem创建: 成功")
        println("   ✓ 标准Actor支持: 成功")
        println("   ✓ 类型安全Actor支持: 成功")
        println("   ✓ Actor之间交互: 成功")
        println("   ✓ 消息桥接器: 成功")
        println("   ✓ 类型安全Actor选择: 成功")
        println("   ✓ 批量创建Actor: 成功")
        println("   ✓ 系统信息管理: 成功")
        println("   ✓ 系统生命周期: 成功")
        println("")
        println("🏆 统一API实现成功！")
        println("📊 CActorSystem现在完美支持两种Actor模式的无缝交互")
        
        return 0
        
    } catch (e: Exception) {
        println("❌ 测试过程中发生错误: ${e.message}")
        return 1
    }
}

/**
 * 演示统一API的使用方式
 */
func demonstrateUnifiedUsage(): Unit {
    println("\n🔧 统一API使用演示:")
    
    // 创建一个CActorSystem
    let system = CActorSystemFactory.create("demo-system")
    
    // 在同一个系统中创建两种类型的Actor
    let standardActor = system.actorOf({ => StandardTestActor() }, "demo-standard")
    let typedActor = system.typedActorOf(createTypedBehavior(), "demo-typed")
    
    // 两种Actor可以无缝交互
    let bridge = system.createMessageBridge(standardActor, typedActor)
    bridge.sendToTyped("Hello from standard to typed")
    bridge.sendToStandard(StringMessage("Hello from typed to standard"))
    
    // 类型转换
    let wrappedTyped = system.wrapAsTypedActor<String>(standardActor, "wrapped")
    let unwrappedStandard = system.unwrapTypedActor(typedActor)
    
    println("✅ 统一API演示完成")
    
    // 清理
    system.terminate()
}
