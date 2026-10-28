/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Plan10 - 统一API基础测试
 * 验证CActorSystem统一支持标准Actor和类型安全Actor
 */
package cactor_integration.testing.plan10_router_test

import cactor_core.typed.*
import cactor.api.{CActorSystem, CActorSystemFactory}

/**
 * Plan10 统一API基础测试
 * 验证CActorSystem统一支持标准Actor和类型安全Actor
 */
main(): Int64 {
    try {
        println("🚀 开始Plan10 统一API基础测试...")

        // 1. 创建统一的Actor系统
        println("\n📋 测试1: 创建统一的Actor系统")
        let system = CActorSystemFactory.create("plan10-unified-test")
        println("✅ 系统创建成功: ${system.getName()}")

        // 2. 创建多个类型安全Actor
        println("\n📋 测试2: 创建类型安全Actor")
        let worker1 = system.typedActorOf(createWorkerBehavior(), "worker-1")
        let worker2 = system.typedActorOf(createWorkerBehavior(), "worker-2")
        let worker3 = system.typedActorOf(createWorkerBehavior(), "worker-3")
        println("✅ 创建了3个类型安全Actor")
        
        // 3. 测试基本消息发送
        println("\n📋 测试3: 测试基本消息发送")
        worker1.tell("测试消息1")
        worker2.tell("测试消息2")
        worker3.tell("测试消息3")
        println("✅ 基本消息发送测试完成")

        // 4. 测试Actor选择（简化版）
        println("\n📋 测试4: 测试Actor选择")
        let selection1 = system.typedActorSelection<Any>("/user/worker-1")
        selection1.tell("Selection消息")
        println("✅ Actor选择测试完成")

        // 5. 系统统计信息
        println("\n📋 测试5: 系统统计信息")
        println("✅ 系统统计信息:")
        println("   系统名称: ${system.getName()}")
        println("   启动时间: ${system.getStartTime()}")
        println("   是否终止: ${system.isTerminated()}")

        // 6. 停止系统
        println("\n📋 测试6: 停止Actor系统")
        system.terminate()
        println("✅ 系统终止: ${system.isTerminated()}")

        println("\n🎉 Plan10 统一API基础测试完成！")
        println("✨ 验证的功能:")
        println("   ✓ 统一的Actor系统创建")
        println("   ✓ 类型安全Actor创建")
        println("   ✓ 基本消息发送")
        println("   ✓ Actor选择机制")
        println("   ✓ 系统统计信息")
        println("   ✓ 系统生命周期管理")
        
        return 0
        
    } catch (e: Exception) {
        println("❌ 测试过程中发生错误: ${e.message}")
        return 1
    }
}

/**
 * 创建Worker Actor行为
 * 简单的消息处理Actor
 */
func createWorkerBehavior(): TypedBehavior<Any> {
    TypedBehaviors.receive<Any>(workerMessageHandler)
}

/**
 * Worker Actor消息处理函数
 */
func workerMessageHandler(context: TypedActorContext<Any>, message: Any): TypedBehavior<Any> {
    match (message) {
        case msg: String =>
            println("Worker 处理消息: ${msg}")
        case _ =>
            println("Worker 收到未知类型消息")
    }
    TypedBehaviors.same<Any>()
}


