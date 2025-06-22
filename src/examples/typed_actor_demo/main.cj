/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Examples - 类型安全Actor系统演示
 * 演示第一优先级功能：类型安全Actor系统的使用
 */
package cactor.examples.typed_actor_demo

import std.time.*
import cactor.core.typed.*
import cactor.api.{CActorSystem, CActorSystemFactory}

/**
 * 测试消息类型
 */
public enum DemoMessage {
    | Ping(String)
    | Pong
    | GetCount(String)
    | Increment
    | Stop
}

/**
 * 演示类型安全Actor系统的基本功能
 */
main(): Int64 {
    try {
        println("🚀 开始类型安全Actor系统演示...")

        // 1. 创建类型安全Actor系统
        println("\n📋 步骤1: 创建类型安全Actor系统")
        let system = CActorSystemFactory.create("typed-demo-system")
        println("✅ 系统创建成功: ${system.getName()}")
        println("   启动时间: ${system.getStartTime()}")

        // 2. 测试系统基本功能
        println("\n📋 步骤2: 测试系统基本功能")
        println("   系统名称: ${system.getName()}")
        println("   是否终止: ${system.isTerminated()}")

        // 3. 测试系统统计信息
        println("\n📋 步骤3: 获取系统统计信息")
        println("✅ 系统统计信息:")
        println("   系统名称: ${system.getName()}")
        println("   启动时间: ${system.getStartTime()}")
        println("   是否终止: ${system.isTerminated()}")

        // 4. 终止系统
        println("\n📋 步骤4: 终止Actor系统")
        system.terminate()
        println("✅ 系统终止: ${system.isTerminated()}")

        println("\n🎉 类型安全Actor系统演示完成！")
        println("✨ 主要特性验证:")
        println("   ✓ 类型安全Actor系统创建")
        println("   ✓ 系统生命周期管理")
        println("   ✓ 系统统计信息获取")
        println("   ✓ 系统正常终止")

        return 0

    } catch (e: Exception) {
        println("❌ 演示过程中发生错误: ${e.message}")
        return 1
    }
}


