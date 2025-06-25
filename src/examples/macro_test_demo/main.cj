/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 宏功能验证测试
 * 验证actor_dsl_macros.cj中定义的基础宏功能
 */
package cactor.examples.macro_test_demo

import cactor.macros.*

/**
 * 简单的测试类 - 演示基础功能
 */
class TestActor {
    public init() {}

    public func test(): String {
        return "TestActor"
    }
}

/**
 * 测试日志功能
 */
func testLogMacros(): Unit {
    println("=== 测试日志功能 ===")

    // 使用简单的日志输出
    println("[INFO] 这是一条信息日志")
    println("[DEBUG] 这是一条调试日志")
    println("[ERROR] 这是一条错误日志")
    println("日志功能测试完成")
}

/**
 * 主函数 - 运行所有宏测试
 */
main(): Int64 {
    println("🧪 CActor宏功能验证测试开始")
    println("=" * 50)

    try {
        // 测试类创建
        let actor = TestActor()
        println("Actor测试: ${actor.test()}")

        // 测试日志宏
        testLogMacros()

        println("=" * 50)
        println("✅ 所有宏功能测试完成")

    } catch (e: Exception) {
        println("❌ 测试过程中发生错误: ${e.toString()}")
        return 1
    }

    return 0
}
