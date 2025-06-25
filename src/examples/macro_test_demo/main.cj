/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 宏功能验证测试
 * 验证actor_dsl_macros.cj中定义的各种宏的功能
 */
package cactor.examples.macro_test_demo

import cactor.macros.*

/**
 * 简单的测试类 - 使用@simple_test宏
 */
@simple_test
class TestActor {
    public init() {}

    public func test(): String {
        return "TestActor"
    }
}

/**
 * 消息处理器 - 使用@message_handler宏
 */
@message_handler
class TestMessageHandler {
    public init() {}
}

/**
 * 状态机 - 使用@state_machine宏
 */
@state_machine
class TestStateMachine {
    public init() {}
}

/**
 * 测试日志宏
 */
func testLogMacros(): Unit {
    println("=== 测试日志宏 ===")

    @log_info("这是一条信息日志")
}

/**
 * 主函数 - 运行所有宏测试
 */
main(): Int64 {
    println("🧪 CActor宏功能验证测试开始")

    try {
        // 测试日志宏
        testLogMacros()

        // 创建测试对象
        let actor = TestActor()
        println("Actor测试: ${actor.test()}")

        let handler = TestMessageHandler()
        println("MessageHandler创建成功")

        let stateMachine = TestStateMachine()
        println("StateMachine创建成功")

        println("✅ 所有宏功能测试完成")

    } catch (e: Exception) {
        println("❌ 测试过程中发生错误: ${e.toString()}")
        return 1
    }

    return 0
}
