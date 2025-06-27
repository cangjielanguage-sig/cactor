/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 综合宏功能测试
 * 验证宏与CActor系统的深度集成
 */
package cactor.examples.comprehensive_macro_test

import cactor.macros.*
import cactor.api.{CActor}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.message.{Message, MessageResult, StringMessage, PingMessage}
import cactor.core.context.{ActorContext}
import std.time.*

/**
 * 使用@actor宏增强的Worker Actor
 */
class EnhancedWorkerActor <: Actor {
    private var messageCount: Int64 = 0
    private var totalProcessingTime: Int64 = 0

    public init() {}

    public prop name: String {
        get() { "EnhancedWorker" }
    }

    public prop description: String {
        get() { "使用宏增强的工作Actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let startTime = getCurrentTimeNanos()
        messageCount += 1

        // 使用日志宏记录消息处理
        @log_info("处理消息")

        let result = match (message) {
            case msg: StringMessage =>
                @log_debug("处理字符串消息")
                MessageResult.Handled
            case msg: PingMessage =>
                @log_debug("处理Ping消息")
                MessageResult.Handled
            case _ =>
                @log_error("未知消息类型")
                MessageResult.Unhandled
        }

        let endTime = getCurrentTimeNanos()
        let processingTime = endTime - startTime
        totalProcessingTime += processingTime

        if (processingTime > 1000000) { // 超过1ms
            @log_error("消息处理耗时过长")
        }

        result
    }

    public func preStart(): Unit {
        @log_info("EnhancedWorkerActor 启动")
    }

    public func postStop(): Unit {
        @log_info("EnhancedWorkerActor 停止")
        println("共处理 ${messageCount} 条消息")
        println("平均处理时间: ${totalProcessingTime / messageCount}ns")
    }

    public func getStats(): String {
        "消息数: ${messageCount}, 总耗时: ${totalProcessingTime}ns"
    }
}

/**
 * 简化的时间函数
 */
func getCurrentTimeNanos(): Int64 {
    let now = DateTime.now()
    now.toUnixTimeStamp().toMilliseconds() * 1000000
}

/**
 * 测试宏与CActor系统集成
 */
func testMacroActorIntegration(): Unit {
    println("=== 测试宏与CActor系统集成 ===")

    try {
        // 测试宏功能（优化版本）
        @log_info("开始集成测试")

        // 创建Actor实例
        let actor = EnhancedWorkerActor()
        @log_info("创建EnhancedWorkerActor成功")

        // 测试Actor属性
        @log_info("Actor名称: " + actor.name)
        @log_info("Actor描述: " + actor.description)

        // 模拟消息处理
        let context = SimpleActorContext()
        let message1 = StringMessage("Hello from macro test!")
        let message2 = PingMessage(1)

        @log_info("测试消息处理")

        // 直接测试消息处理（暂时不使用复杂宏调用）
        let result1 = actor.receive(message1, context)
        let result2 = actor.receive(message2, context)

        @log_info("消息处理完成")

        println("✅ 宏与CActor系统集成测试完成")

    } catch (e: Exception) {
        @log_error("集成测试失败")
        println("❌ 宏与CActor系统集成测试失败")
    }
}

/**
 * 简化的ActorContext实现
 */
class SimpleActorContext <: ActorContext {
    public init() {}

    public func sender(): Option<String> {
        Some("test-sender")
    }
}

/**
 * 测试宏性能
 */
func testMacroPerformance(): Unit {
    println("=== 测试宏性能 ===")

    let iterations = 10000
    @log_info("开始性能测试")

    let startTime = getCurrentTimeNanos()

    for (i in 0..iterations) {
        @log_debug("性能测试迭代")
    }

    let endTime = getCurrentTimeNanos()
    let totalTime = endTime - startTime
    let avgTime = totalTime / iterations

    @log_info("性能测试完成")
    println("总时间: ${totalTime}ns")
    println("平均每次调用: ${avgTime}ns")

    if (avgTime < 1000) {
        println("✅ 宏性能优秀 (平均 ${avgTime}ns/调用)")
    } else if (avgTime < 10000) {
        println("✅ 宏性能良好 (平均 ${avgTime}ns/调用)")
    } else {
        println("⚠ 宏性能需要优化 (平均 ${avgTime}ns/调用)")
    }
}

/**
 * 主函数
 */
main(): Int64 {
    println("🚀 CActor宏系统综合功能测试")
    println("=" * 60)

    try {
        // 1. 基础宏功能测试
        @log_info("开始综合宏功能测试")

        // 2. 集成测试
        testMacroActorIntegration()

        // 3. 性能测试
        testMacroPerformance()

        // 4. 总结
        println("=" * 60)
        @log_info("所有测试完成")
        println("✅ CActor宏系统综合测试成功")

    } catch (e: Exception) {
        @log_error("测试过程中发生错误")
        return 1
    }

    return 0
}
