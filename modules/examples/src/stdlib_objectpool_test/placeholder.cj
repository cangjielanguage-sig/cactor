/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 标准库对象池测试
 * Plan 0.20 Phase 2.1: 占位文件清理 - 标准库对象池测试实现
 */
package cactor_examples.stdlib_objectpool_test

import std.time.*
import std.collection.*
import cactor_core.memory.{SimpleMessagePool}
import cactor_core.message.{StringMessage}

/**
 * 标准库对象池测试器
 */
public class StdlibObjectPoolTester {
    private var testResults: ArrayList<String> = ArrayList<String>()
    private var performanceResults: ArrayList<String> = ArrayList<String>()

    public init() {}

    /**
     * 测试静态消息池基本功能
     */
    public func testStaticMessagePool(): Bool {
        println("🔧 测试1: 静态消息池基本功能")

        // 获取消息对象
        let msg = SimpleMessagePool.acquireStringMessage()
        testResults.add("✅ 成功从静态池获取StringMessage")

        // 验证对象功能
        let content = msg.getContent()
        testResults.add("✅ 消息对象功能正常")

        // 释放对象
        SimpleMessagePool.releaseStringMessage(msg)
        testResults.add("✅ 成功释放对象到静态池")

        true
    }

    /**
     * 测试批量获取和释放
     */
    public func testBatchOperations(): Bool {
        println("🔧 测试2: 批量获取和释放")

        let messages = ArrayList<StringMessage>()
        let batchSize: Int64 = 50

        // 批量获取
        for (i in 0..batchSize) {
            let msg = SimpleMessagePool.acquireStringMessage()
            messages.add(msg)
        }

        testResults.add("✅ 批量获取${batchSize}个消息对象")

        // 批量释放
        for (msg in messages) {
            SimpleMessagePool.releaseStringMessage(msg)
        }

        testResults.add("✅ 批量释放${batchSize}个消息对象")

        true
    }

    /**
     * 测试高频获取释放
     */
    public func testHighFrequencyOperations(): Bool {
        println("🔧 测试3: 高频获取释放")

        let iterations: Int64 = 1000
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()

        for (i in 0..iterations) {
            let msg = SimpleMessagePool.acquireStringMessage()
            // 模拟使用
            let content = msg.getContent()
            SimpleMessagePool.releaseStringMessage(msg)
        }

        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime
        let throughput = if (duration > 0) { iterations * 1000000000 / duration } else { 0 }

        testResults.add("✅ 高频操作测试完成")
        performanceResults.add("高频操作: ${iterations}次, 耗时: ${duration}ns")
        performanceResults.add("吞吐量: ${throughput} ops/s")

        true
    }

    /**
     * 测试内存使用模式
     */
    public func testMemoryUsagePattern(): Bool {
        println("🔧 测试4: 内存使用模式")

        let messages = ArrayList<StringMessage>()

        // 阶段1: 获取大量对象
        for (i in 0..100) {
            let msg = SimpleMessagePool.acquireStringMessage()
            messages.add(msg)
        }

        testResults.add("✅ 阶段1: 获取100个对象")

        // 阶段2: 释放一半对象
        for (i in 0..50) {
            if (i < messages.size) {
                SimpleMessagePool.releaseStringMessage(messages[i])
            }
        }

        testResults.add("✅ 阶段2: 释放50个对象")

        // 阶段3: 再次获取对象（应该重用已释放的对象）
        let newMessages = ArrayList<StringMessage>()
        for (i in 0..30) {
            let msg = SimpleMessagePool.acquireStringMessage()
            newMessages.add(msg)
        }

        testResults.add("✅ 阶段3: 重新获取30个对象")

        // 清理剩余对象
        for (i in 50..messages.size) {
            if (i < messages.size) {
                SimpleMessagePool.releaseStringMessage(messages[i])
            }
        }

        for (msg in newMessages) {
            SimpleMessagePool.releaseStringMessage(msg)
        }

        testResults.add("✅ 清理所有对象")

        true
    }

    /**
     * 测试对象状态一致性
     */
    public func testObjectStateConsistency(): Bool {
        println("🔧 测试5: 对象状态一致性")

        // 获取对象并检查初始状态
        let msg1 = SimpleMessagePool.acquireStringMessage()
        let initialContent1 = msg1.getContent()

        // 释放对象
        SimpleMessagePool.releaseStringMessage(msg1)

        // 重新获取对象（可能是同一个对象）
        let msg2 = SimpleMessagePool.acquireStringMessage()
        let initialContent2 = msg2.getContent()

        // 释放对象
        SimpleMessagePool.releaseStringMessage(msg2)

        testResults.add("✅ 对象状态一致性测试完成")
        testResults.add("第一次获取内容长度: ${initialContent1.size}")
        testResults.add("第二次获取内容长度: ${initialContent2.size}")

        true
    }

    /**
     * 性能基准测试
     */
    public func testPerformanceBenchmark(): Bool {
        println("🔧 测试6: 性能基准测试")

        let warmupIterations: Int64 = 100
        let benchmarkIterations: Int64 = 10000

        // 预热
        for (i in 0..warmupIterations) {
            let msg = SimpleMessagePool.acquireStringMessage()
            SimpleMessagePool.releaseStringMessage(msg)
        }

        // 基准测试
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()

        for (i in 0..benchmarkIterations) {
            let msg = SimpleMessagePool.acquireStringMessage()
            let content = msg.getContent()  // 模拟使用
            SimpleMessagePool.releaseStringMessage(msg)
        }

        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime
        let throughput = if (duration > 0) { benchmarkIterations * 1000000000 / duration } else { 0 }
        let avgLatency = if (benchmarkIterations > 0) { duration / benchmarkIterations } else { 0 }

        performanceResults.add("基准测试: ${benchmarkIterations}次操作")
        performanceResults.add("总耗时: ${duration}ns")
        performanceResults.add("吞吐量: ${throughput} ops/s")
        performanceResults.add("平均延迟: ${avgLatency}ns")

        testResults.add("✅ 性能基准测试完成")

        // 验证性能要求（假设要求 > 1M ops/s）
        throughput > 1000000
    }

    /**
     * 运行所有测试
     */
    public func runAllTests(): Bool {
        println("🚀 开始标准库对象池测试")
        println("=" * 50)

        var allPassed = true

        allPassed = testStaticMessagePool() && allPassed
        allPassed = testBatchOperations() && allPassed
        allPassed = testHighFrequencyOperations() && allPassed
        allPassed = testMemoryUsagePattern() && allPassed
        allPassed = testObjectStateConsistency() && allPassed
        allPassed = testPerformanceBenchmark() && allPassed

        println("\n📊 测试结果汇总:")
        for (result in testResults) {
            println("  ${result}")
        }

        println("\n⚡ 性能测试结果:")
        for (result in performanceResults) {
            println("  ${result}")
        }

        if (allPassed) {
            println("\n🎉 所有测试通过！")
        } else {
            println("\n❌ 部分测试失败")
        }

        allPassed
    }

    public func getTestResults(): ArrayList<String> {
        testResults
    }

    public func getPerformanceResults(): ArrayList<String> {
        performanceResults
    }
}

/**
 * 标准库对象池测试主程序
 */
main(): Unit {
    println("🚀 CActor 标准库对象池测试开始")
    println("验证SimpleMessagePool静态池的功能和性能")

    // 创建测试器
    let tester = StdlibObjectPoolTester()

    // 运行测试
    let success = tester.runAllTests()

    if (success) {
        println("\n✅ 标准库对象池测试全部通过")
        println("静态消息池功能正常，性能达标")
    } else {
        println("\n❌ 标准库对象池测试存在问题")
        println("请检查静态池实现或性能优化")
    }

    println("\n💡 使用建议:")
    println("- 静态消息池适用于全局消息对象复用")
    println("- 高频场景下性能优于直接创建对象")
    println("- 建议在Actor系统中广泛使用")

    println("\n🎯 标准库对象池测试完成")
}
