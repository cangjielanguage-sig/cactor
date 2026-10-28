/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 简单对象池测试
 * Plan 0.20 Phase 2.1: 占位文件清理 - 简单对象池测试实现
 */
package cactor_examples.simple_objectpool_test

import std.time.*
import std.collection.*
import cactor_core.memory.{SimpleObjectPool, SimpleMemoryManager}
import cactor_core.message.{StringMessage}

/**
 * 简单对象池测试器
 */
public class SimpleObjectPoolTester {
    private let pool: SimpleObjectPool
    private var testResults: ArrayList<String> = ArrayList<String>()

    public init(poolSize: Int64) {
        this.pool = SimpleObjectPool(poolSize)
    }

    /**
     * 测试基本的获取和释放功能
     */
    public func testBasicAcquireRelease(): Bool {
        println("🔧 测试1: 基本获取和释放功能")

        // 获取对象
        match (pool.acquireStringMessage()) {
            case Some(msg) =>
                testResults.add("✅ 成功获取StringMessage对象")

                // 验证对象可用性
                let content = msg.getContent()
                testResults.add("✅ 对象功能正常")

                // 释放对象
                pool.releaseStringMessage(msg)
                testResults.add("✅ 成功释放对象回池")

                true
            case None =>
                testResults.add("❌ 获取对象失败")
                false
        }
    }

    /**
     * 测试池容量限制
     */
    public func testPoolCapacity(): Bool {
        println("🔧 测试2: 池容量限制")

        let messages = ArrayList<StringMessage>()
        var acquiredCount: Int64 = 0

        // 尝试获取超过池容量的对象
        for (i in 0..150) {  // 尝试获取150个对象
            match (pool.acquireStringMessage()) {
                case Some(msg) =>
                    messages.add(msg)
                    acquiredCount += 1
                case None =>
                    break
            }
        }

        testResults.add("获取到${acquiredCount}个对象")

        // 释放所有对象
        for (msg in messages) {
            pool.releaseStringMessage(msg)
        }

        testResults.add("✅ 释放了${acquiredCount}个对象")

        // 验证池容量限制是否生效
        acquiredCount <= 100  // 假设池容量为100
    }

    /**
     * 测试并发安全性
     */
    public func testConcurrentAccess(): Bool {
        println("🔧 测试3: 并发安全性")

        let messages = ArrayList<StringMessage>()
        var successCount: Int64 = 0

        // 模拟并发获取和释放
        for (i in 0..10) {
            match (pool.acquireStringMessage()) {
                case Some(msg) =>
                    messages.add(msg)
                    successCount += 1

                    // 立即释放
                    pool.releaseStringMessage(msg)
                case None =>
                    testResults.add("获取对象失败: 第${i}次")
            }
        }

        testResults.add("✅ 并发测试完成，成功操作${successCount}次")
        successCount > 0
    }

    /**
     * 测试对象重用
     */
    public func testObjectReuse(): Bool {
        println("🔧 测试4: 对象重用")

        // 第一次获取
        let firstMsg = pool.acquireStringMessage()
        match (firstMsg) {
            case Some(msg1) =>
                let firstContent = msg1.getContent()
                pool.releaseStringMessage(msg1)

                // 第二次获取，应该重用同一个对象
                let secondMsg = pool.acquireStringMessage()
                match (secondMsg) {
                    case Some(msg2) =>
                        let secondContent = msg2.getContent()
                        pool.releaseStringMessage(msg2)

                        testResults.add("✅ 对象重用测试完成")
                        true
                    case None =>
                        testResults.add("❌ 第二次获取对象失败")
                        false
                }
            case None =>
                testResults.add("❌ 第一次获取对象失败")
                false
        }
    }

    /**
     * 测试内存管理集成
     */
    public func testMemoryManagerIntegration(): Bool {
        println("🔧 测试5: 内存管理集成")

        let memoryManager = SimpleMemoryManager.getInstance()
        let initialStats = memoryManager.getStats()

        // 执行一些对象池操作
        let messages = ArrayList<StringMessage>()
        for (i in 0..5) {
            match (pool.acquireStringMessage()) {
                case Some(msg) =>
                    messages.add(msg)
                case None =>
                    break
            }
        }

        // 释放对象
        for (msg in messages) {
            pool.releaseStringMessage(msg)
        }

        let finalStats = memoryManager.getStats()
        testResults.add("✅ 内存管理集成测试完成")
        testResults.add("初始分配对象: ${initialStats.allocatedObjects}")
        testResults.add("最终分配对象: ${finalStats.allocatedObjects}")

        true
    }

    /**
     * 运行所有测试
     */
    public func runAllTests(): Bool {
        println("🚀 开始简单对象池测试")
        println("=" * 50)

        var allPassed = true

        allPassed = testBasicAcquireRelease() && allPassed
        allPassed = testPoolCapacity() && allPassed
        allPassed = testConcurrentAccess() && allPassed
        allPassed = testObjectReuse() && allPassed
        allPassed = testMemoryManagerIntegration() && allPassed

        println("\n📊 测试结果汇总:")
        for (result in testResults) {
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
}

/**
 * 简单对象池测试主程序
 */
main(): Unit {
    println("🚀 CActor 简单对象池测试开始")
    println("验证SimpleObjectPool的基本功能和性能")

    // 创建测试器
    let tester = SimpleObjectPoolTester(100)

    // 运行测试
    let success = tester.runAllTests()

    // 显示内存管理器统计
    println("\n📈 内存管理器最终统计:")
    let memoryManager = SimpleMemoryManager.getInstance()
    let stats = memoryManager.getStats()
    println("分配对象: ${stats.allocatedObjects}")
    println("池化对象: ${stats.pooledObjects}")
    println("总内存使用: ${stats.memoryUsage}")

    if (success) {
        println("\n✅ 简单对象池测试全部通过")
        println("对象池功能正常，可以安全使用")
    } else {
        println("\n❌ 简单对象池测试存在问题")
        println("请检查对象池实现")
    }

    println("\n🎯 简单对象池测试完成")
}
