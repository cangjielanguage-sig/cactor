/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 对象池性能对比演示
 * Plan 0.20 Phase 2.1: 占位文件清理 - 对象池性能对比实现
 */
package cactor_examples.objectpool_performance_comparison

import std.time.*
import std.collection.*
import cactor_core.memory.{SimpleObjectPool, SimpleMemoryManager, SimpleMessagePool}
import cactor_core.message.{StringMessage}

/**
 * 性能测试结果
 */
public struct PerformanceResult {
    public let testName: String
    public let operationCount: Int64
    public let durationNanos: Int64
    public let throughput: Int64

    public init(name: String, count: Int64, duration: Int64) {
        this.testName = name
        this.operationCount = count
        this.durationNanos = duration
        this.throughput = if (duration > 0) { count * 1000000000 / duration } else { 0 }
    }

    public func toString(): String {
        "测试: ${testName}, 操作数: ${operationCount}, 耗时: ${durationNanos}ns, 吞吐量: ${throughput} ops/s"
    }
}

/**
 * 对象池性能测试器
 */
public class ObjectPoolPerformanceTester {
    private let testIterations: Int64

    public init(iterations: Int64) {
        this.testIterations = iterations
    }

    /**
     * 测试直接对象创建性能
     */
    public func testDirectObjectCreation(): PerformanceResult {
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()

        // 直接创建对象
        for (i in 0..testIterations) {
            let msg = StringMessage("test-message-${i}")
            // 模拟使用对象
            let content = msg.getContent()
        }

        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime

        PerformanceResult("直接对象创建", testIterations, duration)
    }

    /**
     * 测试对象池性能
     */
    public func testObjectPoolPerformance(): PerformanceResult {
        let pool = SimpleObjectPool(100)
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()

        // 使用对象池
        for (i in 0..testIterations) {
            match (pool.acquireStringMessage()) {
                case Some(msg) =>
                    // 模拟使用对象
                    let content = msg.getContent()
                    // 释放对象回池
                    pool.releaseStringMessage(msg)
                case None =>
                    // 池为空，创建新对象
                    let msg = StringMessage("fallback-${i}")
                    let content = msg.getContent()
            }
        }

        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime

        PerformanceResult("对象池复用", testIterations, duration)
    }

    /**
     * 测试静态消息池性能
     */
    public func testStaticMessagePoolPerformance(): PerformanceResult {
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()

        // 使用静态消息池
        for (i in 0..testIterations) {
            let msg = SimpleMessagePool.acquireStringMessage()
            // 模拟使用对象
            let content = msg.getContent()
            // 释放对象回池
            SimpleMessagePool.releaseStringMessage(msg)
        }

        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime

        PerformanceResult("静态消息池", testIterations, duration)
    }

    /**
     * 测试内存分配压力
     */
    public func testMemoryPressure(): PerformanceResult {
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let messages = ArrayList<StringMessage>()

        // 创建大量对象测试内存压力
        for (i in 0..testIterations) {
            let msg = StringMessage("pressure-test-${i}")
            messages.add(msg)
        }

        // 清理内存
        messages.clear()

        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime

        PerformanceResult("内存压力测试", testIterations, duration)
    }
}

/**
 * 性能对比分析器
 */
public class PerformanceAnalyzer {
    private let results: ArrayList<PerformanceResult> = ArrayList<PerformanceResult>()

    public func addResult(result: PerformanceResult): Unit {
        results.add(result)
    }

    public func printAnalysis(): Unit {
        println("\n📊 对象池性能对比分析报告")
        println("=" * 60)

        for (result in results) {
            println(result.toString())
        }

        if (results.size >= 2) {
            println("\n🔍 性能对比分析:")

            // 找到最快和最慢的测试
            var fastest = results[0]
            var slowest = results[0]

            for (result in results) {
                if (result.throughput > fastest.throughput) {
                    fastest = result
                }
                if (result.throughput < slowest.throughput) {
                    slowest = result
                }
            }

            println("🚀 最快: ${fastest.testName} (${fastest.throughput} ops/s)")
            println("🐌 最慢: ${slowest.testName} (${slowest.throughput} ops/s)")

            if (slowest.throughput > 0) {
                let speedup = fastest.throughput / slowest.throughput
                println("⚡ 性能提升: ${speedup}x")
            }
        }

        println("\n💡 优化建议:")
        println("- 对象池适用于频繁创建/销毁的场景")
        println("- 静态消息池提供更好的全局复用")
        println("- 直接创建适用于一次性使用场景")
        println("- 内存压力测试显示GC影响")
    }

    public func getResultCount(): Int64 {
        results.size
    }
}

/**
 * 对象池性能对比演示主程序
 */
main(): Unit {
    println("🚀 CActor 对象池性能对比演示开始")
    println("测试不同对象创建策略的性能差异")

    let testIterations: Int64 = 10000
    let tester = ObjectPoolPerformanceTester(testIterations)
    let analyzer = PerformanceAnalyzer()

    println("\n🔧 开始性能测试 (${testIterations} 次操作)...")

    // 测试1: 直接对象创建
    println("\n1️⃣ 测试直接对象创建...")
    let directResult = tester.testDirectObjectCreation()
    analyzer.addResult(directResult)
    println("✅ 完成")

    // 测试2: 对象池性能
    println("\n2️⃣ 测试对象池性能...")
    let poolResult = tester.testObjectPoolPerformance()
    analyzer.addResult(poolResult)
    println("✅ 完成")

    // 测试3: 静态消息池性能
    println("\n3️⃣ 测试静态消息池性能...")
    let staticPoolResult = tester.testStaticMessagePoolPerformance()
    analyzer.addResult(staticPoolResult)
    println("✅ 完成")

    // 测试4: 内存压力测试
    println("\n4️⃣ 测试内存分配压力...")
    let memoryResult = tester.testMemoryPressure()
    analyzer.addResult(memoryResult)
    println("✅ 完成")

    // 分析结果
    analyzer.printAnalysis()

    // 内存管理器统计
    println("\n📈 内存管理器统计:")
    let memoryManager = SimpleMemoryManager.getInstance()
    let stats = memoryManager.getStats()
    println("分配对象: ${stats.allocatedObjects}")
    println("池化对象: ${stats.pooledObjects}")
    println("总内存使用: ${stats.memoryUsage}")

    println("\n🎯 对象池性能对比演示完成")
    println("建议根据使用场景选择合适的对象创建策略")
}
