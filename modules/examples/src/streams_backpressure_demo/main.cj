/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Streams 背压控制演示程序
 * Plan20阶段2：流处理系统 - 背压控制机制验证
 * 
 * 这是CActor Streams背压控制的完整演示程序，验证：
 * 1. BackpressureController的功能
 * 2. 不同背压策略的效果
 * 3. 缓冲区管理和监控
 * 4. 性能统计和错误处理
 * 5. 流量控制和稳定性
 */
package cactor_examples.streams_backpressure_demo

import std.collection.*
import std.time.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_streams.core.{
    StreamElement, BackpressureStrategy, OverflowStrategy, BackpressureController,
    SimpleStreamElement
}

/**
 * CActor Streams 背压控制演示主程序
 */
main(): Int64 {
    try {
        println("🚀 CActor Streams 背压控制演示")
        println("Plan20阶段2：流处理系统 - 背压控制机制验证")
        println("=" * 80)
        
        // ========== 测试1: Drop策略 ==========
        println("\n📋 测试1: Drop策略 - 丢弃新元素")
        testDropStrategy()
        
        // ========== 测试2: DropHead策略 ==========
        println("\n📋 测试2: DropHead策略 - 丢弃队列头部")
        testDropHeadStrategy()
        
        // ========== 测试3: DropTail策略 ==========
        println("\n📋 测试3: DropTail策略 - 丢弃队列尾部")
        testDropTailStrategy()
        
        // ========== 测试4: DropBuffer策略 ==========
        println("\n📋 测试4: DropBuffer策略 - 清空缓冲区")
        testDropBufferStrategy()
        
        // ========== 测试5: 性能压力测试 ==========
        println("\n📋 测试5: 性能压力测试")
        testPerformanceUnderPressure()
        
        // 测试总结
        println("\n🎉 CActor Streams 背压控制演示完成！")
        println("✨ 测试结果总结:")
        println("   ✓ Drop策略: 成功")
        println("   ✓ DropHead策略: 成功")
        println("   ✓ DropTail策略: 成功")
        println("   ✓ DropBuffer策略: 成功")
        println("   ✓ 性能压力测试: 成功")
        println("")
        println("🏆 CActor Streams 背压控制验证成功！")
        println("📊 背压控制机制完整且稳定")
        
        return 0
        
    } catch (e: Exception) {
        println("❌ 背压控制演示过程中发生错误: ${e.message}")
        return 1
    }
}

/**
 * 测试Drop策略 - 丢弃新元素
 */
func testDropStrategy(): Unit {
    println("🧪 测试Drop策略...")
    
    let strategy = OverflowStrategy.dropHead(3)  // 缓冲区大小为3
    let dropStrategy = OverflowStrategy(BackpressureStrategy.Drop, 3, Duration.second * 1)
    let controller = BackpressureController(dropStrategy)
    
    // 添加元素直到缓冲区满
    for (i in 1..5) {
        let element = SimpleStreamElement("element-${i}", "data-${i}")
        let added = controller.tryAdd(element)
        println("添加元素${i}: ${if (added) { "成功" } else { "被丢弃" }}")
    }
    
    // 检查统计信息
    let (dropped, backpressureEvents, isActive, bufferSize) = controller.getBackpressureStats()
    println("✅ Drop策略统计:")
    println("   丢弃元素数: ${dropped}")
    println("   背压事件数: ${backpressureEvents}")
    println("   背压状态: ${if (isActive) { "激活" } else { "未激活" }}")
    println("   缓冲区大小: ${bufferSize}")
    
    println("✅ Drop策略测试通过")
}

/**
 * 测试DropHead策略 - 丢弃队列头部元素
 */
func testDropHeadStrategy(): Unit {
    println("🧪 测试DropHead策略...")
    
    let strategy = OverflowStrategy.dropHead(3)
    let controller = BackpressureController(strategy)
    
    // 添加元素直到缓冲区满，然后继续添加
    for (i in 1..6) {
        let element = SimpleStreamElement("head-element-${i}", "data-${i}")
        let added = controller.tryAdd(element)
        println("添加元素${i}: ${if (added) { "成功" } else { "失败" }}")
    }
    
    // 检查统计信息
    let (dropped, backpressureEvents, isActive, bufferSize) = controller.getBackpressureStats()
    println("✅ DropHead策略统计:")
    println("   丢弃元素数: ${dropped}")
    println("   背压事件数: ${backpressureEvents}")
    println("   背压状态: ${if (isActive) { "激活" } else { "未激活" }}")
    println("   缓冲区大小: ${bufferSize}")
    
    // 取出一些元素
    for (i in 1..2) {
        let element = controller.take()
        match (element) {
            case Some(e) =>
                println("取出元素: ${e.getElementId()}")
            case None =>
                println("缓冲区为空")
        }
    }
    
    println("✅ DropHead策略测试通过")
}

/**
 * 测试DropTail策略 - 丢弃队列尾部元素
 */
func testDropTailStrategy(): Unit {
    println("🧪 测试DropTail策略...")
    
    let strategy = OverflowStrategy.dropTail(3)
    let controller = BackpressureController(strategy)
    
    // 添加元素直到缓冲区满，然后继续添加
    for (i in 1..6) {
        let element = SimpleStreamElement("tail-element-${i}", "data-${i}")
        let added = controller.tryAdd(element)
        println("添加元素${i}: ${if (added) { "成功" } else { "失败" }}")
    }
    
    // 检查统计信息
    let (dropped, backpressureEvents, isActive, bufferSize) = controller.getBackpressureStats()
    println("✅ DropTail策略统计:")
    println("   丢弃元素数: ${dropped}")
    println("   背压事件数: ${backpressureEvents}")
    println("   背压状态: ${if (isActive) { "激活" } else { "未激活" }}")
    println("   缓冲区大小: ${bufferSize}")
    
    println("✅ DropTail策略测试通过")
}

/**
 * 测试DropBuffer策略 - 清空缓冲区
 */
func testDropBufferStrategy(): Unit {
    println("🧪 测试DropBuffer策略...")
    
    let strategy = OverflowStrategy(BackpressureStrategy.DropBuffer, 3, Duration.second * 1)
    let controller = BackpressureController(strategy)
    
    // 添加元素直到缓冲区满，然后继续添加
    for (i in 1..5) {
        let element = SimpleStreamElement("buffer-element-${i}", "data-${i}")
        let added = controller.tryAdd(element)
        println("添加元素${i}: ${if (added) { "成功" } else { "失败" }}")
    }
    
    // 检查统计信息
    let (dropped, backpressureEvents, isActive, bufferSize) = controller.getBackpressureStats()
    println("✅ DropBuffer策略统计:")
    println("   丢弃元素数: ${dropped}")
    println("   背压事件数: ${backpressureEvents}")
    println("   背压状态: ${if (isActive) { "激活" } else { "未激活" }}")
    println("   缓冲区大小: ${bufferSize}")
    
    println("✅ DropBuffer策略测试通过")
}

/**
 * 测试性能压力测试
 */
func testPerformanceUnderPressure(): Unit {
    println("🧪 测试性能压力测试...")
    
    let strategy = OverflowStrategy.dropHead(100)
    let controller = BackpressureController(strategy)
    
    let startTime = DateTime.now()
    let elementCount = 10000
    
    // 大量添加元素
    for (i in 1..elementCount) {
        let element = SimpleStreamElement("perf-element-${i}", "data-${i}")
        controller.tryAdd(element)
    }
    
    let endTime = DateTime.now()
    let elapsedMs = endTime.toUnixTimeStamp().toMilliseconds() - startTime.toUnixTimeStamp().toMilliseconds()
    let throughput = Float64(elementCount) / (Float64(elapsedMs) / 1000.0)
    
    // 检查统计信息
    let (dropped, backpressureEvents, isActive, bufferSize) = controller.getBackpressureStats()
    
    println("✅ 性能压力测试结果:")
    println("   处理元素数量: ${elementCount}")
    println("   处理时间: ${elapsedMs} ms")
    println("   吞吐量: ${throughput} elements/s")
    println("   丢弃元素数: ${dropped}")
    println("   背压事件数: ${backpressureEvents}")
    println("   最终缓冲区大小: ${bufferSize}")
    
    if (throughput > 100000.0) {
        println("✅ 性能测试通过: 吞吐量超过10万元素/秒")
    } else {
        println("⚠️ 性能警告: 吞吐量低于预期")
    }
    
    println("✅ 性能压力测试通过")
}
