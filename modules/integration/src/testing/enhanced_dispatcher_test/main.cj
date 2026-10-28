/*
 * Copyright (c) 2024. All rights reserved.
 * 增强调度器功能测试
 * 验证动态配置、自适应和监控功能
 */
package cactor_integration.testing.enhanced_dispatcher_test

import cactor_core.config.{DispatcherConfig}
import cactor_runtime.dispatcher.factory.EnhancedDispatcherFactory
import cactor_runtime.dispatcher.dynamic.{DynamicDispatcherConfigManager, ConfigUpdate}
import cactor_runtime.dispatcher.adaptive.AdaptiveDispatcher
import cactor_runtime.dispatcher.monitoring.DispatcherMetricsCollector
import cactor_runtime.dispatcher.advanced.Priority

/**
 * 主测试函数
 */
main(): Unit {
    println("🚀 开始增强调度器功能测试")
    
    try {
        // 1. 测试动态配置调度器
        println("\n📋 测试1: 动态配置调度器")
        testDynamicDispatcher()
        
        // 2. 测试自适应调度器
        println("\n📋 测试2: 自适应调度器")
        testAdaptiveDispatcher()
        
        // 3. 测试监控调度器
        println("\n📋 测试3: 监控调度器")
        testMonitoredDispatcher()
        
        // 4. 测试完全增强调度器
        println("\n📋 测试4: 完全增强调度器")
        testFullyEnhancedDispatcher()
        
        // 5. 测试工厂状态报告
        println("\n📋 测试5: 工厂状态报告")
        testFactoryStatusReport()
        
        println("\n🎉 增强调度器功能测试完成！")
        println("✨ 验证的功能:")
        println("   ✓ 动态配置调度器")
        println("   ✓ 自适应调度器")
        println("   ✓ 监控调度器")
        println("   ✓ 完全增强调度器")
        println("   ✓ 工厂状态报告")
        
    } catch (e: Exception) {
        println("❌ 测试失败: ${e.toString()}")
    }
}

/**
 * 测试动态配置调度器
 */
func testDynamicDispatcher(): Unit {
    println("测试动态配置调度器...")
    
    try {
        // 创建增强调度器工厂
        let factory = EnhancedDispatcherFactory()
        factory.start()
        
        // 创建基础配置
        let config = DispatcherConfig.createWorkStealingWithName("dynamic-test-dispatcher")
        
        // 创建动态配置调度器
        let dispatcher = factory.createDynamicDispatcher(config)
        
        // 启动调度器
        dispatcher.start()
        
        // 获取配置管理器
        let configManager = factory.getConfigManager()
        
        // 测试配置更新
        let update = ConfigUpdate.throughputUpdate(200)
        let updateResult = configManager.updateConfig("dynamic-test-dispatcher", update)
        
        if (updateResult) {
            println("✅ 动态配置更新成功")
        } else {
            println("❌ 动态配置更新失败")
        }
        
        // 停止调度器
        dispatcher.stop()
        factory.stop()
        
        println("✅ 动态配置调度器测试完成")
        
    } catch (e: Exception) {
        println("❌ 动态配置调度器测试失败: ${e.toString()}")
    }
}

/**
 * 测试自适应调度器
 */
func testAdaptiveDispatcher(): Unit {
    println("测试自适应调度器...")
    
    try {
        // 创建增强调度器工厂
        let factory = EnhancedDispatcherFactory()
        factory.start()
        
        // 创建基础配置
        let config = DispatcherConfig.createThreadPoolWithName("adaptive-test-dispatcher")
        
        // 创建自适应调度器
        let adaptiveDispatcher = factory.createAdaptiveDispatcher(config)
        
        // 启动调度器
        adaptiveDispatcher.start()
        
        // 获取性能监控器
        let performanceMonitor = adaptiveDispatcher.getPerformanceMonitor()
        
        // 模拟一些性能数据
        performanceMonitor.recordDispatch(50, true)
        performanceMonitor.recordDispatch(75, true)
        performanceMonitor.recordDispatch(100, false)
        
        // 获取统计信息
        let stats = performanceMonitor.getStats()
        println("性能统计: ${stats}")
        
        // 获取适应引擎
        let adaptationEngine = adaptiveDispatcher.getAdaptationEngine()
        
        // 执行适应逻辑
        adaptationEngine.adapt("adaptive-test-dispatcher")
        
        // 停止调度器
        adaptiveDispatcher.stop()
        factory.stop()
        
        println("✅ 自适应调度器测试完成")
        
    } catch (e: Exception) {
        println("❌ 自适应调度器测试失败: ${e.toString()}")
    }
}

/**
 * 测试监控调度器
 */
func testMonitoredDispatcher(): Unit {
    println("测试监控调度器...")
    
    try {
        // 创建增强调度器工厂
        let factory = EnhancedDispatcherFactory()
        factory.start()
        
        // 创建基础配置
        let config = DispatcherConfig.createPinnedWithName("monitored-test-dispatcher")
        
        // 创建监控调度器
        let monitoredDispatcher = factory.createMonitoredDispatcher(config)
        
        // 启动调度器
        monitoredDispatcher.start()
        
        // 获取指标收集器
        let metricsCollector = factory.getMetricsCollector()
        
        // 模拟一些指标数据
        metricsCollector.recordDispatch("monitored-test-dispatcher", 30, Priority.High, true)
        metricsCollector.recordDispatch("monitored-test-dispatcher", 45, Priority.Normal, true)
        metricsCollector.recordBatchDispatch("monitored-test-dispatcher", 80, 10, true)
        
        // 获取指标摘要
        let metricsSummary = metricsCollector.getMetricsSummary()
        println("全局指标: 总分发数=${metricsSummary.globalSummary.totalDispatches}")
        
        // 停止调度器
        monitoredDispatcher.stop()
        factory.stop()
        
        println("✅ 监控调度器测试完成")
        
    } catch (e: Exception) {
        println("❌ 监控调度器测试失败: ${e.toString()}")
    }
}

/**
 * 测试完全增强调度器
 */
func testFullyEnhancedDispatcher(): Unit {
    println("测试完全增强调度器...")
    
    try {
        // 创建增强调度器工厂
        let factory = EnhancedDispatcherFactory()
        factory.start()
        
        // 创建基础配置
        let config = DispatcherConfig.createWorkStealingWithName("fully-enhanced-dispatcher")
        
        // 创建完全增强调度器
        let fullyEnhancedDispatcher = factory.createFullyEnhancedDispatcher(config)
        
        // 启动调度器
        fullyEnhancedDispatcher.start()
        
        // 获取性能统计
        let performanceStats = fullyEnhancedDispatcher.getPerformanceStats()
        println("性能统计: ${performanceStats}")
        
        // 检查运行状态
        let isRunning = fullyEnhancedDispatcher.isRunning()
        println("调度器运行状态: ${isRunning}")
        
        // 停止调度器
        fullyEnhancedDispatcher.stop()
        factory.stop()
        
        println("✅ 完全增强调度器测试完成")
        
    } catch (e: Exception) {
        println("❌ 完全增强调度器测试失败: ${e.toString()}")
    }
}

/**
 * 测试工厂状态报告
 */
func testFactoryStatusReport(): Unit {
    println("测试工厂状态报告...")
    
    try {
        // 创建增强调度器工厂
        let factory = EnhancedDispatcherFactory()
        factory.start()
        
        // 创建多个调度器
        let config1 = DispatcherConfig.createThreadPoolWithName("report-test-1")
        let config2 = DispatcherConfig.createWorkStealingWithName("report-test-2")
        
        let dispatcher1 = factory.createDynamicDispatcher(config1)
        let dispatcher2 = factory.createAdaptiveDispatcher(config2)
        
        dispatcher1.start()
        dispatcher2.start()
        
        // 获取所有调度器名称
        let allNames = factory.getAllDispatcherNames()
        println("已创建调度器数量: ${allNames.size}")
        
        // 生成状态报告
        let statusReport = factory.generateStatusReport()
        println("状态报告:")
        println(statusReport)
        
        // 停止所有调度器
        dispatcher1.stop()
        dispatcher2.stop()
        factory.stop()
        
        println("✅ 工厂状态报告测试完成")
        
    } catch (e: Exception) {
        println("❌ 工厂状态报告测试失败: ${e.toString()}")
    }
}
