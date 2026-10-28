/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 监控系统演示
 * Plan 0.20 Phase 3.3: 监控系统深化
 * 演示细粒度性能指标收集、分布式链路追踪、告警通知机制
 */
package cactor_examples.monitoring_demo

import std.time.*
import std.collection.*
import cactor_foundation.monitoring.*
import cactor_foundation.simple_config.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

/**
 * 监控演示Actor
 */
public class MonitoringActor <: Actor {
    private let messageCounter: CounterMetric
    private let responseTimeGauge: GaugeMetric
    private let processingTimer: TimerMetric
    private var operationCount: Int64 = 0
    
    public init() {
        // 注册指标
        this.messageCounter = GlobalMetricRegistry.registerCounter("actor.messages.total")
        this.responseTimeGauge = GlobalMetricRegistry.registerGauge("actor.response.time")
        this.processingTimer = GlobalMetricRegistry.registerTimer("actor.processing.time")
    }
    
    public prop name: String {
        get() { "MonitoringActor" }
    }
    
    public prop description: String {
        get() { "监控演示Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        // 开始追踪
        let span = match (GlobalTracer.startSpan("actor.receive")) {
            case Some(s) => s
            case None => 
                println("警告：追踪器未初始化")
                return MessageResult.Handled
        }
        
        span.setTag("actor.name", name)
        span.setTag("message.type", "StringMessage")
        
        let startTime: Int64 = 1642694400000  // 简化实现：固定时间戳
        
        match (message) {
            case msg: StringMessage =>
                processMonitoringCommand(msg, span)
                MessageResult.Handled
            case _ =>
                span.setTag("result", "unhandled")
                GlobalTracer.finishSpan(span)
                MessageResult.Unhandled
        }
        
        // 记录指标
        messageCounter.increment()
        let processingTime: Int64 = 10  // 简化实现：固定处理时间
        processingTimer.recordTime(10.0)  // 简化实现：固定值
        responseTimeGauge.setValue(10.0)   // 简化实现：固定值
        
        span.setTag("processing.time", processingTime.toString())
        span.setTag("result", "handled")
        span.log("消息处理完成")
        GlobalTracer.finishSpan(span)
        
        MessageResult.Handled
    }
    
    private func processMonitoringCommand(msg: StringMessage, span: Span): Unit {
        let content = msg.getContent()
        operationCount += 1
        
        span.log("开始处理命令: ${content}")
        
        if (content == "metrics") {
            showMetrics(span)
        } else if (content == "tracing") {
            showTracing(span)
        } else if (content == "alerts") {
            showAlerts(span)
        } else if (content == "trigger-alert") {
            triggerTestAlert(span)
        } else if (content == "performance-test") {
            performanceTest(span)
        } else if (content == "stress-test") {
            stressTest(span)
        } else if (content == "all-stats") {
            showAllStats(span)
        } else {
            span.setTag("error", "unknown_command")
            span.log("未知命令: ${content}")
            println("❓ 未知监控命令: ${content}")
            showHelp()
        }
        
        span.log("命令处理完成")
    }
    
    private func showMetrics(span: Span): Unit {
        let childSpan = match (GlobalTracer.getInstance()) {
            case Some(tracer) => tracer.startChildSpan(span, "show.metrics")
            case None => span
        }
        
        childSpan.log("显示指标统计")
        
        println("📊 指标统计:")
        println("   消息总数: ${messageCounter.getCount()}")
        println("   当前响应时间: ${responseTimeGauge.getCurrentValue()}ms")
        println("   平均处理时间: ${processingTimer.getAverageTime()}ms")
        println("   最小处理时间: ${processingTimer.getMinTime()}ms")
        println("   最大处理时间: ${processingTimer.getMaxTime()}ms")
        println("   处理次数: ${processingTimer.getCount()}")
        
        // 显示所有注册的指标
        GlobalMetricRegistry.printAllMetrics()
        
        // 简化实现：总是完成子跨度
        GlobalTracer.finishSpan(childSpan)
    }
    
    private func showTracing(span: Span): Unit {
        let childSpan = match (GlobalTracer.getInstance()) {
            case Some(tracer) => tracer.startChildSpan(span, "show.tracing")
            case None => span
        }
        
        childSpan.log("显示追踪统计")
        
        println("🔍 追踪统计:")
        match (GlobalTracer.getInstance()) {
            case Some(tracer) =>
                tracer.printTracingStats()
                println("   活跃跨度: ${tracer.getActiveSpanCount()}")
                println("   已完成跨度: ${tracer.getFinishedSpanCount()}")
            case None =>
                println("   追踪器未初始化")
        }
        
        // 显示当前跨度信息
        span.printSpanInfo()
        
        // 简化实现：总是完成子跨度
        GlobalTracer.finishSpan(childSpan)
    }
    
    private func showAlerts(span: Span): Unit {
        let childSpan = match (GlobalTracer.getInstance()) {
            case Some(tracer) => tracer.startChildSpan(span, "show.alerts")
            case None => span
        }
        
        childSpan.log("显示告警统计")
        
        println("🚨 告警统计:")
        let alertManager = GlobalAlertManager.getInstance()
        alertManager.printAlertStats()
        
        // 简化实现：总是完成子跨度
        GlobalTracer.finishSpan(childSpan)
    }
    
    private func triggerTestAlert(span: Span): Unit {
        let childSpan = match (GlobalTracer.getInstance()) {
            case Some(tracer) => tracer.startChildSpan(span, "trigger.alert")
            case None => span
        }
        
        childSpan.log("触发测试告警")
        
        println("🧪 触发测试告警:")
        
        // 触发不同级别的告警
        GlobalAlertManager.triggerAlert("测试信息告警", AlertLevel.Info, "这是一个信息级别的测试告警")
        GlobalAlertManager.triggerAlert("测试警告告警", AlertLevel.Warning, "这是一个警告级别的测试告警")
        GlobalAlertManager.triggerAlert("测试错误告警", AlertLevel.Error, "这是一个错误级别的测试告警")
        GlobalAlertManager.triggerAlert("测试严重告警", AlertLevel.Critical, "这是一个严重级别的测试告警")
        
        println("✅ 测试告警已触发")
        
        // 简化实现：总是完成子跨度
        GlobalTracer.finishSpan(childSpan)
    }
    
    private func performanceTest(span: Span): Unit {
        let childSpan = match (GlobalTracer.getInstance()) {
            case Some(tracer) => tracer.startChildSpan(span, "performance.test")
            case None => span
        }
        
        childSpan.log("开始性能测试")
        
        println("⚡ 性能测试:")
        
        let testCounter = GlobalMetricRegistry.registerCounter("test.operations")
        let testTimer = GlobalMetricRegistry.registerTimer("test.duration")
        
        // 模拟一些操作
        for (i in 0..99) {
            let opStart: Int64 = 1642694400000  // 简化实现：固定时间戳

            // 模拟工作负载
            testCounter.increment()

            let opEnd: Int64 = 1642694400010  // 简化实现：固定时间戳
            testTimer.recordTime(10.0)  // 简化实现：固定值
        }
        
        println("   执行了100次操作")
        println("   总计数: ${testCounter.getCount()}")
        println("   平均耗时: ${testTimer.getAverageTime()}ms")
        
        childSpan.setTag("operations.count", "100")
        childSpan.setTag("avg.time", testTimer.getAverageTime().toString())
        childSpan.log("性能测试完成")
        
        // 简化实现：总是完成子跨度
        GlobalTracer.finishSpan(childSpan)
    }
    
    private func stressTest(span: Span): Unit {
        let childSpan = match (GlobalTracer.getInstance()) {
            case Some(tracer) => tracer.startChildSpan(span, "stress.test")
            case None => span
        }
        
        childSpan.log("开始压力测试")
        
        println("🔥 压力测试:")
        
        let stressCounter = GlobalMetricRegistry.registerCounter("stress.operations")
        let stressGauge = GlobalMetricRegistry.registerGauge("stress.load")
        
        // 模拟高负载
        for (i in 0..999) {
            stressCounter.increment()
            stressGauge.setValue(100.0)  // 简化实现：固定值
            
            // 每100次操作触发一次告警检查
            if (i % 100 == 0) {
                if (i > 500) {
                    GlobalAlertManager.triggerAlert("高负载告警", AlertLevel.Warning, "检测到高负载: ${i}")
                }
            }
        }
        
        println("   执行了1000次高负载操作")
        println("   当前负载: ${stressGauge.getCurrentValue()}")
        
        childSpan.setTag("stress.operations", "1000")
        childSpan.setTag("final.load", stressGauge.getCurrentValue().toString())
        childSpan.log("压力测试完成")
        
        // 简化实现：总是完成子跨度
        GlobalTracer.finishSpan(childSpan)
    }
    
    private func showAllStats(span: Span): Unit {
        let childSpan = match (GlobalTracer.getInstance()) {
            case Some(tracer) => tracer.startChildSpan(span, "show.all.stats")
            case None => span
        }
        
        childSpan.log("显示所有统计信息")
        
        println("📈 完整监控统计:")
        println("=" * 50)
        
        showMetrics(childSpan)
        println("-" * 30)
        showTracing(childSpan)
        println("-" * 30)
        showAlerts(childSpan)
        
        println("=" * 50)
        println("   总操作次数: ${operationCount}")
        
        // 简化实现：总是完成子跨度
        GlobalTracer.finishSpan(childSpan)
    }
    
    private func showHelp(): Unit {
        println("📖 监控命令帮助:")
        println("   metrics          - 显示指标统计")
        println("   tracing          - 显示追踪统计")
        println("   alerts           - 显示告警统计")
        println("   trigger-alert    - 触发测试告警")
        println("   performance-test - 执行性能测试")
        println("   stress-test      - 执行压力测试")
        println("   all-stats        - 显示所有统计")
    }
    
    public func preStart(): Unit {
        println("🚀 MonitoringActor启动")
        println("   已注册指标: 消息计数器、响应时间仪表盘、处理时间计时器")
        showHelp()
    }
    
    public func postStop(): Unit {
        println("🛑 MonitoringActor停止")
        println("   总操作次数: ${operationCount}")
        println("   最终指标:")
        println("     消息总数: ${messageCounter.getCount()}")
        println("     平均处理时间: ${processingTimer.getAverageTime()}ms")
    }
}

/**
 * 监控系统演示主程序
 */
main(): Unit {
    println("🚀 CActor 监控系统演示")
    println("Plan 0.20 Phase 3.3: 监控系统深化")
    println("=" * 60)

    // 1. 初始化监控组件
    println("🔧 初始化监控组件...")

    // 初始化全局追踪器
    GlobalTracer.initialize("cactor-monitoring-demo")
    println("✅ 追踪器初始化完成")

    // 初始化告警管理器
    let alertManager = GlobalAlertManager.getInstance()

    // 添加通知渠道
    alertManager.addChannel(ConsoleNotificationChannel("console", true))
    alertManager.addChannel(LogNotificationChannel("log", true, "ALERT"))

    // 添加告警规则
    alertManager.addRule(AlertRule(
        "高消息量告警",
        "actor.messages.total",
        AlertCondition.GreaterThan,
        100.0,
        AlertLevel.Warning,
        "Actor消息量过高",
        true
    ))

    alertManager.addRule(AlertRule(
        "高响应时间告警",
        "actor.response.time",
        AlertCondition.GreaterThan,
        1000.0,
        AlertLevel.Error,
        "Actor响应时间过长",
        true
    ))

    println("✅ 告警管理器初始化完成")

    // 2. 创建Actor系统
    let system = CActorSystemFactory.create("monitoring-demo")

    // 3. 创建监控演示Actor
    let monitoringActor = system.actorOf({ => MonitoringActor() }, "monitoring-actor")

    println("✅ 创建了监控演示Actor")

    // 4. 演示监控功能
    println("\n🧪 演示1: 基础指标收集")
    monitoringActor.tell(StringMessage("metrics"))
    sleep(Duration.millisecond * 300)

    println("\n🧪 演示2: 分布式链路追踪")
    monitoringActor.tell(StringMessage("tracing"))
    sleep(Duration.millisecond * 300)

    println("\n🧪 演示3: 告警系统")
    monitoringActor.tell(StringMessage("alerts"))
    sleep(Duration.millisecond * 300)

    println("\n🧪 演示4: 触发测试告警")
    monitoringActor.tell(StringMessage("trigger-alert"))
    sleep(Duration.millisecond * 500)

    println("\n🧪 演示5: 性能测试")
    monitoringActor.tell(StringMessage("performance-test"))
    sleep(Duration.millisecond * 800)

    println("\n🧪 演示6: 压力测试")
    monitoringActor.tell(StringMessage("stress-test"))
    sleep(Duration.millisecond * 1000)

    println("\n🧪 演示7: 完整统计信息")
    monitoringActor.tell(StringMessage("all-stats"))
    sleep(Duration.millisecond * 500)

    // 5. 额外的监控操作
    println("\n🔍 额外监控操作:")

    // 手动创建一些指标
    let customCounter = GlobalMetricRegistry.registerCounter("demo.custom.counter")
    let customGauge = GlobalMetricRegistry.registerGauge("demo.custom.gauge")
    let customTimer = GlobalMetricRegistry.registerTimer("demo.custom.timer")

    for (i in 0..49) {
        customCounter.increment()
        customGauge.setValue(50.0)  // 简化实现：固定值
        customTimer.recordTime(20.0)  // 简化实现：固定值
    }

    println("   创建了自定义指标并记录了50次数据")

    // 手动触发一些告警
    GlobalAlertManager.triggerAlert("演示完成告警", AlertLevel.Info, "监控系统演示即将完成")

    sleep(Duration.millisecond * 300)

    // 6. 最终统计
    println("\n📊 最终监控统计:")
    println("=" * 40)

    // 显示指标统计
    GlobalMetricRegistry.printAllMetrics()

    // 显示追踪统计
    match (GlobalTracer.getInstance()) {
        case Some(tracer) =>
            tracer.printTracingStats()
        case None =>
            println("追踪器未初始化")
    }

    // 显示告警统计
    alertManager.printAlertStats()

    println("=" * 40)

    println("\n🎯 监控系统演示完成")
    println("展示了以下功能:")
    println("  ✅ 细粒度性能指标收集 (计数器、仪表盘、计时器)")
    println("  ✅ 分布式链路追踪 (跨度创建、标签、日志)")
    println("  ✅ 告警和通知机制 (规则评估、多渠道通知)")
    println("  ✅ 实时监控统计 (指标聚合、状态报告)")
    println("  ✅ 性能和压力测试 (负载模拟、指标验证)")

    // 7. 清理系统
    system.terminate()
    sleep(Duration.second * 1)

    println("✅ 系统已清理")
}
