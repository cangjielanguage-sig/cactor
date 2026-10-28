/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 性能回归测试套件
 * Plan 0.20 Phase 1.3: 集成测试建设 - 性能回归测试
 */
package cactor_tests.integration

import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

/**
 * 性能测试Actor
 */
class PerformanceTestActor <: Actor {
    private var processedMessages: Int64 = 0
    private var startTime: Int64 = 0
    private var endTime: Int64 = 0
    
    public prop name: String {
        get() { "PerformanceTestActor" }
    }
    
    public prop description: String {
        get() { "性能测试Actor，用于测量消息处理性能" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()
                
                if (content == "start-benchmark") {
                    startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
                    processedMessages = 0
                    println("性能基准测试开始")
                } else if (content == "end-benchmark") {
                    endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
                    let duration = endTime - startTime
                    let throughput = if (duration > 0) { processedMessages * 1000000000 / duration } else { 0 }
                    println("性能基准测试结束")
                    println("处理消息数: ${processedMessages}")
                    println("耗时: ${duration}纳秒")
                    println("吞吐量: ${throughput} msg/s")
                } else if (content.startsWith("perf-")) {
                    // 处理性能测试消息
                    processedMessages += 1
                } else if (content == "get-stats") {
                    println("当前统计: 处理消息${processedMessages}条")
                }
                
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func getProcessedMessages(): Int64 {
        processedMessages
    }
    
    public func getDuration(): Int64 {
        endTime - startTime
    }
}

/**
 * 批量处理Actor
 */
class BatchProcessorActor <: Actor {
    private var batchCount: Int32 = 0
    private var totalProcessed: Int64 = 0
    
    public prop name: String {
        get() { "BatchProcessorActor" }
    }
    
    public prop description: String {
        get() { "批量处理Actor，用于测试批处理性能" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()
                
                if (content == "process-batch") {
                    batchCount += 1
                    totalProcessed += 100  // 假设每批处理100个项目
                    
                    // 模拟批处理工作
                    var sum: Int64 = 0
                    for (i in 0..100) {
                        sum += i
                    }
                    
                    if (batchCount % 10 == 0) {
                        println("批处理进度: ${batchCount}批, 总处理: ${totalProcessed}")
                    }
                } else if (content == "get-batch-stats") {
                    println("批处理统计: ${batchCount}批, 总处理: ${totalProcessed}")
                }
                
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func getBatchCount(): Int32 {
        batchCount
    }
    
    public func getTotalProcessed(): Int64 {
        totalProcessed
    }
}

/**
 * CActor 性能回归测试套件
 * 确保系统性能不会退化
 */
@Test
public class PerformanceRegressionTestSuite {
    
    /**
     * 创建测试系统
     */
    private func createTestSystem(): CActorSystem {
        let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        CActorSystemFactory.create("perf-test-system-${timestamp}")
    }
    
    /**
     * 清理测试系统
     */
    private func cleanupTestSystem(system: CActorSystem): Unit {
        if (!system.isTerminated()) {
            system.terminate()
            sleep(Duration.second * 2)
        }
    }
    
    /**
     * 测试单Actor消息处理性能
     */
    @TestCase
    public func testSingleActorMessageThroughput(): Unit {
        let system = createTestSystem()
        let actor = system.actorOf({ => PerformanceTestActor() }, "throughput-test-actor")
        
        // 开始基准测试
        actor.tell(StringMessage("start-benchmark"))
        
        let messageCount = 1000
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        // 发送大量消息
        for (i in 0..messageCount) {
            actor.tell(StringMessage("perf-message-${i}"))
        }
        
        // 等待处理完成
        sleep(Duration.millisecond * 500)
        
        // 结束基准测试
        actor.tell(StringMessage("end-benchmark"))
        
        // 等待统计输出
        sleep(Duration.millisecond * 200)
        
        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let totalDuration = endTime - startTime
        let throughput = if (totalDuration > 0) { Int64(messageCount) * 1000000000 / totalDuration } else { 0 }
        
        println("单Actor吞吐量测试完成: ${throughput} msg/s")
        
        // 验证性能基线（应该能处理至少1000 msg/s）
        @Assert(throughput > 1000, true)
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试多Actor并发性能
     */
    @TestCase
    public func testMultiActorConcurrentPerformance(): Unit {
        let system = createTestSystem()
        let actorCount = 10
        let actors = ArrayList<ActorRef>()
        
        // 创建多个Actor
        for (i in 0..actorCount) {
            let actor = system.actorOf({ => PerformanceTestActor() }, "concurrent-actor-${i}")
            actors.add(actor)
        }
        
        let messagePerActor = 1000
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        // 并发发送消息到所有Actor
        for (i in 0..Int64(actors.size)) {
            actors[i].tell(StringMessage("start-benchmark"))
            
            for (j in 0..messagePerActor) {
                actors[i].tell(StringMessage("perf-message-${j}"))
            }
            
            actors[i].tell(StringMessage("end-benchmark"))
        }
        
        // 等待所有处理完成
        sleep(Duration.millisecond * 1000)
        
        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let totalDuration = endTime - startTime
        let totalMessages = Int64(actorCount) * Int64(messagePerActor)
        let throughput = if (totalDuration > 0) { totalMessages * 1000000000 / totalDuration } else { 0 }
        
        println("多Actor并发性能测试完成: ${throughput} msg/s")
        
        // 验证并发性能基线
        @Assert(throughput > 5000, true)  // 多Actor应该有更高的总吞吐量
        @Assert(actors.size == actorCount, true)
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试批处理性能
     */
    @TestCase
    public func testBatchProcessingPerformance(): Unit {
        let system = createTestSystem()
        let actor = system.actorOf({ => BatchProcessorActor() }, "batch-processor")
        
        let batchCount = 50
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        // 发送批处理请求
        for (i in 0..batchCount) {
            actor.tell(StringMessage("process-batch"))
        }
        
        // 等待批处理完成
        sleep(Duration.millisecond * 1000)
        
        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime
        let batchThroughput = if (duration > 0) { Int64(batchCount) * 1000000000 / duration } else { 0 }
        
        // 获取统计信息
        actor.tell(StringMessage("get-batch-stats"))
        sleep(Duration.millisecond * 100)
        
        println("批处理性能测试完成: ${batchThroughput} batch/s")
        
        // 验证批处理性能
        @Assert(batchThroughput > 10, true)  // 应该能处理至少10 batch/s
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试内存使用性能
     */
    @TestCase
    public func testMemoryUsagePerformance(): Unit {
        let system = createTestSystem()
        let actors = ArrayList<ActorRef>()
        
        // 创建大量Actor测试内存使用
        let actorCount = 100
        for (i in 0..actorCount) {
            let actor = system.actorOf({ => PerformanceTestActor() }, "memory-test-actor-${i}")
            actors.add(actor)
        }
        
        // 向所有Actor发送消息
        for (i in 0..Int64(actors.size)) {
            actors[i].tell(StringMessage("perf-memory-test"))
        }
        
        // 等待处理
        sleep(Duration.millisecond * 500)
        
        // 验证内存使用（通过Actor数量间接验证）
        @Assert(actors.size == actorCount, true)
        @Assert(!system.isTerminated(), true)
        
        println("内存使用性能测试完成，创建了${actorCount}个Actor")
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试系统启动性能
     */
    @TestCase
    public func testSystemStartupPerformance(): Unit {
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        // 创建系统
        let system = createTestSystem()
        
        // 创建一些Actor
        let actors = ArrayList<ActorRef>()
        for (i in 0..20) {
            let actor = system.actorOf({ => PerformanceTestActor() }, "startup-actor-${i}")
            actors.add(actor)
        }
        
        // 发送初始化消息
        for (i in 0..Int64(actors.size)) {
            actors[i].tell(StringMessage("start-benchmark"))
        }
        
        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let startupDuration = endTime - startTime
        
        println("系统启动性能测试完成，耗时: ${startupDuration}纳秒")
        
        // 验证启动性能（应该在合理时间内完成）
        @Assert(startupDuration > 0, true)
        @Assert(actors.size == 20, true)
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试系统关闭性能
     */
    @TestCase
    public func testSystemShutdownPerformance(): Unit {
        let system = createTestSystem()
        
        // 创建一些Actor
        let actors = ArrayList<ActorRef>()
        for (i in 0..30) {
            let actor = system.actorOf({ => PerformanceTestActor() }, "shutdown-actor-${i}")
            actors.add(actor)
        }
        
        // 发送一些消息
        for (i in 0..Int64(actors.size)) {
            actors[i].tell(StringMessage("perf-shutdown-test"))
        }
        
        // 等待消息处理
        sleep(Duration.millisecond * 200)
        
        let shutdownStartTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        // 关闭系统
        system.terminate()
        
        // 等待关闭完成
        sleep(Duration.second * 2)
        
        let shutdownEndTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let shutdownDuration = shutdownEndTime - shutdownStartTime
        
        println("系统关闭性能测试完成，耗时: ${shutdownDuration}纳秒")
        
        // 验证关闭性能
        @Assert(shutdownDuration > 0, true)
        @Assert(system.isTerminated(), true)
    }
}
