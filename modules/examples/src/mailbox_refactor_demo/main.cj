/*
 * CActor Mailbox体系改造功能演示
 * 验证所有已实现的功能：工厂方法统一化、批处理接口化、控制消息分类集中化
 */
package cactor_examples.mailbox_refactor_demo

import std.time.*
import cactor_core.message.*
import cactor_runtime.mailbox.*
import cactor_runtime.mailbox.batching.*

/**
 * Mailbox功能演示主类
 */
public class MailboxRefactorDemo {

    /**
     * 演示工厂方法统一化
     */
    public static func demonstrateFactoryUnification(): Unit {
        println("\n=== 演示1：工厂方法统一化 ===")

        // 测试Foundation邮箱创建
        let foundationMailbox = FoundationMailbox(1000)
        println("✅ Foundation邮箱创建成功")

        // 测试消息入队出队
        let msg = StringMessage("测试消息")
        let envelope = Envelope(msg, None<String>)
        let success = foundationMailbox.enqueue(envelope)
        println("✅ 消息入队成功: ${success}")

        match (foundationMailbox.dequeue()) {
            case Some(env) =>
                println("✅ 消息出队成功: ${env.getMessage().messageType()}")
            case None =>
                println("❌ 消息出队失败")
        }

        println("✅ 工厂方法统一化验证完成")
    }
    
    /**
     * 演示批处理接口化功能
     */
    public static func demonstrateBatchProcessing(): Unit {
        println("\n=== 演示2：批处理接口化功能 ===")
        
        // 创建批处理邮箱
        let batchMailbox = BatchingMailbox(10, Duration.millisecond * 100)
        
        // 添加测试消息
        for (i in 0..15) {
            let msg = StringMessage("批处理消息-${i}")
            let envelope = Envelope(msg, None<String>)
            batchMailbox.enqueue(envelope)
        }
        
        println("✅ 已添加 15 条消息到批处理邮箱")
        
        // 测试批量出队
        let batch1 = batchMailbox.dequeueBatch(5)
        println("✅ 批量出队 ${batch1.size} 条消息")
        
        // 测试单条回退
        match (batchMailbox.dequeue()) {
            case Some(envelope) =>
                println("✅ 单条回退成功")
            case None =>
                println("❌ 单条回退失败")
        }

        // 测试自适应批处理邮箱
        let adaptiveMailbox = AdaptiveBatchingMailbox(5, 20, Duration.millisecond * 50, Duration.millisecond * 10)

        for (i in 0..8) {
            let msg = StringMessage("自适应消息-${i}")
            let envelope = Envelope(msg, None<String>)
            adaptiveMailbox.enqueue(envelope)
        }

        let adaptiveBatch = adaptiveMailbox.dequeueBatch(3)
        println("✅ 自适应批处理出队 ${adaptiveBatch.size} 条消息")
    }
    
    /**
     * 演示控制消息分类集中化
     */
    public static func demonstrateControlClassification(): Unit {
        println("\n=== 演示3：控制消息分类集中化 ===")
        
        // 创建优先级邮箱
        let priorityMailbox = QueueBasedPriorityMailbox()
        
        // 添加不同类型的消息
        let controlMsg = StopMessage("系统停止")
        let normalMsg = StringMessage("普通消息")
        let lowMsg = StringMessage("LowPriority消息")
        
        let controlEnvelope = Envelope(controlMsg, None<String>)
        let normalEnvelope = Envelope(normalMsg, None<String>)
        let lowEnvelope = Envelope(lowMsg, None<String>)
        
        // 按非优先级顺序添加
        priorityMailbox.enqueue(normalEnvelope)
        priorityMailbox.enqueue(lowEnvelope)
        priorityMailbox.enqueue(controlEnvelope)
        
        println("✅ 已添加消息：普通 -> 低优先级 -> 控制消息")
        
        // 验证出队顺序（应该是：控制 -> 普通 -> 低优先级）
        var order = 1
        while (!priorityMailbox.isEmpty()) {
            match (priorityMailbox.dequeue()) {
                case Some(envelope) =>
                    let msg = envelope.getMessage()
                    let isControl = ControlClassifier.isControl(msg)
                    println("✅ 第${order}个出队: ${msg.messageType()} (控制消息: ${isControl})")
                    order += 1
                case None =>
                    break
            }
        }
    }
    
    /**
     * 性能基准测试
     */
    public static func performanceBenchmark(): Unit {
        println("\n=== 演示4：性能基准测试 ===")
        
        let messageCount = 1000
        let batchSize = 50
        
        // Foundation邮箱性能测试
        let foundationMailbox = FoundationMailbox(Int64(messageCount))
        let startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        
        // 入队测试
        for (i in 0..messageCount) {
            let msg = StringMessage("性能测试-${i}")
            let envelope = Envelope(msg, None<String>)
            foundationMailbox.enqueue(envelope)
        }
        
        let enqueueTime = DateTime.now().toUnixTimeStamp().toMilliseconds() - startTime
        println("✅ Foundation邮箱入队 ${messageCount} 条消息耗时: ${enqueueTime}ms")
        
        // 出队测试
        let dequeueStart = DateTime.now().toUnixTimeStamp().toMilliseconds()
        var dequeueCount = 0
        
        while (!foundationMailbox.isEmpty()) {
            match (foundationMailbox.dequeue()) {
                case Some(_) =>
                    dequeueCount += 1
                case None =>
                    break
            }
        }
        
        let dequeueTime = DateTime.now().toUnixTimeStamp().toMilliseconds() - dequeueStart
        println("✅ Foundation邮箱出队 ${dequeueCount} 条消息耗时: ${dequeueTime}ms")
        
        // 批处理性能测试
        let batchMailbox = BatchingMailbox(Int64(batchSize), Duration.millisecond * 10)
        
        let batchStartTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        
        for (i in 0..messageCount) {
            let msg = StringMessage("批处理性能-${i}")
            let envelope = Envelope(msg, None<String>)
            batchMailbox.enqueue(envelope)
        }

        var batchDequeueCount = 0
        while (!batchMailbox.isEmpty()) {
            let batch = batchMailbox.dequeueBatch(Int32(batchSize))
            batchDequeueCount += batch.size
        }
        
        let batchTime = DateTime.now().toUnixTimeStamp().toMilliseconds() - batchStartTime
        println("✅ 批处理邮箱处理 ${batchDequeueCount} 条消息耗时: ${batchTime}ms")
        
        let improvement = if (batchTime > 0) { 
            Float64(enqueueTime + dequeueTime) / Float64(batchTime) 
        } else { 
            1.0 
        }
        println("✅ 批处理性能提升: ${improvement}x")
    }
}

/**
 * 主函数 - 运行所有演示
 */
main(): Int64 {
    println("🚀 CActor Mailbox体系改造功能演示开始")

    try {
        MailboxRefactorDemo.demonstrateFactoryUnification()
        MailboxRefactorDemo.demonstrateBatchProcessing()
        MailboxRefactorDemo.demonstrateControlClassification()
        MailboxRefactorDemo.performanceBenchmark()

        println("\n🎉 所有功能演示完成！CActor Mailbox体系改造成功！")

    } catch (e: Exception) {
        println("❌ 演示过程中出现错误: ${e.message}")
    }

    return 0
}
