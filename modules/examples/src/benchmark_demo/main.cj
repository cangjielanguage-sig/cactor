/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - Benchmark Demo
 * 性能基准测试示例
 */
package cactor_examples.benchmark_demo

import std.time.*
import cactor.api.{CActor}
import cactor_core.message.{Message, StringMessage, MessageResult}
import cactor_core.actor.{Actor}
import cactor_core.context.{ActorContext}

/**
 * 简单的测试Actor
 */
class BenchmarkActor <: Actor {
    // 实现Actor接口的必需属性
    public prop name: String {
        get() { "BenchmarkActor" }
    }

    public prop description: String {
        get() { "基准测试Actor" }
    }

    private var messageCount: Int64 = 0
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                messageCount += 1
                if (messageCount % 1000 == 0) {
                    println("Processed " + messageCount.toString() + " messages")
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
    
    public func preStart(): Unit {
        println("BenchmarkActor started")
    }
    
    public func postStop(): Unit {
        println("BenchmarkActor stopped, processed " + messageCount.toString() + " messages")
    }
}

/**
 * 性能基准测试主函数
 */
main(): Int64 {
    println("🚀 CActor Benchmark Demo")
    println("========================")
    
    // 创建Actor系统
    let system = CActor.system("BenchmarkSystem")
    println("✅ Actor系统创建成功")
    
    // 创建测试Actor
    let actor = system.actorOf({ => BenchmarkActor() }, "benchmark-actor")
    println("✅ 测试Actor创建成功")
    
    // 性能测试参数
    let messageCount = 10000
    let batchSize = 1000
    
    println("📊 开始性能测试...")
    println("消息总数: " + messageCount.toString())
    println("批次大小: " + batchSize.toString())
    
    let startTime = getCurrentTimeNanos()
    
    // 发送测试消息
    for (i in 0..messageCount) {
        let message = StringMessage("Benchmark message #" + i.toString())
        actor.tell(message)
        
        // 每批次后短暂暂停，避免过载
        if (i % batchSize == 0 && i > 0) {
            // 简单的延迟
            let pauseStart = getCurrentTimeNanos()
            while (getCurrentTimeNanos() - pauseStart < 1000000) {
                // 1ms暂停
            }
        }
    }
    
    let endTime = getCurrentTimeNanos()
    let totalTime = endTime - startTime
    let avgTime = totalTime / messageCount
    let throughput = (messageCount * 1000000000) / totalTime
    
    println("📈 性能测试结果:")
    println("总时间: " + totalTime.toString() + " ns")
    println("平均时间: " + avgTime.toString() + " ns/msg")
    println("吞吐量: " + throughput.toString() + " msg/s")
    
    // 等待消息处理完成
    println("⏳ 等待消息处理完成...")
    let waitStart = getCurrentTimeNanos()
    while (getCurrentTimeNanos() - waitStart < 1000000000) {
        // 等待1秒
    }
    
    // 关闭系统
    println("🔄 关闭Actor系统...")
    system.terminate()  // 使用正确的终止方法
    
    println("✅ Benchmark Demo 完成!")
    return 0
}

/**
 * 获取当前时间纳秒
 * 简化实现，实际应该使用系统时间API
 */
func getCurrentTimeNanos(): Int64 {
    let now = DateTime.now()
    now.toUnixTimeStamp().toMilliseconds() * 1000000
}
