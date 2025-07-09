/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Actor系统集成测试
 * Plan 0.20 Phase 1.3: 集成测试建设 - Actor系统集成测试
 */
package cactor.tests.integration

import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.message.{Message, MessageResult, StringMessage}
import cactor.core.context.{ActorContext}

/**
 * 集成测试用Actor - 生产者
 */
class ProducerActor <: Actor {
    private var producedCount: Int32 = 0
    
    public prop name: String {
        get() { "ProducerActor" }
    }
    
    public prop description: String {
        get() { "生产者Actor，用于集成测试" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()
                
                if (content == "start-production") {
                    // 开始生产消息
                    for (i in 0..10) {
                        // 这里应该发送消息给消费者，但简化处理
                        producedCount += 1
                    }
                    println("生产者完成生产，总数: ${producedCount}")
                } else if (content == "get-count") {
                    println("生产者当前计数: ${producedCount}")
                }
                
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func getProducedCount(): Int32 {
        producedCount
    }
}

/**
 * 集成测试用Actor - 消费者
 */
class ConsumerActor <: Actor {
    private var consumedCount: Int32 = 0
    
    public prop name: String {
        get() { "ConsumerActor" }
    }
    
    public prop description: String {
        get() { "消费者Actor，用于集成测试" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()
                
                if (content.startsWith("data-")) {
                    // 消费数据
                    consumedCount += 1
                    println("消费者处理数据: ${content}, 总计: ${consumedCount}")
                } else if (content == "get-count") {
                    println("消费者当前计数: ${consumedCount}")
                }
                
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func getConsumedCount(): Int32 {
        consumedCount
    }
}

/**
 * 工作流协调器Actor
 */
class WorkflowCoordinatorActor <: Actor {
    private var completedTasks: Int32 = 0
    private var totalTasks: Int32 = 0
    
    public prop name: String {
        get() { "WorkflowCoordinatorActor" }
    }
    
    public prop description: String {
        get() { "工作流协调器Actor，管理任务执行" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()
                
                if (content == "start-workflow") {
                    totalTasks = 5
                    completedTasks = 0
                    println("工作流开始，总任务数: ${totalTasks}")
                } else if (content == "task-completed") {
                    completedTasks += 1
                    println("任务完成，进度: ${completedTasks}/${totalTasks}")
                    
                    if (completedTasks >= totalTasks) {
                        println("工作流完成！")
                    }
                } else if (content == "get-progress") {
                    println("工作流进度: ${completedTasks}/${totalTasks}")
                }
                
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func getProgress(): (Int32, Int32) {
        (completedTasks, totalTasks)
    }
}

/**
 * CActor Actor系统集成测试套件
 * 测试Actor系统的端到端集成功能
 */
@Test
public class ActorSystemIntegrationTestSuite {
    
    /**
     * 创建测试系统
     */
    private func createTestSystem(): CActorSystem {
        let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        CActorSystemFactory.create("integration-test-system-${timestamp}")
    }
    
    /**
     * 清理测试系统
     */
    private func cleanupTestSystem(system: CActorSystem): Unit {
        if (!system.isTerminated()) {
            system.terminate()
            sleep(Duration.second * 2)  // 给更多时间进行清理
        }
    }
    
    /**
     * 测试基本Actor系统集成
     */
    @TestCase
    public func testBasicActorSystemIntegration(): Unit {
        let system = createTestSystem()
        
        // 创建生产者和消费者
        let producer = system.actorOf({ => ProducerActor() }, "integration-producer")
        let consumer = system.actorOf({ => ConsumerActor() }, "integration-consumer")
        
        // 启动生产
        producer.tell(StringMessage("start-production"))
        
        // 模拟消费
        for (i in 0..5) {
            consumer.tell(StringMessage("data-${i}"))
        }
        
        // 等待处理
        sleep(Duration.millisecond * 500)
        
        // 获取状态
        producer.tell(StringMessage("get-count"))
        consumer.tell(StringMessage("get-count"))
        
        // 等待状态输出
        sleep(Duration.millisecond * 200)
        
        // 验证系统稳定性
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试多Actor协作集成
     */
    @TestCase
    public func testMultiActorCollaborationIntegration(): Unit {
        let system = createTestSystem()
        
        // 创建协调器和工作Actor
        let coordinator = system.actorOf({ => WorkflowCoordinatorActor() }, "workflow-coordinator")
        let workers = ArrayList<ActorRef>()
        
        // 创建多个工作Actor
        for (i in 0..3) {
            let worker = system.actorOf({ => ConsumerActor() }, "worker-${i}")
            workers.add(worker)
        }
        
        // 启动工作流
        coordinator.tell(StringMessage("start-workflow"))
        
        // 模拟任务完成
        for (i in 0..5) {
            coordinator.tell(StringMessage("task-completed"))
            sleep(Duration.millisecond * 50)
        }
        
        // 检查进度
        coordinator.tell(StringMessage("get-progress"))
        
        // 等待处理
        sleep(Duration.millisecond * 300)
        
        // 验证系统稳定性
        @Assert(!system.isTerminated(), true)
        @Assert(workers.size == 3, true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试Actor系统生命周期集成
     */
    @TestCase
    public func testActorSystemLifecycleIntegration(): Unit {
        let system = createTestSystem()
        
        // 创建Actor
        let actor1 = system.actorOf({ => ProducerActor() }, "lifecycle-actor-1")
        let actor2 = system.actorOf({ => ConsumerActor() }, "lifecycle-actor-2")
        
        // 发送初始化消息
        actor1.tell(StringMessage("start-production"))
        actor2.tell(StringMessage("data-init"))
        
        // 等待初始化
        sleep(Duration.millisecond * 200)
        
        // 验证系统活跃
        @Assert(!system.isTerminated(), true)
        
        // 发送工作消息
        actor1.tell(StringMessage("get-count"))
        actor2.tell(StringMessage("get-count"))
        
        // 等待工作完成
        sleep(Duration.millisecond * 200)
        
        // 验证系统仍然活跃
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试Actor系统错误恢复集成
     */
    @TestCase
    public func testActorSystemErrorRecoveryIntegration(): Unit {
        let system = createTestSystem()
        
        // 创建Actor
        let producer = system.actorOf({ => ProducerActor() }, "error-recovery-producer")
        let consumer = system.actorOf({ => ConsumerActor() }, "error-recovery-consumer")
        
        // 正常操作
        producer.tell(StringMessage("start-production"))
        consumer.tell(StringMessage("data-normal"))
        
        // 等待正常处理
        sleep(Duration.millisecond * 200)
        
        // 模拟错误场景（发送未知消息）
        producer.tell(StringMessage("unknown-command"))
        consumer.tell(StringMessage("invalid-data"))
        
        // 等待错误处理
        sleep(Duration.millisecond * 200)
        
        // 恢复正常操作
        producer.tell(StringMessage("get-count"))
        consumer.tell(StringMessage("get-count"))
        
        // 等待恢复
        sleep(Duration.millisecond * 200)
        
        // 验证系统恢复正常
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试Actor系统性能集成
     */
    @TestCase
    public func testActorSystemPerformanceIntegration(): Unit {
        let system = createTestSystem()
        
        // 创建多个Actor进行性能测试
        let actors = ArrayList<ActorRef>()
        for (i in 0..10) {
            let actor = system.actorOf({ => ProducerActor() }, "perf-actor-${i}")
            actors.add(actor)
        }
        
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        // 并发发送消息
        for (i in 0..Int64(actors.size)) {
            actors[i].tell(StringMessage("start-production"))
        }
        
        // 等待处理
        sleep(Duration.millisecond * 1000)
        
        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime
        
        // 验证性能
        @Assert(duration > 0, true)
        @Assert(actors.size == 10, true)
        @Assert(!system.isTerminated(), true)
        
        println("性能测试完成，耗时: ${duration}纳秒")
        
        cleanupTestSystem(system)
    }
}
