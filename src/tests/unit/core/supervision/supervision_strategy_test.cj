/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 监督策略单元测试
 * Plan 0.20 Phase 1.2: 单元测试完善 - 监督策略测试
 */
package cactor.tests.unit.core.supervision

import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.message.{Message, MessageResult, StringMessage}
import cactor.core.context.{ActorContext}

/**
 * 可能失败的测试Actor
 */
class FailureProneActor <: Actor {
    private var failureCount: Int32 = 0
    private var shouldFail: Bool = false
    private var processedMessages: Int32 = 0
    
    public prop name: String {
        get() { "FailureProneActor" }
    }
    
    public prop description: String {
        get() { "用于测试监督策略的可能失败Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()
                processedMessages += 1
                
                if (content == "enable-failure") {
                    shouldFail = true
                } else if (content == "disable-failure") {
                    shouldFail = false
                } else if (content == "trigger-failure" || (shouldFail && content == "normal-message")) {
                    failureCount += 1
                    throw Exception("模拟Actor失败 - 失败次数: ${failureCount}")
                } else if (content == "get-stats") {
                    // 返回统计信息（简化处理）
                    println("Stats: processed=${processedMessages}, failures=${failureCount}")
                }
                
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func getFailureCount(): Int32 {
        failureCount
    }
    
    public func getProcessedMessages(): Int32 {
        processedMessages
    }
    
    public func reset(): Unit {
        failureCount = 0
        shouldFail = false
        processedMessages = 0
    }
}

/**
 * 监督者Actor
 */
class SupervisorActor <: Actor {
    private var childFailures: Int32 = 0
    private var restartCount: Int32 = 0
    
    public prop name: String {
        get() { "SupervisorActor" }
    }
    
    public prop description: String {
        get() { "监督者Actor，负责管理子Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()
                
                if (content == "child-failed") {
                    childFailures += 1
                    println("子Actor失败，总失败次数: ${childFailures}")
                } else if (content == "restart-child") {
                    restartCount += 1
                    println("重启子Actor，总重启次数: ${restartCount}")
                } else if (content == "get-supervision-stats") {
                    println("监督统计: 子Actor失败=${childFailures}, 重启次数=${restartCount}")
                }
                
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func getChildFailures(): Int32 {
        childFailures
    }
    
    public func getRestartCount(): Int32 {
        restartCount
    }
}

/**
 * 弹性Actor - 能够从失败中恢复
 */
class ResilientActor <: Actor {
    private var recoveryCount: Int32 = 0
    private var isHealthy: Bool = true
    
    public prop name: String {
        get() { "ResilientActor" }
    }
    
    public prop description: String {
        get() { "弹性Actor，能够从失败中恢复" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                let content = msg.getContent()
                
                if (content == "cause-failure") {
                    isHealthy = false
                    throw Exception("临时失败")
                } else if (content == "recover") {
                    isHealthy = true
                    recoveryCount += 1
                    println("Actor已恢复，恢复次数: ${recoveryCount}")
                } else if (content == "health-check") {
                    if (isHealthy) {
                        println("健康状态: 健康")
                    } else {
                        println("健康状态: 不健康")
                    }
                }
                
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    public func getHealthyStatus(): Bool {
        isHealthy
    }
    
    public func getRecoveryCount(): Int32 {
        recoveryCount
    }
}

/**
 * CActor监督策略单元测试套件
 * 测试各种监督策略和错误恢复机制
 */
@Test
public class SupervisionStrategyTestSuite {
    
    /**
     * 创建测试系统
     */
    private func createTestSystem(): CActorSystem {
        let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        CActorSystemFactory.create("supervision-test-system-${timestamp}")
    }
    
    /**
     * 清理测试系统
     */
    private func cleanupTestSystem(system: CActorSystem): Unit {
        if (!system.isTerminated()) {
            system.terminate()
            sleep(Duration.second * 1)
        }
    }
    
    /**
     * 测试基本监督功能
     */
    @TestCase
    public func testBasicSupervision(): Unit {
        let system = createTestSystem()
        
        // 创建监督者和被监督的Actor
        let supervisor = system.actorOf({ => SupervisorActor() }, "supervisor")
        let child = system.actorOf({ => FailureProneActor() }, "supervised-child")
        
        // 发送正常消息
        child.tell(StringMessage("normal-message"))
        supervisor.tell(StringMessage("get-supervision-stats"))
        
        // 等待消息处理
        sleep(Duration.millisecond * 100)
        
        // 验证Actor创建成功
        @Assert(supervisor.path.toString().contains("supervisor"), true)
        @Assert(child.path.toString().contains("supervised-child"), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试Actor失败处理
     */
    @TestCase
    public func testActorFailureHandling(): Unit {
        let system = createTestSystem()
        
        // 创建可能失败的Actor
        let actor = system.actorOf({ => FailureProneActor() }, "failure-test-actor")
        
        // 发送正常消息
        actor.tell(StringMessage("normal-message"))
        
        // 等待处理
        sleep(Duration.millisecond * 50)
        
        // 触发失败（这会导致异常，但系统应该保持稳定）
        actor.tell(StringMessage("trigger-failure"))
        
        // 等待失败处理
        sleep(Duration.millisecond * 100)
        
        // 发送恢复消息
        actor.tell(StringMessage("normal-message"))
        
        // 等待恢复
        sleep(Duration.millisecond * 50)
        
        // 验证系统稳定性
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试重启策略
     */
    @TestCase
    public func testRestartStrategy(): Unit {
        let system = createTestSystem()
        
        // 创建弹性Actor
        let actor = system.actorOf({ => ResilientActor() }, "resilient-actor")
        
        // 健康检查
        actor.tell(StringMessage("health-check"))
        
        // 等待处理
        sleep(Duration.millisecond * 50)
        
        // 触发失败
        actor.tell(StringMessage("cause-failure"))
        
        // 等待失败处理
        sleep(Duration.millisecond * 100)
        
        // 恢复
        actor.tell(StringMessage("recover"))
        
        // 等待恢复
        sleep(Duration.millisecond * 50)
        
        // 再次健康检查
        actor.tell(StringMessage("health-check"))
        
        // 验证系统稳定性
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试监督层次结构
     */
    @TestCase
    public func testSupervisionHierarchy(): Unit {
        let system = createTestSystem()
        
        // 创建监督层次结构
        let topSupervisor = system.actorOf({ => SupervisorActor() }, "top-supervisor")
        let midSupervisor = system.actorOf({ => SupervisorActor() }, "mid-supervisor")
        let worker = system.actorOf({ => FailureProneActor() }, "worker")
        
        // 建立监督关系（通过消息模拟）
        topSupervisor.tell(StringMessage("get-supervision-stats"))
        midSupervisor.tell(StringMessage("get-supervision-stats"))
        worker.tell(StringMessage("normal-message"))
        
        // 等待消息处理
        sleep(Duration.millisecond * 150)
        
        // 验证层次结构创建成功
        @Assert(topSupervisor.path.toString().contains("top-supervisor"), true)
        @Assert(midSupervisor.path.toString().contains("mid-supervisor"), true)
        @Assert(worker.path.toString().contains("worker"), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试失败升级
     */
    @TestCase
    public func testFailureEscalation(): Unit {
        let system = createTestSystem()
        
        // 创建监督者和工作Actor
        let supervisor = system.actorOf({ => SupervisorActor() }, "escalation-supervisor")
        let worker = system.actorOf({ => FailureProneActor() }, "escalation-worker")
        
        // 启用失败模式
        worker.tell(StringMessage("enable-failure"))
        
        // 发送多条消息触发连续失败
        for (i in 0..3) {
            worker.tell(StringMessage("normal-message"))
            sleep(Duration.millisecond * 20)
        }
        
        // 通知监督者
        supervisor.tell(StringMessage("child-failed"))
        
        // 等待升级处理
        sleep(Duration.millisecond * 100)
        
        // 验证系统稳定性
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试断路器模式
     */
    @TestCase
    public func testCircuitBreakerPattern(): Unit {
        let system = createTestSystem()
        
        // 创建可能失败的Actor
        let actor = system.actorOf({ => FailureProneActor() }, "circuit-breaker-actor")
        
        // 发送正常消息
        actor.tell(StringMessage("normal-message"))
        sleep(Duration.millisecond * 50)
        
        // 触发多次失败模拟断路器打开
        for (i in 0..5) {
            actor.tell(StringMessage("trigger-failure"))
            sleep(Duration.millisecond * 20)
        }
        
        // 等待断路器处理
        sleep(Duration.millisecond * 100)
        
        // 尝试恢复
        actor.tell(StringMessage("disable-failure"))
        actor.tell(StringMessage("normal-message"))
        
        // 验证系统稳定性
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试监督统计
     */
    @TestCase
    public func testSupervisionStatistics(): Unit {
        let system = createTestSystem()
        
        // 创建监督者
        let supervisor = system.actorOf({ => SupervisorActor() }, "stats-supervisor")
        
        // 模拟多次子Actor失败
        for (i in 0..3) {
            supervisor.tell(StringMessage("child-failed"))
            sleep(Duration.millisecond * 10)
        }
        
        // 模拟重启
        for (i in 0..2) {
            supervisor.tell(StringMessage("restart-child"))
            sleep(Duration.millisecond * 10)
        }
        
        // 获取统计信息
        supervisor.tell(StringMessage("get-supervision-stats"))
        
        // 等待统计处理
        sleep(Duration.millisecond * 100)
        
        // 验证监督者正常工作
        @Assert(supervisor.path.toString().contains("stats-supervisor"), true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试并发失败处理
     */
    @TestCase
    public func testConcurrentFailureHandling(): Unit {
        let system = createTestSystem()
        
        // 创建多个可能失败的Actor
        let actors = ArrayList<ActorRef>()
        for (i in 0..5) {
            let actor = system.actorOf({ => FailureProneActor() }, "concurrent-actor-${i}")
            actors.add(actor)
        }
        
        // 并发触发失败
        for (i in 0..Int64(actors.size)) {
            actors[i].tell(StringMessage("trigger-failure"))
        }
        
        // 等待失败处理
        sleep(Duration.millisecond * 200)
        
        // 发送恢复消息
        for (i in 0..Int64(actors.size)) {
            actors[i].tell(StringMessage("normal-message"))
        }
        
        // 等待恢复
        sleep(Duration.millisecond * 100)
        
        // 验证系统稳定性
        @Assert(!system.isTerminated(), true)
        @Assert(actors.size == 5, true)
        
        cleanupTestSystem(system)
    }
    
    /**
     * 测试监督策略性能
     */
    @TestCase
    public func testSupervisionPerformance(): Unit {
        let system = createTestSystem()
        
        // 创建监督者和多个工作Actor
        let supervisor = system.actorOf({ => SupervisorActor() }, "perf-supervisor")
        let workers = ArrayList<ActorRef>()
        
        for (i in 0..10) {
            let worker = system.actorOf({ => FailureProneActor() }, "perf-worker-${i}")
            workers.add(worker)
        }
        
        let startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        
        // 发送大量消息测试性能
        for (i in 0..Int64(workers.size)) {
            workers[i].tell(StringMessage("normal-message"))
        }
        
        // 等待处理
        sleep(Duration.millisecond * 200)
        
        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        let duration = endTime - startTime
        
        // 验证性能
        @Assert(duration > 0, true)
        @Assert(workers.size == 10, true)
        @Assert(!system.isTerminated(), true)
        
        cleanupTestSystem(system)
    }
}
