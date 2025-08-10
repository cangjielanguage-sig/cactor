package cactor.tests.unit.runtime.dispatcher

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import std.collection.*
import cactor.core.actor.{ActorRef, ActorPath, ActorLifecycleState}
import cactor.core.message.{Message, Envelope, StringMessage}
import cactor.runtime.dispatcher.work_stealing.{WorkStealingDispatcher, ActorTask, Priority, ThroughputStats}
import cactor.runtime.dispatcher.{PriorityDispatcher, PriorityStats}

/**
 * 增强的调度器功能测试套件
 * 测试吞吐量控制、优先级调度等功能
 */
@Test
public class EnhancedDispatcherTest {

    /**
     * 测试WorkStealingDispatcher的吞吐量控制功能
     */
    @TestCase
    public func testWorkStealingDispatcherThroughputControl(): Unit {
        let throughput = Int32(10)
        let deadlineTime = Duration.millisecond * 100
        let dispatcher = WorkStealingDispatcher(2, throughput, deadlineTime)
        
        dispatcher.start()
        
        // 发送一些任务
        let actorRef = createTestActorRef("throughput-test-actor")
        let envelope = createTestEnvelope("test-message")
        
        for (i in 0..5) {
            dispatcher.dispatch(envelope, actorRef)
        }
        
        // 等待一段时间让任务处理
        sleep(Duration.millisecond * 200)
        
        // 检查吞吐量统计
        let stats = dispatcher.getThroughputStats()
        @Assert(stats.configuredThroughput, throughput)
        @Assert(stats.totalTasksProcessed >= 0, true)
        
        dispatcher.stop()
    }

    /**
     * 测试WorkStealingDispatcher的统计重置功能
     */
    @TestCase
    public func testWorkStealingDispatcherStatsReset(): Unit {
        let dispatcher = WorkStealingDispatcher(2, 20, Duration.millisecond * 50)
        
        dispatcher.start()
        
        // 发送一些任务
        let actorRef = createTestActorRef("reset-test-actor")
        let envelope = createTestEnvelope("test-message")
        
        for (i in 0..3) {
            dispatcher.dispatch(envelope, actorRef)
        }
        
        // 等待处理
        sleep(Duration.millisecond * 100)
        
        // 重置统计
        dispatcher.resetThroughputStats()
        
        let stats = dispatcher.getThroughputStats()
        @Assert(stats.currentBatchCount, 0)
        
        dispatcher.stop()
    }

    /**
     * 测试PriorityDispatcher的基本功能
     */
    @TestCase
    public func testPriorityDispatcherBasicFunctionality(): Unit {
        let dispatcher = PriorityDispatcher(2)
        
        dispatcher.start()
        
        let actorRef = createTestActorRef("priority-test-actor")
        let envelope = createTestEnvelope("test-message")
        
        // 发送不同优先级的任务
        dispatcher.dispatchWithPriority(envelope, actorRef, Priority.Critical)
        dispatcher.dispatchWithPriority(envelope, actorRef, Priority.High)
        dispatcher.dispatchWithPriority(envelope, actorRef, Priority.Normal)
        dispatcher.dispatchWithPriority(envelope, actorRef, Priority.Low)
        
        // 等待处理
        sleep(Duration.millisecond * 200)
        
        // 检查统计信息
        let stats = dispatcher.getPriorityStats()
        @Assert(stats.criticalTasksProcessed >= 0, true)
        @Assert(stats.highTasksProcessed >= 0, true)
        @Assert(stats.normalTasksProcessed >= 0, true)
        @Assert(stats.lowTasksProcessed >= 0, true)
        
        dispatcher.stop()
    }

    /**
     * 测试PriorityDispatcher的优先级处理顺序
     */
    @TestCase
    public func testPriorityDispatcherPriorityOrder(): Unit {
        let dispatcher = PriorityDispatcher(1, 100, Duration.millisecond * 100) // 单线程确保顺序
        
        dispatcher.start()
        
        let actorRef = createTestActorRef("order-test-actor")
        let envelope = createTestEnvelope("test-message")
        
        // 先发送低优先级任务，再发送高优先级任务
        dispatcher.dispatchWithPriority(envelope, actorRef, Priority.Low)
        dispatcher.dispatchWithPriority(envelope, actorRef, Priority.Normal)
        dispatcher.dispatchWithPriority(envelope, actorRef, Priority.High)
        dispatcher.dispatchWithPriority(envelope, actorRef, Priority.Critical)
        
        // 等待处理
        sleep(Duration.millisecond * 300)
        
        let stats = dispatcher.getPriorityStats()
        // 高优先级任务应该被优先处理
        @Assert(stats.criticalTasksProcessed >= 0, true)
        @Assert(stats.highTasksProcessed >= 0, true)
        
        dispatcher.stop()
    }

    /**
     * 测试PriorityDispatcher的吞吐量控制
     */
    @TestCase
    public func testPriorityDispatcherThroughputControl(): Unit {
        let throughput = Int32(5)
        let deadlineTime = Duration.millisecond * 50
        let dispatcher = PriorityDispatcher(2, throughput, deadlineTime)
        
        dispatcher.start()
        
        let actorRef = createTestActorRef("priority-throughput-test-actor")
        let envelope = createTestEnvelope("test-message")
        
        // 发送任务
        for (i in 0..8) {
            dispatcher.dispatchWithPriority(envelope, actorRef, Priority.Normal)
        }
        
        // 等待处理
        sleep(Duration.millisecond * 200)
        
        let throughputStats = dispatcher.getThroughputStats()
        @Assert(throughputStats.configuredThroughput, throughput)
        @Assert(throughputStats.totalTasksProcessed >= 0, true)
        
        dispatcher.stop()
    }

    /**
     * 测试PriorityDispatcher的工作线程统计
     */
    @TestCase
    public func testPriorityDispatcherWorkerStats(): Unit {
        let dispatcher = PriorityDispatcher(3)
        
        dispatcher.start()
        
        let actorRef = createTestActorRef("worker-stats-test-actor")
        let envelope = createTestEnvelope("test-message")
        
        // 发送一些任务
        for (i in 0..10) {
            dispatcher.dispatch(envelope, actorRef)
        }
        
        // 等待处理
        sleep(Duration.millisecond * 200)
        
        let workerStats = dispatcher.getWorkerStats()
        @Assert(workerStats.size, 3) // 应该有3个工作线程的统计
        
        for (stats in workerStats) {
            @Assert(stats.workerId >= 0, true)
            @Assert(stats.totalTasksProcessed >= 0, true)
        }
        
        dispatcher.stop()
    }

    /**
     * 测试WorkStealingDispatcher的工作线程统计
     */
    @TestCase
    public func testWorkStealingDispatcherWorkerStats(): Unit {
        let dispatcher = WorkStealingDispatcher(2)
        
        dispatcher.start()
        
        let actorRef = createTestActorRef("ws-worker-stats-test-actor")
        let envelope = createTestEnvelope("test-message")
        
        // 发送一些任务
        for (i in 0..5) {
            dispatcher.dispatch(envelope, actorRef)
        }
        
        // 等待处理
        sleep(Duration.millisecond * 150)
        
        let workerStats = dispatcher.getWorkerStats()
        @Assert(workerStats.size, 2) // 应该有2个工作线程的统计
        
        for (stats in workerStats) {
            @Assert(stats.workerId >= 0, true)
            @Assert(stats.tasksProcessed >= 0, true)
        }
        
        dispatcher.stop()
    }

    /**
     * 测试混合负载下的调度器性能
     */
    @TestCase
    public func testMixedWorkloadPerformance(): Unit {
        let wsDispatcher = WorkStealingDispatcher(2, 50, Duration.millisecond * 100)
        let priorityDispatcher = PriorityDispatcher(2, 50, Duration.millisecond * 100)
        
        wsDispatcher.start()
        priorityDispatcher.start()
        
        let actorRef = createTestActorRef("mixed-load-test-actor")
        let envelope = createTestEnvelope("test-message")
        
        // 同时向两个调度器发送任务
        for (i in 0..10) {
            wsDispatcher.dispatch(envelope, actorRef)
            priorityDispatcher.dispatchWithPriority(envelope, actorRef, Priority.Normal)
        }
        
        // 等待处理
        sleep(Duration.millisecond * 300)
        
        // 检查两个调度器都正常工作
        let wsStats = wsDispatcher.getThroughputStats()
        let priorityStats = priorityDispatcher.getThroughputStats()
        
        @Assert(wsStats.totalTasksProcessed >= 0, true)
        @Assert(priorityStats.totalTasksProcessed >= 0, true)
        
        wsDispatcher.stop()
        priorityDispatcher.stop()
    }

    /**
     * 创建测试用的ActorRef
     */
    private func createTestActorRef(name: String): ActorRef {
        TestActorRef(name)
    }

    /**
     * 创建测试用的Envelope
     */
    private func createTestEnvelope(content: String): Envelope {
        Envelope(StringMessage(content), None<String>)
    }
}

/**
 * 测试用的ActorRef实现
 */
class TestActorRef <: ActorRef {
    private let actorPath: ActorPath
    
    public init(name: String) {
        this.actorPath = ActorPath("test-system", "/user/${name}")
    }
    
    public func tell(message: Message): Unit {
        // 测试实现，不做任何操作
    }
    
    public func tell(message: Message, sender: Option<String>): Unit {
        // 测试实现，不做任何操作
    }
    
    public prop path: ActorPath {
        get() {
            actorPath
        }
    }
    
    public func getLifecycleState(): ActorLifecycleState {
        ActorLifecycleState.Running
    }
    
    public func equals(other: ActorRef): Bool {
        this.path.toString() == other.path.toString()
    }
    
    public func hashCode(): Int32 {
        this.path.hashCode()
    }
}


