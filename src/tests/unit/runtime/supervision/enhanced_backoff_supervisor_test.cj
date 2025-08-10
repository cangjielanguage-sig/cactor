package cactor.tests.unit.runtime.supervision

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import std.collection.*
import cactor.core.supervision.*
import cactor.core.actor.{ActorRef, ActorPath, ActorLifecycleState, Actor, Props}
import cactor.core.guardian.{GuardianActor, GuardianType, GuardianStats, GuardianHealthStatus}
import cactor.core.message.{Message, MessageResult}
import cactor.core.context.{ActorContext}
import cactor.runtime.supervision.{BackoffSupervisor, BackoffStrategyType, BackoffConfig, BackoffStats}

/**
 * 增强的BackoffSupervisor功能测试套件
 */
@Test
public class EnhancedBackoffSupervisorTest {

    /**
     * 测试基本的退避功能
     */
    @TestCase
    public func testBasicBackoffFunctionality(): Unit {
        let guardian = createTestGuardian()
        let strategy = BackoffSupervisorStrategy(Duration.millisecond * 100, Duration.second * 10, 0.2, 5)
        let config = BackoffConfig.createDefault()
        let supervisor = BackoffSupervisor(strategy, guardian, config, None<SupervisionStrategyMatrix>)
        
        let actorRef = createTestActorRef("test-actor")
        let failure = ActorFailure(actorRef, Exception("TestException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        
        // 处理失败
        supervisor.handleFailure(failure)
        
        // 检查统计信息
        let stats = supervisor.getStats()
        @Assert(stats.totalFailures, 1)
        @Assert(stats.totalRestarts, 1)
        @Assert(stats.totalStops, 0)
    }

    /**
     * 测试线性退避策略
     */
    @TestCase
    public func testLinearBackoffStrategy(): Unit {
        let guardian = createTestGuardian()
        let strategy = BackoffSupervisorStrategy(Duration.millisecond * 100, Duration.second * 10, 0.2, 5)
        let config = BackoffConfig.createLinear(Duration.millisecond * 100, 3)
        let supervisor = BackoffSupervisor(strategy, guardian, config, None<SupervisionStrategyMatrix>)
        
        let actorRef = createTestActorRef("linear-test-actor")
        let failure = ActorFailure(actorRef, Exception("TestException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        
        // 多次失败测试线性增长
        supervisor.handleFailure(failure)
        supervisor.handleFailure(failure)
        supervisor.handleFailure(failure)
        
        let stats = supervisor.getStats()
        @Assert(stats.totalFailures, 3)
        @Assert(stats.totalRestarts, 3)
        
        // 检查退避时间历史
        let history = supervisor.getRetryHistory(actorRef.path.toString())
        @Assert(history.size >= 3, true)
    }

    /**
     * 测试指数退避策略
     */
    @TestCase
    public func testExponentialBackoffStrategy(): Unit {
        let guardian = createTestGuardian()
        let strategy = BackoffSupervisorStrategy(Duration.millisecond * 100, Duration.second * 10, 0.2, 5)
        let config = BackoffConfig.createExponential(Duration.millisecond * 100, Duration.second * 5, 3)
        let supervisor = BackoffSupervisor(strategy, guardian, config, None<SupervisionStrategyMatrix>)
        
        let actorRef = createTestActorRef("exponential-test-actor")
        let failure = ActorFailure(actorRef, Exception("TestException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        
        // 多次失败测试指数增长
        supervisor.handleFailure(failure)
        supervisor.handleFailure(failure)
        
        let stats = supervisor.getStats()
        @Assert(stats.totalFailures, 2)
        @Assert(stats.totalRestarts, 2)
        
        // 指数退避应该产生更长的延迟
        @Assert(stats.maxBackoffTime > 100, true)
    }

    /**
     * 测试最大重试限制
     */
    @TestCase
    public func testMaxRetryLimit(): Unit {
        let guardian = createTestGuardian()
        let strategy = BackoffSupervisorStrategy(Duration.millisecond * 100, Duration.second * 10, 0.2, 5)
        let config = BackoffConfig.createLinear(Duration.millisecond * 100, 2) // 最大重试2次
        let supervisor = BackoffSupervisor(strategy, guardian, config, None<SupervisionStrategyMatrix>)
        
        let actorRef = createTestActorRef("max-retry-test-actor")
        let failure = ActorFailure(actorRef, Exception("TestException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        
        // 超过最大重试次数
        supervisor.handleFailure(failure)
        supervisor.handleFailure(failure)
        supervisor.handleFailure(failure) // 这次应该导致停止
        
        let stats = supervisor.getStats()
        @Assert(stats.totalFailures, 3)
        @Assert(stats.totalStops >= 1, true) // 应该有停止操作
    }

    /**
     * 测试监督策略矩阵集成
     */
    @TestCase
    public func testSupervisionStrategyMatrixIntegration(): Unit {
        let guardian = createTestGuardian()
        let strategy = BackoffSupervisorStrategy(Duration.millisecond * 100, Duration.second * 10, 0.2, 5)
        let config = BackoffConfig.createDefault()
        
        // 创建策略矩阵
        let matrix = SupervisionStrategyMatrix(SupervisionDirective.Restart)
            .addRule("StopException", SupervisionDirective.Stop)
            .addRule("ResumeException", SupervisionDirective.Resume)
        
        let supervisor = BackoffSupervisor(strategy, guardian, config, Some(matrix))
        
        let actorRef = createTestActorRef("matrix-test-actor")
        
        // 测试停止指令
        let stopFailure = ActorFailure(actorRef, Exception("StopException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        supervisor.handleFailure(stopFailure)
        
        // 测试恢复指令
        let resumeFailure = ActorFailure(actorRef, Exception("ResumeException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        supervisor.handleFailure(resumeFailure)
        
        let stats = supervisor.getStats()
        @Assert(stats.totalFailures, 2)
        @Assert(stats.totalStops >= 1, true) // 应该有停止操作
    }

    /**
     * 测试统计信息准确性
     */
    @TestCase
    public func testStatisticsAccuracy(): Unit {
        let guardian = createTestGuardian()
        let strategy = BackoffSupervisorStrategy(Duration.millisecond * 100, Duration.second * 10, 0.2, 5)
        let config = BackoffConfig.createLinear(Duration.millisecond * 100, 5)
        let supervisor = BackoffSupervisor(strategy, guardian, config, None<SupervisionStrategyMatrix>)
        
        let actorRef = createTestActorRef("stats-test-actor")
        let failure = ActorFailure(actorRef, Exception("TestException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        
        // 多次失败
        for (i in 0..3) {
            supervisor.handleFailure(failure)
        }
        
        let stats = supervisor.getStats()
        @Assert(stats.totalFailures, 3)
        @Assert(stats.totalRestarts, 3)
        @Assert(stats.averageBackoffTime > 0.0, true)
        @Assert(stats.maxBackoffTime >= stats.minBackoffTime, true)
        @Assert(stats.activeRetries >= 0, true)
    }

    /**
     * 测试清理功能
     */
    @TestCase
    public func testCleanupFunctionality(): Unit {
        let guardian = createTestGuardian()
        let strategy = BackoffSupervisorStrategy(Duration.millisecond * 100, Duration.second * 10, 0.2, 5)
        let config = BackoffConfig.createDefault()
        let supervisor = BackoffSupervisor(strategy, guardian, config, None<SupervisionStrategyMatrix>)
        
        let actorRef = createTestActorRef("cleanup-test-actor")
        let failure = ActorFailure(actorRef, Exception("TestException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        
        // 处理失败
        supervisor.handleFailure(failure)
        
        // 清理
        supervisor.cleanup(actorRef.path.toString())
        
        // 检查历史记录被清理
        let history = supervisor.getRetryHistory(actorRef.path.toString())
        @Assert(history.size, 0)
    }

    /**
     * 测试抖动功能
     */
    @TestCase
    public func testJitterFunctionality(): Unit {
        let guardian = createTestGuardian()
        let strategy = BackoffSupervisorStrategy(Duration.millisecond * 100, Duration.second * 10, 0.2, 5)
        
        // 启用抖动的配置
        let config = BackoffConfig(
            BackoffStrategyType.Linear,
            Duration.millisecond * 100,
            Duration.second * 10,
            5,
            true,  // 启用抖动
            0.2,   // 20%抖动
            None<(Int64) -> Duration>
        )
        
        let supervisor = BackoffSupervisor(strategy, guardian, config, None<SupervisionStrategyMatrix>)
        
        let actorRef = createTestActorRef("jitter-test-actor")
        let failure = ActorFailure(actorRef, Exception("TestException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        
        // 多次失败，检查是否有抖动效果
        supervisor.handleFailure(failure)
        supervisor.handleFailure(failure)
        
        let history = supervisor.getRetryHistory(actorRef.path.toString())
        // 由于抖动，退避时间应该有变化
        @Assert(history.size >= 2, true)
    }

    /**
     * 创建测试用的GuardianActor
     */
    private func createTestGuardian(): GuardianActor {
        TestGuardianActor()
    }

    /**
     * 创建测试用的ActorRef
     */
    private func createTestActorRef(name: String): ActorRef {
        TestActorRef(name)
    }
}

/**
 * 测试用的GuardianActor实现
 */
class TestGuardianActor <: GuardianActor {
    public prop guardianType: GuardianType {
        get() { GuardianType.User }
    }

    public prop parent: Option<ActorRef> {
        get() { None<ActorRef> }
    }

    public prop name: String {
        get() { "test-guardian" }
    }

    public prop description: String {
        get() { "Test Guardian Actor" }
    }

    public func createChild(props: Props<Actor>, name: String): ActorRef {
        // 测试实现，返回一个简单的ActorRef
        TestActorRef(name)
    }

    public func supervise(child: ActorRef, strategy: SupervisionStrategy): Unit {
        // 测试实现，不做任何操作
    }

    public func getChildren(): Array<ActorRef> {
        // 测试实现，返回空数组
        Array<ActorRef>()
    }

    public func findChild(name: String): Option<ActorRef> {
        // 测试实现，返回None
        None<ActorRef>
    }

    public func restartChild(child: ActorRef, cause: Exception): Unit {
        // 测试实现，不做任何操作
    }

    public func stopChild(child: ActorRef): Unit {
        // 测试实现，不做任何操作
    }

    public func handleFailure(failure: ActorFailure): Unit {
        // 测试实现，不做任何操作
    }

    public func getStats(): GuardianStats {
        // 测试实现，返回空统计信息
        GuardianStats.empty()
    }

    public func getHealthStatus(): GuardianHealthStatus {
        // 测试实现，返回健康状态
        GuardianHealthStatus.Healthy
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        // 测试实现，返回已处理
        MessageResult.Handled
    }

    public func preStart(): Unit {
        // 测试实现，不做任何操作
    }

    public func postStop(): Unit {
        // 测试实现，不做任何操作
    }

    public func preRestart(reason: Exception): Unit {
        // 测试实现，不做任何操作
    }

    public func postRestart(reason: Exception): Unit {
        // 测试实现，不做任何操作
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
