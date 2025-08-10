package cactor.tests.unit.core.supervision

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import std.collection.*
import cactor.core.supervision.*
import cactor.core.actor.{ActorRef, ActorPath, ActorLifecycleState}
import cactor.core.message.{Message}

/**
 * 监督策略矩阵增强功能测试套件
 */
@Test
public class SupervisionStrategyMatrixTest {

    /**
     * 测试基本的策略矩阵功能
     */
    @TestCase
    public func testBasicStrategyMatrix(): Unit {
        let matrix = SupervisionStrategyMatrix(SupervisionDirective.Restart)
            .addRule("NullPointerException", SupervisionDirective.Resume)
            .addRule("IllegalArgumentException", SupervisionDirective.Stop)
        
        // 测试精确匹配
        let nullException = Exception("NullPointerException")
        @Assert(matrix.decide(nullException).toString(), "Resume")
        
        let illegalException = Exception("IllegalArgumentException")
        @Assert(matrix.decide(illegalException).toString(), "Stop")
        
        // 测试默认行为
        let unknownException = Exception("UnknownException")
        @Assert(matrix.decide(unknownException).toString(), "Restart")
    }

    /**
     * 测试通配符规则
     */
    @TestCase
    public func testWildcardRules(): Unit {
        let matrix = SupervisionStrategyMatrix(SupervisionDirective.Restart)
            .addWildcardRule("Timeout", SupervisionDirective.Resume)
            .addWildcardRule("OutOfMemory", SupervisionDirective.Stop)
            .addWildcardRule("Connection", SupervisionDirective.Restart)
        
        // 测试通配符匹配
        let timeoutException = Exception("ConnectionTimeoutException")
        @Assert(matrix.decide(timeoutException).toString(), "Resume")
        
        let memoryException = Exception("OutOfMemoryError")
        @Assert(matrix.decide(memoryException).toString(), "Stop")
        
        let connectionException = Exception("DatabaseConnectionException")
        @Assert(matrix.decide(connectionException).toString(), "Restart")
    }

    /**
     * 测试策略模板
     */
    @TestCase
    public func testStrategyTemplates(): Unit {
        // 测试严格模板
        let strictMatrix = SupervisionStrategyTemplates.createStrictMatrix()
        let nullException = Exception("NullPointerException")
        @Assert(strictMatrix.decide(nullException).toString(), "Restart")
        
        let memoryException = Exception("OutOfMemoryError")
        @Assert(strictMatrix.decide(memoryException).toString(), "Stop")
        
        // 测试宽松模板
        let lenientMatrix = SupervisionStrategyTemplates.createLenientMatrix()
        let generalException = Exception("GeneralException")
        @Assert(lenientMatrix.decide(generalException).toString(), "Restart")
        
        let fatalException = Exception("FatalSystemError")
        @Assert(fatalException.toString().contains("Fatal"), true)
    }

    /**
     * 测试网络应用模板
     */
    @TestCase
    public func testNetworkTemplate(): Unit {
        let networkMatrix = SupervisionStrategyTemplates.createNetworkMatrix()
        
        let connectionException = Exception("ConnectionRefusedException")
        @Assert(networkMatrix.decide(connectionException).toString(), "Restart")
        
        let timeoutException = Exception("SocketTimeoutException")
        @Assert(networkMatrix.decide(timeoutException).toString(), "Restart")
        
        let certificateException = Exception("CertificateException")
        @Assert(networkMatrix.decide(certificateException).toString(), "Stop")
    }

    /**
     * 测试数据库应用模板
     */
    @TestCase
    public func testDatabaseTemplate(): Unit {
        let dbMatrix = SupervisionStrategyTemplates.createDatabaseMatrix()
        
        let sqlException = Exception("SQLException")
        @Assert(dbMatrix.decide(sqlException).toString(), "Restart")
        
        let deadlockException = Exception("DeadlockException")
        @Assert(dbMatrix.decide(deadlockException).toString(), "Restart")
        
        let constraintException = Exception("ConstraintViolationException")
        @Assert(dbMatrix.decide(constraintException).toString(), "Stop")
        
        let permissionException = Exception("PermissionDeniedException")
        @Assert(dbMatrix.decide(permissionException).toString(), "Escalate")
    }

    /**
     * 测试基于矩阵的监督策略
     */
    @TestCase
    public func testMatrixBasedStrategy(): Unit {
        let matrix = SupervisionStrategyMatrix(SupervisionDirective.Restart)
            .addRule("TestException", SupervisionDirective.Resume)
        
        let strategy = DefaultSupervisionStrategy.oneForOneWithMatrix(3, Duration.minute, matrix)
        
        // 创建测试用的ActorFailure
        let actorRef = createTestActorRef()
        let failure = ActorFailure(actorRef, Exception("TestException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        
        let directive = strategy.decide(failure)
        @Assert(directive.toString(), "Resume")
    }

    /**
     * 测试策略组合器
     */
    @TestCase
    public func testStrategyComposer(): Unit {
        let strategy1 = DefaultSupervisionStrategy.stoppingStrategy()
        let strategy2 = DefaultSupervisionStrategy.oneForOne()
        let fallbackStrategy = DefaultSupervisionStrategy.resumingStrategy()
        
        let compositeStrategy = SupervisionStrategyComposer(fallbackStrategy)
            .addStrategy(strategy1)
            .addStrategy(strategy2)
            .compose()
        
        // 创建测试用的ActorFailure
        let actorRef = createTestActorRef()
        let failure = ActorFailure(actorRef, Exception("TestException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        
        let directive = compositeStrategy.decide(failure)
        // 第一个策略是停止策略，应该返回Stop
        @Assert(directive.toString(), "Stop")
    }

    /**
     * 测试动态监督策略
     */
    @TestCase
    public func testDynamicStrategy(): Unit {
        let initialStrategy = DefaultSupervisionStrategy.oneForOne()
        let dynamicStrategy = DynamicSupervisionStrategy(initialStrategy)
        
        // 测试初始策略
        let actorRef = createTestActorRef()
        let failure = ActorFailure(actorRef, Exception("TestException"), DateTime.now().toUnixTimeStamp().toMilliseconds())
        
        let directive1 = dynamicStrategy.decide(failure)
        @Assert(directive1.toString(), "Restart")
        
        // 切换策略
        let newStrategy = DefaultSupervisionStrategy.stoppingStrategy()
        dynamicStrategy.switchStrategy(newStrategy, "SwitchToStoppingStrategy")
        
        let directive2 = dynamicStrategy.decide(failure)
        @Assert(directive2.toString(), "Stop")
        
        // 检查历史记录
        let history = dynamicStrategy.getStrategyHistory()
        @Assert(history.size >= 2, true)
    }

    /**
     * 测试矩阵描述功能
     */
    @TestCase
    public func testMatrixDescription(): Unit {
        let matrix = SupervisionStrategyMatrix(SupervisionDirective.Restart)
            .addRule("TestException", SupervisionDirective.Resume)
            .addWildcardRule("Timeout", SupervisionDirective.Stop)
        
        let description = matrix.getDescription()
        @Assert(description.contains("SupervisionStrategyMatrix"), true)
        @Assert(description.contains("default=Restart"), true)
        @Assert(description.contains("exact_rules=1"), true)
        @Assert(description.contains("wildcard_rules=1"), true)
    }

    /**
     * 创建测试用的ActorRef
     */
    private func createTestActorRef(): ActorRef {
        TestActorRef("test-actor")
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
