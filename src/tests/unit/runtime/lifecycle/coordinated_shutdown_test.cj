package cactor.tests.unit.runtime.lifecycle

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import std.collection.*
import cactor.runtime.lifecycle.*

/**
 * CoordinatedShutdown 增强功能测试套件
 */
@Test
public class CoordinatedShutdownTest {

    /**
     * 测试基本的关停流程
     */
    @TestCase
    public func testBasicShutdown(): Unit {
        let shutdown = CoordinatedShutdown.get("test-system-1")
        shutdown.reset() // 重置状态

        let taskTracker = TaskTracker()
        shutdown.registerTask("service-stop", TestShutdownTask("basic-task", { => taskTracker.markExecuted() }))

        @Assert(shutdown.getStatus().toString(), "NotStarted")
        @Assert(shutdown.isShutdownInProgress(), false)
        @Assert(shutdown.isShutdownCompleted(), false)

        shutdown.run()

        @Assert(taskTracker.isExecuted(), true)
        @Assert(shutdown.getStatus().toString(), "Completed")
        @Assert(shutdown.isShutdownCompleted(), true)
    }

    /**
     * 测试关停原因跟踪
     */
    @TestCase
    public func testShutdownReason(): Unit {
        let shutdown = CoordinatedShutdown.get("test-system-2")
        shutdown.reset()
        
        @Assert(shutdown.getReason().isSome(), false)
        
        shutdown.run(ShutdownReason.ClusterLeaving)
        
        match (shutdown.getReason()) {
            case Some(reason) => @Assert(reason.toString(), "ClusterLeaving")
            case None => @Assert(false, true) // 不应该到这里
        }
    }

    /**
     * 测试增强任务接口
     */
    @TestCase
    public func testEnhancedTask(): Unit {
        let shutdown = CoordinatedShutdown.get("test-system-3")
        shutdown.reset()

        let taskTracker = TaskTracker()
        let enhancedTask = TestShutdownTask("enhanced-task", { => taskTracker.setValue("executed") })

        shutdown.registerTask("service-stop", enhancedTask)
        shutdown.run()

        @Assert(taskTracker.getLastValue(), "executed")
        @Assert(shutdown.getStatus().toString(), "Completed")
    }

    /**
     * 测试阶段配置
     */
    @TestCase
    public func testPhaseConfig(): Unit {
        let shutdown = CoordinatedShutdown.get("test-system-4")
        shutdown.reset()

        let customPhase = PhaseConfig.createWithTimeout("custom-phase", Duration.second * 5)
        shutdown.addPhase(customPhase)

        let taskTracker = TaskTracker()
        shutdown.registerTask("custom-phase", TestShutdownTask("custom-task", { => taskTracker.markExecuted() }))

        shutdown.run()

        @Assert(taskTracker.isExecuted(), true)
    }

    /**
     * 测试关停钩子函数
     */
    @TestCase
    public func testShutdownHooks(): Unit {
        let shutdown = CoordinatedShutdown.get("test-system-5")
        shutdown.reset()

        let hookTracker = HookTracker()

        shutdown.addShutdownHook({ phase, status =>
            hookTracker.recordCall(phase, status)
        })

        shutdown.registerTask("service-stop", TestShutdownTask("hook-test-task", { => () }))
        shutdown.run()

        @Assert(hookTracker.isCalled(), true)
        // 钩子会被多次调用，最后一次应该是完成状态
        @Assert(hookTracker.getLastStatus(), "Completed")
    }

    /**
     * 测试错误处理
     */
    @TestCase
    public func testErrorHandling(): Unit {
        let shutdown = CoordinatedShutdown.get("test-system-6")
        shutdown.reset()

        // 注册一个会抛出异常的任务
        shutdown.registerTask("service-stop", TestShutdownTask("error-task", { => throw Exception("测试异常") }))

        // 注册一个正常任务
        let taskTracker = TaskTracker()
        shutdown.registerTask("service-stop", TestShutdownTask("normal-task", { => taskTracker.markExecuted() }))

        shutdown.run()

        // 即使有任务失败，其他任务仍应执行，整体关停应该完成
        @Assert(taskTracker.isExecuted(), true)
        @Assert(shutdown.getStatus().toString(), "Completed")
    }

    /**
     * 测试统计信息
     */
    @TestCase
    public func testStats(): Unit {
        let shutdown = CoordinatedShutdown.get("test-system-7")
        shutdown.reset()

        shutdown.registerTask("service-stop", TestShutdownTask("stats-task", { => () }))
        shutdown.addShutdownHook({ phase, status => () })

        let statsBefore = shutdown.getStats()
        @Assert(statsBefore.contains("未开始"), true)

        shutdown.run()

        let statsAfter = shutdown.getStats()
        @Assert(statsAfter.contains("Completed"), true)
        @Assert(statsAfter.contains("钩子数: 1"), true)
    }

    /**
     * 测试重复关停保护
     */
    @TestCase
    public func testDuplicateShutdownProtection(): Unit {
        let shutdown = CoordinatedShutdown.get("test-system-8")
        shutdown.reset()

        let taskTracker = TaskTracker()
        shutdown.registerTask("service-stop", TestShutdownTask("duplicate-test-task", { => taskTracker.markExecuted() }))

        // 第一次关停
        shutdown.run()
        @Assert(taskTracker.getExecutionCount(), 1)
        @Assert(shutdown.getStatus().toString(), "Completed")

        // 第二次关停应该被忽略
        shutdown.run()
        @Assert(taskTracker.getExecutionCount(), 1) // 任务不应该再次执行
    }
}

/**
 * 任务执行跟踪器，用于避免lambda捕获可变变量的问题
 */
class TaskTracker {
    private var executed: Bool = false
    private var executionCount: Int32 = 0
    private var lastValue: String = ""

    public func markExecuted(): Unit {
        executed = true
        executionCount += 1
    }

    public func setValue(value: String): Unit {
        lastValue = value
        markExecuted()
    }

    public func isExecuted(): Bool { executed }
    public func getExecutionCount(): Int32 { executionCount }
    public func getLastValue(): String { lastValue }

    public func reset(): Unit {
        executed = false
        executionCount = 0
        lastValue = ""
    }
}

/**
 * 钩子跟踪器
 */
class HookTracker {
    private var called: Bool = false
    private var lastPhase: String = ""
    private var lastStatus: String = ""

    public func recordCall(phase: String, status: ShutdownStatus): Unit {
        called = true
        lastPhase = phase
        lastStatus = status.toString()
    }

    public func isCalled(): Bool { called }
    public func getLastPhase(): String { lastPhase }
    public func getLastStatus(): String { lastStatus }
}

/**
 * 测试用的关停任务实现
 */
class TestShutdownTask <: ShutdownTask {
    private let name: String
    private let task: () -> Unit

    public init(name: String, task: () -> Unit) {
        this.name = name
        this.task = task
    }

    public func execute(): String {
        task()
        name
    }

    public func getTimeout(): Duration {
        Duration.second * 10
    }
}
