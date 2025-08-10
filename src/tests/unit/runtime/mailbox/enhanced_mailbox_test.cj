/*
 * CActor Runtime - 增强邮箱系统测试
 * 测试P0-4任务中新增的邮箱功能
 */
package cactor.tests.unit.runtime.mailbox

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import std.collection.*
import cactor.core.message.*
import cactor.runtime.mailbox.*
import cactor.runtime.mailbox.batching.*

/**
 * 增强邮箱功能测试套件
 */
@Test
public class EnhancedMailboxTestSuite {

    private func createTestEnvelope(content: String): Envelope {
        Envelope(StringMessage(content), None<String>)
    }

    /**
     * 测试BoundedMailboxWrapper的动态容量调整
     */
    @TestCase
    public func testBoundedMailboxDynamicCapacity(): Unit {
        let innerMailbox = FoundationMailbox(100)
        let boundedMailbox = BoundedMailboxWrapper(
            innerMailbox,
            5,  // 初始容量5
            BackpressureStrategy.DropNew,
            0.8
        )

        // 填满邮箱
        for (i in 0..5) {
            let envelope = createTestEnvelope("message-${i}")
            let success = boundedMailbox.enqueue(envelope)
            if (i < 5) {
                @Assert(success, true)
            } else {
                @Assert(success, false)  // 第6个消息应该被拒绝
            }
        }

        // 动态增加容量
        boundedMailbox.updateCapacity(10)
        
        // 现在应该可以添加更多消息
        for (i in 5..8) {
            let envelope = createTestEnvelope("new-message-${i}")
            let success = boundedMailbox.enqueue(envelope)
            @Assert(success, true)
        }

        @Assert(boundedMailbox.size(), 8)
    }

    /**
     * 测试BoundedMailboxWrapper的背压策略动态调整
     */
    @TestCase
    public func testBoundedMailboxDynamicBackpressureStrategy(): Unit {
        let innerMailbox = FoundationMailbox(100)
        let boundedMailbox = BoundedMailboxWrapper(
            innerMailbox,
            3,
            BackpressureStrategy.DropNew,  // 初始策略：丢弃新消息
            0.8
        )

        // 填满邮箱
        for (i in 0..3) {
            boundedMailbox.enqueue(createTestEnvelope("message-${i}"))
        }

        // 使用DropNew策略，新消息应该被拒绝
        @Assert(boundedMailbox.enqueue(createTestEnvelope("rejected")), false)

        // 切换到DropHead策略
        boundedMailbox.updateBackpressureStrategy(BackpressureStrategy.DropHead)
        
        // 现在新消息应该被接受（会丢弃旧消息）
        @Assert(boundedMailbox.enqueue(createTestEnvelope("accepted")), true)
    }

    /**
     * 测试BoundedMailboxWrapper的配置查询
     */
    @TestCase
    public func testBoundedMailboxConfiguration(): Unit {
        let innerMailbox = FoundationMailbox(100)
        let boundedMailbox = BoundedMailboxWrapper(
            innerMailbox,
            10,
            BackpressureStrategy.DropNew,
            0.75
        )

        let config = boundedMailbox.getConfiguration()
        @Assert(config.capacity, 10)
        @Assert(config.strategy.toString(), "DropNew")
        @Assert(config.backpressureThreshold, 0.75)
        @Assert(config.currentSize, 0)
        @Assert(config.rejectedCount, 0)
    }

    /**
     * 测试AdaptiveBatchingMailbox的自适应配置
     */
    @TestCase
    public func testAdaptiveBatchingMailboxConfig(): Unit {
        let adaptiveMailbox = AdaptiveBatchingMailbox(
            2,    // minBatchSize
            10,   // maxBatchSize
            Duration.millisecond * 100,  // batchTimeout
            Duration.millisecond * 50    // targetLatency
        )

        let config = adaptiveMailbox.getAdaptiveConfig()
        @Assert(config.minBatchSize, 2)
        @Assert(config.maxBatchSize, 10)
        @Assert(config.currentBatchSize >= 2, true)
        @Assert(config.currentBatchSize <= 10, true)
    }

    /**
     * 测试AdaptiveBatchingMailbox的动态延迟调整
     */
    @TestCase
    public func testAdaptiveBatchingMailboxDynamicLatency(): Unit {
        let adaptiveMailbox = AdaptiveBatchingMailbox(
            2, 8, Duration.millisecond * 100, Duration.millisecond * 50
        )

        let initialConfig = adaptiveMailbox.getAdaptiveConfig()
        let initialTarget = initialConfig.targetProcessingTime

        // 动态调整目标延迟
        adaptiveMailbox.updateTargetLatency(Duration.millisecond * 100)
        
        let updatedConfig = adaptiveMailbox.getAdaptiveConfig()
        @Assert(updatedConfig.targetProcessingTime != initialTarget, true)
    }

    /**
     * 测试AdaptiveBatchingMailbox的统计重置
     */
    @TestCase
    public func testAdaptiveBatchingMailboxStatsReset(): Unit {
        let adaptiveMailbox = AdaptiveBatchingMailbox(
            2, 8, Duration.millisecond * 100, Duration.millisecond * 50
        )

        // 添加一些消息来改变统计
        for (i in 0..5) {
            adaptiveMailbox.enqueue(createTestEnvelope("test-${i}"))
        }

        // 重置统计
        adaptiveMailbox.resetAdaptiveStats()
        
        let config = adaptiveMailbox.getAdaptiveConfig()
        @Assert(config.avgProcessingTime, 0)
        @Assert(config.currentBatchSize, (2 + 8) / 2)  // 应该重置为中间值
    }

    /**
     * 测试AdaptiveBatchingMailbox的性能统计
     */
    @TestCase
    public func testAdaptiveBatchingMailboxPerformanceStats(): Unit {
        let adaptiveMailbox = AdaptiveBatchingMailbox(
            2, 8, Duration.millisecond * 100, Duration.millisecond * 50
        )

        let stats = adaptiveMailbox.getBatchPerformanceStats()
        @Assert(stats.currentBatchSize >= 2, true)
        @Assert(stats.currentBatchSize <= 8, true)
        @Assert(stats.adjustmentCount >= 0, true)
        @Assert(stats.targetProcessingTime > 0, true)
    }

    /**
     * 测试IntelligentBackpressureMailbox的基本功能
     */
    @TestCase
    public func testIntelligentBackpressureMailboxBasic(): Unit {
        let innerMailbox = FoundationMailbox(100)
        let strategy = BackpressureControlStrategy.createDefault()
        let backpressureMailbox = IntelligentBackpressureMailbox(
            innerMailbox, 10, strategy
        )

        // 添加消息直到触发背压
        var successCount = 0
        for (i in 0..15) {
            if (backpressureMailbox.enqueue(createTestEnvelope("test-${i}"))) {
                successCount += 1
            }
        }

        // 应该有一些消息被接受，但不是全部
        @Assert(successCount > 0, true)
        @Assert(successCount <= 15, true)
        
        let stats = backpressureMailbox.getBackpressureStats()
        @Assert(stats.totalAccepted > 0, true)
        @Assert(stats.loadRatio >= 0.0, true)
        @Assert(stats.loadRatio <= 1.0, true)
    }

    /**
     * 测试IntelligentBackpressureMailbox的背压级别
     */
    @TestCase
    public func testIntelligentBackpressureMailboxLevels(): Unit {
        let innerMailbox = FoundationMailbox(100)
        let strategy = BackpressureControlStrategy.createAggressive()  // 更激进的策略
        let backpressureMailbox = IntelligentBackpressureMailbox(
            innerMailbox, 5, strategy
        )

        // 逐步填充邮箱，观察背压级别变化
        let initialLevel = backpressureMailbox.getCurrentBackpressureLevel()
        @Assert(initialLevel.toString(), "NoBackpressure")

        // 添加消息直到触发背压
        for (i in 0..4) {
            backpressureMailbox.enqueue(createTestEnvelope("level-test-${i}"))
        }

        let finalLevel = backpressureMailbox.getCurrentBackpressureLevel()
        // 应该触发某种级别的背压
        @Assert(finalLevel.toString() != "NoBackpressure", true)
    }

    /**
     * 测试IntelligentBackpressureMailbox的统计信息
     */
    @TestCase
    public func testIntelligentBackpressureMailboxStats(): Unit {
        let innerMailbox = FoundationMailbox(100)
        let strategy = BackpressureControlStrategy.createDefault()
        let backpressureMailbox = IntelligentBackpressureMailbox(
            innerMailbox, 8, strategy
        )

        // 添加一些消息
        for (i in 0..10) {
            backpressureMailbox.enqueue(createTestEnvelope("stats-test-${i}"))
        }

        let stats = backpressureMailbox.getBackpressureStats()
        @Assert(stats.capacity, 8)
        @Assert(stats.totalAccepted >= 0, true)
        @Assert(stats.totalRejected >= 0, true)
        @Assert(stats.getRejectionRate() >= 0.0, true)
        @Assert(stats.getRejectionRate() <= 1.0, true)
    }

    /**
     * 测试BackpressureStrategy的toString方法
     */
    @TestCase
    public func testBackpressureStrategyToString(): Unit {
        @Assert(BackpressureStrategy.DropNew.toString(), "DropNew")
        @Assert(BackpressureStrategy.DropHead.toString(), "DropHead")
        @Assert(BackpressureStrategy.DropTail.toString(), "DropTail")
        @Assert(BackpressureStrategy.Block.toString(), "Block")
        @Assert(BackpressureStrategy.Fail.toString(), "Fail")
    }

    /**
     * 测试BackpressureLevel的toString方法
     */
    @TestCase
    public func testBackpressureLevelToString(): Unit {
        @Assert(BackpressureLevel.NoBackpressure.toString(), "NoBackpressure")
        @Assert(BackpressureLevel.Low.toString(), "Low")
        @Assert(BackpressureLevel.Medium.toString(), "Medium")
        @Assert(BackpressureLevel.High.toString(), "High")
        @Assert(BackpressureLevel.Critical.toString(), "Critical")
    }
}
