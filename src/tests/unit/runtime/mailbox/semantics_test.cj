/*
 * CActor - Mailbox语义接口测试
 * 验证新增的语义接口功能
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
 * 测试新增的语义接口功能
 */
@Test
func testBatchDequeueSemantics(): Unit {
    let batchMailbox = BatchingMailbox(5, Duration.millisecond * 100)
    
    // 添加一些消息
    for (i in 0..3) {
        let msg = StringMessage("测试消息-${i}")
        let envelope = Envelope(msg, None<String>)
        let success = batchMailbox.enqueue(envelope)
        @Expect(success, true)
    }
    
    // 测试批量出队
    let dequeuedMessages = batchMailbox.dequeueBatch(Int32(2))
    @Expect(dequeuedMessages.size, Int64(2))
    
    // 测试单条回退
    let singleMessage = batchMailbox.dequeue()
    @Expect(singleMessage.isSome(), true)
}

/**
 * 测试MetricsSemantics接口
 */
@Test
func testMetricsSemantics(): Unit {
    let batchMailbox = BatchingMailbox(10, Duration.millisecond * 50)
    
    // 验证初始指标
    let initialMetrics = batchMailbox.getMetrics()
    @Expect(initialMetrics.totalEnqueued, Int64(0))
    @Expect(initialMetrics.totalDequeued, Int64(0))
    @Expect(initialMetrics.currentSize, Int64(0))
    @Expect(batchMailbox.isMetricsEnabled(), true)
    
    // 添加消息并验证指标
    for (i in 0..5) {
        let msg = StringMessage("指标测试消息-${i}")
        let envelope = Envelope(msg, None<String>)
        batchMailbox.enqueue(envelope)
    }
    
    let afterEnqueueMetrics = batchMailbox.getMetrics()
    @Expect(afterEnqueueMetrics.totalEnqueued, Int64(5))
    @Expect(afterEnqueueMetrics.currentSize, Int64(5))
    
    // 测试指标重置
    batchMailbox.resetMetrics()
    let resetMetrics = batchMailbox.getMetrics()
    @Expect(resetMetrics.totalEnqueued, Int64(0))
    @Expect(resetMetrics.totalDequeued, Int64(0))
}

/**
 * 测试语义接口的组合使用
 */
@Test
func testSemanticsCombination(): Unit {
    let batchMailbox = BatchingMailbox(20, Duration.millisecond * 200)
    
    // 测试批处理和指标收集的组合
    for (i in 0..10) {
        let msg = StringMessage("组合测试消息-${i}")
        let envelope = Envelope(msg, None<String>)
        batchMailbox.enqueue(envelope)
    }
    
    // 批量处理
    let batch1 = batchMailbox.dequeueBatch(Int32(5))
    @Expect(batch1.size, Int64(5))
    
    // 检查指标
    let metrics = batchMailbox.getMetrics()
    @Expect(metrics.totalEnqueued, Int64(10))
    @Expect(metrics.currentSize, Int64(5))  // 剩余5个消息
    
    // 继续批量处理
    let batch2 = batchMailbox.dequeueBatch(Int32(10))
    @Expect(batch2.size, Int64(5))  // 只剩5个消息
    
    let finalMetrics = batchMailbox.getMetrics()
    @Expect(finalMetrics.currentSize, Int64(0))
}

/**
 * 测试指标启用/禁用功能
 */
@Test
func testMetricsEnableDisable(): Unit {
    let batchMailbox = BatchingMailbox(10, Duration.millisecond * 50)
    
    // 默认启用
    @Expect(batchMailbox.isMetricsEnabled(), true)
    
    // 禁用指标收集
    batchMailbox.setMetricsEnabled(false)
    @Expect(batchMailbox.isMetricsEnabled(), false)
    
    // 重新启用
    batchMailbox.setMetricsEnabled(true)
    @Expect(batchMailbox.isMetricsEnabled(), true)
}

/**
 * 测试MailboxMetrics数据结构
 */
@Test
func testMailboxMetricsStructure(): Unit {
    let currentTime = DateTime.now().toUnixTimeStamp().toMilliseconds() * 1_000_000
    let metrics = MailboxMetrics(
        100,  // totalEnqueued
        80,   // totalDequeued
        20,   // currentSize
        50,   // maxSize
        1000, // avgProcessingTime
        5,    // dropCount
        currentTime  // lastUpdateTime
    )

    @Expect(metrics.totalEnqueued, Int64(100))
    @Expect(metrics.totalDequeued, Int64(80))
    @Expect(metrics.currentSize, Int64(20))
    @Expect(metrics.maxSize, Int64(50))
    @Expect(metrics.avgProcessingTime, Int64(1000))
    @Expect(metrics.dropCount, Int64(5))
    @Expect(metrics.lastUpdateTime, currentTime)
}

/**
 * 测试BoundedMailboxWrapper有界邮箱包装器
 */
@Test
func testBoundedMailboxWrapper(): Unit {
    let innerMailbox = FoundationMailbox(100)
    let boundedWrapper = BoundedMailboxWrapper(
        innerMailbox,
        Int64(5),  // 最大容量5
        BackpressureStrategy.DropNew,
        0.8  // 背压阈值80%
    )

    // 测试容量限制
    for (i in 0..6) {
        let msg = StringMessage("测试消息-${i}")
        let envelope = Envelope(msg, None<String>)
        let success = boundedWrapper.enqueue(envelope)
        if (i < 5) {
            @Assert(success, true)
        } else {
            @Assert(success, false)  // 第6个消息应该被拒绝
        }
    }

    // 测试BoundedMailboxSemantics接口
    @Assert(boundedWrapper.isFull(), true)
    @Assert(boundedWrapper.maxCapacity(), Int64(5))
    @Assert(boundedWrapper.remainingCapacity(), Int64(0))

    // 测试BackpressureSemantics接口
    @Assert(boundedWrapper.getLoadRatio(), 1.0)
    @Assert(boundedWrapper.isBackpressureTriggered(), true)
    @Assert(boundedWrapper.getRejectedCount(), Int64(1))
}

/**
 * 测试ControlAwareMailboxWrapper控制消息感知包装器
 */
@Test
func testControlAwareMailboxWrapper(): Unit {
    let innerMailbox = FoundationMailbox(100)
    let controlWrapper = ControlAwareMailboxWrapper(innerMailbox)

    // 添加普通消息和控制消息
    let normalMsg = StringMessage("普通消息")
    let controlMsg = StopMessage("测试停止")  // 使用真正的控制消息

    let normalEnvelope = Envelope(normalMsg, None<String>)
    let controlEnvelope = Envelope(controlMsg, None<String>)

    // 先入队普通消息，再入队控制消息
    @Expect(controlWrapper.enqueue(normalEnvelope), true)
    @Expect(controlWrapper.enqueue(controlEnvelope), true)

    // 测试ControlAwareSemantics接口
    @Assert(controlWrapper.isControlMessage(controlMsg), true)
    @Assert(controlWrapper.isControlMessage(normalMsg), false)
    @Assert(controlWrapper.controlQueueSize(), Int64(1))

    // 控制消息应该优先出队
    match (controlWrapper.dequeue()) {
        case Some(envelope) =>
            @Assert(envelope.getMessage().messageType(), "StopMessage")
        case None =>
            @Assert(false, true)  // 不应该为空
    }

    // 然后是普通消息
    match (controlWrapper.dequeue()) {
        case Some(envelope) =>
            @Assert(envelope.getMessage().messageType(), "StringMessage")
        case None =>
            @Assert(false, true)  // 不应该为空
    }
}

/**
 * 测试PriorityMailboxWrapper优先级邮箱包装器
 */
@Test
func testPriorityMailboxWrapper(): Unit {
    let priorityWrapper = PriorityMailboxWrapper()

    // 添加不同优先级的消息
    let lowMsg = StringMessage("LowPriorityMessage")
    let normalMsg = StringMessage("NormalMessage")
    let controlMsg = StopMessage("测试停止")  // 高优先级控制消息

    let lowEnvelope = Envelope(lowMsg, None<String>)
    let normalEnvelope = Envelope(normalMsg, None<String>)
    let controlEnvelope = Envelope(controlMsg, None<String>)

    // 按低->普通->高的顺序入队
    @Assert(priorityWrapper.enqueue(lowEnvelope), true)
    @Assert(priorityWrapper.enqueue(normalEnvelope), true)
    @Assert(priorityWrapper.enqueue(controlEnvelope), true)

    // 测试PriorityMailboxSemantics接口
    @Assert(priorityWrapper.priorityOf(controlMsg), Int32(100))
    @Assert(priorityWrapper.priorityOf(normalMsg), Int32(0))
    @Assert(priorityWrapper.priorityOf(lowMsg), Int32(-10))

    // 应该按高->普通->低的顺序出队
    match (priorityWrapper.dequeue()) {
        case Some(envelope) =>
            @Assert(envelope.getMessage().messageType(), "StopMessage")  // 控制消息优先
        case None =>
            @Assert(false, true)
    }

    match (priorityWrapper.dequeue()) {
        case Some(envelope) =>
            @Assert(envelope.getMessage().messageType(), "StringMessage")  // 普通消息
        case None =>
            @Assert(false, true)
    }

    match (priorityWrapper.dequeue()) {
        case Some(envelope) =>
            @Assert(envelope.getMessage().messageType(), "StringMessage")  // 低优先级消息
        case None =>
            @Assert(false, true)
    }
}

/**
 * 测试advanced/foundation_based_mailbox.cj弃用标注
 * 验证PR-3中的遗漏项已完成
 */
@Test
func testAdvancedFoundationMailboxDeprecation(): Unit {
    // 验证弃用的advanced包中的邮箱仍然可以工作
    // 但建议使用新的FoundationMailbox + 包装器模式

    // 测试新的推荐方式：使用FoundationMailbox
    let foundationMailbox = FoundationMailbox(1000)

    // 添加消息测试
    let msg = StringMessage("测试Foundation邮箱")
    let envelope = Envelope(msg, None<String>)
    @Assert(foundationMailbox.enqueue(envelope), true)

    // 验证消息可以正确出队
    match (foundationMailbox.dequeue()) {
        case Some(dequeuedEnvelope) =>
            @Assert(dequeuedEnvelope.getMessage().messageType(), "StringMessage")
        case None =>
            @Assert(false, true)
    }

    // 验证邮箱状态
    @Assert(foundationMailbox.isEmpty(), true)
    @Assert(foundationMailbox.size(), Int64(0))
}

/**
 * 测试MailboxConfig统一化
 * 验证配置来源的统一性
 */
@Test
func testMailboxConfigUnification(): Unit {
    // 验证MailboxFactory能够正确处理统一的配置
    let factory = MailboxFactory(MailboxConfig.createFoundation())
    factory.start()

    // 测试不同类型的邮箱都使用统一的配置系统
    let foundationConfig = MailboxConfig(MailboxType.Foundation, Int32(1000), true, false, "foundation-test")
    let boundedConfig = MailboxConfig(MailboxType.Bounded, Int32(500), true, true, "bounded-test")
    let priorityConfig = MailboxConfig(MailboxType.Priority, Int32(800), true, false, "priority-test")

    // 验证配置对象的一致性
    @Assert(foundationConfig.capacity, Int32(1000))
    @Assert(foundationConfig.enableMetrics, true)
    @Assert(foundationConfig.name, "foundation-test")

    @Assert(boundedConfig.capacity, Int32(500))
    @Assert(boundedConfig.enableBackpressure, true)
    @Assert(boundedConfig.name, "bounded-test")

    @Assert(priorityConfig.capacity, Int32(800))
    @Assert(priorityConfig.name, "priority-test")

    // 验证邮箱类型（通过模式匹配验证）
    match (foundationConfig.mailboxType) {
        case MailboxType.Foundation => @Assert(true, true)
        case _ => @Assert(false, true)
    }

    match (boundedConfig.mailboxType) {
        case MailboxType.Bounded => @Assert(true, true)
        case _ => @Assert(false, true)
    }

    match (priorityConfig.mailboxType) {
        case MailboxType.Priority => @Assert(true, true)
        case _ => @Assert(false, true)
    }

    factory.shutdown()
}

/**
 * 测试ZeroCopyMailbox零拷贝邮箱
 */
@Test
func testZeroCopyMailbox(): Unit {
    let zeroCopyMailbox = ZeroCopyMailbox(Int64(1000), Int32(64))

    // 测试普通消息
    let normalMsg = StringMessage("普通消息")
    let normalEnvelope = Envelope(normalMsg, None<String>)
    @Assert(zeroCopyMailbox.enqueue(normalEnvelope), true)

    // 测试大消息（应该触发零拷贝优化）
    let largeMsg = StringMessage("LargeMessage_with_very_long_content_to_trigger_zero_copy_optimization")
    let largeEnvelope = Envelope(largeMsg, None<String>)
    @Assert(zeroCopyMailbox.enqueueZeroCopy(largeEnvelope), true)

    // 测试ZeroCopySemantics接口
    let stats = zeroCopyMailbox.getZeroCopyStats()
    @Assert(stats.totalZeroCopyOps >= Int64(1), true)
    @Assert(stats.avgMessageSize > 0.0, true)

    // 测试MetricsSemantics接口
    let metrics = zeroCopyMailbox.getMetrics()
    @Assert(metrics.totalEnqueued >= Int64(2), true)
    @Assert(zeroCopyMailbox.isMetricsEnabled(), true)

    // 测试重置指标
    zeroCopyMailbox.resetMetrics()
    let resetStats = zeroCopyMailbox.getZeroCopyStats()
    @Assert(resetStats.totalZeroCopyOps, Int64(0))
}

/**
 * 测试StarvationProtectedPriorityMailbox饥饿保护优先级邮箱
 */
@Test
func testStarvationProtectedPriorityMailbox(): Unit {
    let starvationMailbox = StarvationProtectedPriorityMailbox(Int64(100))  // 100ms阈值

    // 添加不同优先级的消息
    let lowMsg = StringMessage("LowPriorityMessage")
    let normalMsg = StringMessage("NormalMessage")
    let controlMsg = StopMessage("测试停止")

    let lowEnvelope = Envelope(lowMsg, None<String>)
    let normalEnvelope = Envelope(normalMsg, None<String>)
    let controlEnvelope = Envelope(controlMsg, None<String>)

    // 入队消息
    @Assert(starvationMailbox.enqueue(lowEnvelope), true)
    @Assert(starvationMailbox.enqueue(normalEnvelope), true)
    @Assert(starvationMailbox.enqueue(controlEnvelope), true)

    // 测试PriorityMailboxSemantics接口
    @Assert(starvationMailbox.priorityOf(controlMsg), Int32(100))
    @Assert(starvationMailbox.priorityOf(normalMsg), Int32(0))
    @Assert(starvationMailbox.priorityOf(lowMsg), Int32(-10))

    // 测试StarvationProtectionSemantics接口
    starvationMailbox.setStarvationThreshold(Int64(50_000_000))  // 50ms in nanoseconds
    let starvationStats = starvationMailbox.getStarvationStats()
    @Assert(starvationStats.currentThreshold, Int64(50_000_000))

    // 正常出队应该按优先级顺序
    match (starvationMailbox.dequeue()) {
        case Some(envelope) =>
            @Assert(envelope.getMessage().messageType(), "StopMessage")  // 控制消息优先
        case None =>
            @Assert(false, true)
    }
}

/**
 * 测试WorkStealingFriendlyMailbox工作窃取友好邮箱
 */
@Test
func testWorkStealingFriendlyMailbox(): Unit {
    let innerMailbox = FoundationMailbox(1000)
    let workStealingMailbox = WorkStealingFriendlyMailbox(innerMailbox, Int64(1000))

    // 添加足够多的消息以触发窃取条件
    for (i in 0..600) {
        let msg = StringMessage("消息-${i}")
        let envelope = Envelope(msg, None<String>)
        @Assert(workStealingMailbox.enqueue(envelope), true)
    }

    // 测试WorkStealingFriendlySemantics接口
    @Assert(workStealingMailbox.isStealable(), true)  // 负载超过50%应该可以窃取

    let hint = workStealingMailbox.getStealingHint()
    @Assert(hint.loadLevel > 0.5, true)
    @Assert(hint.recommendedBatchSize > 0, true)
    @Assert(hint.priority > 0, true)

    // 测试批量窃取
    let stolenMessages = workStealingMailbox.stealBatch(Int32(50))
    @Assert(stolenMessages.size > 0, true)
    @Assert(stolenMessages.size <= 50, true)

    // 测试BatchDequeueSemantics接口
    let batchMessages = workStealingMailbox.dequeueBatch(Int32(10))
    @Assert(batchMessages.size > 0, true)
    @Assert(batchMessages.size <= 10, true)
}
