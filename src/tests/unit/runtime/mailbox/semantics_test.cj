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
