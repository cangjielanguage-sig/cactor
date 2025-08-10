/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 邮箱包导出
 */
package cactor.runtime.mailbox

// 邮箱系统包 - 避免循环依赖，通过文件自动导出


import cactor.core.message.Envelope


/**
 * 批处理可选语义接口
 * 支持批量出队操作，提高消息处理吞吐量
 */
public interface BatchDequeueSemantics {
    /**
     * 批量出队消息
     * @param max 最大出队数量
     * @return 实际出队的消息数组，可能少于max
     */
    func dequeueBatch(max: Int32): Array<Envelope>
}

/**
 * 邮箱指标数据结构
 * 统一的邮箱性能指标定义
 */
public struct MailboxMetrics {
    public let totalEnqueued: Int64      // 总入队数量
    public let totalDequeued: Int64      // 总出队数量
    public let currentSize: Int64        // 当前队列大小
    public let maxSize: Int64           // 历史最大队列大小
    public let avgProcessingTime: Int64  // 平均处理时间（纳秒）
    public let dropCount: Int64         // 丢弃消息数量
    public let lastUpdateTime: Int64    // 最后更新时间戳

    public init(totalEnqueued: Int64, totalDequeued: Int64, currentSize: Int64,
                maxSize: Int64, avgProcessingTime: Int64, dropCount: Int64, lastUpdateTime: Int64) {
        this.totalEnqueued = totalEnqueued
        this.totalDequeued = totalDequeued
        this.currentSize = currentSize
        this.maxSize = maxSize
        this.avgProcessingTime = avgProcessingTime
        this.dropCount = dropCount
        this.lastUpdateTime = lastUpdateTime
    }
}

/**
 * 邮箱指标语义接口
 * 提供统一的指标收集和查询能力
 */
public interface MetricsSemantics {
    /**
     * 获取邮箱指标
     * @return 当前的邮箱指标数据
     */
    func getMetrics(): MailboxMetrics

    /**
     * 重置指标计数器
     */
    func resetMetrics(): Unit

    /**
     * 是否启用指标收集
     * @return true表示启用指标收集
     */
    func isMetricsEnabled(): Bool
}
