/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 优先级调度器
 * 基于优先级的消息调度，支持多级优先队列和吞吐量控制
 */
package cactor.runtime.dispatcher

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*
import cactor.core.actor.{ActorRef}
import cactor.core.message.{Envelope}
import cactor.runtime.dispatcher.work_stealing.{ActorTask, Priority, ThroughputStats}

/**
 * 获取当前时间的纳秒表示
 */
func getCurrentTimeNanos(): Int64 {
    let now = DateTime.now()
    return now.toUnixTimeStamp().toMilliseconds() * 1000000
}

/**
 * 优先级调度器
 * 支持基于优先级的消息调度和吞吐量控制
 */
public class PriorityDispatcher {
    private let workerCount: Int32
    private let workers: Array<PriorityWorkerThread>
    private let criticalQueue: ConcurrentLinkedQueue<ActorTask>
    private let highQueue: ConcurrentLinkedQueue<ActorTask>
    private let normalQueue: ConcurrentLinkedQueue<ActorTask>
    private let lowQueue: ConcurrentLinkedQueue<ActorTask>
    private let roundRobinCounter: AtomicInt64
    private let isStarted: AtomicBool
    private let totalTasksSubmitted: AtomicInt64
    private let totalTasksCompleted: AtomicInt64
    
    // 吞吐量控制
    private let throughput: Int32
    private let throughputDeadlineTime: Duration
    private let lastThroughputCheck: AtomicInt64
    private let currentBatchCount: AtomicInt32
    private let throughputMutex: Mutex
    
    // 优先级统计
    private let criticalTasksProcessed: AtomicInt64
    private let highTasksProcessed: AtomicInt64
    private let normalTasksProcessed: AtomicInt64
    private let lowTasksProcessed: AtomicInt64

    public init(workerCount: Int32) {
        this.workerCount = workerCount
        this.criticalQueue = ConcurrentLinkedQueue<ActorTask>()
        this.highQueue = ConcurrentLinkedQueue<ActorTask>()
        this.normalQueue = ConcurrentLinkedQueue<ActorTask>()
        this.lowQueue = ConcurrentLinkedQueue<ActorTask>()
        this.roundRobinCounter = AtomicInt64(0)
        this.isStarted = AtomicBool(false)
        this.totalTasksSubmitted = AtomicInt64(0)
        this.totalTasksCompleted = AtomicInt64(0)
        
        // 默认吞吐量配置
        this.throughput = 100
        this.throughputDeadlineTime = Duration.millisecond * 100
        this.lastThroughputCheck = AtomicInt64(getCurrentTimeNanos())
        this.currentBatchCount = AtomicInt32(0)
        this.throughputMutex = Mutex()
        
        // 初始化优先级统计
        this.criticalTasksProcessed = AtomicInt64(0)
        this.highTasksProcessed = AtomicInt64(0)
        this.normalTasksProcessed = AtomicInt64(0)
        this.lowTasksProcessed = AtomicInt64(0)

        // 创建优先级工作线程
        let cQueue = this.criticalQueue
        let hQueue = this.highQueue
        let nQueue = this.normalQueue
        let lQueue = this.lowQueue
        let configThroughput = this.throughput
        let configDeadlineTime = this.throughputDeadlineTime
        this.workers = Array<PriorityWorkerThread>(Int64(workerCount), { i =>
            PriorityWorkerThread(
                Int32(i),
                cQueue,
                hQueue,
                nQueue,
                lQueue,
                configThroughput,
                configDeadlineTime
            )
        })
    }
    
    /**
     * 带配置的构造函数
     */
    public init(workerCount: Int32, throughput: Int32, throughputDeadlineTime: Duration) {
        this.workerCount = workerCount
        this.criticalQueue = ConcurrentLinkedQueue<ActorTask>()
        this.highQueue = ConcurrentLinkedQueue<ActorTask>()
        this.normalQueue = ConcurrentLinkedQueue<ActorTask>()
        this.lowQueue = ConcurrentLinkedQueue<ActorTask>()
        this.roundRobinCounter = AtomicInt64(0)
        this.isStarted = AtomicBool(false)
        this.totalTasksSubmitted = AtomicInt64(0)
        this.totalTasksCompleted = AtomicInt64(0)
        
        // 配置的吞吐量参数
        this.throughput = throughput
        this.throughputDeadlineTime = throughputDeadlineTime
        this.lastThroughputCheck = AtomicInt64(getCurrentTimeNanos())
        this.currentBatchCount = AtomicInt32(0)
        this.throughputMutex = Mutex()
        
        // 初始化优先级统计
        this.criticalTasksProcessed = AtomicInt64(0)
        this.highTasksProcessed = AtomicInt64(0)
        this.normalTasksProcessed = AtomicInt64(0)
        this.lowTasksProcessed = AtomicInt64(0)

        // 创建优先级工作线程
        let cQueue = this.criticalQueue
        let hQueue = this.highQueue
        let nQueue = this.normalQueue
        let lQueue = this.lowQueue
        let configThroughput = this.throughput
        let configDeadlineTime = this.throughputDeadlineTime
        this.workers = Array<PriorityWorkerThread>(Int64(workerCount), { i =>
            PriorityWorkerThread(
                Int32(i),
                cQueue,
                hQueue,
                nQueue,
                lQueue,
                configThroughput,
                configDeadlineTime
            )
        })
    }

    public func start(): Unit {
        if (isStarted.compareAndSwap(false, true)) {
            for (worker in workers) {
                worker.start()
            }
            println("优先级调度器启动，工作线程数: ${workerCount}")
        }
    }

    public func stop(): Unit {
        if (isStarted.compareAndSwap(true, false)) {
            for (worker in workers) {
                worker.stop()
            }
            println("优先级调度器停止")
        }
    }

    public func dispatch(envelope: Envelope, actorRef: ActorRef): Unit {
        dispatchWithPriority(envelope, actorRef, Priority.Normal)
    }

    public func dispatchWithPriority(envelope: Envelope, actorRef: ActorRef, priority: Priority): Unit {
        let task = ActorTask(envelope, actorRef, priority)
        totalTasksSubmitted.fetchAdd(1)

        // 根据优先级分发到不同队列
        match (priority) {
            case Priority.Critical => 
                criticalQueue.add(task)
            case Priority.High => 
                highQueue.add(task)
            case Priority.Normal => 
                normalQueue.add(task)
            case Priority.Low => 
                lowQueue.add(task)
        }
    }

    public func getWorkerCount(): Int32 {
        workerCount
    }

    public func getTotalTasksSubmitted(): Int64 {
        totalTasksSubmitted.load()
    }

    public func getTotalTasksProcessed(): Int64 {
        var total = 0
        for (worker in workers) {
            total += worker.getTasksProcessed()
        }
        total
    }
    
    /**
     * 获取优先级统计信息
     */
    public func getPriorityStats(): PriorityStats {
        PriorityStats(
            criticalTasksProcessed.load(),
            highTasksProcessed.load(),
            normalTasksProcessed.load(),
            lowTasksProcessed.load(),
            criticalQueue.size,
            highQueue.size,
            normalQueue.size,
            lowQueue.size
        )
    }
    
    /**
     * 获取吞吐量统计
     */
    public func getThroughputStats(): ThroughputStats {
        let currentTime = getCurrentTimeNanos()
        let lastCheck = lastThroughputCheck.load()
        let timeDiff = currentTime - lastCheck
        let timeDiffSeconds = Float64(timeDiff) / 1000000000.0
        
        let totalProcessed = getTotalTasksProcessed()
        let currentThroughput = if (timeDiffSeconds > 0.0) {
            Float64(totalProcessed) / timeDiffSeconds
        } else {
            0.0
        }
        
        ThroughputStats(
            throughput,
            throughputDeadlineTime,
            currentThroughput,
            totalProcessed,
            currentBatchCount.load(),
            timeDiffSeconds
        )
    }
    
    /**
     * 重置吞吐量统计
     */
    public func resetThroughputStats(): Unit {
        throughputMutex.lock()
        try {
            lastThroughputCheck.store(getCurrentTimeNanos())
            currentBatchCount.store(0)
        } finally {
            throughputMutex.unlock()
        }
    }

    public func getWorkerStats(): Array<PriorityWorkerStats> {
        let stats = ArrayList<PriorityWorkerStats>()
        for (worker in workers) {
            stats.add(PriorityWorkerStats(
                worker.getWorkerId(),
                worker.getTasksProcessed(),
                worker.getCriticalTasksProcessed(),
                worker.getHighTasksProcessed(),
                worker.getNormalTasksProcessed(),
                worker.getLowTasksProcessed()
            ))
        }
        stats.toArray()
    }
}

/**
 * 优先级工作线程
 * 按优先级顺序处理任务：Critical -> High -> Normal -> Low
 */
public class PriorityWorkerThread {
    private let workerId: Int32
    private let criticalQueue: ConcurrentLinkedQueue<ActorTask>
    private let highQueue: ConcurrentLinkedQueue<ActorTask>
    private let normalQueue: ConcurrentLinkedQueue<ActorTask>
    private let lowQueue: ConcurrentLinkedQueue<ActorTask>
    private let isRunning: AtomicBool
    private let tasksProcessed: AtomicInt64

    // 优先级统计
    private let criticalTasksProcessed: AtomicInt64
    private let highTasksProcessed: AtomicInt64
    private let normalTasksProcessed: AtomicInt64
    private let lowTasksProcessed: AtomicInt64

    // 吞吐量控制
    private let throughput: Int32
    private let throughputDeadlineTime: Duration
    private let currentBatchCount: AtomicInt32
    private let lastBatchTime: AtomicInt64

    public init(workerId: Int32,
                criticalQueue: ConcurrentLinkedQueue<ActorTask>,
                highQueue: ConcurrentLinkedQueue<ActorTask>,
                normalQueue: ConcurrentLinkedQueue<ActorTask>,
                lowQueue: ConcurrentLinkedQueue<ActorTask>,
                throughput: Int32,
                throughputDeadlineTime: Duration) {
        this.workerId = workerId
        this.criticalQueue = criticalQueue
        this.highQueue = highQueue
        this.normalQueue = normalQueue
        this.lowQueue = lowQueue
        this.isRunning = AtomicBool(false)
        this.tasksProcessed = AtomicInt64(0)

        // 初始化优先级统计
        this.criticalTasksProcessed = AtomicInt64(0)
        this.highTasksProcessed = AtomicInt64(0)
        this.normalTasksProcessed = AtomicInt64(0)
        this.lowTasksProcessed = AtomicInt64(0)

        // 吞吐量控制配置
        this.throughput = throughput
        this.throughputDeadlineTime = throughputDeadlineTime
        this.currentBatchCount = AtomicInt32(0)
        this.lastBatchTime = AtomicInt64(getCurrentTimeNanos())
    }

    public func start(): Unit {
        if (isRunning.compareAndSwap(false, true)) {
            spawn {
                workerLoop()
            }
        }
    }

    public func stop(): Unit {
        isRunning.store(false)
    }

    private func workerLoop(): Unit {
        while (isRunning.load()) {
            // 检查是否需要进行吞吐量控制
            if (shouldThrottleThroughput()) {
                sleep(Duration.microsecond * 100)
                continue
            }

            match (findTaskByPriority()) {
                case Some(task) =>
                    executeTask(task)
                    tasksProcessed.fetchAdd(1)
                    updatePriorityStats(task.getPriority())

                    // 更新批次计数
                    let batchCount = currentBatchCount.fetchAdd(1)
                    if (batchCount >= throughput) {
                        // 达到吞吐量限制，重置计数器并记录时间
                        currentBatchCount.store(0)
                        lastBatchTime.store(getCurrentTimeNanos())
                    }
                case None =>
                    // 没有任务，短暂休眠
                    sleep(Duration.microsecond * 10)
            }
        }
    }

    /**
     * 按优先级顺序查找任务
     */
    private func findTaskByPriority(): Option<ActorTask> {
        // 1. 首先检查关键优先级队列
        match (criticalQueue.remove()) {
            case Some(task) => Some(task)
            case None =>
                // 2. 检查高优先级队列
                match (highQueue.remove()) {
                    case Some(task) => Some(task)
                    case None =>
                        // 3. 检查普通优先级队列
                        match (normalQueue.remove()) {
                            case Some(task) => Some(task)
                            case None =>
                                // 4. 最后检查低优先级队列
                                lowQueue.remove()
                        }
                }
        }
    }

    /**
     * 更新优先级统计
     */
    private func updatePriorityStats(priority: Priority): Unit {
        match (priority) {
            case Priority.Critical => criticalTasksProcessed.fetchAdd(1)
            case Priority.High => highTasksProcessed.fetchAdd(1)
            case Priority.Normal => normalTasksProcessed.fetchAdd(1)
            case Priority.Low => lowTasksProcessed.fetchAdd(1)
        }
    }

    /**
     * 检查是否需要进行吞吐量控制
     */
    private func shouldThrottleThroughput(): Bool {
        let currentTime = getCurrentTimeNanos()
        let lastTime = lastBatchTime.load()
        let timeDiff = currentTime - lastTime
        let deadlineNanos = throughputDeadlineTime.toNanoseconds()

        // 如果在截止时间内已经处理了足够的任务，则需要节流
        if (timeDiff < deadlineNanos && currentBatchCount.load() >= throughput) {
            return true
        }

        // 如果超过截止时间，重置计数器
        if (timeDiff >= deadlineNanos) {
            currentBatchCount.store(0)
            lastBatchTime.store(currentTime)
        }

        return false
    }

    private func executeTask(task: ActorTask): Unit {
        try {
            // 执行任务的逻辑
            let message = task.getEnvelope().getMessage()
            // 实际的消息处理逻辑应该在这里
        } catch (e: Exception) {
            // 错误处理
        }
    }

    // Getter方法
    public func getWorkerId(): Int32 { workerId }
    public func getTasksProcessed(): Int64 { tasksProcessed.load() }
    public func getCriticalTasksProcessed(): Int64 { criticalTasksProcessed.load() }
    public func getHighTasksProcessed(): Int64 { highTasksProcessed.load() }
    public func getNormalTasksProcessed(): Int64 { normalTasksProcessed.load() }
    public func getLowTasksProcessed(): Int64 { lowTasksProcessed.load() }
}

/**
 * 优先级统计信息
 */
public struct PriorityStats {
    public let criticalTasksProcessed: Int64
    public let highTasksProcessed: Int64
    public let normalTasksProcessed: Int64
    public let lowTasksProcessed: Int64
    public let criticalQueueSize: Int64
    public let highQueueSize: Int64
    public let normalQueueSize: Int64
    public let lowQueueSize: Int64

    public init(criticalTasksProcessed: Int64, highTasksProcessed: Int64,
                normalTasksProcessed: Int64, lowTasksProcessed: Int64,
                criticalQueueSize: Int64, highQueueSize: Int64,
                normalQueueSize: Int64, lowQueueSize: Int64) {
        this.criticalTasksProcessed = criticalTasksProcessed
        this.highTasksProcessed = highTasksProcessed
        this.normalTasksProcessed = normalTasksProcessed
        this.lowTasksProcessed = lowTasksProcessed
        this.criticalQueueSize = criticalQueueSize
        this.highQueueSize = highQueueSize
        this.normalQueueSize = normalQueueSize
        this.lowQueueSize = lowQueueSize
    }

    public func getDescription(): String {
        "PriorityStats(critical=${criticalTasksProcessed}/${criticalQueueSize}, " +
        "high=${highTasksProcessed}/${highQueueSize}, " +
        "normal=${normalTasksProcessed}/${normalQueueSize}, " +
        "low=${lowTasksProcessed}/${lowQueueSize})"
    }
}

/**
 * 优先级工作线程统计信息
 */
public struct PriorityWorkerStats {
    public let workerId: Int32
    public let totalTasksProcessed: Int64
    public let criticalTasksProcessed: Int64
    public let highTasksProcessed: Int64
    public let normalTasksProcessed: Int64
    public let lowTasksProcessed: Int64

    public init(workerId: Int32, totalTasksProcessed: Int64,
                criticalTasksProcessed: Int64, highTasksProcessed: Int64,
                normalTasksProcessed: Int64, lowTasksProcessed: Int64) {
        this.workerId = workerId
        this.totalTasksProcessed = totalTasksProcessed
        this.criticalTasksProcessed = criticalTasksProcessed
        this.highTasksProcessed = highTasksProcessed
        this.normalTasksProcessed = normalTasksProcessed
        this.lowTasksProcessed = lowTasksProcessed
    }

    public func getDescription(): String {
        "PriorityWorkerStats(worker=${workerId}, total=${totalTasksProcessed}, " +
        "critical=${criticalTasksProcessed}, high=${highTasksProcessed}, " +
        "normal=${normalTasksProcessed}, low=${lowTasksProcessed})"
    }
}
