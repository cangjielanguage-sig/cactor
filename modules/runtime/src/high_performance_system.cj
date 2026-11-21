/*
 * CActor Runtime - 高性能 Actor 系统实现（D1 门面收敛）
 *
 * 本类只被 CActorRuntime 使用，随门面收敛降为 internal。Cangjie 的 internal 是包级可见，
 * 因此从 cactor_runtime.system 移入 cactor_runtime 包，与唯一消费者同包。
 * 其配置与统计类型仍留在 cactor_runtime.system（属于公开 API）。
 */
package cactor_runtime

import std.sync.*
import std.time.*
import std.collection.*
import cactor_core.actor.{ActorRef, ActorPath}
import cactor_core.message.*
import cactor_runtime.context.{PooledActorContextPool, PoolStats}
import cactor_runtime.actor.{HighPerformanceActor, HighPerformanceActorBehavior, ActorStats}
import cactor_runtime.system.{HighPerformanceActorSystemConfig, SystemStats, MonitorStats, DispatcherStats}
import cactor_foundation.logging.{Logger, Logging}

let log_high_performance_actor_system: Logger = Logging.logger("cactor_runtime.high_performance_actor_system")

// 简化时间函数
func highPerfCurrentTimeNanos(): Int64 {
    let now = DateTime.now()
    now.toUnixTimeStamp().toMilliseconds() * 1000000
}

/**
 * 高性能Actor系统 - Runtime层实现
 * 实现tu.md中的核心优化：无锁邮箱、对象池化
 */
internal class HighPerformanceActorSystem {
    // 系统配置
    private let config: HighPerformanceActorSystemConfig

    // Actor注册表 (使用标准HashMap)
    private let actors: HashMap<String, HighPerformanceActor>

    // 上下文对象池
    private let contextPool: PooledActorContextPool

    // 系统状态
    private let isRunning: AtomicBool
    private let actorCount: AtomicInt64
    private let messageCount: AtomicInt64

    public init(config: HighPerformanceActorSystemConfig) {
        this.config = config
        this.actors = HashMap<String, HighPerformanceActor>()
        this.contextPool = PooledActorContextPool(config.contextPoolSize)
        this.isRunning = AtomicBool(false)
        this.actorCount = AtomicInt64(0)
        this.messageCount = AtomicInt64(0)
    }

    /**
     * 启动Actor系统
     */
    public func start(): Unit {
        if (isRunning.compareAndSwap(false, true)) {
            // 预热对象池
            contextPool.warmup()

            log_high_performance_actor_system.debug("🚀 高性能Actor系统启动成功")
            log_high_performance_actor_system.debug("  最大Actor数: ${config.maxActors}")
            log_high_performance_actor_system.debug("  工作线程数: ${config.workerThreads}")
            log_high_performance_actor_system.debug("  上下文池大小: ${config.contextPoolSize}")
            let batchStatus = if (config.enableBatchProcessing) { "启用" } else { "禁用" }
            log_high_performance_actor_system.debug("  批处理: ${batchStatus}")
        }
    }

    /**
     * 停止Actor系统
     */
    public func shutdown(): Unit {
        if (isRunning.compareAndSwap(true, false)) {
            // 停止所有Actor
            for ((actorId, actor) in actors) {
                actor.stop()
            }
            actors.clear()

            log_high_performance_actor_system.debug("🛑 高性能Actor系统已停止")
        }
    }

    /**
     * 创建高性能Actor
     */
    public func createActor(actorId: String, behavior: HighPerformanceActorBehavior): Option<HighPerformanceActorRef> {
        if (!isRunning.load()) {
            return None
        }

        if (actorCount.load() >= config.maxActors) {
            log_high_performance_actor_system.debug("已达到最大Actor数量限制: ${config.maxActors}")
            return None
        }

        match (actors.get(actorId)) {
            case Some(_) => return None  // Actor已存在
            case None => ()
        }

        // 创建高性能Actor
        let actor = HighPerformanceActor(actorId, behavior, contextPool)
        actors.add(actorId, actor)
        actorCount.fetchAdd(1)

        // 启动Actor
        actor.start()

        return Some(HighPerformanceActorRef(actorId, actor, this))
    }

    /**
     * 获取Actor引用
     */
    public func getActor(actorId: String): Option<HighPerformanceActorRef> {
        match (actors.get(actorId)) {
            case Some(actor) => Some(HighPerformanceActorRef(actorId, actor, this))
            case None => None
        }
    }

    /**
     * 移除Actor
     */
    public func removeActor(actorId: String): Bool {
        match (actors.get(actorId)) {
            case Some(actor) =>
                // 停止Actor
                actor.stop()

                // 从注册表移除
                actors.remove(actorId)
                actorCount.fetchAdd(-1)

                return true
            case None =>
                return false
        }
    }

    /**
     * 高性能消息发送
     */
    public func sendMessage(actorId: String, message: Message): Bool {
        match (getActor(actorId)) {
            case Some(actorRef) =>
                actorRef.tell(message)
                messageCount.fetchAdd(1)
                return true
            case None =>
                return false
        }
    }

    /**
     * 批量消息发送
     */
    public func sendMessageBatch(messages: Array<(String, Message)>): Int32 {
        var successCount: Int32 = 0

        for ((actorId, message) in messages) {
            if (sendMessage(actorId, message)) {
                successCount += 1
            }
        }

        return successCount
    }

    /**
     * 获取系统统计信息
     */
    public func getSystemStats(): SystemStats {
        let poolStats = contextPool.getPoolStats()
        let dispatcherStats = DispatcherStats(messageCount.load(), 0.5)
        let monitorStats = MonitorStats(actorCount.load(), 0, messageCount.load(), 0, 0)

        SystemStats(
            actorCount.load(),
            messageCount.load(),
            isRunning.load(),
            poolStats,
            dispatcherStats,
            monitorStats
        )
    }

    /**
     * 获取配置信息
     */
    public func getConfig(): HighPerformanceActorSystemConfig {
        config
    }
}

/**
 * 高性能Actor引用
 */
internal class HighPerformanceActorRef <: ActorRef {
    private let actorId: String
    private let actor: HighPerformanceActor
    private let system: HighPerformanceActorSystem

    public init(actorId: String, actor: HighPerformanceActor, system: HighPerformanceActorSystem) {
        this.actorId = actorId
        this.actor = actor
        this.system = system
    }

    public prop path: ActorPath {
        get() {
            ActorPath("cactor", "/user/${actorId}")
        }
    }

    /**
     * 高性能消息发送
     */
    public func tell(message: Message): Unit {
        actor.tell(message)
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        actor.tell(message)
    }

    /**
     * 批量消息处理
     */
    public func processBatch(batchSize: Int32): Int32 {
        actor.processBatch(batchSize)
    }

    /**
     * 获取Actor统计信息
     */
    public func getStats(): ActorStats {
        actor.getStats()
    }

    /**
     * 获取Actor状态值
     */
    public func getStateValue(): Int64 {
        actor.getStateValue()
    }

    public func equals(other: ActorRef): Bool {
        match (other) {
            case ref: HighPerformanceActorRef => ref.actorId == this.actorId
            case _ => false
        }
    }

    public func hashCode(): Int32 {
        Int32(actorId.hashCode())
    }
}

/**
 * 系统性能监控器
 */
internal class SystemPerformanceMonitor {
    private let enabled: Bool
    private let startTime: AtomicInt64
    private let actorsCreated: AtomicInt64
    private let actorsDestroyed: AtomicInt64
    private let messagesSent: AtomicInt64
    private let messagesFailed: AtomicInt64

    public init(enabled: Bool) {
        this.enabled = enabled
        this.startTime = AtomicInt64(0)
        this.actorsCreated = AtomicInt64(0)
        this.actorsDestroyed = AtomicInt64(0)
        this.messagesSent = AtomicInt64(0)
        this.messagesFailed = AtomicInt64(0)
    }

    public func start(): Unit {
        if (enabled) {
            startTime.store(highPerfCurrentTimeNanos())
            log_high_performance_actor_system.debug("📊 性能监控已启动")
        }
    }

    public func stop(): Unit {
        if (enabled) {
            log_high_performance_actor_system.debug("📊 性能监控已停止")
        }
    }

    public func recordActorCreated(): Unit {
        if (enabled) {
            actorsCreated.fetchAdd(1)
        }
    }

    public func recordActorDestroyed(): Unit {
        if (enabled) {
            actorsDestroyed.fetchAdd(1)
        }
    }

    public func recordMessageSent(): Unit {
        if (enabled) {
            messagesSent.fetchAdd(1)
        }
    }

    public func recordMessageFailed(): Unit {
        if (enabled) {
            messagesFailed.fetchAdd(1)
        }
    }

    public func getStats(): MonitorStats {
        let uptime = if (startTime.load() > 0) {
            highPerfCurrentTimeNanos() - startTime.load()
        } else {
            0
        }

        MonitorStats(
            actorsCreated.load(),
            actorsDestroyed.load(),
            messagesSent.load(),
            messagesFailed.load(),
            uptime
        )
    }
}
