/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Runtime - 统一运行时管理器
 * 实现plan10.md中的CActorRuntime设计
 */
package cactor_runtime

import std.sync.*
import std.time.*
import cactor_foundation.logging.{Logging}
import cactor_core.actor.{Actor, ActorRef, Props, ActorSystemView, PoisonPill, isAffinityWorkerThread, ActorScheduler, RealTimeScheduler}
import cactor_core.message.{Message}
import cactor_core.system.{ActorSystem}
import cactor_runtime.guardian.{RuntimeSystemGuardian, RuntimeUserGuardian, SimpleUserActorRef, isActorThread}
import cactor_runtime.system.{HighPerformanceActorSystem, HighPerformanceActorSystemConfig, SystemStats}
import cactor_runtime.mailbox.factory.{AdvancedMailboxFactory, DefaultAdvancedMailboxFactory}
import cactor_runtime.mailbox.{MailboxFactory as RuntimeMailboxFactory}
import cactor_runtime.dispatcher.factory.{AdvancedDispatcherFactory, DefaultAdvancedDispatcherFactory}
import cactor_runtime.system.{SimpleActorSystem}
import cactor_runtime.mailbox.{MailboxFactory, MailboxConfig as RuntimeMailboxConfig}
import cactor_core.guardian.{GuardianStats}
import cactor_core.config.{MailboxConfig, DispatcherConfig}
import cactor_runtime.lifecycle.{ActorLifecycleManager, CoordinatedShutdown}
import cactor_runtime.registry.{SimpleActorRegistry}
import cactor_runtime.supervision.{SupervisionManager, SupervisionConfig}
import cactor_runtime.monitoring.{ActorSystemMetrics}
import cactor_runtime.events.{SimpleActorEventBus}
import cactor_core.supervision.{SupervisionStrategy}



let runtimeLog = Logging.logger("cactor.runtime")

/**
 * CActorRuntime - 统一运行时管理器
 *
 * 核心职责：
 * 1. 统一管理Actor系统的生命周期
 * 2. 提供Guardian Actor的完整实现
 * 3. 集成调度器、邮箱、定时器等运行时组件
 * 4. 提供生产级的Actor创建、停止、重启接口
 */
public class CActorRuntime {
    // 核心组件
    private let systemName: String
    private let actorSystem: ActorSystem
    private let systemGuardian: RuntimeSystemGuardian
    private let userGuardian: RuntimeUserGuardian

    // 运行时组件
    private let mailboxFactory: AdvancedMailboxFactory
    private let runtimeMailboxFactory: RuntimeMailboxFactory
    private let dispatcherFactory: AdvancedDispatcherFactory
    private let highPerformanceSystem: HighPerformanceActorSystem

    // plan10.md新增的企业级组件
    private let lifecycleManager: ActorLifecycleManager
    private let actorRegistry: SimpleActorRegistry
    private let supervisionManager: SupervisionManager
    private let systemMetrics: ActorSystemMetrics
    private let eventBus: SimpleActorEventBus

    // 运行时状态
    private let isRunning: AtomicBool
    private let startTime: AtomicInt64
    private let actorCount: AtomicInt64
    private let nameSequence: AtomicInt64
    // 串行化 Actor 创建中的“名称检查 + 创建 + 注册”
    private let creationLock: Mutex
    // 协调关闭：由本实例持有（E34，旧实现按系统名放在静态表里，闭包永不释放，同名系统还会共享任务）
    private let coordinatedShutdown: CoordinatedShutdown

    // 配置
    private let config: CActorRuntimeConfig

    public init(systemName: String, config: CActorRuntimeConfig) {
        this.systemName = systemName
        this.config = config

        // 创建基础ActorSystem
        this.actorSystem = SimpleActorSystem(systemName)

        // 创建企业级组件 (plan10.md)
        this.eventBus = SimpleActorEventBus()
        this.systemMetrics = ActorSystemMetrics()
        this.actorRegistry = SimpleActorRegistry()
        this.supervisionManager = SupervisionManager(SupervisionConfig.createDefault())
        this.lifecycleManager = ActorLifecycleManager()

        // 创建Guardian Actors
        this.systemGuardian = RuntimeSystemGuardian(actorSystem)
        this.userGuardian = RuntimeUserGuardian(actorSystem, config.scheduler)

        // 创建运行时组件
        this.mailboxFactory = DefaultAdvancedMailboxFactory()
        this.runtimeMailboxFactory = RuntimeMailboxFactory(RuntimeMailboxConfig.createUnbounded())
        this.dispatcherFactory = DefaultAdvancedDispatcherFactory()
        this.highPerformanceSystem = HighPerformanceActorSystem(config.highPerformanceConfig)

        // 初始化状态
        this.isRunning = AtomicBool(false)
        this.startTime = AtomicInt64(0)
        this.actorCount = AtomicInt64(0)
        this.nameSequence = AtomicInt64(0)
        this.creationLock = Mutex()
        this.coordinatedShutdown = CoordinatedShutdown(systemName)
    }

    /**
     * 启动Runtime系统
     */
    public func start(): Unit {
        if (isRunning.compareAndSwap(false, true)) {
            // 顶层 Actor 自行停止时（PoisonPill、MessageResult.Stop、监督 Stop 等）清理登记信息
            userGuardian.setTopLevelTerminatedHook({ ref => this.onTopLevelTerminated(ref) })
            userGuardian.setSystemView(RuntimeSystemView(this))
            startTime.store(getCurrentTimeNanos())

            // 启动高性能系统
            highPerformanceSystem.start()

            // Actor 由 ActorCellDispatcher 调度。这里曾经还会启动一个 DispatcherPool（4 个旧调度器），
            // 它从不接收任务，却有多个线程以 10µs–100µs 的间隔轮询：空闲时占约 10% CPU，
            // 并不断唤醒仓颉调度器，与 Actor 工作线程争抢处理器，所以已移除
            // MailboxFactory 在首次使用前也可启动，这里提前启动
            // 若后续改为惰性启动，可移除
            // try 启动 mailboxFactory（DefaultAdvancedMailboxFactory 并无 start，但 runtime.MailboxFactory 有）
            // 启动运行时MailboxFactory
            runtimeMailboxFactory.start()


            // 创建用户Guardian
            systemGuardian.createUserGuardian()

            // 注册 CoordinatedShutdown 阶段任务（只注册一次）
            registerCoordinatedShutdownTasks()

            runtimeLog.info("CActorRuntime 启动: 系统=${systemName}，配置=${config.getDescription()}")
        }
    }

    /**
     * 停止Runtime系统
     */
    public func shutdown(): Unit {
        // 发起关闭后即视为已终止（isTerminated 为真、不再接受新 Actor），与此前语义相同；
        // 已有的 Actor 要到 actor-system-terminate 阶段才停止（E34），之前各阶段的任务仍可与它们交互。
        // 只执行一次：并发或重复的 shutdown 都等待同一次关闭完成
        if (!isRunning.compareAndSwap(true, false) && !coordinatedShutdown.isRunning()) {
            return
        }
        let done = coordinatedShutdown.runAsync()
        // 在 Actor 中调用：同步等待就是等自己停止（与 Akka 相同，terminate 是异步的），只发起关闭
        if (isActorThread()) {
            return
        }
        done.get()
    }

    /** 本系统的协调关闭：可在各阶段注册关闭任务（对标 Akka CoordinatedShutdown(system)） */
    public func getCoordinatedShutdown(): CoordinatedShutdown {
        coordinatedShutdown
    }

    /**
     * 创建Actor - 统一接口 (plan10.md设计)
     * 实现完整的Actor创建流程
     */
    public func actorOf(props: Props<Actor>, name: String): ActorRef {
        if (!isRunning.load()) {
            throw Exception("Runtime系统未启动")
        }

        let actorRef = synchronized(creationLock) {
            // 1. 验证和准备
            validateActorCreation(name, None<ActorRef>)
            // 2. 通过用户Guardian创建Actor
            let created = userGuardian.createUserActor(props, name)
            // 3. 注册到Actor注册表
            actorRegistry.register(created, Some("/user"))
            created
        }

        // 4. 注册到生命周期管理器
        lifecycleManager.registerActor(actorRef)

        // 监督由 Actor Cell 与用户 Guardian 完成，这里不再向 SupervisionManager 逐个登记
        // （旧实现登记后从不注销，创建再停止的 Actor 会一直占用内存）

        // 6. 启动Actor
        lifecycleManager.startActor(actorRef)

        // 7. 更新统计
        systemMetrics.incrementActorCount()
        actorCount.fetchAdd(1)

        return actorRef
    }

    /**
     * 创建Actor - 自动生成名称
     */
    public func actorOf(props: Props<Actor>): ActorRef {
        let autoName = generateActorName()
        return actorOf(props, autoName)
    }

    /** 顶层 Actor 自行停止后的清理（由用户 Guardian 回调） */
    private func onTopLevelTerminated(actorRef: ActorRef): Unit {
        // 与 actorOf 的"创建 + 登记"互斥（R11）：Actor 在登记之前就终止时，先登记、后注销，
        // 否则注销落空、随后的登记留下死条目，此后同名 actorOf 永远报"名称已存在"
        synchronized(creationLock) {
            actorRegistry.unregister(actorRef)
        }
        lifecycleManager.unregisterActor(actorRef)
        systemMetrics.decrementActorCount()
        actorCount.fetchAdd(-1)
    }

    /** 按路径查找存活的 Actor（含子 Actor），例如 "/user/parent/child" */
    public func lookup(path: String): Option<ActorRef> {
        userGuardian.lookup(path)
    }

    /** 订阅死信：参数为消息和原目标路径 */
    public func subscribeDeadLetters(listener: (Message, String) -> Unit): Unit {
        userGuardian.subscribeDeadLetters(listener)
    }

    /** 系统内累计的死信数 */
    public func deadLetterCount(): Int64 {
        userGuardian.deadLetterCount()
    }

    /**
     * 把投递不出去的消息交给死信（计数、通知 subscribeDeadLetters 的订阅者、限速记日志），
     * 供运行时之外的路由组件使用：例如分片区域在节点离开集群、缓冲区满时丢弃的消息（对标 Akka system.deadLetters）
     */
    public func publishDeadLetter(message: Message, recipient: String): Unit {
        userGuardian.publishDeadLetter(message, recipient)
    }

    /** 系统内累计的未处理消息数（receive 返回 Unhandled、类型化协议之外的消息；不含死信） */
    public func unhandledCount(): Int64 {
        userGuardian.unhandledCount()
    }

    /**
     * 停止 Actor（对标 Akka `system.stop`，E33）：处理完当前消息即停，邮箱中剩余的消息计为死信；
     * 名字保留到真正终止，登记与计数在终止时由 onTopLevelTerminated 统一清理（重复停止不会重复计数）。
     * 顶层与子 Actor 都适用。需要"处理完已排队的消息再停"请用 gracefulStop 或 PoisonPill
     */
    public func stop(actorRef: ActorRef): Unit {
        userGuardian.removeUserActor(actorRef) |> ignoreBool
    }

    /**
     * 处理完已入队的消息后停止，并等待它终止（对标 Akka `Patterns.gracefulStop`）：发 PoisonPill，
     * 返回 true 表示已终止且名字已释放（可立即同名重建）；超时返回 false（目标可能仍在处理）。
     * 会阻塞调用线程，只能在 Actor 之外调用；远程引用只发 PoisonPill、返回 false
     */
    public func gracefulStop(actorRef: ActorRef, timeout: Duration): Bool {
        if (isAffinityWorkerThread()) {
            throw IllegalStateException(
                "gracefulStop 会阻塞等待目标终止，不能在 Actor 处理方法内调用：会占住亲和调度线程。" +
                "Actor 内请 watch 目标后发 PoisonPill，在 Terminated 里继续")
        }
        actorRef.tell(PoisonPill())
        match (actorRef) {
            case cell: SimpleUserActorRef => userGuardian.awaitTerminated(cell, timeout)
            case _ => false
        }
    }

    /**
     * 重启Actor
     */
    public func restart(actorRef: ActorRef, reason: Exception): Unit {
        userGuardian.restartChild(actorRef, reason)
    }

    /**
     * 恢复Actor
     */
    public func resumeActor(actorRef: ActorRef): Unit {
        runtimeLog.debug("恢复Actor: ${actorRef.path.toString()}")

        // 简化实现：记录恢复操作
        
    }

    /**
     * 停止Actor（重命名以避免冲突）
     */
    public func stopActor(actorRef: ActorRef): Unit {
        runtimeLog.debug("停止Actor: ${actorRef.path.toString()}")

        // 调用原有的stop方法
        stop(actorRef)

        // 简化实现：记录停止操作
        
    }

    /**
     * 重启Actor（重命名以避免冲突）
     */
    public func restartActor(actorRef: ActorRef): Unit {
        runtimeLog.debug("重启Actor: ${actorRef.path.toString()}")

        // 调用原有的restart方法
        restart(actorRef, Exception("Supervision restart"))

        // 简化实现：记录重启操作
        
    }

    /**
     * 获取系统Guardian
     */
    public func getSystemGuardian(): RuntimeSystemGuardian {
        systemGuardian
    }

    /**
     * 获取用户Guardian
     */
    public func getSystemName(): String {
        systemName
    }

    public func getUserGuardian(): RuntimeUserGuardian {
        userGuardian
    }

    /**
     * 获取高性能系统
     */
    public func getHighPerformanceSystem(): HighPerformanceActorSystem {
        highPerformanceSystem
    }

    /**
     * 获取运行时统计
     */
    public func getRuntimeStats(): CActorRuntimeStats {
        let uptime = if (startTime.load() > 0) {
            getCurrentTimeNanos() - startTime.load()
        } else {
            0
        }

        CActorRuntimeStats(
            systemName,
            isRunning.load(),
            actorCount.load(),
            uptime,
            systemGuardian.getStats(),
            userGuardian.getStats(),
            highPerformanceSystem.getSystemStats()
        )
    }

    /**
     * 获取配置
     */
    public func getConfig(): CActorRuntimeConfig {
        config
    }

    /** 本系统的调度器（E52） */
    public func scheduler(): ActorScheduler {
        config.scheduler
    }

    // 私有辅助方法
    private func createBaseActorSystem(name: String): ActorSystem {
        // 创建基础ActorSystem实现
        // 这里应该使用Core层的基础实现
        SimpleActorSystem(name)
    }

    /**
     * 获取邮箱工厂
     */
    public func getMailboxFactory(): AdvancedMailboxFactory {
        mailboxFactory
    }

    /**
     * 获取调度器工厂
     */
    public func getDispatcherFactory(): AdvancedDispatcherFactory {
        dispatcherFactory
    }

    /**
     * 以 core ActorSystem 接口访问本运行时（E29）：actorOf / actorSelection / terminate 都作用于真实的 Cell 运行时。
     * 此前返回内部占位的 SimpleActorSystem，经它创建的 Actor 在调用方线程同步执行、不受监督。
     */
    public func getActorSystem(): ActorSystem {
        RuntimeActorSystem(this)
    }

    private func generateActorName(): String {
        "auto-${nameSequence.fetchAdd(1)}"
    }

    private func getCurrentTimeNanos(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds() * 1000000
    }

    /**
     * 验证Actor创建参数
     */
    private func validateActorCreation(name: String, parent: Option<ActorRef>): Unit {
        if (name.isEmpty()) {
            throw Exception("Actor名称不能为空")
        }

        // 检查名称是否已存在。已终止的旧实例只是终止回调还没注销它（回调要等本锁）：名字视为可用，
        // 由用户 Guardian 的 createUserActor 代它释放（与 lookup 一致：lookup 已看不到已终止的 Actor）
        match (actorRegistry.findByName(name)) {
            case Some(existingActor) where !existingActor.isTerminated() =>
                throw Exception("Actor名称已存在: ${name}")
            case _ => ()  // 名称可用
        }
    }

    /**
     * 注册 CoordinatedShutdown 阶段任务
     */
    private func registerCoordinatedShutdownTasks(): Unit {
        let cs = coordinatedShutdown
        // 停止全部用户 Actor：与 Akka 一致，每个 Actor 执行 postStop、取消定时器
        cs.registerTask(CoordinatedShutdown.PhaseActorSystemTerminate, "terminate-user-actors", { =>
            userGuardian.terminateAll(Duration.second * 10)
        })
        // 最后释放运行时资源：此时已没有存活的用户 Actor
        cs.registerTask(CoordinatedShutdown.PhaseSystemTerminate, "runtime-resources", { =>
            highPerformanceSystem.shutdown()
            try {
                runtimeMailboxFactory.shutdown()
            } catch (e: Exception) {
                runtimeLog.warn("runtimeMailboxFactory.shutdown 失败: ${e.message}")
            }
            runtimeLog.info("CActorRuntime 已停止: ${systemName}")
        })
    }

    /**
     * 获取生命周期管理器
     */
    public func getLifecycleManager(): ActorLifecycleManager {
        lifecycleManager
    }

    /**
     * 获取Actor注册表
     */
    public func getActorRegistry(): SimpleActorRegistry {
        actorRegistry
    }

    /**
     * 获取监督管理器
     */
    public func getSupervisionManager(): SupervisionManager {
        supervisionManager
    }

    /**
     * 获取系统指标
     */
    public func getSystemMetrics(): ActorSystemMetrics {
        systemMetrics
    }

    /**
     * 获取事件总线
     */
    public func getEventBus(): SimpleActorEventBus {
        eventBus
    }
}

/**
 * CActorRuntime配置
 */
public struct CActorRuntimeConfig {
    public let mailboxConfig: MailboxConfig
    public let dispatcherConfig: DispatcherConfig
    public let highPerformanceConfig: HighPerformanceActorSystemConfig
    public let enableMonitoring: Bool
    public let enablePersistence: Bool
    /**
     * 定时器 / 接收超时 / 退避重启 / Actor 内 ask 超时使用的调度器（E52）。
     * 默认真实时间；测试注入 cactor_testkit.ManualTime 后这些路径只随虚拟时间触发
     */
    public let scheduler: ActorScheduler

    public init(
        mailboxConfig: MailboxConfig,
        dispatcherConfig: DispatcherConfig,
        highPerformanceConfig: HighPerformanceActorSystemConfig,
        enableMonitoring: Bool,
        enablePersistence: Bool,
        scheduler!: ActorScheduler = RealTimeScheduler.instance
    ) {
        this.mailboxConfig = mailboxConfig
        this.dispatcherConfig = dispatcherConfig
        this.highPerformanceConfig = highPerformanceConfig
        this.enableMonitoring = enableMonitoring
        this.enablePersistence = enablePersistence
        this.scheduler = scheduler
    }

    /** 换用另一个调度器（其余配置不变） */
    public func withScheduler(scheduler: ActorScheduler): CActorRuntimeConfig {
        CActorRuntimeConfig(mailboxConfig, dispatcherConfig, highPerformanceConfig, enableMonitoring, enablePersistence,
            scheduler: scheduler)
    }

    /**
     * 创建默认配置
     */
    public static func createDefault(): CActorRuntimeConfig {
        CActorRuntimeConfig(
            MailboxConfig.createUnbounded(),
            DispatcherConfig.createThreadPool(),
            HighPerformanceActorSystemConfig.createDefault(),
            true,  // 启用监控
            false  // 禁用持久化
        )
    }

    /**
     * 创建生产级配置
     */
    public static func createProduction(): CActorRuntimeConfig {
        CActorRuntimeConfig(
            MailboxConfig.createBounded(10000),
            DispatcherConfig.createWorkStealing(),
            HighPerformanceActorSystemConfig.createExtreme(),
            true,  // 启用监控
            true   // 启用持久化
        )
    }

    /**
     * 创建极限性能配置 - 基于plan2.md优化方案
     * 目标：从4,982 msg/s提升到1M msg/s (200x提升)
     */
    public static func createExtreme(): CActorRuntimeConfig {
        // 高性能邮箱配置
        let mailboxConfig = MailboxConfig.createUnbounded()

        // 工作窃取调度器配置
        let dispatcherConfig = DispatcherConfig.createWorkStealing()

        // 极限性能系统配置
        let highPerfConfig = HighPerformanceActorSystemConfig.createExtreme()

        return CActorRuntimeConfig(
            mailboxConfig,
            dispatcherConfig,
            highPerfConfig,
            true,  // 启用监控
            false  // 禁用持久化（性能优先）
        )
    }

    /**
     * 创建内存优化配置
     * 目标：<1KB/Actor内存使用
     */
    public static func createMemoryOptimized(): CActorRuntimeConfig {
        let mailboxConfig = MailboxConfig.createBounded(1000)
        let dispatcherConfig = DispatcherConfig.createThreadPool()
        let highPerfConfig = HighPerformanceActorSystemConfig.createMemoryOptimized()

        return CActorRuntimeConfig(
            mailboxConfig,
            dispatcherConfig,
            highPerfConfig,
            false, // 禁用监控（节省内存）
            false  // 禁用持久化
        )
    }

    /**
     * 创建低延迟配置
     * 目标：P99 < 100μs
     */
    public static func createLowLatency(): CActorRuntimeConfig {
        let mailboxConfig = MailboxConfig.createUnbounded()
        let dispatcherConfig = DispatcherConfig.createPinned()
        let highPerfConfig = HighPerformanceActorSystemConfig.createLowLatency()

        return CActorRuntimeConfig(
            mailboxConfig,
            dispatcherConfig,
            highPerfConfig,
            true,  // 启用监控（延迟统计）
            false  // 禁用持久化
        )
    }

    /**
     * 创建高吞吐量配置
     * 目标：50M msg/s
     */
    public static func createHighThroughput(): CActorRuntimeConfig {
        let mailboxConfig = MailboxConfig.createUnbounded()
        let dispatcherConfig = DispatcherConfig.createWorkStealing()
        let highPerfConfig = HighPerformanceActorSystemConfig.createHighThroughput()

        return CActorRuntimeConfig(
            mailboxConfig,
            dispatcherConfig,
            highPerfConfig,
            true,  // 启用监控（吞吐量统计）
            false  // 禁用持久化
        )
    }

    public func getDescription(): String {
        "CActorRuntimeConfig(monitoring=${enableMonitoring}, persistence=${enablePersistence})"
    }
}

/**
 * CActorRuntime统计信息
 */
public struct CActorRuntimeStats {
    public let systemName: String
    public let isRunning: Bool
    public let actorCount: Int64
    public let uptime: Int64
    public let systemGuardianStats: GuardianStats
    public let userGuardianStats: GuardianStats
    public let highPerformanceStats: SystemStats

    public init(
        systemName: String,
        isRunning: Bool,
        actorCount: Int64,
        uptime: Int64,
        systemGuardianStats: GuardianStats,
        userGuardianStats: GuardianStats,
        highPerformanceStats: SystemStats
    ) {
        this.systemName = systemName
        this.isRunning = isRunning
        this.actorCount = actorCount
        this.uptime = uptime
        this.systemGuardianStats = systemGuardianStats
        this.userGuardianStats = userGuardianStats
        this.highPerformanceStats = highPerformanceStats
    }

    public func printReport(): Unit {
        println("=== CActorRuntime统计报告 ===")
        println("系统名称: ${systemName}")
        let status = if (isRunning) { "运行中" } else { "已停止" }
        println("运行状态: ${status}")
        println("Actor数量: ${actorCount}")
        println("运行时长: ${getUptimeInSeconds()}秒")
        println("系统Guardian: ${systemGuardianStats.childCount}个子Actor")
        println("用户Guardian: ${userGuardianStats.childCount}个子Actor")
        println("高性能系统: ${highPerformanceStats.actorCount}个高性能Actor")
    }

    public func getUptimeInSeconds(): Float64 {
        Float64(uptime) / 1000000000.0
    }
}

/** Actor 中 context.system() 的运行时实现 */
class RuntimeSystemView <: ActorSystemView {
    private let runtime: CActorRuntime

    init(runtime: CActorRuntime) {
        this.runtime = runtime
    }

    public prop name: String {
        get() { runtime.getSystemName() }
    }

    public func lookup(path: String): Option<ActorRef> {
        runtime.lookup(path)
    }

    public func actorOf(props: Props<Actor>, name: String): ActorRef {
        runtime.actorOf(props, name)
    }

    public func stop(ref: ActorRef): Unit {
        runtime.stop(ref)
    }

    public func deadLetterCount(): Int64 {
        runtime.deadLetterCount()
    }

    public func terminate(): Unit {
        runtime.shutdown()
    }

    public func isTerminated(): Bool {
        !runtime.getRuntimeStats().isRunning
    }

    public func scheduler(): ActorScheduler {
        runtime.scheduler()
    }
}

func ignoreBool(_: Bool): Unit {}
