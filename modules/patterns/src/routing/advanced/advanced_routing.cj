/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 高级路由系统
 * 基于plan3.md Phase 6设计，增强现有的路由功能
 */
package cactor_patterns.routing.advanced

import std.time.*
import std.collection.*
import std.sync.*
import cactor_core.message.{Message}
import cactor_core.actor.ActorRef
import cactor_patterns.routing.{
    RoutingStrategy, 
    Router, 
    BasicRouter,
    RoundRobinRoutingStrategy,
    RandomRoutingStrategy,
    ConsistentHashRoutingStrategy,
    BroadcastRoutingStrategy,
    RouterFactory,
    forwardTo
}
import cactor_foundation.logging.{Logger, Logging}
import cactor_foundation.hashing.{stableHash64, rendezvousIndex}

let log_advanced_routing: Logger = Logging.logger("cactor_patterns.routing.advanced.advanced_routing")

/**
 * 高级路由策略工厂
 * 提供更多预定义的路由策略和自定义选项
 */
public class AdvancedRoutingStrategyFactory {
    
    /**
     * 创建加权轮询路由策略
     * 根据权重分配消息
     */
    public static func createWeightedRoundRobin(weights: Array<Int32>): WeightedRoundRobinRoutingStrategy {
        WeightedRoundRobinRoutingStrategy(weights)
    }
    
    /**
     * 创建故障转移路由策略
     * 主Actor失败时自动切换到备用Actor
     */
    public static func createFailover(primaryIndex: Int32): FailoverRoutingStrategy {
        FailoverRoutingStrategy(primaryIndex)
    }
    
    /**
     * 创建基于内容的路由策略
     * 根据消息内容选择Actor
     */
    public static func createContentBased(selector: (Message) -> String): ContentBasedRoutingStrategy {
        ContentBasedRoutingStrategy(selector)
    }
}

/**
 * 加权轮询路由策略
 * 根据权重分配消息，权重高的Actor处理更多消息
 */
public class WeightedRoundRobinRoutingStrategy <: RoutingStrategy {
    private let weights: Array<Int32>
    // 平滑加权轮询（nginx smooth WRR）的当前权重；按 routees 下标对齐，长度随 routees 变化
    private let current = ArrayList<Int64>()
    private let mutex = Mutex()

    /**
     * weights[i] 是第 i 个 routee 的权重；routee 多于权重时，多出的按权重 1 参与（不会被排除），
     * 权重多于 routee 时多余权重忽略。负权重按 0 处理（不被选中）。
     */
    public init(weights: Array<Int32>) {
        this.weights = weights
    }

    private func weightOf(i: Int64): Int64 {
        if (i < weights.size) {
            let w = Int64(weights[i])
            if (w > 0) { w } else { 0 }
        } else {
            1
        }
    }

    /**
     * 平滑加权轮询：每次给每个存活目标加上自身权重，选当前值最大的，再从它身上减去总权重。
     * 结果序列在任意窗口内都按权重比例交错（权重 5:1:1 时为 a a b a c a a，而不是 a×5 连发），
     * 且只要存在正权重就必然选出一个目标（E25：此前 routees 少于 weights 时权重和失衡，永久返回 None）。
     */
    public func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef> {
        if (routees.size == 0) {
            return None
        }
        synchronized(mutex) {
            while (current.size < routees.size) {
                current.add(0)
            }
            while (current.size > routees.size) {
                current.remove(at: current.size - 1)
            }
            var total: Int64 = 0
            var best: Int64 = -1
            for (i in 0..routees.size) {
                if (routees[i].isTerminated()) {
                    continue
                }
                let w = weightOf(i)
                if (w == 0) {
                    continue
                }
                current[i] += w
                total += w
                if (best < 0 || current[i] > current[best]) {
                    best = i
                }
            }
            if (best < 0) {
                // 没有正权重的存活目标：退回第一个（由死信统计暴露配置问题），不返回 None 丢消息
                return Some(routees[0])
            }
            current[best] -= total
            Some(routees[best])
        }
    }
}

// LeastConnections / FastestResponse 已删除（E37）：两者依赖调用方手动回报"处理完成"和"响应耗时"，
// 没有任何调用方这样做——LeastConnections 的计数只增不减，退化为一直选同一个；FastestResponse 永远按默认 100ms 比较。
// 需要按负载分配请用 SmallestMailboxRoutingStrategy（真实邮箱长度），或 Routers.pool 路由器 Actor。

/**
 * 故障转移路由策略
 * 主Actor失败时自动切换到备用Actor
 */
public class FailoverRoutingStrategy <: RoutingStrategy {
    private let primaryIndex: Int32
    private let failedActors: HashSet<String>
    private let mutex: Mutex
    
    public init(primaryIndex: Int32) {
        this.primaryIndex = primaryIndex
        this.failedActors = HashSet<String>()
        this.mutex = Mutex()
    }
    
    public func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef> {
        if (routees.size == 0) {
            return None
        }
        
        mutex.lock()
        try {
            // 首先尝试主Actor
            if (primaryIndex >= 0 && Int64(primaryIndex) < routees.size) {
                let primaryActor = routees[Int64(primaryIndex)]
                let primaryKey = primaryActor.path.toString()
                
                if (!failedActors.contains(primaryKey)) {
                    return Some(primaryActor)
                }
            }
            
            // 主Actor失败，选择第一个可用的备用Actor
            for (i in 0..routees.size) {
                if (Int32(i) != primaryIndex) {
                    let actor = routees[i]
                    let actorKey = actor.path.toString()
                    
                    if (!failedActors.contains(actorKey)) {
                        return Some(actor)
                    }
                }
            }
            
            return None
        } finally {
            mutex.unlock()
        }
    }
    
    /**
     * 标记Actor为失败状态
     */
    public func markActorAsFailed(actor: ActorRef): Unit {
        mutex.lock()
        try {
            failedActors.add(actor.path.toString())
        } finally {
            mutex.unlock()
        }
    }
    
    /**
     * 恢复Actor（从失败状态中移除）
     */
    public func recoverActor(actor: ActorRef): Unit {
        mutex.lock()
        try {
            failedActors.remove(actor.path.toString())
        } finally {
            mutex.unlock()
        }
    }
}

/**
 * 基于内容的路由策略
 * 根据消息内容选择Actor
 */
public class ContentBasedRoutingStrategy <: RoutingStrategy {
    private let contentSelector: (Message) -> String
    private let routingTable: HashMap<String, Int32>
    private let mutex: Mutex
    
    public init(contentSelector: (Message) -> String) {
        this.contentSelector = contentSelector
        this.routingTable = HashMap<String, Int32>()
        this.mutex = Mutex()
    }
    
    public func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef> {
        if (routees.size == 0) {
            return None
        }
        
        let content = contentSelector(message)
        
        mutex.lock()
        try {
            // 检查是否有预定义的路由规则
            match (routingTable.get(content)) {
                case Some(index) =>
                    if (index >= 0 && Int64(index) < routees.size) {
                        return Some(routees[Int64(index)])
                    }
                case None => ()
            }

            // 没有预定义规则：按内容做 Rendezvous 哈希（稳定、非负下标，目标增减只迁移 1/n 的内容键）
            let candidates = Array<UInt64>(routees.size, { i => stableHash64(routees[i].path.toString()) })
            return Some(routees[rendezvousIndex(stableHash64(content), candidates)])
        } finally {
            mutex.unlock()
        }
    }
    
    /**
     * 添加路由规则
     */
    public func addRoutingRule(content: String, actorIndex: Int32): Unit {
        mutex.lock()
        try {
            routingTable.add(content, actorIndex)
        } finally {
            mutex.unlock()
        }
    }
    
    /**
     * 移除路由规则
     */
    public func removeRoutingRule(content: String): Unit {
        mutex.lock()
        try {
            routingTable.remove(content)
        } finally {
            mutex.unlock()
        }
    }
}

/**
 * 高级路由器
 * 提供更丰富的路由功能和监控
 */
public class AdvancedRouter <: Router {
    private let strategy: RoutingStrategy
    private let routees: ArrayList<ActorRef>
    private let mutex: Mutex
    private let routingMetrics: RoutingMetrics
    private let healthChecker: RouterHealthChecker

    public init(strategy: RoutingStrategy) {
        this.strategy = strategy
        this.routees = ArrayList<ActorRef>()
        this.mutex = Mutex()
        this.routingMetrics = RoutingMetrics()
        this.healthChecker = RouterHealthChecker()
    }

    public func route(message: Message): Unit {
        route(message, None<ActorRef>)
    }

    /** 携带发送方路由（E37）：目标用 context.reply 回复时直接回到原发送方 */
    public func route(message: Message, sender: Option<ActorRef>): Unit {
        mutex.lock()
        let routeeArray = routees.toArray()
        mutex.unlock()

        // 过滤健康的Actor
        let healthyRoutees = healthChecker.filterHealthyActors(routeeArray)

        if (healthyRoutees.size == 0) {
            routingMetrics.recordFailedRoute("no_healthy_routees")
            log_advanced_routing.warn("警告: 没有健康的路由目标")
            return
        }

        // 特殊处理广播策略
        match (strategy) {
            case _: BroadcastRoutingStrategy =>
                for (r in healthyRoutees) {
                    forwardTo(r, message, sender)
                }
                routingMetrics.recordSuccessfulRoute(Int32(healthyRoutees.size))
            case _ =>
                match (strategy.selectRoutee(healthyRoutees, message)) {
                    case Some(routee) =>
                        forwardTo(routee, message, sender)
                        routingMetrics.recordSuccessfulRoute(1)
                    case None =>
                        routingMetrics.recordFailedRoute("no_routee_selected")
                        log_advanced_routing.warn("警告: 没有选中路由目标")
                }
        }
    }

    public func addRoutee(routee: ActorRef): Unit {
        mutex.lock()
        try {
            routees.add(routee)
            healthChecker.addActor(routee)
            routingMetrics.recordRouteeAdded()
        } finally {
            mutex.unlock()
        }
    }

    public func removeRoutee(routee: ActorRef): Unit {
        mutex.lock()
        try {
            // 简化实现：遍历查找并移除
            var index = -1
            for (i in 0..routees.size) {
                if (routees[i].equals(routee)) {
                    index = i
                    break
                }
            }
            if (index >= 0) {
                routees.remove(index..index+1)
                healthChecker.removeActor(routee)
                routingMetrics.recordRouteeRemoved()
            }
        } finally {
            mutex.unlock()
        }
    }

    public func getRoutees(): Array<ActorRef> {
        mutex.lock()
        try {
            routees.toArray()
        } finally {
            mutex.unlock()
        }
    }

    public func getStrategy(): RoutingStrategy {
        strategy
    }

    /**
     * 获取路由指标
     */
    public func getRoutingMetrics(): RoutingMetrics {
        routingMetrics
    }

    /**
     * 获取健康检查器
     */
    public func getHealthChecker(): RouterHealthChecker {
        healthChecker
    }
}

/**
 * 路由指标
 * 收集和统计路由相关的指标
 */
public class RoutingMetrics {
    private let totalRoutes: AtomicInt64
    private let successfulRoutes: AtomicInt64
    private let failedRoutes: AtomicInt64
    private let routeesAdded: AtomicInt64
    private let routeesRemoved: AtomicInt64
    private let failureReasons: HashMap<String, AtomicInt64>
    private let startTime: Int64
    private let mutex: Mutex

    public init() {
        this.totalRoutes = AtomicInt64(0)
        this.successfulRoutes = AtomicInt64(0)
        this.failedRoutes = AtomicInt64(0)
        this.routeesAdded = AtomicInt64(0)
        this.routeesRemoved = AtomicInt64(0)
        this.failureReasons = HashMap<String, AtomicInt64>()
        this.startTime = DateTime.now().toUnixTimeStamp().toMilliseconds()
        this.mutex = Mutex()
    }

    public func recordSuccessfulRoute(routeeCount: Int32): Unit {
        totalRoutes.fetchAdd(1)
        successfulRoutes.fetchAdd(1)
    }

    public func recordFailedRoute(reason: String): Unit {
        totalRoutes.fetchAdd(1)
        failedRoutes.fetchAdd(1)

        mutex.lock()
        try {
            let currentCount = match (failureReasons.get(reason)) {
                case Some(counter) => counter.load()
                case None => 0
            }
            failureReasons.add(reason, AtomicInt64(currentCount + 1))
        } finally {
            mutex.unlock()
        }
    }

    public func recordRouteeAdded(): Unit {
        routeesAdded.fetchAdd(1)
    }

    public func recordRouteeRemoved(): Unit {
        routeesRemoved.fetchAdd(1)
    }

    public func getTotalRoutes(): Int64 {
        totalRoutes.load()
    }

    public func getSuccessfulRoutes(): Int64 {
        successfulRoutes.load()
    }

    public func getFailedRoutes(): Int64 {
        failedRoutes.load()
    }

    public func getSuccessRate(): Float64 {
        let total = totalRoutes.load()
        if (total == 0) {
            return 1.0
        }
        Float64(successfulRoutes.load()) / Float64(total)
    }

    public func getRouteRate(): Float64 {
        let now = DateTime.now().toUnixTimeStamp().toMilliseconds()
        let elapsedSeconds = Float64(now - startTime) / 1000.0

        if (elapsedSeconds > 0.0) {
            Float64(totalRoutes.load()) / elapsedSeconds
        } else {
            0.0
        }
    }

    public func getMetricsSummary(): String {
        "RoutingMetrics[" +
        "总路由: ${getTotalRoutes()}, " +
        "成功: ${getSuccessfulRoutes()}, " +
        "失败: ${getFailedRoutes()}, " +
        "成功率: ${getSuccessRate()}, " +
        "路由率: ${getRouteRate()}/s, " +
        "添加: ${routeesAdded.load()}, " +
        "移除: ${routeesRemoved.load()}]"
    }
}

/**
 * 路由器健康检查器
 * 监控Actor的健康状态
 */
public class RouterHealthChecker {
    private let actorHealth: HashMap<String, ActorHealthStatus>
    private let mutex: Mutex
    private let healthCheckInterval: Duration

    public init() {
        this.actorHealth = HashMap<String, ActorHealthStatus>()
        this.mutex = Mutex()
        this.healthCheckInterval = Duration.second * 30  // 30秒检查一次
    }

    public func addActor(actor: ActorRef): Unit {
        mutex.lock()
        try {
            let actorKey = actor.path.toString()
            actorHealth.add(actorKey, ActorHealthStatus())
        } finally {
            mutex.unlock()
        }
    }

    public func removeActor(actor: ActorRef): Unit {
        mutex.lock()
        try {
            let actorKey = actor.path.toString()
            actorHealth.remove(actorKey)
        } finally {
            mutex.unlock()
        }
    }

    public func markActorUnhealthy(actor: ActorRef, reason: String): Unit {
        mutex.lock()
        try {
            let actorKey = actor.path.toString()
            match (actorHealth.get(actorKey)) {
                case Some(status) =>
                    status.markUnhealthy(reason)
                case None => ()
            }
        } finally {
            mutex.unlock()
        }
    }

    public func markActorHealthy(actor: ActorRef): Unit {
        mutex.lock()
        try {
            let actorKey = actor.path.toString()
            match (actorHealth.get(actorKey)) {
                case Some(status) =>
                    status.markHealthy()
                case None => ()
            }
        } finally {
            mutex.unlock()
        }
    }

    public func filterHealthyActors(actors: Array<ActorRef>): Array<ActorRef> {
        mutex.lock()
        try {
            let healthyActors = ArrayList<ActorRef>()

            for (actor in actors) {
                let actorKey = actor.path.toString()
                match (actorHealth.get(actorKey)) {
                    case Some(status) =>
                        if (status.isHealthy()) {
                            healthyActors.add(actor)
                        }
                    case None =>
                        // 未知状态的Actor默认为健康
                        healthyActors.add(actor)
                }
            }

            Array<ActorRef>(Int64(healthyActors.size), { i => healthyActors[i] })
        } finally {
            mutex.unlock()
        }
    }
}

/**
 * Actor健康状态
 */
public class ActorHealthStatus {
    private let isHealthyFlag: AtomicBool
    private let lastFailureTime: AtomicInt64
    private let failureCount: AtomicInt32
    private let lastFailureReason: String

    public init() {
        this.isHealthyFlag = AtomicBool(true)
        this.lastFailureTime = AtomicInt64(0)
        this.failureCount = AtomicInt32(0)
        this.lastFailureReason = ""
    }

    public func isHealthy(): Bool {
        isHealthyFlag.load()
    }

    public func markHealthy(): Unit {
        isHealthyFlag.store(true)
        failureCount.store(0)
    }

    public func markUnhealthy(reason: String): Unit {
        isHealthyFlag.store(false)
        lastFailureTime.store(DateTime.now().toUnixTimeStamp().toMilliseconds())
        failureCount.fetchAdd(1)
    }

    public func getFailureCount(): Int32 {
        failureCount.load()
    }

    public func getLastFailureTime(): Int64 {
        lastFailureTime.load()
    }
}
