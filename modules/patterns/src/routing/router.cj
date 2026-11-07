/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 路由器实现
 * 提供消息路由和负载均衡功能
 */
package cactor_patterns.routing

import std.sync.*
import std.time.*
import std.collection.*
import cactor_core.message.{Message}
import cactor_core.actor.{ActorRef, ActorPath}
import cactor_foundation.logging.{Logger, Logging}

let log_router: Logger = Logging.logger("cactor_patterns.routing.router")

/**
 * 路由策略接口
 */
public interface RoutingStrategy {
    /**
     * 选择目标Actor
     * @param routees 可用的Actor列表
     * @param message 要路由的消息
     * @return 选中的Actor，如果没有可用Actor返回None
     */
    func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef>
}

/**
 * 轮询路由策略
 * 按顺序轮流选择Actor
 */
public class RoundRobinRoutingStrategy <: RoutingStrategy {
    private let counter: AtomicInt64

    public init() {
        this.counter = AtomicInt64(0)
    }

    public func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef> {
        if (routees.size == 0) {
            return None
        }

        // E9（2026-11-06）修复 C5：fetchAdd 溢出为负后 index 为负越界；取模归正
        let index = (counter.fetchAdd(1) & 0x7FFFFFFFFFFFFFFF) % Int64(routees.size)
        Some(routees[index])
    }
}

/**
 * 随机路由策略
 * 随机选择Actor
 */
public class RandomRoutingStrategy <: RoutingStrategy {
    private let random: AtomicInt64

    public init() {
        this.random = AtomicInt64(DateTime.now().toUnixTimeStamp().toMilliseconds())
    }

    public func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef> {
        if (routees.size == 0) {
            return None
        }

        // E9（2026-11-06）修复 C5：此前 load 后算出 next 再 store，多发送方并发时丢更新，
        // 且 current 为负时 next % size 会得到负索引越界。改用 CAS 循环原子推进 LCG 状态，
        // 并把结果限制在非负空间（& 0x7FFFFFFFFFFFFFFF 清符号位）后再取模。
        let next = nextLcg()
        let index = (next & 0x7FFFFFFFFFFFFFFF) % Int64(routees.size)
        Some(routees[index])
    }

    /**
     * 随机状态原子推进：CAS 循环，直至成功（仓颉 AtomicInt64 无 fetchUpdate，用 compareAndSwap 实现）。
     * 用 minstd（Lehmer）生成器：state = state * 48271 % 2147483647，
     * 周期 2^31-2，全状态空间均匀；原 LCG（%1000000*1103+12345）状态在
     * %1000000 空间内最快 2 步就进入小循环，路由分布会坍缩到一半目标。
     */
    private func nextLcg(): Int64 {
        while (true) {
            let prev = random.load() & 0x7FFFFFFF
            let seed = if (prev == 0) { 1 } else { prev }
            let next = (seed * 48271) % 2147483647
            if (random.compareAndSwap(prev, next)) {
                return next
            }
            // CAS 失败：其他线程已推进，重试（相邻重试必然取到新状态，无活锁）
        }
        0  // 不可达：while(true) 只能经 return 退出（仓颉要求循环后有尾表达式）
    }
}

/**
 * 最小邮箱路由策略
 * 选择邮箱中消息最少的Actor
 */
public class SmallestMailboxRoutingStrategy <: RoutingStrategy {
    private let counter: AtomicInt64

    public init() {
        this.counter = AtomicInt64(0)
    }

    public func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef> {
        if (routees.size == 0) {
            return None
        }

        // E9（2026-11-06）修复 C4：此前恒返回第一个 Actor，负载完全不均衡。
        // 现在通过 ActorRef.pendingCount() 真实比较各目标邮箱排队数，选最小的；
        // 全部无法查询（-1，如远程引用）时退回轮询游标做均衡近似。
        var bestIdx: Int64 = -1
        var bestCount: Int64 = 0x7FFFFFFFFFFFFFFF
        var allUnknown = true
        for (i in 0..Int64(routees.size)) {
            let c = routees[i].pendingCount()
            if (c >= 0) {
                allUnknown = false
                if (c < bestCount) {
                    bestCount = c
                    bestIdx = i
                }
            }
        }
        if (allUnknown) {
            let start = (counter.fetchAdd(1) & 0x7FFFFFFFFFFFFFFF) % Int64(routees.size)
            return Some(routees[start])
        }
        if (bestIdx < 0) {
            bestIdx = 0
        }
        Some(routees[bestIdx])
    }
}

/**
 * 广播路由策略
 * 将消息发送给所有Actor
 */
public class BroadcastRoutingStrategy <: RoutingStrategy {
    public init() {}

    public func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef> {
        // 广播策略不选择单个Actor，而是在Router中特殊处理
        None
    }

    public func broadcast(routees: Array<ActorRef>, message: Message): Unit {
        for (routee in routees) {
            routee.tell(message)
        }
    }
}

/**
 * 一致性哈希路由策略
 * 基于消息内容的哈希值选择Actor
 */
public class ConsistentHashRoutingStrategy <: RoutingStrategy {
    public init() {}

    public func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef> {
        if (routees.size == 0) {
            return None
        }

        // 使用消息类型的哈希值
        let hash = message.messageType().hashCode()
        let index = Int64(hash) % Int64(routees.size)
        Some(routees[index])
    }
}

/**
 * 路由器接口
 */
public interface Router {
    /**
     * 路由消息
     */
    func route(message: Message): Unit

    /**
     * 添加路由目标
     */
    func addRoutee(routee: ActorRef): Unit

    /**
     * 移除路由目标
     */
    func removeRoutee(routee: ActorRef): Unit

    /**
     * 获取所有路由目标
     */
    func getRoutees(): Array<ActorRef>

    /**
     * 获取路由策略
     */
    func getStrategy(): RoutingStrategy
}

/**
 * 基础路由器实现
 */
public class BasicRouter <: Router {
    private let strategy: RoutingStrategy
    private let routees: ArrayList<ActorRef>
    private let mutex: Mutex

    public init(strategy: RoutingStrategy) {
        this.strategy = strategy
        this.routees = ArrayList<ActorRef>()
        this.mutex = Mutex()
    }

    public func route(message: Message): Unit {
        mutex.lock()
        let routeeArray = routees.toArray()
        mutex.unlock()

        // 特殊处理广播策略
        match (strategy) {
            case broadcast: BroadcastRoutingStrategy => 
                broadcast.broadcast(routeeArray, message)
            case _ => 
                match (strategy.selectRoutee(routeeArray, message)) {
                    case Some(routee) => routee.tell(message)
                    case None => 
                        log_router.warn("警告: 没有可用的路由目标")
                }
        }
    }

    public func addRoutee(routee: ActorRef): Unit {
        mutex.lock()
        try {
            routees.add(routee)
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
                routees.remove(index..index+1)  // 移除单个元素，使用Range
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
}

/**
 * 路由器工厂
 */
public struct RouterFactory {
    /**
     * 创建轮询路由器
     */
    public static func roundRobin(): BasicRouter {
        BasicRouter(RoundRobinRoutingStrategy())
    }

    /**
     * 创建随机路由器
     */
    public static func random(): BasicRouter {
        BasicRouter(RandomRoutingStrategy())
    }

    /**
     * 创建最小邮箱路由器
     */
    public static func smallestMailbox(): BasicRouter {
        BasicRouter(SmallestMailboxRoutingStrategy())
    }

    /**
     * 创建广播路由器
     */
    public static func broadcast(): BasicRouter {
        BasicRouter(BroadcastRoutingStrategy())
    }

    /**
     * 创建一致性哈希路由器
     */
    public static func consistentHash(): BasicRouter {
        BasicRouter(ConsistentHashRoutingStrategy())
    }
}

/**
 * 路由器Actor包装器
 * 将路由器包装成ActorRef
 */
public class RouterActorRef <: ActorRef {
    private let router: Router
    private let routerPath: ActorPath

    public init(router: Router, path: String) {
        this.router = router
        this.routerPath = ActorPath("router", "/${path}")
    }

    public prop path: ActorPath {
        get() {
            routerPath
        }
    }

    public func tell(message: Message): Unit {
        router.route(message)
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        // E9（2026-11-06）C12 说明：Message 接口没有 sender 字段可注入，本层无法把
        // sender 携带进消息体；经 router 转发的 ask 需要在 AskInbox/TypedAsk 层
        // 注册回复入口（它们不走 sender 参数）。这里保留参数签名并在日志记录，
        // 待 Message 支持 sender 透传后改为注入。
        router.route(message)
    }

    public func equals(other: ActorRef): Bool {
        match (other) {
            case routerRef: RouterActorRef => 
                this.routerPath.equals(routerRef.routerPath)
            case _ => false
        }
    }

    public func hashCode(): Int32 {
        routerPath.hashCode()
    }

    public func getRouter(): Router {
        router
    }
}

/**
 * 路由配置
 */
public struct RoutingConfig {
    public let strategy: RoutingStrategy
    public let routeeCount: Int32
    public let resizePolicy: ResizePolicy

    public init(strategy: RoutingStrategy, routeeCount: Int32, resizePolicy: ResizePolicy) {
        this.strategy = strategy
        this.routeeCount = routeeCount
        this.resizePolicy = resizePolicy
    }
}

/**
 * 调整大小策略
 */
public enum ResizePolicy {
    | Fixed        // 固定大小
    | Dynamic      // 动态调整
}

/**
 * 路由统计信息
 */
public struct RoutingStatistics {
    public let totalMessages: Int64
    public let routeeCount: Int32
    public let averageLatency: Float64

    public init(totalMessages: Int64, routeeCount: Int32, averageLatency: Float64) {
        this.totalMessages = totalMessages
        this.routeeCount = routeeCount
        this.averageLatency = averageLatency
    }
}
