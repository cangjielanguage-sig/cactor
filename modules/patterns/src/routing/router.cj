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
import cactor_foundation.hashing.{stableHash64, rendezvousIndex}

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
        // Lehmer 生成器的状态必须落在 [1, 2^31-2]：种子先折叠进该区间
        let ms = DateTime.now().toUnixTimeStamp().toMilliseconds()
        this.random = AtomicInt64((ms & 0x7FFFFFFF) % 2147483646 + 1)
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
            // CAS 的期望值必须是 load() 读到的原值（T1）：此前用掩码后的值做期望值，
            // 而初始种子是毫秒时间戳（> 2^31），期望值与实际值永远不等，首次调用即死循环。
            let prev = random.load()
            let seed = (prev & 0x7FFFFFFF) % 2147483647
            let state = if (seed == 0) { 1 } else { seed }
            let next = (state * 48271) % 2147483647
            if (random.compareAndSwap(prev, next)) {
                return next
            }
            // CAS 失败：其他线程已推进，下一轮读到新状态后重试（失败必然意味着别人成功，无活锁）
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
            // 已终止的目标邮箱计数恒为 0，比任何存活目标都"空"——必须跳过，否则流量全变死信
            if (routees[i].isTerminated()) {
                continue
            }
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
            // 无法查询负载：轮询存活目标（全部已终止时退回第一个，由死信统计暴露问题）
            let n = Int64(routees.size)
            let start = (counter.fetchAdd(1) & 0x7FFFFFFFFFFFFFFF) % n
            for (k in 0..n) {
                let r = routees[(start + k) % n]
                if (!r.isTerminated()) {
                    return Some(r)
                }
            }
            return Some(routees[start])
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

    /** 携带发送方广播：每个目标都能直接回复原发送方 */
    public func broadcast(routees: Array<ActorRef>, message: Message, sender: Option<ActorRef>): Unit {
        for (routee in routees) {
            forwardTo(routee, message, sender)
        }
    }
}

/**
 * 一致性哈希路由策略（对标 Akka ConsistentHashingRoutingLogic 的 hashMapping）
 *
 * - 路由键由 keyOf 从消息中提取（默认取 messageType，兼容旧行为；实际使用应传入业务键，如用户 id）；
 * - 用 Rendezvous（HRW）哈希选目标：同一键稳定落到同一目标，目标增减只迁移 1/n 的键。
 *   此前用 `hashCode % n`：hashCode 为负时下标为负直接越界，且目标数一变几乎全部键重新分布；
 * - 已终止的目标不参与选择（其余键的落点不受影响，这正是 HRW 的性质）。
 */
public class ConsistentHashRoutingStrategy <: RoutingStrategy {
    private let keyOf: (Message) -> String

    public init() {
        this({ m: Message => m.messageType() })
    }

    public init(keyOf: (Message) -> String) {
        this.keyOf = keyOf
    }

    public func selectRoutee(routees: Array<ActorRef>, message: Message): Option<ActorRef> {
        if (routees.size == 0) {
            return None
        }
        let alive = ArrayList<ActorRef>(routees.size)
        for (r in routees) {
            if (!r.isTerminated()) {
                alive.add(r)
            }
        }
        let pool = if (alive.isEmpty()) { routees } else { alive.toArray() }
        let candidates = Array<UInt64>(pool.size, { i => stableHash64(pool[i].path.toString()) })
        let index = rendezvousIndex(stableHash64(keyOf(message)), candidates)
        Some(pool[index])
    }
}

/**
 * 转发并保留原发送方（对标 Akka forward）：目标的 context.senderRef() / reply 指向原发送方而不是路由器。
 * 发送方为 None 时与普通 tell 相同
 */
public func forwardTo(routee: ActorRef, message: Message, sender: Option<ActorRef>): Unit {
    match (sender) {
        case Some(s) => routee.tell(message, s)
        case None => routee.tell(message)
    }
}

/**
 * 路由器接口（不是 Actor 的路由助手，对标 Akka akka.routing.Router）。
 * 需要监视目标、自动摘除已终止目标、按池创建目标时，用 Routers.pool / Routers.group 创建路由器 Actor
 */
public interface Router {
    /**
     * 路由消息
     */
    func route(message: Message): Unit

    /**
     * 携带发送方路由（E37）：选中的目标回复时直接回到 sender。默认实现丢弃发送方，实现类应覆盖
     */
    func route(message: Message, sender: Option<ActorRef>): Unit {
        route(message)
    }

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
        route(message, None<ActorRef>)
    }

    public func route(message: Message, sender: Option<ActorRef>): Unit {
        let routeeArray = synchronized(mutex) {
            routees.toArray()
        }
        // 特殊处理广播策略
        match (strategy) {
            case broadcast: BroadcastRoutingStrategy => broadcast.broadcast(routeeArray, message, sender)
            case _ =>
                match (strategy.selectRoutee(routeeArray, message)) {
                    case Some(routee) => forwardTo(routee, message, sender)
                    case None => log_router.warn("警告: 没有可用的路由目标")
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
        // 只有发送方路径、没有引用时无法回程：按普通 tell 路由。需要回程请用 tell(message, senderRef)
        router.route(message)
    }

    /** 透传发送方引用（E37，C12）：选中的目标 context.reply 直接回到原发送方 */
    public func tell(message: Message, sender: ActorRef): Unit {
        router.route(message, Some(sender))
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
