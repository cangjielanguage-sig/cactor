/*
 * CActor Patterns.Routing - 路由器 Actor（E37，对标 Akka Routers.pool / Routers.group）
 *
 * 路由器本身是一个 Actor：
 *   - 转发时保留原发送方（Akka forward）：目标 context.reply 直接回到原发送方，ask 经路由器也能往返；
 *   - watch 全部目标，目标终止后立即摘除，后续流量只分给存活目标；
 *     池路由器的目标全部终止后路由器自行停止（与 Akka 相同）；
 *   - 池路由器的目标是它的子 Actor，随路由器重启 / 停止一起重建 / 停止；
 *   - 管理消息：Broadcast、GetRoutees → Routees、AddRoutee、RemoveRoutee、AdjustPoolSize。
 *
 * 选目标的算法复用 RoutingStrategy（轮询、随机、最小邮箱、一致性哈希、加权轮询……）。
 * 所有状态只在路由器 Actor 的线程里读写，不需要锁。
 */
package cactor_patterns.routing

import std.collection.*
import cactor_core.message.{Message, MessageResult}
import cactor_core.actor.{Actor, ActorContext, ActorRef, Props, PropsFactory, Terminated}

// ---------------- 管理消息 ----------------

/** 发给全部目标（不经路由策略）；目标收到的是被包装的 message，发送方保留 */
public class Broadcast <: Message {
    public let message: Message

    public init(message: Message) {
        this.message = message
    }

    public func messageType(): String {
        "Broadcast(${message.messageType()})"
    }
}

/** 查询当前目标，回复 Routees */
public class GetRoutees <: Message {
    public init() {}

    public func messageType(): String {
        "GetRoutees"
    }
}

public class Routees <: Message {
    public let routees: Array<ActorRef>

    public init(routees: Array<ActorRef>) {
        this.routees = routees
    }

    public func messageType(): String {
        "Routees(${routees.size})"
    }
}

/** 增加一个目标（并 watch 它） */
public class AddRoutee <: Message {
    public let routee: ActorRef

    public init(routee: ActorRef) {
        this.routee = routee
    }

    public func messageType(): String {
        "AddRoutee"
    }
}

/** 移除一个目标；池路由器的目标是自己的子 Actor，移除时一并停止 */
public class RemoveRoutee <: Message {
    public let routee: ActorRef

    public init(routee: ActorRef) {
        this.routee = routee
    }

    public func messageType(): String {
        "RemoveRoutee"
    }
}

/** 池路由器增减目标数（正数新建、负数停止最后创建的几个）；组路由器忽略 */
public class AdjustPoolSize <: Message {
    public let change: Int64

    public init(change: Int64) {
        this.change = change
    }

    public func messageType(): String {
        "AdjustPoolSize(${change})"
    }
}

// ---------------- 路由器 Actor ----------------

/**
 * 路由器 Actor。通常不直接构造，用 Routers.pool / Routers.group 得到 Props
 */
public class RouterActor <: Actor {
    private let strategy: RoutingStrategy
    private let poolSize: Int64
    private let routeeProps: Option<Props<Actor>>
    private let initialGroup: Array<ActorRef>
    private let routees = ArrayList<ActorRef>()
    private var seq: Int64 = 0

    /** 池路由器：启动时创建 poolSize 个子 Actor 作为目标 */
    public init(strategy: RoutingStrategy, poolSize: Int64, routeeProps: Props<Actor>) {
        if (poolSize <= 0) {
            throw IllegalArgumentException("路由池大小必须为正数: ${poolSize}")
        }
        this.strategy = strategy
        this.poolSize = poolSize
        this.routeeProps = Some(routeeProps)
        this.initialGroup = Array<ActorRef>()
    }

    /** 组路由器：目标是已存在的 Actor，路由器只 watch 不管理它们的生命周期 */
    public init(strategy: RoutingStrategy, group: Array<ActorRef>) {
        this.strategy = strategy
        this.poolSize = 0
        this.routeeProps = None<Props<Actor>>
        this.initialGroup = group
    }

    public prop name: String {
        get() { "router" }
    }

    public prop description: String {
        get() { if (isPool()) { "pool-router" } else { "group-router" } }
    }

    private func isPool(): Bool {
        routeeProps.isSome()
    }

    public func onStarted(context: ActorContext): Unit {
        // 重启后由新实例重新建池：旧的子 Actor 已随重启停止
        routees.clear()
        if (let Some(props) <- routeeProps) {
            for (_ in 0..poolSize) {
                spawnRoutee(props, context)
            }
        } else {
            for (r in initialGroup) {
                addAndWatch(r, context)
            }
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case t: Terminated =>
                removeByRef(t.actor)
                // 池路由器的目标全部终止：路由器没有存在的意义，自行停止（Akka 相同）
                if (isPool() && routees.isEmpty()) {
                    context.stop(context.self)
                }
            case b: Broadcast =>
                let sender = context.senderRef()
                for (r in routees) {
                    forwardTo(r, b.message, sender)
                }
            case _: GetRoutees => context.reply(Routees(routees.toArray()))
            case a: AddRoutee => addAndWatch(a.routee, context)
            case r: RemoveRoutee =>
                if (removeByRef(r.routee)) {
                    unwatchQuietly(r.routee, context)
                    if (isPool()) {
                        context.stop(r.routee)
                    }
                }
            case adj: AdjustPoolSize => adjust(adj.change, context)
            case _ => return route(message, context)
        }
        MessageResult.Handled
    }

    private func route(message: Message, context: ActorContext): MessageResult {
        let sender = context.senderRef()
        let current = routees.toArray()
        match (strategy) {
            case _: BroadcastRoutingStrategy =>
                for (r in current) {
                    forwardTo(r, message, sender)
                }
                if (current.isEmpty()) {
                    MessageResult.Unhandled
                } else {
                    MessageResult.Handled
                }
            case _ =>
                match (strategy.selectRoutee(current, message)) {
                    case Some(r) =>
                        forwardTo(r, message, sender)
                        MessageResult.Handled
                    // 没有目标：计为未处理（可在指标里看到），而不是悄悄丢掉
                    case None => MessageResult.Unhandled
                }
        }
    }

    private func adjust(change: Int64, context: ActorContext): Unit {
        match (routeeProps) {
            case Some(props) =>
                if (change > 0) {
                    for (_ in 0..change) {
                        spawnRoutee(props, context)
                    }
                } else {
                    var n = -change
                    while (n > 0 && routees.size > 1) {
                        let last = routees.remove(at: routees.size - 1)
                        unwatchQuietly(last, context)
                        context.stop(last)
                        n--
                    }
                }
            case None => ()
        }
    }

    private func spawnRoutee(props: Props<Actor>, context: ActorContext): Unit {
        seq++
        let r = context.actorOf(props, "routee-${seq}")
        addAndWatch(r, context)
    }

    private func addAndWatch(r: ActorRef, context: ActorContext): Unit {
        for (existing in routees) {
            if (sameRoutee(existing, r)) {
                return
            }
        }
        routees.add(r)
        try {
            context.watch(r)
        } catch (e: Exception) {
            // 无法监视的引用（例如远程引用的本地代理）：仍然路由，只是不会自动摘除
            ()
        }
    }

    private func unwatchQuietly(r: ActorRef, context: ActorContext): Unit {
        try {
            context.unwatch(r)
        } catch (e: Exception) {
            ()
        }
    }

    private func removeByRef(r: ActorRef): Bool {
        let before = routees.size
        routees.removeIf({ x => sameRoutee(x, r) })
        routees.size != before
    }
}

func sameRoutee(a: ActorRef, b: ActorRef): Bool {
    a.equals(b) || a.path.toString() == b.path.toString()
}

// ---------------- 工厂 ----------------

/**
 * 创建路由器 Actor 的 Props（对标 Akka Routers）
 *
 * ```cangjie
 * let workers = system.actorOf(Routers.pool(4, PropsFactory.create<Actor>({ => Worker() })), "workers")
 * workers.tell(Job(1), self)        // Worker 里 context.reply(...) 直接回到 self
 * workers << Broadcast(Reload())    // 发给全部 Worker
 * ```
 */
public struct Routers {
    /** 池路由器（默认轮询） */
    public static func pool(poolSize: Int64, routeeProps: Props<Actor>): Props<Actor> {
        pool(poolSize, routeeProps, RoundRobinRoutingStrategy())
    }

    public static func pool(poolSize: Int64, routeeProps: Props<Actor>, strategy: RoutingStrategy): Props<Actor> {
        PropsFactory.create<Actor>({ => RouterActor(strategy, poolSize, routeeProps) })
    }

    public static func pool(poolSize: Int64, creator: () -> Actor, strategy: RoutingStrategy): Props<Actor> {
        pool(poolSize, PropsFactory.create<Actor>(creator), strategy)
    }

    /** 组路由器（默认轮询）：目标是已存在的 Actor */
    public static func group(routees: Array<ActorRef>): Props<Actor> {
        group(routees, RoundRobinRoutingStrategy())
    }

    public static func group(routees: Array<ActorRef>, strategy: RoutingStrategy): Props<Actor> {
        PropsFactory.create<Actor>({ => RouterActor(strategy, routees) })
    }
}
