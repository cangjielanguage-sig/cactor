/*
 * CActor Core.Typed - 类型化路由器（E37，对标 Akka Typed Routers.pool / Routers.group）
 *
 * 路由器是一个普通的类型化行为，协议与目标相同（TypedBehavior<T>），可以直接 spawn：
 *
 * ```cangjie
 * // 池：启动 4 个 worker 子 Actor，按用户 id 一致性哈希；Reload 广播给全部 worker
 * let pool = TypedRouters.pool<Job>(4, worker(),
 *     TypedRouting<Job>().consistentHashing({ j => j.userId }).withBroadcastPredicate({ j => j.isReload }))
 * let workers = ctx.actorOf(pool, "workers")
 *
 * // 组：路由到 Receptionist 中登记为 key 的全部实例，实例增减自动生效
 * let group = ctx.actorOf(TypedRouters.group<Job>(WorkerKey), "worker-group")
 * ```
 *
 * 语义：
 *   - 池路由器 watch 每个目标，目标终止后摘除，后续流量只分给存活目标；全部终止后路由器停止（与 Akka 相同）；
 *   - 组路由器订阅 Receptionist，列表变化（含实例停止后的自动注销）立即生效，路由器停止时退订；
 *   - 没有目标时消息记为未处理；
 *   - 类型化消息自带 replyTo，路由器只需 tell，回复不经过路由器。
 */
package cactor_core.typed

import std.sync.*
import std.collection.*
import std.random.*
import cactor_foundation.hashing.{stableHash64, rendezvousIndex}

let ROUTE_ROUND_ROBIN: Int64 = 0
let ROUTE_RANDOM: Int64 = 1
let ROUTE_CONSISTENT_HASH: Int64 = 2

/**
 * 类型化路由配置：不可变，每个 with/选择方法返回新配置
 */
public class TypedRouting<T> {
    let mode: Int64
    let keyOf: Option<(T) -> String>
    let broadcastWhen: Option<(T) -> Bool>
    let routeeProps: Option<TypedProps>

    /** 默认轮询 */
    public init() {
        this(ROUTE_ROUND_ROBIN, None<(T) -> String>, None<(T) -> Bool>, None<TypedProps>)
    }

    init(mode: Int64, keyOf: Option<(T) -> String>, broadcastWhen: Option<(T) -> Bool>, routeeProps: Option<TypedProps>) {
        this.mode = mode
        this.keyOf = keyOf
        this.broadcastWhen = broadcastWhen
        this.routeeProps = routeeProps
    }

    /** 轮询 */
    public func roundRobin(): TypedRouting<T> {
        TypedRouting<T>(ROUTE_ROUND_ROBIN, None<(T) -> String>, broadcastWhen, routeeProps)
    }

    /** 随机 */
    public func random(): TypedRouting<T> {
        TypedRouting<T>(ROUTE_RANDOM, None<(T) -> String>, broadcastWhen, routeeProps)
    }

    /**
     * 一致性哈希（Rendezvous）：同一个键稳定落到同一个目标；目标增减时只有落在被删目标上的键迁移
     */
    public func consistentHashing(keyOf: (T) -> String): TypedRouting<T> {
        TypedRouting<T>(ROUTE_CONSISTENT_HASH, Some(keyOf), broadcastWhen, routeeProps)
    }

    /** 满足谓词的消息发给全部目标（例如配置刷新），其余照常路由 */
    public func withBroadcastPredicate(predicate: (T) -> Bool): TypedRouting<T> {
        TypedRouting<T>(mode, keyOf, Some(predicate), routeeProps)
    }

    /** 池路由器创建目标时使用的 TypedProps（调度器、邮箱等）；组路由器忽略 */
    public func withRouteeProps(props: TypedProps): TypedRouting<T> {
        TypedRouting<T>(mode, keyOf, broadcastWhen, Some(props))
    }
}

/**
 * 路由目标表。池路由器只在路由器线程里改；组路由器的列表由 Receptionist 回调在任意线程整体替换，
 * 因此用不可变数组 + 锁内替换，路由时取快照
 */
class RouteeTable<T> {
    private let lock = Mutex()
    private var refs: Array<TypedActorRef<T>> = Array<TypedActorRef<T>>()

    func snapshot(): Array<TypedActorRef<T>> {
        synchronized(lock) {
            refs
        }
    }

    func replace(next: Array<TypedActorRef<T>>): Unit {
        synchronized(lock) {
            refs = next
        }
    }

    func add(r: TypedActorRef<T>): Unit {
        synchronized(lock) {
            let cur = refs
            refs = Array<TypedActorRef<T>>(cur.size + 1, { i => if (i < cur.size) { cur[i] } else { r } })
        }
    }

    /** 按路径摘除；返回摘除后是否为空 */
    func removePath(path: String): Bool {
        synchronized(lock) {
            let kept = ArrayList<TypedActorRef<T>>(refs.size)
            for (r in refs) {
                if (r.pathString() != path) {
                    kept.add(r)
                }
            }
            refs = kept.toArray()
            refs.isEmpty()
        }
    }
}

class TypedRouterBehavior<T> <: TypedBehavior<T> {
    private let routing: TypedRouting<T>
    private let table: RouteeTable<T>
    private let pool: Bool
    private let onStop: Option<() -> Unit>
    // 以下只在路由器 Actor 的线程里访问
    private var cursor: Int64 = 0
    private var rnd: Option<Random> = None<Random>

    init(routing: TypedRouting<T>, table: RouteeTable<T>, pool: Bool, onStop: Option<() -> Unit>) {
        this.routing = routing
        this.table = table
        this.pool = pool
        this.onStop = onStop
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        let refs = table.snapshot()
        if (refs.isEmpty()) {
            return TypedBehaviors.unhandled<T>()
        }
        if (let Some(p) <- routing.broadcastWhen) {
            if (p(message)) {
                for (r in refs) {
                    r.tell(message)
                }
                return TypedBehaviors.same<T>()
            }
        }
        refs[select(refs, message)].tell(message)
        TypedBehaviors.same<T>()
    }

    private func select(refs: Array<TypedActorRef<T>>, message: T): Int64 {
        if (routing.mode == ROUTE_RANDOM) {
            let r = match (rnd) {
                case Some(x) => x
                case None =>
                    let x = Random()
                    rnd = Some(x)
                    x
            }
            return Int64(r.nextUInt64() % UInt64(refs.size))
        }
        if (routing.mode == ROUTE_CONSISTENT_HASH) {
            if (let Some(keyOf) <- routing.keyOf) {
                let candidates = Array<UInt64>(refs.size, { i => stableHash64(refs[i].pathString()) })
                return rendezvousIndex(stableHash64(keyOf(message)), candidates)
            }
        }
        let i = cursor % refs.size
        cursor = if (cursor == Int64.Max) { 0 } else { cursor + 1 }
        i
    }

    public func onSignal(signal: TypedSignal, context: TypedActorContext<T>): TypedBehavior<T> {
        match (signal) {
            case t: TypedTerminated =>
                let empty = table.removePath(t.pathString())
                if (pool && empty) {
                    TypedBehaviors.stopped<T>()
                } else {
                    TypedBehaviors.same<T>()
                }
            case _: PostStop =>
                if (let Some(f) <- onStop) {
                    f()
                }
                TypedBehaviors.same<T>()
            case _ => TypedBehaviors.unhandled<T>()
        }
    }

    public func getDescription(): String {
        if (pool) { "PoolRouter" } else { "GroupRouter" }
    }
}

/**
 * 类型化路由器工厂
 */
public struct TypedRouters {
    /** 池路由器（轮询）：启动 poolSize 个 behavior 子 Actor */
    public static func pool<T>(poolSize: Int64, behavior: TypedBehavior<T>): TypedBehavior<T> {
        pool<T>(poolSize, behavior, TypedRouting<T>())
    }

    public static func pool<T>(poolSize: Int64, behavior: TypedBehavior<T>, routing: TypedRouting<T>): TypedBehavior<T> {
        if (poolSize <= 0) {
            throw IllegalArgumentException("路由池大小必须为正数: ${poolSize}")
        }
        TypedBehaviors.setup<T>({ ctx =>
            let table = RouteeTable<T>()
            for (i in 0..poolSize) {
                let r = match (routing.routeeProps) {
                    case Some(p) => ctx.actorOf<T>(behavior, "routee-${i}", p)
                    case None => ctx.actorOf<T>(behavior, "routee-${i}")
                }
                ctx.watch<T>(r)
                table.add(r)
            }
            TypedRouterBehavior<T>(routing, table, true, None<() -> Unit>)
        })
    }

    /** 组路由器（轮询）：目标为 Receptionist 中登记为 key 的全部实例 */
    public static func group<T>(key: ServiceKey<T>): TypedBehavior<T> {
        group<T>(key, TypedRouting<T>())
    }

    public static func group<T>(key: ServiceKey<T>, routing: TypedRouting<T>): TypedBehavior<T> {
        TypedBehaviors.setup<T>({ ctx =>
            let table = RouteeTable<T>()
            let receptionist = ctx.system().receptionist()
            let sub = receptionist.subscribe<T>(key, { listing: Listing<T> => table.replace(listing.services) })
            TypedRouterBehavior<T>(routing, table, false, Some({ => receptionist.unsubscribe(sub) |> ignoreUnsub }))
        })
    }
}

func ignoreUnsub(_: Bool): Unit {}
