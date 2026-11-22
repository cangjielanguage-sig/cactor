/*
 * CActor Cluster - 集群感知路由器（对标 Akka cluster-aware routers，差距 4.5）
 *
 * 与本地 `Routers.pool/group` 的区别：目标是**集群成员**，每次路由读取实时成员视图，
 * 成员被 down / 移除后自动剔除；没有候选时明确失败，而不是悄悄丢消息。
 *
 * 选择策略：
 * - RoundRobin：按候选顺序轮询（候选按地址排序，保证各节点顺序一致）；
 * - LeastLoaded：由应用提供 `loadOf(address)`（队列深度、在途请求数等），选择负载最低者，
 *   相同负载按地址最小者决胜。
 *
 * 不做进程内缓存：成员增减直接反映在 cluster.state() 上，缓存只会引入"视图过期"的竞态。
 */
package cactor_cluster.routing

import std.collection.*
import std.sync.*
import std.sort.*
import cactor_core.actor.{ActorRef}
import cactor_core.message.{Message}
import cactor_foundation.network.{NetworkAddress}
import cactor_remote.{RemoteActorRef, SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, MemberStatus}

/** 集群路由目标选择策略 */
public enum ClusterRouterStrategy {
    /** 按候选顺序轮询 */
    | RoundRobin
    /** 选择 loadOf 返回值最低的成员（相同则地址最小者） */
    | LeastLoaded
}

/**
 * 集群感知路由器：按成员视图 + 可选负载回调选择目标，消息发往各成员上同名的 Actor。
 *
 * 使用方需保证每个成员都用 `remote.exposeActor(actorName, localActor)` 暴露了同名 Actor；
 * 没有暴露的成员会在对端计入死信（与普通远程 tell 一致）。
 */
public class ClusterRouter {
    private let cluster: Cluster
    private let remote: SimpleRemoteManager
    private let actorName: String
    private let strategy: ClusterRouterStrategy
    private let loadOf: (String) -> Int64
    // 角色过滤（对标 Akka cluster router 的 role）：只为具备该角色的成员路由；None 表示不过滤
    private var role: Option<String> = None<String>
    // 数据中心过滤（4.3 的部分实现）：只为该 DC 的成员路由；None 表示不过滤
    private var dataCenter: Option<String> = None<String>
    private let roundRobin = AtomicInt64(0)
    private let stopped = AtomicBool(false)

    public init(cluster: Cluster, remote: SimpleRemoteManager, actorName: String,
        strategy!: ClusterRouterStrategy = ClusterRouterStrategy.RoundRobin,
        loadOf!: (String) -> Int64 = { _: String => 0 },
        role!: Option<String> = None<String>) {
        if (actorName.isEmpty()) {
            throw IllegalArgumentException("路由目标名称不能为空")
        }
        this.cluster = cluster
        this.remote = remote
        this.actorName = actorName
        this.strategy = strategy
        this.loadOf = loadOf
        this.role = role
    }

    /** 只为具备该角色的成员路由（链式；空角色非法） */
    public func withRole(r: String): ClusterRouter {
        if (r.isEmpty()) {
            throw IllegalArgumentException("路由角色不能为空")
        }
        role = Some<String>(r)
        this
    }

    /** 当前的角色过滤（None 表示不过滤） */
    public func roleFilter(): Option<String> {
        role
    }

    /** 只为指定数据中心的成员路由（链式；空 DC 非法） */
    public func withDataCenter(dc: String): ClusterRouter {
        if (dc.isEmpty()) {
            throw IllegalArgumentException("数据中心名不能为空")
        }
        dataCenter = Some<String>(dc)
        this
    }

    /** 当前的数据中心过滤（None 表示不过滤） */
    public func dataCenterFilter(): Option<String> {
        dataCenter
    }

    public prop name: String {
        get() { actorName }
    }

    /** 当前候选成员地址（Up、满足角色过滤、集群未终止；排序后返回） */
    public func candidates(): Array<String> {
        if (stopped.load() || cluster.isTerminated()) {
            return Array<String>()
        }
        let out = ArrayList<String>()
        for (m in cluster.state().members) {
            if (m.status == MemberStatus.Up) {
                var ok = true
                match (role) {
                    case Some(r) => ok = m.hasRole(r)
                    case None => ()
                }
                match (dataCenter) {
                    case Some(dc) => ok = ok && m.dataCenter == dc
                    case None => ()
                }
                if (ok) {
                    out.add(m.address)
                }
            }
        }
        let arr = out.toArray()
        sort(arr)
        arr
    }

    /**
     * 选择目标地址；没有候选时抛 IllegalStateException（明确失败，不静默丢消息）。
     */
    public func route(): String {
        let cs = candidates()
        if (cs.isEmpty()) {
            throw IllegalStateException("集群路由器 ${actorName} 没有可用候选成员")
        }
        match (strategy) {
            case RoundRobin =>
                let idx = roundRobin.fetchAdd(1) % Int64(cs.size)
                cs[idx]
            case LeastLoaded =>
                var best = cs[0]
                var bestLoad = loadOf(best)
                for (i in 1..cs.size) {
                    let load = loadOf(cs[i])
                    if (load < bestLoad) {
                        best = cs[i]
                        bestLoad = load
                    }
                }
                best
        }
    }

    /** 按策略解析出远程引用（每次新建，成员重启后旧引用会失效） */
    public func ref(): RemoteActorRef {
        remote.remoteActorOf(NetworkAddress.parse(route()), actorName)
    }

    /** 按策略投递；没有候选时抛 IllegalStateException */
    public func tell(message: Message): Unit {
        ref().tell(message)
    }

    /** 带发送方的投递（对端 `context.reply` 会回到 sender） */
    public func tell(message: Message, sender: ActorRef): Unit {
        ref().tell(message, sender)
    }

    /** 停止使用（之后 candidates 为空、route 明确失败） */
    public func stop(): Unit {
        stopped.store(true)
    }
}
