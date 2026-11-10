/* CActor Cluster.Ops - 集群视图管理端点（运维可见性）
 *
 * 门面模块 `cactor.api.ManagementServer` 只认识 CActorSystem，不认识集群类型（分层：cluster → cactor）。
 * 因此集群视图以"路由"的形式从上层注入：
 *
 *   let mgmt = ManagementServer(system, 8500)
 *       .addReadinessCheck("cluster", { => cluster.isUp() && cluster.state().converged })
 *   attachClusterRoutes(mgmt, cluster, sharding: Some(carts))
 *   mgmt.start()
 *
 * 提供两个端点（与 docs/TROUBLESHOOTING.md 里的排障命令一致）：
 *   GET /cluster/status  —— 本节点视图、收敛状态、领导者、成员表（地址/状态/upNumber/uid/是否可达）
 *   GET /cluster/shards  —— 本节点的分片托管情况与负载（需要传入 ClusterSharding）
 *
 * 两个端点都是同步快照：集群状态读的是守护 Actor 的不可变快照，分片计数读区域共享锁，
 * 不向其他节点发消息，因此可以在请求里直接调用（不会因为某个节点卡住而拖住管理端口）。
 */
package cactor_cluster.ops

import std.collection.*
import std.sort.*
import cactor.api.{ManagementServer, ManagementRoute}
import cactor_cluster.membership.{Cluster, ClusterSharding, ClusterSnapshot}

/** GET /cluster/status 的响应体：本节点当前视图 */
public func clusterStatusJson(cluster: Cluster): String {
    let snapshot = cluster.state()
    let self = cluster.selfUniqueAddress()
    let selfAddr = selfAddressOf(cluster)
    let sb = StringBuilder()
    sb.append("{\"self\":\"${jsonEscape(selfAddr)}\"")
    sb.append(",\"selfUniqueAddress\":\"${jsonEscape(self)}\"")
    sb.append(",\"state\":\"${selfStatus(cluster, snapshot)}\"")
    sb.append(",\"converged\":${snapshot.converged}")
    sb.append(",\"upCount\":${snapshot.upCount()}")
    sb.append(",\"leader\":${quoteOrNull(snapshot.leader)}")
    sb.append(",\"sbrDowned\":${cluster.isDownedBySbr()}")
    sb.append(",\"gossipCount\":${cluster.gossipCount()}")
    sb.append(",\"unreachable\":[")
    for (i in 0..snapshot.unreachable.size) {
        if (i > 0) {
            sb.append(",")
        }
        sb.append("\"${jsonEscape(snapshot.unreachable[i])}\"")
    }
    sb.append("],\"members\":[")
    let members = snapshot.members
    for (i in 0..members.size) {
        if (i > 0) {
            sb.append(",")
        }
        let m = members[i]
        sb.append("{\"address\":\"${jsonEscape(m.address)}\"")
        sb.append(",\"status\":\"${m.status}\"")
        sb.append(",\"upNumber\":${m.upNumber}")
        sb.append(",\"uid\":\"${m.uid}\"")
        sb.append(",\"reachable\":${!isUnreachable(snapshot, m.address)}")
    sb.append(",\"self\":${m.address == selfAddr}}")
    }
    sb.append("]}")
    sb.toString()
}

/** GET /cluster/shards 的响应体：本节点的分片托管情况与负载 */
public func shardingStatusJson(region: ClusterSharding): String {
    let (hosted, acquiring, acquisitions) = region.shardStats()
    let (delivered, forwarded, handedOff, dropped, buffered) = region.stats()
    let load = region.shardLoad()
    let busiest = region.busiestShards(5)
    let sb = StringBuilder()
    sb.append("{\"type\":\"${jsonEscape(region.typeName)}\"")
    sb.append(",\"shards\":${region.numberOfShards}")
    sb.append(",\"ownerCount\":${region.ownerCount()}")
    sb.append(",\"hosted\":${hosted}")
    sb.append(",\"acquiring\":${acquiring}")
    sb.append(",\"acquisitions\":${acquisitions}")
    sb.append(",\"entities\":${region.localEntityCount()}")
    sb.append(",\"delivered\":${delivered}")
    sb.append(",\"forwarded\":${forwarded}")
    sb.append(",\"handedOff\":${handedOff}")
    sb.append(",\"buffered\":${buffered}")
    sb.append(",\"dropped\":${dropped}")
    sb.append(",\"passivated\":${region.passivatedCount()}")
    sb.append(",\"restarted\":${region.restartedCount()}")
    let health = region.acquisitionHealth()
    sb.append(",\"slowAcquires\":${health.slow}")
    sb.append(",\"oldestAcquireMs\":${health.oldestMs}")
    sb.append(",\"acquireSlowWarnings\":${health.slowWarnings}")
    sb.append(",\"pendingAcquireMembers\":[")
    for (i in 0..health.pendingMembers.size) {
        if (i > 0) {
            sb.append(",")
        }
        sb.append("\"${jsonEscape(health.pendingMembers[i])}\"")
    }
    sb.append("],\"busiest\":[")
    for (i in 0..busiest.size) {
        if (i > 0) {
            sb.append(",")
        }
        sb.append("${busiest[i]}")
    }
    sb.append("],\"load\":{")
    // 按分片号排序，便于人读与测试比较（shardLoad 返回 HashMap，迭代顺序不稳定）
    let shards = ArrayList<Int64>()
    for ((sh, _) in load) {
        shards.add(sh)
    }
    let ordered = shards.toArray()
    sort(ordered)
    var first = true
    for (sh in ordered) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${sh}\":${load.get(sh) ?? 0}")
    }
    sb.append("}}")
    sb.toString()
}

/** 把集群视图挂到管理端口上；sharding 传 None 时只挂 /cluster/status */
public func attachClusterRoutes(mgmt: ManagementServer, cluster: Cluster,
    sharding!: Option<ClusterSharding> = None<ClusterSharding>): ManagementServer {
    mgmt.addRoute(ManagementRoute("/cluster/status", "application/json", { => clusterStatusJson(cluster) }))
    match (sharding) {
        case Some(region) =>
            mgmt.addRoute(ManagementRoute("/cluster/shards", "application/json", { => shardingStatusJson(region) }))
        case None => ()
    }
    mgmt
}

/** 本节点的 "host:port"（成员表里的键） */
private func selfAddressOf(cluster: Cluster): String {
    let unique = cluster.selfUniqueAddress()
    match (unique.lastIndexOf("#")) {
        case Some(i) => unique[0..i]
        case None => unique
    }
}

private func selfStatus(cluster: Cluster, snapshot: ClusterSnapshot): String {
    match (snapshot.member(selfAddressOf(cluster))) {
        case Some(m) => m.status.toString()
        case None => "Unknown"
    }
}

private func isUnreachable(snapshot: ClusterSnapshot, address: String): Bool {
    for (a in snapshot.unreachable) {
        if (a == address) {
            return true
        }
    }
    false
}

private func quoteOrNull(value: Option<String>): String {
    match (value) {
        case Some(v) => "\"${jsonEscape(v)}\""
        case None => "null"
    }
}

private func jsonEscape(s: String): String {
    s.replace("\\", "\\\\").replace("\"", "\\\"")
}
