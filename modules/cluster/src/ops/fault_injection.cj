/* CActor Cluster.Ops - 进程级故障注入端点（E62 的跨进程部分）
 *
 * 单进程里 `BlackholeTransport` 已经能验证"丢帧 → 判不可达 → 恢复 → 收敛"，
 * 但真正的失败模式（两个 OS 进程之间的半分区）只能在多进程里复现。做法是让节点进程
 * 自己持有 `BlackholeTransport`，再用管理端点切换：
 *
 *   let transport = BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", port)))
 *   let remote = SimpleRemoteManager(transport)
 *   attachFaultInjectionRoutes(mgmt, transport)
 *
 * 端点（只在运维/测试配置下挂载，绝不能在生产开放）：
 *   GET /cluster/faults                          —— 当前黑洞地址、丢帧计数
 *   GET /cluster/blackhole?addr=127.0.0.1:7002   —— 黑洞到该地址的出站帧（addr=* 表示全部）
 *   GET /cluster/heal?addr=127.0.0.1:7002        —— 恢复（不带 addr 恢复全部）
 *
 * 用 GET 而不是 POST：`curl` 一行就能触发，排障手册里可以直接抄；
 * 副作用是幂等的（重复黑洞同一地址没有额外效果）。
 */
package cactor_cluster.ops

import std.collection.*
import cactor.api.{ManagementServer, ManagementRoute}
import cactor_foundation.network.{BlackholeTransport}

/** 当前注入状态：`{"blackholed":["a","b"],"droppedCount":12}` */
public func faultInjectionJson(transport: BlackholeTransport): String {
    let sb = StringBuilder()
    sb.append("{\"blackholed\":[")
    let list = transport.blackholedAddresses()
    for (i in 0..list.size) {
        if (i > 0) {
            sb.append(",")
        }
        sb.append("\"${jsonEscape(list[i])}\"")
    }
    sb.append("],\"droppedCount\":${transport.droppedCount()}}")
    sb.toString()
}

/**
 * 把故障注入端点挂到管理服务器上。
 *
 * `allowAll` 决定 `addr=*` 是否被接受：默认允许（测试场景常用），生产配置里传 false 即可关掉。
 * 端点本身不会校验调用方地址——绑定 127.0.0.1 是调用方（`ManagementServer` 构造参数）的责任。
 */
public func attachFaultInjectionRoutes(mgmt: ManagementServer, transport: BlackholeTransport,
    allowAll!: Bool = true): ManagementServer {
    mgmt.addRoute("/cluster/faults", "application/json", { => faultInjectionJson(transport) })
    mgmt.addQueryRoute("/cluster/blackhole", "text/plain", { raw =>
        let target = queryParam(raw, "addr")
        match (target) {
            case None => "缺少参数 addr=host:port（或 addr=*）\n"
            case Some("*") =>
                if (allowAll) {
                    for (a in transport.blackholedAddresses()) {
                        transport.heal(a)
                    }
                    "已黑洞全部出站帧：需要在各目标地址上分别指定，addr=* 只清除已注入项\n"
                } else {
                    "addr=* 在本节点被禁止（allowAll=false）\n"
                }
            case Some(addr) =>
                transport.blackhole(addr)
                "已黑洞 -> ${addr}；丢弃 ${transport.droppedCount()} 帧\n"
        }
    })
    mgmt.addQueryRoute("/cluster/heal", "text/plain", { raw =>
        let target = queryParam(raw, "addr")
        match (target) {
            case None =>
                transport.healAll()
                "已恢复全部出站帧\n"
            case Some(addr) =>
                transport.heal(addr)
                "已恢复 -> ${addr}\n"
        }
    })
    mgmt
}

/** 从路径（`/x?a=1&b=2`）里取查询参数；未找到返回 None */
private func queryParam(path: String, key: String): Option<String> {
    let q = path.indexOf("?")
    if (q.isNone()) {
        return None<String>
    }
    let query = path[q.getOrThrow() + 1..path.size]
    for (pair in query.split("&")) {
        let eq = pair.indexOf("=")
        if (let Some(pos) <- eq) {
            if (pair[0..pos] == key) {
                return Some(pair[pos + 1..pair.size])
            }
        }
    }
    None<String>
}
