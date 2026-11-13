/*
 * CActor - 跨进程文件租约 + fencing 功能验证（DA4，配合 tools/lease_contend_demo.sh 使用；不是压测）
 *
 *   cactor_examples.lease_contend hold  <目录> <持有者> <ttl 毫秒>   竞争租约；拿到后每 100 ms 经 fencing 写一次共享状态
 *   cactor_examples.lease_contend stale <目录> <token>               以给定（旧）token 写共享状态，期望被拒绝
 *   cactor_examples.lease_contend show  <目录>                       打印 fencing 已见最大 token 与当前状态
 *
 * 目录布局：<目录>/leases（FileLease 共享目录）、<目录>/fence（FileFencingTokenStore）、<目录>/data（FileDurableStateStore）。
 * 事件行写到 stderr（不缓冲：被 kill -9 之前已输出的行都在）：
 *   DENIED <持有者>            第一次被拒（另一进程持有）
 *   ACQUIRED <持有者> <token>  获得租约
 *   WROTE <持有者> <token> <修订号>
 *   LOST <持有者>              失去租约（随即退出，退出码 3）
 * stale 模式：被拒绝打印 "REJECTED <token> <已见最大>" 并以 0 退出；被接受打印 "ACCEPTED" 并以 1 退出。
 */
package cactor_examples.lease_contend

import std.time.*
import std.sync.*
import std.convert.*
import cactor_foundation.coordination.*
import cactor_persistence.*

let LEASE_NAME = "ledger"

func hold(dir: String, owner: String, ttlMs: Int64): Int64 {
    // 心跳 ttl/10、时钟偏差余量 ttl/4：本地有效期 = 0.75 ttl，心跳间隔远小于它
    let settings = LeaseSettings(LEASE_NAME, owner, Duration.millisecond * ttlMs, Duration.millisecond * (ttlMs / 10),
        Duration.millisecond * (ttlMs / 4))
    let lease = FileLease("${dir}/leases", settings)
    let lost = AtomicBool(false)
    var reportedDenied = false
    while (true) {
        let ok = try {
            lease.acquire({ _ => lost.store(true) })
        } catch (e: Exception) {
            eprintln("ERROR ${owner} ${e}")
            false
        }
        if (ok) {
            break
        }
        if (!reportedDenied) {
            eprintln("DENIED ${owner}")
            reportedDenied = true
        }
        sleep(Duration.millisecond * 100)
    }
    let token = lease.fencingToken().getOrThrow()
    eprintln("ACQUIRED ${owner} ${token}")
    // 拿到租约之后才打开数据存储；修订号每次从磁盘读，不依赖进程内缓存
    let store = FencedDurableStateStore(FileDurableStateStore("${dir}/data"), FileFencingTokenStore("${dir}/fence"), lease)
    while (true) {
        if (lost.load()) {
            eprintln("LOST ${owner}")
            return 3
        }
        try {
            let rev = store.revisionOf(LEASE_NAME) + 1
            store.upsert(LEASE_NAME, rev, "${owner}:${token}")
            eprintln("WROTE ${owner} ${token} ${rev}")
        } catch (e: LeaseNotHeldException) {
            eprintln("LOST ${owner}")
            return 3
        }
        sleep(Duration.millisecond * 100)
    }
    0
}

func staleWrite(dir: String, token: Int64): Int64 {
    let fence = FileFencingTokenStore("${dir}/fence")
    let data = FileDurableStateStore("${dir}/data")
    let store = FencedDurableStateStore(data, fence, LEASE_NAME, { => Some(token) })
    try {
        store.upsert(LEASE_NAME, data.revisionOf(LEASE_NAME) + 1, "stale:${token}")
        println("ACCEPTED")
        1
    } catch (e: StaleFencingTokenException) {
        println("REJECTED ${e.token} ${e.highestSeen}")
        0
    }
}

func show(dir: String): Int64 {
    let fence = FileFencingTokenStore("${dir}/fence")
    let data = FileDurableStateStore("${dir}/data")
    let state = match (data.get(LEASE_NAME)) {
        case Some(r) => "${r.state} rev=${r.revision}"
        case None => "-"
    }
    println("FENCE ${fence.highestToken(LEASE_NAME)} STATE ${state}")
    0
}

main(args: Array<String>): Int64 {
    if (args.size >= 4 && args[0] == "hold") {
        return hold(args[1], args[2], Int64.parse(args[3]))
    }
    if (args.size >= 3 && args[0] == "stale") {
        return staleWrite(args[1], Int64.parse(args[2]))
    }
    if (args.size >= 2 && args[0] == "show") {
        return show(args[1])
    }
    eprintln("用法: lease_contend hold <目录> <持有者> <ttl毫秒> | stale <目录> <token> | show <目录>")
    2
}
