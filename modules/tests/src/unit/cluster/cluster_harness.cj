/*
 * CActor - 多节点测试 harness（E62 的一部分，对标 Akka MultiNodeSpec 的 barrier / 黑洞注入）
 *
 *   - `ClusterBarrier`：N 个节点在某一步同步等待，全部到达后再继续（避免"某节点还没 Up 就开始发消息"的竞态）。
 *   - `ClusterBlackhole`：对一个节点注入黑洞（drop 所有出站帧）并恢复，基于已有的 PartitionTransport。
 *   - `awaitAllUp(nodes, n)`：等待全部节点看到 n 个 Up 成员且收敛。
 *
 * 这些工具是纯测试辅助，不进入生产代码；生产侧的黑洞注入仍由 PartitionTransport / ClusterSettings 提供。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import cactor_cluster.membership.*
import cactor_testkit.*

/** N 方同步屏障：每个参与者调用 arrive()，全部到达后同时放行 */
public class ClusterBarrier {
    private let lock = Mutex()
    private var arrived: Int64 = 0
    private let total: Int64
    private var released: Bool = false

    public init(total: Int64) {
        this.total = total
    }

    /** 到达屏障并等待其他参与者；全部到达后同时放行 */
    public func arrive(timeout: Duration): Bool {
        synchronized(lock) {
            arrived++
            if (arrived >= total) {
                released = true
            }
        }
        let deadline = DateTime.now().toUnixTimeStamp().toMilliseconds() + timeout.toMilliseconds()
        while (DateTime.now().toUnixTimeStamp().toMilliseconds() < deadline) {
            synchronized(lock) {
                if (released) {
                    return true
                }
            }
            sleep(Duration.millisecond * 10)
        }
        synchronized(lock) { released }
    }

    /** 重置屏障（新一轮同步） */
    public func reset(): Unit {
        synchronized(lock) {
            arrived = 0
            released = false
        }
    }
}

/** 等待所有节点的分片区域完成获取（托管总数 = 分片数、无正在获取） */
public func awaitAllAcquired(regions: Array<ClusterSharding>, shards: Int64, timeout: Duration): Bool {
    let deadline = DateTime.now().toUnixTimeStamp().toMilliseconds() + timeout.toMilliseconds()
    while (DateTime.now().toUnixTimeStamp().toMilliseconds() < deadline) {
        var hosted: Int64 = 0
        var acquiring: Int64 = 0
        for (r in regions) {
            let (h, a, _) = r.shardStats()
            hosted += h
            acquiring += a
        }
        if (acquiring == 0 && hosted == shards) {
            return true
        }
        sleep(Duration.millisecond * 50)
    }
    false
}
