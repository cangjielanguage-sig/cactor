/*
 * CActor - 多节点测试 harness（E62，对标 Akka MultiNodeSpec 的 barrier / 故障注入）
 *
 *   - `ClusterBarrier`：N 个节点在某一步同步等待，全部到达后再继续（避免"某节点还没 Up 就开始发消息"的竞态）。
 *   - `ProcessBarrier`（`cactor_testkit`）：跨进程版本，用共享目录的 `.ready` 文件会合，shell 也能参与。
 *   - 黑洞注入：`cactor_foundation.network.BlackholeTransport` 包住真实传输即可丢出站帧；
 *     真进程之间走 `tools/cluster_chaos.sh` 的 `/cluster/blackhole` 端点。
 *   - `awaitAllUp(nodes, n)`：等待全部节点看到 n 个 Up 成员且收敛。
 *   - `awaitGossipRounds(nodes, rounds, timeout)`：让集群再跑若干轮 gossip——比 `sleep` 更贴近
 *     "信息已经传播过"的语义，用来替代"等一会儿再看没变化"这类负向断言前的固定睡眠。
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

/**
 * 让每个节点再各自跑 `rounds` 轮 gossip。
 *
 * 负向断言（"迟到的心跳/订阅删除没有生效""墓碑没有被传回来"）以前用固定 `sleep` 等一会儿，
 * 在负载高的机器上会偶发变红：睡眠结束时消息可能还没传播完。这里改成等 gossip 真的推进了
 * 若干轮——语义是"信息至少绕了一圈"，且等待时长随集群活跃度自适应。
 */
public func awaitGossipRounds(nodes: Array<Cluster>, rounds: Int64, timeout: Duration): Bool {
    let base = ArrayList<Int64>()
    for (n in nodes) {
        base.add(n.gossipCount())
    }
    let deadline = DateTime.now().toUnixTimeStamp().toMilliseconds() + timeout.toMilliseconds()
    while (DateTime.now().toUnixTimeStamp().toMilliseconds() < deadline) {
        var settled = true
        for (i in 0..nodes.size) {
            if (nodes[i].gossipCount() - base[i] < rounds) {
                settled = false
                break
            }
        }
        if (settled) {
            return true
        }
        sleep(Duration.millisecond * 10)
    }
    false
}
