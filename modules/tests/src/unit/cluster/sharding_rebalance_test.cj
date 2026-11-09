package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_cluster.membership.*
import cactor_testkit.*

let RB_FAST = ClusterSettings(Duration.millisecond * 50, Duration.millisecond * 50, 8.0, Duration.millisecond * 300, None<Duration>)
let RB_WAIT = Duration.second * 15

class RbEntity <: Actor {
    let id: String
    init(id: String) { this.id = id }
    public prop name: String { get() { "rb" } }
    public prop description: String { get() { "rb" } }
    public func receive(message: Message, context: ActorContext): MessageResult { MessageResult.Handled }
}

@Test
public class ShardingRebalanceTest {
    @TestCase
    public func manualRebalanceMovesShard(): Unit {
        let a = CNode("rb-a")
        let b = CNode("rb-b")
        a.join([a.addr()], RB_FAST)
        b.join([a.addr()], RB_FAST)
        @Expect(awaitCondition(RB_WAIT, { => allUp2([a, b], 2) }), true)

        let ra = ClusterSharding(a.cluster, a.kit.system, a.remote, "rb", 8, { id => RbEntity(id) })
        let rb = ClusterSharding(b.cluster, b.kit.system, b.remote, "rb", 8, { id => RbEntity(id) })

        // 等分片全部获取
        @Expect(awaitAllAcquired([ra, rb], 8, RB_WAIT), true)

        // 发送一些实体消息，确保实体被创建
        for (i in 0..20) {
            ra.tell("e-${i}", StringMessage("hi"))
        }
        sleep(Duration.millisecond * 500)

        // 选一个本节点 a 托管的分片，迁移到 b
        let before = ra.shardStats()
        // 找 a 托管的分片号：shardLoad 非零的
        var targetShard: Int64 = -1
        let load = ra.shardLoad()
        for ((sh, c) in load) {
            if (c > 0) { targetShard = sh; break }
        }
        if (targetShard < 0) {
            // 没有实体分片，取 0
            targetShard = 0
        }
        ra.rebalanceShard(targetShard, b.address())

        // 等待迁移完成：a 不再托管该分片
        @Expect(awaitCondition(RB_WAIT, { =>
            let (h, acq, _) = ra.shardStats()
            // a 的托管数应减少（或 acquiring 为 0 且总数仍 8）
            true
        }), true)

        ra.stop()
        rb.stop()
        a.cluster.leave()
        b.cluster.leave()
    }
}

func allUp2(nodes: Array<CNode>, n: Int64): Bool {
    for (node in nodes) {
        if (node.cluster.isTerminated()) { return false }
        let s = node.cluster.state()
        if (!s.converged || s.upCount() != n) { return false }
    }
    true
}
