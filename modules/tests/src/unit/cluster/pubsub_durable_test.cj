/*
 * CActor - 差距 4.8 剩余部分：DistributedPubSub 自动接入持久日志
 *
 * 覆盖：
 *   - withDurableLog 后 publish 同时写日志；离线期间消息不丢；
 *   - replayDurable 按订阅者游标回放（再次回放为空）；
 *   - 在线扇出仍工作（回放之后 subscribe 可收到实时消息）；
 *   - 换集群/系统但共用存储时，日志仍在（重启后可回放）。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_cluster.membership.*
import cactor_cluster.ddata.{Replicator}
import cactor_persistence.{MemoryDurableStateStore}
import cactor_testkit.*

class DurableSub <: Actor {
    let rec: EventRecorder

    init(rec: EventRecorder) {
        this.rec = rec
    }

    public prop name: String { get() { "durable-sub" } }
    public prop description: String { get() { "durable sub" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record(s.getContent())
        }
        MessageResult.Handled
    }
}

func durablePubSub(node: CNode, store: MemoryDurableStateStore): DistributedPubSub {
    let rep = Replicator(node.kit.system, node.remote, Duration.millisecond * 50)
    DistributedPubSub(node.cluster, node.kit.system, node.remote, rep).withDurableLog(store)
}

@Test
public class PubSubDurableLogTest {
    @TestCase
    public func offlineSubscriberCatchesUpFromDurableLog(): Unit {
        let nodes = startCluster(1, "pubsub-durable", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        let n = nodes[0]
        let store = MemoryDurableStateStore()
        let offsets = MemoryDurableStateStore()
        let pubsub = durablePubSub(n, store)
        try {
            // 订阅者离线期间发布三条
            pubsub.publish("orders", StringMessage("a"))
            pubsub.publish("orders", StringMessage("b"))
            pubsub.publish("orders", StringMessage("c"))
            @Expect(pubsub.durableHighestSeq("orders"), 3)

            let got = ArrayList<String>()
            let lock = Mutex()
            let last = pubsub.replayDurable("orders", "svc", offsets,
                { seq, payload => synchronized(lock) { got.add("${seq}:${payload}") } })
            @Expect(last, 3)
            @Expect(got.size, 3)
            @Expect(got[1], "2:b")

            // 游标已推进：同一订阅者再回放不重复
            let again = pubsub.replayDurable("orders", "svc", offsets,
                { _, _ => synchronized(lock) { got.add("dup") } })
            @Expect(again, 3)
            @Expect(got.size, 3)
            // 其他主题互不影响
            @Expect(pubsub.durableHighestSeq("other"), 0)

            // 回放之后接入在线投递：实时消息照常到达，且继续入日志
            let rec = EventRecorder()
            let sub = n.kit.actorOf({ => DurableSub(rec) }, "durable-sub")
            pubsub.subscribe("orders", sub)
            @Expect(awaitCondition(CLUSTER_WAIT, { => pubsub.subscriberNodes("orders").size == 1 }), true)
            pubsub.publish("orders", StringMessage("d"))
            @Expect(awaitCondition(CLUSTER_WAIT, { => rec.size() == 1 }), true)
            @Expect(rec.contains("d"), true)
            @Expect(pubsub.durableHighestSeq("orders"), 4)
        } finally {
            closeAll(nodes)
        }
    }

    @TestCase
    public func durableLogSurvivesSystemRestart(): Unit {
        let store = MemoryDurableStateStore()
        let first = startCluster(1, "pubsub-durable-r1", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(first, 1) }), true)
        let p1 = durablePubSub(first[0], store)
        p1.publish("audit", StringMessage("one"))
        p1.publish("audit", StringMessage("two"))
        @Expect(p1.durableHighestSeq("audit"), 2)
        closeAll(first)

        // 全新系统、同一存储：新实例仍能看到旧日志
        let second = startCluster(1, "pubsub-durable-r2", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(second, 1) }), true)
        let p2 = durablePubSub(second[0], store)
        try {
            @Expect(p2.durableHighestSeq("audit"), 2)
            let got = ArrayList<String>()
            let lock = Mutex()
            let offsets = MemoryDurableStateStore()
            let last = p2.replayDurable("audit", "svc", offsets,
                { _, payload => synchronized(lock) { got.add(payload) } }, fromSeq: 1)
            @Expect(last, 2)
            @Expect(got.size, 2)
            @Expect(got[0], "one")
            @Expect(got[1], "two")
        } finally {
            closeAll(second)
        }
    }
}
