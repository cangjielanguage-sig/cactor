/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R6 测试：分布式发布订阅（三个真实节点）
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress}
import cactor_cluster.membership.*
import cactor_cluster.ddata.{Replicator}
import cactor_testkit.*

class Sub <: Actor {
    let tag: String
    let rec: EventRecorder

    init(tag: String, rec: EventRecorder) {
        this.tag = tag
        this.rec = rec
    }

    public prop name: String { get() { "sub" } }
    public prop description: String { get() { "sub" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record("${tag}:${s.getContent()}")
        }
        MessageResult.Handled
    }
}

class PNode {
    let node: CNode
    let pubsub: DistributedPubSub

    init(node: CNode) {
        this.node = node
        let rep = Replicator(node.kit.system, node.remote, Duration.millisecond * 50)
        this.pubsub = DistributedPubSub(node.cluster, node.kit.system, node.remote, rep)
    }
}

func pubsubCluster(name: String): Array<PNode> {
    let nodes = startCluster(3, name, FAST_AUTODOWN)
    let ok = awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) })
    if (!ok) {
        throw Exception("集群没有在期限内形成")
    }
    Array<PNode>(3, { i => PNode(nodes[i]) })
}

func closePubSub(ps: Array<PNode>): Unit {
    for (p in ps) {
        if (!p.node.cluster.isTerminated()) {
            p.node.close()
        }
    }
}

@Test
public class DistributedPubSubTest {
    @TestCase
    public func testPublishReachesSubscribersOnAllNodesExactlyOnce(): Unit {
        let ps = pubsubCluster("ps-basic")
        let rec = EventRecorder()
        let s1 = ps[1].node.kit.actorOf({ => Sub("n1a", rec) }, "s1")
        let s1b = ps[1].node.kit.actorOf({ => Sub("n1b", rec) }, "s1b")
        let s2 = ps[2].node.kit.actorOf({ => Sub("n2", rec) }, "s2")
        ps[1].pubsub.subscribe("news", s1)
        ps[1].pubsub.subscribe("news", s1b)
        ps[2].pubsub.subscribe("news", s2)
        // 订阅信息同步到发布者所在的节点
        @Expect(awaitCondition(CLUSTER_WAIT, { => ps[0].pubsub.subscriberNodes("news").size == 2 }), true)
        @Expect(ps[0].pubsub.publish("news", StringMessage("hello")), 2)
        @Expect(rec.awaitSize(3, CLUSTER_WAIT), true)
        // 从有订阅者的节点自己发布，本地订阅者同样收到
        @Expect(awaitCondition(CLUSTER_WAIT, { => ps[2].pubsub.subscriberNodes("news").size == 2 }), true)
        ps[2].pubsub.publish("news", StringMessage("again"))
        @Expect(rec.awaitSize(6, CLUSTER_WAIT), true)
        sleep(Duration.millisecond * 200)
        // 恰好一次：没有重复
        @Expect(rec.size(), 6)
        for (tag in ["n1a", "n1b", "n2"]) {
            @Expect(rec.count("${tag}:hello"), 1)
            @Expect(rec.count("${tag}:again"), 1)
        }
        // 没有订阅者的主题
        @Expect(ps[0].pubsub.publish("nobody", StringMessage("x")), 0)
        closePubSub(ps)
    }

    @TestCase
    public func testUnsubscribeAndSubscriberTermination(): Unit {
        let ps = pubsubCluster("ps-unsub")
        let rec = EventRecorder()
        let a = ps[1].node.kit.actorOf({ => Sub("a", rec) }, "a")
        let b = ps[2].node.kit.actorOf({ => Sub("b", rec) }, "b")
        ps[1].pubsub.subscribe("t", a)
        ps[2].pubsub.subscribe("t", b)
        @Expect(awaitCondition(CLUSTER_WAIT, { => ps[0].pubsub.subscriberNodes("t").size == 2 }), true)
        // 显式退订：节点 1 不再有订阅者，从主题节点集合中消失
        ps[1].pubsub.unsubscribe("t", a)
        @Expect(awaitCondition(CLUSTER_WAIT, { => ps[0].pubsub.subscriberNodes("t") == [ps[2].node.address()] }), true)
        // 订阅者停止：自动退订
        ps[2].node.kit.system.stop(b)
        @Expect(awaitCondition(CLUSTER_WAIT, { => ps[0].pubsub.subscriberNodes("t").isEmpty() }), true)
        @Expect(ps[2].pubsub.localSubscriberCount("t"), 0)
        @Expect(ps[0].pubsub.publish("t", StringMessage("lost")), 0)
        sleep(Duration.millisecond * 100)
        @Expect(rec.size(), 0)
        closePubSub(ps)
    }

    @TestCase
    public func testCrashedNodeIsRemovedFromTopics(): Unit {
        let ps = pubsubCluster("ps-crash")
        let rec = EventRecorder()
        ps[1].pubsub.subscribe("t", ps[1].node.kit.actorOf({ => Sub("one", rec) }, "x"))
        ps[2].pubsub.subscribe("t", ps[2].node.kit.actorOf({ => Sub("two", rec) }, "y"))
        @Expect(awaitCondition(CLUSTER_WAIT, { => ps[0].pubsub.subscriberNodes("t").size == 2 }), true)
        ps[2].node.crash()
        // 自动 down 并移除后，其他节点把它从主题中删除
        @Expect(awaitCondition(CLUSTER_WAIT, { => ps[0].pubsub.subscriberNodes("t") == [ps[1].node.address()] }), true)
        @Expect(ps[0].pubsub.publish("t", StringMessage("m")), 1)
        @Expect(rec.await("one:m", CLUSTER_WAIT), true)
        ps[0].node.close()
        ps[1].node.close()
    }

    @TestCase
    public func testTopicNameValidation(): Unit {
        let ps = pubsubCluster("ps-names")
        var rejected = 0
        for (bad in ["", "a/b", "has space", "中文"]) {
            try {
                ps[0].pubsub.publish(bad, StringMessage("x"))
            } catch (_: IllegalArgumentException) {
                rejected++
            }
        }
        @Expect(rejected, 4)
        @Expect(ps[0].pubsub.publish("ok.topic-1_x", StringMessage("x")), 0)
        closePubSub(ps)
    }
}
