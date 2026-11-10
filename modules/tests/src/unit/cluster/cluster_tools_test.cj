/*
 * CActor - E12′ 测试：集群工具补齐
 * - 回复入口按 Actor 生命周期注册与注销（N7、v1 B6）：同路径重建后回复送到新实例；10 万次远程回复后入口不增长
 * - 分片与单例透传 sender（N16）：经分片 / 单例的 ask 能往返，跨节点回复直接回到发起方
 * - Grains 异步调用（N13）：callAsync 不占线程；可重入 Grain 的 A→B→A 调用链完成；Actor 内同步 call 立即报错而不是死锁
 * - pubsub 的节点集合由主题 Actor 串行写入（N12）：退订与紧随其后的订阅不会把有订阅者的节点移出主题
 * - 清理（N14）：Removed 成员从 gossip 删除只留墓碑、墓碑按时清除；非成员的心跳不建故障检测器；被 down 的成员被隔离
 * 全部在本机回环 TCP 上的真实节点之间进行。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.typed.{TypedPromise, TypedPromises}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_cluster.membership.*
import cactor_cluster.ddata.{Replicator, FieldWriter}
import cactor_testkit.*

/** 回声：把收到的文本加上标签回复给发送方 */
class EchoActor <: Actor {
    let tag: String

    init(tag: String) {
        this.tag = tag
    }

    public prop name: String { get() { "echo" } }
    public prop description: String { get() { "echo ${tag}" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            context.reply(StringMessage("${tag}:${s.getContent()}"))
        }
        MessageResult.Handled
    }
}

/** 收集回复 */
class ReplyCollector <: Actor {
    let label: String
    let rec: EventRecorder

    init(label: String, rec: EventRecorder) {
        this.label = label
        this.rec = rec
    }

    public prop name: String { get() { "collector" } }
    public prop description: String { get() { "collector ${label}" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record("${label}|${s.getContent()}")
        }
        MessageResult.Handled
    }
}

/** 短命请求者：与远程回声一问一答 n 次，收齐后停止自己 */
class PingPongRequester <: Actor {
    let target: ActorRef
    let n: Int64
    let replies: AtomicInt64
    let finished: AtomicInt64
    var got: Int64 = 0

    init(target: ActorRef, n: Int64, replies: AtomicInt64, finished: AtomicInt64) {
        this.target = target
        this.n = n
        this.replies = replies
        this.finished = finished
    }

    public prop name: String { get() { "requester" } }
    public prop description: String { get() { "requester" } }

    public func onStarted(context: ActorContext): Unit {
        target.tell(StringMessage("0"), context.self)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (message is StringMessage) {
            got++
            replies.fetchAdd(1)
            if (got < n) {
                target.tell(StringMessage("${got}"), context.self)
            } else {
                finished.fetchAdd(1)
                context.stop(context.self)
            }
        }
        MessageResult.Handled
    }
}

/** 只有远程层、没有集群的节点 */
class RemoteNode {
    let kit: ActorTestKit
    let remote: SimpleRemoteManager

    init(name: String) {
        kit = ActorTestKit(name)
        remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        remote.start()
    }

    func close(): Unit {
        remote.stop()
        kit.shutdown()
    }
}

func textOf(m: Message): String {
    (m as StringMessage).getOrThrow().getContent()
}

func ownersReady(regions: Array<ClusterSharding>, n: Int64): Bool {
    awaitCondition(CLUSTER_WAIT, { =>
        var ok = true
        for (r in regions) {
            ok = ok && r.ownerCount() == n
        }
        ok
    })
}

@Test
public class ReplyEntryLifecycleTest {
    /** N7：同一路径上重建的 Actor 能收到发给它的回复（此前入口绑定第一个实例，回复投给已死的旧实例） */
    @TestCase
    public func repliesReachActorRecreatedAtSamePath(): Unit {
        let a = RemoteNode("reply-path-a")
        let b = RemoteNode("reply-path-b")
        b.remote.exposeActor("echo", b.kit.actorOf({ => EchoActor("b") }, "echo"))
        let echo = a.remote.remoteActorOf(b.remote.getLocalAddress(), "echo")
        let rec = EventRecorder()
        let first = a.kit.actorOf({ => ReplyCollector("first", rec) }, "requester")
        echo.tell(StringMessage("1"), first)
        @Expect(rec.await("first|b:1", CLUSTER_WAIT), true)
        a.kit.system.stop(first)
        @Expect(awaitCondition(CLUSTER_WAIT, { => first.isTerminated() }), true)
        // 名称释放后在同一路径上重建
        let created = ArrayList<ActorRef>()
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            try {
                created.add(a.kit.actorOf({ => ReplyCollector("second", rec) }, "requester"))
                true
            } catch (_: Exception) {
                false
            }
        }), true)
        let s = created[0]
        @Expect(s.path.getPath(), first.path.getPath())
        echo.tell(StringMessage("2"), s)
        @Expect(rec.await("second|b:2", CLUSTER_WAIT), true)
        // 旧实例的入口已注销：同一路径只占一个入口
        @Expect(awaitCondition(CLUSTER_WAIT, { => a.remote.replyEntryCount() == 1 }), true)
        a.close()
        b.close()
    }

    /** 验收：10 万次远程回复（1000 个短命 Actor 各一问一答 100 次）之后，回复入口与处理器数量回到起点 */
    @TestCase
    public func replyEntriesDoNotGrowAfter100kReplies(): Unit {
        let a = RemoteNode("reply-leak-a")
        let b = RemoteNode("reply-leak-b")
        b.remote.exposeActor("echo", b.kit.actorOf({ => EchoActor("b") }, "echo"))
        let echo = a.remote.remoteActorOf(b.remote.getLocalAddress(), "echo")
        let handlersBefore = a.remote.getReceiver().handlerCount()
        let replies = AtomicInt64(0)
        let finished = AtomicInt64(0)
        let actors: Int64 = 1000
        let perActor: Int64 = 100
        let batch: Int64 = 50
        var started: Int64 = 0
        while (started < actors) {
            let upTo = min(started + batch, actors)
            for (i in started..upTo) {
                a.kit.actorOf({ => PingPongRequester(echo, perActor, replies, finished) }, "requester-${i}")
            }
            started = upTo
            // 同时在途的请求者不超过一批，避免压满出站队列（tell 是至多一次）
            let target = upTo
            @Expect(awaitCondition(Duration.second * 60, { => finished.load() >= target }), true)
        }
        @Expect(replies.load(), actors * perActor)
        // 已终止的请求者的入口在下一轮清扫（每秒）被注销
        @Expect(awaitCondition(CLUSTER_WAIT, { => a.remote.replyEntryCount() == 0 }), true)
        @Expect(a.remote.getReceiver().handlerCount(), handlersBefore)
        a.close()
        b.close()
    }
}

@Test
public class ShardingSingletonSenderTest {
    /** 经分片的 ask：从每个节点问每个实体，实体用 context.reply 回复，跨节点的回复直接回到发起方 */
    @TestCase
    public func askThroughShardingRoundTrips(): Unit {
        let nodes = startCluster(3, "e12-shard-ask", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let regions = Array<ClusterSharding>(3, { i =>
            let n = nodes[i]
            let addr = n.address()
            ClusterSharding(n.cluster, n.kit.system, n.remote, "echo", 32, { _ => EchoActor(addr) })
        })
        @Expect(ownersReady(regions, 3), true)
        var remoteReplies = 0
        for (i in 0..3) {
            let pending = ArrayList<TypedPromise<Message>>()
            for (k in 0..20) {
                pending.add(regions[i].ask("e${k}", StringMessage("p${k}"), CLUSTER_WAIT))
            }
            for (k in 0..20) {
                let reply = textOf(pending[k].get(CLUSTER_WAIT))
                @Expect(reply.endsWith(":p${k}"), true)
                if (!reply.startsWith(nodes[i].address())) {
                    remoteReplies++
                }
            }
        }
        // 60 次里绝大多数实体不在发起方节点上：确实走了远程回复
        @Expect(remoteReplies > 20, true)
        // 带 sender 的 tell：回复送到指定的本地 Actor
        let rec = EventRecorder()
        let collector = nodes[1].kit.actorOf({ => ReplyCollector("c", rec) }, "collector")
        for (k in 0..10) {
            regions[1].tell("e${k}", StringMessage("t${k}"), collector)
        }
        @Expect(rec.awaitSize(10, CLUSTER_WAIT), true)
        for (r in regions) {
            r.stop()
        }
        closeAll(nodes)
    }

    /** 经单例的 ask：任意节点都能拿到唯一实例的回复 */
    @TestCase
    public func askThroughSingletonRoundTrips(): Unit {
        let nodes = startCluster(3, "e12-singleton-ask", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let singletons = Array<ClusterSingleton>(3, { i =>
            let n = nodes[i]
            let addr = n.address()
            ClusterSingleton(n.cluster, n.kit.system, n.remote, "echo", { => EchoActor(addr) })
        })
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[0].isRunningHere() }), true)
        let oldest = nodes[0].address()
        for (i in 0..3) {
            for (k in 0..5) {
                @Expect(textOf(singletons[i].ask(StringMessage("s${k}"), CLUSTER_WAIT).get(CLUSTER_WAIT)), "${oldest}:s${k}")
            }
        }
        let rec = EventRecorder()
        let collector = nodes[2].kit.actorOf({ => ReplyCollector("c", rec) }, "collector")
        singletons[2].tell(StringMessage("x"), collector)
        @Expect(rec.await("c|${oldest}:x", CLUSTER_WAIT), true)
        for (s in singletons) {
            s.stop()
        }
        closeAll(nodes)
    }
}

/** 构造后再填入的 Grains 引用（Grain 的工厂需要它来调用其他 Grain） */
class GrainsHolder {
    var value: Option<Grains> = None
}

/**
 * 调用链 Grain：
 * - chain:<B>   调用 B 的 bounce:<自己>，返回 chain-done:<B 的结果>
 * - bounce:<A>  回调 A 的 leaf，返回 bounced:<A 的结果>
 * - leaf        立即返回 leaf@<自己>
 * - nested-sync 在 onCall 里同步 call 另一个 Grain（Actor 线程上阻塞等待，应立即报错）
 */
class ChainGrain <: Grain {
    let holder: GrainsHolder
    let reentrantCalls: Bool

    init(holder: GrainsHolder, reentrantCalls: Bool) {
        this.holder = holder
        this.reentrantCalls = reentrantCalls
    }

    public func reentrant(): Bool {
        reentrantCalls
    }

    public func onCall(request: Message, ctx: GrainContext): Message {
        let t = textOf(request)
        if (t == "nested-sync") {
            return holder.value.getOrThrow().call("other-${ctx.id}", StringMessage("leaf"), Duration.second * 3)
        }
        StringMessage("leaf@${ctx.id}")
    }

    public func onCallAsync(request: Message, ctx: GrainContext): TypedPromise<Message> {
        let t = textOf(request)
        let grains = holder.value.getOrThrow()
        if (t.startsWith("chain:")) {
            let other = t["chain:".size..]
            return grains.callAsync(other, StringMessage("bounce:${ctx.id}"), Duration.second * 3).map<Message>({ r =>
                StringMessage("chain-done:${textOf(r)}")
            })
        }
        if (t.startsWith("bounce:")) {
            let origin = t["bounce:".size..]
            return grains.callAsync(origin, StringMessage("leaf"), Duration.second * 3).map<Message>({ r =>
                StringMessage("bounced:${textOf(r)}")
            })
        }
        TypedPromises.successful<Message>(onCall(request, ctx))
    }
}

func chainGrains(nodes: Array<CNode>, kind: String, reentrantCalls: Bool): Array<Grains> {
    Array<Grains>(nodes.size, { i =>
        let n = nodes[i]
        let holder = GrainsHolder()
        let g = Grains(n.cluster, n.kit.system, n.remote, kind, { _ => ChainGrain(holder, reentrantCalls) },
            Duration.second * 30)
        holder.value = Some(g)
        g
    })
}

@Test
public class GrainsAsyncTest {
    /** 可重入 Grain：A→B→A 的调用链完成（A 等待 B 期间处理 B 的回调）；不可重入时与 Orleans 默认相同，调用链超时 */
    @TestCase
    public func reentrantCallChainCompletes(): Unit {
        let nodes = startCluster(2, "e12-grain-chain", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let reentrantGrains = chainGrains(nodes, "chain-r", true)
        let plainGrains = chainGrains(nodes, "chain-p", false)
        @Expect(waitOwners(reentrantGrains, 2) && waitOwners(plainGrains, 2), true)
        for (k in 0..5) {
            @Expect(textOf(reentrantGrains[k % 2].call("a${k}", StringMessage("chain:b${k}"), CLUSTER_WAIT)),
                "chain-done:bounced:leaf@a${k}")
        }
        var failed = false
        try {
            plainGrains[0].call("a", StringMessage("chain:b"), CLUSTER_WAIT)
        } catch (_: GrainCallException) {
            failed = true
        }
        @Expect(failed, true)
        closeAll(nodes)
    }

    /** callAsync 不阻塞调用线程：一个线程同时发出 200 个调用，再一起等结果 */
    @TestCase
    public func callAsyncIssuesManyCallsWithoutBlocking(): Unit {
        let nodes = startCluster(2, "e12-grain-async", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let grains = chainGrains(nodes, "chain-a", false)
        @Expect(waitOwners(grains, 2), true)
        let started = MonoTime.now()
        let calls = Array<TypedPromise<Message>>(200, { k => grains[0].callAsync("g${k % 20}", StringMessage("leaf"), CLUSTER_WAIT) })
        // 发出 200 个调用本身不等待任何应答
        @Expect(MonoTime.now() - started < Duration.second * 2, true)
        let all = TypedPromises.sequence<Message>(calls).get(CLUSTER_WAIT)
        for (k in 0..200) {
            @Expect(textOf(all[k]), "leaf@g${k % 20}")
        }
        closeAll(nodes)
    }

    /** 在 Grain（Actor 线程）里同步 call：立即以错误返回，而不是占住工作线程直到超时 */
    @TestCase
    public func blockingCallInsideGrainFailsFast(): Unit {
        let nodes = startCluster(1, "e12-grain-sync", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        let grains = chainGrains(nodes, "chain-s", false)
        @Expect(waitOwners(grains, 1), true)
        let started = MonoTime.now()
        var message = ""
        try {
            grains[0].call("x", StringMessage("nested-sync"), CLUSTER_WAIT)
        } catch (e: GrainCallException) {
            message = e.message
        }
        @Expect(message.contains("不能在 Actor 处理方法内阻塞"), true)
        @Expect(MonoTime.now() - started < Duration.second * 3, true)
        closeAll(nodes)
    }
}

@Test
public class PubSubSerialWritesTest {
    /** N12：退订与紧随其后的订阅交替 30 次，最后仍有订阅者的节点必须留在主题里（此前异步删除可能晚于添加） */
    @TestCase
    public func resubscribeRightAfterLastUnsubscribeKeepsNode(): Unit {
        let ps = pubsubCluster("e12-ps-race")
        let rec = EventRecorder()
        let a = ps[1].node.kit.actorOf({ => Sub("a", rec) }, "a")
        @Expect(ps[1].pubsub.subscribeAsync("t", a).get(CLUSTER_WAIT) == (), true)
        // 本地已生效：subscribeAsync 完成时本节点的节点集合里已有自己
        @Expect(ps[1].pubsub.subscriberNodes("t").contains(ps[1].node.address()), true)
        for (_ in 0..30) {
            ps[1].pubsub.unsubscribe("t", a)
            ps[1].pubsub.subscribe("t", a)
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ps[0].pubsub.subscriberNodes("t") == [ps[1].node.address()] }), true)
        // 再等几个 gossip 周期：节点没有被迟到的删除移出
        @Expect(awaitGossipRounds([ps[0].node.cluster, ps[1].node.cluster], 3, CLUSTER_WAIT), true)
        @Expect(ps[0].pubsub.subscriberNodes("t"), [ps[1].node.address()])
        @Expect(ps[1].pubsub.subscriberNodes("t"), [ps[1].node.address()])
        @Expect(ps[0].pubsub.publish("t", StringMessage("m")), 1)
        @Expect(rec.await("a:m", CLUSTER_WAIT), true)
        closePubSub(ps)
    }
}

@Test
public class MembershipCleanupTest {
    /** 离开的成员被移除后只留墓碑，成员记录与故障检测器一并删除；墓碑到期后清除 */
    @TestCase
    public func removedMemberIsPurgedAndTombstonePruned(): Unit {
        let settings = FAST.withPruneTombstonesAfter(Duration.second * 2)
        let nodes = startCluster(3, "e12-purge", settings)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        nodes[2].cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[0].cluster.state().members.size == 2 }), true)
        // (gossip 中的成员记录, 墓碑, 故障检测器)
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            let (records, tombs, detectors) = nodes[0].cluster.membershipStats()
            records == 2 && tombs == 1 && detectors == 1
        }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            let (records, tombs, _) = nodes[1].cluster.membershipStats()
            records == 2 && tombs == 1
        }), true)
        // 墓碑在 2 s 后清除（每个节点按 gossip 中带的移除时间清除，不会被别的节点再传回来）
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            nodes[0].cluster.membershipStats()[1] == 0 && nodes[1].cluster.membershipStats()[1] == 0
        }), true)
        @Expect(awaitGossipRounds([nodes[0].cluster, nodes[1].cluster], 2, CLUSTER_WAIT), true)
        @Expect(nodes[0].cluster.membershipStats()[1], 0)
        @Expect(nodes[0].cluster.state().upCount(), 2)
        closeAll(nodes)
    }

    /** 非成员地址发来的心跳不会建故障检测器（此前任意来源都会建一个，永不删除） */
    @TestCase
    public func heartbeatsFromNonMembersCreateNoDetectors(): Unit {
        let nodes = startCluster(2, "e12-hb", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let stranger = RemoteNode("e12-hb-stranger")
        for (i in 0..50) {
            stranger.remote.sendControl(nodes[0].addr(), "cluster",
                StringMessage(FieldWriter().put("HB").put("10.255.0.${i}:7000").done()))
        }
        @Expect(awaitGossipRounds([nodes[0].cluster, nodes[1].cluster], 2, CLUSTER_WAIT), true)
        @Expect(nodes[0].cluster.membershipStats()[2], 1)
        stranger.close()
        closeAll(nodes)
    }

    /** 被 down 后移除的成员被隔离（对标 Akka：移除 Down 成员时隔离其 UID），入站连接随之关闭 */
    @TestCase
    public func downedMemberIsQuarantinedOnRemoval(): Unit {
        let nodes = startCluster(3, "e12-quarantine", FAST_AUTODOWN)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let before = nodes[0].remote.quarantinedCount()
        nodes[2].crash()
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[0].cluster.state().members.size == 2 }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[0].remote.quarantinedCount() > before }), true)
        @Expect(nodes[0].remote.isQuarantined(nodes[2].addr()), true)
        nodes[0].close()
        nodes[1].close()
    }
}
