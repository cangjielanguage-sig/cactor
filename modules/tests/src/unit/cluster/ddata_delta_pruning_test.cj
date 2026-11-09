/*
 * CActor - E64 剩余部分的测试：delta 传播与移除节点的 pruning（对标 Akka delta-CRDT / RemovedNodePruning）
 *
 * delta：反熵间隔设为 60 s，收敛只能靠增量推送与断档补齐——
 *   并发增删零丢失、增量远小于整值、丢包造成的断档向来源请求整值补齐。
 * pruning：被移除节点的计数器分量并入执行者、ORSet 的点改记到执行者，总值 / 元素不变；
 *   要等全部成员都见过标记才执行；陈旧副本（含被移除节点的数据）重新连上后不重复计数；
 *   被移除节点迟到的更新被丢弃；Leader 能从数据里发现不在集群的节点；集群绑定端到端。
 * 全部在本机回环 TCP 上的真实节点之间进行。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TcpTransport, TransportProtocol, TransportHandshake}
import cactor_remote.*
import cactor_cluster.ddata.*
import cactor_cluster.membership.*
import cactor_testkit.*

let NO_ANTI_ENTROPY = Duration.second * 60
let PRUNE_WAIT = Duration.second * 15

/**
 * 可开关的丢包：开启时每 3 条丢 1 条（确定性，必然造成序号断档）。
 * 握手等钩子都转发给内层传输——对端是普通 TCP 传输，要求先完成远程层握手
 */
class SwitchDrop <: NetworkTransport {
    private let inner = TcpTransport(NetworkAddress("127.0.0.1", 0))
    let dropping = AtomicBool(false)
    let dropped = AtomicInt64(0)
    private let counter = AtomicInt64(0)

    public func start(): Unit { inner.start() }
    public func stop(): Unit { inner.stop() }
    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit { inner.setDataHandler(handler) }
    public func getLocalAddress(): NetworkAddress { inner.getLocalAddress() }
    public func isActive(): Bool { inner.isActive() }
    public func setHandshake(handshake: TransportHandshake): Unit { inner.setHandshake(handshake) }
    public func closeInboundFrom(peerKey: String): Int64 { inner.closeInboundFrom(peerKey) }
    public func resetOutbound(address: NetworkAddress): Unit { inner.resetOutbound(address) }

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        if (dropping.load() && counter.fetchAdd(1) % 3 == 1) {
            dropped.fetchAdd(1)
            return
        }
        inner.send(address, data)
    }
}

/** 可指定反熵间隔、传输与节点标识的副本节点 */
class DNode {
    let kit: ActorTestKit
    let remote: SimpleRemoteManager
    let rep: Replicator

    init(name: String, gossip: Duration) {
        this(name, gossip, None<NetworkTransport>, None<String>)
    }

    init(name: String, gossip: Duration, transport: Option<NetworkTransport>, selfNode: Option<String>) {
        kit = ActorTestKit(name)
        remote = match (transport) {
            case Some(t) => SimpleRemoteManager(t)
            case None => SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        }
        remote.start()
        rep = match (selfNode) {
            case Some(id) => Replicator(kit.system, remote, gossip, id)
            case None => Replicator(kit.system, remote, gossip)
        }
    }

    func addr(): NetworkAddress {
        remote.getLocalAddress()
    }

    func id(): String {
        rep.selfNode
    }

    func shutdown(): Unit {
        rep.stop()
        remote.stop()
        kit.shutdown()
    }
}

func connect(nodes: Array<DNode>): Unit {
    for (a in nodes) {
        for (b in nodes) {
            if (!refEq(a, b)) {
                a.rep.join(b.addr())
            }
        }
    }
}

func disconnect(a: DNode, b: DNode): Unit {
    a.rep.leave(b.addr())
    b.rep.leave(a.addr())
}

func nodesIn(rep: Replicator, key: String): HashSet<String> {
    match (rep.get(key)) {
        case Some(v) => HashSet<String>(v.modifiedByNodes())
        case None => HashSet<String>()
    }
}

/** 键的值里已没有 node 的数据（分量、点、版本向量条目） */
func freeOf(rep: Replicator, key: String, node: String): Bool {
    match (rep.get(key)) {
        case Some(v) => !v.needsPruning(node)
        case None => false
    }
}

/** 以 leader 为发起者、members 为成员视图，接上 pruning（不经集群，便于精确控制） */
func enablePruning(leader: DNode, others: Array<DNode>, members: () -> Array<String>): Unit {
    leader.rep.setMembership({ => true }, members)
    for (o in others) {
        o.rep.setMembership({ => false }, members)
    }
    for (n in [leader]) {
        n.rep.configurePruning(interval: Duration.millisecond * 50, maxDissemination: Duration.millisecond * 200)
    }
    for (n in others) {
        n.rep.configurePruning(interval: Duration.millisecond * 50, maxDissemination: Duration.millisecond * 200)
    }
}

func expected(n: Int64, f: (Int64) -> String): Array<String> {
    let out = ArrayList<String>()
    for (i in 0..n) {
        out.add(f(i))
    }
    out.toArray()
}

@Test
public class ReplicatorDeltaTest {
    /**
     * 三个节点各自并发增删自己的元素、并发累加计数器；反熵关闭（60 s），收敛只靠增量推送。
     * 每个节点只删自己加过的元素，结果是确定的：3 × 30 个元素 + 两个节点都加过的 shared，计数 240
     */
    @TestCase
    public func concurrentUpdatesConvergeThroughDeltasAlone(): Unit {
        let nodes = Array<DNode>(3, { i => DNode("delta-conc-${i}", NO_ANTI_ENTROPY) })
        connect(nodes)
        let futures = ArrayList<Future<Unit>>()
        for (i in 0..3) {
            let n = nodes[i]
            futures.add(spawn {
                for (j in 0..40) {
                    n.rep.addToSet("items", "n${i}-${j}")
                    n.rep.incrementCounter("count", i + 1)
                    if (j % 4 == 0) {
                        n.rep.removeFromSet("items", "n${i}-${j}")
                    }
                }
                if (i < 2) {
                    n.rep.addToSet("items", "shared")
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        let want = ArrayList<String>()
        for (i in 0..3) {
            for (j in 0..40) {
                if (j % 4 != 0) {
                    want.add("n${i}-${j}")
                }
            }
        }
        want.add("shared")
        let wantSet = HashSet<String>(want)
        for (n in nodes) {
            @Expect(awaitCondition(CONVERGE, { =>
                let got = n.rep.setElements("items")
                got.size == wantSet.size && HashSet<String>(got) == wantSet && n.rep.counterValue("count") == 240
            }), true)
        }
        // 确实走的是增量：每个节点的更新绝大多数以增量推送、对方按序应用
        for (n in nodes) {
            let st = n.rep.deltaStats()
            @Expect(st.deltaPushes > st.fullPushes * 10, true)
            @Expect(st.deltasApplied > 0, true)
        }
        for (n in nodes) {
            n.shutdown()
        }
    }

    /** 300 个元素的集合上再加 50 个：增量推送的字节数远小于整值推送（同一组操作对照） */
    @TestCase
    public func deltasAreFarSmallerThanFullState(): Unit {
        let a = DNode("delta-bytes-a", NO_ANTI_ENTROPY)
        let b = DNode("delta-bytes-b", NO_ANTI_ENTROPY)
        connect([a, b])
        for (i in 0..300) {
            a.rep.addToSet("big", "element-number-${i}")
        }
        @Expect(awaitCondition(CONVERGE, { => b.rep.setElements("big").size == 300 }), true)
        let before = a.rep.bytesSent()
        for (i in 0..50) {
            a.rep.addToSet("big", "delta-${i}")
        }
        @Expect(awaitCondition(CONVERGE, { => b.rep.setElements("big").size == 350 }), true)
        let deltaBytes = a.rep.bytesSent() - before
        a.rep.setDeltaPropagation(false)
        let before2 = a.rep.bytesSent()
        for (i in 0..50) {
            a.rep.addToSet("big", "full-${i}")
        }
        @Expect(awaitCondition(CONVERGE, { => b.rep.setElements("big").size == 400 }), true)
        let fullBytes = a.rep.bytesSent() - before2
        println("50 次添加：增量 ${deltaBytes} 字节，整值 ${fullBytes} 字节（${fullBytes / max(deltaBytes, 1)} 倍）")
        @Expect(deltaBytes * 20 < fullBytes, true)
        a.shutdown()
        b.shutdown()
    }

    /**
     * 来源每 3 条消息丢 1 条：接收方发现序号断档，不应用断档后的增量（否则版本向量会越过没收到的点，
     * 之后整值合并时把它们当成已删除），向来源请求整值补齐。反熵关闭，补齐只能来自断档请求
     */
    @TestCase
    public func sequenceGapsAreRepairedFromTheOrigin(): Unit {
        let drop = SwitchDrop()
        let a = DNode("delta-gap-a", NO_ANTI_ENTROPY, Some<NetworkTransport>(drop), None<String>)
        let b = DNode("delta-gap-b", NO_ANTI_ENTROPY)
        connect([a, b])
        a.rep.addToSet("s", "init")
        @Expect(awaitCondition(CONVERGE, { => b.rep.setElements("s").size == 1 }), true)
        drop.dropping.store(true)
        for (i in 0..60) {
            a.rep.addToSet("s", "e${i}")
        }
        drop.dropping.store(false)
        a.rep.addToSet("s", "final")
        @Expect(drop.dropped.load() > 0, true)
        @Expect(awaitCondition(CONVERGE, { => b.rep.setElements("s").size == 62 }), true)
        @Expect(b.rep.deltaStats().gaps > 0, true)
        a.shutdown()
        b.shutdown()
    }
}

@Test
public class ReplicatorPruningTest {
    /** 计数器与 ORSet：被移除节点的数据并入执行者，值不变，之后各节点都不再含它的数据 */
    @TestCase
    public func removedNodeDataIsCollapsedWithoutChangingValues(): Unit {
        let nodes = Array<DNode>(3, { i => DNode("prune-basic-${i}", GOSSIP) })
        let (a, b, c) = (nodes[0], nodes[1], nodes[2])
        connect(nodes)
        a.rep.incrementCounter("hits", 5)
        b.rep.incrementCounter("hits", 7)
        c.rep.incrementCounter("hits", 11)
        c.rep.incrementCounter("hits", -1)
        c.rep.addToSet("s", "from-c")
        a.rep.addToSet("s", "from-a")
        for (n in nodes) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.counterValue("hits") == 22 && n.rep.setElements("s").size == 2 }), true)
        }
        let removed = c.id()
        @Expect(nodesIn(a.rep, "hits").contains(removed), true)
        c.shutdown()
        a.rep.leave(c.addr())
        b.rep.leave(c.addr())
        let members = [a.id(), b.id()]
        enablePruning(a, [b], { => members })
        a.rep.nodeRemoved(removed)
        b.rep.nodeRemoved(removed)
        for (n in [a, b]) {
            @Expect(awaitCondition(PRUNE_WAIT, { => freeOf(n.rep, "hits", removed) && freeOf(n.rep, "s", removed) }), true)
            @Expect(n.rep.counterValue("hits"), 22)
            @Expect(n.rep.setElements("s"), ["from-a", "from-c"])
            @Expect(nodesIn(n.rep, "hits").size, 2)
        }
        @Expect(a.rep.pruningStats()[1] >= 2, true)
        // 执行者继续更新：被并入的部分不丢、不重复
        a.rep.incrementCounter("hits", 1)
        b.rep.removeFromSet("s", "from-c")
        for (n in [a, b]) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.counterValue("hits") == 23 && n.rep.setElements("s") == ["from-a"] }), true)
        }
        a.shutdown()
        b.shutdown()
    }

    /** 成员视图里有一个从不登记的成员：只发起、不执行；它离开视图后才执行 */
    @TestCase
    public func pruningWaitsUntilEveryMemberHasSeenTheMarker(): Unit {
        let nodes = Array<DNode>(3, { i => DNode("prune-seen-${i}", GOSSIP) })
        let (a, b, c) = (nodes[0], nodes[1], nodes[2])
        connect(nodes)
        c.rep.incrementCounter("hits", 4)
        a.rep.incrementCounter("hits", 1)
        for (n in [a, b]) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.counterValue("hits") == 5 }), true)
        }
        let removed = c.id()
        c.shutdown()
        a.rep.leave(c.addr())
        b.rep.leave(c.addr())
        let phantom = AtomicBool(true)
        let members = { =>
            if (phantom.load()) {
                [a.id(), b.id(), "127.0.0.1:1#424242"]
            } else {
                [a.id(), b.id()]
            }
        }
        enablePruning(a, [b], members)
        a.rep.nodeRemoved(removed)
        b.rep.nodeRemoved(removed)
        @Expect(awaitCondition(PRUNE_WAIT, { => a.rep.pruningStats()[0] >= 1 }), true)
        // 不该执行：phantom 从未登记
        @Expect(awaitCondition(Duration.second, { => a.rep.pruningStats()[1] > 0 }), false)
        @Expect(freeOf(a.rep, "hits", removed), false)
        phantom.store(false)
        for (n in [a, b]) {
            @Expect(awaitCondition(PRUNE_WAIT, { => freeOf(n.rep, "hits", removed) }), true)
            @Expect(n.rep.counterValue("hits"), 5)
        }
        a.shutdown()
        b.shutdown()
    }

    /**
     * 陈旧副本：d 在 pruning 期间与其他节点断开（不在成员视图中），手里还有被移除节点 c 的分量；
     * 重新连上后，合并先剥掉 c 的数据（已执行标记），不会把 c 的 11 再加一遍，d 自己也被清理
     */
    @TestCase
    public func staleReplicaRejoiningDoesNotResurrectPrunedData(): Unit {
        let nodes = Array<DNode>(4, { i => DNode("prune-stale-${i}", GOSSIP) })
        let (a, b, c, d) = (nodes[0], nodes[1], nodes[2], nodes[3])
        connect(nodes)
        a.rep.incrementCounter("hits", 5)
        b.rep.incrementCounter("hits", 7)
        c.rep.incrementCounter("hits", 11)
        c.rep.addToSet("s", "from-c")
        for (n in nodes) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.counterValue("hits") == 23 && n.rep.setElements("s").size == 1 }), true)
        }
        let removed = c.id()
        disconnect(d, a)
        disconnect(d, b)
        disconnect(d, c)
        c.shutdown()
        a.rep.leave(c.addr())
        b.rep.leave(c.addr())
        let members = [a.id(), b.id()]
        enablePruning(a, [b], { => members })
        a.rep.nodeRemoved(removed)
        b.rep.nodeRemoved(removed)
        for (n in [a, b]) {
            @Expect(awaitCondition(PRUNE_WAIT, { => freeOf(n.rep, "hits", removed) && freeOf(n.rep, "s", removed) }), true)
        }
        @Expect(freeOf(d.rep, "hits", removed), false)
        // d 带着陈旧数据回来
        connect([a, b, d])
        for (n in [a, b, d]) {
            @Expect(awaitCondition(CONVERGE, { => freeOf(n.rep, "hits", removed) && freeOf(n.rep, "s", removed) }), true)
        }
        // 再等几轮反熵：值稳定在 23，不会因为 d 的陈旧分量变成 34
        @Expect(awaitCondition(Duration.millisecond * 600, { =>
            a.rep.counterValue("hits") != 23 || b.rep.counterValue("hits") != 23 || d.rep.counterValue("hits") != 23
        }), false)
        for (n in [a, b, d]) {
            @Expect(n.rep.setElements("s"), ["from-c"])
        }
        a.shutdown()
        b.shutdown()
        d.shutdown()
    }

    /** 被移除的节点其实还活着（僵尸）并继续更新：pruning 后它的增量被丢弃，计数不会重复 */
    @TestCase
    public func lateUpdatesFromAPrunedNodeAreIgnored(): Unit {
        let nodes = Array<DNode>(3, { i => DNode("prune-zombie-${i}", GOSSIP) })
        let (a, b, c) = (nodes[0], nodes[1], nodes[2])
        connect(nodes)
        a.rep.incrementCounter("hits", 5)
        c.rep.incrementCounter("hits", 11)
        for (n in nodes) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.counterValue("hits") == 16 }), true)
        }
        let removed = c.id()
        // c 仍然连着 b（b 不再主动向 c 推送，但收得到 c 的消息）
        a.rep.leave(c.addr())
        b.rep.leave(c.addr())
        let members = [a.id(), b.id()]
        enablePruning(a, [b], { => members })
        a.rep.nodeRemoved(removed)
        b.rep.nodeRemoved(removed)
        for (n in [a, b]) {
            @Expect(awaitCondition(PRUNE_WAIT, { => freeOf(n.rep, "hits", removed) }), true)
        }
        // 订阅记录每一次变化：短暂的重复计数（下一轮反熵才被剥掉）也算失败
        let wrong = AtomicInt64(0)
        for (n in [a, b]) {
            n.rep.subscribe("hits", { v =>
                match (v) {
                    case ReplicatedValue.PNCounterValue(x) where x.value() != 16 => wrong.store(x.value())
                    case _ => ()
                }
            })
        }
        for (_ in 0..5) {
            c.rep.incrementCounter("hits", 1)
        }
        @Expect(awaitCondition(Duration.millisecond * 800, { =>
            wrong.load() != 0 || a.rep.counterValue("hits") != 16 || b.rep.counterValue("hits") != 16 ||
                !freeOf(b.rep, "hits", removed)
        }), false)
        @Expect(wrong.load(), 0)
        for (n in nodes) {
            n.shutdown()
        }
    }

    /**
     * Leader 没收到移除通知（例如同址重启后的新实例成了 Leader），也能从数据里发现不在集群的节点并清理；
     * 不是 "地址#uid" 形式的标识（自定义节点名）不会被当成已移除
     */
    @TestCase
    public func leaderFindsNodesThatAreNoLongerMembersInTheData(): Unit {
        let a = DNode("prune-scan-a", GOSSIP)
        let b = DNode("prune-scan-b", GOSSIP)
        let ghost = DNode("prune-scan-ghost", GOSSIP, None<NetworkTransport>, Some("127.0.0.1:1#4242"))
        let custom = DNode("prune-scan-custom", GOSSIP, None<NetworkTransport>, Some("custom-node"))
        connect([a, b, ghost, custom])
        a.rep.incrementCounter("hits", 2)
        ghost.rep.incrementCounter("hits", 3)
        custom.rep.incrementCounter("hits", 4)
        ghost.rep.addToSet("s", "g")
        for (n in [a, b]) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.counterValue("hits") == 9 && n.rep.setElements("s").size == 1 }), true)
        }
        for (n in [ghost, custom]) {
            a.rep.leave(n.addr())
            b.rep.leave(n.addr())
            n.shutdown()
        }
        let members = [a.id(), b.id()]
        enablePruning(a, [b], { => members })
        for (n in [a, b]) {
            @Expect(awaitCondition(PRUNE_WAIT, { => freeOf(n.rep, "hits", "127.0.0.1:1#4242") && freeOf(n.rep, "s", "127.0.0.1:1#4242") }), true)
            @Expect(n.rep.counterValue("hits"), 9)
            @Expect(n.rep.setElements("s"), ["g"])
            @Expect(nodesIn(n.rep, "hits").contains("custom-node"), true)
        }
        a.shutdown()
        b.shutdown()
    }

    /** 集群绑定端到端：节点优雅离开后，Leader 自动清理它的数据，计数不变 */
    @TestCase
    public func clusterBindingPrunesANodeThatLeft(): Unit {
        let nodes = startCluster(3, "prune-cluster", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let reps = Array<Replicator>(3, { i => Replicator(nodes[i].kit.system, nodes[i].remote, GOSSIP) })
        for (i in 0..3) {
            reps[i].configurePruning(interval: Duration.millisecond * 50, maxDissemination: Duration.millisecond * 300)
            ClusterReplicatorBinding.bind(nodes[i].cluster, reps[i])
            reps[i].incrementCounter("hits", i + 1)
        }
        for (r in reps) {
            @Expect(awaitCondition(CONVERGE, { => r.counterValue("hits") == 6 }), true)
        }
        let leaving = nodes[2].cluster.selfUniqueAddress()
        @Expect(reps[2].selfNode, leaving)
        nodes[2].cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[2].cluster.isTerminated() }), true)
        reps[2].stop()
        for (i in 0..2) {
            @Expect(awaitCondition(PRUNE_WAIT, { => freeOf(reps[i], "hits", leaving) }), true)
            @Expect(reps[i].counterValue("hits"), 6)
            @Expect(nodesIn(reps[i], "hits").size, 2)
        }
        reps[1].incrementCounter("hits", 1)
        for (i in 0..2) {
            @Expect(awaitCondition(CONVERGE, { => reps[i].counterValue("hits") == 7 }), true)
        }
        reps[0].stop()
        reps[1].stop()
        closeAll(nodes)
    }
}
