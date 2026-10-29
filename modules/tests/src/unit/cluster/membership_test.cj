/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R6 测试：集群成员协议与 Cluster Singleton
 * 全部在本机回环 TCP 上的真实节点之间进行；宕机用“停止远程管理器 + 停止集群守护 Actor”模拟。
 */
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

let FAST = ClusterSettings(Duration.millisecond * 50, Duration.millisecond * 50, 8.0, Duration.millisecond * 300,
    None<Duration>)
let FAST_AUTODOWN = ClusterSettings(Duration.millisecond * 50, Duration.millisecond * 50, 8.0,
    Duration.millisecond * 300, Some(Duration.millisecond * 300))
let CLUSTER_WAIT = Duration.second * 15

class CNode {
    let kit: ActorTestKit
    let remote: SimpleRemoteManager
    var clusterValue: Option<Cluster> = None
    let events = EventRecorder()

    init(name: String) {
        kit = ActorTestKit(name)
        remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        remote.start()
    }

    func join(seeds: Array<NetworkAddress>, settings: ClusterSettings): CNode {
        let c = Cluster(kit.system, remote, seeds, settings)
        let rec = events
        c.subscribe({ e => rec.record(e.toString()) })
        clusterValue = Some(c)
        this
    }

    prop cluster: Cluster {
        get() { clusterValue.getOrThrow() }
    }

    func addr(): NetworkAddress {
        remote.getLocalAddress()
    }

    func address(): String {
        addr().toString()
    }

    /** 模拟宕机：不通知任何成员 */
    func crash(): Unit {
        cluster.shutdown()
        remote.stop()
        kit.shutdown()
    }

    func close(): Unit {
        cluster.shutdown()
        remote.stop()
        kit.shutdown()
    }
}

func startCluster(n: Int64, name: String, settings: ClusterSettings): Array<CNode> {
    let nodes = Array<CNode>(n, { i => CNode("${name}-${i}") })
    let seed = nodes[0].addr()
    for (node in nodes) {
        node.join([seed], settings)
    }
    nodes
}

func allUp(nodes: Array<CNode>, count: Int64): Bool {
    for (n in nodes) {
        let s = n.cluster.state()
        if (s.upCount() != count || !s.converged) {
            return false
        }
    }
    true
}

func closeAll(nodes: Array<CNode>): Unit {
    for (n in nodes) {
        if (!n.cluster.isTerminated()) {
            n.close()
        } else {
            n.remote.stop()
            n.kit.shutdown()
        }
    }
}

func minAddress(nodes: Array<CNode>): String {
    var best = nodes[0].address()
    for (n in nodes) {
        if (n.address() < best) {
            best = n.address()
        }
    }
    best
}

@Test
public class MembershipTest {
    @TestCase
    public func testThreeNodesJoinConvergeAndAgree(): Unit {
        let nodes = startCluster(3, "cm-join", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let expectedLeader = minAddress(nodes)
        // 所有节点对成员、upNumber、Leader 的看法一致
        let reference = nodes[0].cluster.state()
        let ups = HashSet<Int64>()
        for (m in reference.members) {
            ups.add(m.upNumber)
        }
        @Expect(ups.size, 3)
        @Expect(ups.contains(1) && ups.contains(2) && ups.contains(3), true)
        for (n in nodes) {
            let s = n.cluster.state()
            @Expect(s.leader, Some(expectedLeader))
            for (m in reference.members) {
                @Expect(s.member(m.address).getOrThrow().upNumber, m.upNumber)
            }
            @Expect(n.cluster.isUp(), true)
            // 种子节点最先 Up，是最老的成员
            @Expect(n.cluster.oldest().getOrThrow().address, nodes[0].address())
        }
        // 每个节点都收到了三个成员的 MemberUp
        for (n in nodes) {
            for (m in nodes) {
                @Expect(n.events.await("MemberUp(${m.address()})", CLUSTER_WAIT), true)
            }
        }
        closeAll(nodes)
    }

    @TestCase
    public func testGracefulLeave(): Unit {
        let nodes = startCluster(3, "cm-leave", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let leaving = nodes[2]
        leaving.cluster.leave()
        let rest = [nodes[0], nodes[1]]
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            rest[0].cluster.state().member(leaving.address()).isNone() &&
            rest[1].cluster.state().member(leaving.address()).isNone()
        }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => leaving.cluster.isTerminated() }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(rest, 2) }), true)
        // 事件顺序：Left → Exited → Removed
        let ev = nodes[0].events
        let a = leaving.address()
        @Expect(ev.await("MemberRemoved(${a})", CLUSTER_WAIT), true)
        @Expect(ev.indexOf("MemberLeft(${a})") < ev.indexOf("MemberExited(${a})"), true)
        @Expect(ev.indexOf("MemberExited(${a})") < ev.indexOf("MemberRemoved(${a})"), true)
        closeAll(nodes)
    }

    @TestCase
    public func testWholeClusterLeavesOneByOneThenTogether(): Unit {
        let nodes = startCluster(3, "cm-allleave", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        // 种子（也是最老的成员）先离开
        nodes[0].cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[0].cluster.isTerminated() }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[1].cluster.state().member(nodes[0].address()).isNone() }), true)
        // 剩下的两个同时离开（包括当前 Leader 自己）
        nodes[1].cluster.leave()
        nodes[2].cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[1].cluster.isTerminated() && nodes[2].cluster.isTerminated() }), true)
        closeAll(nodes)
    }

    @TestCase
    public func testCrashIsDetectedAndManualDownRemovesIt(): Unit {
        let nodes = startCluster(3, "cm-crash", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let victim = nodes[2]
        let va = victim.address()
        victim.crash()
        @Expect(nodes[0].events.await("UnreachableMember(${va})", CLUSTER_WAIT), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[1].cluster.state().unreachable == [va] }), true)
        // 不可达成员阻止收敛：不 down 就一直留在成员表里
        sleep(Duration.millisecond * 500)
        @Expect(nodes[0].cluster.state().member(va).isSome(), true)
        @Expect(nodes[0].cluster.state().converged, false)
        nodes[1].cluster.down(va)
        let rest = [nodes[0], nodes[1]]
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            rest[0].cluster.state().member(va).isNone() && rest[1].cluster.state().member(va).isNone()
        }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(rest, 2) }), true)
        @Expect(nodes[0].events.contains("MemberDowned(${va})"), true)
        nodes[0].close()
        nodes[1].close()
    }

    @TestCase
    public func testAutoDownAndLeaderFailover(): Unit {
        let nodes = startCluster(3, "cm-autodown", FAST_AUTODOWN)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        // 让 Leader 宕机：剩下的节点自动 down 它，并选出新的 Leader
        var leaderIdx = 0
        for (i in 0..3) {
            if (nodes[i].address() == minAddress(nodes)) {
                leaderIdx = i
            }
        }
        let leaderAddr = nodes[leaderIdx].address()
        nodes[leaderIdx].crash()
        let rest = ArrayList<CNode>()
        for (i in 0..3) {
            if (i != leaderIdx) {
                rest.add(nodes[i])
            }
        }
        let survivors = rest.toArray()
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(survivors, 2) &&
            survivors[0].cluster.state().member(leaderAddr).isNone() }), true)
        let newLeader = minAddress(survivors)
        for (n in survivors) {
            @Expect(n.cluster.leader(), Some(newLeader))
            @Expect(n.events.await("LeaderChanged(${newLeader})", CLUSTER_WAIT), true)
        }
        for (n in survivors) {
            n.close()
        }
    }

    @TestCase
    public func testJoinThroughAnySeedAndLateJoiner(): Unit {
        let nodes = startCluster(2, "cm-late", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        // 新节点的种子列表里第一个不可用，第二个是现有成员
        let dead = CNode("cm-late-dead")
        let deadAddr = dead.addr()
        dead.remote.stop()
        dead.kit.shutdown()
        let late = CNode("cm-late-new").join([deadAddr, nodes[1].addr()], FAST)
        let all = [nodes[0], nodes[1], late]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(all, 3) }), true)
        // 晚加入的成员最年轻
        let lm = nodes[0].cluster.state().member(late.address()).getOrThrow()
        @Expect(lm.upNumber, 3)
        closeAll(all)
    }
}

// ======================= Singleton =======================

/** 记录收到的消息以及自己所在的节点 */
class Ledger <: Actor {
    let node: String
    let rec: EventRecorder
    let instances: AtomicInt64

    init(node: String, rec: EventRecorder, instances: AtomicInt64) {
        this.node = node
        this.rec = rec
        this.instances = instances
    }

    public prop name: String { get() { "ledger" } }
    public prop description: String { get() { "ledger" } }

    public func preStart(): Unit {
        instances.fetchAdd(1)
    }

    public func postStop(): Unit {
        instances.fetchSub(1)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record("${s.getContent()}@${node}")
        }
        MessageResult.Handled
    }
}

@Test
public class ClusterSingletonTest {
    @TestCase
    public func testSingleInstanceOnOldestAndFailover(): Unit {
        let nodes = startCluster(3, "cs", FAST_AUTODOWN)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let rec = EventRecorder()
        // 同一进程内全部节点共享计数，用来验证“任何时刻最多一个实例”
        let instances = AtomicInt64(0)
        let maxInstances = AtomicInt64(0)
        let singletons = Array<ClusterSingleton>(3, { i =>
            let n = nodes[i]
            let addr = n.address()
            ClusterSingleton(n.cluster, n.kit.system, n.remote, "ledger", { => Ledger(addr, rec, instances) })
        })
        let watching = AtomicBool(true)
        let watcher = spawn {
            while (watching.load()) {
                let v = instances.load()
                if (v > maxInstances.load()) {
                    maxInstances.store(v)
                }
                sleep(Duration.millisecond)
            }
        }
        let oldest = nodes[0].address()
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[0].isRunningHere() }), true)
        @Expect(singletons[1].isRunningHere() || singletons[2].isRunningHere(), false)
        // 从每个节点通过代理发送
        for (i in 0..3) {
            for (k in 0..5) {
                singletons[i].tell(StringMessage("m${i}-${k}"))
            }
        }
        @Expect(rec.awaitSize(15, CLUSTER_WAIT), true)
        for (e in rec.snapshot()) {
            @Expect(e.endsWith("@${oldest}"), true)
        }

        // 最老的节点宕机：自动 down 后实例迁到下一个最老的节点
        nodes[0].crash()
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[1].isRunningHere() || singletons[2].isRunningHere() }), true)
        let nextOldest = if (singletons[1].isRunningHere()) { nodes[1].address() } else { nodes[2].address() }
        // 迁移前 node1 的成员记录中更老的那个就是 nextOldest
        @Expect(nodes[1].cluster.oldest().getOrThrow().address, nextOldest)
        rec.clear()
        singletons[1].tell(StringMessage("after-1"))
        singletons[2].tell(StringMessage("after-2"))
        @Expect(rec.awaitSize(2, CLUSTER_WAIT), true)
        for (e in rec.snapshot()) {
            @Expect(e.endsWith("@${nextOldest}"), true)
        }
        watching.store(false)
        watcher.get()
        // 崩溃节点上的实例随 ActorSystem 关闭而停止，之后新实例才启动：任何时刻最多一个
        @Expect(maxInstances.load(), 1)
        @Expect(singletons[1].startCount() + singletons[2].startCount(), 1)
        singletons[1].stop()
        singletons[2].stop()
        nodes[1].close()
        nodes[2].close()
    }

    @TestCase
    public func testGracefulHandoverOnLeave(): Unit {
        let nodes = startCluster(2, "cs-leave", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let rec = EventRecorder()
        let instances = AtomicInt64(0)
        let singletons = Array<ClusterSingleton>(2, { i =>
            let n = nodes[i]
            let addr = n.address()
            ClusterSingleton(n.cluster, n.kit.system, n.remote, "ledger", { => Ledger(addr, rec, instances) })
        })
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[0].isRunningHere() }), true)
        nodes[0].cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[1].isRunningHere() }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => !singletons[0].isRunningHere() }), true)
        singletons[1].tell(StringMessage("x"))
        @Expect(rec.await("x@${nodes[1].address()}", CLUSTER_WAIT), true)
        @Expect(instances.load(), 1)
        singletons[0].stop()
        singletons[1].stop()
        closeAll(nodes)
    }
}
