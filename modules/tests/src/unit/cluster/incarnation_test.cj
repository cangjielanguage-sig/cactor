/*
 * CActor - E7′（N4 / N5）：节点实例身份与种子加入
 * - 同一地址重启的进程是新实例：旧实例被 down、移除后新实例重新 Joining → Up，不继承旧身份；
 * - 首个种子重启后加入原集群，不另起一个集群；
 * - 首个种子启动时先问其他种子：全部 NACK 立即组成集群，没有回应则等 seedNodeTimeout；
 * - 同一地址重启后的 CRDT 写入不被旧进程的分量吞掉。
 * 全部在本机回环 TCP 上的真实节点之间进行。
 */
package cactor_tests.unit.cluster

import std.net.*
import std.time.*
import std.random.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TcpTransport}
import cactor_remote.*
import cactor_cluster.membership.*
import cactor_cluster.ddata.*
import cactor_testkit.*

let RESTART_WAIT = Duration.second * 10

func tcpAt(port: Int32): NetworkTransport {
    TcpTransport(NetworkAddress("127.0.0.1", port))
}

/**
 * 要在原端口重启的节点用这个端口：取自 Linux 临时端口段（默认 32768–60999）之外。
 * 绑定 0 端口拿到的是临时端口，节点宕机释放后，并行跑的其他测试进程发起的出站连接
 * 随时可能把它占作源端口，原端口重启就会 "Address already in use"（并行跑套件时实测出现）。
 */
func restartablePort(): Int32 {
    let rnd = Random()
    for (_ in 0..100) {
        let port = UInt16(20000 + rnd.nextUInt64() % 12000)
        try {
            let probe = TcpServerSocket(bindAt: port)
            probe.reuseAddress = true
            probe.bind()
            probe.close()
            return Int32(port)
        } catch (_: Exception) {}
    }
    throw IllegalStateException("20000–32000 之间找不到空闲端口")
}

/** 所有节点都把 addr 记为 uid 这个实例 */
func allSeeInstance(nodes: Array<CNode>, addr: String, uid: UInt64): Bool {
    for (n in nodes) {
        match (n.cluster.state().member(addr)) {
            case Some(m) where m.uid == uid => ()
            case _ => return false
        }
    }
    true
}

func lastIndexOf(rec: EventRecorder, event: String): Int64 {
    let all = rec.snapshot()
    var i = all.size - 1
    while (i >= 0 && all[i] != event) {
        i--
    }
    i
}

@Test
public class IncarnationTest {
    @TestCase
    public func restartedMemberRejoinsAsNewIncarnation(): Unit {
        let nodes = [CNode("inc-restart-0"), CNode("inc-restart-1"), CNode("inc-restart-2", tcpAt(restartablePort()))]
        for (n in nodes) {
            n.join([nodes[0].addr()], FAST)
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let victim = nodes[2]
        let va = victim.address()
        let oldUid = victim.cluster.selfUid
        victim.crash()
        // 同一端口立即重启：新进程 = 新启动标识
        let reborn = CNode("inc-restart-2b", tcpAt(victim.addr().port)).join([nodes[0].addr()], FAST)
        @Expect(reborn.cluster.selfUid != oldUid, true)
        let all = [nodes[0], nodes[1], reborn]
        @Expect(awaitCondition(RESTART_WAIT, { => allUp(all, 3) && allSeeInstance(all, va, reborn.cluster.selfUid) }), true)
        @Expect(reborn.cluster.isUp(), true)
        // 旧实例先被移除，新实例作为新成员再次 Up（此前新进程直接继承旧成员的 Up 状态与 upNumber）
        for (n in [nodes[0], nodes[1]]) {
            @Expect(n.events.await("MemberRemoved(${va})", CLUSTER_WAIT), true)
            @Expect(awaitCondition(CLUSTER_WAIT, { => n.events.count("MemberUp(${va})") == 2 }), true)
            // 顺序：旧实例的移除在新实例的 Up 之前
            @Expect(n.events.indexOf("MemberRemoved(${va})") < lastIndexOf(n.events, "MemberUp(${va})"), true)
        }
        closeAll(all)
    }

    @TestCase
    public func restartedFirstSeedRejoinsAfterItWasRemoved(): Unit {
        let nodes = Array<CNode>(3, { i => if (i == 0) { CNode("inc-seedrm-0", tcpAt(restartablePort())) } else { CNode("inc-seedrm-${i}") } })
        let seeds = [nodes[0].addr(), nodes[1].addr()]
        for (n in nodes) {
            n.join(seeds, FAST_AUTODOWN)
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let sa = nodes[0].address()
        let port = nodes[0].addr().port
        nodes[0].crash()
        let rest = [nodes[1], nodes[2]]
        // 自动 down 并移除宕机的首个种子
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            rest[0].cluster.state().member(sa).isNone() && rest[1].cluster.state().member(sa).isNone() && allUp(rest, 2)
        }), true)
        // 首个种子在原地址重启：应加入原集群，而不是自己组成一个只有自己的集群（此前：被拒绝加入，成为孤岛）
        let reborn = CNode("inc-seedrm-0b", tcpAt(port)).join(seeds, FAST_AUTODOWN)
        let all = [reborn, nodes[1], nodes[2]]
        @Expect(awaitCondition(RESTART_WAIT, { => allUp(all, 3) }), true)
        @Expect(reborn.cluster.state().members.size, 3)
        // 重启的种子是最年轻的成员，不是 Singleton 所在的最老成员
        @Expect(reborn.cluster.oldest().getOrThrow().address != sa, true)
        closeAll(all)
    }

    @TestCase
    public func restartedFirstSeedWaitsForOldInstanceInsteadOfFormingIsland(): Unit {
        let nodes = Array<CNode>(3, { i => if (i == 0) { CNode("inc-seedfast-0", tcpAt(restartablePort())) } else { CNode("inc-seedfast-${i}") } })
        let seeds = [nodes[0].addr(), nodes[1].addr()]
        for (n in nodes) {
            n.join(seeds, FAST)
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let sa = nodes[0].address()
        let other = nodes[1].address()
        let port = nodes[0].addr().port
        // 没有自动 down：旧实例只能靠新实例的 JOIN 被替换
        nodes[0].crash()
        let reborn = CNode("inc-seedfast-0b", tcpAt(port)).join(seeds, FAST)
        let all = [reborn, nodes[1], nodes[2]]
        @Expect(awaitCondition(RESTART_WAIT, { => allUp(all, 3) && allSeeInstance(all, sa, reborn.cluster.selfUid) }), true)
        // 重启的种子先看到原集群的成员 Up，自己后 Up：没有先自己组成集群
        let ev = reborn.events
        @Expect(ev.await("MemberUp(${sa})", CLUSTER_WAIT), true)
        @Expect(ev.indexOf("MemberUp(${other})") >= 0, true)
        @Expect(ev.indexOf("MemberUp(${other})") < ev.indexOf("MemberUp(${sa})"), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[1].events.count("MemberUp(${sa})") == 2 }), true)
        closeAll(all)
    }

    @TestCase
    public func freshSeedsFormOneClusterWithoutWaitingForTimeout(): Unit {
        // 超时设得很长：只有"其他种子全部 NACK"这条路径能让集群及时组成
        let settings = FAST.withSeedNodeTimeout(Duration.second * 60)
        let nodes = Array<CNode>(3, { i => CNode("inc-fresh-${i}") })
        let seeds = [nodes[0].addr(), nodes[1].addr(), nodes[2].addr()]
        for (n in nodes) {
            n.join(seeds, settings)
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        // 只有一个集群：首个种子最老
        for (n in nodes) {
            @Expect(n.cluster.oldest().getOrThrow().address, nodes[0].address())
        }
        closeAll(nodes)
    }

    @TestCase
    public func firstSeedFormsAfterTimeoutWhenOtherSeedsAreDown(): Unit {
        let dead = CNode("inc-timeout-dead")
        let deadAddr = dead.addr()
        dead.remote.stop()
        dead.kit.shutdown()
        let timeout = Duration.second
        let node = CNode("inc-timeout-0")
        let begin = MonoTime.now()
        node.join([node.addr(), deadAddr], FAST.withSeedNodeTimeout(timeout))
        @Expect(awaitCondition(CLUSTER_WAIT, { => node.cluster.isUp() }), true)
        // 另一个种子不回应：等满 seedNodeTimeout 才自己组成集群
        @Expect(MonoTime.now() - begin >= timeout, true)
        closeAll([node])
    }

    @TestCase
    public func crdtWritesAfterSameAddressRestartAreKept(): Unit {
        let a = Node("inc-rep-a", None<NetworkTransport>)
        let b = Node("inc-rep-b", Some(tcpAt(restartablePort())))
        mesh([a, b])
        b.rep.incrementCounter("c", 5)
        b.rep.addToSet("s", "x")
        @Expect(awaitCondition(CONVERGE, { => a.rep.counterValue("c") == 5 && a.rep.setElements("s") == ["x"] }), true)
        let port = b.addr().port
        let oldNode = b.rep.selfNode
        b.shutdown()
        // 同一地址的新进程：先在本地写入，再与 a 同步
        let b2 = Node("inc-rep-b2", Some(tcpAt(port)))
        @Expect(b2.rep.selfNode != oldNode, true)
        b2.rep.incrementCounter("c", 3)
        b2.rep.addToSet("s", "y")
        b2.rep.join(a.addr())
        // 此前节点标识只是地址：计数停在 5（新增的 3 被吞），集合里 x、y 都丢失
        for (n in [a, b2]) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.counterValue("c") == 8 && n.rep.setElements("s") == ["x", "y"] }), true)
        }
        shutdownAll([a, b2])
    }
}
