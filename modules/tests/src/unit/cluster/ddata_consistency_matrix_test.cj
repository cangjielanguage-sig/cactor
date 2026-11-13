/*
 * CActor - DA5：分布式数据在成员变化下的一致性验收矩阵（对标 Akka Distributed Data 的一致性边界）
 *
 * 每个用例断言一条可证伪的性质（保证）或一条明确的边界（不保证）：
 *   Q1 5 节点、2 节点不可达：不可达副本仍计入分母，少数派过半写失败；多数派过半写后，换一组过半副本读取必然读到
 *   Q2 非对称分区：只有确认回不来时过半写失败（不是虚假成功），失败的写已在部分副本生效（结果未知，不回滚）
 *   Q3 进行中的过半写在视图变化时按开始时冻结的副本数判定，新加入的对等节点不会被算作确认
 *   Q4 旧 Replicator 实例迟到的确认不能让新实例的过半写虚假成功（DA5 发现的缺陷，见 replicator.cj nextOpId）
 *   Q5 边界：两个节点的成员视图不同（一个仍是 3 节点、一个已是 5 节点）时，过半写与过半读可以不相交
 *   Q6 集群 3→5 扩容期间持续过半写：全部确认、单节点过半读单调、扩容后的过半分母为 5
 *   Q7 同址重启（新 uid）：不复活已删除的元素、计数器不重复不丢失，旧实例被 pruning 后值不变
 *   Q8 已确认的 ORSet 添加不会因写入节点宕机、被移除并 pruning 而丢失
 *   Q9 删除不会被分区期间的陈旧副本带回（叠加被移除节点的 pruning）；少数派分区期间的并发添加不丢
 *   Q10 边界：pruning 的执行等价于执行者重新添加，与之并发的删除会输给它（add-wins，与 Akka 相同）
 * 全部在本机回环 TCP 上的真实节点之间进行，不用固定睡眠（awaitCondition / 计数条件）。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TcpTransport, TransportHandshake,
    BlackholeTransport}
import cactor_remote.*
import cactor_cluster.ddata.*
import cactor_cluster.membership.*
import cactor_testkit.*

let DA5_SHORT = Duration.millisecond * 600

/**
 * 可暂扣的传输：开启后发往指定地址的帧先攒在内存里，release() 时按原顺序发出——
 * 模拟"对端已处理并回了确认，但确认在网络里滞留"，用来构造跨实例的迟到确认
 */
class HoldTransport <: NetworkTransport {
    private let inner = TcpTransport(NetworkAddress("127.0.0.1", 0))
    private let lock = Mutex()
    private var holdTo: Option<String> = None<String>
    private let held = ArrayList<(NetworkAddress, Array<UInt8>)>()

    public func start(): Unit { inner.start() }
    public func stop(): Unit { inner.stop() }
    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit { inner.setDataHandler(handler) }
    public func getLocalAddress(): NetworkAddress { inner.getLocalAddress() }
    public func isActive(): Bool { inner.isActive() }
    public func setHandshake(handshake: TransportHandshake): Unit { inner.setHandshake(handshake) }
    public func closeInboundFrom(peerKey: String): Int64 { inner.closeInboundFrom(peerKey) }
    public func resetOutbound(address: NetworkAddress): Unit { inner.resetOutbound(address) }

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        let kept = synchronized(lock) {
            match (holdTo) {
                case Some(t) where t == address.toString() =>
                    held.add((address, data))
                    true
                case _ => false
            }
        }
        if (!kept) {
            inner.send(address, data)
        }
    }

    func hold(address: String): Unit {
        synchronized(lock) {
            holdTo = Some(address)
        }
    }

    /** 暂扣的帧中含有 marker（按字节匹配）的条数 */
    func heldContaining(marker: String): Int64 {
        let m = marker.toArray()
        synchronized(lock) {
            var n: Int64 = 0
            for ((_, data) in held) {
                if (containsBytes(data, m)) {
                    n++
                }
            }
            n
        }
    }

    func release(): Unit {
        let frames = synchronized(lock) {
            holdTo = None<String>
            let out = held.toArray()
            held.clear()
            out
        }
        for ((a, d) in frames) {
            inner.send(a, d)
        }
    }
}

func containsBytes(data: Array<UInt8>, m: Array<UInt8>): Bool {
    if (m.size == 0 || data.size < m.size) {
        return m.size == 0
    }
    for (i in 0..(data.size - m.size + 1)) {
        var ok = true
        for (j in 0..m.size) {
            if (data[i + j] != m[j]) {
                ok = false
                break
            }
        }
        if (ok) {
            return true
        }
    }
    false
}

func bhNode(name: String, gossip: Duration): (DNode, BlackholeTransport) {
    let t = BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", 0)))
    (DNode(name, gossip, Some<NetworkTransport>(t), None<String>), t)
}

/** 执行 f，返回它抛出的一致性异常；没有抛出时为 None（即"成功"） */
func consistencyFailure(f: () -> Unit): Option<ReplicatorConsistencyException> {
    try {
        f()
        None<ReplicatorConsistencyException>
    } catch (e: ReplicatorConsistencyException) {
        Some(e)
    }
}

func orRemove(element: String): (ReplicatedValue) -> ReplicatedValue {
    { v =>
        match (v) {
            case ORSetValue(s) => ReplicatedValue.ORSetValue(s.remove(element))
            case _ => throw IllegalArgumentException("不是 ORSet")
        }
    }
}

func counterOf(v: Option<ReplicatedValue>): Int64 {
    match (v) {
        case Some(PNCounterValue(c)) => c.value()
        case _ => -1
    }
}

func sorted(xs: Array<String>): Array<String> {
    let l = ArrayList<String>(xs)
    l.sort()
    l.toArray()
}

@Test
public class DDataQuorumMatrixTest {
    /**
     * Q1：5 个副本分成 {a,b,c} | {d,e}。分母始终是 5（不可达副本不从分母里去掉，否则少数派会"自成多数"）：
     * 少数派的过半写失败且报告 2/5、需要 3；多数派过半写成功后，换成 {c,d,e} 一侧过半读，必然经 c 读到。
     * 反熵关闭（60 s），读到只可能来自法定集合的相交
     */
    @TestCase
    public func majorityQuorumsIntersectAcrossPartitionSwitch(): Unit {
        let partition = Partition()
        let ns = Array<DNode>(5, { i =>
            DNode("da5-q1-${i}", NO_ANTI_ENTROPY, Some<NetworkTransport>(PartitionTransport(partition)), None<String>)
        })
        connect(ns)
        let ad = Array<String>(5, { i => ns[i].addr().toString() })
        partition.split([[ad[0], ad[1], ad[2]], [ad[3], ad[4]]])
        let ex = consistencyFailure({ =>
            ns[3].rep.addToSetWith("q", "minority", WriteConsistency.WriteMajority(DA5_SHORT)).get(CONVERGE)
        })
        @Expect(ex.isSome(), true)
        if (let Some(e) <- ex) {
            @Expect(e.total, 5)
            @Expect(e.needed, 3)
            @Expect(e.replied, 2)
        }
        ns[0].rep.addToSetWith("q", "v1", WriteConsistency.WriteMajority(CONVERGE)).get(CONVERGE)
        // 确认集合正好是 {a,b,c}：d、e 都没有收到
        @Expect(ns[2].rep.setElements("q").contains("v1"), true)
        @Expect(ns[3].rep.setElements("q").contains("v1"), false)
        @Expect(ns[4].rep.setElements("q").contains("v1"), false)
        partition.split([[ad[2], ad[3], ad[4]], [ad[0], ad[1]]])
        let got = setOf(ns[4].rep.getWith("q", ReadConsistency.ReadMajority(CONVERGE)).get(CONVERGE))
        @Expect(got.contains("v1"), true)
        // 失败的过半写没有回滚：它已在 d 本地生效，同样被读到（失败 = 结果未知，不是"没发生"）
        @Expect(got.contains("minority"), true)
        // 读到的合并值写回读方本地
        @Expect(ns[4].rep.setElements("q").contains("v1"), true)
        for (n in ns) {
            n.shutdown()
        }
    }

    /**
     * Q2：非对称分区。b、c 发往 a 的帧全部丢弃：a 的 W 送达、确认回不来 → a 的过半写必须失败（不能虚假成功），
     * 而 b、c 实际已经合并了这次写；反过来 a 发往 b、c 的帧丢弃时，b 经 c 仍能完成过半写
     */
    @TestCase
    public func asymmetricPartitionFailsHonestly(): Unit {
        let (a, ta) = bhNode("da5-q2-a", NO_ANTI_ENTROPY)
        let (b, tb) = bhNode("da5-q2-b", NO_ANTI_ENTROPY)
        let (c, tc) = bhNode("da5-q2-c", NO_ANTI_ENTROPY)
        connect([a, b, c])
        tb.blackhole(a.addr())
        tc.blackhole(a.addr())
        let ex = consistencyFailure({ =>
            a.rep.addToSetWith("s", "w1", WriteConsistency.WriteMajority(DA5_SHORT)).get(CONVERGE)
        })
        @Expect(ex.isSome(), true)
        if (let Some(e) <- ex) {
            @Expect(e.replied, 1)
            @Expect(e.needed, 2)
        }
        for (n in [b, c]) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.setElements("s").contains("w1") }), true)
        }
        tb.healAll()
        tc.healAll()
        ta.blackhole(b.addr())
        ta.blackhole(c.addr())
        @Expect(consistencyFailure({ =>
            a.rep.addToSetWith("s", "w2", WriteConsistency.WriteMajority(DA5_SHORT)).get(CONVERGE)
        }).isSome(), true)
        @Expect(b.rep.setElements("s").contains("w2"), false)
        // b → a 通、a → b 断：a 的确认回不来，但 c 的确认足够
        b.rep.addToSetWith("s", "w3", WriteConsistency.WriteMajority(CONVERGE)).get(CONVERGE)
        ta.healAll()
        let all = setOf(c.rep.getWith("s", ReadConsistency.ReadAll(CONVERGE)).get(CONVERGE))
        @Expect(all, ["w1", "w2", "w3"])
        for (n in [a, b, c]) {
            n.shutdown()
        }
    }

    /**
     * Q3：过半写进行中视图发生变化（c 离开、d 加入）。副本数在开始时冻结（3 个、需要 2），
     * W 只发给开始时的对等节点：d 没收到、也不会被算作确认 → 超时失败；之后的新操作按新视图 {a,b,d} 判定
     */
    @TestCase
    public func inFlightOpKeepsItsQuorumAcrossViewChange(): Unit {
        let (a, ta) = bhNode("da5-q3-a", NO_ANTI_ENTROPY)
        let b = DNode("da5-q3-b", NO_ANTI_ENTROPY)
        let c = DNode("da5-q3-c", NO_ANTI_ENTROPY)
        let d = DNode("da5-q3-d", NO_ANTI_ENTROPY)
        connect([a, b, c])
        d.rep.join(a.addr())
        ta.blackhole(b.addr())
        ta.blackhole(c.addr())
        let base = ta.droppedCount()
        let p = a.rep.addToSetWith("s", "inflight", WriteConsistency.WriteMajority(Duration.millisecond * 1500))
        // 增量推送 + W 各发给 b、c 一次之后才改视图（操作确已开始）
        @Expect(awaitCondition(CONVERGE, { => ta.droppedCount() - base >= 4 }), true)
        a.rep.leave(c.addr())
        a.rep.join(d.addr())
        let ex = consistencyFailure({ => p.get(CONVERGE) })
        @Expect(ex.isSome(), true)
        if (let Some(e) <- ex) {
            @Expect(e.total, 3)
            @Expect(e.needed, 2)
            @Expect(e.replied, 1)
        }
        @Expect(d.rep.setElements("s").contains("inflight"), false)
        // 新操作用新视图：b 仍不可达，d 的确认足够
        a.rep.addToSetWith("s", "after", WriteConsistency.WriteMajority(CONVERGE)).get(CONVERGE)
        @Expect(d.rep.setElements("s").contains("after"), true)
        for (n in [a, b, c, d]) {
            n.shutdown()
        }
    }

    /**
     * Q4（DA5 发现的缺陷）：同一地址上的 Replicator 重建后，操作序号从头开始；旧实例的过半写确认在网络里滞留，
     * 到达新实例时恰好命中新实例同序号的进行中操作。此前它被算作确认，新实例的过半写在 W 一个都没送达时就"成功"了。
     * 修复后操作序号起点随机（每个实例不同），迟到确认对不上任何操作
     */
    @TestCase
    public func staleAckFromPreviousReplicatorInstanceIsNotCounted(): Unit {
        let hb = HoldTransport()
        let (x, tx) = bhNode("da5-q4-x", NO_ANTI_ENTROPY)
        let b = DNode("da5-q4-b", NO_ANTI_ENTROPY, Some<NetworkTransport>(hb), None<String>)
        let c = DNode("da5-q4-c", NO_ANTI_ENTROPY)
        connect([x, b, c])
        hb.hold(x.addr().toString())
        // 旧实例的过半写：c 的确认足够；b 的确认滞留在网络里
        x.rep.addToSetWith("k", "old", WriteConsistency.WriteMajority(CONVERGE)).get(CONVERGE)
        @Expect(awaitCondition(CONVERGE, { => hb.heldContaining("2:WA") >= 1 }), true)
        x.rep.stop()
        let kit2 = ActorTestKit("da5-q4-x2")
        let rep2 = Replicator(kit2.system, x.remote, NO_ANTI_ENTROPY)
        rep2.join(b.addr())
        rep2.join(c.addr())
        tx.blackhole(b.addr())
        tx.blackhole(c.addr())
        let base = tx.droppedCount()
        let p = rep2.addToSetWith("k", "new", WriteConsistency.WriteMajority(Duration.millisecond * 1500))
        @Expect(awaitCondition(CONVERGE, { => tx.droppedCount() - base >= 4 }), true)
        hb.release()
        let ex = consistencyFailure({ => p.get(CONVERGE) })
        @Expect(ex.isSome(), true)
        // 确实没有任何其他副本拿到这次写：成功即为虚假确认
        @Expect(b.rep.setElements("k").contains("new"), false)
        @Expect(c.rep.setElements("k").contains("new"), false)
        rep2.stop()
        kit2.shutdown()
        b.shutdown()
        c.shutdown()
        x.remote.stop()
        x.kit.shutdown()
    }

    /**
     * Q5（边界，不是缺陷）：法定人数按各节点自己的视图计算。a 的视图仍是 {a,b,c}（2 个即过半），
     * e 的视图已是 5 个节点（3 个即过半）：a 经 b 确认的过半写，e 从 {c,d,e} 过半读可以读不到。
     * 与 Akka 相同；成员视图收敛后（或用 ReadAll / WriteAll）才恢复相交
     */
    @TestCase
    public func divergentViewsCanBreakQuorumIntersection(): Unit {
        let pairs = Array<(DNode, BlackholeTransport)>(5, { i => bhNode("da5-q5-${i}", NO_ANTI_ENTROPY) })
        let ns = Array<DNode>(5, { i => pairs[i][0] })
        for (i in 1..5) {
            for (j in 0..5) {
                if (i != j) {
                    ns[i].rep.join(ns[j].addr())
                }
            }
        }
        ns[0].rep.join(ns[1].addr())
        ns[0].rep.join(ns[2].addr())
        pairs[0][1].blackhole(ns[2].addr())
        ns[0].rep.addToSetWith("s", "v", WriteConsistency.WriteMajority(CONVERGE)).get(CONVERGE)
        pairs[4][1].blackhole(ns[0].addr())
        pairs[4][1].blackhole(ns[1].addr())
        let got = setOf(ns[4].rep.getWith("s", ReadConsistency.ReadMajority(CONVERGE)).get(CONVERGE))
        @Expect(got.contains("v"), false)
        // 全部连通后 ReadAll 必然读到
        for (p in pairs) {
            p[1].healAll()
        }
        let all = setOf(ns[4].rep.getWith("s", ReadConsistency.ReadAll(CONVERGE)).get(CONVERGE))
        @Expect(all.contains("v"), true)
        for (n in ns) {
            n.shutdown()
        }
    }
}

@Test
public class DDataMembershipMatrixTest {
    /**
     * Q6：集群从 3 个节点扩到 5 个的过程中 a 持续过半写计数器（ClusterReplicatorBinding 跟随成员变化）。
     * 保证：每次过半写都在时限内确认（视图变化不让操作失败）；c 上并发的过半读结果单调不减（读写回本地）；
     * 扩容收敛后每个节点读到的值都等于已确认次数；过半分母随视图变为 5（a 隔离时报告 1/5、需要 3）
     */
    @TestCase
    public func majorityWritesStayAcknowledgedWhileClusterGrows(): Unit {
        let ts = Array<BlackholeTransport>(5, { _ => BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", 0))) })
        let nodes = Array<CNode>(5, { i => CNode("da5-grow-${i}", ts[i]) })
        let reps = Array<Replicator>(5, { i => Replicator(nodes[i].kit.system, nodes[i].remote, GOSSIP) })
        let first = [nodes[0], nodes[1], nodes[2]]
        for (i in 0..3) {
            nodes[i].join([nodes[0].addr()], FAST)
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(first, 3) }), true)
        for (i in 0..3) {
            ClusterReplicatorBinding.bind(nodes[i].cluster, reps[i])
        }
        let grown = AtomicBool(false)
        let acked = AtomicInt64(0)
        let failed = AtomicInt64(0)
        let regressions = AtomicInt64(0)
        let writer = spawn {
            var k = 0
            while ((!grown.load() || k < 50) && k < 3000) {
                try {
                    reps[0].incrementCounterWith("g", 1, WriteConsistency.WriteMajority(CLUSTER_WAIT)).get(CLUSTER_WAIT)
                    acked.fetchAdd(1)
                } catch (_: ReplicatorConsistencyException) {
                    failed.fetchAdd(1)
                }
                k++
            }
        }
        let reader = spawn {
            var last: Int64 = 0
            while (!grown.load()) {
                try {
                    let v = counterOf(reps[2].getWith("g", ReadConsistency.ReadMajority(CLUSTER_WAIT)).get(CLUSTER_WAIT))
                    if (v < last) {
                        regressions.fetchAdd(1)
                    }
                    last = max(last, v)
                } catch (_: ReplicatorConsistencyException) {}
            }
        }
        for (i in 3..5) {
            nodes[i].join([nodes[0].addr()], FAST)
            ClusterReplicatorBinding.bind(nodes[i].cluster, reps[i])
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 5) }), true)
        grown.store(true)
        writer.get()
        reader.get()
        @Expect(failed.load(), 0)
        @Expect(regressions.load(), 0)
        let total = acked.load()
        @Expect(total >= 50, true)
        for (r in reps) {
            @Expect(awaitCondition(CONVERGE, { => r.counterValue("g") == total }), true)
            @Expect(counterOf(r.getWith("g", ReadConsistency.ReadMajority(CONVERGE)).get(CONVERGE)), total)
        }
        for (i in 1..5) {
            ts[0].blackhole(nodes[i].addr())
        }
        let ex = consistencyFailure({ =>
            reps[0].incrementCounterWith("g", 1, WriteConsistency.WriteMajority(DA5_SHORT)).get(CONVERGE)
        })
        @Expect(ex.isSome(), true)
        if (let Some(e) <- ex) {
            @Expect(e.total, 5)
            @Expect(e.needed, 3)
            @Expect(e.replied, 1)
        }
        for (r in reps) {
            r.stop()
        }
        closeAll(nodes)
    }

    /**
     * Q7：c 在同一地址重启（新 uid）。旧实例加过 x、y 并计数 5，a 已删除 x。
     * 保证：新实例的写不被旧实例的分量吞掉（计数 5+2+3=10），x 不被复活；旧实例被移除并 pruning 后值不变
     */
    @TestCase
    public func restartWithNewUidNeitherResurrectsNorDoubleCounts(): Unit {
        let nodes = [CNode("da5-rs-0"), CNode("da5-rs-1"), CNode("da5-rs-2", tcpAt(restartablePort()))]
        for (n in nodes) {
            n.join([nodes[0].addr()], FAST)
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let reps = ArrayList<Replicator>()
        for (n in nodes) {
            let r = Replicator(n.kit.system, n.remote, GOSSIP)
            r.configurePruning(interval: Duration.millisecond * 50, maxDissemination: Duration.millisecond * 300)
            ClusterReplicatorBinding.bind(n.cluster, r)
            reps.add(r)
        }
        reps[2].incrementCounter("hits", 5)
        reps[2].addToSet("s", "x")
        reps[2].addToSet("s", "y")
        reps[0].incrementCounter("hits", 2)
        for (r in reps) {
            @Expect(awaitCondition(CONVERGE, { => r.counterValue("hits") == 7 && r.setElements("s") == ["x", "y"] }), true)
        }
        reps[0].removeFromSet("s", "x")
        for (r in reps) {
            @Expect(awaitCondition(CONVERGE, { => r.setElements("s") == ["y"] }), true)
        }
        let old = reps[2].selfNode
        let port = nodes[2].addr().port
        reps[2].stop()
        nodes[2].crash()
        let reborn = CNode("da5-rs-2b", tcpAt(port)).join([nodes[0].addr()], FAST)
        let r2 = Replicator(reborn.kit.system, reborn.remote, GOSSIP)
        r2.configurePruning(interval: Duration.millisecond * 50, maxDissemination: Duration.millisecond * 300)
        ClusterReplicatorBinding.bind(reborn.cluster, r2)
        @Expect(r2.selfNode != old, true)
        // 新实例在拿到任何状态之前就写入
        r2.incrementCounter("hits", 3)
        r2.addToSet("s", "z")
        let all = [nodes[0], nodes[1], reborn]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(all, 3) && allSeeInstance(all, reborn.address(), reborn.cluster.selfUid) }), true)
        let live = [reps[0], reps[1], r2]
        for (r in live) {
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                r.counterValue("hits") == 10 && r.setElements("s") == ["y", "z"] && freeOf(r, "hits", old) && freeOf(r, "s", old)
            }), true)
        }
        // 再绕几轮 gossip：值稳定，没有被旧分量重复计数或复活
        @Expect(awaitGossipRounds([all[0].cluster, all[1].cluster, all[2].cluster], 5, CLUSTER_WAIT), true)
        for (r in live) {
            @Expect(r.counterValue("hits"), 10)
            @Expect(r.setElements("s"), ["y", "z"])
        }
        for (r in live) {
            r.stop()
        }
        closeAll(all)
    }

    /**
     * Q8：c 以过半写确认了 10 个元素与计数 10 后宕机，被自动 down、移除并 pruning（它的点改记到执行者）。
     * 保证：已确认的添加一个不丢、计数不变，且剩余两节点上已不含 c 的数据
     */
    @TestCase
    public func acknowledgedAddsSurviveWriterRemovalAndPruning(): Unit {
        let nodes = startCluster(3, "da5-prune", FAST_AUTODOWN)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let reps = Array<Replicator>(3, { i => Replicator(nodes[i].kit.system, nodes[i].remote, GOSSIP) })
        for (i in 0..3) {
            reps[i].configurePruning(interval: Duration.millisecond * 50, maxDissemination: Duration.millisecond * 300)
            ClusterReplicatorBinding.bind(nodes[i].cluster, reps[i])
        }
        let want = ArrayList<String>()
        for (i in 0..10) {
            reps[2].addToSetWith("w", "c-${i}", WriteConsistency.WriteMajority(CLUSTER_WAIT)).get(CLUSTER_WAIT)
            reps[2].incrementCounterWith("n", 1, WriteConsistency.WriteMajority(CLUSTER_WAIT)).get(CLUSTER_WAIT)
            want.add("c-${i}")
        }
        let expectSet = sorted(want.toArray())
        let gone = reps[2].selfNode
        reps[2].stop()
        nodes[2].crash()
        let rest = [nodes[0], nodes[1]]
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            rest[0].cluster.state().member(nodes[2].address()).isNone() &&
                rest[1].cluster.state().member(nodes[2].address()).isNone()
        }), true)
        for (i in 0..2) {
            @Expect(awaitCondition(CLUSTER_WAIT, { => freeOf(reps[i], "w", gone) && freeOf(reps[i], "n", gone) }), true)
            @Expect(reps[i].setElements("w"), expectSet)
            @Expect(reps[i].counterValue("n"), 10)
            @Expect(setOf(reps[i].getWith("w", ReadConsistency.ReadMajority(CONVERGE)).get(CONVERGE)), expectSet)
        }
        @Expect(reps[0].pruningStats()[1] + reps[1].pruningStats()[1] >= 2, true)
        reps[0].stop()
        reps[1].stop()
        closeAll(nodes)
    }

    /**
     * Q9：d 被分区隔离（不可达但仍是成员）期间，多数派以过半写删除了 c 添加的 x，随后 c 被移除。
     * 保证：c 的 pruning 只发起、不执行——d 还没登记见过标记（否则 d 手里 c 的数据会在执行后被当成陈旧数据丢弃）；
     * d 带着陈旧的 x（c 的点）回来后：x 不复活、w 不丢、d 分区期间的并发添加 d1 不丢、计数不重复，之后 pruning 完成。
     * （开发本用例时曾把 d 漏出成员视图：Leader 从数据里把 d 当成已移除节点一并 pruning，d1 随之丢失——
     *   这是"不在成员视图里的节点的数据会被清理"的预期行为，集群绑定只把 Down / Removed 的成员排除在视图外）
     */
    @TestCase
    public func staleReplicaDoesNotResurrectRemovedElement(): Unit {
        let partition = Partition()
        let ns = Array<DNode>(4, { i =>
            DNode("da5-tomb-${i}", GOSSIP, Some<NetworkTransport>(PartitionTransport(partition)), None<String>)
        })
        let (a, b, c, d) = (ns[0], ns[1], ns[2], ns[3])
        connect(ns)
        c.rep.addToSet("s", "x")
        c.rep.addToSet("s", "w")
        d.rep.addToSet("s", "d0")
        c.rep.incrementCounter("n", 4)
        d.rep.incrementCounter("n", 1)
        for (n in ns) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.setElements("s") == ["d0", "w", "x"] && n.rep.counterValue("n") == 5 }), true)
        }
        let ad = Array<String>(4, { i => ns[i].addr().toString() })
        partition.split([[ad[0], ad[1], ad[2]], [ad[3]]])
        a.rep.updateWith("s", ReplicatedValue.ORSetValue(ORSet()), orRemove("x"), WriteConsistency.WriteMajority(CONVERGE))
            .get(CONVERGE)
        d.rep.addToSet("s", "d1")
        let removed = c.id()
        c.shutdown()
        for (n in [a, b, d]) {
            n.rep.leave(c.addr())
        }
        enablePruning(a, [b, d], { => [a.id(), b.id(), d.id()] })
        a.rep.nodeRemoved(removed)
        b.rep.nodeRemoved(removed)
        @Expect(awaitCondition(PRUNE_WAIT, { => a.rep.pruningStats()[0] >= 1 }), true)
        // d 不可达、没登记：不得执行（约 20 个检查周期）
        @Expect(awaitCondition(Duration.second, { => a.rep.pruningStats()[1] > 0 }), false)
        @Expect(a.rep.setElements("s"), ["d0", "w"])
        // d 仍持有被移除节点的陈旧点（含已删除的 x）
        @Expect(d.rep.setElements("s"), ["d0", "d1", "w", "x"])
        partition.heal()
        let want = ["d0", "d1", "w"]
        for (n in [a, b, d]) {
            @Expect(awaitCondition(PRUNE_WAIT, { =>
                n.rep.setElements("s") == want && n.rep.counterValue("n") == 5 && freeOf(n.rep, "s", removed) &&
                    freeOf(n.rep, "n", removed)
            }), true)
        }
        @Expect(a.rep.pruningStats()[1] >= 2, true)
        // 反熵继续运行（gossip 间隔 50 ms，约十余轮）：x 不会被任何一方带回，计数不重复
        @Expect(awaitCondition(Duration.millisecond * 600, { =>
            a.rep.setElements("s") != want || b.rep.setElements("s") != want || d.rep.setElements("s") != want ||
                a.rep.counterValue("n") != 5 || d.rep.counterValue("n") != 5
        }), false)
        @Expect(setOf(d.rep.getWith("s", ReadConsistency.ReadMajority(CONVERGE)).get(CONVERGE)), want)
        for (n in [a, b, d]) {
            n.shutdown()
        }
    }
}

@Test
public class DDataPruningBoundaryTest {
    /**
     * Q10（边界）：pruning 把被移除节点的点改记为执行者的新点，相当于执行者重新添加了这些元素。
     * 另一个副本在没见过新点时删除该元素，两者是并发的 add 与 remove，ORSet 取 add-wins：元素保留。
     * 与 Akka ORSet.prune 一致；需要"删除一定生效"的业务应在 pruning 窗口外删除或改用自己的墓碑
     */
    @TestCase
    public func removeConcurrentWithPruningLosesToCollapsedAdd(): Unit {
        let s0 = ORSet().add("10.0.0.3:1#7", "x")
        let pruned = s0.prune("10.0.0.3:1#7", "10.0.0.1:1#1")
        let removedConcurrently = s0.remove("x")
        // 两个方向合并结果相同（交换律），都保留 x
        @Expect(pruned.cleanup("10.0.0.3:1#7").merge(removedConcurrently.cleanup("10.0.0.3:1#7")).contains("x"), true)
        @Expect(removedConcurrently.cleanup("10.0.0.3:1#7").merge(pruned.cleanup("10.0.0.3:1#7")).contains("x"), true)
        // 见过 pruning 结果之后的删除照常生效
        let removedAfter = pruned.remove("x")
        @Expect(removedAfter.merge(pruned).contains("x"), false)
    }
}
