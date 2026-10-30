/*
 * CActor - R6 测试：脑裂处理（Split Brain Resolver）
 * 真实回环 TCP 节点 + PartitionTransport 注入网络分区（分区两侧之间的帧全部丢弃，TCP 连接本身不断）。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.sort.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{StringMessage}
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TcpTransport}
import cactor_cluster.membership.*
import cactor_testkit.*

/** 分区表：地址 -> 所在分组；不同分组之间不通。未分区时全部互通 */
class Partition {
    private let lock = Mutex()
    private let groups = HashMap<String, Int64>()
    // 单独断开的链路（双向），用于非对称分区：两端都还能和其他节点通信
    private let cutLinks = HashSet<String>()
    let dropped = AtomicInt64(0)

    func cutLink(a: String, b: String): Unit {
        synchronized(lock) {
            cutLinks.add("${a}|${b}")
            cutLinks.add("${b}|${a}")
        }
    }

    func split(sides: Array<Array<String>>): Unit {
        synchronized(lock) {
            groups.clear()
            for (i in 0..sides.size) {
                for (a in sides[i]) {
                    groups[a] = i
                }
            }
        }
    }

    func heal(): Unit {
        synchronized(lock) {
            groups.clear()
            cutLinks.clear()
        }
    }

    func connected(from: String, to: String): Bool {
        synchronized(lock) {
            if (cutLinks.contains("${from}|${to}")) {
                return false
            }
            match ((groups.get(from), groups.get(to))) {
                case (Some(a), Some(b)) => a == b
                case _ => true
            }
        }
    }
}

class PartitionTransport <: NetworkTransport {
    private let inner = TcpTransport(NetworkAddress("127.0.0.1", 0))
    private let partition: Partition

    init(partition: Partition) {
        this.partition = partition
    }

    public func start(): Unit { inner.start() }
    public func stop(): Unit { inner.stop() }
    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit { inner.setDataHandler(handler) }
    public func getLocalAddress(): NetworkAddress { inner.getLocalAddress() }
    public func isActive(): Bool { inner.isActive() }

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        if (!partition.connected(inner.getLocalAddress().toString(), address.toString())) {
            partition.dropped.fetchAdd(1)
            return
        }
        inner.send(address, data)
    }
}

func sbrSettings(strategy: DowningStrategy): ClusterSettings {
    ClusterSettings(Duration.millisecond * 50, Duration.millisecond * 50, 8.0, Duration.millisecond * 300, strategy)
}

let STABLE = Duration.millisecond * 800

func startPartitionable(n: Int64, name: String, settings: ClusterSettings, partition: Partition): Array<CNode> {
    let nodes = Array<CNode>(n, { i => CNode("${name}-${i}", PartitionTransport(partition)) })
    let seed = nodes[0].addr()
    for (node in nodes) {
        node.join([seed], settings)
    }
    nodes
}

func addressesOf(nodes: Array<CNode>): Array<String> {
    Array<String>(nodes.size, { i => nodes[i].address() })
}

/** 本侧每个节点都只看到本侧成员 Up 且已收敛，且没有节点被终止 */
func sideSurvived(side: Array<CNode>): Bool {
    let mine = HashSet<String>(addressesOf(side))
    for (n in side) {
        if (n.cluster.isTerminated()) {
            return false
        }
        let s = n.cluster.state()
        if (!s.converged || s.upCount() != side.size) {
            return false
        }
        for (m in s.members) {
            if (!mine.contains(m.address)) {
                return false
            }
        }
    }
    true
}

func sideDowned(side: Array<CNode>): Bool {
    for (n in side) {
        if (!n.cluster.isTerminated() || !n.cluster.isDownedBySbr()) {
            return false
        }
    }
    true
}

func describe(d: SbrDecision): String {
    match (d) {
        case DownUnreachable(list) =>
            let sorted = list.clone()
            sort(sorted)
            "down:" + String.join(sorted, delimiter: ",")
        case DownSelf => "self"
        case NoAction => "none"
    }
}

func ups(addrs: Array<String>): Array<Member> {
    Array<Member>(addrs.size, { i => Member(addrs[i], MemberStatus.Up, i + 1) })
}

func setOf(xs: Array<String>): HashSet<String> {
    HashSet<String>(xs)
}

@Test
public class SbrDecideTest {
    let km = DowningStrategy.KeepMajority(Duration.second)

    @TestCase
    public func testKeepMajority(): Unit {
        let five = ups(["a", "b", "c", "d", "e"])
        @Expect(describe(sbrDecide(km, five, setOf([]))), "none")
        @Expect(describe(sbrDecide(km, five, setOf(["d", "e"]))), "down:d,e")
        @Expect(describe(sbrDecide(km, five, setOf(["a", "b", "c"]))), "self")
        // 正好一半：包含地址最小成员（a）的一侧存活
        let four = ups(["a", "b", "c", "d"])
        @Expect(describe(sbrDecide(km, four, setOf(["c", "d"]))), "down:c,d")
        @Expect(describe(sbrDecide(km, four, setOf(["a", "b"]))), "self")
        @Expect(describe(sbrDecide(km, four, setOf(["b", "d"]))), "down:b,d")
        @Expect(describe(sbrDecide(km, four, setOf(["a", "c"]))), "self")
    }

    @TestCase
    public func testJoiningMembersDoNotVoteButAreDowned(): Unit {
        let ms: Array<Member> = [Member("a", MemberStatus.Up, 1), Member("b", MemberStatus.Up, 2),
            Member("c", MemberStatus.Joining, 0), Member("d", MemberStatus.Joining, 0), Member("e", MemberStatus.Joining, 0)]
        // 计票成员只有 a、b，都可达：存活，并 down 掉不可达的 Joining 成员
        @Expect(describe(sbrDecide(km, ms, setOf(["c", "d", "e"]))), "down:c,d,e")
        // 从 c 的视角：a、b 都不可达，本侧没有计票成员 -> 不存活
        @Expect(describe(sbrDecide(km, ms, setOf(["a", "b"]))), "self")
    }

    @TestCase
    public func testKeepOldest(): Unit {
        let ko = DowningStrategy.KeepOldest(Duration.second)
        // c 最老（upNumber 1）
        let ms: Array<Member> = [Member("a", MemberStatus.Up, 3), Member("b", MemberStatus.Up, 2),
            Member("c", MemberStatus.Up, 1), Member("d", MemberStatus.Up, 4), Member("e", MemberStatus.Up, 5)]
        // 最老成员单独在一侧也存活，另外四个自行停止
        @Expect(describe(sbrDecide(ko, ms, setOf(["c"]))), "self")
        @Expect(describe(sbrDecide(ko, ms, setOf(["a", "b", "d", "e"]))), "down:a,b,d,e")
    }

    @TestCase
    public func testStaticQuorumAndAutoDown(): Unit {
        let sq = DowningStrategy.StaticQuorum(3, Duration.second)
        let five = ups(["a", "b", "c", "d", "e"])
        @Expect(describe(sbrDecide(sq, five, setOf(["a", "b"]))), "down:a,b")
        @Expect(describe(sbrDecide(sq, five, setOf(["a", "b", "c"]))), "self")
        // 三方分区 2|2|1：没有一侧达到 3，全部停止（安全优先）
        @Expect(describe(sbrDecide(sq, five, setOf(["c", "d", "e"]))), "self")
        // AutoDownAfter 不做存活判定（两侧都会 down 对方，这正是它不安全的原因）
        let ad = DowningStrategy.AutoDownAfter(Duration.second)
        @Expect(describe(sbrDecide(ad, five, setOf(["a", "b", "c"]))), "down:a,b,c")
        @Expect(DowningStrategy.NoDowning.stableAfter().isNone(), true)
        @Expect(ad.stableAfter().isNone(), true)
        @Expect(sq.stableAfter().isSome(), true)
    }
}

func describe4(d: SbrDecision): String {
    describe(d)
}

@Test
public class IndirectConnectionTest {
    @TestCase
    public func testIndirectMembersDoNotVoteAndAreDowned(): Unit {
        let km = DowningStrategy.KeepMajority(Duration.second)
        let five = ups(["a", "b", "c", "d", "e"])
        // c 能连上 a、b，但有其他成员报告 a、b 之间断开：a、b 不计票，剩下 3 个全部可达 -> 存活并 down a、b
        @Expect(describe4(sbrDecide(km, five, setOf([]), setOf(["a", "b"]))), "down:a,b")
        // 不可达与间接连接同时存在：c、d、e 中 e 不可达、a 间接连接，计票成员 b、c、d、e 中可达 3 个 -> 存活
        @Expect(describe4(sbrDecide(km, five, setOf(["e"]), setOf(["a"]))), "down:a,e")
        // 计票成员 b、c、d、e 中 d、e 不可达：正好一半，包含最小地址 b 的一侧存活
        @Expect(describe4(sbrDecide(km, five, setOf(["d", "e"]), setOf(["a"]))), "down:a,d,e")
        // 计票成员中只有 b 可达 -> 不存活
        @Expect(describe4(sbrDecide(km, five, setOf(["c", "d", "e"]), setOf(["a"]))), "self")
    }

    /**
     * 只断开两个成员之间的链路（都不是地址最小的成员）。只看本地视图时，Leader 看不到任何问题，
     * 两个断开的成员永远认为对方不可达，集群一直不能收敛。可达性表随 gossip 传播后，
     * Leader 发现它们是间接连接的成员并把两个都 down，其余 3 个成员重新收敛。
     */
    @TestCase
    public func testCutLinkBetweenTwoMembersIsResolved(): Unit {
        let p = Partition()
        let nodes = startPartitionable(5, "sbr-ind", sbrSettings(DowningStrategy.KeepMajority(STABLE)), p)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 5) }), true)
        let addrs = addressesOf(nodes)
        sort(addrs)
        let byAddr = HashMap<String, CNode>()
        for (n in nodes) {
            byAddr[n.address()] = n
        }
        let x = byAddr[addrs[3]]
        let y = byAddr[addrs[4]]
        p.cutLink(x.address(), y.address())
        let rest = [byAddr[addrs[0]], byAddr[addrs[1]], byAddr[addrs[2]]]
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideSurvived(rest) }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => x.cluster.isTerminated() && y.cluster.isTerminated() }), true)
        closeAll(nodes)
    }

    /** 断开的一端是 Leader（地址最小）：Leader 自己看到另一端不可达，只 down 另一端，其余 4 个成员保留 */
    @TestCase
    public func testCutLinkFromLeaderDownsOnlyTheOtherEnd(): Unit {
        let p = Partition()
        let nodes = startPartitionable(5, "sbr-ind2", sbrSettings(DowningStrategy.KeepMajority(STABLE)), p)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 5) }), true)
        let addrs = addressesOf(nodes)
        sort(addrs)
        let byAddr = HashMap<String, CNode>()
        for (n in nodes) {
            byAddr[n.address()] = n
        }
        let leader = byAddr[addrs[0]]
        let other = byAddr[addrs[4]]
        p.cutLink(leader.address(), other.address())
        let rest = [leader, byAddr[addrs[1]], byAddr[addrs[2]], byAddr[addrs[3]]]
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideSurvived(rest) }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => other.cluster.isTerminated() }), true)
        closeAll(nodes)
    }
}

@Test
public class SplitBrainResolverTest {
    @TestCase
    public func testKeepMajorityThreeVsTwo(): Unit {
        let p = Partition()
        let nodes = startPartitionable(5, "sbr-maj", sbrSettings(DowningStrategy.KeepMajority(STABLE)), p)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 5) }), true)
        let major = [nodes[0], nodes[1], nodes[2]]
        let minor = [nodes[3], nodes[4]]
        p.split([addressesOf(major), addressesOf(minor)])
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideDowned(minor) }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideSurvived(major) }), true)
        for (n in major) {
            @Expect(n.cluster.isDownedBySbr(), false)
            for (m in minor) {
                @Expect(n.events.await("MemberRemoved(${m.address()})", CLUSTER_WAIT), true)
            }
        }
        @Expect(p.dropped.load() > 0, true)
        closeAll(nodes)
    }

    @TestCase
    public func testKeepMajorityTieKeepsLowestAddressSide(): Unit {
        let p = Partition()
        let nodes = startPartitionable(4, "sbr-tie", sbrSettings(DowningStrategy.KeepMajority(STABLE)), p)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 4) }), true)
        // 按地址排序后把最小地址与另一个节点分到一侧，其余两个到另一侧
        let addrs = addressesOf(nodes)
        sort(addrs)
        let byAddr = HashMap<String, CNode>()
        for (n in nodes) {
            byAddr[n.address()] = n
        }
        let low = [byAddr[addrs[0]], byAddr[addrs[3]]]
        let high = [byAddr[addrs[1]], byAddr[addrs[2]]]
        p.split([addressesOf(low), addressesOf(high)])
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideDowned(high) }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideSurvived(low) }), true)
        closeAll(nodes)
    }

    @TestCase
    public func testKeepOldestSurvivesAlone(): Unit {
        let p = Partition()
        let nodes = startPartitionable(3, "sbr-old", sbrSettings(DowningStrategy.KeepOldest(STABLE)), p)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        // 种子节点最先 Up，是最老的成员
        @Expect(nodes[0].cluster.oldest().getOrThrow().address, nodes[0].address())
        let oldSide = [nodes[0]]
        let others = [nodes[1], nodes[2]]
        p.split([addressesOf(oldSide), addressesOf(others)])
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideDowned(others) }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideSurvived(oldSide) }), true)
        closeAll(nodes)
    }

    @TestCase
    public func testSingletonNeverRunsTwiceDuringPartition(): Unit {
        let p = Partition()
        let nodes = startPartitionable(5, "sbr-cs", sbrSettings(DowningStrategy.KeepMajority(STABLE)), p)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 5) }), true)
        let rec = EventRecorder()
        let instances = AtomicInt64(0)
        let maxInstances = AtomicInt64(0)
        let singletons = Array<ClusterSingleton>(5, { i =>
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
                sleep(Duration.microsecond * 200)
            }
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[0].isRunningHere() }), true)
        // 最老节点（单例所在）落在少数派：它必须先停掉实例，多数派再启动新实例
        let minor = [nodes[0], nodes[1]]
        let major = [nodes[2], nodes[3], nodes[4]]
        p.split([addressesOf(minor), addressesOf(major)])
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideDowned(minor) }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideSurvived(major) }), true)
        // 新实例在多数派中最老的成员上（并发加入时 upNumber 顺序不固定，按成员表计算）
        let target = nodes[2].cluster.oldest().getOrThrow().address
        var targetIndex = -1
        for (i in 2..5) {
            if (nodes[i].address() == target) {
                targetIndex = i
            }
        }
        @Expect(targetIndex >= 2, true)
        let targetSingleton = singletons[targetIndex]
        @Expect(awaitCondition(CLUSTER_WAIT, { => targetSingleton.isRunningHere() }), true)
        @Expect(singletons[0].isRunningHere() || singletons[1].isRunningHere(), false)
        rec.clear()
        for (i in 2..5) {
            singletons[i].tell(StringMessage("after-${i}"))
        }
        @Expect(rec.awaitSize(3, CLUSTER_WAIT), true)
        for (e in rec.snapshot()) {
            @Expect(e.endsWith("@${target}"), true)
        }
        watching.store(false)
        watcher.get()
        @Expect(maxInstances.load(), 1)
        @Expect(instances.load(), 1)
        for (s in singletons) {
            s.stop()
        }
        closeAll(nodes)
    }

    /** 对照：AutoDownAfter 在分区时两侧互相 down，各自继续运行（脑裂），这正是需要 SBR 的原因 */
    @TestCase
    public func testAutoDownSplitsBrainAsControl(): Unit {
        let p = Partition()
        let nodes = startPartitionable(4, "sbr-ctl", sbrSettings(DowningStrategy.AutoDownAfter(Duration.millisecond * 300)), p)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 4) }), true)
        let a = [nodes[0], nodes[1]]
        let b = [nodes[2], nodes[3]]
        p.split([addressesOf(a), addressesOf(b)])
        @Expect(awaitCondition(CLUSTER_WAIT, { => sideSurvived(a) && sideSurvived(b) }), true)
        closeAll(nodes)
    }
}

/**
 * 回归：加入应答迟到时，非种子节点不能自己组成集群。
 * 旧实现中，节点在第一个 gossip 周期就把只含自己的成员表“收敛”并把自己推进到 Up（upNumber 1），
 * 高负载下几个节点各自 Up 后再合并，出现重复的 upNumber、Singleton 位置错误、allUp 长时间不满足。
 */
@Test
public class DelayedJoinTest {
    @TestCase
    public func testJoinerWaitsForSeedInsteadOfFormingItsOwnCluster(): Unit {
        let p = Partition()
        let seed = CNode("dj-0", PartitionTransport(p))
        let a = CNode("dj-1", PartitionTransport(p))
        let b = CNode("dj-2", PartitionTransport(p))
        // 种子与两个加入者之间先不通：加入请求和应答都丢失
        p.split([[seed.address()], [a.address(), b.address()]])
        let settings = sbrSettings(DowningStrategy.NoDowning)
        seed.join([seed.addr()], settings)
        a.join([seed.addr()], settings)
        b.join([seed.addr()], settings)
        @Expect(awaitCondition(CLUSTER_WAIT, { => seed.cluster.isUp() }), true)
        // 等待 20 个 gossip 周期：加入者保持 Joining，也不能互相接纳
        sleep(Duration.second)
        @Expect(a.cluster.isUp() || b.cluster.isUp(), false)
        @Expect(a.cluster.state().members.size, 1)
        @Expect(b.cluster.state().members.size, 1)
        p.heal()
        let nodes = [seed, a, b]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let ups = HashSet<Int64>()
        for (m in seed.cluster.state().members) {
            ups.add(m.upNumber)
        }
        @Expect(ups.size, 3)
        @Expect(seed.cluster.oldest().getOrThrow().address, seed.address())
        closeAll(nodes)
    }
}
