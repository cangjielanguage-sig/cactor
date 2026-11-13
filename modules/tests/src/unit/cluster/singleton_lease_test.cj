/*
 * CActor - Cluster Singleton + 租约（DA4，对标 Akka singleton use-lease）
 *
 * 场景：两个节点之间双向黑洞（集群网络分区），B 侧把 A 手动 down（等价于错误的 downing / 非对称分区下
 * 只有一侧做出了决定）。A 看不到这个决定，仍认为自己最老、实例继续运行；B 移除 A 后成为最老成员。
 *   - 不配租约（对照，记录现状）：B 启动第二个实例，两个实例同时运行——成员视图不是外部 fencing。
 *   - 配租约：租约存储与集群网络无关、双方都能访问。A 持续续约，B 的获取一直被拒、不启动实例；
 *     A 与租约存储断连后，本地有效期先到期、实例被停止，存储随后判过期，B 获取并启动；全程最多一个实例。
 *   - 正常离开：旧实例停止后先释放租约再回 HANDOVER_DONE，新宿主不必等 ttl（ttl 设成 60 s，远大于等待上限）。
 * 等待全部用 awaitCondition；负向断言（B 不启动）以"B 已被拒绝若干次"为前提，而不是睡一段固定时间。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{StringMessage}
import cactor_foundation.network.{NetworkAddress, TcpTransport, BlackholeTransport}
import cactor_foundation.coordination.*
import cactor_cluster.membership.*
import cactor_testkit.*

/** 实例数峰值监视：后台线程每毫秒采样一次 */
class InstanceWatcher {
    let instances = AtomicInt64(0)
    let peak = AtomicInt64(0)
    let running = AtomicBool(true)
    var future: Option<Future<Unit>> = None

    init() {}

    func start(): InstanceWatcher {
        let inst = instances
        let pk = peak
        let run = running
        future = Some(spawn {
            while (run.load()) {
                let v = inst.load()
                if (v > pk.load()) {
                    pk.store(v)
                }
                sleep(Duration.millisecond)
            }
        })
        this
    }

    func stop(): Int64 {
        running.store(false)
        if (let Some(f) <- future) {
            f.get()
        }
        // 采样线程退出前可能还没采到最后一次变化：收尾时再取一次当前值
        max(peak.load(), instances.load())
    }
}

func partitionedPair(tag: String): (CNode, CNode, BlackholeTransport, BlackholeTransport) {
    let ta = BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", 0)))
    let tb = BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", 0)))
    let a = CNode("${tag}-a", ta)
    let b = CNode("${tag}-b", tb)
    a.join([a.addr()], FAST)
    b.join([a.addr()], FAST)
    (a, b, ta, tb)
}

/** 双向黑洞，等 B 判 A 不可达后由 B 单方面 down A */
func partitionAndDownFromB(a: CNode, b: CNode, ta: BlackholeTransport, tb: BlackholeTransport): Bool {
    ta.blackhole(b.address())
    tb.blackhole(a.address())
    if (!awaitCondition(Duration.second * 15, { => b.cluster.state().unreachable.contains(a.address()) })) {
        return false
    }
    b.cluster.down(a.address())
    awaitCondition(CLUSTER_WAIT, { =>
        match (b.cluster.oldest()) {
            case Some(m) => m.address == b.address()
            case None => false
        }
    })
}

func singletonLeaseSettings(owner: String, ttl: Duration, margin: Duration): LeaseSettings {
    LeaseSettings("singleton-ledger", owner, ttl, Duration.millisecond * 100, margin)
}

@Test
public class SingletonLeaseTest {
    /** 对照（不配租约）：分区 + 单侧 down 后两个实例同时运行。这是 DA4 的问题本身，配租约的用例据此可证伪 */
    @TestCase
    public func withoutLeaseOneSidedDownRunsTwoInstances(): Unit {
        let (a, b, ta, tb) = partitionedPair("sl-none")
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)
        let rec = EventRecorder()
        let w = InstanceWatcher().start()
        let sa = ClusterSingleton(a.cluster, a.kit.system, a.remote, "ledger", { => Ledger(a.address(), rec, w.instances) })
        let sb = ClusterSingleton(b.cluster, b.kit.system, b.remote, "ledger", { => Ledger(b.address(), rec, w.instances) })
        @Expect(awaitCondition(CLUSTER_WAIT, { => sa.isRunningHere() }), true)
        @Expect(partitionAndDownFromB(a, b, ta, tb), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => sb.isRunningHere() }), true)
        @Expect(sa.isRunningHere(), true)
        @Expect(w.instances.load(), 2)
        @Expect(w.stop(), 2)
        sa.stop()
        sb.stop()
        a.close()
        b.close()
    }

    @TestCase
    public func leaseBlocksSecondInstanceUntilOldOwnerLosesLease(): Unit {
        let (a, b, ta, tb) = partitionedPair("sl-lease")
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)
        let store = InMemoryLeaseStore()
        // ttl 2 s、余量 600 ms：A 本地失效后，存储还要再过 600 ms 才判过期，足够 A 的管理器停掉实例
        let ttl = Duration.second * 2
        let margin = Duration.millisecond * 600
        let leaseA = InMemoryLease(store, singletonLeaseSettings("A", ttl, margin))
        let leaseB = InMemoryLease(store, singletonLeaseSettings("B", ttl, margin))
        let base = ClusterSingletonSettings().withLeaseRetry(Duration.millisecond * 50, Duration.millisecond * 200)
        let rec = EventRecorder()
        let w = InstanceWatcher().start()
        let sa = ClusterSingleton(a.cluster, a.kit.system, a.remote, "ledger",
            { => Ledger(a.address(), rec, w.instances) }, base.withLease(leaseA))
        let sb = ClusterSingleton(b.cluster, b.kit.system, b.remote, "ledger",
            { => Ledger(b.address(), rec, w.instances) }, base.withLease(leaseB))
        @Expect(awaitCondition(CLUSTER_WAIT, { => sa.isRunningHere() && sa.isLeaseHeld() }), true)
        @Expect(store.owner("singleton-ledger"), Some("A"))
        @Expect(leaseA.fencingToken(), Some(1))

        // 分区 + B 单方面 down A：B 成为最老成员，但 A 仍持有并续约，B 的获取被拒、不启动
        @Expect(partitionAndDownFromB(a, b, ta, tb), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            let (_, failures, _) = sb.leaseStats()
            failures >= 3 && store.deniedCount("singleton-ledger") >= 3
        }), true)
        @Expect(sb.isRunningHere(), false)
        @Expect(sa.isRunningHere(), true)
        @Expect(w.instances.load(), 1)

        // A 与租约存储断连（旧宿主被彻底隔离）：A 续约失败 → 本地有效期到 → lost → 实例停止；
        // 存储在 ttl 后判过期 → B 获取（token 2）并启动
        store.setOwnerReachable("A", false)
        @Expect(awaitCondition(CLUSTER_WAIT, { => sb.isRunningHere() }), true)
        @Expect(sa.isRunningHere(), false)
        @Expect(sa.leaseStats()[2], 1)
        @Expect(leaseB.fencingToken(), Some(2))
        @Expect(store.owner("singleton-ledger"), Some("B"))
        // A 仍认为自己最老，但既等不到 B 的 HandOver 确认、也拿不到租约：再跑若干轮 gossip 后仍不会启动
        @Expect(awaitGossipRounds([a.cluster], 10, CLUSTER_WAIT), true)
        @Expect(sa.isRunningHere(), false)
        @Expect(w.instances.load(), 1)
        @Expect(w.stop(), 1)
        sa.stop()
        sb.stop()
        a.close()
        b.close()
    }

    @TestCase
    public func gracefulLeaveReleasesLeaseBeforeHandOverDone(): Unit {
        let nodes = startCluster(2, "sl-leave", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let store = InMemoryLeaseStore()
        // ttl 远大于等待上限：B 能启动只可能是因为 A 主动释放了租约
        let ttl = Duration.second * 60
        let margin = Duration.second
        let rec = EventRecorder()
        let w = InstanceWatcher().start()
        let singletons = Array<ClusterSingleton>(2, { i =>
            let n = nodes[i]
            let addr = n.address()
            let lease = InMemoryLease(store, singletonLeaseSettings(addr, ttl, margin))
            ClusterSingleton(n.cluster, n.kit.system, n.remote, "ledger", { => Ledger(addr, rec, w.instances) },
                ClusterSingletonSettings().withLease(lease).withLeaseRetry(Duration.millisecond * 50,
                    Duration.millisecond * 200))
        })
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[0].isRunningHere() }), true)
        nodes[0].cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[1].isRunningHere() }), true)
        @Expect(singletons[0].isRunningHere(), false)
        @Expect(store.owner("singleton-ledger"), Some(nodes[1].address()))
        @Expect(store.lastToken("singleton-ledger"), 2)
        singletons[1].tell(StringMessage("x"))
        @Expect(rec.await("x@${nodes[1].address()}", CLUSTER_WAIT), true)
        @Expect(w.stop(), 1)
        singletons[0].stop()
        singletons[1].stop()
        closeAll(nodes)
    }
}
