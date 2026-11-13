/*
 * CActor - Cluster Sharding 按分片租约 + fencing（DA4，对标 Akka sharding use-lease）
 *
 * 核心场景与单例租约用例相同：两节点双向黑洞，B 单方面 down A（错误 downing / 非对称分区下只有一侧做了决定）。
 * A 看不到这个决定，继续托管自己的分片；B 移除 A 后按 rendezvous 认为自己拥有全部分片，且 askSet 里已没有 A，
 * 许可式移交立即"完成"——这正是许可式移交保护不到的地方。
 *   - 不配租约（对照）：B 收到发往 A 实体的消息就创建第二个实例，两个实例同时运行。
 *   - 配租约：A 持续续约，B 的分片停在"只差租约"（leaseStatus.waiting / acquisitionHealth.awaitingLease 可见），
 *     消息缓冲、不创建实体；A 与租约存储断连后本地有效期先到、A 立即停止实体，存储判过期后 B 以更大的 token
 *     托管并投递缓冲的消息；全程最多一个实例。
 *   - fencing：实体用 FencedDurableStateStore(inner, fence, 分片租约) 写入；B 写入之后，A 侧同一个包装存储的写入
 *     （租约已失）与携带旧 token 的迟到写入都被拒绝，数据保持 B 写的值。
 *   - 正常离开：分片移交时 A 的实体终止后先交还租约再回 STOPPED，ttl 设成 60 s，B 仍能在等待上限内托管。
 *   - 管理端点：/cluster/shards 的 lease 字段与 /cluster/singletons 反映真实租约状态。
 * 等待全部用 awaitCondition / awaitGossipRounds；"B 不创建实体"以"B 的获取已被拒绝若干次"为前提断言，不睡固定时长。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.coordination.*
import cactor_persistence.{DurableStateStore, FencedDurableStateStore, MemoryDurableStateStore,
    MemoryFencingTokenStore, StaleFencingTokenException, LeaseNotHeldException}
import cactor.api.{ManagementServer}
import cactor_cluster.membership.*
import cactor_cluster.ops.*
import cactor_testkit.*

let SL_TYPE = "sl-acct"
// 16 个分片：两个成员时某一方一个分片都分不到的概率是 2^-15，用例里按归属挑实体才稳定（4 个分片时是 1/8）
let SL_SHARDS: Int64 = 16

/**
 * 分片实体：preStart / postStop 维护全局实例计数（同一进程里两个"节点"共享，用来断言峰值）；
 * "w:<值>" 经持久化存储写入并记录结果，其他消息记录 "<内容>@<节点>"
 */
class SlEntity <: Actor {
    let id: String
    let node: String
    let rec: EventRecorder
    let instances: AtomicInt64
    let store: Option<DurableStateStore>

    init(id: String, node: String, rec: EventRecorder, instances: AtomicInt64, store: Option<DurableStateStore>) {
        this.id = id
        this.node = node
        this.rec = rec
        this.instances = instances
        this.store = store
    }

    public prop name: String { get() { "sl-entity" } }
    public prop description: String { get() { "sl-entity" } }

    public func preStart(): Unit {
        instances.fetchAdd(1)
    }

    public func postStop(): Unit {
        instances.fetchSub(1)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let text = s.getContent()
            match (store) {
                case Some(st) where text.startsWith("w:") =>
                    let value = text[2..]
                    try {
                        st.upsert(id, st.revisionOf(id) + 1, value)
                        rec.record("ok:${value}@${node}")
                    } catch (e: StaleFencingTokenException) {
                        rec.record("stale:${value}@${node}")
                    } catch (e: LeaseNotHeldException) {
                        rec.record("nolease:${value}@${node}")
                    }
                case _ => rec.record("${text}@${node}")
            }
        }
        MessageResult.Handled
    }
}

func slLeaseSettings(name: String, owner: String, ttl: Duration, margin: Duration): LeaseSettings {
    LeaseSettings(name, owner, ttl, Duration.millisecond * 100, margin)
}

func slLeases(store: InMemoryLeaseStore, owner: String, ttl: Duration, margin: Duration): ShardLeases {
    ShardLeases({ name => InMemoryLease(store, slLeaseSettings(name, owner, ttl, margin)) })
}

/**
 * 按两个成员的 rendezvous 归属找一个初始落在 owner 上的实体 ID。
 * 搜索有上限：owner 一个分片都没分到时不能死循环（那样用例会卡到超时而不是给出失败）
 */
func entityOwnedBy(owner: String, members: Array<String>): String {
    for (i in 0..10000) {
        let id = "acct-${i}"
        if (shardOwner(shardOf(id, SL_SHARDS), members) == Some(owner)) {
            return id
        }
    }
    throw IllegalStateException("${owner} 没有分到任何分片")
}

func slRegion(n: CNode, tag: String, rec: EventRecorder, instances: AtomicInt64, settings: ShardingSettings,
    store: (String) -> Option<DurableStateStore>): ClusterSharding {
    ClusterSharding(n.cluster, n.kit.system, n.remote, SL_TYPE, SL_SHARDS,
        { id => SlEntity(id, tag, rec, instances, store(id)) }, settings)
}

func noStore(_: String): Option<DurableStateStore> {
    None<DurableStateStore>
}

@Test
public class ShardingLeaseTest {
    /** 对照（不配租约）：分区 + 单侧 down 后同一实体在两个节点上同时运行。配租约的用例据此可证伪 */
    @TestCase
    public func withoutLeaseOneSidedDownRunsEntityTwice(): Unit {
        let (a, b, ta, tb) = partitionedPair("shl-none")
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)
        let rec = EventRecorder()
        let w = InstanceWatcher().start()
        let ra = slRegion(a, "A", rec, w.instances, ShardingSettings.defaults(), noStore)
        let rb = slRegion(b, "B", rec, w.instances, ShardingSettings.defaults(), noStore)
        @Expect(awaitAllAcquired([ra, rb], SL_SHARDS, CLUSTER_WAIT), true)
        let id = entityOwnedBy(a.address(), [a.address(), b.address()])
        ra.tell(id, StringMessage("x"))
        @Expect(rec.await("x@A", CLUSTER_WAIT), true)

        @Expect(partitionAndDownFromB(a, b, ta, tb), true)
        rb.tell(id, StringMessage("y"))
        @Expect(rec.await("y@B", CLUSTER_WAIT), true)
        @Expect(ra.localEntityIds().contains(id), true)
        @Expect(w.instances.load(), 2)
        @Expect(w.stop(), 2)
        ra.stop()
        rb.stop()
        a.close()
        b.close()
    }

    /**
     * 红绿关键用例：旧宿主分区但仍持有分片租约 → 新宿主停在"只差租约"、不创建实体；
     * 旧宿主失去租约后实体立即停止，新宿主以更大的 token 托管；全程峰值 1 个实例。
     */
    @TestCase
    public func shardLeaseBlocksNewHostUntilOldHostLosesLease(): Unit {
        let (a, b, ta, tb) = partitionedPair("shl-lease")
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)
        let store = InMemoryLeaseStore()
        // ttl 2 s、余量 600 ms：A 本地失效后存储还要再过 600 ms 才判过期，足够 A 的区域停掉实体
        let ttl = Duration.second * 2
        let margin = Duration.millisecond * 600
        let base = ShardingSettings.defaults().withLeaseRetry(Duration.millisecond * 50, Duration.millisecond * 200)
        let rec = EventRecorder()
        let w = InstanceWatcher().start()
        let ra = slRegion(a, "A", rec, w.instances, base.withLease(slLeases(store, "A", ttl, margin)), noStore)
        let rb = slRegion(b, "B", rec, w.instances, base.withLease(slLeases(store, "B", ttl, margin)), noStore)
        @Expect(awaitAllAcquired([ra, rb], SL_SHARDS, CLUSTER_WAIT), true)
        let id = entityOwnedBy(a.address(), [a.address(), b.address()])
        let sh = shardOf(id, SL_SHARDS)
        let leaseName = shardLeaseName(SL_TYPE, sh)
        ra.tell(id, StringMessage("x"))
        @Expect(rec.await("x@A", CLUSTER_WAIT), true)
        @Expect(store.owner(leaseName), Some("A"))
        @Expect(ra.leaseStatus().holds(sh), true)
        @Expect(ra.fencingToken(id), Some(1))
        // 托管的每个分片都持有租约：托管数 == 持有数
        @Expect(ra.leaseStatus().held.size + rb.leaseStatus().held.size, SL_SHARDS)

        @Expect(partitionAndDownFromB(a, b, ta, tb), true)
        rb.tell(id, StringMessage("y"))
        // B 的移交确认已"完成"（A 不在 askSet 里），但租约被 A 占着：停在等租约，至少被拒 3 次
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            rb.leaseStatus().isWaiting(sh) && store.deniedCount(leaseName) >= 3 && rb.leaseStatus().failures >= 3
        }), true)
        @Expect(rb.acquisitionHealth().awaitingLease >= 1, true)
        @Expect(rb.localEntityIds().contains(id), false)
        @Expect(rec.contains("y@B"), false)
        @Expect(w.instances.load(), 1)
        // 管理端点能看到"在等租约"
        let waitingJson = shardingStatusJson(rb)
        @Expect(waitingJson.contains("\"lease\":{\"enabled\":true,\"owner\":\"B\""), true)
        @Expect(waitingJson.contains("\"waiting\":[") && waitingJson.contains("{\"shard\":${sh},\"waitingMs\":"), true)

        // A 与租约存储断连：续约失败 → 本地有效期到 → lost → 立即停止实体；存储判过期 → B 获取（token 2）并投递缓冲的 y
        store.setOwnerReachable("A", false)
        @Expect(rec.await("y@B", CLUSTER_WAIT), true)
        @Expect(ra.localEntityIds().contains(id), false)
        @Expect(ra.leaseStatus().lost >= 1, true)
        @Expect(ra.leaseStatus().holds(sh), false)
        @Expect(rb.fencingToken(id), Some(2))
        @Expect(store.owner(leaseName), Some("B"))
        @Expect(rb.leaseStatus().isWaiting(sh), false)
        let heldJson = shardingStatusJson(rb)
        @Expect(heldJson.contains("{\"shard\":${sh},\"token\":2}"), true)
        // A 仍认为自己是拥有者，但拿不到租约（也等不到 B 的确认）：再跑若干轮 gossip 后仍不会重建实体
        @Expect(awaitGossipRounds([a.cluster], 10, CLUSTER_WAIT), true)
        @Expect(ra.localEntityIds().contains(id), false)
        @Expect(w.instances.load(), 1)
        @Expect(w.stop(), 1)
        ra.stop()
        rb.stop()
        a.close()
        b.close()
    }

    /** fencing：B 托管并写入之后，A 侧（租约已失的包装存储、携带旧 token 的迟到写入）都写不进去 */
    @TestCase
    public func oldHostCannotWriteAfterNewHostAcquired(): Unit {
        let (a, b, ta, tb) = partitionedPair("shl-fence")
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)
        let store = InMemoryLeaseStore()
        let ttl = Duration.second * 2
        let margin = Duration.millisecond * 600
        // 两个节点共用的"外部"存储与 fencing 记录（例如同一个数据库）
        let data = MemoryDurableStateStore()
        let fence = MemoryFencingTokenStore()
        let leasesA = slLeases(store, "A", ttl, margin)
        let leasesB = slLeases(store, "B", ttl, margin)
        let base = ShardingSettings.defaults().withLeaseRetry(Duration.millisecond * 50, Duration.millisecond * 200)
        let rec = EventRecorder()
        let w = InstanceWatcher().start()
        // 实体工厂里经 ShardLeases 取到守护本实体的同一个租约对象：写入自动携带该分片的 fencing token
        let ra = slRegion(a, "A", rec, w.instances, base.withLease(leasesA), { id =>
            Some<DurableStateStore>(FencedDurableStateStore(data, fence, leasesA.leaseForEntity(SL_TYPE, SL_SHARDS, id)))
        })
        let rb = slRegion(b, "B", rec, w.instances, base.withLease(leasesB), { id =>
            Some<DurableStateStore>(FencedDurableStateStore(data, fence, leasesB.leaseForEntity(SL_TYPE, SL_SHARDS, id)))
        })
        @Expect(awaitAllAcquired([ra, rb], SL_SHARDS, CLUSTER_WAIT), true)
        let id = entityOwnedBy(a.address(), [a.address(), b.address()])
        let leaseName = shardLeaseName(SL_TYPE, shardOf(id, SL_SHARDS))
        ra.tell(id, StringMessage("w:a1"))
        @Expect(rec.await("ok:a1@A", CLUSTER_WAIT), true)
        let oldToken = ra.fencingToken(id).getOrThrow()
        @Expect(fence.highestToken(leaseName), oldToken)
        // A 侧与实体同一个租约对象包装出的存储（模拟 A 上仍在运行、稍后才写的代码路径）
        let oldHostStore = FencedDurableStateStore(data, fence, leasesA.leaseForEntity(SL_TYPE, SL_SHARDS, id))
        // 停顿中的旧写者：写入时带的是停顿前拿到的 token
        let pausedWriter = FencedDurableStateStore(data, fence, leaseName, { => Some(oldToken) })

        @Expect(partitionAndDownFromB(a, b, ta, tb), true)
        store.setOwnerReachable("A", false)
        rb.tell(id, StringMessage("w:b1"))
        @Expect(rec.await("ok:b1@B", CLUSTER_WAIT), true)
        @Expect(rb.fencingToken(id).getOrThrow() > oldToken, true)
        @Expect(fence.highestToken(leaseName), rb.fencingToken(id).getOrThrow())

        var notHeld = false
        try {
            oldHostStore.upsert(id, data.revisionOf(id) + 1, "a2")
        } catch (_: LeaseNotHeldException) {
            notHeld = true
        }
        @Expect(notHeld, true)
        var stale = false
        try {
            pausedWriter.upsert(id, data.revisionOf(id) + 1, "a3")
        } catch (_: StaleFencingTokenException) {
            stale = true
        }
        @Expect(stale, true)
        @Expect(data.get(id).getOrThrow().state, "b1")
        @Expect(w.stop(), 1)
        ra.stop()
        rb.stop()
        a.close()
        b.close()
    }

    /** 正常离开：ttl 60 s 远大于等待上限，B 能托管只可能是 A 在回 STOPPED 前交还了租约 */
    @TestCase
    public func gracefulLeaveReleasesShardLeaseBeforeStopped(): Unit {
        let nodes = startCluster(2, "shl-leave", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let store = InMemoryLeaseStore()
        let ttl = Duration.second * 60
        let margin = Duration.second
        let base = ShardingSettings.defaults().withLeaseRetry(Duration.millisecond * 50, Duration.millisecond * 200)
        let rec = EventRecorder()
        let w = InstanceWatcher().start()
        let a = nodes[0]
        let b = nodes[1]
        let ra = slRegion(a, "A", rec, w.instances, base.withLease(slLeases(store, a.address(), ttl, margin)), noStore)
        let rb = slRegion(b, "B", rec, w.instances, base.withLease(slLeases(store, b.address(), ttl, margin)), noStore)
        @Expect(awaitAllAcquired([ra, rb], SL_SHARDS, CLUSTER_WAIT), true)
        let id = entityOwnedBy(a.address(), [a.address(), b.address()])
        let leaseName = shardLeaseName(SL_TYPE, shardOf(id, SL_SHARDS))
        ra.tell(id, StringMessage("x"))
        @Expect(rec.await("x@A", CLUSTER_WAIT), true)
        @Expect(store.owner(leaseName), Some(a.address()))

        a.cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => rb.shardStats()[0] == SL_SHARDS }), true)
        rb.tell(id, StringMessage("y"))
        @Expect(rec.await("y@B", CLUSTER_WAIT), true)
        @Expect(store.owner(leaseName), Some(b.address()))
        @Expect(store.lastToken(leaseName), 2)
        // 交还而不是丢失：A 一次都没有失去租约
        @Expect(ra.leaseStatus().lost, 0)
        @Expect(w.stop(), 1)
        ra.stop()
        rb.stop()
        closeAll(nodes)
    }

    /** 区域停止（ClusterSharding.stop）交还全部分片租约：新的持有者无需等 ttl */
    @TestCase
    public func regionStopReleasesAllShardLeases(): Unit {
        let nodes = startCluster(1, "shl-stop", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        let store = InMemoryLeaseStore()
        let rec = EventRecorder()
        let instances = AtomicInt64(0)
        let n = nodes[0]
        let ra = slRegion(n, "A", rec, instances,
            ShardingSettings.defaults().withLease(slLeases(store, "A", Duration.second * 60, Duration.second)), noStore)
        @Expect(awaitAllAcquired([ra], SL_SHARDS, CLUSTER_WAIT), true)
        @Expect(ra.leaseStatus().held.size, SL_SHARDS)
        ra.stop()
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            var free = true
            for (sh in 0..SL_SHARDS) {
                if (store.owner(shardLeaseName(SL_TYPE, sh)).isSome()) {
                    free = false
                }
            }
            free
        }), true)
        closeAll(nodes)
    }

    /** 未启用租约：状态报告 enabled = false，管理端点给出 {"enabled":false} */
    @TestCase
    public func leaseDisabledByDefault(): Unit {
        let nodes = startCluster(1, "shl-off", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        let rec = EventRecorder()
        let instances = AtomicInt64(0)
        let ra = slRegion(nodes[0], "A", rec, instances, ShardingSettings.defaults(), noStore)
        @Expect(awaitAllAcquired([ra], SL_SHARDS, CLUSTER_WAIT), true)
        @Expect(ra.leaseStatus().enabled, false)
        @Expect(ra.entityLease("x").isNone(), true)
        @Expect(ra.fencingToken("x"), None<Int64>)
        @Expect(ra.acquisitionHealth().awaitingLease, 0)
        @Expect(shardingStatusJson(ra).contains("\"lease\":{\"enabled\":false}"), true)
        ra.stop()
        closeAll(nodes)
    }

    /** ShardLeases 拒绝返回错误租约名的工厂：多个分片共用一个后端记录会让同一持有者互相顶掉 token */
    @TestCase
    public func shardLeasesRejectsMismatchedLeaseName(): Unit {
        let store = InMemoryLeaseStore()
        let leases = ShardLeases({ _ => InMemoryLease(store, LeaseSettings("same-for-all", "A")) })
        var rejected = false
        try {
            leases.leaseForShard(SL_TYPE, 0)
        } catch (_: IllegalArgumentException) {
            rejected = true
        }
        @Expect(rejected, true)
        let ok = slLeases(store, "A", Duration.second * 10, Duration.second)
        // 按名字记忆：区域与实体拿到的是同一个对象
        let l1 = (ok.leaseForShard(SL_TYPE, 1) as InMemoryLease).getOrThrow()
        let l2 = (ok.leaseForEntity(SL_TYPE, SL_SHARDS, entityInShard(1)) as InMemoryLease).getOrThrow()
        @Expect(refEq(l1, l2), true)
    }

    /** /cluster/singletons：单例的运行位置与租约（持有、token、持有者） */
    @TestCase
    public func singletonEndpointShowsLeaseState(): Unit {
        let nodes = startCluster(1, "shl-sgl", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        let n = nodes[0]
        let store = InMemoryLeaseStore()
        let lease = InMemoryLease(store, LeaseSettings("singleton-sgl", "A", Duration.second * 10,
            Duration.millisecond * 100, Duration.second))
        let rec = EventRecorder()
        let instances = AtomicInt64(0)
        let s = ClusterSingleton(n.cluster, n.kit.system, n.remote, "sgl", { => Ledger(n.address(), rec, instances) },
            ClusterSingletonSettings().withLease(lease))
        let plain = ClusterSingleton(n.cluster, n.kit.system, n.remote, "plain", { => Ledger(n.address(), rec, instances) })
        @Expect(awaitCondition(CLUSTER_WAIT, { => s.isRunningHere() && s.isLeaseHeld() && plain.isRunningHere() }), true)
        @Expect(s.fencingToken(), Some(1))
        @Expect(plain.fencingToken(), None<Int64>)
        let mgmt = ManagementServer(n.kit.system, "127.0.0.1", 0)
        attachClusterRoutes(mgmt, n.cluster, singletons: [s, plain])
        mgmt.start()
        let (st, body) = cmHttp(mgmt.port(), "/cluster/singletons")
        @Expect(st, 200)
        @Expect(body.contains("\"name\":\"sgl\",\"runningHere\":true"), true)
        @Expect(body.contains("\"lease\":{\"enabled\":true,\"name\":\"singleton-sgl\",\"owner\":\"A\",\"held\":true,\"token\":1"), true)
        @Expect(body.contains("\"name\":\"plain\",\"runningHere\":true"), true)
        @Expect(body.contains("\"lease\":{\"enabled\":false}"), true)
        mgmt.stop()
        s.stop()
        plain.stop()
        closeAll(nodes)
    }
}

func entityInShard(sh: Int64): String {
    for (i in 0..100000) {
        let id = "acct-${i}"
        if (shardOf(id, SL_SHARDS) == sh) {
            return id
        }
    }
    throw IllegalStateException("找不到落在分片 ${sh} 的实体")
}
