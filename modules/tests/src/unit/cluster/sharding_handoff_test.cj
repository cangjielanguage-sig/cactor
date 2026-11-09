/*
 * CActor - 分片的许可式移交与可用性（E13′）、加入时的配置兼容检查（E42）
 *
 *   - 成员持续变化（加入两个节点、再先后优雅离开两个）期间不停地发消息：任何实体在任何时刻最多一个实例
 *     （全局记录每个实体的 preStart / postStop，重叠 0）。此前新拥有者按新视图直接创建实体，旧拥有者在
 *     下一次刷新视图、再处理完邮箱里的消息后才停止，二者重叠（B4）
 *   - 黑洞一个节点（不 down）：其余节点拥有的分片照常投递，只有拥有者不可达的分片缓冲；恢复后缓冲的消息送达。
 *     此前任一节点不可达，视图不收敛，全部分片的消息都被缓冲（N6）
 *   - 节点离开集群后，经它的区域发送的消息进死信，不再永远滞留在缓冲区（N8）
 *   - 兼容键不一致的节点加入被拒绝（REJECT），永远不会成为成员、不托管任何实体；
 *     不带兼容信息的旧版本 JOIN 同样被拒绝（N9 / E42）
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.random.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_cluster.membership.*
import cactor_cluster.ddata.{FieldWriter, FieldReader}
import cactor_testkit.*

/** 全局记录每个实体当前的存活实例数：preStart 加一、postStop 减一，加一后大于 1 即为重叠（双活） */
class Activations {
    let lock = Mutex()
    let live = HashMap<String, Int64>()
    let hostsEver = HashMap<String, HashSet<String>>()
    var overlaps: Int64 = 0
    var starts: Int64 = 0
    var received: Int64 = 0
    let overlapDetails = ArrayList<String>()

    func started(id: String, node: String): Unit {
        synchronized(lock) {
            let n = (live.get(id) ?? 0) + 1
            live[id] = n
            starts++
            if (n > 1) {
                overlaps++
                if (overlapDetails.size < 10) {
                    overlapDetails.add("${id}@${node}（同时 ${n} 个）")
                }
            }
            match (hostsEver.get(id)) {
                case Some(s) => s.add(node)
                case None =>
                    let s = HashSet<String>()
                    s.add(node)
                    hostsEver[id] = s
            }
        }
    }

    func stopped(id: String): Unit {
        synchronized(lock) {
            live[id] = (live.get(id) ?? 1) - 1
        }
    }

    func onMessage(): Unit {
        synchronized(lock) {
            received++
        }
    }

    func receivedCount(): Int64 {
        synchronized(lock) { received }
    }

    func startCount(): Int64 {
        synchronized(lock) { starts }
    }

    func overlapCount(): Int64 {
        synchronized(lock) { overlaps }
    }

    /** 曾在不止一个节点上运行过的实体数（说明发生过移交） */
    func movedEntities(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for ((_, s) in hostsEver) {
                if (s.size > 1) {
                    n++
                }
            }
            n
        }
    }

    func maxLive(): Int64 {
        synchronized(lock) {
            var m: Int64 = 0
            for ((_, n) in live) {
                m = max(m, n)
            }
            m
        }
    }
}

class TrackedShardEntity <: Actor {
    let id: String
    let node: String
    let acts: Activations

    init(id: String, node: String, acts: Activations) {
        this.id = id
        this.node = node
        this.acts = acts
    }

    public prop name: String { get() { "tracked" } }
    public prop description: String { get() { "tracked" } }

    public func preStart(): Unit {
        acts.started(id, node)
    }

    public func postStop(): Unit {
        acts.stopped(id)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        acts.onMessage()
        MessageResult.Handled
    }
}

/** 记录收到的字符串消息（冒充集群守护 Actor，接收 REJECT） */
class TextSink <: Actor {
    let rec: EventRecorder

    init(rec: EventRecorder) {
        this.rec = rec
    }

    public prop name: String { get() { "sink" } }
    public prop description: String { get() { "sink" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record(s.getContent())
        }
        MessageResult.Handled
    }
}

/** 线程安全的"当前可用区域"列表：发送线程从中随机挑一个 */
class LiveRegions {
    let lock = Mutex()
    let items = ArrayList<ClusterSharding>()

    func add(r: ClusterSharding): Unit {
        synchronized(lock) { items.add(r) }
    }

    func remove(r: ClusterSharding): Unit {
        synchronized(lock) { items.removeIf({ x => refEq(x, r) }) }
    }

    func pick(i: Int64): Option<ClusterSharding> {
        synchronized(lock) {
            if (items.isEmpty()) { None } else { Some(items[i % items.size]) }
        }
    }
}

let HANDOFF_SHARDS: Int64 = 32

func trackedSharding(n: CNode, acts: Activations): ClusterSharding {
    let addr = n.address()
    ClusterSharding(n.cluster, n.kit.system, n.remote, "tracked", HANDOFF_SHARDS,
        { id => TrackedShardEntity(id, addr, acts) })
}

func shardsSettled(regions: Array<ClusterSharding>, members: Int64): Bool {
    for (r in regions) {
        if (r.ownerCount() != members) {
            return false
        }
    }
    true
}

/** 全部分片都已被某个节点获取（托管总数 = 分片数，没有正在获取的） */
func allAcquired(regions: Array<ClusterSharding>, shards: Int64): Bool {
    var hosted: Int64 = 0
    for (r in regions) {
        let (h, acquiring, _) = r.shardStats()
        if (acquiring > 0) {
            return false
        }
        hosted += h
    }
    hosted == shards
}

@Test
public class ShardingHandOffTest {
    @TestCase
    public func singleActivationWhileMembersJoinAndLeave(): Unit {
        let a = CNode("ho-churn-a")
        let b = CNode("ho-churn-b")
        a.join([a.addr()], FAST)
        b.join([a.addr()], FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp([a, b], 2) }), true)
        let acts = Activations()
        let ra = trackedSharding(a, acts)
        let rb = trackedSharding(b, acts)
        let live = LiveRegions()
        live.add(ra)
        live.add(rb)
        // 发送线程：60 个实体轮流发，每轮之间稍停，持续到成员变化全部结束
        let running = AtomicBool(true)
        let sent = AtomicInt64(0)
        let pump = spawn {
            var k: Int64 = 0
            while (running.load()) {
                for (e in 0..60) {
                    if (let Some(r) <- live.pick(k + e)) {
                        r.tell("t-${e}", StringMessage("${k}"))
                        sent.fetchAdd(1)
                    }
                }
                k++
                sleep(Duration.millisecond * 2)
            }
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => acts.startCount() >= 60 }), true)
        // 两个节点先后加入：部分分片移到新节点
        let c = CNode("ho-churn-c").join([a.addr()], FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp([a, b, c], 3) }), true)
        let rc = trackedSharding(c, acts)
        live.add(rc)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rc.localEntityCount() > 0 }), true)
        let d = CNode("ho-churn-d").join([a.addr()], FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp([a, b, c, d], 4) }), true)
        let rd = trackedSharding(d, acts)
        live.add(rd)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rd.localEntityCount() > 0 }), true)
        // 两个节点先后优雅离开：它们的分片移交给剩下的节点
        live.remove(rc)
        c.cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => c.cluster.isTerminated() && allUp([a, b, d], 3) }), true)
        live.remove(rb)
        b.cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => b.cluster.isTerminated() && allUp([a, d], 2) }), true)
        // 剩下的两个节点接管全部实体并继续处理消息
        let before = acts.receivedCount()
        @Expect(awaitCondition(CLUSTER_WAIT, { => acts.receivedCount() > before + 600 }), true)
        running.store(false)
        pump.get()
        let report = synchronized(acts.lock) { acts.overlapDetails.toArray() }
        @Expect(acts.overlapCount(), 0)
        if (acts.overlapCount() > 0) {
            println("重叠：${report}")
        }
        @Expect(acts.maxLive() <= 1, true)
        // 确实发生过移交（否则本用例什么也没验证）
        @Expect(acts.movedEntities() > 10, true)
        @Expect(ra.localEntityCount() + rd.localEntityCount() <= 60, true)
        ra.stop()
        rd.stop()
        closeAll([a, b, c, d])
    }

    @TestCase
    public func blackholedNodeDoesNotStallOtherShards(): Unit {
        let partition = Partition()
        let nodes = startPartitionable(3, "ho-bh", FAST, partition)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let ledger = Ledgers()
        let regions = Array<ClusterSharding>(3, { i =>
            let n = nodes[i]
            let addr = n.address()
            ClusterSharding(n.cluster, n.kit.system, n.remote, "bh", HANDOFF_SHARDS, { id => CountingEntity(id, addr, ledger) })
        })
        // 先等各节点获取完自己的分片：获取要征得每个成员的确认，正在获取时黑洞一个节点，这些分片要等它恢复或被 down
        @Expect(awaitCondition(CLUSTER_WAIT, { => shardsSettled(regions, 3) && allAcquired(regions, HANDOFF_SHARDS) }), true)
        let addrs = addressesOf(nodes)
        let victim = addrs[2]
        // 黑洞第三个节点（不 down：NoDowning）
        partition.split([[addrs[0], addrs[1]], [victim]])
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            nodes[0].cluster.state().unreachable.contains(victim) && nodes[1].cluster.state().unreachable.contains(victim)
        }), true)
        let healthy = ArrayList<String>()
        let stranded = ArrayList<String>()
        for (e in 0..120) {
            let id = "bh-${e}"
            if (shardOwner(shardOf(id, HANDOFF_SHARDS), addrs) == Some(victim)) {
                stranded.add(id)
            } else {
                healthy.add(id)
            }
        }
        @Expect(stranded.size > 0 && healthy.size > 0, true)
        for (k in 0..120) {
            regions[k % 2].tell("bh-${k}", StringMessage("during"))
        }
        // 拥有者可达的分片照常投递（此前视图不收敛，全部缓冲）
        @Expect(awaitCondition(Duration.second * 5, { => ledger.count() == healthy.size }), true)
        for (id in stranded) {
            @Expect(ledger.hostsOf(id).size, 0)
        }
        let bufferedDuring = regions[0].stats()[4] + regions[1].stats()[4]
        @Expect(bufferedDuring, stranded.size)
        // 恢复连通：缓冲的消息送到第三个节点
        partition.heal()
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 120 }), true)
        for (id in stranded) {
            @Expect(ledger.hostsOf(id), [victim])
        }
        for (r in regions) {
            r.stop()
        }
        closeAll(nodes)
    }

    @TestCase
    public func messagesAfterLeavingGoToDeadLetters(): Unit {
        let nodes = startCluster(2, "ho-left", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let ledger = Ledgers()
        let regions = shardings(nodes, ledger)
        @Expect(awaitCondition(CLUSTER_WAIT, { => shardsSettled(regions, 2) }), true)
        let dead = EventRecorder()
        nodes[1].kit.system.subscribeDeadLetters({ m, path => dead.record(path) })
        nodes[1].cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[1].cluster.isTerminated() }), true)
        for (e in 0..5) {
            regions[1].tell("gone-${e}", StringMessage("late"))
        }
        // 此前离开后的区域把消息留在缓冲区里，永远不处理
        @Expect(dead.awaitSize(5, Duration.second * 5), true)
        @Expect(dead.contains("sharding/counter/gone-0"), true)
        @Expect(regions[1].stats()[3], 5)
        @Expect(regions[1].stats()[4], 0)
        for (r in regions) {
            r.stop()
        }
        closeAll(nodes)
    }

    @TestCase
    public func shardsAreHostedOnlyAfterAcquisition(): Unit {
        let nodes = startCluster(3, "ho-acq", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let ledger = Ledgers()
        let regions = shardings(nodes, ledger)
        // 视图建立后各节点立即获取自己的分片（不等第一条消息）：托管总数 = 分片数，没有分片被两个节点托管
        @Expect(awaitCondition(CLUSTER_WAIT, { => allAcquired(regions, 64) }), true)
        for (r in regions) {
            @Expect(r.shardStats()[2] > 0, true)
            r.stop()
        }
        closeAll(nodes)
    }
}

@Test
public class JoinCompatibilityTest {
    @TestCase
    public func incompatibilityIsReportedPerKey(): Unit {
        let mine = [("app.protocol", "1"), ("cactor.sharding.hash", "2")]
        let same = HashMap<String, String>([("app.protocol", "1"), ("cactor.sharding.hash", "2")])
        @Expect(joinIncompatibility(mine, Some(same)), None<String>)
        let other = HashMap<String, String>([("app.protocol", "2"), ("cactor.sharding.hash", "2"), ("x", "y")])
        let reason = joinIncompatibility(mine, Some(other)).getOrThrow()
        @Expect(reason.contains("app.protocol：本节点 1，对方 2"), true)
        @Expect(reason.contains("x：本节点没有，对方 y"), true)
        let missing = HashMap<String, String>([("app.protocol", "1")])
        @Expect(joinIncompatibility(mine, Some(missing)).getOrThrow().contains("cactor.sharding.hash：本节点 2，对方没有"), true)
        @Expect(joinIncompatibility(mine, None<HashMap<String, String>>).isSome(), true)
        var rejected = false
        try {
            FAST.withJoinCompatibility("cactor.sharding.hash", "1")
        } catch (e: IllegalArgumentException) {
            rejected = true
        }
        @Expect(rejected, true)
    }

    @TestCase
    public func incompatibleNodeIsRejectedAndNeverHostsEntities(): Unit {
        let v1 = FAST.withJoinCompatibility("app.protocol", "1")
        let v2 = FAST.withJoinCompatibility("app.protocol", "2")
        let a = CNode("cc-a")
        let b = CNode("cc-b")
        a.join([a.addr()], v1)
        b.join([a.addr()], v1)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp([a, b], 2) }), true)
        let c = CNode("cc-c").join([a.addr()], v2)
        @Expect(awaitCondition(CLUSTER_WAIT, { => c.cluster.joinRejection().isSome() }), true)
        @Expect(c.cluster.joinRejection().getOrThrow().contains("app.protocol：本节点 1，对方 2"), true)
        @Expect(a.cluster.state().member(c.address()).isNone(), true)
        // 三个节点都创建同一种分片：被拒绝的节点没有可用视图，不托管任何实体
        let ledger = Ledgers()
        let regions = shardings([a, b, c], ledger)
        @Expect(awaitCondition(CLUSTER_WAIT, { => regions[0].ownerCount() == 2 && regions[1].ownerCount() == 2 }), true)
        for (e in 0..40) {
            regions[e % 2].tell("v-${e}", StringMessage("ok"))
            regions[2].tell("v-${e}", StringMessage("from-rejected"))
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 40 }), true)
        @Expect(regions[2].localEntityCount(), 0)
        @Expect(regions[2].ownerCount(), 0)
        for (e in 0..40) {
            let hosts = ledger.hostsOf("v-${e}")
            @Expect(hosts.size, 1)
            @Expect(hosts[0] != c.address(), true)
        }
        for (r in regions) {
            r.stop()
        }
        @Expect(a.cluster.state().member(c.address()).isNone(), true)
        closeAll([a, b, c])
    }

    @TestCase
    public func legacyJoinWithoutCompatibilityIsRejected(): Unit {
        let nodes = startCluster(1, "cc-legacy", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        // 冒充 E42 之前的节点：JOIN 只有 地址|启动标识 两个字段
        let kit = ActorTestKit("cc-legacy-joiner")
        let fake = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        fake.start()
        let rec = EventRecorder()
        fake.exposeActor("cluster", kit.actorOf({ => TextSink(rec) }, "sink"))
        let me = fake.getLocalAddress().toString()
        fake.sendControl(nodes[0].addr(), "cluster", StringMessage(FieldWriter().put("JOIN").put(me).put("4242").done()))
        @Expect(awaitCondition(CLUSTER_WAIT, { => rec.snapshot().iterator().any({ t => FieldReader(t).next() == "REJECT" }) }), true)
        let reject = rec.snapshot().iterator().filter({ t => FieldReader(t).next() == "REJECT" }).next().getOrThrow()
        let r = FieldReader(reject)
        r.next()
        @Expect(r.next(), nodes[0].address())
        @Expect(r.next().contains("没有提供兼容信息"), true)
        @Expect(nodes[0].cluster.state().member(me).isNone(), true)
        fake.stop()
        kit.shutdown()
        closeAll(nodes)
    }
}
