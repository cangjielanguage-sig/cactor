/*
 * CActor Platform.Lifecycle/Session - 集群睡/醒与迁移（F1 迁移部分 + P8）验证
 *
 * 1. sessionWakesIfSleeping：会话睡下（实例停止、状态落盘）后，经 SessionRegistry 再次寻址
 *    会按存储里的状态唤醒并处理消息（单节点，真实集群视图）。
 * 2. sleepingSessionMigratesWhenOwnerDies：2 个真实节点，会话归属 owner；
 *    owner 崩溃、成员视图收敛后，发往幸存节点的消息由幸存节点接手唤醒，状态连续（不丢）。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.convert.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_persistence.{DurableStateStore, MemoryDurableStateStore}
import cactor_platform.lifecycle.{SleepableActor, ClusterSleepRegistry, SleepPolicy}
import cactor_platform.session.{SessionRegistry}
import cactor_testkit.*

/** 可睡眠的计数会话：状态就是一个自增计数，用于断言"睡醒后状态连续" */
class ClusterCountSession <: SleepableActor {
    private let key: String
    private let observed: AtomicInt64
    private var count: Int64 = 0

    init(key: String, restored: Option<String>, observed: AtomicInt64) {
        super(restored)
        this.key = key
        this.observed = observed
        match (restored) {
            case Some(s) => count = Int64.parse(s)
            case None => count = 0
        }
    }

    public prop name: String {
        get() { "cluster-count-session" }
    }

    public prop description: String {
        get() { "cluster sleepable counter" }
    }

    public func state(): String {
        count.toString()
    }

    public func restore(state: String): Unit {
        count = Int64.parse(state)
    }

    public func onMessage(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: StringMessage =>
                count++
                observed.store(count)
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

@Test
public class ClusterSleepTest {
    @TestCase
    public func sessionWakesIfSleeping(): Unit {
        let nodes = startCluster(1, "cluster-sleep-wake", FAST)
        let store = MemoryDurableStateStore()
        let observed = AtomicInt64(0)
        let sleepers = ArrayList<ClusterSleepRegistry>()
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
            let n = nodes[0]
            let sleeper = ClusterSleepRegistry(n.kit.system, n.cluster, n.remote, "wake",
                { key, restored => ClusterCountSession(key, restored, observed) },
                numberOfShards: 8, store: store, policy: SleepPolicy.Explicit)
            sleepers.add(sleeper)
            let registry = SessionRegistry(sleeper, typeName: "wake")

            registry.tell("s1", StringMessage("a"))
            @Expect(awaitCondition(Duration.second * 10, { => observed.load() == 1 }), true)
            @Expect(registry.localSessionCount(), 1)
            let owner = registry.ownerOf("s1")
            @Expect(owner.isSome(), true)
            @Expect(owner.getOrThrow(), n.address())

            // 睡：实例停止、状态落盘
            sleeper.handle("s1").sleep()
            @Expect(awaitCondition(Duration.second * 10, { => registry.localSessionCount() == 0 }), true)
            @Expect(store.get("cluster-sleepable/wake/s1").isSome(), true)

            // 再次寻址：睡着即唤醒，状态连续（1 -> 2）
            registry.session<StringMessage>("s1").tell(StringMessage("b"))
            @Expect(awaitCondition(Duration.second * 10, { => observed.load() == 2 }), true)
            @Expect(registry.localSessionCount(), 1)
        } finally {
            for (s in sleepers) {
                try {
                    s.stop()
                } catch (e: Exception) {
                    ()
                }
            }
            closeAll(nodes)
        }
    }

    @TestCase
    public func sleepingSessionMigratesWhenOwnerDies(): Unit {
        // FAST_AUTODOWN：崩溃节点被自动 down/移除，Up 集合收缩，归属才会落到幸存节点
        let nodes = startCluster(2, "cluster-sleep-mig", FAST_AUTODOWN)
        let store = MemoryDurableStateStore()
        let observed = AtomicInt64(0)
        let sleepers = ArrayList<ClusterSleepRegistry>()
        var victim = -1
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
            for (i in 0..2) {
                let n = nodes[i]
                sleepers.add(ClusterSleepRegistry(n.kit.system, n.cluster, n.remote, "mig",
                    { key, restored => ClusterCountSession(key, restored, observed) },
                    numberOfShards: 8, store: store, policy: SleepPolicy.Explicit))
            }
            let regs = Array<SessionRegistry>(2, { i => SessionRegistry(sleepers[i], typeName: "mig") })
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                sleepers[0].ownerCount() == 2 && sleepers[1].ownerCount() == 2
            }), true)

            // 选一个归属 node0 的 key，并故意从 node1 投递（覆盖跨节点转发）
            var id = ""
            for (k in 0..200) {
                let cand = "session-${k}"
                let o = sleepers[0].ownerOf(cand)
                if (o.isSome() && o.getOrThrow() == nodes[0].address()) {
                    id = cand
                    break
                }
            }
            @Expect(id.size > 0, true)
            let ownerIdx = 0
            let callerIdx = 1

            regs[callerIdx].tell(id, StringMessage("first"))
            @Expect(awaitCondition(CLUSTER_WAIT, { => observed.load() == 1 }), true)
            @Expect(awaitCondition(CLUSTER_WAIT, { => sleepers[ownerIdx].localActiveCount() == 1 }), true)

            // 归属节点上的实例睡下，状态落盘（跨节点共享同一个存储）
            sleepers[ownerIdx].handle(id).sleep()
            @Expect(awaitCondition(CLUSTER_WAIT, { => sleepers[ownerIdx].localActiveCount() == 0 }), true)
            @Expect(store.get("cluster-sleepable/mig/${id}").isSome(), true)

            // owner 崩溃；幸存节点收敛到 1 个 Up
            victim = ownerIdx
            nodes[victim].crash()
            @Expect(awaitCondition(CLUSTER_WAIT, { => allUp([nodes[callerIdx]], 1) }), true)

            // 幸存节点接手：按存储里的状态唤醒，计数从 1 继续到 2（不丢、不重置）
            regs[callerIdx].tell(id, StringMessage("after"))
            @Expect(awaitCondition(CLUSTER_WAIT, { => observed.load() == 2 }), true)
            @Expect(sleepers[callerIdx].localActiveCount(), 1)
            let ownerNow = regs[callerIdx].ownerOf(id)
            @Expect(ownerNow.isSome(), true)
            @Expect(ownerNow.getOrThrow(), nodes[callerIdx].address())
        } finally {
            for (s in sleepers) {
                try {
                    s.stop()
                } catch (e: Exception) {
                    ()
                }
            }
            // 崩溃节点已停机，只关闭未终止的节点
            for (n in nodes) {
                if (!n.cluster.isTerminated()) {
                    n.close()
                }
            }
        }
    }
}
