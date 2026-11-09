/*
 * CActor - 分布式补齐测试（计划 §3.10）：
 * - 单例 HandOver 握手（N16）：新实例在旧实例停止（先处理完邮箱）之后才启动，任何时刻最多一个
 * - 集群版 Receptionist（E63）：跨节点登记与发现、实例停止自动注销、节点宕机后登记被删除、订阅变化
 * - Replicator 一致性级别（E64）：过半写 + 过半读必然读到；少数派一侧过半写 / 读失败，全部写在有节点不可达时失败
 * 全部在本机回环 TCP 上的真实节点之间进行。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.collection.*
import std.sort.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_cluster.membership.*
import cactor_cluster.ddata.{Replicator, ReplicatedValue, WriteConsistency, ReadConsistency,
    ReplicatorConsistencyException}
import cactor_foundation.network.{NetworkTransport}
import cactor_testkit.*

/** 存活实例计数（全部节点共享，同一进程内） */
class LiveCounter {
    let live = AtomicInt64(0)
    let maxLive = AtomicInt64(0)

    func enter(): Unit {
        let v = live.fetchAdd(1) + 1
        while (true) {
            let m = maxLive.load()
            if (v <= m || maxLive.compareAndSwap(m, v)) {
                break
            }
        }
    }

    func leave(): Unit {
        live.fetchSub(1)
    }
}

/** 停止较慢的单例：postStop 停顿 300 ms，暴露"新实例在旧实例停止前启动"的重叠窗口 */
class SlowStopSingleton <: Actor {
    let node: String
    let rec: EventRecorder
    let counter: LiveCounter

    init(node: String, rec: EventRecorder, counter: LiveCounter) {
        this.node = node
        this.rec = rec
        this.counter = counter
    }

    public prop name: String { get() { "slow-singleton" } }
    public prop description: String { get() { "slow singleton" } }

    public func preStart(): Unit {
        counter.enter()
        rec.record("start@${node}")
    }

    public func postStop(): Unit {
        sleep(Duration.millisecond * 300)
        rec.record("stop@${node}")
        counter.leave()
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record("${s.getContent()}@${node}")
        }
        MessageResult.Handled
    }
}

@Test
public class SingletonHandOverTest {
    /** 最老成员离开：新的最老成员等旧实例停止后才启动（此前目标一变就启动，与正在停止的旧实例重叠） */
    @TestCase
    public func handOverWaitsForPreviousInstanceToStop(): Unit {
        let nodes = startCluster(3, "handover-leave", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let rec = EventRecorder()
        let counter = LiveCounter()
        let singletons = Array<ClusterSingleton>(3, { i =>
            let n = nodes[i]
            let addr = n.address()
            ClusterSingleton(n.cluster, n.kit.system, n.remote, "slow", { => SlowStopSingleton(addr, rec, counter) })
        })
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[0].isRunningHere() }), true)
        let old = nodes[0].address()
        singletons[2].tell(StringMessage("before"))
        @Expect(rec.await("before@${old}", CLUSTER_WAIT), true)
        nodes[0].cluster.leave()
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[1].isRunningHere() || singletons[2].isRunningHere() }), true)
        let next = if (singletons[1].isRunningHere()) { nodes[1].address() } else { nodes[2].address() }
        singletons[2].tell(StringMessage("after"))
        @Expect(rec.await("after@${next}", CLUSTER_WAIT), true)
        // 任何时刻最多一个实例；旧实例的 postStop 完成在新实例启动之前
        @Expect(counter.maxLive.load(), 1)
        @Expect(rec.indexOf("stop@${old}") >= 0 && rec.indexOf("stop@${old}") < rec.indexOf("start@${next}"), true)
        for (s in singletons) {
            s.stop()
        }
        closeAll(nodes)
    }

    /** 最老成员宕机并被 down：不需要它确认（已 down 的成员视为已停止），实例在下一个最老成员上启动 */
    @TestCase
    public func crashedOldestDoesNotBlockHandOver(): Unit {
        let nodes = startCluster(3, "handover-crash", FAST_AUTODOWN)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let rec = EventRecorder()
        let counter = LiveCounter()
        let singletons = Array<ClusterSingleton>(3, { i =>
            let n = nodes[i]
            let addr = n.address()
            ClusterSingleton(n.cluster, n.kit.system, n.remote, "slow", { => SlowStopSingleton(addr, rec, counter) })
        })
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[0].isRunningHere() }), true)
        nodes[0].crash()
        @Expect(awaitCondition(CLUSTER_WAIT, { => singletons[1].isRunningHere() || singletons[2].isRunningHere() }), true)
        let next = if (singletons[1].isRunningHere()) { nodes[1].address() } else { nodes[2].address() }
        singletons[1].tell(StringMessage("x"))
        @Expect(rec.await("x@${next}", CLUSTER_WAIT), true)
        @Expect(singletons[1].startCount() + singletons[2].startCount(), 1)
        singletons[1].stop()
        singletons[2].stop()
        nodes[1].close()
        nodes[2].close()
    }
}

class RNode {
    let node: CNode
    let rep: Replicator
    let receptionist: ClusterReceptionist

    init(node: CNode) {
        this.node = node
        this.rep = Replicator(node.kit.system, node.remote, Duration.millisecond * 50)
        this.receptionist = ClusterReceptionist(node.cluster, node.kit.system, node.remote, rep)
    }
}

func receptionistCluster(name: String, settings: ClusterSettings): Array<RNode> {
    let nodes = startCluster(3, name, settings)
    if (!awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) })) {
        throw Exception("集群没有在期限内形成")
    }
    Array<RNode>(3, { i => RNode(nodes[i]) })
}

@Test
public class ClusterReceptionistTest {
    /** 两个节点登记、第三个节点发现并调用：本节点得到本地引用，其他节点得到远程引用，回复经网络送回 */
    @TestCase
    public func registerFindAndCallAcrossNodes(): Unit {
        let rs = receptionistCluster("rcpt-basic", FAST)
        let e1 = rs[1].node.kit.actorOf({ => EchoActor("n1") }, "echo")
        let e2 = rs[2].node.kit.actorOf({ => EchoActor("n2") }, "echo")
        rs[1].receptionist.register("echo", e1)
        rs[2].receptionist.register("echo", e2)
        rs[2].receptionist.register("echo", e2) // 重复登记只算一次
        @Expect(rs[2].receptionist.localRegistrationCount(), 1)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[0].receptionist.find("echo").size == 2 }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[1].receptionist.find("echo").size == 2 }), true)
        // 本节点的实例是本地引用
        var localFound = false
        for (r in rs[1].receptionist.find("echo")) {
            if (r.path.toString() == e1.path.toString()) {
                localFound = true
            }
        }
        @Expect(localFound, true)
        let rec = EventRecorder()
        let collector = rs[0].node.kit.actorOf({ => ReplyCollector("c", rec) }, "collector")
        for (r in rs[0].receptionist.find("echo")) {
            r.tell(StringMessage("hi"), collector)
        }
        @Expect(rec.await("c|n1:hi", CLUSTER_WAIT), true)
        @Expect(rec.await("c|n2:hi", CLUSTER_WAIT), true)
        @Expect(rs[0].receptionist.find("nobody").size, 0)
        closeAll([rs[0].node, rs[1].node, rs[2].node])
    }

    /** 实例停止后自动注销；显式注销 */
    @TestCase
    public func stoppedOrDeregisteredInstancesDisappear(): Unit {
        let rs = receptionistCluster("rcpt-stop", FAST)
        let e1 = rs[1].node.kit.actorOf({ => EchoActor("n1") }, "echo")
        let e2 = rs[2].node.kit.actorOf({ => EchoActor("n2") }, "echo")
        rs[1].receptionist.register("echo", e1)
        rs[2].receptionist.register("echo", e2)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[0].receptionist.find("echo").size == 2 }), true)
        rs[2].node.kit.system.stop(e2)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[0].receptionist.find("echo").size == 1 }), true)
        @Expect(rs[2].receptionist.localRegistrationCount(), 0)
        rs[1].receptionist.deregister("echo", e1)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[0].receptionist.find("echo").size == 0 }), true)
        closeAll([rs[0].node, rs[1].node, rs[2].node])
    }

    /** 节点宕机并被 down：它的登记从全集群删除；订阅者收到变化 */
    @TestCase
    public func crashedNodeRegistrationsAreRemovedAndSubscribersNotified(): Unit {
        let rs = receptionistCluster("rcpt-crash", FAST_AUTODOWN)
        let sizes = EventRecorder()
        rs[0].receptionist.subscribe("echo", { refs => sizes.record("size=${refs.size}") })
        rs[1].receptionist.register("echo", rs[1].node.kit.actorOf({ => EchoActor("n1") }, "echo"))
        rs[2].receptionist.register("echo", rs[2].node.kit.actorOf({ => EchoActor("n2") }, "echo"))
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[0].receptionist.find("echo").size == 2 }), true)
        @Expect(sizes.await("size=2", CLUSTER_WAIT), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[0].rep.setElements("receptionist/echo").size == 2 }), true)
        sizes.clear()
        rs[2].node.crash()
        // 不可达期间 find 已不返回它（findAll 仍包含），down 并移除后它的登记从登记表删除，订阅者收到变化
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[0].receptionist.find("echo").size == 1 }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[0].receptionist.findAll("echo").size == 1 }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            rs[0].rep.setElements("receptionist/echo").size == 1 && rs[1].rep.setElements("receptionist/echo").size == 1
        }), true)
        @Expect(sizes.await("size=1", CLUSTER_WAIT), true)
        rs[0].node.close()
        rs[1].node.close()
    }
}

func setOf(v: Option<ReplicatedValue>): Array<String> {
    match (v) {
        case Some(ORSetValue(s)) =>
            let es = s.elements()
            sort(es)
            es
        case _ => Array<String>()
    }
}

func failsWithConsistency(f: () -> Unit): Bool {
    try {
        f()
        false
    } catch (_: ReplicatorConsistencyException) {
        true
    }
}

@Test
public class ReplicatorConsistencyTest {
    /** 过半写成功后，从任一节点过半读都能读到（两个过半集合必然相交），读到的合并值写回本地 */
    @TestCase
    public func majorityWriteThenMajorityReadSeesValue(): Unit {
        let nodes = cluster(3, "rc-majority")
        for (k in 0..10) {
            nodes[k % 3].rep.addToSetWith("k", "e${k}", WriteConsistency.WriteMajority(CLUSTER_WAIT)).get(CLUSTER_WAIT)
            let seen = setOf(nodes[(k + 1) % 3].rep.getWith("k", ReadConsistency.ReadMajority(CLUSTER_WAIT)).get(CLUSTER_WAIT))
            @Expect(seen.contains("e${k}"), true)
        }
        nodes[0].rep.incrementCounterWith("c", 5, WriteConsistency.WriteAll(CLUSTER_WAIT)).get(CLUSTER_WAIT)
        // WriteAll 成功后每个节点的本地副本都已有这次增量
        for (n in nodes) {
            @Expect(n.rep.counterValue("c"), 5)
        }
        // 本地级别立即完成
        @Expect(setOf(nodes[2].rep.getWith("missing", ReadConsistency.ReadLocal).get(CLUSTER_WAIT)).size, 0)
        shutdownAll(nodes)
    }

    /** E64 验收：分区后少数派一侧的过半写与过半读失败，多数派一侧成功；有节点不可达时 WriteAll 失败；恢复后值照常传播 */
    @TestCase
    public func majorityFailsOnMinoritySide(): Unit {
        let partition = Partition()
        let nodes = Array<Node>(3, { i => Node("rc-split-${i}", Some<NetworkTransport>(PartitionTransport(partition))) })
        mesh(nodes)
        let a = nodes[0].addr().toString()
        let b = nodes[1].addr().toString()
        let c = nodes[2].addr().toString()
        partition.split([[a, b], [c]])
        let short = Duration.millisecond * 800
        @Expect(failsWithConsistency({ =>
            nodes[2].rep.addToSetWith("k", "minority", WriteConsistency.WriteMajority(short)).get(CLUSTER_WAIT)
        }), true)
        @Expect(failsWithConsistency({ =>
            nodes[2].rep.getWith("k", ReadConsistency.ReadMajority(short)).get(CLUSTER_WAIT)
        }), true)
        // 多数派一侧成功
        nodes[0].rep.addToSetWith("k", "majority", WriteConsistency.WriteMajority(CLUSTER_WAIT)).get(CLUSTER_WAIT)
        @Expect(setOf(nodes[1].rep.getWith("k", ReadConsistency.ReadMajority(CLUSTER_WAIT)).get(CLUSTER_WAIT)).contains("majority"), true)
        @Expect(failsWithConsistency({ =>
            nodes[0].rep.addToSetWith("k", "all", WriteConsistency.WriteAll(short)).get(CLUSTER_WAIT)
        }), true)
        // 失败的写已在本地生效，恢复后经反熵传播到全部节点（同 Akka）
        partition.heal()
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            var ok = true
            for (n in nodes) {
                ok = ok && n.rep.setElements("k") == ["all", "majority", "minority"]
            }
            ok
        }), true)
        shutdownAll(nodes)
    }
}
