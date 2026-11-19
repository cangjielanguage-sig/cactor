/*
 * CActor - 差距 4.5：集群感知路由器（ClusterRouter）
 *
 * 覆盖：
 *   - LeastLoaded：按应用提供的负载选择最低者，相同负载按地址最小者；
 *   - RoundRobin：候选按地址排序后轮询，各成员均匀分到；
 *   - 成员被 down 后自动从候选中剔除；集群终止 / 路由器停止后无候选；
 *   - 无候选时 route/tell 明确失败（不静默丢消息）。
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
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings}
import cactor_cluster.routing.{ClusterRouter, ClusterRouterStrategy}
import cactor_testkit.*

class RouterSink <: Actor {
    private let lock = Mutex()
    private let items = ArrayList<String>()

    public prop name: String { get() { "router-sink" } }
    public prop description: String { get() { "router sink" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            synchronized(lock) {
                items.add(s.getContent())
            }
        }
        MessageResult.Handled
    }

    func count(): Int64 {
        synchronized(lock) {
            items.size
        }
    }
}

@Test
public class ClusterRouterTest {
    @TestCase
    public func routingPicksByStrategyAndDropsDowned(): Unit {
        let nodes = Array<CNode>(3, { i => CNode("router-${i}") })
        let seed = nodes[0].addr()
        for (n in nodes) {
            n.join([seed], FAST)
        }
        let leastSinks = Array<RouterSink>(3, { _ => RouterSink() })
        let rrSinks = Array<RouterSink>(3, { _ => RouterSink() })
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[0].cluster.state().upCount() == 3 }), true)
            for (i in 0..3) {
                nodes[i].remote.exposeActor("least-worker",
                    nodes[i].kit.actorOf({ => leastSinks[i] }, "least-${i}"))
                nodes[i].remote.exposeActor("rr-worker",
                    nodes[i].kit.actorOf({ => rrSinks[i] }, "rr-${i}"))
            }

            // ---- LeastLoaded ----
            let loads = HashMap<String, Int64>()
            for (i in 0..3) {
                loads[nodes[i].address()] = if (i == 1) { 1 } else if (i == 2) { 3 } else { 5 }
            }
            let least = ClusterRouter(nodes[0].cluster, nodes[0].remote, "least-worker",
                strategy: ClusterRouterStrategy.LeastLoaded, loadOf: { a: String => loads.get(a) ?? 0 })
            @Expect(least.candidates().size, 3)
            @Expect(least.route(), nodes[1].address())
            for (i in 0..5) {
                least.tell(StringMessage("job-${i}"))
            }
            @Expect(awaitCondition(CLUSTER_WAIT, { => leastSinks[1].count() == 5 }), true)
            @Expect(leastSinks[0].count(), 0)
            @Expect(leastSinks[2].count(), 0)

            // 负载变化后选择随之改变
            loads[nodes[1].address()] = 100
            loads[nodes[2].address()] = 0
            @Expect(least.route(), nodes[2].address())

            // ---- RoundRobin ----
            let rr = ClusterRouter(nodes[0].cluster, nodes[0].remote, "rr-worker")
            for (i in 0..6) {
                rr.tell(StringMessage("rr-${i}"))
            }
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                rrSinks[0].count() == 2 && rrSinks[1].count() == 2 && rrSinks[2].count() == 2
            }), true)

            // ---- 成员被 down 后自动剔除 ----
            let downed = nodes[2].address()
            nodes[0].cluster.down(downed)
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                let cs = rr.candidates()
                !cs.contains(downed) && cs.size == 2
            }), true)
            for (i in 0..20) {
                @Expect(rr.route() != downed, true)
            }
            least.stop()
            @ExpectThrows[IllegalStateException](least.route())
        } finally {
            closeAll(nodes)
        }
    }

    @TestCase
    public func noCandidatesFailsExplicitly(): Unit {
        let node = CNode("router-single")
        node.join([node.addr()], FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => node.cluster.state().upCount() == 1 }), true)
        let router = ClusterRouter(node.cluster, node.remote, "worker")
        @Expect(router.candidates().size, 1)
        @Expect(router.route(), node.address())
        // down 自己 → 集群视图终止（shutdown 只停守护 Actor，不改 terminated）
        node.cluster.down(node.address())
        @Expect(awaitCondition(CLUSTER_WAIT, { => node.cluster.isTerminated() }), true)
        @Expect(router.candidates().isEmpty(), true)
        var routeError = ""
        try {
            let _ = router.route()
        } catch (e: Exception) {
            routeError = e.message
        }
        @Expect(routeError.contains("没有可用候选成员"), true)
        var tellError = ""
        try {
            router.tell(StringMessage("x"))
        } catch (e: Exception) {
            tellError = e.message
        }
        @Expect(tellError.contains("没有可用候选成员"), true)
        closeAll([node])
    }
}
