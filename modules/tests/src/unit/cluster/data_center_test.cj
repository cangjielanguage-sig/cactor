/*
 * CActor - 数据中心属性与 DC 感知路由（差距 4.3 的部分实现）
 *
 * 覆盖：
 *   - Member 的 dataCenter 默认值、替换与比较；
 *   - 真实集群里各节点的数据中心通过 gossip 传播（跨 DC 成员仍在同一成员表，本轮不做 gossip 隔离）；
 *   - ClusterRouter.withDataCenter 只选该 DC 的 Up 成员，可与角色过滤叠加。
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
import cactor_cluster.membership.{Cluster, DEFAULT_DATA_CENTER, Member, MemberStatus}
import cactor_cluster.routing.{ClusterRouter, ClusterRouterStrategy}
import cactor_testkit.*

class DcEcho <: Actor {
    let node: String
    let rec: EventRecorder

    init(node: String, rec: EventRecorder) {
        this.node = node
        this.rec = rec
    }

    public prop name: String { get() { "dc-echo" } }
    public prop description: String { get() { "dc echo" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record("${node}:${s.getContent()}")
        }
        MessageResult.Handled
    }
}

class DcNode {
    let kit: ActorTestKit
    let remote: SimpleRemoteManager
    let cluster: Cluster

    init(name: String, seed: Option<NetworkAddress>, roles: Array<String>, dc: String) {
        this.kit = ActorTestKit(name)
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let s = match (seed) {
            case Some(a) => a
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(kit.system, remote, [s], FAST, roles: roles, dataCenter: dc)
    }

    func address(): String {
        remote.getLocalAddress().toString()
    }

    func shutdown(): Unit {
        if (!cluster.isTerminated()) {
            cluster.shutdown()
        }
        remote.stop()
        kit.shutdown()
    }
}

func dcConverged(nodes: Array<DcNode>, dcs: Int64): Bool {
    for (n in nodes) {
        let s = n.cluster.state()
        if (s.upCount() != Int64(nodes.size) || s.dataCenters().size != dcs) {
            return false
        }
    }
    true
}

@Test
public class DataCenterTest {
    @TestCase
    public func memberDataCenterDefaultsAndReplaces(): Unit {
        let plain = Member("a:1", MemberStatus.Up, 1, 1)
        @Expect(plain.dataCenter, DEFAULT_DATA_CENTER)
        let dc = plain.withDataCenter("dc1")
        @Expect(dc.dataCenter, "dc1")
        @Expect(dc.withStatus(MemberStatus.Leaving).dataCenter, "dc1")
        @Expect(dc.withRoles(["w"]).dataCenter, "dc1")
        @Expect(plain.sameAs(dc), false)
        @Expect(dc.sameAs(plain.withDataCenter("dc1")), true)
    }

    @TestCase
    public func dataCenterPropagatesAndRouterFiltersByIt(): Unit {
        let a1 = DcNode("dc-a1", None<NetworkAddress>, ["app"], "dc1")
        let seed = NetworkAddress.parse(a1.address())
        let a2 = DcNode("dc-a2", Some<NetworkAddress>(seed), ["app"], "dc1")
        let b1 = DcNode("dc-b1", Some<NetworkAddress>(seed), ["app"], "dc2")
        let b2 = DcNode("dc-b2", Some<NetworkAddress>(seed), [], "dc2")
        let nodes = [a1, a2, b1, b2]
        let rec = EventRecorder()
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => dcConverged(nodes, 2) }), true)
            for (n in nodes) {
                @Expect(n.cluster.state().membersInDataCenter("dc1").size, 2)
                @Expect(n.cluster.state().membersInDataCenter("dc2").size, 2)
                @Expect(n.cluster.dataCenter(), if (n.address() == b1.address() || n.address() == b2.address()) { "dc2" } else { "dc1" })
            }
            // 所有节点都暴露同名服务
            for (n in nodes) {
                let addr = n.address()
                let ref = n.kit.system.actorOf({ => DcEcho(addr, rec) }, "dc-svc")
                n.remote.exposeActor("dc-svc", ref)
            }
            let router = ClusterRouter(a1.cluster, a1.remote, "dc-svc",
                strategy: ClusterRouterStrategy.RoundRobin).withDataCenter("dc1")
            @Expect(router.candidates().size, 2)
            @Expect(router.candidates().contains(b1.address()), false)
            // 角色 + DC 叠加：dc1 里只有 a1/a2 有 app 角色
            let roleRouter = ClusterRouter(a1.cluster, a1.remote, "dc-svc")
                .withRole("app").withDataCenter("dc1")
            @Expect(roleRouter.candidates().size, 2)
            for (i in 0..6) {
                router.tell(StringMessage("job-${i}"))
            }
            @Expect(awaitCondition(CLUSTER_WAIT, { => rec.size() == 6 }), true)
            var dc2Hits: Int64 = 0
            for (line in rec.snapshot()) {
                if (line.startsWith(b1.address() + ":") || line.startsWith(b2.address() + ":")) {
                    dc2Hits++
                }
            }
            @Expect(dc2Hits, 0)
            router.stop()
            roleRouter.stop()
        } finally {
            for (n in nodes) {
                n.shutdown()
            }
        }
    }
}
