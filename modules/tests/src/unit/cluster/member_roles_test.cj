/*
 * CActor - 集群成员角色（对标 Akka Member.roles）
 *
 * 覆盖：
 *   - 本节点角色通过 gossip 传播到其他节点，`membersWithRole` / `Cluster.roles()` 一致；
 *   - 与"没有角色的节点"互发 gossip 时角色不会被抹掉（旧版本/未配置角色的对端安全）；
 *   - 集群感知路由器按角色过滤候选，只把消息发到具备该角色的成员。
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
import cactor_cluster.membership.{Cluster, ClusterSettings, Member, MemberStatus}
import cactor_cluster.routing.{ClusterRouter, ClusterRouterStrategy}
import cactor_testkit.*

class RoleEcho <: Actor {
    let node: String
    let rec: EventRecorder

    init(node: String, rec: EventRecorder) {
        this.node = node
        this.rec = rec
    }

    public prop name: String { get() { "role-echo" } }
    public prop description: String { get() { "role echo" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record("${node}:${s.getContent()}")
        }
        MessageResult.Handled
    }
}

class RoleNode {
    let kit: ActorTestKit
    let remote: SimpleRemoteManager
    let cluster: Cluster

    init(name: String, seed: Option<NetworkAddress>, roles: Array<String>) {
        this.kit = ActorTestKit(name)
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let s = match (seed) {
            case Some(a) => a
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(kit.system, remote, [s], FAST, roles: roles)
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

func rolesConverged(nodes: Array<RoleNode>, role: String, n: Int64): Bool {
    for (node in nodes) {
        if (node.cluster.state().membersWithRole(role).size != n || !node.cluster.state().converged) {
            return false
        }
    }
    true
}

@Test
public class MemberRolesTest {
    @TestCase
    public func memberRolesAreNormalizedAndPreserved(): Unit {
        let m = Member("a:1", MemberStatus.Up, 1, 7, ["worker", "backend", "worker", ""])
        @Expect(m.roles, ["backend", "worker"])
        @Expect(m.hasRole("worker"), true)
        @Expect(m.hasRole("frontend"), false)
        @Expect(m.withStatus(MemberStatus.Down).roles, ["backend", "worker"])
        @Expect(m.withRoles(["z", "a"]).roles, ["a", "z"])
        @Expect(m.sameAs(m.withRoles(["backend", "worker"])), true)
        @Expect(m.sameAs(m.withRoles(["backend"])), false)
    }

    @TestCase
    public func rolesAreVisibleAcrossRealCluster(): Unit {
        let first = RoleNode("roles-a", None<NetworkAddress>, ["frontend"])
        let seed = NetworkAddress.parse(first.address())
        let second = RoleNode("roles-b", Some<NetworkAddress>(seed), ["worker"])
        let third = RoleNode("roles-c", Some<NetworkAddress>(seed), ["worker", "backend"])
        let nodes = [first, second, third]
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => rolesConverged(nodes, "worker", 2) }), true)
            @Expect(awaitCondition(CLUSTER_WAIT, { => allUpSnapshots(nodes, 3) }), true)
            // 无角色的节点（frontend）不会让 worker 角色消失
            for (n in nodes) {
                @Expect(n.cluster.state().membersWithRole("frontend").size, 1)
                @Expect(n.cluster.state().membersWithRole("backend").size, 1)
                @Expect(n.cluster.state().hasRole(first.address(), "worker"), false)
                @Expect(n.cluster.state().hasRole(second.address(), "worker"), true)
            }
        } finally {
            for (n in nodes) {
                n.shutdown()
            }
        }
    }

    @TestCase
    public func clusterRouterFiltersByRole(): Unit {
        let first = RoleNode("router-a", None<NetworkAddress>, ["frontend"])
        let seed = NetworkAddress.parse(first.address())
        let second = RoleNode("router-b", Some<NetworkAddress>(seed), ["worker"])
        let third = RoleNode("router-c", Some<NetworkAddress>(seed), ["worker"])
        let nodes = [first, second, third]
        let rec = EventRecorder()
        try {
            // 三个节点都暴露同名服务，但只有 worker 角色应被选中
            for (n in nodes) {
                let ref = n.kit.system.actorOf({ => RoleEcho(n.address(), rec) },
                    "svc-${n.address().replace(":", "_").replace(".", "_")}")
                n.remote.exposeActor("svc", ref)
            }
            @Expect(awaitCondition(CLUSTER_WAIT, { => allUpSnapshots(nodes, 3) }), true)
            let router = ClusterRouter(first.cluster, first.remote, "svc",
                strategy: ClusterRouterStrategy.RoundRobin).withRole("worker")
            @Expect(router.candidates().size, 2)
            @Expect(router.candidates().contains(first.address()), false)
            for (i in 0..6) {
                router.tell(StringMessage("job-${i}"))
            }
            @Expect(awaitCondition(CLUSTER_WAIT, { => rec.size() == 6 }), true)
            var frontendHits: Int64 = 0
            for (line in rec.snapshot()) {
                if (line.startsWith(first.address() + ":")) {
                    frontendHits++
                }
            }
            @Expect(frontendHits, 0)
            router.stop()
        } finally {
            for (n in nodes) {
                n.shutdown()
            }
        }
    }
}

func allUpSnapshots(nodes: Array<RoleNode>, n: Int64): Bool {
    for (node in nodes) {
        if (node.cluster.state().upCount() != n || !node.cluster.state().converged) {
            return false
        }
    }
    true
}
