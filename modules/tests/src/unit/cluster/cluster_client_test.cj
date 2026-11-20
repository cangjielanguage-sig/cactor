/*
 * CActor - 差距 4.3 / C2c：集群客户端（外部进程不加入集群访问服务）
 *
 * 覆盖：
 *   - 从任一联系点查询 Up 成员（排序）；
 *   - 按成员轮询投递到同名 Actor；
 *   - 成员被 down 后查询结果随之减少；
 *   - 联系点全部不可达时明确失败；空联系点在构造期拒绝。
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
import cactor_cluster.membership.{ClusterClient}
import cactor_testkit.*

class ClientEcho <: Actor {
    let hits: AtomicInt64

    init(hits: AtomicInt64) {
        this.hits = hits
    }

    public prop name: String { get() { "client-echo" } }
    public prop description: String { get() { "client echo" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(_) <- (message as StringMessage)) {
            hits.fetchAdd(1)
        }
        MessageResult.Handled
    }
}

@Test
public class ClusterClientTest {
    @TestCase
    public func resolvesMembersAndRoutesWithoutJoining(): Unit {
        let nodes = Array<CNode>(2, { i => CNode("client-${i}") })
        let seed = nodes[0].addr()
        for (n in nodes) {
            n.join([seed], FAST)
        }
        let hits = AtomicInt64(0)
        let external = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        external.start()
        let client = ClusterClient(external, [nodes[0].addr(), nodes[1].addr()],
            timeout: Duration.millisecond * 800)
        try {
            @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[0].cluster.state().upCount() == 2 }), true)
            for (n in nodes) {
                n.remote.exposeActor("echo", n.kit.actorOf({ => ClientEcho(hits) }, "echo-${n.address().hashCode()}"))
            }

            let members = client.members()
            @Expect(members.size, 2)
            @Expect(members[0] < members[1], true)

            // 轮询投递两次：两个节点各收到一次
            client.tell("echo", StringMessage("hi-1"))
            client.tell("echo", StringMessage("hi-2"))
            @Expect(awaitCondition(CLUSTER_WAIT, { => hits.load() == 2 }), true)

            // 成员被 down 后，查询结果不再包含它
            let downed = nodes[1].address()
            nodes[0].cluster.down(downed)
            @Expect(awaitCondition(CLUSTER_WAIT, { =>
                let m = client.members()
                m.size == 1 && !m.contains(downed)
            }), true)
        } finally {
            external.stop()
            closeAll(nodes)
        }
    }

    @TestCase
    public func unreachableContactsFailExplicitly(): Unit {
        let external = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        external.start()
        try {
            let client = ClusterClient(external, [NetworkAddress("127.0.0.1", 1)],
                timeout: Duration.millisecond * 200)
            var message = ""
            try {
                let _ = client.members()
            } catch (e: IllegalStateException) {
                message = e.message
            }
            @Expect(message.contains("没有可用的集群联系点"), true)
            @ExpectThrows[IllegalArgumentException](
                ClusterClient(external, Array<NetworkAddress>()))
        } finally {
            external.stop()
        }
    }
}
