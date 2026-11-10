/*
 * CActor - 集群运行在 TLS 上（E65）
 *
 * 集群的 gossip、心跳、ddata 与 Receptionist 都经各节点的 SimpleRemoteManager，配置一次 TLS 即全部加密：
 *   - 3 个只开 TLS 的节点组成集群，跨节点发现服务并调用，所有连接都经 TLS 建立
 *   - 不配 TLS 的节点以同一种子加入：连接被拒绝，永远成不了成员
 */
package cactor_tests.unit.cluster

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{StringMessage}
import cactor_cluster.membership.*
import cactor_remote.{RemoteTls, RemotingSettings}
import cactor_testkit.*
import cactor_tests.unit.remote_semantics.{TEST_TLS_CA, TEST_TLS_NODE_CERT, TEST_TLS_NODE_KEY}

@Test
public class ClusterTlsTest {
    @TestCase
    public func clusterFormsAndCallsAcrossNodesOverTls(): Unit {
        let remoting = RemotingSettings.defaults().withTls(RemoteTls.fromPem(TEST_TLS_NODE_CERT, TEST_TLS_NODE_KEY, TEST_TLS_CA))
        let nodes = Array<CNode>(3, { i => CNode("e65-tls-${i}", remoting) })
        let seed = nodes[0].addr()
        for (n in nodes) {
            n.join([seed], FAST)
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let rs = Array<RNode>(3, { i => RNode(nodes[i]) })
        let e1 = nodes[1].kit.actorOf({ => EchoActor("n1") }, "echo")
        let e2 = nodes[2].kit.actorOf({ => EchoActor("n2") }, "echo")
        rs[1].receptionist.register("echo", e1)
        rs[2].receptionist.register("echo", e2)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[0].receptionist.find("echo").size == 2 }), true)
        let rec = EventRecorder()
        let collector = nodes[0].kit.actorOf({ => ReplyCollector("c", rec) }, "collector")
        for (r in rs[0].receptionist.find("echo")) {
            r.tell(StringMessage("hi"), collector)
        }
        @Expect(rec.await("c|n1:hi", CLUSTER_WAIT), true)
        @Expect(rec.await("c|n2:hi", CLUSTER_WAIT), true)
        for (n in nodes) {
            // 与另外两个节点各至少一条出站 TLS 连接
            @Expect(n.remote.securedConnections() >= 2, true)
            @Expect(n.remote.rejectedConnections(), 0)
        }
        // 明文节点以同一种子加入：连接被拒绝，成不了成员
        let intruder = CNode("e65-intruder")
        intruder.join([seed], FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => nodes[0].remote.rejectedConnections() >= 1 }), true)
        @Expect(awaitGossipRounds([nodes[0].cluster, nodes[1].cluster, nodes[2].cluster], 2, CLUSTER_WAIT), true)
        for (n in nodes) {
            @Expect(n.cluster.state().member(intruder.address()).isNone(), true)
            @Expect(n.cluster.state().upCount(), 3)
        }
        intruder.close()
        closeAll(nodes)
    }
}
