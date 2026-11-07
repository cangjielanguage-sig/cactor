/*
 * CActor - 黑洞节点不拖累健康成员（E40，N1）
 *
 * 伪造的心跳 "HB|10.255.255.1:1" 让成员守护 Actor 向黑洞地址回 HBR。
 * 此前发送在守护 Actor 的线程上同步 connect（3 s 超时，失败再试一次），守护 Actor 每次卡 6 s，
 * 心跳停发，健康成员互相判为不可达；E40 起发送只入队，连接在出站关联的写线程里进行。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*
import cactor_cluster.ddata.{FieldWriter}
import cactor_testkit.*

@Test
public class TransportIsolationTest {
    @TestCase
    public func blackHoledPeerDoesNotStallHealthyMembers(): Unit {
        let nodes = startCluster(2, "e40-isolation", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let a = nodes[0].address()
        let b = nodes[1].address()
        // 伪造方：一个独立的远程节点，持续向 A 发送"来自黑洞地址"的心跳；B 不受干扰，
        // 若 A 的守护 Actor 被卡住，B 会把 A 判为不可达
        let forger = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        forger.start()
        let forged = StringMessage(FieldWriter().put("HB").put("10.255.255.1:1").done())
        let deadline = MonoTime.now() + Duration.second * 3
        var healthy = true
        while (MonoTime.now() < deadline) {
            forger.remoteActorOf(nodes[0].addr(), "cluster").tell(forged)
            // 心跳容忍 300 ms：守护 Actor 卡住一次就会被判不可达
            if (!nodes[0].cluster.state().unreachable.isEmpty() || !nodes[1].cluster.state().unreachable.isEmpty()) {
                healthy = false
            }
            sleep(Duration.millisecond * 50)
        }
        @Expect(healthy, true)
        @Expect(nodes[0].events.contains("UnreachableMember(${b})"), false)
        @Expect(nodes[1].events.contains("UnreachableMember(${a})"), false)
        // 守护 Actor 仍在正常工作：gossip 继续增长
        let before = nodes[0].cluster.gossipCount()
        @Expect(awaitCondition(Duration.second * 2, { => nodes[0].cluster.gossipCount() > before }), true)
        forger.stop()
        closeAll(nodes)
    }
}
