/*
 * CActor - 集群视图管理端点（E66）
 *
 * docs/TROUBLESHOOTING.md 让我们 `curl /cluster/status` 与 `/cluster/shards`，但管理端口以前只有
 * 健康检查与指标，这两个端点根本不存在。本用例验证：
 *   - 真实两节点集群收敛后 `/cluster/status` 报出两个成员、领导者、收敛状态与可达性；
 *   - `/cluster/shards` 报出分片数、托管数与负载，字段随真实分片状态变化；
 *   - 自定义路由的错误路径：重复注册、非绝对路径、处理器抛异常 → 500 且进程继续服务。
 */
package cactor_tests.unit.cluster

import std.collection.*
import std.net.*
import std.time.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{ManagementServer}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_cluster.membership.*
import cactor_cluster.ops.*
import cactor_testkit.*

let CM_FAST = ClusterSettings(Duration.millisecond * 50, Duration.millisecond * 50, 8.0, Duration.millisecond * 300, None<Duration>)
let CM_WAIT = Duration.second * 15

@Test
public class ClusterManagementTest {
    /** 两节点收敛后，/cluster/status 与 /cluster/shards 反映真实视图 */
    @TestCase
    public func clusterEndpointsReflectRealMembershipAndShards(): Unit {
        let a = CNode("cm-a")
        let b = CNode("cm-b")
        a.join([a.addr()], CM_FAST)
        b.join([a.addr()], CM_FAST)
        @Expect(awaitCondition(CM_WAIT, { =>
            a.cluster.state().converged && a.cluster.state().upCount() == 2
        }), true)

        // 两个节点都要跑同类型的区域：许可式两阶段移交要求每个可能托管者确认，否则分片永远处于获取中
        let carts = ClusterSharding(a.cluster, a.kit.system, a.remote, "cm-cart", 8, { id => CmEntity(id) })
        let cartsB = ClusterSharding(b.cluster, b.kit.system, b.remote, "cm-cart", 8, { id => CmEntity(id) })
        @Expect(awaitAllAcquired([carts, cartsB], 8, CM_WAIT), true)
        let mgmt = ManagementServer(a.kit.system, "127.0.0.1", 0)
        attachClusterRoutes(mgmt, a.cluster, sharding: Some(carts))
        mgmt.start()
        let port = mgmt.port()
        @Expect(port > 0, true)

        // 状态端点：两个成员都在，自己是 self，领导者可解析，收敛为真
        let (st, body) = cmHttp(port, "/cluster/status")
        @Expect(st, 200)
        @Expect(body.contains("\"self\":\"${a.address()}\""), true)
        @Expect(body.contains("\"upCount\":2"), true)
        @Expect(body.contains("\"converged\":true"), true)
        @Expect(body.contains("\"address\":\"${b.address()}\""), true)
        @Expect(body.contains("\"reachable\":true"), true)
        @Expect(body.contains("\"leader\":\""), true)
        @Expect(body.contains("\"sbrDowned\":false"), true)

        // 分片端点：字段与真实状态一致（分片数、托管数）
        let (ss, sbody) = cmHttp(port, "/cluster/shards")
        @Expect(ss, 200)
        @Expect(sbody.contains("\"type\":\"cm-cart\""), true)
        @Expect(sbody.contains("\"shards\":8"), true)
        @Expect(sbody.contains("\"hosted\":"), true)
        @Expect(sbody.contains("\"ownerCount\":2"), true)
        @Expect(jsonInt(sbody, "hosted") >= 0, true)

        // 发消息后字段要跟着真实状态变（不是写死的常量）：a 要么自己创建实体，要么转发给 b
        for (i in 0..30) {
            carts.tell("cm-e-${i}", StringMessage("hi"))
        }
        @Expect(awaitCondition(CM_WAIT, { => carts.localEntityCount() + cartsB.localEntityCount() > 0 }), true)
        @Expect(awaitCondition(CM_WAIT, { =>
            let (_, body2) = cmHttp(port, "/cluster/shards")
            jsonInt(body2, "entities") + jsonInt(body2, "forwarded") + jsonInt(body2, "delivered") > 0
        }), true)

        // 未注册路径仍是 404；查询串被剥离
        @Expect(cmHttp(port, "/cluster/nope")[0], 404)
        @Expect(cmHttp(port, "/cluster/status?verbose=1")[0], 200)

        mgmt.stop()
        carts.stop()
        cartsB.stop()
        a.cluster.leave()
        b.cluster.leave()
    }

    /** 路由注册的错误路径：重复、非绝对路径、处理器抛异常 */
    @TestCase
    public func routeRegistrationIsValidatedAndHandlerFailureIsContained(): Unit {
        let kit = ActorTestKit("cm-routes")
        let mgmt = ManagementServer(kit.system, "127.0.0.1", 0)
        mgmt.addRoute("/custom/ok", "text/plain", { => "ok" })
        mgmt.addRoute("/custom/boom", "text/plain", { => throw Exception("boom") })
        // @ExpectThrows 的宏参数必须是调用表达式，这里直接断言抛出的异常类型
        var dupRejected = false
        try {
            mgmt.addRoute("/custom/ok", "text/plain", { => "dup" })
        } catch (_: IllegalArgumentException) {
            dupRejected = true
        }
        @Expect(dupRejected, true)
        var relativeRejected = false
        try {
            mgmt.addRoute("custom/relative", "text/plain", { => "" })
        } catch (_: IllegalArgumentException) {
            relativeRejected = true
        }
        @Expect(relativeRejected, true)
        mgmt.start()
        let port = mgmt.port()
        @Expect(cmHttp(port, "/custom/ok")[0], 200)
        @Expect(cmHttp(port, "/custom/ok")[1], "ok")
        @Expect(cmHttp(port, "/custom/boom")[0], 500)
        @Expect(mgmt.handlerErrors(), 1)
        // 处理器失败不影响后续请求
        @Expect(cmHttp(port, "/custom/ok")[0], 200)
        @Expect(cmHttp(port, "/health/live")[0], 200)
        mgmt.stop()
    }
}

class CmEntity <: Actor {
    let id: String
    init(id: String) { this.id = id }
    public prop name: String { get() { "cm" } }
    public prop description: String { get() { "cm" } }
    public func receive(message: Message, context: ActorContext): MessageResult { MessageResult.Handled }
}

func cmHttp(port: UInt16, path: String): (Int64, String) {
    let s = TcpSocket("127.0.0.1", port)
    s.connect(timeout: Duration.second * 3)
    s.readTimeout = Duration.second * 5
    s.write("GET ${path} HTTP/1.1\r\nHost: localhost\r\n\r\n".toArray())
    let acc = ArrayList<UInt8>()
    let buf = Array<UInt8>(4096, repeat: 0)
    while (true) {
        let n = try {
            s.read(buf)
        } catch (_: Exception) {
            0
        }
        if (n <= 0) {
            break
        }
        acc.add(all: buf[0..n])
    }
    s.close()
    let text = String.fromUtf8(acc.toArray())
    let status = Int64.parse(text[9..12])
    let body = match (text.indexOf("\r\n\r\n")) {
        case Some(i) => text[(i + 4)..]
        case None => ""
    }
    (status, body)
}

/** 从扁平 JSON 里取一个整数字段的第一个值（这些端点格式固定，不值得引入解析器） */
func jsonInt(body: String, key: String): Int64 {
    let needle = "\"${key}\":"
    match (body.indexOf(needle)) {
        case Some(i) =>
            var j = i + needle.size
            var digits = ""
            while (j < body.size) {
                let ch = Int64(body[j])
                if (ch >= 48 && ch <= 57) {
                    digits += ch.toString()
                    j++
                } else {
                    break
                }
            }
            if (digits.isEmpty()) {
                -1
            } else {
                Int64.parse(digits)
            }
        case None => -1
    }
}
