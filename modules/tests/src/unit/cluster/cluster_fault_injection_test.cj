/*
 * CActor - 故障注入端点的单进程校验（E62 跨进程注入的端点部分）
 *
 * `tools/cluster_chaos.sh` 验证真进程之间的半分区；这里验证端点本身的语义，避免每次调脚本才能发现拼写错误：
 *   - `/cluster/faults` 报出被黑洞的地址与丢帧数；
 *   - `/cluster/blackhole?addr=` / `/cluster/heal?addr=` / `/cluster/heal` 真的改变传输状态；
 *   - 缺参数只给提示、不抛异常（端点不能因为运维打错命令就回 500）。
 * 传输不启动、也不发帧：这里只测"开关"，实际丢帧由 `ClusterBlackholeTest`（单进程）与 chaos 脚本（跨进程）覆盖。
 */
package cactor_tests.unit.cluster

import std.net.*
import std.time.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{ManagementServer}
import cactor_foundation.network.{BlackholeTransport, NetworkAddress, TcpTransport}
import cactor_cluster.ops.*
import cactor_testkit.*

@Test
public class ClusterFaultInjectionTest {
    @TestCase
    public func faultRoutesToggleTransportAndReportState(): Unit {
        let target = "127.0.0.1:7002"
        let transport = BlackholeTransport(TcpTransport(NetworkAddress("127.0.0.1", 0)))
        let kit = ActorTestKit("fault-routes")
        let mgmt = ManagementServer(kit.system, "127.0.0.1", 0)
        attachFaultInjectionRoutes(mgmt, transport)
        mgmt.start()
        let port = mgmt.port()
        @Expect(port > 0, true)

        // 初始状态：没有黑洞
        let (st0, f0) = cmHttp(port, "/cluster/faults")
        @Expect(st0, 200)
        @Expect(f0.contains("\"blackholed\":[]"), true)
        @Expect(f0.contains("\"droppedCount\":0"), true)

        // 黑洞一个地址：传输状态立刻变化，端点报出来
        let (st1, b1) = cmHttp(port, "/cluster/blackhole?addr=${target}")
        @Expect(st1, 200)
        @Expect(b1.contains("已黑洞"), true)
        @Expect(transport.isBlackholed(target), true)
        let (_, f1) = cmHttp(port, "/cluster/faults")
        @Expect(f1.contains("\"${target}\""), true)

        // 缺参数：只给用法提示，仍然 200（运维打错命令不该惊动告警）
        let (st2, b2) = cmHttp(port, "/cluster/blackhole")
        @Expect(st2, 200)
        @Expect(b2.contains("addr=host:port"), true)

        // 恢复指定地址
        let (st3, _) = cmHttp(port, "/cluster/heal?addr=${target}")
        @Expect(st3, 200)
        @Expect(transport.isBlackholed(target), false)

        // 恢复全部：第一个用 addr=* 清掉已注入项，第二个不带参数清空
        transport.blackhole(target)
        transport.blackhole("127.0.0.1:7003")
        let (st4, _) = cmHttp(port, "/cluster/heal?addr=*")
        @Expect(st4, 200)
        @Expect(transport.blackholedAddresses().size, 2)   // addr=* 只清已注入项，不新注入
        let (st5, _) = cmHttp(port, "/cluster/heal")
        @Expect(st5, 200)
        @Expect(transport.blackholedAddresses().size, 0)

        // 进程内注入开关与真实丢帧计数配合（BlackholeTransport 的 dropped 只在 send 时增加）
        @Expect(transport.droppedCount(), 0)
        mgmt.stop()
        kit.shutdown()
    }
}
