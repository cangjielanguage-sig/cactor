/*
 * CActor - 分片获取的卡死可见化（E13′ 观测补充）
 *
 * 获取是许可式两阶段移交：本方要等每个可能托管者回复"已停止"，否则两个节点可能同时托管同一分片。
 * 所以这里**不加超时强推**（那会制造双主），而是把"卡多久、在等谁"变成数据：
 *   - ShardingSettings.acquireWarnAfter 控制告警阈值（默认 10 s，可调到毫秒级做测试）；
 *   - AcquisitionHealth 暴露 slow / oldestMs / pendingMembers / slowWarnings；
 *   - 每过一个阈值窗口再告警一次，长时间故障不会被一条旧 warn 淹没。
 *
 * 本用例：一个节点跑区域、另一个不跑（最典型的运维事故），断言
 *   1) 分片一直"获取中"、本节点不托管（不会因为等久了就自己当主）；
 *   2) health 报出卡住的分片数、已等待时长与"在等谁"（另一个节点地址）；
 *   3) 告警会重复（slowWarnings 继续增长）；
 *   4) 另一个节点起区域后自动收敛：acquiring = 0、hosted = 分片数、slow 归零。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_cluster.membership.*
import cactor_testkit.*

let ACQ_FAST = ClusterSettings(Duration.millisecond * 50, Duration.millisecond * 50, 8.0, Duration.millisecond * 300,
    None<Duration>)
let ACQ_WAIT = Duration.second * 15
let ACQ_WARN_AFTER = Duration.millisecond * 300
// 分片数取 64 而不是 8：该用例的前提是"a 确实托管了至少一个分片"。
// 两个节点做 rendezvous 哈希时，8 个分片全落到 b 的概率是 1/256，偶发时 a 没有可获取的分片，
// 用例会在"acquiring > 0"上失败（与产品行为无关）。64 个分片把这一概率降到可忽略。
let ACQ_SHARDS = 64

class AcqEntity <: Actor {
    let id: String
    init(id: String) { this.id = id }
    public prop name: String { get() { "acq-entity" } }
    public prop description: String { get() { "acq-entity" } }
    public func receive(message: Message, context: ActorContext): MessageResult { MessageResult.Handled }
}

@Test
public class ShardingAcquisitionHealthTest {
    @TestCase
    public func pendingAcquisitionBecomesVisibleAndResolvesWhenMissingRegionStarts(): Unit {
        let a = CNode("acq-a")
        let b = CNode("acq-b")
        a.join([a.addr()], ACQ_FAST)
        b.join([a.addr()], ACQ_FAST)
        @Expect(awaitCondition(ACQ_WAIT, { =>
            a.cluster.state().converged && a.cluster.state().upCount() == 2
        }), true)

        // 只有 a 跑区域：b 是 Up 成员但不会确认，获取会一直等下去
        let settings = ShardingSettings.defaults().withAcquireWarnAfter(ACQ_WARN_AFTER)
        let region = ClusterSharding(a.cluster, a.kit.system, a.remote, "acq-cart", ACQ_SHARDS,
            { id => AcqEntity(id) }, settings)
        region.tell("warm", StringMessage("hi"))

        // 1) 等久了也不会自己当主：托管数为 0，获取中的分片数大于 0
        @Expect(awaitCondition(ACQ_WAIT, { =>
            let h = region.acquisitionHealth()
            h.acquiring > 0 && h.hosted == 0
        }), true)

        // 2) 超过阈值后能看到卡住的分片数、时长与在等谁
        @Expect(awaitCondition(ACQ_WAIT, { =>
            let h = region.acquisitionHealth()
            h.slow > 0 && h.oldestMs >= ACQ_WARN_AFTER.toMilliseconds() - 50 && h.pendingMembers.contains(b.address())
        }), true)
        let h1 = region.acquisitionHealth()
        @Expect(h1.hosted, 0)
        @Expect(h1.acquiring > 0, true)

        // 3) 告警会重复：再等两个窗口，累计告警次数必须继续增长
        let warnings1 = h1.slowWarnings
        @Expect(awaitCondition(ACQ_WAIT, { =>
            region.acquisitionHealth().slowWarnings > warnings1
        }), true)

        // 4) b 起同类型区域后自动收敛：不再有慢获取，分片全部托管
        let regionB = ClusterSharding(b.cluster, b.kit.system, b.remote, "acq-cart", ACQ_SHARDS,
            { id => AcqEntity(id) }, settings)
        @Expect(awaitCondition(ACQ_WAIT, { =>
            let h = region.acquisitionHealth()
            h.acquiring == 0 && h.slow == 0 && h.pendingMembers.size == 0
        }), true)
        @Expect(awaitAllAcquired([region, regionB], ACQ_SHARDS, ACQ_WAIT), true)
        @Expect(region.acquisitionHealth().hosted + regionB.acquisitionHealth().hosted, ACQ_SHARDS)
    }
}
