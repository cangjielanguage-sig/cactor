/*
 * CActor - C5 邮箱积压告警（只告警，不丢弃）
 *
 * 慢消费者 + 低阈值：积压超过阈值时 `cactor_actor_backlog_warnings_total` 增长，
 * 但全部消息仍然处理完（零丢弃）；同时断言 /metrics 文本里有该计数。
 */
package cactor_tests.unit.runtime.backpressure

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory}
import cactor_core.message.{StringMessage}
import cactor_runtime.{DispatcherDiagnostics}
import cactor_testkit.*

@Test
public class BacklogWarningTest {
    @TestCase
    public func backlogBeyondThresholdWarnsWithoutDropping(): Unit {
        // 50ms 阈值：每批最多 64 条、每条 5ms，第一批结束时最老待处理消息已等约 300ms
        DispatcherDiagnostics.setBacklogWarnMillis(50)
        let system = CActorSystemFactory.create("backlog-warn-${MonoTime.now().hashCode()}")
        let gate = AtomicBool(false)
        let count = AtomicInt64(0)
        let before = DispatcherDiagnostics.backlogWarnings()
        try {
            let ref = system.actorOf({ => Gated(gate, Duration.millisecond * 5, count) }, "slow-backlog")
            let total: Int64 = 120
            for (_ in 0..total) {
                ref.tell(StringMessage("m"))
            }
            // 打开 gate：开始消费，但积压远高于阈值
            gate.store(true)
            @Expect(awaitCondition(Duration.second * 10, { =>
                DispatcherDiagnostics.backlogWarnings() > before
            }), true)
            // 只告警不丢弃：全部消息处理完
            @Expect(awaitCondition(Duration.second * 15, { => count.load() == total }), true)
            @Expect(system.metricsText(false).contains("cactor_actor_backlog_warnings_total"), true)
        } finally {
            gate.store(true)
            DispatcherDiagnostics.resetBacklogWarnThreshold()
            system.terminate()
        }
    }
}
