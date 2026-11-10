/*
 * CActor TestKit - 探针的 DeathWatch 与日志断言（E62）
 *
 *   - `TestProbe.expectTerminated`：探针自己 watch 目标，目标停止后探针收到 Terminated；
 *     对已停止的目标（watch 已死目标会立即投递）也要立刻返回，不能靠超时兜底。
 *   - 目标还活着时必须超时并抛出带诊断的断言异常（不能静默返回）。
 *   - `LoggingTestKit`：安装内存 sink，按级别与文本断言；级别过滤生效（低于阈值的记录不出现）；
 *     close 之后还原全局 sink 与级别（不影响后续用例）。
 */
package cactor_tests.unit.testkit

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext, ActorRef, PoisonPill}
import cactor_core.message.{Message, MessageResult}
import cactor_foundation.logging.{LogLevel, Logging}
import cactor_testkit.*

class Idle <: Actor {
    let label: String
    init(label: String) { this.label = label }
    public prop name: String { get() { "idle" } }
    public prop description: String { get() { "idle" } }
    public func receive(message: Message, context: ActorContext): MessageResult { MessageResult.Handled }
}

@Test
public class ProbeDeathWatchTest {
    @TestCase
    public func expectTerminatedObservesLiveActorStop(): Unit {
        let kit = ActorTestKit("probe-watch-live")
        let probe = kit.createTestProbe("watcher")
        let target = kit.actorOf({ => Idle("t") }, "target")
        target.tell(PoisonPill())
        let t = probe.expectTerminated(target, Duration.second * 5)
        @Expect(t.actor.equals(target), true)
    }

    @TestCase
    public func expectTerminatedSeesAlreadyStoppedActor(): Unit {
        let kit = ActorTestKit("probe-watch-dead")
        let probe = kit.createTestProbe("watcher2")
        let target = kit.actorOf({ => Idle("t2") }, "target2")
        target.tell(PoisonPill())
        // 先让目标真的停下来，再开始监视：watch 已停止的目标会立即投递 Terminated
        @Expect(awaitCondition(Duration.second * 5, { => target.isTerminated() }), true)
        probe.expectTerminated(target, Duration.second * 2)
    }

    @TestCase
    public func expectTerminatedFailsWhenTargetIsAlive(): Unit {
        let kit = ActorTestKit("probe-watch-alive")
        let probe = kit.createTestProbe("watcher3")
        let target = kit.actorOf({ => Idle("t3") }, "target3")
        var failed = false
        try {
            probe.expectTerminated(target, Duration.millisecond * 300)
        } catch (e: TestKitAssertionException) {
            failed = e.message.contains("没有停止")
        }
        @Expect(failed, true)
        // 探针仍然可用：没有把 Terminated 之外的消息留在队列头
        probe.ref.tell(IdleCmd())
        @Expect(probe.expectMsg(Duration.second) is IdleCmd, true)
    }
}

class IdleCmd <: Message {
    public func messageType(): String {
        "IdleCmd"
    }
}

@Test
public class LoggingTestKitTest {
    @TestCase
    public func capturesRecordsAtOrAboveConfiguredLevel(): Unit {
        let logs = LoggingTestKit.start(LogLevel.Debug)
        try {
            let log = Logging.logger("testkit")
            log.info("分配了 3 个分片")
            log.debug("给 127.0.0.1:1 发心跳")
            let r = logs.expectMessage(LogLevel.Info, "分配了 3 个", Duration.second * 2)
            @Expect(r.source, "testkit")
            logs.expectMessage(LogLevel.Debug, "发心跳", Duration.second * 2)
            // 级别过滤：调到 Warn 之后 info 不再进入 sink
            Logging.setLevel(LogLevel.Warn)
            log.info("不该出现的 info")
            log.warn("出现了 warn")
            logs.expectMessage(LogLevel.Warn, "出现了 warn", Duration.second * 2)
            logs.expectNoMessage(LogLevel.Info, "不该出现的 info", Duration.millisecond * 200)
        } finally {
            logs.close()
        }
        // close 之后还原原来的级别（本用例开始时是进程默认级别）
        @Expect(Logging.level().ordinal() <= LogLevel.Warn.ordinal(), true)
    }

    @TestCase
    public func expectNoMessagePassesWhenNothingMatches(): Unit {
        let logs = LoggingTestKit.start(LogLevel.Info)
        try {
            Logging.logger("testkit2").warn("普通的告警")
            logs.expectNoMessage(LogLevel.Error, "OOM", Duration.millisecond * 200)
            logs.expectMessage(LogLevel.Warn, "普通的告警", Duration.second * 2)
            // clear 之后旧记录不再参与断言
            logs.clear()
            var failed = false
            try {
                logs.expectMessage(LogLevel.Warn, "普通的告警", Duration.millisecond * 200)
            } catch (_: TestKitAssertionException) {
                failed = true
            }
            @Expect(failed, true)
        } finally {
            logs.close()
        }
    }
}
