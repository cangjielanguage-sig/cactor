/*
 * CActor Platform.Exec - 受监督执行（P1）验证
 *
 * 1. 失败按策略重启并重新执行 work（前两次失败、第三次成功）；
 * 2. 超时以明确异常失败，并置位取消令牌；
 * 3. 一次调用失败不影响其他调用；
 * 4. 大量调用后不泄漏 Actor（协调者与工作者都停止）。
 */
package cactor_tests.unit.platform.exec

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.{TypedSupervisorStrategy}
import cactor_platform.exec.*
import cactor_testkit.*

@Test
public class SupervisedCallTest {
    @TestCase
    public func supervisedCallRestartsOnFailure(): Unit {
        let system = CActorSystemFactory.create("supervised-restart")
        try {
            let exec = SupervisedCall<String>(system.typed(), timeout: Duration.second * 10)
            let attempts = AtomicInt64(0)
            let promise = exec.submit("flaky", { _ =>
                let n = attempts.fetchAdd(1) + 1
                if (n < 3) {
                    throw Exception("第 ${n} 次失败")
                }
                "ok-${n}"
            })
            @Expect(promise.get(Duration.second * 10), "ok-3")
            @Expect(attempts.load(), 3)
        } finally {
            system.terminate()
        }
    }

    @TestCase
    public func supervisedCallTimesOutAndCancels(): Unit {
        let system = CActorSystemFactory.create("supervised-timeout")
        try {
            let exec = SupervisedCall<String>(system.typed(),
                timeout: Duration.millisecond * 200,
                isolation: IsolationLevel.ThreadPerTask)
            let observedCancel = AtomicBool(false)
            let promise = exec.submit("slow", { token =>
                while (!token.isCancellationRequested()) {
                    sleep(Duration.millisecond * 5)
                }
                observedCancel.store(true)
                "cancelled"
            })
            var timedOut = false
            try {
                let _ = promise.get(Duration.second * 5)
            } catch (e: SupervisedCallTimeoutException) {
                timedOut = true
            }
            @Expect(timedOut, true)
            // 取消是协作式的：令牌置位后工作函数应尽快退出
            @Expect(awaitCondition(Duration.second * 5, { => observedCancel.load() }), true)
        } finally {
            system.terminate()
        }
    }

    @TestCase
    public func supervisedCallIsolatesFailure(): Unit {
        let system = CActorSystemFactory.create("supervised-isolate")
        try {
            // 只重启 1 次，失败调用尽快结束
            let policy = TypedSupervisorStrategy.restart().withLimit(1, Duration.second * 10)
            let exec = SupervisedCall<String>(system.typed(), policy: policy, timeout: Duration.second * 10)
            let bad = exec.submit("bad", { _ => throw Exception("boom") })
            let good = exec.submit("good", { _ => "fine" })
            @Expect(good.get(Duration.second * 10), "fine")
            var failed = false
            try {
                let _ = bad.get(Duration.second * 10)
            } catch (e: SupervisedCallFailedException) {
                failed = true
            }
            @Expect(failed, true)
        } finally {
            system.terminate()
        }
    }

    @TestCase
    public func supervisedCallDoesNotLeakActors(): Unit {
        let system = CActorSystemFactory.create("supervised-leak")
        try {
            let baseline = system.actorBudgetUsage().liveActors
            let exec = SupervisedCall<Int64>(system.typed(), timeout: Duration.second * 10)
            let n: Int64 = 1000
            var ok = true
            for (i in 0..n) {
                let v = exec.submit("call-${i}", { _ => i }).get(Duration.second * 10)
                if (v != i) {
                    ok = false
                }
            }
            @Expect(ok, true)
            @Expect(awaitCondition(Duration.second * 20, { => system.actorBudgetUsage().liveActors == baseline }), true)
        } finally {
            system.terminate()
        }
    }
}
