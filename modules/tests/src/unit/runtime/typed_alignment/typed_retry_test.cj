/* CActor Core.Typed - 异步重试：虚拟时间验证退避、取消与异常传播。 */
package cactor_tests.unit.runtime.typed_alignment

import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.*
import cactor_core.supervision.{BackoffPolicy}
import cactor_testkit.*

func retryPolicy(): BackoffPolicy {
    BackoffPolicy(Duration.millisecond * 10, Duration.millisecond * 25, randomFactor: 0.0)
}

@Test
public class TypedRetryTest {
    @TestCase
    public func exponentialDelayCapsAndStopsOnSuccess(): Unit {
        let time = ManualTime()
        let attempts = ArrayList<Int64>()
        let p = TypedRetry.run<Int64>({ n =>
            attempts.add(n)
            if (n == 4) { TypedPromises.successful<Int64>(42) }
            else { TypedPromises.failed<Int64>(IllegalStateException("transient")) }
        }, 5, retryPolicy(), schedule: { d, a => time.schedule(d, a) })
        @Expect(attempts.size, 1)
        @Expect(time.pendingCount(), 1)
        time.timePasses(Duration.millisecond * 9)
        @Expect(attempts.size, 1)
        time.timePasses(Duration.millisecond)
        @Expect(attempts.size, 2)
        time.timePasses(Duration.millisecond * 19)
        @Expect(attempts.size, 2)
        time.timePasses(Duration.millisecond)
        @Expect(attempts.size, 3)
        time.timePasses(Duration.millisecond * 24)
        @Expect(attempts.size, 3)
        time.timePasses(Duration.millisecond)
        @Expect(p.get(Duration.second), 42)
        @Expect(attempts[3], 4)
        @Expect(time.pendingCount(), 0)
    }

    @TestCase
    public func limitIncludesFirstAttemptAndPreservesLastFailure(): Unit {
        let time = ManualTime()
        let p = TypedRetry.run<Int64>({ n => throw IllegalStateException("attempt-${n}") },
            3, retryPolicy(), schedule: { d, a => time.schedule(d, a) })
        time.timePasses(Duration.second)
        @Expect(p.outcome().getOrThrow().failure().getOrThrow().message, "attempt-3")
        @Expect(time.pendingCount(), 0)
        @ExpectThrows[IllegalArgumentException](TypedRetry.run<Int64>({ _ => TypedPromises.successful<Int64>(1) }, 0, retryPolicy()))
    }

    @TestCase
    public func cancellationRemovesBackoffAndPreCancellationSkipsOperation(): Unit {
        let time = ManualTime()
        let token = AskCancellationToken()
        let calls = ArrayList<Int64>()
        let operation = { n: Int64 =>
            calls.add(n)
            TypedPromises.failed<Int64>(IllegalStateException("retry"))
        }
        let p = TypedRetry.run<Int64>(operation, 5, retryPolicy(), cancellation: Some(token),
            schedule: { d, a => time.schedule(d, a) })
        @Expect(time.pendingCount(), 1)
        token.cancel()
        @Expect(wasCancelled(p), true)
        @Expect(time.pendingCount(), 0)
        time.timePasses(Duration.second)
        let skipped = TypedRetry.run<Int64>(operation, 5, retryPolicy(), cancellation: Some(token))
        @Expect(wasCancelled(skipped), true)
        @Expect(calls.size, 1)
    }

    @TestCase
    public func inFlightOperationNeverOverlapsAndLateCompletionIsIgnored(): Unit {
        let time = ManualTime()
        let token = AskCancellationToken()
        let first = TypedPromise<Int64>()
        let calls = ArrayList<Int64>()
        let p = TypedRetry.run<Int64>({ n => calls.add(n); first }, 5, retryPolicy(),
            cancellation: Some(token), schedule: { d, a => time.schedule(d, a) })
        time.timePasses(Duration.second)
        @Expect(calls.size, 1)
        @Expect(time.pendingCount(), 0)
        token.cancel()
        first.trySuccess(99)
        @Expect(wasCancelled(p), true)
        @Expect(time.pendingCount(), 0)
    }

    @TestCase
    public func failureFilterAndCancellationAreTerminal(): Unit {
        let time = ManualTime()
        let rejected = TypedRetry.run<Int64>({ _ => TypedPromises.failed<Int64>(IllegalArgumentException("bad")) },
            5, retryPolicy(), shouldRetry: { _ => false }, schedule: { d, a => time.schedule(d, a) })
        @Expect(rejected.outcome().getOrThrow().failure().getOrThrow().message, "bad")
        let cancelled = TypedRetry.run<Int64>({ _ => TypedPromises.failed<Int64>(TypedAskCancelledException("cancel")) },
            5, retryPolicy(), schedule: { d, a => time.schedule(d, a) })
        @Expect(wasCancelled(cancelled), true)
        let brokenFilter = TypedRetry.run<Int64>({ _ => TypedPromises.failed<Int64>(IllegalStateException("first")) },
            5, retryPolicy(), shouldRetry: { _ => throw IllegalArgumentException("filter") })
        @Expect(brokenFilter.outcome().getOrThrow().failure().getOrThrow().message, "filter")
        @Expect(time.pendingCount(), 0)
    }

    @TestCase
    public func schedulingFailureCompletesAndInlineSchedulingIsSafe(): Unit {
        let failed = TypedRetry.run<Int64>({ _ => TypedPromises.failed<Int64>(IllegalStateException("first")) },
            2, retryPolicy(), schedule: { _, _ => throw IllegalStateException("scheduler") })
        @Expect(failed.outcome().getOrThrow().failure().getOrThrow().message, "scheduler")
        let time = ManualTime()
        let p = TypedRetry.run<Int64>({ n =>
            if (n == 2) { TypedPromises.successful<Int64>(7) }
            else { TypedPromises.failed<Int64>(IllegalStateException("first")) }
        }, 2, retryPolicy(), schedule: { d, a =>
            let task = time.schedule(d, a)
            time.timePasses(d)
            task
        })
        @Expect(p.get(Duration.second), 7)
        @Expect(time.pendingCount(), 0)
    }

    @TestCase
    public func realTimerRetriesWithoutBlockingCaller(): Unit {
        let p = TypedRetry.run<Int64>({ n =>
            if (n == 2) { TypedPromises.successful<Int64>(9) }
            else { TypedPromises.failed<Int64>(IllegalStateException("first")) }
        }, 2, retryPolicy())
        @Expect(p.get(Duration.second * 5), 9)
    }
}
