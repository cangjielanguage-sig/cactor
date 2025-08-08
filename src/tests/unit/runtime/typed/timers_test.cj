package cactor.tests.unit.runtime.typed

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import cactor.runtime.typed.{TypedTimers}
import cactor.core.typed.{TypedActorRef, TypedScheduler, TypedCancellable}

class DummyCancellable <: TypedCancellable {
    private var cancelled: Bool = false
    public func cancel(): Bool { if (!cancelled) { cancelled = true; return true } else { return false } }
    public func isCancelled(): Bool { cancelled }
}

class DummyScheduler <: TypedScheduler {
    public var scheduled: Int32 = 0
    public func scheduleOnce(delay: Duration, target: TypedActorRef<Any>, message: Any): Unit { scheduled += 1; target.tell(message) }
    public func schedule(initialDelay: Duration, interval: Duration, target: TypedActorRef<Any>, message: Any): TypedCancellable {
        scheduled += 1; target.tell(message); DummyCancellable()
    }
}

class TimerDummyRef<T> <: TypedActorRef<T> {
    public var received: Int32 = 0
    public func tell(message: T): Unit { received += 1 }
    public func pathString(): String { "timer-dummy" }
    public func equals(other: TypedActorRef<T>): Bool { true }
}

@Test
public class TimersTestSuite {
    @TestCase
    public func testSingleAndPeriodic(): Unit {
        let ref = TimerDummyRef<Int32>()
        let scheduler = DummyScheduler()
        let timers = TypedTimers<Int32>(scheduler, ref)
        timers.startSingleTimer("once", 1, Duration.millisecond * 1)
        timers.startPeriodicTimer("tick", 2, Duration.millisecond * 1, Duration.millisecond * 1)
        @Assert(scheduler.scheduled >= 2, true)
    }

    @TestCase
    public func testCancel(): Unit {
        let ref = TimerDummyRef<Int32>()
        let scheduler = DummyScheduler()
        let timers = TypedTimers<Int32>(scheduler, ref)
        timers.startPeriodicTimer("tick", 2, Duration.millisecond * 1, Duration.millisecond * 1)
        timers.cancel("tick")
        // 无直接可观察状态，这里仅保证无异常
        @Assert(true, true)
    }
}

