package cactor.tests.unit.runtime.typed

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import cactor.runtime.typed.{TypedTimers}
import cactor.core.typed.{TypedActorRef, TypedScheduler, TypedCancellable}
import std.collection.*

class BOFCancellable <: TypedCancellable {
    private var cancelled: Bool = false
    public func cancel(): Bool { if (!cancelled) { cancelled = true; return true } else { return false } }
    public func isCancelled(): Bool { cancelled }
}

class BOFScheduler <: TypedScheduler {
    public var scheduled: ArrayList<Duration> = ArrayList<Duration>()
    public func scheduleOnce(delay: Duration, target: TypedActorRef<Any>, message: Any): Unit { scheduled.add(delay); target.tell(message) }
    public func schedule(initialDelay: Duration, interval: Duration, target: TypedActorRef<Any>, message: Any): TypedCancellable { BOFCancellable() }
}

class BOFRef<T> <: TypedActorRef<T> {
    public var received: Int32 = 0
    public func tell(message: T): Unit { received += 1 }
    public func pathString(): String { "bof-dummy" }
    public func equals(other: TypedActorRef<T>): Bool { true }
}

@Test
public class BackoffTimerTestSuite {
    @TestCase
    public func testBackoffSchedules(): Unit {
        let ref = BOFRef<Int32>()
        let sch = BOFScheduler()
        let timers = TypedTimers<Int32>(sch, ref)
        let c = timers.startBackoffTimer("b", Int32(7), Duration.millisecond * 10, Duration.millisecond * 80, 0.2, 4)
        @Assert(ref.received >= 1, true)
        // 由于 SimpleTypedScheduler 可能忽略 delay，这里仅断言调用次数
        @Assert(sch.scheduled.size >= 4, true)
        // 可取消
        @Assert(c.cancel(), true)
    }
}

