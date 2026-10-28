package cactor_tests.unit.runtime.typed

import std.unittest.*
import std.unittest.testmacro.*
import cactor_runtime.typed.{StashBuffer}
import cactor_core.typed.{TypedActorRef}

class DummyRef<T> <: TypedActorRef<T> {
    public var received: Int32 = 0
    public var last: Option<T> = None
    public func tell(message: T): Unit { received += 1; last = Some(message) }
    public func pathString(): String { "dummy" }
    public func equals(other: TypedActorRef<T>): Bool { true }
}

@Test
public class StashBufferTestSuite {
    @TestCase
    public func testStashAndDrain(): Unit {
        let stash = StashBuffer<Int32>(4)
        @Assert(stash.isEmpty(), true)
        @Assert(stash.stash(1), true)
        @Assert(stash.stash(2), true)
        @Assert(stash.size(), 2)
        let ref = DummyRef<Int32>()
        stash.drainTo(ref)
        @Assert(ref.received, 2)
        @Assert(stash.isEmpty(), true)
    }

    @TestCase
    public func testCapacity(): Unit {
        let stash = StashBuffer<Int32>(2)
        @Assert(stash.stash(1), true)
        @Assert(stash.stash(2), true)
        @Assert(stash.stash(3), false)
    }
}

