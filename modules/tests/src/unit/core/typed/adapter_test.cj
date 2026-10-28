package cactor_tests.unit.core.typed

import std.unittest.*
import std.unittest.testmacro.*
import std.collection.*
import cactor_core.typed.{TypedAdapters, TypedActorRef}

class CaptureRef<T> <: TypedActorRef<T> {
    public let captured: ArrayList<T> = ArrayList<T>()
    public func tell(message: T): Unit { captured.add(message) }
    public func pathString(): String { "capture" }
    public func equals(other: TypedActorRef<T>): Bool { pathString() == other.pathString() }
}

@Test
public class AdapterTestSuite {
    @TestCase
    public func testMessageAdapter(): Unit {
        let target = CaptureRef<Int32>()
        let adapter = TypedAdapters.messageAdapter<String, Int32>(target, { s => Int32(1) })
        adapter.tell("abc")
        adapter.tell("hello")
        @Assert(target.captured.size, 2)
    }
}

