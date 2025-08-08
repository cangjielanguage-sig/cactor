package cactor.tests.unit.core.typed

import std.unittest.*
import std.unittest.testmacro.*
import std.collection.*
import std.time.*
import cactor.core.typed.{TypedActorRef, TypedActorSelection, SimpleTypedActorSelection}

class TestRef<T> <: TypedActorRef<T> {
    private let p: String
    public var received: Int32 = 0
    public init(p: String) { this.p = p }
    public func tell(message: T): Unit { received += 1 }
    public func pathString(): String { p }
    public func equals(other: TypedActorRef<T>): Bool { p == other.pathString() }
}

@Test
public class SelectionTestSuite {
    @TestCase
    public func testWildcardSingle(): Unit {
        let r0 = TestRef<String>("/user/a/worker-1")
        let r1 = TestRef<String>("/user/a/worker-2")
        let r2 = TestRef<String>("/user/b/worker-1")
        let refs: Array<TypedActorRef<String>> = [r0, r1, r2]
        let sel = SimpleTypedActorSelection<String>("/user/a/*", refs)
        sel.tell("x")
        @Assert(r0.received, 1)
        @Assert(r1.received, 1)
        @Assert(r2.received, 0)
    }

    @TestCase
    public func testWildcardDeep(): Unit {
        let r0 = TestRef<String>("/user/a/x/worker")
        let r1 = TestRef<String>("/user/a/y/z/worker")
        let r2 = TestRef<String>("/user/b/worker")
        let refs: Array<TypedActorRef<String>> = [r0, r1, r2]
        let sel = SimpleTypedActorSelection<String>("/user/a/**", refs)
        sel.tell("x")
        @Assert(r0.received, 1)
        @Assert(r1.received, 1)
        @Assert(r2.received, 0)
    }

    @TestCase
    public func testResolveOne(): Unit {
        let refs: Array<TypedActorRef<String>> = [
            TestRef<String>("/user/a"),
            TestRef<String>("/user/b")
        ]
        let sel = SimpleTypedActorSelection<String>("/user/b", refs)
        match (sel.resolveOne(Duration.second)) {
            case Some(ref) => @Assert(ref.pathString(), "/user/b")
            case None => @Fail("should resolveOne")
        }
    }
}

