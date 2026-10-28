package cactor_tests.unit.core.typed

import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.{TypedBehaviors, TypedActorContext, TypedBehavior, TypedActorRef, TypedLogger, TypedActorSystem, TypedScheduler, TypedFuture}
import std.time.*

@Test
public class BehaviorMapContramapTestSuite {
    @TestCase
    public func testMapMessagesAlias(): Unit {
        let inner = TypedBehaviors.receive<Int32>({ ctx, x => TypedBehaviors.same<Int32>() })
        let mapped = TypedBehaviors.mapMessages<String, Int32>(inner, { s => 42 })
        let next = mapped.onMessage("any", MapNullTypedContext<String>())
        @Assert(next.getDescription(), "TransformBehavior")
    }

    @TestCase
    public func testContramapAlias(): Unit {
        let inner = TypedBehaviors.receive<Int32>({ ctx, x => TypedBehaviors.same<Int32>() })
        let mapped = TypedBehaviors.contramap<String, Int32>(inner, { s => 1 })
        let next = mapped.onMessage("x", MapNullTypedContext<String>())
        @Assert(next.getDescription(), "TransformBehavior")
    }
}

class MapNullTypedContext<T> <: TypedActorContext<T> {
    public prop self: TypedActorRef<T> { get() { throw Exception("not used") } }
    public func sender(): Option<String> { None }
    public func stopSelf(): Unit { () }
    public func log(): TypedLogger { throw Exception("not used") }
    public func spawnChild(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any> { throw Exception("not used") }
    public func spawnAnonymousChild(behavior: TypedBehavior<Any>): TypedActorRef<Any> { throw Exception("not used") }
    public func stopChild(child: TypedActorRef<Any>): Unit { () }
    public func watchActor(other: TypedActorRef<Any>): Unit { () }
    public func unwatchActor(other: TypedActorRef<Any>): Unit { () }
    public func askMessage(target: TypedActorRef<Any>, message: Any, timeout: Duration): TypedFuture<Any> { throw Exception("not used") }
    public func askMessage(target: TypedActorRef<Any>, message: Any): TypedFuture<Any> { throw Exception("not used") }
    public func system(): TypedActorSystem { throw Exception("not used") }
    public func scheduler(): TypedScheduler { throw Exception("not used") }
    public func child(name: String): Option<TypedActorRef<Any>> { None }
    public func children(): Array<TypedActorRef<Any>> { Array<TypedActorRef<Any>>() }
}

