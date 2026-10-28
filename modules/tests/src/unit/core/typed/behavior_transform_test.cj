package cactor_tests.unit.core.typed

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import cactor_core.typed.{TypedBehaviors, TypedActorContext, TypedBehavior, TypedActorRef, TypedLogger, TypedActorSystem, TypedScheduler, TypedFuture}

@Test
public class BehaviorTransformTestSuite {
    @TestCase
    public func testReceivePartial(): Unit {
        let beh = TypedBehaviors.receive<Int32>({ ctx, x =>
            TypedBehaviors.same<Int32>()
        })
        let partial = TypedBehaviors.receivePartial<Int32>({ x => x > 0 }, { ctx, x => TypedBehaviors.stopped<Int32>() })
        // 负数不匹配，保持 partial
        let b1 = partial.onMessage(-1, NullTypedContext<Int32>())
        // 正数匹配，转为 stopped
        let b2 = partial.onMessage(1, NullTypedContext<Int32>())
        @Assert(b1.getDescription(), "PartialBehavior")
        @Assert(b2.getDescription(), "StoppedBehavior")
    }

    @TestCase
    public func testTransformMessages(): Unit {
        let inner = TypedBehaviors.receive<Int32>({ ctx, x => TypedBehaviors.same<Int32>() })
        let transformed = TypedBehaviors.transformMessages<String, Int32>(inner, { s => 1 })
        let next = transformed.onMessage("hello", NullTypedContext<String>())
        @Assert(next.getDescription(), "TransformBehavior")
    }
}

class NullTypedContext<T> <: TypedActorContext<T> {
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

