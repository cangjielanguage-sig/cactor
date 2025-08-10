package cactor.tests.unit.core.typed

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import std.collection.*
import cactor.core.typed.*

// 简单的计数器，避免在Lambda中捕获可变局部变量
class Counter {
  private var v: Int32 = 0
  public func add(n: Int32): Unit { v += n }
  public func value(): Int32 { v }
}

// 最小Future实现
class TestFuture<T> <: TypedFuture<T> {
  private let r: T
  public init(r: T) { this.r = r }
  public func isCompleted(): Bool { true }
  public func get(): T { r }
  public func get(timeout: Duration): T { r }
  public func value(): Option<T> { Some(r) }
}
// 简单日志
class TestLogger <: TypedLogger {
  public func debug(message: String): Unit { () }
  public func info(message: String): Unit { () }
  public func warn(message: String): Unit { () }
  public func error(message: String): Unit { () }
}

// 持有列表，避免闭包捕获可变局部
class IntListHolder {
  public let list: ArrayList<Int32> = ArrayList<Int32>()
  public func add(n: Int32): Unit { list.add(n) }
  public func size(): Int32 { Int32(list.size) }
}


// 最小Scheduler实现
class TestScheduler <: TypedScheduler {
  public func scheduleOnce(delay: Duration, target: TypedActorRef<Any>, message: Any): Unit { () }
  public func schedule(initialDelay: Duration, interval: Duration, target: TypedActorRef<Any>, message: Any): TypedCancellable { TestCancellable() }
}
class TestCancellable <: TypedCancellable {
  private var cancelled: Bool = false
  public func cancel(): Bool { if (!cancelled) { cancelled = true; return true } else { return false } }
  public func isCancelled(): Bool { cancelled }
}

// 最小可用的 TypedActorContext 测试桩
class TestCtx<T> <: TypedActorContext<T> {
  public prop self: TypedActorRef<T> { get() { DummyRef<T>("/test/self") } }
  public func sender(): Option<String> { None }
  public func stopSelf(): Unit { () }
  public func log(): TypedLogger { TestLogger() }
  public func spawnChild(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any> { DummyRef<Any>("/test/" + name) }
  public func spawnAnonymousChild(behavior: TypedBehavior<Any>): TypedActorRef<Any> { DummyRef<Any>("/test/anon") }
  public func stopChild(child: TypedActorRef<Any>): Unit { () }
  public func watchActor(other: TypedActorRef<Any>): Unit { () }
  public func unwatchActor(other: TypedActorRef<Any>): Unit { () }
  public func askMessage(target: TypedActorRef<Any>, message: Any, timeout: Duration): TypedFuture<Any> { TestFuture<Any>(message) }
  public func askMessage(target: TypedActorRef<Any>, message: Any): TypedFuture<Any> { TestFuture<Any>(message) }
  public func system(): TypedActorSystem { DummySystem() }
  public func scheduler(): TypedScheduler { TestScheduler() }
  public func child(name: String): Option<TypedActorRef<Any>> { None }
  public func children(): Array<TypedActorRef<Any>> { Array<TypedActorRef<Any>>(0, { i => DummyRef<Any>("/empty") }) }
}

class DummyRef<T> <: TypedActorRef<T> {
  public let path: String
  public var received: ArrayList<T> = ArrayList<T>()
  public init(path: String) { this.path = path }
  public func tell(message: T): Unit { received.add(message) }
  public func pathString(): String { path }
  public func equals(other: TypedActorRef<T>): Bool { path == other.pathString() }
}

class DummySystem <: TypedActorSystem {
  public prop name: String { get() { "test" } }
  public func createActor(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any> { DummyRef<Any>("/user/" + name) }
  public func actorSelection(path: String): TypedActorSelection<Any> { SimpleTypedActorSelection<Any>(path, Array<TypedActorRef<Any>>(0, { i => DummyRef<Any>("/empty") })) }
  public func terminate(): Bool { true }
  public func isTerminated(): Bool { false }
  public func startTime(): Int64 { 0 }
}

@Test
public class BehaviorTestSuite {
  @TestCase
  public func testTransformMessages(): Unit {
    let ctr = Counter()
    let base = TypedBehaviors.receive<Int32>({ ctx, msg =>
      ctr.add(msg)
      TypedBehaviors.same<Int32>()
    })
    // 将 String 转换为预期的数值（避免依赖字符串长度API）
    let trans = TypedBehaviors.transformMessages<String, Int32>(base, { s => match(s){ case "hi" => Int32(2); case "hello" => Int32(5); case _ => Int32(0) } })
    let ctx = TestCtx<String>()
    trans.onMessage("hi", ctx)
    trans.onMessage("hello", ctx)
    @Assert(ctr.value(), Int32(2 + 5))
  }

  @TestCase
  public func testFilterBehavior(): Unit {
    let holder = IntListHolder()
    let inner = TypedBehaviors.receive<Int32>({ ctx, m => holder.add(m); TypedBehaviors.same<Int32>() })
    let filtered = TypedBehaviors.filter<Int32>(inner, { m => m % 2 == 0 })
    let ctx = TestCtx<Int32>()
    filtered.onMessage(1, ctx)
    filtered.onMessage(2, ctx)
    filtered.onMessage(3, ctx)
    filtered.onMessage(4, ctx)
    @Assert(holder.size(), 2)
  }

  @TestCase
  public func testPartialBehavior(): Unit {
    let sumBox = Counter()
    let pb = TypedBehaviors.receivePartial<Int32>({ m => m > 3 }, { ctx, m => sumBox.add(m); TypedBehaviors.same<Int32>() })
    let ctx = TestCtx<Int32>()
    pb.onMessage(2, ctx)
    pb.onMessage(5, ctx)
    pb.onMessage(1, ctx)
    pb.onMessage(7, ctx)
    @Assert(sumBox.value(), Int32(12))
  }
}

