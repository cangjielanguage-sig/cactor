/*
 * CActor - 类型化语义对齐（E36 部分）
 *
 *   - unstash 逐条出队：处理中抛出异常时，未处理的暂存消息仍留在缓冲区（T10，与 Akka 相同）
 *   - 在 Actor 处理方法里阻塞等待未完成的 TypedPromise 立即报错，而不是把死锁伪装成超时（T15）
 */
package cactor_tests.unit.runtime.typed_alignment

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}
import cactor_core.typed.{TypedBehavior, TypedBehaviors, TypedStashBuffer, TypedPromise, TypedAskTimeoutException}
import cactor_testkit.*

class TaBoom <: Exception {
    init(msg: String) {
        super(msg)
    }
}

/** 闭包不能捕获可变局部变量后逃逸：用盒子保存 stash 缓冲区 */
class StashBox {
    var buffer: Option<TypedStashBuffer<String>> = None<TypedStashBuffer<String>>
}

func active(rec: EventRecorder): TypedBehavior<String> {
    TypedBehaviors.receiveMessage<String>({ m =>
        if (m == "boom") {
            throw TaBoom("boom")
        }
        rec.record(m)
        TypedBehaviors.same<String>()
    })
}

class PromiseWaiter <: Actor {
    let rec: EventRecorder

    init(rec: EventRecorder) {
        this.rec = rec
    }

    public prop name: String {
        get() { "waiter" }
    }

    public prop description: String {
        get() { "waiter" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let p = TypedPromise<Int64>()
        let start = MonoTime.now()
        try {
            p.get(Duration.second)
            rec.record("returned")
        } catch (e: IllegalStateException) {
            rec.record("rejected:${(MonoTime.now() - start).toMilliseconds() < 500}")
        } catch (e: TypedAskTimeoutException) {
            rec.record("timeout")
        }
        // 已完成的结果在任何线程都可以取
        let done = TypedPromise<Int64>()
        done.trySuccess(42) |> ignoreBool
        rec.record("completed:${done.get(Duration.second)}")
        MessageResult.Handled
    }
}

class Go <: Message {
    public func messageType(): String {
        "Go"
    }
}

@Test
public class TypedAlignmentTest {
    @TestCase
    public func unstashKeepsUnprocessedMessagesWhenOneThrows(): Unit {
        let rec = EventRecorder()
        let box = StashBox()
        let b = TypedBehaviors.withStash<String>(10, { stash =>
            box.buffer = Some(stash)
            TypedBehaviors.receiveMessage<String>({ m =>
                if (m == "go") {
                    stash.unstashAll(active(rec))
                } else {
                    stash.stash(m)
                    TypedBehaviors.same<String>()
                }
            })
        })
        let kit = BehaviorTestKit<String>(b)
        kit.run("a")
        kit.run("boom")
        kit.run("b")
        kit.run("c")
        var threw = false
        try {
            kit.run("go")
        } catch (e: TaBoom) {
            threw = true
        }
        @Expect(threw, true)
        @Expect(rec.snapshot(), ["a"])
        // a、boom 已出队；b、c 仍在缓冲区，可以再次 unstash（此前先清空缓冲区，b、c 丢失）
        let buf = box.buffer.getOrThrow()
        @Expect(buf.size(), 2)
        buf.unstashAll(active(rec)) |> ignoreBehavior
        @Expect(rec.snapshot(), ["a", "b", "c"])
        @Expect(buf.isEmpty(), true)
    }

    @TestCase
    public func unstashAllOnlyProcessesMessagesPresentAtCallTime(): Unit {
        let rec = EventRecorder()
        let box = StashBox()
        let b = TypedBehaviors.withStash<String>(10, { stash =>
            box.buffer = Some(stash)
            TypedBehaviors.receiveMessage<String>({ m =>
                stash.stash(m)
                TypedBehaviors.same<String>()
            })
        })
        let kit = BehaviorTestKit<String>(b)
        kit.run("x")
        kit.run("y")
        let buf = box.buffer.getOrThrow()
        // 处理时再次 stash 的消息排到末尾、本次不处理（否则会无限循环）
        let restash = TypedBehaviors.receiveMessage<String>({ m =>
            rec.record(m)
            buf.stash("again-${m}")
            TypedBehaviors.same<String>()
        })
        buf.unstashAll(restash) |> ignoreBehavior
        @Expect(rec.snapshot(), ["x", "y"])
        @Expect(buf.size(), 2)
    }

    @TestCase
    public func blockingOnPromiseInsideActorFailsFast(): Unit {
        let kit = ActorTestKit("ta-promise")
        let rec = EventRecorder()
        let w = kit.actorOf({ => PromiseWaiter(rec) }, "waiter")
        w << Go()
        @Expect(rec.awaitSize(2, Duration.second * 5), true)
        @Expect(rec.snapshot(), ["rejected:true", "completed:42"])
        kit.shutdown()
    }
}

func ignoreBehavior(_: TypedBehavior<String>): Unit {}

func ignoreBool(_: Bool): Unit {}
