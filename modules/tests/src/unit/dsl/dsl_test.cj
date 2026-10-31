/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 测试（R5）：@actorReceive 宏、时间字面量、<< 发送
 */
package cactor_tests.unit.dsl

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorRef, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.dsl.*
import cactor_macros.*
import cactor_testkit.*

class Greet <: Message {
    let who: String
    init(who: String) { this.who = who }
    public func messageType(): String { "Greet" }
}

class Add <: Message {
    let n: Int64
    init(n: Int64) { this.n = n }
    public func messageType(): String { "Add" }
}

class Shutdown <: Message {
    public func messageType(): String { "Shutdown" }
}

@actorReceive[]
class Greeter <: Actor {
    let rec: EventRecorder
    var total: Int64 = 0

    init(rec: EventRecorder) {
        this.rec = rec
    }

    func onGreet(msg: Greet, ctx: ActorContext): MessageResult {
        rec.record("hello ${msg.who}")
        MessageResult.Handled
    }

    func onAdd(msg: Add, ctx: ActorContext): MessageResult {
        total += msg.n
        rec.record("total ${total}")
        MessageResult.Handled
    }

    func onShutdown(msg: Shutdown, ctx: ActorContext): MessageResult {
        MessageResult.Stop
    }

    public func postStop(): Unit {
        rec.record("stopped")
    }
}

@Test
public class DslTest {
    @TestCase
    public func testDurationLiterals(): Unit {
        @Expect(100.millis, Duration.millisecond * 100)
        @Expect(3.seconds, Duration.second * 3)
        @Expect(2.minutes, Duration.minute * 2)
        @Expect(1.hours, Duration.hour)
        @Expect(5.micros, Duration.microsecond * 5)
        @Expect(7.nanos, Duration.nanosecond * 7)
    }

    @TestCase
    public func testGeneratedReceiveDispatchesByType(): Unit {
        let kit = ActorTestKit("dsl-receive")
        let rec = EventRecorder()
        let g = kit.actorOf({ => Greeter(rec) }, "greeter")
        g << Greet("cangjie") << Add(2) << Add(3) << StringMessage("ignored") << Shutdown()
        @Expect(rec.await("stopped", 3.seconds), true)
        @Expect(rec.contains("hello cangjie"), true)
        @Expect(rec.contains("total 5"), true)
        @Expect(Greeter(rec).name, "Greeter")
        kit.shutdown()
    }
}
