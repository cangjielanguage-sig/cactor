/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R4 测试：有界邮箱满时的三种处理方式（DropNew / Fail / Block），以及 ctx.log 的来源标签
 */
package cactor_tests.unit.runtime.backpressure

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.logging.{Logging, MemorySink, StderrSink, LogLevel}
import cactor_testkit.*

/** 处理每条消息前等待 gate 打开，然后计数 */
class Gated <: Actor {
    let gate: AtomicBool
    let delay: Duration
    let count: AtomicInt64
    init(gate: AtomicBool, delay: Duration, count: AtomicInt64) {
        this.gate = gate
        this.delay = delay
        this.count = count
    }
    public prop name: String { get() { "gated" } }
    public prop description: String { get() { "gated" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        while (!gate.load()) {
            sleep(Duration.millisecond)
        }
        if (delay > Duration.Zero) {
            sleep(delay)
        }
        count.fetchAdd(1)
        MessageResult.Handled
    }
}

class LogsOnMessage <: Actor {
    public prop name: String { get() { "logger" } }
    public prop description: String { get() { "logger" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        context.log().warn("got message")
        MessageResult.Handled
    }
}

func gatedProps(gate: AtomicBool, delay: Duration, count: AtomicInt64, overflow: MailboxOverflow): Props<Actor> {
    PropsFactory.create<Actor>({ => Gated(gate, delay, count) })
        .withBoundedMailbox(4, overflow)
        .withDispatcher("blocking")
}

@Test
public class BackpressureTest {
    @TestCase
    public func testFailThrowsToSender(): Unit {
        let kit = ActorTestKit("bp-fail")
        let gate = AtomicBool(false)
        let count = AtomicInt64(0)
        let r = kit.actorOf(gatedProps(gate, Duration.Zero, count, MailboxOverflow.Fail), "fail")
        var failures: Int64 = 0
        for (i in 0..20) {
            try {
                r << StringMessage("${i}")
            } catch (_: MailboxFullException) {
                failures++
            }
        }
        // 容量 4，外加一条可能已被取出正在等待 gate 的消息
        @Expect(failures >= 15 && failures <= 16, true)
        @Expect(kit.system.deadLetterCount() >= failures, true)
        gate.store(true)
        let expected = 20 - failures
        @Expect(awaitCondition(Duration.second * 3, { => count.load() == expected }), true)
        kit.shutdown()
    }

    @TestCase
    public func testBlockAppliesBackpressureWithoutLoss(): Unit {
        let kit = ActorTestKit("bp-block")
        let gate = AtomicBool(true)
        let count = AtomicInt64(0)
        let r = kit.actorOf(gatedProps(gate, Duration.millisecond * 2, count, MailboxOverflow.Block(Duration.second * 5)), "block")
        let start = MonoTime.now()
        for (i in 0..60) {
            r << StringMessage("${i}")
        }
        let elapsed = MonoTime.now() - start
        // 发送方被限速：60 条、每条 2ms，容量只有 4，发送至少要等待约 (60-5)*2ms
        @Expect(elapsed >= Duration.millisecond * 80, true)
        @Expect(awaitCondition(Duration.second * 5, { => count.load() == 60 }), true)
        @Expect(kit.system.deadLetterCount(), 0)
        kit.shutdown()
    }

    @TestCase
    public func testBlockTimesOutToDeadLetter(): Unit {
        let kit = ActorTestKit("bp-block-timeout")
        let gate = AtomicBool(false)
        let count = AtomicInt64(0)
        let r = kit.actorOf(gatedProps(gate, Duration.Zero, count, MailboxOverflow.Block(Duration.millisecond * 50)), "bt")
        for (i in 0..5) {
            r << StringMessage("${i}")
        }
        sleep(Duration.millisecond * 50)
        let before = kit.system.deadLetterCount()
        let start = MonoTime.now()
        r << StringMessage("overflow-1")
        r << StringMessage("overflow-2")
        let waited = MonoTime.now() - start
        @Expect(waited >= Duration.millisecond * 90, true)
        @Expect(kit.system.deadLetterCount() - before, 2)
        gate.store(true)
        kit.shutdown()
    }

    @TestCase
    public func testDropNewIsDefault(): Unit {
        let kit = ActorTestKit("bp-drop")
        let gate = AtomicBool(false)
        let count = AtomicInt64(0)
        let r = kit.actorOf(PropsFactory.create<Actor>({ => Gated(gate, Duration.Zero, count) }).withBoundedMailbox(4)
            .withDispatcher("blocking"), "drop")
        for (i in 0..20) {
            r << StringMessage("${i}")
        }
        @Expect(kit.system.deadLetterCount() >= 15, true)
        gate.store(true)
        kit.shutdown()
    }

    @TestCase
    public func testContextLogIsTaggedWithActorPath(): Unit {
        let sink = MemorySink()
        Logging.setSink(sink)
        let kit = ActorTestKit("bp-log")
        let r = kit.actorOf({ => LogsOnMessage() }, "logger")
        r << StringMessage("x")
        @Expect(awaitCondition(Duration.second * 3, { => sink.count(LogLevel.Warn, "got message") == 1 }), true)
        @Expect(sink.snapshot()[0].source, "/user/logger")
        Logging.setSink(StderrSink())
        kit.shutdown()
    }
}
