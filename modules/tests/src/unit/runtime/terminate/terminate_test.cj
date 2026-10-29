/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 系统终止测试：terminate 停止全部 Actor（含子 Actor），执行 postStop，取消定时器；
 * 在 Actor 中调用 terminate 不会卡住
 */
package cactor_tests.unit.runtime.terminate

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_testkit.*

class Tree <: Actor {
    let depth: Int64
    let rec: EventRecorder
    let ticks: AtomicInt64

    init(depth: Int64, rec: EventRecorder, ticks: AtomicInt64) {
        this.depth = depth
        this.rec = rec
        this.ticks = ticks
    }

    public prop name: String { get() { "tree" } }
    public prop description: String { get() { "tree" } }

    public func onStarted(context: ActorContext): Unit {
        context.timers.startTimerAtFixedRate("tick", StringMessage("tick"), Duration.millisecond * 5)
        if (depth > 0) {
            let r = rec
            let t = ticks
            let d = depth - 1
            context.actorOf({ => Tree(d, r, t) }, "a")
            context.actorOf({ => Tree(d, r, t) }, "b")
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        ticks.fetchAdd(1)
        MessageResult.Handled
    }

    public func postStop(): Unit {
        rec.record("stop:${depth}")
    }
}

class Terminator <: Actor {
    let system: CActorSystem
    let done: AtomicBool

    init(system: CActorSystem, done: AtomicBool) {
        this.system = system
        this.done = done
    }

    public prop name: String { get() { "terminator" } }
    public prop description: String { get() { "terminator" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        system.terminate()
        done.store(true)
        MessageResult.Handled
    }
}

@Test
public class TerminateTest {
    @TestCase
    public func testTerminateStopsWholeTreeAndTimers(): Unit {
        let system = CActorSystemFactory.create("terminate-tree")
        let rec = EventRecorder()
        let ticks = AtomicInt64(0)
        // 两棵深度为 3 的树：每棵 1 + 2 + 4 + 8 = 15 个 Actor
        system.actorOf({ => Tree(3, rec, ticks) }, "t1")
        system.actorOf({ => Tree(3, rec, ticks) }, "t2")
        @Expect(awaitCondition(Duration.second * 3, { => ticks.load() > 100 }), true)
        system.terminate()
        // terminate 返回时全部 postStop 已执行
        @Expect(rec.size(), 30)
        @Expect(rec.count("stop:0"), 16)
        @Expect(rec.count("stop:3"), 2)
        // 子 Actor 先于父 Actor 停止
        @Expect(rec.indexOf("stop:0") < rec.indexOf("stop:3"), true)
        // 定时器不再触发
        let after = ticks.load()
        sleep(Duration.millisecond * 50)
        @Expect(ticks.load(), after)
        @Expect(system.isTerminated(), true)
    }

    @TestCase
    public func testTerminateFromInsideAnActorDoesNotHang(): Unit {
        let system = CActorSystemFactory.create("terminate-inside")
        let done = AtomicBool(false)
        let rec = EventRecorder()
        let ticks = AtomicInt64(0)
        system.actorOf({ => Tree(1, rec, ticks) }, "tree")
        let t = system.actorOf({ => Terminator(system, done) }, "terminator")
        let start = MonoTime.now()
        t << StringMessage("go")
        @Expect(awaitCondition(Duration.second * 3, { => done.load() }), true)
        @Expect(MonoTime.now() - start < Duration.second * 2, true)
        @Expect(awaitCondition(Duration.second * 3, { => rec.size() == 3 }), true)
    }
}
