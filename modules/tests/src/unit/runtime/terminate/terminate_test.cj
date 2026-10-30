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

/** 监视同名子 Actor：每次收到 Terminated 立即用同一名称重建，直到重建 rounds 次 */
class Reviver <: Actor {
    let rec: EventRecorder
    let rounds: Int64
    var revived: Int64 = 0

    init(rec: EventRecorder, rounds: Int64) {
        this.rec = rec
        this.rounds = rounds
    }

    public prop name: String { get() { "reviver" } }
    public prop description: String { get() { "reviver" } }

    public func onStarted(context: ActorContext): Unit {
        spawnAndStop(context)
    }

    private func spawnAndStop(context: ActorContext): Unit {
        let child = context.actorOf({ => Tree(0, rec, AtomicInt64(0)) }, "same-name")
        context.watch(child)
        context.stop(child)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: Terminated =>
                revived++
                rec.record("revived")
                if (revived < rounds) {
                    // 名称在 Terminated 投递前已释放，这里不能抛 “名称已存在”
                    spawnAndStop(context)
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

@Test
public class ChildNameReleaseTest {
    @TestCase
    public func testChildNameIsFreeWhenTerminatedArrives(): Unit {
        let system = CActorSystemFactory.create("name-release")
        let rec = EventRecorder()
        let r = system.actorOf({ => Reviver(rec, 200) }, "reviver")
        @Expect(awaitCondition(Duration.second * 10, { => rec.count("revived") == 200 }), true)
        // 每轮的子 Actor 都正常停止（postStop 记录 stop:0），父 Actor 没有因名称冲突重启
        @Expect(rec.count("stop:0"), 200)
        system.stop(r)
        system.terminate()
    }
}

/** 通过 context.system() 使用系统能力 */
class SystemUser <: Actor {
    let rec: EventRecorder

    init(rec: EventRecorder) {
        this.rec = rec
    }

    public prop name: String { get() { "system-user" } }
    public prop description: String { get() { "system-user" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let sys = context.system()
        if (let Some(s) <- (message as StringMessage)) {
            match (s.getContent()) {
                case "info" => rec.record("name:${sys.name}")
                case "spawn" =>
                    // 创建顶层 Actor（不是子 Actor）
                    let r = sys.actorOf({ => Tree(0, rec, AtomicInt64(0)) }, "spawned-top")
                    rec.record("spawned:${r.path.getPath()}")
                case "lookup" =>
                    let found = sys.lookup("/user/spawned-top")
                    rec.record("lookup:${found.isSome()}")
                    let missing = sys.lookup("/user/nobody")
                    rec.record("missing:${missing.isNone()}")
                case "stop-top" =>
                    if (let Some(r) <- sys.lookup("/user/spawned-top")) {
                        sys.stop(r)
                    }
                case "dead" =>
                    rec.record("dead:${sys.deadLetterCount()}")
                case "terminate" =>
                    sys.terminate()
                    rec.record("terminated:${sys.isTerminated()}")
                case _ => ()
            }
        }
        MessageResult.Handled
    }
}

@Test
public class ContextSystemTest {
    @TestCase
    public func testContextSystemCapabilities(): Unit {
        let system = CActorSystemFactory.create("ctx-system")
        let rec = EventRecorder()
        let u = system.actorOf({ => SystemUser(rec) }, "user")
        u << StringMessage("info")
        @Expect(rec.await("name:ctx-system", Duration.second * 3), true)
        u << StringMessage("spawn")
        @Expect(rec.awaitSize(2, Duration.second * 3), true)
        @Expect(rec.snapshot()[1], "spawned:/user/spawned-top")
        // 外部也能看到这个顶层 Actor
        @Expect(system.lookup("/user/spawned-top").isSome(), true)
        u << StringMessage("lookup")
        @Expect(rec.await("lookup:true", Duration.second * 3), true)
        @Expect(rec.await("missing:true", Duration.second * 3), true)
        u << StringMessage("stop-top")
        @Expect(rec.await("stop:0", Duration.second * 3), true)
        @Expect(awaitCondition(Duration.second * 3, { => system.lookup("/user/spawned-top").isNone() }), true)
        u << StringMessage("dead")
        @Expect(rec.awaitSize(6, Duration.second * 3), true)
        @Expect(rec.snapshot()[5].startsWith("dead:"), true)
        // 在 Actor 中终止系统：不卡住，系统随后进入终止状态
        let start = MonoTime.now()
        u << StringMessage("terminate")
        @Expect(rec.await("terminated:true", Duration.second * 3), true)
        @Expect(MonoTime.now() - start < Duration.second * 2, true)
        @Expect(system.isTerminated(), true)
    }

    @TestCase
    public func testStubContextReportsUnsupported(): Unit {
        // 只实现 sender() 的测试桩上下文：system() 明确报不支持，而不是返回空值
        let ctx = StubContext()
        var unsupported = false
        try {
            ctx.system()
        } catch (e: UnsupportedContextException) {
            unsupported = e.message.contains("system")
        }
        @Expect(unsupported, true)
    }
}

class StubContext <: ActorContext {
    public func sender(): Option<String> {
        None<String>
    }
}

/** 收到 "boom" 抛异常；重启后继续处理 */
class Flaky <: Actor {
    let rec: EventRecorder

    init(rec: EventRecorder) {
        this.rec = rec
    }

    public prop name: String { get() { "flaky" } }
    public prop description: String { get() { "flaky" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            if (s.getContent() == "boom") {
                throw Exception("故障注入")
            }
            rec.record(s.getContent())
        }
        MessageResult.Handled
    }

    public func postRestart(reason: Exception): Unit {
        rec.record("restarted")
    }
}

@Test
public class DefaultSupervisionTest {
    @TestCase
    public func testDefaultStrategyRestartsWithoutLimit(): Unit {
        // 与 Akka 经典 API 相同：未指定策略时不限次数重启（旧默认值一分钟内第 4 次失败就永久停止 Actor）
        let system = CActorSystemFactory.create("default-supervision")
        let rec = EventRecorder()
        let r = system.actorOf({ => Flaky(rec) }, "flaky")
        for (i in 0..10) {
            r << StringMessage("boom")
            r << StringMessage("ok-${i}")
        }
        @Expect(awaitCondition(Duration.second * 5, { => rec.count("ok-9") == 1 }), true)
        @Expect(rec.count("restarted"), 10)
        @Expect(system.lookup("/user/flaky").isSome(), true)
        system.terminate()
    }
}
