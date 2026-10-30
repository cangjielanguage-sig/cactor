/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 监督重启测试
 * 验证：异常触发重启 → 用工厂重建新实例（状态清零）→ 回调顺序正确 → 超过重试上限后停止
 */
package cactor_tests.unit.runtime.concurrency

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, PropsFactory}
import cactor_core.supervision.{DefaultSupervisionStrategy}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

/** 记录各实例的状态与回调，"boom" 时抛异常 */
class FlakyActor <: Actor {
    private var count: Int64 = 0
    let lastCount: AtomicInt64
    let events: ArrayList<String>
    let eventsLock: Mutex

    init(lastCount: AtomicInt64, events: ArrayList<String>, eventsLock: Mutex) {
        this.lastCount = lastCount
        this.events = events
        this.eventsLock = eventsLock
    }

    public prop name: String {
        get() { "FlakyActor" }
    }

    public prop description: String {
        get() { "会失败的 Actor" }
    }

    private func record(e: String): Unit {
        synchronized(eventsLock) {
            events.add(e)
        }
    }

    public func preRestart(reason: Exception): Unit {
        record("preRestart")
    }

    public func postRestart(reason: Exception): Unit {
        record("postRestart")
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: StringMessage =>
                if (m.getContent() == "boom") {
                    throw Exception("boom")
                }
                count += 1
                lastCount.store(count)
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

@Test
public class SupervisionRestartTest {
    @TestCase
    func failureRestartsWithFreshInstance(): Unit {
        let system = CActorSystemFactory.create("restart-${MonoTime.now().hashCode()}")
        let created = AtomicInt64(0)
        let lastCount = AtomicInt64(0)
        let events = ArrayList<String>()
        let lock = Mutex()
        let ref = system.actorOf({ =>
            created.fetchAdd(1)
            FlakyActor(lastCount, events, lock)
        }, "flaky")
        ref.tell(StringMessage("a"))
        ref.tell(StringMessage("b"))
        @Expect(awaitCondition(Duration.second * 5, { => lastCount.load() == 2 }), true)

        ref.tell(StringMessage("boom"))
        ref.tell(StringMessage("c"))
        // 新实例从 0 开始计数，因此处理 "c" 后为 1
        @Expect(awaitCondition(Duration.second * 5, { => lastCount.load() == 1 }), true)
        @Expect(created.load(), 2)
        let seen = synchronized(lock) { events.toArray() }
        @Expect(seen.size, 2)
        @Expect(seen[0], "preRestart")
        @Expect(seen[1], "postRestart")
        system.terminate()
    }

    @TestCase
    func tooManyFailuresStopTheActor(): Unit {
        let system = CActorSystemFactory.create("restart-limit-${MonoTime.now().hashCode()}")
        let runtime = system.getRuntime()
        let created = AtomicInt64(0)
        let lastCount = AtomicInt64(0)
        // 显式使用有上限的 OneForOne 策略：1 分钟内最多重启 3 次，第 4 次失败后停止
        //（未指定策略时与 Akka 经典 API 相同，不限次数重启，见 DefaultSupervisionTest）
        let props = PropsFactory.create<Actor>({ =>
            created.fetchAdd(1)
            FlakyActor(lastCount, ArrayList<String>(), Mutex())
        }).withSupervisorStrategy(DefaultSupervisionStrategy.oneForOne())
        let ref = system.actorOf(props, "flaky-limit")
        for (_ in 0..4) {
            ref.tell(StringMessage("boom"))
        }
        @Expect(awaitCondition(Duration.second * 5, { => runtime.getUserGuardian().getUserActor("flaky-limit").isNone() }), true)
        @Expect(created.load(), 4)
        // 停止后的消息不再处理
        ref.tell(StringMessage("x"))
        sleep(Duration.millisecond * 100)
        @Expect(lastCount.load(), 0)
        system.terminate()
    }
}
