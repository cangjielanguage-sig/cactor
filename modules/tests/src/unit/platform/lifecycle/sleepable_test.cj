/*
 * CActor Platform.Lifecycle - 睡眠/唤醒（P2 / F1）验证
 *
 * 1. 睡 → 状态落盘、存活 Actor 数 −1；再投递则唤醒并恢复状态；
 * 2. 睡眠期间到达的消息被唤醒后处理，且每条只处理一次；
 * 3. SleepPolicy.Idle(d) 空闲自动睡，之后仍可正常唤醒。
 */
package cactor_tests.unit.platform.lifecycle

import std.sync.*
import std.time.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_platform.lifecycle.*
import cactor_testkit.*

class CountSession <: SleepableActor {
    let observed: AtomicInt64
    var count: Int64 = 0

    init(restored: Option<String>, observed: AtomicInt64) {
        super(restored)
        this.observed = observed
        match (restored) {
            case Some(s) => count = Int64.parse(s)
            case None => count = 0
        }
    }

    public prop name: String {
        get() { "count-session" }
    }

    public prop description: String {
        get() { "sleepable counter" }
    }

    public func state(): String {
        count.toString()
    }

    public func restore(state: String): Unit {
        count = Int64.parse(state)
    }

    public func onMessage(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: StringMessage =>
                count++
                observed.store(count)
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

@Test
public class SleepableTest {
    @TestCase
    public func actorSleepsAndWakesWithState(): Unit {
        let system = CActorSystemFactory.create("sleep-state")
        let registry = SleepRegistry(system, "sessions")
        try {
            let baseline = system.actorBudgetUsage().liveActors
            let observed = AtomicInt64(0)
            let handle = registry.handle("s1", { restored => CountSession(restored, observed) })
            handle.tell(StringMessage("a"))
            handle.tell(StringMessage("b"))
            handle.tell(StringMessage("c"))
            @Expect(awaitCondition(Duration.second * 10, { => observed.load() == 3 }), true)
            // 会话实例存活：比基线多 1 个 Actor
            @Expect(system.actorBudgetUsage().liveActors, baseline + 1)

            handle.sleep()
            // 睡：会话实例停止，存活数回到基线（状态只留在存储里）
            @Expect(awaitCondition(Duration.second * 10, { => system.actorBudgetUsage().liveActors == baseline }), true)

            // 醒：状态恢复后继续计数
            handle.tell(StringMessage("d"))
            @Expect(awaitCondition(Duration.second * 10, { => observed.load() == 4 }), true)
        } finally {
            registry.stop()
            system.terminate()
        }
    }

    @TestCase
    public func messageDuringWakeupIsNotLost(): Unit {
        let system = CActorSystemFactory.create("sleep-wakeup")
        let registry = SleepRegistry(system, "wakeup")
        try {
            let baseline = system.actorBudgetUsage().liveActors
            let observed = AtomicInt64(0)
            let handle = registry.handle("s1", { restored => CountSession(restored, observed) })
            handle.tell(StringMessage("first"))
            @Expect(awaitCondition(Duration.second * 10, { => observed.load() == 1 }), true)
            handle.sleep()
            @Expect(awaitCondition(Duration.second * 10, { => system.actorBudgetUsage().liveActors == baseline }), true)

            // 睡眠期间连续到达：第一条触发唤醒，其余按顺序处理；每条恰好一次
            let n: Int64 = 50
            for (i in 0..n) {
                handle.tell(StringMessage("m${i}"))
            }
            @Expect(awaitCondition(Duration.second * 10, { => observed.load() == 1 + n }), true)
            // 稳定后再等一会，确认没有重复处理
            sleep(Duration.millisecond * 200)
            @Expect(observed.load(), 1 + n)
        } finally {
            registry.stop()
            system.terminate()
        }
    }

    @TestCase
    public func idlePolicySleepsAfterTimeout(): Unit {
        let system = CActorSystemFactory.create("sleep-idle")
        let registry = SleepRegistry(system, "idle",
            policy: SleepPolicy.Idle(Duration.millisecond * 100))
        try {
            let baseline = system.actorBudgetUsage().liveActors
            let observed = AtomicInt64(0)
            let handle = registry.handle("s-idle", { restored => CountSession(restored, observed) })
            handle.tell(StringMessage("x"))
            @Expect(awaitCondition(Duration.second * 10, { => observed.load() == 1 }), true)
            // 空闲 100ms 后自动睡
            @Expect(awaitCondition(Duration.second * 10, { => system.actorBudgetUsage().liveActors == baseline }), true)
            // 唤醒后状态继续
            handle.tell(StringMessage("y"))
            @Expect(awaitCondition(Duration.second * 10, { => observed.load() == 2 }), true)
        } finally {
            registry.stop()
            system.terminate()
        }
    }
}
