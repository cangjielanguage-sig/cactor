/*
 * CActor 运行时 - 全局 Actor 预算（C1）
 *
 * 验证：超过 maxActors 的创建被明确拒绝（ActorBudgetExceededException），
 * 已创建的 Actor 不受影响、仍可正常处理消息；停止后名额释放；
 * 内存预算按每 Actor 估计字节换算成 Actor 上限，并与 maxActors 取小。
 */
package cactor_tests.unit.runtime.budget

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.context.{ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_runtime.{CActorRuntimeConfig}
import cactor_runtime.budget.{ActorBudgetExceededException, ESTIMATED_BYTES_PER_ACTOR}
import cactor_testkit.*

class BudgetCounter <: Actor {
    let handled: AtomicInt64

    init(handled: AtomicInt64) {
        this.handled = handled
    }

    public prop name: String {
        get() { "budget-counter" }
    }

    public prop description: String {
        get() { "counts messages" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: StringMessage => handled.fetchAdd(1)
            case _ => ()
        }
        MessageResult.Handled
    }
}

@Test
public class ActorBudgetTest {
    @TestCase
    public func actorBudgetRejectsInsteadOfOom(): Unit {
        let config = CActorRuntimeConfig.createDefault().withMaxActors(50)
        let system = CActorSystemFactory.create("budget-reject", config)
        try {
            let handled = AtomicInt64(0)
            let refs = ArrayList<ActorRef>()
            var rejected = 0
            for (i in 0..100) {
                try {
                    refs.add(system.actorOf({ => BudgetCounter(handled) }, "b${i}"))
                } catch (e: ActorBudgetExceededException) {
                    rejected++
                }
            }
            @Expect(refs.size, 50)
            @Expect(rejected, 50)

            // 已有 Actor 仍可用
            for (r in refs) {
                r.tell(StringMessage("ping"))
            }
            @Expect(awaitCondition(Duration.second * 10, { => handled.load() == 50 }), true)

            let usage = system.actorBudgetUsage()
            @Expect(usage.maxActors, 50)
            @Expect(usage.rejections, 50)
            @Expect(usage.liveActors, 50)
            @Expect(usage.remaining(), 0)
            @Expect(usage.isLimited(), true)

            // 停止后名额释放：可以继续创建
            for (r in refs) {
                system.stop(r)
            }
            @Expect(awaitCondition(Duration.second * 10, { => system.actorBudgetUsage().liveActors == 0 }), true)
            let again = system.actorOf({ => BudgetCounter(handled) }, "after-stop")
            @Expect(again.path.getPath().endsWith("after-stop"), true)
        } finally {
            system.terminate()
        }
    }

    @TestCase
    public func memoryBudgetDerivesActorLimit(): Unit {
        let byMemory = CActorRuntimeConfig.createDefault()
            .withActorMemoryBudget(ESTIMATED_BYTES_PER_ACTOR * 10)
        @Expect(byMemory.effectiveActorLimit(), 10)

        let both = CActorRuntimeConfig.createDefault()
            .withMaxActors(100)
            .withActorMemoryBudget(ESTIMATED_BYTES_PER_ACTOR * 10)
        @Expect(both.effectiveActorLimit(), 10)

        let byCount = CActorRuntimeConfig.createDefault().withMaxActors(100)
        @Expect(byCount.effectiveActorLimit(), 100)
        @Expect(CActorRuntimeConfig.createDefault().effectiveActorLimit(), 0)
    }

    @TestCase
    public func budgetAppliesToChildActors(): Unit {
        // 顶层 1 个 + 子 Actor 共享同一份预算
        let config = CActorRuntimeConfig.createDefault().withMaxActors(5)
        let system = CActorSystemFactory.create("budget-children", config)
        try {
            let handled = AtomicInt64(0)
            let parent = system.actorOf({ => Spawner(handled) }, "parent")
            parent.tell(StringMessage("spawn"))
            // 父 Actor 尝试创建 10 个子 Actor：预算 5（父占 1）→ 只成功 4 个
            @Expect(awaitCondition(Duration.second * 10, { =>
                system.actorBudgetUsage().liveActors == 5 || system.actorBudgetUsage().rejections > 0
            }), true)
            @Expect(system.actorBudgetUsage().liveActors, 5)
            @Expect(system.actorBudgetUsage().rejections > 0, true)
        } finally {
            system.terminate()
        }
    }
}

/**
 * 收到 spawn 后创建 10 个子 Actor；超预算的创建在 receive 中抛异常，
 * 由监督按默认策略重启（本用例只关心预算计数）。
 */
class Spawner <: Actor {
    let handled: AtomicInt64

    init(handled: AtomicInt64) {
        this.handled = handled
    }

    public prop name: String {
        get() { "spawner" }
    }

    public prop description: String {
        get() { "spawns children" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: StringMessage =>
                for (i in 0..10) {
                    try {
                        context.actorOf({ => BudgetCounter(handled) }, "c${i}")
                    } catch (e: ActorBudgetExceededException) {
                        // 预算拒绝：停止尝试，保持父 Actor 存活
                        break
                    }
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}
