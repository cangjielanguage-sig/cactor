/*
 * CActor - 阻塞调度器扩容（E39，R16）
 *
 * 此前扩容条件是"排队数 > 线程总数"：线程全被阻塞中的 Actor 占住后，新任务一个个到达时排队数只有 1，
 * 永远不扩容，第 17 个阻塞 Actor 无限期排队。现在按"空闲线程数"判断，没有空闲线程就扩容（上限 64）。
 *
 * 用例逐个启动阻塞 Actor，每个都等到它真正开始执行（占住一个线程）后再启动下一个，
 * 这样每次只有 1 个任务在排队——正是旧实现永远不扩容的情形。
 */
package cactor_tests.unit.runtime.scheduling

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}
import cactor_testkit.*

class BlockOn <: Message {
    public func messageType(): String {
        "BlockOn"
    }
}

/** 收到 BlockOn 后占住线程，直到 release（最多 30 s） */
class Blocker <: Actor {
    let started: AtomicInt64
    let release: AtomicBool

    init(started: AtomicInt64, release: AtomicBool) {
        this.started = started
        this.release = release
    }

    public prop name: String {
        get() { "blocker" }
    }

    public prop description: String {
        get() { "blocker" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        started.fetchAdd(1)
        let _ = awaitCondition(Duration.second * 30, { => release.load() })
        MessageResult.Handled
    }
}

@Test
public class BlockingPoolTest {
    @TestCase
    public func blockingPoolGrowsWhenAllWorkersAreBlocked(): Unit {
        let kit = ActorTestKit("blocking-pool")
        let started = AtomicInt64(0)
        let release = AtomicBool(false)
        // 默认初始 16 个线程；24 个阻塞 Actor 必须全部开始执行
        let n: Int64 = 24
        var ok = true
        for (i in 0..n) {
            let ref = kit.actorOf(PropsFactory.create<Actor>({ => Blocker(started, release) }).withDispatcher("blocking"),
                "b${i}")
            ref << BlockOn()
            if (!awaitCondition(Duration.second * 5, { => started.load() == i + 1 })) {
                ok = false
                break
            }
        }
        release.store(true)
        @Expect(ok, true)
        @Expect(started.load(), n)
        kit.shutdown()
    }
}
