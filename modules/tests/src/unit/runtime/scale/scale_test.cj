/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R4 规模测试：大量子 Actor / 顶层 Actor 的创建、处理与停止，停止后不残留登记信息
 * （百万级与每个 Actor 的内存见 cactor_examples.scale_test）
 */
package cactor_tests.unit.runtime.scale

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_testkit.*

class Leaf <: Actor {
    let processed: AtomicInt64
    let stopped: AtomicInt64

    init(processed: AtomicInt64, stopped: AtomicInt64) {
        this.processed = processed
        this.stopped = stopped
    }

    public prop name: String { get() { "leaf" } }
    public prop description: String { get() { "leaf" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        processed.fetchAdd(1)
        MessageResult.Handled
    }

    public func postStop(): Unit {
        stopped.fetchAdd(1)
    }
}

/** 收到 "spawn" 时创建 n 个子 Actor 并各发一条消息 */
class Fanout <: Actor {
    let n: Int64
    let processed: AtomicInt64
    let stopped: AtomicInt64

    init(n: Int64, processed: AtomicInt64, stopped: AtomicInt64) {
        this.n = n
        this.processed = processed
        this.stopped = stopped
    }

    public prop name: String { get() { "fanout" } }
    public prop description: String { get() { "fanout" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let p = processed
        let s = stopped
        let msg = StringMessage("work")
        for (i in 0..n) {
            context.actorOf({ => Leaf(p, s) }, "c${i}").tell(msg)
        }
        MessageResult.Handled
    }
}

let SCALE_TIMEOUT = Duration.second * 60

@Test
public class ScaleTest {
    @TestCase
    public func testTwentyThousandChildren(): Unit {
        let kit = ActorTestKit("scale-children")
        let n = 20000
        let processed = AtomicInt64(0)
        let stopped = AtomicInt64(0)
        let parent = kit.actorOf({ => Fanout(n, processed, stopped) }, "parent")
        parent << StringMessage("spawn")
        @Expect(awaitCondition(SCALE_TIMEOUT, { => processed.load() == n }), true)
        @Expect(kit.system.lookup("/user/parent/c19999").isSome(), true)
        // 停止父 Actor：全部子 Actor 先停止，各自的 postStop 恰好执行一次
        kit.system.stop(parent)
        @Expect(awaitCondition(SCALE_TIMEOUT, { => stopped.load() == n }), true)
        @Expect(awaitCondition(Duration.second * 5, { => kit.system.lookup("/user/parent/c0").isNone() }), true)
        // 父 Actor 在最后一个子 Actor 停止后才完成终止
        @Expect(awaitCondition(Duration.second * 5, { => kit.system.lookup("/user/parent").isNone() }), true)
        @Expect(processed.load(), n)
        kit.shutdown()
    }

    @TestCase
    public func testTopLevelCreateStopDoesNotLeak(): Unit {
        let kit = ActorTestKit("scale-top")
        let n = 5000
        let processed = AtomicInt64(0)
        let stopped = AtomicInt64(0)
        // 两轮使用相同的名字：第一轮停止后名字必须可以复用
        for (round in 0..2) {
            let refs = Array<ActorRef>(n, { i => kit.actorOf({ => Leaf(processed, stopped) }, "t${i}") })
            for (r in refs) {
                r << StringMessage("x")
            }
            let target = (round + 1) * n
            @Expect(awaitCondition(SCALE_TIMEOUT, { => processed.load() == target }), true)
            for (r in refs) {
                kit.system.stop(r)
            }
            @Expect(awaitCondition(SCALE_TIMEOUT, { => stopped.load() == target }), true)
            @Expect(awaitCondition(Duration.second * 5, { => kit.system.lookup("/user/t0").isNone() }), true)
        }
        // system.stop 的停止消息不能进入用户 receive
        @Expect(processed.load(), 2 * n)
        @Expect(kit.system.getRuntime().getActorRegistry().getTotalActorCount(), 0)
        kit.shutdown()
    }
}
