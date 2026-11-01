/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 停止与并发发送的回归测试
 *
 * 停止 Actor 时，邮箱的"关闭"判定必须在入队同一把锁内完成：
 * 否则"发送方刚检查完 terminated"与"Cell 排空邮箱"之间的窗口里到达的消息
 * 会滞留在邮箱中——既不投递给用户，也不计为死信（静默丢失）。
 *
 * 不变量：并发发送 N 条并同时停止 Actor，最终"被处理数 + 死信数 == N"。
 */
package cactor_tests.unit.runtime.stop_race

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}

class Counter <: Actor {
    let processed: AtomicInt64
    let stopped: AtomicBool
    init(processed: AtomicInt64, stopped: AtomicBool) {
        this.processed = processed
        this.stopped = stopped
    }
    public prop name: String { get() { "counter" } }
    public prop description: String { get() { "计数" } }
    public func receive(message: Message, context: ActorContext): MessageResult {
        processed.fetchAdd(1)
        MessageResult.Handled
    }
    public func postStop(): Unit {
        stopped.store(true)
    }
}

private func waitUntil(cond: () -> Bool, timeout: Duration): Bool {
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        if (cond()) {
            return true
        }
        sleep(Duration.millisecond)
    }
    cond()
}

@Test
public class StopRaceTestSuite {
    /**
     * 并发发送与停止交错：不允许出现滞留消息
     * （修复前：滞留消息既不计入 processed 也不计入死信，总数对不上）
     */
    @TestCase
    public func testNoMessageLostWhenStoppingConcurrently(): Unit {
        let system = CActorSystemFactory.create("stop-race")
        let processed = AtomicInt64(0)
        let stopped = AtomicBool(false)
        let ref = system.actorOf({ => Counter(processed, stopped) }, "counter")
        let total: Int64 = 20000

        // 4 个发送线程持续发送，同时另一个线程停止该 Actor
        let senders = ArrayList<Future<Unit>>()
        for (_ in 0..4) {
            senders.add(spawn {
                for (_ in 0..total / 4) {
                    ref.tell(StringMessage("x"))
                }
            })
        }
        let stopper = spawn {
            system.stop(ref)
        }
        for (f in senders) {
            f.get()
        }
        stopper.get()
        // 停止后死信可能还在补记，等计数稳定
        waitUntil({ => stopped.load() }, Duration.second * 5)
        waitUntil({ => processed.load() + system.deadLetterCount() >= total }, Duration.second * 5)

        let got = processed.load() + system.deadLetterCount()
        @Assert(got, total)
        system.terminate()
    }

    /**
     * 停止后发送的消息必须计为死信（而不是静默丢弃或滞留）
     */
    @TestCase
    public func testMessagesAfterStopAreDeadLettered(): Unit {
        let system = CActorSystemFactory.create("stop-race-after")
        let processed = AtomicInt64(0)
        let stopped = AtomicBool(false)
        let ref = system.actorOf({ => Counter(processed, stopped) }, "counter")
        system.stop(ref)
        // 等 Actor 真正停止后再发送
        @Assert(waitUntil({ => stopped.load() }, Duration.second * 5), true)
        let before = system.deadLetterCount()
        for (_ in 0..100) {
            ref.tell(StringMessage("x"))
        }
        @Assert(system.deadLetterCount(), before + 100)
        @Assert(processed.load(), 0)
        system.terminate()
    }
}
