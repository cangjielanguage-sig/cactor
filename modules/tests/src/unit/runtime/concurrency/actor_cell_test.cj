/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - Actor Cell 并发语义测试
 * 验证：tell 异步、同一 Actor 串行处理、单发送方有序、自发消息不递归、停止后丢弃消息
 */
package cactor_tests.unit.runtime.concurrency

import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

/** 轮询等待条件成立，避免固定 sleep */
func awaitCondition(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        if (cond()) {
            return true
        }
        sleep(Duration.millisecond * 2)
    }
    return cond()
}

/** 检测是否存在并发进入 receive */
class SerialCheckActor <: Actor {
    let inFlight = AtomicInt64(0)
    let violations = AtomicInt64(0)
    let handled = AtomicInt64(0)
    // 非原子字段：只有在串行执行时才能得到准确计数
    var plainCounter: Int64 = 0

    public prop name: String {
        get() { "SerialCheckActor" }
    }

    public prop description: String {
        get() { "检测并发进入" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (inFlight.fetchAdd(1) != 0) {
            violations.fetchAdd(1)
        }
        // 运行时自身也会投递生命周期消息（如 StartMessage），只统计用户消息
        if (message is StringMessage) {
            plainCounter += 1
            handled.fetchAdd(1)
        }
        inFlight.fetchAdd(-1)
        MessageResult.Handled
    }
}

/** 记录收到的序号，检测乱序 */
class OrderCheckActor <: Actor {
    var last: Int64 = -1
    let outOfOrder = AtomicInt64(0)
    let handled = AtomicInt64(0)

    public prop name: String {
        get() { "OrderCheckActor" }
    }

    public prop description: String {
        get() { "检测消息顺序" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: StringMessage =>
                let n = Int64.parse(m.getContent())
                if (n != last + 1) {
                    outOfOrder.fetchAdd(1)
                }
                last = n
                handled.fetchAdd(1)
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

/** 给自己发消息直到计数归零：同步实现会递归导致栈溢出 */
class SelfSendActor <: Actor {
    var selfRef: Option<ActorRef> = None
    let remaining = AtomicInt64(0)

    public prop name: String {
        get() { "SelfSendActor" }
    }

    public prop description: String {
        get() { "自发消息" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (remaining.fetchAdd(-1) > 1) {
            if (let Some(r) <- selfRef) {
                r.tell(StringMessage("again"))
            }
        }
        MessageResult.Handled
    }
}

/** 处理很慢的 Actor，用于验证 tell 不阻塞调用方 */
class SlowActor <: Actor {
    let handled = AtomicInt64(0)

    public prop name: String {
        get() { "SlowActor" }
    }

    public prop description: String {
        get() { "慢处理" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        sleep(Duration.millisecond * 200)
        handled.fetchAdd(1)
        MessageResult.Handled
    }
}

@Test
public class ActorCellConcurrencyTest {
    private func newSystem(tag: String): CActorSystem {
        CActorSystemFactory.create("cell-${tag}-${MonoTime.now().hashCode()}")
    }

    @TestCase
    func tellDoesNotBlockSender(): Unit {
        let system = newSystem("async")
        let actor = SlowActor()
        let ref = system.actorOf({ => actor }, "slow")
        let start = MonoTime.now()
        ref.tell(StringMessage("x"))
        let elapsed = MonoTime.now() - start
        @Expect(elapsed < Duration.millisecond * 100, true)
        @Expect(awaitCondition(Duration.second * 5, { => actor.handled.load() == 1 }), true)
        system.terminate()
    }

    @TestCase
    func concurrentSendersAreProcessedSerially(): Unit {
        let system = newSystem("serial")
        let actor = SerialCheckActor()
        let ref = system.actorOf({ => actor }, "serial")
        let senders = 8
        let perSender = 500
        let futures = ArrayList<Future<Unit>>()
        for (_ in 0..senders) {
            futures.add(spawn {
                for (i in 0..perSender) {
                    ref.tell(StringMessage("${i}"))
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        let total = Int64(senders * perSender)
        @Expect(awaitCondition(Duration.second * 10, { => actor.handled.load() == total }), true)
        @Expect(actor.violations.load(), 0)
        @Expect(actor.plainCounter, total)
        system.terminate()
    }

    @TestCase
    func singleSenderOrderIsPreserved(): Unit {
        let system = newSystem("order")
        let actor = OrderCheckActor()
        let ref = system.actorOf({ => actor }, "order")
        let n = 2000
        for (i in 0..n) {
            ref.tell(StringMessage("${i}"))
        }
        @Expect(awaitCondition(Duration.second * 10, { => actor.handled.load() == Int64(n) }), true)
        @Expect(actor.outOfOrder.load(), 0)
        @Expect(actor.last, Int64(n - 1))
        system.terminate()
    }

    @TestCase
    func selfSendDoesNotRecurse(): Unit {
        let system = newSystem("self")
        let actor = SelfSendActor()
        let depth: Int64 = 50000
        actor.remaining.store(depth)
        let ref = system.actorOf({ => actor }, "self")
        actor.selfRef = Some(ref)
        ref.tell(StringMessage("start"))
        @Expect(awaitCondition(Duration.second * 20, { => actor.remaining.load() <= 0 }), true)
        system.terminate()
    }

    @TestCase
    func stoppedActorDropsMessages(): Unit {
        let system = newSystem("stop")
        let actor = SerialCheckActor()
        let ref = system.actorOf({ => actor }, "stop")
        ref.tell(StringMessage("before"))
        @Expect(awaitCondition(Duration.second * 5, { => actor.handled.load() == 1 }), true)
        system.getRuntime().stop(ref)
        for (_ in 0..10) {
            ref.tell(StringMessage("after"))
        }
        sleep(Duration.millisecond * 100)
        @Expect(actor.handled.load(), 1)
        system.terminate()
    }
}
