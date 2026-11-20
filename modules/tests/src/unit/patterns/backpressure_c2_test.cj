/*
 * CActor - 差距 C2：显式背压 + 负载自报
 *
 * 1. ActorRef.isBusy()/mailboxBacklogSeconds()：处理中为忙；积压时最老消息等待时长上升；
 * 2. BackpressuredRef.offer：在途 >= 水位返回 Backpressured(inFlight, limit)，消息不入队；
 *    慢消费者 + 5000 条，按水位降速后零丢弃、有界在途；目标停止后返回 Closed。
 */
package cactor_tests.unit.patterns

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_patterns.backpressure.{BackpressuredRef, OfferResult}
import cactor_testkit.*

/** 收到一条消息就占住线程，直到 release */
class GatedActor <: Actor {
    let started: AtomicInt64
    let release: AtomicBool
    let processed: AtomicInt64

    init(started: AtomicInt64, release: AtomicBool, processed: AtomicInt64) {
        this.started = started
        this.release = release
        this.processed = processed
    }

    public prop name: String { get() { "gated" } }
    public prop description: String { get() { "gated" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        started.fetchAdd(1)
        let _ = awaitCondition(Duration.second * 30, { => release.load() })
        processed.fetchAdd(1)
        MessageResult.Handled
    }
}

/** 每条消息做一点工作，制造可观察的积压 */
class SlowConsumer <: Actor {
    let processed: AtomicInt64
    let delay: Duration

    init(processed: AtomicInt64, delay: Duration) {
        this.processed = processed
        this.delay = delay
    }

    public prop name: String { get() { "slow" } }
    public prop description: String { get() { "slow" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        sleep(delay)
        processed.fetchAdd(1)
        MessageResult.Handled
    }
}

@Test
public class BackpressureC2Test {
    @TestCase
    public func actorReportsBusyAndBacklogWhileProcessing(): Unit {
        let system = CActorSystemFactory.create("bp-busy")
        try {
            let started = AtomicInt64(0)
            let release = AtomicBool(false)
            let processed = AtomicInt64(0)
            let ref = system.actorOf({ => GatedActor(started, release, processed) }, "gated")
            ref.tell(StringMessage("m1"))
            @Expect(awaitCondition(Duration.second * 10, { => started.load() == 1 }), true)
            @Expect(ref.isBusy(), true)

            ref.tell(StringMessage("m2"))
            ref.tell(StringMessage("m3"))
            @Expect(awaitCondition(Duration.second * 10, { => ref.pendingCount() >= 2 }), true)
            sleep(Duration.millisecond * 80)
            @Expect(ref.mailboxBacklogSeconds() > 0.0, true)

            release.store(true)
            @Expect(awaitCondition(Duration.second * 10, { => processed.load() == 3 }), true)
            @Expect(awaitCondition(Duration.second * 10, { => !ref.isBusy() }), true)
            @Expect(awaitCondition(Duration.second * 10, { => ref.mailboxBacklogSeconds() == 0.0 }), true)
        } finally {
            system.terminate()
        }
    }

    @TestCase
    public func backpressuredRefBoundsInFlightWithoutDrops(): Unit {
        let system = CActorSystemFactory.create("bp-ref")
        try {
            let processed = AtomicInt64(0)
            let ref = system.actorOf({ => SlowConsumer(processed, Duration.microsecond * 20) }, "slow")
            let limit: Int64 = 32
            let bp = BackpressuredRef<StringMessage>(ref, limit: limit)
            let n: Int64 = 2000
            var offered: Int64 = 0
            var maxPending: Int64 = 0
            var maxExactInFlight: Int64 = 0
            var sawBackpressure = false
            while (offered < n) {
                match (bp.offer(StringMessage("x"))) {
                    case Enqueued =>
                        offered++
                        // pendingCount 是跨线程近似计数（读侧可能看到滞后的 drIndex），允许 1–2 的抖动
                        let p = bp.inFlight()
                        if (p > maxPending) { maxPending = p }
                        let exact = offered - processed.load()
                        if (exact > maxExactInFlight) { maxExactInFlight = exact }
                    case Backpressured(inFlight, lim) =>
                        sawBackpressure = true
                        if (inFlight > maxPending) { maxPending = inFlight }
                        @Expect(lim, limit)
                        if (!bp.whenWritable(Duration.second * 5)) {
                            throw Exception("等待可写超时")
                        }
                    case Closed =>
                        throw Exception("目标提前停止")
                }
            }
            @Expect(sawBackpressure, true)
            // 精确在途（已 offer - 已处理）不超过水位；近似计数允许少量抖动
            @Expect(maxExactInFlight <= limit + 1, true)
            @Expect(maxPending <= limit + 2, true)
            @Expect(awaitCondition(Duration.second * 30, { => processed.load() == n }), true)

            system.stop(ref)
            @Expect(awaitCondition(Duration.second * 10, { => ref.isTerminated() }), true)
            @Expect(bp.offer(StringMessage("after-stop")).isClosed(), true)
        } finally {
            system.terminate()
        }
    }
}
