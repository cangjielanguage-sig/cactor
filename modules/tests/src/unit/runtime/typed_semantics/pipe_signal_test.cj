/*
 * CActor - ctx.pipeSignalToSelf（E44 的核心扩展）
 *
 *   - Future 完成后结果以信号送回 Actor：在 Actor 线程的 onSignal 中处理，不占命令协议 T
 *     （此前只有 pipeToSelf，结果必须映射成 T；持久化行为的内部 IO 通知没法放进用户的命令枚举）
 *   - 失败的 Future 同样送回（Failed 分支）
 *   - 信号与普通消息走同一个邮箱：200 个 Future 在其他线程完成、同时有 200 条消息到达，处理从不重叠
 *   - 经过 setup / supervise 包装后信号照样送达内层行为
 */
package cactor_tests.unit.runtime.typed_semantics

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.*
import cactor_testkit.*

/** 忙等约 50µs：让处理过程有一段时间窗口，信号若能并发进来就会被 overlaps 抓到（sleep 的粒度约 10ms，太慢） */
func psSpin(): Unit {
    let until = MonoTime.now() + Duration.microsecond * 50
    while (MonoTime.now() < until) {}
}

class PsDone <: TypedSignal {
    let outcome: AskOutcome<Int64>

    init(outcome: AskOutcome<Int64>) {
        this.outcome = outcome
    }

    public func signalName(): String {
        "PsDone"
    }
}

class PsActor <: TypedBehavior<String> {
    let rec: EventRecorder
    let active = AtomicInt64(0)
    let overlaps: AtomicInt64

    init(rec: EventRecorder, overlaps: AtomicInt64) {
        this.rec = rec
        this.overlaps = overlaps
    }

    private func enter(): Unit {
        if (active.fetchAdd(1) != 0) {
            overlaps.fetchAdd(1)
        }
    }

    private func leave(): Unit {
        active.fetchSub(1)
    }

    private func pipe(ctx: TypedActorContext<String>, outcome: AskOutcome<Int64>, delayMs: Int64): Unit {
        let p = TypedPromise<Int64>()
        ctx.pipeSignalToSelf<Int64>(p, { o => PsDone(o) })
        spawn {
            if (delayMs > 0) {
                sleep(Duration.millisecond * delayMs)
            }
            p.tryComplete(outcome)
        }
    }

    public func onMessage(message: String, ctx: TypedActorContext<String>): TypedBehavior<String> {
        enter()
        match (message) {
            case "ok" => pipe(ctx, AskOutcome<Int64>.Replied(42), 10)
            case "fail" => pipe(ctx, AskOutcome<Int64>.Failed(Exception("io failed")), 10)
            case "burst" =>
                for (i in 0..200) {
                    pipe(ctx, AskOutcome<Int64>.Replied(1000 + i), 0)
                }
            case m => rec.record("msg")
        }
        // 处理时间拉长一点，让信号有机会"想要"并发进来
        psSpin()
        leave()
        TypedBehaviors.same<String>()
    }

    public func onSignal(signal: TypedSignal, ctx: TypedActorContext<String>): TypedBehavior<String> {
        match (signal) {
            case d: PsDone =>
                enter()
                match (d.outcome) {
                    case Replied(v) where v == 42 => rec.record("done:42")
                    case Replied(_) => rec.record("b")
                    case Failed(e) => rec.record("failed:${e.message}")
                }
                psSpin()
                leave()
                TypedBehaviors.same<String>()
            case _ => TypedBehaviors.unhandled<String>()
        }
    }
}

let PS_T: Duration = Duration.second * 5

@Test
public class PipeSignalToSelfTest {
    @TestCase
    public func resultArrivesAsSignal(): Unit {
        let (kit, sys) = newSystem("pipe-signal")
        let rec = EventRecorder()
        let ref = sys.actorOf(PsActor(rec, AtomicInt64(0)), "ps")
        ref << "ok" << "fail"
        @Expect(rec.await("done:42", PS_T), true)
        @Expect(rec.await("failed:io failed", PS_T), true)
        kit.shutdown()
    }

    @TestCase
    public func signalsAndMessagesNeverOverlap(): Unit {
        let (kit, sys) = newSystem("pipe-signal-burst")
        let rec = EventRecorder()
        let overlaps = AtomicInt64(0)
        let ref = sys.actorOf(PsActor(rec, overlaps), "ps")
        ref.tell("burst")
        for (_ in 0..200) {
            ref.tell("m")
        }
        @Expect(awaitCondition(PS_T, { => rec.count("b") == 200 && rec.count("msg") == 200 }), true)
        @Expect(overlaps.load(), 0)
        kit.shutdown()
    }

    @TestCase
    public func signalReachesBehaviorInsideSetupAndSupervise(): Unit {
        let (kit, sys) = newSystem("pipe-signal-wrapped")
        let rec = EventRecorder()
        let b = TypedBehaviors.supervise(TypedBehaviors.setup<String>({ _ => PsActor(rec, AtomicInt64(0)) }))
            .onFailure(TypedSupervisorStrategy.restart())
        let ref = sys.actorOf(b, "ps")
        ref.tell("ok")
        @Expect(rec.await("done:42", PS_T), true)
        kit.shutdown()
    }
}
