/*
 * CActor - 经典 ask 的超时语义（E38，T8）
 *
 * 此前超时只由 30 s 一次的清理循环处理：1 s 的超时最多晚 30 s 才回调，未调用 start() 时永不超时。
 * 现在每个请求自带一次性定时器，按自己的时限准时超时；收到回复时取消定时器。
 */
package cactor_tests.unit.patterns

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{Message}
import cactor_core.actor.{ActorRef, ActorPath}
import cactor_patterns.ask.{AskPatternManager, AskMessage, AskTimeoutException, Result}
import cactor_testkit.{awaitCondition}

class AskQuestion <: Message {
    public func messageType(): String {
        "AskQuestion"
    }
}

class AskAnswer <: Message {
    public func messageType(): String {
        "AskAnswer"
    }
}

/** 目标：answer 为真时立即回复，否则吞掉请求 */
class AskTarget <: ActorRef {
    let answer: Bool
    private let p = ActorPath("ask-test", "target")

    init(answer: Bool) {
        this.answer = answer
    }

    public prop path: ActorPath {
        get() { p }
    }

    public func tell(message: Message): Unit {
        if (let Some(ask) <- (message as AskMessage)) {
            if (answer) {
                ask.getReplyTo().tell(AskAnswer())
            }
        }
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        tell(message)
    }

    public func equals(other: ActorRef): Bool {
        other.path.toString() == p.toString()
    }

    public func hashCode(): Int32 {
        0
    }
}

@Test
public class AskTimeoutTest {
    @TestCase
    public func timeoutFiresOnTimeWithoutCleanupLoop(): Unit {
        // 不调用 start()：此前没有清理循环就永不超时
        let manager = AskPatternManager(Duration.second * 5)
        let done = AtomicBool(false)
        let timedOut = AtomicBool(false)
        let start = MonoTime.now()
        let elapsedMs = AtomicInt64(-1)
        let future = manager.ask(AskTarget(false), AskQuestion(), Duration.millisecond * 300)
        future.onComplete({ r =>
            elapsedMs.store((MonoTime.now() - start).toMilliseconds())
            match (r) {
                case Result.Failure(e) => timedOut.store(e is AskTimeoutException)
                case _ => ()
            }
            done.store(true)
        })
        @Expect(awaitCondition(Duration.second * 5, { => done.load() }), true)
        @Expect(timedOut.load(), true)
        // 按自己的时限超时：不早于 300ms，也不会拖到清理周期
        @Expect(elapsedMs.load() >= 295 && elapsedMs.load() < 2000, true)
        @Expect(manager.getPendingRequestCount(), 0)
    }

    @TestCase
    public func replyCancelsTimeout(): Unit {
        let manager = AskPatternManager(Duration.second * 5)
        let failures = AtomicInt64(0)
        let successes = AtomicInt64(0)
        let future = manager.ask(AskTarget(true), AskQuestion(), Duration.millisecond * 100)
        future.onComplete({ r =>
            match (r) {
                case Result.Success(_) => successes.fetchAdd(1)
                case Result.Failure(_) => failures.fetchAdd(1)
            }
            ()
        })
        @Expect(awaitCondition(Duration.second * 3, { => successes.load() == 1 }), true)
        // 超时时刻过去后也不会再以失败结束一次
        sleep(Duration.millisecond * 300)
        @Expect(failures.load(), 0)
        @Expect(successes.load(), 1)
    }
}
