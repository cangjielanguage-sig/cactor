/*
 * CActor Core.Typed - ask 主动取消回归
 *
 * 用同步收件箱控制回复顺序，竞态使用真实并发；不靠固定 sleep 排出事件顺序。
 */
package cactor_tests.unit.runtime.typed_alignment

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.*
import cactor_testkit.*

func wasCancelled<T>(promise: TypedPromise<T>): Bool {
    match (promise.outcome()) {
        case Some(AskOutcome<T>.Failed(e)) => (e as TypedAskCancelledException).isSome()
        case _ => false
    }
}

@Test
public class AskCancellationTest {
    @TestCase
    public func preCancelledSkipsFactoryAndRemoteTarget(): Unit {
        let token = AskCancellationToken()
        @Expect(token.cancel(), true)
        @Expect(token.cancel(), false)
        let calls = AtomicInt64(0)
        let inbox = TestInbox<TypedActorRef<Int64>>("pre-cancel")
        let p = TypedAsk.ask<TypedActorRef<Int64>, Int64>(inbox, { ref =>
            calls.fetchAdd(1)
            ref
        }, Duration.second, cancellation: Some(token))
        @Expect(wasCancelled(p), true)
        @Expect(calls.load(), 0)
        @Expect(inbox.hasMessages(), false)
        // 收件箱没有经典目标；预取消必须在解析目标前生效。
        let remote = TypedAsk.askRemote<TypedActorRef<Int64>, Int64>(
            inbox, TestInbox<Int64>("unused-reply"), Duration.second, cancellation: Some(token))
        @Expect(wasCancelled(remote), true)
    }

    @TestCase
    public func sharedTokenCancelsAllAndLateRepliesCannotReplaceFailure(): Unit {
        let token = AskCancellationToken()
        let inbox = TestInbox<TypedActorRef<Int64>>("shared")
        let first = TypedAsk.ask<TypedActorRef<Int64>, Int64>(inbox, { r => r }, Duration.second * 30,
            cancellation: Some(token))
        let second = TypedAsk.ask<TypedActorRef<Int64>, Int64>(inbox, { r => r }, Duration.second * 30,
            cancellation: Some(token))
        let firstReply = inbox.receiveMessage()
        let secondReply = inbox.receiveMessage()
        let callbacks = AtomicInt64(0)
        first.onComplete({ _ =>
            // 重入令牌必须安全，不能在持有订阅集合锁时完成 Promise。
            token.cancel()
            callbacks.fetchAdd(1)
            ()
        })
        @Expect(token.cancel(), true)
        firstReply.tell(1)
        secondReply.tell(2)
        @Expect(wasCancelled(first), true)
        @Expect(wasCancelled(second), true)
        @Expect(callbacks.load(), 1)
        var thrown = false
        try { first.get(Duration.second) } catch (_: TypedAskCancelledException) { thrown = true }
        @Expect(thrown, true)
    }

    @TestCase
    public func replyAndFactoryFailureWinBeforeLaterCancellation(): Unit {
        let token = AskCancellationToken()
        let inbox = TestInbox<TypedActorRef<Int64>>("success")
        let p = TypedAsk.ask<TypedActorRef<Int64>, Int64>(inbox, { r => r }, Duration.second,
            cancellation: Some(token))
        inbox.receiveMessage().tell(42)
        let failed = TypedAsk.ask<TypedActorRef<Int64>, Int64>(inbox, { _ =>
            throw IllegalArgumentException("factory-failed")
        }, Duration.second, cancellation: Some(token))
        token.cancel()
        @Expect(p.get(Duration.second), 42)
        @Expect(wasCancelled(failed), false)
        @Expect(failed.outcome().getOrThrow().failure().getOrThrow().message, "factory-failed")
    }

    @TestCase
    public func cancellationInsideFactoryPreventsDispatch(): Unit {
        let token = AskCancellationToken()
        let inbox = TestInbox<TypedActorRef<Int64>>("factory")
        let p = TypedAsk.ask<TypedActorRef<Int64>, Int64>(inbox, { r =>
            token.cancel()
            r
        }, Duration.second, cancellation: Some(token))
        @Expect(wasCancelled(p), true)
        @Expect(inbox.hasMessages(), false)
    }

    @TestCase
    public func timeoutWinsBeforeLaterCancellation(): Unit {
        let token = AskCancellationToken()
        let inbox = TestInbox<TypedActorRef<Int64>>("timeout")
        let p = TypedAsk.ask<TypedActorRef<Int64>, Int64>(inbox, { r => r }, Duration.millisecond,
            cancellation: Some(token))
        var timedOut = false
        try { p.get(Duration.second * 5) } catch (_: TypedAskTimeoutException) { timedOut = true }
        @Expect(timedOut, true)
        token.cancel()
        if (inbox.hasMessages()) { inbox.receiveMessage().tell(99) }
        @Expect((p.outcome().getOrThrow().failure().getOrThrow() as TypedAskTimeoutException).isSome(), true)
    }

    @TestCase
    public func cancellationRacesWithAskSubscription(): Unit {
        for (_ in 0..100) {
            let token = AskCancellationToken()
            let inbox = TestInbox<TypedActorRef<Int64>>("subscribe-race")
            let creation = spawn {
                TypedAsk.ask<TypedActorRef<Int64>, Int64>(inbox, { r => r }, Duration.second * 30,
                    cancellation: Some(token))
            }
            let cancellation = spawn { token.cancel() }
            let p = creation.get()
            cancellation.get()
            @Expect(wasCancelled(p), true)
            @Expect(token.isCancellationRequested(), true)
            @Expect(token.cancel(), false)
        }
    }

    @TestCase
    public func replyCancellationRaceCompletesExactlyOnce(): Unit {
        for (_ in 0..100) {
            let token = AskCancellationToken()
            let inbox = TestInbox<TypedActorRef<Int64>>("race")
            let p = TypedAsk.ask<TypedActorRef<Int64>, Int64>(inbox, { r => r }, Duration.second * 30,
                cancellation: Some(token))
            let reply = inbox.receiveMessage()
            let callbacks = AtomicInt64(0)
            p.onComplete({ _ => callbacks.fetchAdd(1); () })
            let cancel = spawn { token.cancel() }
            let success = spawn { reply.tell(7) }
            cancel.get()
            success.get()
            @Expect(p.isCompleted(), true)
            @Expect(callbacks.load(), 1)
            @Expect(wasCancelled(p) || p.value() == Some(7), true)
        }
    }
}
