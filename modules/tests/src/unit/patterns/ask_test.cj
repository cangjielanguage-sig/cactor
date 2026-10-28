/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - Ask 模式测试
 * 验证：请求-响应、超时、回调、并发 ask、只完成一次
 */
package cactor_tests.unit.patterns

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{Actor}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}
import cactor_patterns.ask.{AskPatternManager, AskMessage, AskResponse, AskFuture, AskTimeoutException, Result,
    TypedAskFuture, TypedResult}

/** 收到 AskMessage 后回复 "echo:<内容>"；内容为 "slow" 时不回复 */
class EchoActor <: Actor {
    public prop name: String {
        get() { "EchoActor" }
    }

    public prop description: String {
        get() { "回声" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case ask: AskMessage =>
                match (ask.getOriginalMessage()) {
                    case m: StringMessage =>
                        if (m.getContent() != "slow") {
                            ask.getReplyTo().tell(AskResponse(ask.getRequestId(), StringMessage("echo:${m.getContent()}"), true))
                        }
                    case _ => ()
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

func contentOf(r: Result<Message>): String {
    match (r) {
        case Result.Success(m) =>
            match (m) {
                case s: StringMessage => s.getContent()
                case _ => "<non-string>"
            }
        case Result.Failure(e) => "<failure:${e}>"
    }
}

@Test
public class AskPatternTest {
    @TestCase
    func askReceivesReply(): Unit {
        let system = CActorSystemFactory.create("ask-${MonoTime.now().hashCode()}")
        let echo = system.actorOf({ => EchoActor() }, "echo")
        let manager = AskPatternManager(Duration.second * 2)
        let start = MonoTime.now()
        let result = manager.ask(echo, StringMessage("hello")).get()
        @Expect(contentOf(result), "echo:hello")
        // 通知式等待：应远小于超时时间
        @Expect(MonoTime.now() - start < Duration.second, true)
        @Expect(manager.getPendingRequestCount(), 0)
        system.terminate()
    }

    @TestCase
    func askTimesOutWithoutReply(): Unit {
        let system = CActorSystemFactory.create("ask-timeout-${MonoTime.now().hashCode()}")
        let echo = system.actorOf({ => EchoActor() }, "echo")
        let manager = AskPatternManager(Duration.second * 2)
        let start = MonoTime.now()
        let result = manager.ask(echo, StringMessage("slow"), Duration.millisecond * 200).get()
        let elapsed = MonoTime.now() - start
        let timedOut = match (result) {
            case Result.Failure(e) => e is AskTimeoutException
            case _ => false
        }
        @Expect(timedOut, true)
        @Expect(elapsed >= Duration.millisecond * 190, true)
        @Expect(elapsed < Duration.second, true)
        system.terminate()
    }

    @TestCase
    func onCompleteCallbackFiresOnce(): Unit {
        let future = AskFuture<Message>("req-1", Duration.second)
        let calls = AtomicInt64(0)
        future.onComplete({ _ => calls.fetchAdd(1) })
        future.complete(StringMessage("first"))
        future.complete(StringMessage("second"))
        future.completeWithError(Exception("ignored"))
        @Expect(calls.load(), 1)
        @Expect(contentOf(future.get()), "first")
        // 完成后注册的回调立即执行
        future.onComplete({ _ => calls.fetchAdd(1) })
        @Expect(calls.load(), 2)
    }

    @TestCase
    func waiterIsWokenByCompletion(): Unit {
        let future = TypedAskFuture<Int64>("typed-1", Duration.second * 5)
        let start = MonoTime.now()
        spawn {
            sleep(Duration.millisecond * 50)
            future.complete(42)
        }
        let r = future.get()
        let value = match (r) {
            case TypedResult.Success(v) => v
            case _ => -1
        }
        @Expect(value, 42)
        @Expect(MonoTime.now() - start < Duration.second, true)
        let late = TypedAskFuture<Int64>("typed-2", Duration.millisecond * 50)
        let isTimeout = match (late.get()) {
            case TypedResult.Timeout(_) => true
            case _ => false
        }
        @Expect(isTimeout, true)
    }

    @TestCase
    func concurrentAsks(): Unit {
        let system = CActorSystemFactory.create("ask-concurrent-${MonoTime.now().hashCode()}")
        let echo = system.actorOf({ => EchoActor() }, "echo")
        let manager = AskPatternManager(Duration.second * 5)
        let ok = AtomicInt64(0)
        let futures = ArrayList<Future<Unit>>()
        for (t in 0..8) {
            futures.add(spawn {
                for (i in 0..100) {
                    let payload = "${t}-${i}"
                    if (contentOf(manager.ask(echo, StringMessage(payload)).get()) == "echo:${payload}") {
                        ok.fetchAdd(1)
                    }
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        @Expect(ok.load(), 800)
        @Expect(manager.getPendingRequestCount(), 0)
        system.terminate()
    }
}
