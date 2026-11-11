/*
 * CActor Tests - 类型化远程 ask 的共享预算与停机生命周期。
 * 跨节点用真实 TCP；发送异常通过显式传输注入。
 */
package cactor_tests.unit.remote_semantics

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.ActorRef
import cactor_core.message.StringMessage
import cactor_core.typed.*
import cactor_foundation.network.NetworkAddress
import cactor_remote.*
import cactor_testkit.*

@Test
public class TypedRemoteAskLifecycleTest {
    @TestCase
    public func typedAndClassicShareCapacityWithoutSendingRejectedRequests(): Unit {
        let kit = ActorTestKit("typed-budget")
        let wire = LossyTransport(0.0, 42u64)
        let a = limitedAskNode(1, transport: wire)
        let b = node()
        try {
            let senders = TestInbox<ActorRef>("budget-senders")
            let held = kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ ctx, _ =>
                senders.tell(ctx.toClassic().getOrThrow().senderRef().getOrThrow())
                TypedBehaviors.same<StringMessage>()
            }), "held")
            b.exposeActor("held", held.toClassic().getOrThrow())
            let echoActor = kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ ctx, msg =>
                ctx.toClassic().getOrThrow().reply(StringMessage("echo:${msg.getContent()}"))
                TypedBehaviors.same<StringMessage>()
            }), "echo")
            b.exposeActor("echo", echoActor.toClassic().getOrThrow())
            let classic = a.remoteActorOf(b.getLocalAddress(), "held")
            let typed = a.typedRemoteActorOf<StringMessage>(b.getLocalAddress(), "held")
            let first = a.ask(classic, StringMessage("classic"), Duration.minute)
            @Expect(awaitCondition(T5S, { => senders.hasMessages() }), true)
            let sent = wire.sent.load()
            @ExpectThrows[RemoteAskCapacityException](
                TypedAsk.askRemote<StringMessage, StringMessage>(typed, StringMessage("rejected"), T5S).get(T5S))
            @Expect(wire.sent.load(), sent)
            @Expect(a.pendingTypedAskCount(), 0)
            senders.receiveMessage().tell(StringMessage("classic-ok"))
            first.get(T5S)
            @Expect(awaitCondition(T5S, { => a.pendingAskCount() == 0 }), true)
            let token = AskCancellationToken()
            let second = TypedAsk.askRemote<StringMessage, StringMessage>(typed, StringMessage("typed"),
                Duration.minute, cancellation: Some(token))
            @Expect(awaitCondition(T5S, { => senders.hasMessages() }), true)
            @Expect(a.pendingTypedAskCount(), 1)
            @Expect(a.replyEntryCount(), 1)
            let typedSent = wire.sent.load()
            @ExpectThrows[RemoteAskCapacityException](a.ask(classic, StringMessage("rejected"), T5S).get(T5S))
            @Expect(wire.sent.load(), typedSent)
            token.cancel()
            @ExpectThrows[TypedAskCancelledException](second.get(T5S))
            @Expect(a.pendingTypedAskCount(), 0)
            @Expect(a.replyEntryCount(), 0)
            senders.receiveMessage().tell(StringMessage("late"))
            let echo = a.typedRemoteActorOf<StringMessage>(b.getLocalAddress(), "echo")
            @Expect(TypedAsk.askRemote<StringMessage, StringMessage>(echo, StringMessage("again"), T5S)
                .get(T5S).getContent(), "echo:again")
            @Expect(awaitCondition(T5S, { => a.pendingTypedAskCount() == 0 }), true)
        } finally { a.stop(); b.stop(); kit.shutdown() }
    }

    @TestCase
    public func shutdownCompletesBothKindsAndAllowsTypedCallbackReentry(): Unit {
        let kit = ActorTestKit("typed-stop")
        let a = limitedAskNode(2)
        let b = node()
        try {
            b.exposeActor("silent", kit.actorOf({ => Silent() }, "silent"))
            let target = a.typedRemoteActorOf<StringMessage>(b.getLocalAddress(), "silent")
            let typed = TypedAsk.askRemote<StringMessage, StringMessage>(target, StringMessage("one"), Duration.minute)
            let classic = a.ask(a.remoteActorOf(b.getLocalAddress(), "silent"), StringMessage("two"), Duration.minute)
            let reentered = AtomicBool(false)
            typed.onComplete({ _ =>
                a.stop()
                reentered.store(a.pendingTypedAskCount() == 0 && a.pendingAskCount() == 0 &&
                    TypedAsk.askRemote<StringMessage, StringMessage>(target, StringMessage("stopped"), T5S).isCompleted())
            })
            a.stop()
            @ExpectThrows[IllegalStateException](typed.get(T5S))
            @ExpectThrows[IllegalStateException](classic.get(T5S))
            @Expect(reentered.load(), true)
            @Expect(a.replyEntryCount(), 0)
        } finally { a.stop(); b.stop(); kit.shutdown() }
    }

    @TestCase
    public func timeoutAndLateReplyDoNotConsumeTheNextTypedAsk(): Unit {
        let kit = ActorTestKit("typed-timeout")
        let a = limitedAskNode(1)
        let b = node()
        try {
            let senders = TestInbox<ActorRef>("timeout-senders")
            let held = kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ ctx, _ =>
                senders.tell(ctx.toClassic().getOrThrow().senderRef().getOrThrow())
                TypedBehaviors.same<StringMessage>()
            }), "held")
            b.exposeActor("held", held.toClassic().getOrThrow())
            let target = a.typedRemoteActorOf<StringMessage>(b.getLocalAddress(), "held")
            let first = TypedAsk.askRemote<StringMessage, StringMessage>(target, StringMessage("one"), Duration.millisecond * 100)
            @Expect(awaitCondition(T5S, { => senders.hasMessages() }), true)
            let oldSender = senders.receiveMessage()
            @ExpectThrows[TypedAskTimeoutException](first.get(T5S))
            @Expect(awaitCondition(T5S, { => a.pendingTypedAskCount() == 0 && a.replyEntryCount() == 0 }), true)
            let second = TypedAsk.askRemote<StringMessage, StringMessage>(target, StringMessage("two"), Duration.minute)
            @Expect(awaitCondition(T5S, { => senders.hasMessages() }), true)
            oldSender.tell(StringMessage("stale"))
            senders.receiveMessage().tell(StringMessage("current"))
            @Expect(second.get(T5S).getContent(), "current")
            @Expect(awaitCondition(T5S, { => a.pendingTypedAskCount() == 0 }), true)
        } finally { a.stop(); b.stop(); kit.shutdown() }
    }

    @TestCase
    public func sendFailureInvalidTimeoutAndPreCancellationReleaseEverything(): Unit {
        let wire = AskFailingTransport()
        let a = limitedAskNode(1, transport: wire)
        try {
            let target = a.typedRemoteActorOf<StringMessage>(NetworkAddress("127.0.0.1", 2), "missing")
            for (_ in 0..3) {
                @ExpectThrows[IllegalStateException](
                    TypedAsk.askRemote<StringMessage, StringMessage>(target, StringMessage("fail"), T5S).get(T5S))
                @Expect(a.pendingTypedAskCount(), 0)
                @Expect(a.replyEntryCount(), 0)
            }
            @Expect(wire.sends.load(), 3)
            @ExpectThrows[IllegalArgumentException](
                TypedAsk.askRemote<StringMessage, StringMessage>(target, StringMessage("invalid"), Duration.Zero))
            @ExpectThrows[IllegalArgumentException](
                TypedAsk.askRemote<StringMessage, StringMessage>(target, StringMessage("invalid"), Duration.second * -1))
            let token = AskCancellationToken()
            token.cancel()
            @ExpectThrows[TypedAskCancelledException](TypedAsk.askRemote<StringMessage, StringMessage>(target,
                StringMessage("skip"), T5S, cancellation: Some(token)).get(T5S))
            @Expect(wire.sends.load(), 3)
            a.stop()
            @ExpectThrows[IllegalStateException](
                TypedAsk.askRemote<StringMessage, StringMessage>(target, StringMessage("stopped"), T5S).get(T5S))
            @Expect(a.pendingTypedAskCount(), 0)
        } finally { a.stop() }
    }

    @TestCase
    public func wrongReplyTypeReleasesCapacityAndLocalFallbackStillWorks(): Unit {
        let kit = ActorTestKit("typed-typecheck")
        let a = limitedAskNode(1)
        let b = node()
        try {
            let echo = kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ ctx, msg =>
                ctx.toClassic().getOrThrow().reply(StringMessage(msg.getContent()))
                TypedBehaviors.same<StringMessage>()
            }), "echo")
            b.exposeActor("echo", echo.toClassic().getOrThrow())
            let target = a.typedRemoteActorOf<StringMessage>(b.getLocalAddress(), "echo")
            @ExpectThrows[IllegalStateException](
                TypedAsk.askRemote<StringMessage, Int64>(target, StringMessage("wrong-type"), T5S).get(T5S))
            @Expect(awaitCondition(T5S, { => a.pendingTypedAskCount() == 0 && a.replyEntryCount() == 0 }), true)
            @Expect(TypedAsk.askRemote<StringMessage, StringMessage>(echo, StringMessage("local"), T5S)
                .get(T5S).getContent(), "local")
        } finally { a.stop(); b.stop(); kit.shutdown() }
    }

    @TestCase
    public func localProxyForwardingRetainsTheRemoteAskLifecycle(): Unit {
        let kit = ActorTestKit("typed-forward")
        let a = limitedAskNode(1)
        let b = node()
        try {
            b.exposeActor("silent", kit.actorOf({ => Silent() }, "silent"))
            let remote = a.remoteActorOf(b.getLocalAddress(), "silent")
            let proxy = kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ ctx, msg =>
                remote.tell(TypedPayloadBox(msg), ctx.toClassic().getOrThrow().senderRef().getOrThrow())
                TypedBehaviors.same<StringMessage>()
            }), "proxy")
            let result = TypedAsk.askRemote<StringMessage, StringMessage>(proxy, StringMessage("forwarded"), Duration.minute)
            @Expect(awaitCondition(T5S, { => a.pendingTypedAskCount() == 1 }), true)
            a.stop()
            @ExpectThrows[IllegalStateException](result.get(T5S))
            @Expect(a.pendingTypedAskCount(), 0)
            @Expect(a.replyEntryCount(), 0)
        } finally { a.stop(); b.stop(); kit.shutdown() }
    }

    @TestCase
    public func stoppingRacesWithTypedAdmissionWithoutLeavingWaiters(): Unit {
        for (_ in 0..20) {
            let a = limitedAskNode(1)
            let target = a.typedRemoteActorOf<StringMessage>(NetworkAddress("127.0.0.1", 1), "missing")
            let stopper = spawn { => a.stop() }
            let pending = TypedAsk.askRemote<StringMessage, StringMessage>(target, StringMessage("race"), Duration.minute)
            stopper.get()
            @Expect(pending.isCompleted(), true)
            @ExpectThrows[IllegalStateException](pending.get(T5S))
            @Expect(a.pendingTypedAskCount(), 0)
            @Expect(a.replyEntryCount(), 0)
            a.stop()
        }
    }
}
