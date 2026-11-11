/*
 * CActor - 类型化引用的跨进程能力（E63）
 *
 *   - 类型化 tell 过线（Message 载荷）：`TypedPayloadBoxCodec` 嵌套一帧完整消息传输，
 *     接收侧由用户包的桥接闭包完成 `as T`。
 *   - 类型化 tell 过线（普通 enum 载荷）：用 TypedPayloadCodecs 注册一对编解码闭包，
 *     这是类型化协议最常见的形态——枚举本来就不实现 Message。
 *   - 集群版 Receptionist 的类型化入口：`registerTyped(ServiceKey<T>, TypedActorRef<T>)` /
 *     `findTyped(ServiceKey<T>)`，找到的引用在本节点是本地引用、在其他节点是远程类型化引用。
 *
 * 两个用例都用真实回环 TCP + 真实成员协议；断言用 EventRecorder 轮询，不写固定 sleep。
 */
package cactor_tests.unit.cluster

import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import std.convert.*
import cactor_core.message.{Message, MessageResult, StringMessage, DeserializationException}
import std.sync.*
import cactor_core.supervision.{BackoffPolicy}
import cactor_core.actor.{ActorRef}
import cactor_core.typed.*
import cactor_testkit.*
import cactor_persistence.{MemoryDurableStateStore}
import cactor_persistence.typed.{PersistenceId}
import cactor_persistence.reminders.*

/** 类型化协议最常见的形态：不实现 Message 的普通 enum */
enum Traffic {
    | Ping(String)
    | Count(Int64)
}

func encodeTraffic(m: Traffic): Array<UInt8> {
    let out = ArrayList<UInt8>()
    match (m) {
        case Traffic.Ping(text) =>
            out.add(1u8)
            for (b in text.toArray()) {
                out.add(b)
            }
        case Traffic.Count(n) =>
            out.add(2u8)
            for (b in n.toString().toArray()) {
                out.add(b)
            }
    }
    out.toArray()
}

func decodeTraffic(b: Array<UInt8>): Traffic {
    if (b.size < 1) {
        throw DeserializationException("Traffic 载荷为空")
    }
    let text = String.fromUtf8(b.slice(1, b.size - 1))
    match (b[0]) {
        case 1u8 => Traffic.Ping(text)
        case 2u8 => Traffic.Count(Int64.parse(text))
        case _ => throw DeserializationException("未知 Traffic 种类")
    }
}

func trafficRecorder(rec: EventRecorder): TypedBehavior<Traffic> {
    TypedBehaviors.receive<Traffic>({ _, msg =>
        match (msg) {
            case Traffic.Ping(text) => rec.record("ping:${text}")
            case Traffic.Count(n) => rec.record("count:${n}")
        }
        TypedBehaviors.same<Traffic>()
    })
}

/** 收到 StringMessage 就记一笔，便于断言"确实在目标节点上被处理了" */
func typedRecorder(label: String, rec: EventRecorder): TypedBehavior<StringMessage> {
    TypedBehaviors.receiveMessage<StringMessage>({ msg =>
        rec.record("${label}:${msg.getContent()}")
        TypedBehaviors.same<StringMessage>()
    })
}

/** 收到请求就回一条 StringMessage（走经典上下文的 reply，回复引用来自信封 sender） */
func replyingRecorder(rec: EventRecorder): TypedBehavior<StringMessage> {
    TypedBehaviors.receive<StringMessage>({ ctx, msg =>
        let text = "pong:${msg.getContent()}"
        match (ctx.toClassic()) {
            case Some(classic) => classic.reply(StringMessage(text))
            case None => ()
        }
        rec.record(text)
        TypedBehaviors.same<StringMessage>()
    })
}

/** 只记录不回复：用于验证跨节点 ask 的超时 */
func silentRecorder(rec: EventRecorder): TypedBehavior<StringMessage> {
    TypedBehaviors.receiveMessage<StringMessage>({ msg =>
        rec.record("silent:${msg.getContent()}")
        TypedBehaviors.same<StringMessage>()
    })
}

@Test
public class TypedRemoteTest {
    /** 一个节点暴露类型化 Actor，另一个节点用 typedRemoteActorOf 拿到远程类型化引用并 tell */
    @TestCase
    public func typedTellCrossesNodes(): Unit {
        let a = CNode("tr-a")
        let b = CNode("tr-b")
        a.join([a.addr()], FAST)
        b.join([a.addr()], FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)

        let rec = EventRecorder()
        let behaviour = typedRecorder("b", rec)
        let refB = b.kit.spawnTyped<StringMessage>(behaviour, "typed-echo")
        match (refB.toClassic()) {
            case Some(classic) => b.remote.exposeActor("typed-echo", classic)
            case None => @Fail("spawnTyped 应当返回由经典 Actor 支撑的引用")
        }

        let refA = a.remote.typedRemoteActorOf<StringMessage>(b.addr(), "typed-echo")
        @Expect(refA.tell(StringMessage("跨节点")), ())
        @Expect(rec.await("b:跨节点", CLUSTER_WAIT), true)

        // 载荷不是 Message 时发送侧立即失败，而不是悄悄丢包
        a.close()
        b.close()
    }

    /** 跨节点类型化 ask：回复引用走信封 sender，请求消息里不需要带引用 */
    @TestCase
    public func typedAskCrossesNodes(): Unit {
        let a = CNode("tq-a")
        let b = CNode("tq-b")
        a.join([a.addr()], FAST)
        b.join([a.addr()], FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)

        let rec = EventRecorder()
        let refB = b.kit.spawnTyped<StringMessage>(replyingRecorder(rec), "typed-ask")
        match (refB.toClassic()) {
            case Some(classic) => b.remote.exposeActor("typed-ask", classic)
            case None => @Fail("spawnTyped 应当返回由经典 Actor 支撑的引用")
        }

        let refA = a.remote.typedRemoteActorOf<StringMessage>(b.addr(), "typed-ask")
        let p = TypedAsk.askRemote<StringMessage, StringMessage>(refA, StringMessage("ping"), CLUSTER_WAIT)
        @Expect(p.get(CLUSTER_WAIT).getContent(), "pong:ping")
        @Expect(rec.await("pong:ping", CLUSTER_WAIT), true)

        a.close()
        b.close()
    }

    /** 目标不回复时，跨节点 ask 以 TypedAskTimeoutException 失败，而不是永久挂起 */
    @TestCase
    public func typedAskTimesOutWithoutReply(): Unit {
        let a = CNode("tqt-a")
        let b = CNode("tqt-b")
        a.join([a.addr()], FAST)
        b.join([a.addr()], FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)

        let rec = EventRecorder()
        let refB = b.kit.spawnTyped<StringMessage>(silentRecorder(rec), "typed-silent")
        match (refB.toClassic()) {
            case Some(classic) => b.remote.exposeActor("typed-silent", classic)
            case None => @Fail("spawnTyped 应当返回由经典 Actor 支撑的引用")
        }

        let refA = a.remote.typedRemoteActorOf<StringMessage>(b.addr(), "typed-silent")
        let p = TypedAsk.askRemote<StringMessage, StringMessage>(refA, StringMessage("ping"), Duration.millisecond * 500)
        var timedOut = false
        try {
            p.get(Duration.second * 5)
        } catch (e: TypedAskTimeoutException) {
            timedOut = true
        }
        @Expect(timedOut, true)
        // 请求确实到了对端（不是超时掩盖了丢包）
        @Expect(rec.await("silent:ping", CLUSTER_WAIT), true)

        a.close()
        b.close()
    }

    /** 取消发起方等待后，远端仍可处理请求；迟到回复不改变结果，回复入口最终清理。 */
    @TestCase
    public func typedRemoteAskCancellationReleasesReplyEntry(): Unit {
        let a = CNode("tqc-a")
        let b = CNode("tqc-b")
        try {
            a.join([a.addr()], FAST)
            b.join([a.addr()], FAST)
            @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)
            let senders = TestInbox<ActorRef>("held-senders")
            let received = EventRecorder()
            let behavior = TypedBehaviors.receive<StringMessage>({ ctx, msg =>
                if (let Some(sender) <- ctx.toClassic().getOrThrow().senderRef()) {
                    senders.tell(sender)
                }
                received.record(msg.getContent())
                TypedBehaviors.same<StringMessage>()
            })
            let target = b.kit.spawnTyped<StringMessage>(behavior, "cancel-ask")
            b.remote.exposeActor("cancel-ask", target.toClassic().getOrThrow())
            let remote = a.remote.typedRemoteActorOf<StringMessage>(b.addr(), "cancel-ask")
            let before = a.remote.replyEntryCount()
            let token = AskCancellationToken()
            let p = TypedAsk.askRemote<StringMessage, StringMessage>(remote, StringMessage("held"),
                Duration.second * 30, cancellation: Some(token))
            @Expect(received.await("held", CLUSTER_WAIT), true)
            @Expect(senders.hasMessages(), true)
            @Expect(a.remote.replyEntryCount() > before, true)
            @Expect(token.cancel(), true)
            var cancelled = false
            try { p.get(CLUSTER_WAIT) } catch (_: TypedAskCancelledException) { cancelled = true }
            @Expect(cancelled, true)
            senders.receiveMessage().tell(StringMessage("late"))
            @Expect(awaitCondition(CLUSTER_WAIT, { => a.remote.replyEntryCount() == before }), true)
            @Expect((p.outcome().getOrThrow().failure().getOrThrow() as TypedAskCancelledException).isSome(), true)
            // 已取消令牌不会再发一条网络请求或登记回复入口。
            let skipped = TypedAsk.askRemote<StringMessage, StringMessage>(remote, StringMessage("skipped"),
                Duration.second, cancellation: Some(token))
            @Expect((skipped.outcome().getOrThrow().failure().getOrThrow() as TypedAskCancelledException).isSome(), true)
            @Expect(a.remote.replyEntryCount(), before)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 首次响应丢失后重试同一个幂等读请求，第二次沿真实 TCP 回复。 */
    @TestCase
    public func typedRemoteAskRetriesAfterMissingReply(): Unit {
        let a = CNode("retry-a")
        let b = CNode("retry-b")
        try {
            a.join([a.addr()], FAST)
            b.join([a.addr()], FAST)
            @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)
            let calls = AtomicInt64(0)
            let behavior = TypedBehaviors.receive<StringMessage>({ ctx, _ =>
                if (calls.fetchAdd(1) > 0) {
                    ctx.toClassic().getOrThrow().reply(StringMessage("read-result"))
                }
                TypedBehaviors.same<StringMessage>()
            })
            let target = b.kit.spawnTyped<StringMessage>(behavior, "retry-read")
            b.remote.exposeActor("retry-read", target.toClassic().getOrThrow())
            let remote = a.remote.typedRemoteActorOf<StringMessage>(b.addr(), "retry-read")
            let token = AskCancellationToken()
            let retried = TypedRetry.run<StringMessage>({ _ =>
                TypedAsk.askRemote<StringMessage, StringMessage>(remote, StringMessage("read"),
                    Duration.second, cancellation: Some(token))
            }, 3, BackoffPolicy(Duration.millisecond * 10, Duration.millisecond * 50, randomFactor: 0.0),
                cancellation: Some(token), shouldRetry: { e => (e as TypedAskTimeoutException).isSome() })
            @Expect(retried.get(CLUSTER_WAIT).getContent(), "read-result")
            @Expect(calls.load(), 2)
            token.cancel()
            @Expect(retried.get(CLUSTER_WAIT).getContent(), "read-result")
        } finally {
            a.close()
            b.close()
        }
    }

    /** 远端已提交但回复丢失：提醒重发同一去重键，业务只提交一次，确认后才删除。 */
    @TestCase
    public func durableReminderRedeliversAcrossTcpUntilBusinessAcknowledges(): Unit {
        let a = CNode("reminder-a")
        let b = CNode("reminder-b")
        try {
            a.join([a.addr()], FAST)
            b.join([a.addr()], FAST)
            @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)
            let deliveries = AtomicInt64(0)
            let commits = AtomicInt64(0)
            let seen = EventRecorder()
            let business = b.kit.spawnTyped<StringMessage>(TypedBehaviors.receive<StringMessage>({ ctx, msg =>
                if (!seen.contains(msg.getContent())) {
                    seen.record(msg.getContent())
                    commits.fetchAdd(1)
                }
                if (deliveries.fetchAdd(1) > 0) {
                    ctx.toClassic().getOrThrow().reply(StringMessage("committed"))
                }
                TypedBehaviors.same<StringMessage>()
            }), "reminder-business")
            b.remote.exposeActor("reminder-business", business.toClassic().getOrThrow())
            let remote = a.remote.typedRemoteActorOf<StringMessage>(b.addr(), "reminder-business")
            let pending = TestInbox<ReminderDelivery>("reminder-deliveries")
            let reminders = a.kit.spawnTyped<ReminderCommand>(DurableReminders.behavior(PersistenceId("tcp-reminder"),
                MemoryDurableStateStore(), { d => pending.tell(d) }, pollInterval: Duration.hour,
                nowMillis: { => 100 }), "reminders")
            let generation = TypedAsk.ask<ReminderCommand, Int64>(reminders, { r =>
                ReminderCommand.Register(ReminderDefinition("work", "payload", 100), r)
            }, CLUSTER_WAIT).get(CLUSTER_WAIT)
            reminders.tell(ReminderCommand.Tick)
            @Expect(awaitCondition(CLUSTER_WAIT, { => pending.hasMessages() }), true)
            let first = pending.receiveMessage()
            let key = "tcp-reminder:${first.definition.name}:${first.generation}:${first.definition.dueAtMillis}"
            let lost = TypedAsk.askRemote<StringMessage, StringMessage>(remote, StringMessage(key), Duration.second)
            @ExpectThrows[TypedAskTimeoutException](lost.get(CLUSTER_WAIT))
            @Expect(commits.load(), 1)
            reminders.tell(ReminderCommand.Tick)
            @Expect(awaitCondition(CLUSTER_WAIT, { => pending.hasMessages() }), true)
            let repeated = pending.receiveMessage()
            @Expect(repeated.generation, generation)
            @Expect(repeated.definition.dueAtMillis, first.definition.dueAtMillis)
            let repeatedKey = "tcp-reminder:${repeated.definition.name}:${repeated.generation}:${repeated.definition.dueAtMillis}"
            @Expect(TypedAsk.askRemote<StringMessage, StringMessage>(remote, StringMessage(repeatedKey),
                CLUSTER_WAIT).get(CLUSTER_WAIT).getContent(), "committed")
            @Expect(commits.load(), 1)
            @Expect(deliveries.load(), 2)
            @Expect(TypedAsk.ask<ReminderCommand, Bool>(reminders, { r => ReminderCommand.Acknowledge(
                repeated.definition.name, repeated.generation, repeated.definition.dueAtMillis, r)
            }, CLUSTER_WAIT).get(CLUSTER_WAIT), true)
            @Expect(TypedAsk.ask<ReminderCommand, Array<ReminderDelivery>>(reminders,
                { r => ReminderCommand.Get(r) }, CLUSTER_WAIT).get(CLUSTER_WAIT).size, 0)
        } finally {
            a.close()
            b.close()
        }
    }

    /** 普通 enum 协议（不实现 Message）经命名编解码器跨节点传输 */
    @TestCase
    public func enumProtocolCrossesNodes(): Unit {
        ensureTypedPayloadCodecRegistered()
        if (TypedPayloadCodecs.find("tests.Traffic").isNone()) {
            TypedPayloadCodecs.register<Traffic>(
                typeName: "tests.Traffic",
                encode: { m => encodeTraffic(m) },
                decode: { b => decodeTraffic(b) })
        }
        let a = CNode("te-a")
        let b = CNode("te-b")
        a.join([a.addr()], FAST)
        b.join([a.addr()], FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp2([a, b], 2) }), true)

        let rec = EventRecorder()
        let refB = b.kit.spawnTyped<Traffic>(trafficRecorder(rec), "typed-enum")
        match (refB.toClassic()) {
            case Some(classic) => b.remote.exposeActor("typed-enum", classic)
            case None => @Fail("spawnTyped 应当返回由经典 Actor 支撑的引用")
        }

        let refA = a.remote.typedRemoteActorOf<Traffic>(b.addr(), "typed-enum")
        refA.tell(Traffic.Ping("枚举过线"))
        refA.tell(Traffic.Count(42))
        @Expect(rec.await("ping:枚举过线", CLUSTER_WAIT), true)
        @Expect(rec.await("count:42", CLUSTER_WAIT), true)

        a.close()
        b.close()
        TypedPayloadCodecs.unregister("tests.Traffic")
    }

    /** 协议别名随发现引用过线；相同载荷类型、错误协议名也必须拒收。 */
    @TestCase
    public func typedReceptionistChecksDeclaredProtocolAcrossNodes(): Unit {
        let rs = receptionistCluster("typed-tag", FAST)
        try {
            let rec = EventRecorder()
            let key = ServiceKey<StringMessage>("tagged-echo", protocolTypeName: Some("tests.Echo.v1"))
            let ref = rs[1].node.kit.system.typed().actorOf<StringMessage>(typedRecorder("n1", rec),
                "tagged-echo", TypedProps().withProtocolTypeName("tests.Echo.v1"))
            let registrationMismatch = ServiceKey<StringMessage>("tagged-echo", protocolTypeName: Some("tests.Other.v1"))
            var refused = false
            try {
                rs[1].receptionist.registerTyped<StringMessage>(registrationMismatch, ref)
            } catch (_: IllegalArgumentException) {
                refused = true
            }
            @Expect(refused, true)
            @Expect(rs[1].receptionist.localRegistrationCount(), 0)
            rs[1].receptionist.registerTyped<StringMessage>(key, ref)
            let probe = rs[0].receptionist
            @Expect(awaitCondition(CLUSTER_WAIT, { => probe.findTyped<StringMessage>(key).size == 1 }), true)
            let before = TypedTypeExpectations.mismatchCount()
            let wrong = probe.findTyped<StringMessage>(registrationMismatch)[0]
            wrong.tell(StringMessage("拒收"))
            @Expect(awaitCondition(CLUSTER_WAIT, { => TypedTypeExpectations.mismatchCount() > before }), true)
            probe.findTyped<StringMessage>(key)[0].tell(StringMessage("接受"))
            @Expect(rec.await("n1:接受", CLUSTER_WAIT), true)
            @Expect(rec.snapshot().contains("n1:拒收"), false)
            @Expect(TypedTypeExpectations.lastMismatchText().contains("tests.Other.v1"), true)
        } finally {
            closeAll([rs[0].node, rs[1].node, rs[2].node])
        }
    }

    /** 集群 Receptionist 的类型化入口：跨节点 register/find，find 到的引用可以直接 tell */
    @TestCase
    public func typedReceptionistFindsAndTellsAcrossNodes(): Unit {
        let rs = receptionistCluster("typed-rcpt", FAST)
        let rec = EventRecorder()
        let key = ServiceKey<StringMessage>("typed-echo")

        let ref1 = rs[1].node.kit.spawnTyped<StringMessage>(typedRecorder("n1", rec), "typed-echo")
        let ref2 = rs[2].node.kit.spawnTyped<StringMessage>(typedRecorder("n2", rec), "typed-echo")
        rs[1].receptionist.registerTyped<StringMessage>(key, ref1)
        rs[2].receptionist.registerTyped<StringMessage>(key, ref2)
        @Expect(rs[2].receptionist.localRegistrationCount(), 1)

        // lambda 不能捕获 var，改用 count() 轮询（每次查询都会重新走一遍 find）
        let probe = rs[0].receptionist
        @Expect(awaitCondition(CLUSTER_WAIT, { => probe.findTyped<StringMessage>(key).size == 2 }), true)
        let found = probe.findTyped<StringMessage>(key)
        @Expect(found.size, 2)

        for (r in found) {
            r.tell(StringMessage("hi"))
        }
        @Expect(rec.await("n1:hi", CLUSTER_WAIT), true)
        @Expect(rec.await("n2:hi", CLUSTER_WAIT), true)

        // 注销后全集群不再返回该实例
        rs[1].receptionist.deregisterTyped<StringMessage>(key, ref1)
        @Expect(awaitCondition(CLUSTER_WAIT, { => rs[0].receptionist.findTyped<StringMessage>(key).size == 1 }), true)

        closeAll([rs[0].node, rs[1].node, rs[2].node])
    }
}
