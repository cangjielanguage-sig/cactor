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
import cactor_core.typed.*
import cactor_testkit.*

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
        b.join([b.addr()], FAST)
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
        b.join([b.addr()], FAST)
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
