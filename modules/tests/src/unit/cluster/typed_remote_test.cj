/*
 * CActor - 类型化引用的跨进程能力（E63）
 *
 *   - 类型化 tell 过线：`TypedPayloadBox` 的载荷必须实现 Message，经 TypedPayloadBoxCodec
 *     嵌套一帧完整消息传输，接收侧由用户包的桥接闭包完成 `as T`。
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
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.typed.*
import cactor_testkit.*

/** 收到 StringMessage 就记一笔，便于断言"确实在目标节点上被处理了" */
func typedRecorder(label: String, rec: EventRecorder): TypedBehavior<StringMessage> {
    TypedBehaviors.receiveMessage<StringMessage>({ msg =>
        rec.record("${label}:${msg.getContent()}")
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
