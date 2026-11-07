/*
 * CActor - 类型化监督按嵌套层级匹配、未处理消息进死信（E36：T9、T11）
 *
 * T9 监督规则挂到行为结构上（对标 Akka Typed 的 Supervisor 拦截器）：
 *   - setup 里的 supervise(...).withLimit(n) 跨重启累计次数：此前每次重启重新执行 setup 都登记一组新规则、
 *     历史为空，withLimit 永远到不了（无限重启），规则表也只增不减
 *   - 嵌套 supervise 由内向外匹配：内层规则先于外层（此前按登记先后，外层先匹配）
 *   - transformMessages 里的 supervise 生效（此前根本没有登记，失败按默认停止）
 * T11 未处理消息（receive 返回 Unhandled、类型化协议之外的消息）以 UnhandledMessage 交给死信订阅者，
 *   另计 unhandledCount，不计入 deadLetterCount（此前只加一个计数，静默消失）
 */
package cactor_tests.unit.runtime.typed_alignment

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.typed.*
import cactor_testkit.*

class T9Boom <: Exception {
    init(msg: String) {
        super(msg)
    }
}

/** 收到 "boom" 抛异常、"skip" 返回 unhandled，其余记录 "<tag>:<msg>"；停止时记 "postStop" */
func t9Worker(rec: EventRecorder, tag: String): TypedBehavior<String> {
    TypedBehaviors.receive<String>({ _, m =>
        if (m == "boom") {
            throw T9Boom(tag)
        }
        if (m == "skip") {
            return TypedBehaviors.unhandled<String>()
        }
        rec.record("${tag}:${m}")
        TypedBehaviors.same<String>()
    }, { _, sig =>
        if (sig is PostStop) {
            rec.record("postStop")
        }
        TypedBehaviors.same<String>()
    })
}

/** 只处理 Go 的经典 Actor：其余消息返回 Unhandled */
class OnlyGo <: Actor {
    public prop name: String {
        get() { "only-go" }
    }

    public prop description: String {
        get() { "only-go" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (message is Go) {
            MessageResult.Handled
        } else {
            MessageResult.Unhandled
        }
    }
}

@Test
public class SupervisionLayersTest {
    @TestCase
    public func superviseInsideSetupHonoursRestartLimit(): Unit {
        let kit = ActorTestKit("t9-setup-limit")
        let rec = EventRecorder()
        let b = TypedBehaviors.setup<String>({ _ =>
            rec.record("setup")
            TypedBehaviors.supervise<String>(t9Worker(rec, "w"))
                .onFailure(TypedSupervisorStrategy.restart().withLimit(2, Duration.minute))
        })
        let ref = kit.system.typedActorOf<String>(b, "limited")
        for (_ in 0..5) {
            ref << "boom"
        }
        ref << "after"
        @Expect(rec.await("postStop", Duration.second * 5), true)
        // 初始 1 次 + 重启 2 次，第 3 次失败超出上限而停止
        @Expect(rec.count("setup"), 3)
        @Expect(rec.contains("w:after"), false)
        kit.shutdown()
    }

    @TestCase
    public func nestedSuperviseMatchesInnermostFirst(): Unit {
        let kit = ActorTestKit("t9-nested")
        let rec = EventRecorder()
        let inner = TypedBehaviors.supervise<String>(t9Worker(rec, "n")).onFailure(TypedSupervisorStrategy.resume())
        let b = TypedBehaviors.supervise<String>(inner).onFailure(TypedSupervisorStrategy.stop())
        let ref = kit.system.typedActorOf<String>(b, "nested")
        ref << "boom" << "after"
        // 内层 resume 先匹配：失败的消息被跳过，Actor 继续处理（外层 stop 先匹配时 Actor 已停止）
        @Expect(rec.await("n:after", Duration.second * 5), true)
        @Expect(rec.contains("postStop"), false)
        kit.shutdown()
    }

    @TestCase
    public func superviseInsideTransformMessagesTakesEffect(): Unit {
        let kit = ActorTestKit("t9-transform")
        let rec = EventRecorder()
        let inner = TypedBehaviors.supervise<String>(t9Worker(rec, "t")).onFailure(TypedSupervisorStrategy.resume())
        let b = TypedBehaviors.transformMessages<Int64, String>(inner, { i =>
            if (i < 0) {
                "boom"
            } else {
                "n${i}"
            }
        })
        let ref = kit.system.typedActorOf<Int64>(b, "transformed")
        ref << -1 << 5
        @Expect(rec.await("t:n5", Duration.second * 5), true)
        @Expect(rec.contains("postStop"), false)
        kit.shutdown()
    }

    @TestCase
    public func restartLimitIsSharedAcrossBehaviorChanges(): Unit {
        // 内层行为每条消息都换成新实例（状态即行为）：规则位置不变，次数照常累计
        let kit = ActorTestKit("t9-behavior-change")
        let rec = EventRecorder()
        let b = TypedBehaviors.supervise<String>(counting(rec, 0))
            .onFailure(TypedSupervisorStrategy.restart().withLimit(1, Duration.minute))
        let ref = kit.system.typedActorOf<String>(b, "changing")
        ref << "a" << "boom" << "b" << "boom" << "c"
        @Expect(rec.await("postStop", Duration.second * 5), true)
        // 第一次失败重启（计数从 0 重来），第二次失败超出上限停止，c 不再处理
        @Expect(rec.snapshot(), ["a#1", "b#1", "postStop"])
        kit.shutdown()
    }

    @TestCase
    public func unhandledMessagesReachDeadLetterSubscribers(): Unit {
        let kit = ActorTestKit("t11-unhandled")
        let rec = EventRecorder()
        let seen = EventRecorder()
        kit.system.subscribeDeadLetters({ m, path =>
            match (m as UnhandledMessage) {
                case Some(u) => seen.record("unhandled@${u.recipient}")
                case None => seen.record("dead@${path}")
            }
        })
        let deadBefore = kit.system.deadLetterCount()
        let unhandledBefore = kit.system.unhandledCount()
        // 1. 经典 Actor 返回 Unhandled
        let classic = kit.actorOf({ => OnlyGo() }, "only-go")
        classic << StringMessage("nope") << Go()
        // 2. 类型化行为返回 unhandled
        let typed = kit.system.typedActorOf<String>(t9Worker(rec, "p"), "proto")
        typed << "skip"
        // 3. 协议之外的消息：经未检查的引用转换把 Int64 发给 String 协议的 Actor
        let wrong = ClassicTypedRef<Int64>((typed as ClassicTypedRef<String>).getOrThrow().classic)
        wrong << 42
        typed << "ok"
        @Expect(rec.await("p:ok", Duration.second * 5), true)
        @Expect(awaitCondition(Duration.second * 5, { => seen.size() >= 3 }), true)
        let events = seen.snapshot()
        var onlyGo = 0
        var proto = 0
        for (e in events) {
            if (e.startsWith("unhandled@") && e.contains("only-go")) {
                onlyGo++
            }
            if (e.startsWith("unhandled@") && e.contains("proto")) {
                proto++
            }
        }
        @Expect(onlyGo, 1)
        @Expect(proto, 2)
        @Expect(kit.system.unhandledCount() - unhandledBefore, 3)
        // 未处理不是"没送达"：死信计数不变
        @Expect(kit.system.deadLetterCount(), deadBefore)
        kit.shutdown()
    }
}

/** 每条非 boom 消息记 "<msg>#<n>" 并换成计数 +1 的新行为 */
func counting(rec: EventRecorder, n: Int64): TypedBehavior<String> {
    TypedBehaviors.receive<String>({ _, m =>
        if (m == "boom") {
            throw T9Boom("counting")
        }
        rec.record("${m}#${n + 1}")
        counting(rec, n + 1)
    }, { _, sig =>
        if (sig is PostStop) {
            rec.record("postStop")
        }
        TypedBehaviors.same<String>()
    })
}
