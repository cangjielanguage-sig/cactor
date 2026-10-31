/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - DSL 宏等价性与增强功能测试（对照 docs/plan/dsl20261030.md §8.4 / P1）
 *
 * 验证：
 * 1. @actor 宏展开语义 = 手写 receive 等价类（等价性测试，A4）
 * 2. 签名增强：onXxx(msg) 单参 + Unit 返回 → Handled；onXxx(msg, ctx): MessageResult
 * 3. @within / @retry 运行时组合子（P0 假设 B 证伪后的 §8.2 降级方案）
 * 4. 时长字面量与 << 链式发送（回归）
 */
package cactor_tests.unit.dsl

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorRef, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.dsl.*
import cactor_core.typed.*
import cactor_macros.*
import cactor_testkit.*

// ---------- 消息 ----------

class Inc <: Message {
    let n: Int64
    init(n: Int64) { this.n = n }
    public func messageType(): String { "Inc" }
}

class Query <: Message {
    let replyTo: ActorRef
    init(replyTo: ActorRef) { this.replyTo = replyTo }
    public func messageType(): String { "Query" }
}

class Boom <: Message {
    public func messageType(): String { "Boom" }
}

class Done <: Message {
    public func messageType(): String { "Done" }
}

class Reply <: Message {
    let text: String
    init(text: String) { this.text = text }
    public func messageType(): String { "Reply" }
}

// ---------- 手写等价类（与 @actor 版语义一致，供等价性对照） ----------

class CounterManual <: Actor {
    let rec: EventRecorder
    var total: Int64 = 0

    init(rec: EventRecorder) { this.rec = rec }

    public prop name: String { get() { "CounterManual" } }
    public prop description: String { get() { "手写等价类" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case m: Inc =>
                total += m.n
                rec.record("total ${total}")
                MessageResult.Handled
            case m: Query =>
                m.replyTo << Reply("total=${total}")
                rec.record("replied")
                MessageResult.Handled
            case m: Done =>
                rec.record("done")
                MessageResult.Stop
            case _ => MessageResult.Unhandled
        }
    }

    public func postStop(): Unit {
        rec.record("stopped")
    }
}

// ---------- @actor 版（形态 A：混合签名增强） ----------

@actor[]
class CounterDsl <: Actor {
    let rec: EventRecorder
    var total: Int64 = 0

    init(rec: EventRecorder) { this.rec = rec }

    // 单参 + Unit 返回：宏生成 `... ; MessageResult.Handled`
    func onInc(m: Inc) {
        total += m.n
        rec.record("total ${total}")
    }

    // 双参 + MessageResult 返回：宏原样透传返回值
    func onQuery(m: Query, ctx: ActorContext): MessageResult {
        m.replyTo << Reply("total=${total}")
        rec.record("replied")
        MessageResult.Handled
    }

    func onDone(m: Done, ctx: ActorContext): MessageResult {
        rec.record("done")
        MessageResult.Stop
    }

    // postStop 不是 onXxx，宏不会碰它，手写即可
    public func postStop(): Unit {
        rec.record("stopped")
    }
}

// ---------- @within / @retry 运行时组合子（§8.2 降级方案） ----------

@actor[]
class GuardedActor <: Actor {
    let rec: EventRecorder
    var boomCount: Int64 = 0

    init(rec: EventRecorder) { this.rec = rec }

    func onInc(m: Inc, ctx: ActorContext): MessageResult {
        within(3.seconds, "onInc") {
            rec.record("guarded ${m.n}")
            MessageResult.Handled
        }
    }

    // 前 1 次 Unhandled，第 2 次成功 —— 验证重试路径
    func onBoom(m: Boom, ctx: ActorContext): MessageResult {
        retry(3) {
            boomCount += 1
            if (boomCount < 2) {
                rec.record("boom attempt ${boomCount}")
                MessageResult.Unhandled
            } else {
                rec.record("boom recovered at ${boomCount}")
                MessageResult.Handled
            }
        }
    }

    func onDone(m: Done, ctx: ActorContext): MessageResult {
        rec.record("done")
        MessageResult.Stop
    }

    public func postStop(): Unit {
        rec.record("stopped")
    }
}

// ---------- 协议清单（cases）/ 单一协议（protocol）：编译期穷尽性检查 ----------

@actor[cases = [Inc, Query, Done]]
class CounterWithCases <: Actor {
    let rec: EventRecorder
    var total: Int64 = 0

    init(rec: EventRecorder) { this.rec = rec }

    func onInc(m: Inc, ctx: ActorContext) {
        total += m.n
        rec.record("total ${total}")
    }

    func onQuery(m: Query, ctx: ActorContext) {
        m.replyTo << Reply("total=${total}")
        rec.record("replied")
    }

    func onDone(m: Done, ctx: ActorContext): MessageResult {
        rec.record("done")
        MessageResult.Stop
    }

    public func postStop(): Unit {
        rec.record("stopped")
    }
}

// 枚举协议：一个处理方法覆盖整个枚举，枚举内部的穷尽性由仓颉 match 保证
enum CounterCmd <: Message {
    | Add(Int64)
    | Reset

    public func messageType(): String { "CounterCmd" }
}

@actor[protocol = CounterCmd]
class CmdActor <: Actor {
    let rec: EventRecorder
    var total: Int64 = 0

    init(rec: EventRecorder) { this.rec = rec }

    func onCounterCmd(m: CounterCmd, ctx: ActorContext): MessageResult {
        match (m) {
            case CounterCmd.Add(n) =>
                total += n
                rec.record("cmd total ${total}")
                MessageResult.Handled
            case CounterCmd.Reset =>
                total = 0
                rec.record("cmd reset")
                MessageResult.Handled
        }
    }
}

// ---------- 形态 A（宏）vs 形态 B（lambda）行为语义等价 ----------

/** 形态 B：类型化 Actor（Behaviors 组合子 + 尾随 lambda），与形态 A 语义等价 */
func typedCounter(rec: EventRecorder): TypedBehavior<Inc> {
    Behaviors.setup<Inc>({ ctx =>
        let box = CounterBox()
        Behaviors.receive<Inc>({ _, m =>
            box.add(m.n)
            rec.record("total ${box.value()}")
            Behaviors.same<Inc>()
        })
    })
}

/** 可变状态盒子（仓颉闭包不能捕获可变局部变量后逃逸） */
class CounterBox {
    private var v: Int64 = 0

    func add(n: Int64): Unit {
        v += n
    }

    func value(): Int64 {
        v
    }
}

// ---------- 测试 ----------

@Test
public class ActorMacroEquivTest {
    @TestCase
    public func testDslMatchesManualReceive(): Unit {
        // 手写等价类
        let kit1 = ActorTestKit("equiv-manual")
        let rec1 = EventRecorder()
        let a1 = kit1.actorOf({ => CounterManual(rec1) }, "manual")
        a1 << Inc(2) << Inc(3) << Query(a1) << StringMessage("ignored") << Done()
        @Expect(rec1.await("stopped", 3.seconds), true)
        @Expect(rec1.contains("total 5"), true)
        @Expect(rec1.contains("replied"), true)
        kit1.shutdown()

        // @actor 宏版本：同样的消息序列，同样的结果
        let kit2 = ActorTestKit("equiv-dsl")
        let rec2 = EventRecorder()
        let a2 = kit2.actorOf({ => CounterDsl(rec2) }, "dsl")
        a2 << Inc(2) << Inc(3) << Query(a2) << StringMessage("ignored") << Done()
        @Expect(rec2.await("stopped", 3.seconds), true)
        @Expect(rec2.contains("total 5"), true)
        @Expect(rec2.contains("replied"), true)
        kit2.shutdown()
    }

    @TestCase
    public func testGeneratedNameDefaults(): Unit {
        @Expect(CounterDsl(EventRecorder()).name, "CounterDsl")
        @Expect(CounterDsl(EventRecorder()).description, "CounterDsl")
    }

    @TestCase
    public func testWithinCombinator(): Unit {
        let kit = ActorTestKit("within")
        let rec = EventRecorder()
        let g = kit.actorOf({ => GuardedActor(rec) }, "guarded")
        g << Inc(1) << Done()
        @Expect(rec.await("stopped", 3.seconds), true)
        @Expect(rec.contains("guarded 1"), true)
        kit.shutdown()
    }

    @TestCase
    public func testRetryCombinatorRecovers(): Unit {
        let kit = ActorTestKit("retry")
        let rec = EventRecorder()
        let g = kit.actorOf({ => GuardedActor(rec) }, "retrier")
        g << Boom() << Done()
        @Expect(rec.await("stopped", 3.seconds), true)
        @Expect(rec.contains("boom attempt 1"), true)
        @Expect(rec.contains("boom recovered at 2"), true)
        kit.shutdown()
    }

    @TestCase
    public func testWithinCombinatorDirect(): Unit {
        let rec = EventRecorder()
        let r = within(1.nanos, "direct") {
            rec.record("call")
            MessageResult.Handled
        }
        match (r) {
            case MessageResult.Handled => ()
            case _ => @Fail("expected Handled")
        }
        @Expect(rec.snapshot().size, 1)
    }

    @TestCase
    public func testRetryCombinatorExhausted(): Unit {
        let rec = EventRecorder()
        let r = retry(3) {
            rec.record("attempt")
            MessageResult.Unhandled
        }
        match (r) {
            case MessageResult.Unhandled => ()
            case _ => @Fail("expected Unhandled")
        }
        @Expect(rec.snapshot().size, 3)
    }

    @TestCase
    public func testRetryCombinatorEscalatesOnException(): Unit {
        let rec = EventRecorder()
        let r = retry(2) {
            rec.record("attempt")
            throw Exception("boom")
            MessageResult.Handled
        }
        match (r) {
            case MessageResult.Escalate => ()
            case _ => @Fail("expected Escalate")
        }
        @Expect(rec.snapshot().size, 2)
    }

    @TestCase
    public func testCasesProtocolDispatchesAllTypes(): Unit {
        let kit = ActorTestKit("cases")
        let rec = EventRecorder()
        let a = kit.actorOf({ => CounterWithCases(rec) }, "cases")
        a << Inc(2) << Query(a) << Done()
        @Expect(rec.await("stopped", 3.seconds), true)
        @Expect(rec.contains("total 2"), true)
        @Expect(rec.contains("replied"), true)
        kit.shutdown()
    }

    @TestCase
    public func testEnumProtocolHandler(): Unit {
        let kit = ActorTestKit("enum-protocol")
        let rec = EventRecorder()
        let a = kit.actorOf({ => CmdActor(rec) }, "cmd")
        a << CounterCmd.Add(5) << CounterCmd.Reset
        @Expect(rec.await("cmd reset", 3.seconds), true)
        @Expect(rec.contains("cmd total 5"), true)
        kit.shutdown()
    }

    @TestCase
    public func testMacroPathEqualsLambdaPath(): Unit {
        // 形态 A：@actor 宏（经典 Actor）
        let kitA = ActorTestKit("macro-path")
        let recA = EventRecorder()
        let a = kitA.actorOf({ => CounterDsl(recA) }, "dsl")
        a << Inc(2) << Inc(3)
        sleep(200.millis)
        @Expect(recA.snapshot().size, 2)
        @Expect(recA.contains("total 2"), true)
        @Expect(recA.contains("total 5"), true)
        kitA.shutdown()

        // 形态 B：Behaviors + lambda（类型化 Actor）
        let kitB = ActorTestKit("lambda-path")
        let recB = EventRecorder()
        let sys = kitB.system.typed()
        let b = sys.actorOf(typedCounter(recB), "typed")
        b << Inc(2) << Inc(3)
        sleep(200.millis)
        @Expect(recB.snapshot().size, 2)
        @Expect(recB.contains("total 2"), true)
        @Expect(recB.contains("total 5"), true)
        kitB.shutdown()
    }

    @TestCase
    public func testDurationAndSendRegression(): Unit {
        @Expect(100.millis, Duration.millisecond * 100)
        @Expect(3.seconds, Duration.second * 3)
    }
}
