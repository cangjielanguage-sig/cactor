/*
 * CActor - 文档示例可编译性测试（对照 docs/dsl.md）
 *
 * 目的：docs/dsl.md 里的每段代码都必须能编译（避免文档与实现漂移）。
 * 这里把文档中的关键片段原样搬过来，跑通后再改文档就不会写出编译不过的示例。
 */
package cactor_tests.unit.dsl

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.*
import cactor_core.dsl.*
import cactor_core.typed.*
import cactor_macros.*

// ---------- §2.1 枚举协议 ----------

enum CounterMsg <: Message {
    | Increment(Int64)
    | GetCount

    public func messageType(): String {
        "CounterMsg"
    }
}

// ---------- §2.2 @Message 类 ----------

@Message
class StartJob <: Message {
    let jobId: String

    init(jobId: String) {
        this.jobId = jobId
    }
}

// ---------- §3.1 @actor[cases = ...] ----------

class IncrementMsg <: Message {
    let n: Int64

    init(n: Int64) {
        this.n = n
    }

    public func messageType(): String {
        "IncrementMsg"
    }
}

class GetCountMsg <: Message {
    let replyTo: ActorRef

    init(replyTo: ActorRef) {
        this.replyTo = replyTo
    }

    public func messageType(): String {
        "GetCountMsg"
    }
}

class CountIsMsg <: Message {
    let value: Int64

    init(value: Int64) {
        this.value = value
    }

    public func messageType(): String {
        "CountIsMsg"
    }
}

@actor[cases = [IncrementMsg, GetCountMsg]]
class CounterActorDoc <: Actor {
    private var count: Int64 = 0

    func onIncrement(m: IncrementMsg, ctx: ActorContext): MessageResult {
        count += m.n
        MessageResult.Handled
    }

    func onGetCount(m: GetCountMsg, ctx: ActorContext): MessageResult {
        m.replyTo << CountIsMsg(count)
        MessageResult.Handled
    }
}

// ---------- §3.2 三种签名（每种消息类型只能有一个处理方法） ----------

class SigA <: Message {
    public func messageType(): String {
        "SigA"
    }
}

class SigB <: Message {
    public func messageType(): String {
        "SigB"
    }
}

class SigC <: Message {
    public func messageType(): String {
        "SigC"
    }
}

@actor[cases = [SigA, SigB, SigC]]
class DocSignatures <: Actor {
    func onSigA(m: SigA) {
        // 单参 + Unit 返回 → 视为 Handled
    }

    func onSigB(m: SigB, ctx: ActorContext) {
        // 双参 + Unit 返回 → 视为 Handled
    }

    func onSigC(m: SigC, ctx: ActorContext): MessageResult {
        // 双参 + MessageResult 返回 → 原样透传
        MessageResult.Handled
    }
}

// ---------- §3.3 protocol = E ----------

@actor[protocol = CounterMsg]
class CounterProtocolDoc <: Actor {
    private var count: Int64 = 0

    func onCounterMsg(m: CounterMsg, ctx: ActorContext): MessageResult {
        match (m) {
            case CounterMsg.Increment(n) =>
                count += n
            case CounterMsg.GetCount =>
                ()
        }
        MessageResult.Handled
    }
}

// ---------- §4 within / retry ----------

@actor[]
class DocGuarded <: Actor {
    func onPing(m: SigA, ctx: ActorContext): MessageResult {
        within(3.seconds, "onPing") {
            retry(3) {
                MessageResult.Handled
            }
        }
    }
}

// ---------- §5 Behaviors（形态 B） ----------

func docCounterBehavior(): TypedBehavior<CounterMsg> {
    Behaviors.receive<CounterMsg> { ctx, msg =>
        match (msg) {
            case CounterMsg.Increment(n) => Behaviors.same<CounterMsg>()
            case CounterMsg.GetCount => Behaviors.stopped<CounterMsg>()
        }
    }
}

// ---------- §7 @StateMachine ----------

enum DocJobState {
    | Idle
    | Running
    | Stopped
}

@StateMachine[
    stateType = DocJobState,
    states = [Idle, Running, Stopped],
    initial = Idle,
    transitions = [(Idle, Running), (Running, Stopped), (Stopped, Idle)]
]
class DocJobMachine {
}

// ---------- 测试 ----------

@Test
public class DocExamplesTest {
    @TestCase
    public func testActorCasesExample(): Unit {
        let a = CounterActorDoc()
        @Expect(a.name, "CounterActorDoc")
        @Expect(a.description, "CounterActorDoc")
    }

    @TestCase
    public func testSignaturesExample(): Unit {
        @Expect(DocSignatures().name, "DocSignatures")
    }

    @TestCase
    public func testProtocolExample(): Unit {
        @Expect(CounterProtocolDoc().name, "CounterProtocolDoc")
    }

    @TestCase
    public func testWithinRetryExample(): Unit {
        @Expect(DocGuarded().name, "DocGuarded")
    }

    @TestCase
    public func testBehaviorsExample(): Unit {
        // 文档示例能构造出接收行为（本身不是 stopped；解释 GetCount 后才返回 stopped）
        let b = docCounterBehavior()
        @Expect(TypedBehaviors.isStopped(b), false)
    }

    @TestCase
    public func testStateMachineExample(): Unit {
        @Expect(DocJobMachine.stateNames.size, 3)
        @Expect(DocJobMachine.canTransition(DocJobState.Idle, DocJobState.Running), true)
    }

    @TestCase
    public func testMessageResultMappingExample(): Unit {
        @Expect(TypedBehaviors.isSame(messageResultToBehavior<CounterMsg>(MessageResult.Handled)), true)
    }

    @TestCase
    public func testDurationAndSendExample(): Unit {
        @Expect(100.millis, Duration.millisecond * 100)
        @Expect(3.seconds, Duration.second * 3)
    }
}
