/*
 * CActor Runtime - 批量投递 tellBatch（AR3 / A3）
 *
 * 覆盖：
 *   1. tellBatchPreservesOrder：1000 条批量消息按顺序恰好处理一次；
 *   2. tellBatchInterleavesWithTell：tell 与 tellBatch 混用时整体顺序不变；
 *   3. typedTellBatchPreservesOrder：类型化引用走装箱后的批量路径，顺序一致；
 *   4. 空批次是 no-op。
 */
package cactor_tests.unit.runtime.semantics

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.typed.{TypedBehaviors}
import cactor_testkit.*

class BatchRecorder <: Actor {
    let rec = EventRecorder()

    public prop name: String {
        get() { "batch-recorder" }
    }

    public prop description: String {
        get() { "batch recorder" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage => rec.record(s.getContent())
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }
}

@Test
public class TellBatchTest {
    @TestCase
    public func tellBatchPreservesOrder(): Unit {
        let kit = ActorTestKit("tell-batch-order")
        let actor = BatchRecorder()
        let ref = kit.actorOf({ => actor }, "batch-order")
        let messages = Array<Message>(1000, { i => StringMessage("m${i}") })
        ref.tellBatch(messages)
        @Expect(awaitCondition(Duration.second * 10, { => actor.rec.size() == 1000 }), true)
        let got = actor.rec.snapshot()
        @Expect(got.size, 1000)
        for (i in 0..1000) {
            @Expect(got[i], "m${i}")
        }
        kit.shutdown()
    }

    @TestCase
    public func tellBatchInterleavesWithTell(): Unit {
        let kit = ActorTestKit("tell-batch-mix")
        let actor = BatchRecorder()
        let ref = kit.actorOf({ => actor }, "batch-mix")
        ref.tell(StringMessage("a"))
        ref.tellBatch([StringMessage("b"), StringMessage("c")])
        ref.tell(StringMessage("d"))
        @Expect(awaitCondition(Duration.second * 5, { => actor.rec.size() == 4 }), true)
        let got = actor.rec.snapshot()
        @Expect(got[0], "a")
        @Expect(got[1], "b")
        @Expect(got[2], "c")
        @Expect(got[3], "d")
        // 空批次不影响任何状态
        ref.tellBatch(Array<Message>())
        @Expect(awaitCondition(Duration.second * 5, { => actor.rec.size() == 4 }), true)
        kit.shutdown()
    }

    @TestCase
    public func typedTellBatchPreservesOrder(): Unit {
        let kit = ActorTestKit("tell-batch-typed")
        let rec = EventRecorder()
        let typed = kit.spawnTyped(TypedBehaviors.receive<StringMessage>({ _, msg =>
            rec.record(msg.getContent())
            TypedBehaviors.same<StringMessage>()
        }), "typed-batch")
        typed.tellBatch(Array<StringMessage>(200, { i => StringMessage("t${i}") }))
        @Expect(awaitCondition(Duration.second * 10, { => rec.size() == 200 }), true)
        let got = rec.snapshot()
        for (i in 0..200) {
            @Expect(got[i], "t${i}")
        }
        kit.shutdown()
    }
}
