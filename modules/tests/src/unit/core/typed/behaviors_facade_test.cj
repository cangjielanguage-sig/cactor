/*
 * CActor - 行为层门面测试（P3：Behaviors 收敛）
 *
 * 验证：
 * 1. cactor_core.dsl.Behaviors 门面与 TypedBehaviors 语义一致（同一份实现）；
 * 2. MessageResult（形态 A）与 TypedBehavior（形态 B）的语义映射一致。
 */
package cactor_tests.unit.core.typed

import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.dsl.*
import cactor_core.typed.*
import cactor_core.message.MessageResult

@Test
public class BehaviorsFacadeTest {
    @TestCase
    public func testFacadeReceiveWorks(): Unit {
        let ctx = TestCtx<Int64>()
        let ctr = Counter()
        let b = Behaviors.receive<Int64>({ _, m =>
            ctr.add(Int32(m))
            Behaviors.same<Int64>()
        })
        b.onMessage(3, ctx)
        b.onMessage(4, ctx)
        @Expect(ctr.value(), 7)
    }

    @TestCase
    public func testFacadeCombinators(): Unit {
        @Expect(TypedBehaviors.isSame(Behaviors.same<Int64>()), true)
        @Expect(TypedBehaviors.isUnhandled(Behaviors.unhandled<Int64>()), true)
        @Expect(TypedBehaviors.isStopped(Behaviors.stopped<Int64>()), true)
        @Expect(TypedBehaviors.isSame(TypedBehaviors.same<Int64>()), true)
    }

    @TestCase
    public func testMessageResultMapping(): Unit {
        @Expect(TypedBehaviors.isSame(messageResultToBehavior<Int64>(MessageResult.Handled)), true)
        @Expect(TypedBehaviors.isUnhandled(messageResultToBehavior<Int64>(MessageResult.Unhandled)), true)
        @Expect(TypedBehaviors.isStopped(messageResultToBehavior<Int64>(MessageResult.Stop)), true)
    }

    @TestCase
    public func testFacadeReceiveMessage(): Unit {
        let ctx = TestCtx<Int64>()
        let ctr = Counter()
        let b = Behaviors.receiveMessage<Int64>({ m =>
            ctr.add(Int32(m))
            Behaviors.same<Int64>()
        })
        b.onMessage(5, ctx)
        @Expect(ctr.value(), 5)
    }
}
