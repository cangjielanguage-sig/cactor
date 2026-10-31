/*
 * CActor - @StateMachine 宏测试（对照 docs/plan/dsl20261030.md §9 P4.1 / §12 假设 D）
 *
 * 验证：
 * 1. 宏生成的状态机骨架：状态名、初始状态、允许转换、状态名映射、实例状态迁移；
 * 2. 非法转换在运行时被拒绝（transitionTo 返回 false）；
 * 3. 穷尽 match 由编译器保证（states 少写一个状态 → 编译期报错，见 tools/negative）。
 */
package cactor_tests.unit.dsl

import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.Message
import cactor_macros.*

enum OrderState {
    | Idle
    | Running
    | Stopped
}

@StateMachine[
    stateType = OrderState,
    states = [Idle, Running, Stopped],
    initial = Idle,
    transitions = [(Idle, Running), (Running, Stopped), (Stopped, Idle)]
]
class OrderMachine {
    var orders: Int64 = 0

    func onOrder(): Unit {
        orders += 1
    }
}

@Test
public class StateMachineMacroTest {
    @TestCase
    public func testStateNamesAndInitialState(): Unit {
        @Expect(OrderMachine.stateNames.size, 3)
        @Expect(OrderMachine.stateNames.contains("Idle"), true)
        @Expect(OrderMachine.stateNames.contains("Running"), true)
        @Expect(OrderMachine.stateNames.contains("Stopped"), true)
        @Expect(OrderMachine.stateName(OrderMachine.initialState), "Idle")
    }

    @TestCase
    public func testStateNameMapping(): Unit {
        @Expect(OrderMachine.stateName(OrderState.Idle), "Idle")
        @Expect(OrderMachine.stateName(OrderState.Running), "Running")
        @Expect(OrderMachine.stateName(OrderState.Stopped), "Stopped")
    }

    @TestCase
    public func testAllowedTargets(): Unit {
        @Expect(OrderMachine.allowedTargets(OrderState.Idle).size, 1)
        @Expect(OrderMachine.allowedTargets(OrderState.Idle).contains("Running"), true)
        @Expect(OrderMachine.allowedTargets(OrderState.Running).contains("Stopped"), true)
        @Expect(OrderMachine.allowedTargets(OrderState.Stopped).contains("Idle"), true)
    }

    @TestCase
    public func testCanTransition(): Unit {
        @Expect(OrderMachine.canTransition(OrderState.Idle, OrderState.Running), true)
        @Expect(OrderMachine.canTransition(OrderState.Idle, OrderState.Stopped), false)
        @Expect(OrderMachine.canTransition(OrderState.Running, OrderState.Idle), false)
    }

    @TestCase
    public func testInstanceTransitionRejectsIllegalMove(): Unit {
        let m = OrderMachine()
        @Expect(OrderMachine.stateName(m.currentState()), "Idle")
        @Expect(m.transitionTo(OrderState.Running), true)
        @Expect(OrderMachine.stateName(m.currentState()), "Running")
        // Running -> Idle 不在转换表里，必须被拒绝且状态不变
        @Expect(m.transitionTo(OrderState.Idle), false)
        @Expect(OrderMachine.stateName(m.currentState()), "Running")
        @Expect(m.transitionTo(OrderState.Stopped), true)
        @Expect(OrderMachine.stateName(m.currentState()), "Stopped")
    }
}
