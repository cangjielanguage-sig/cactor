/*
 * 负例：转换表引用了不在 states 清单里的状态 —— 宏直接报错。
 */
package neg_state_machine_bad_transition

import cactor_macros.*

enum OrderState {
    | Idle
    | Running
}

@StateMachine[
    stateType = OrderState,
    states = [Idle],
    initial = Idle,
    transitions = [(Idle, Running)]
]
class OrderMachine {
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
