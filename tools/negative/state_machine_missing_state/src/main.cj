/*
 * 负例：states 少写一个状态 —— 生成的穷尽 match 不完整，必须编译期报错。
 */
package neg_state_machine_missing_state

import cactor_macros.*

enum OrderState {
    | Idle
    | Running
    | Stopped
}

@StateMachine[
    stateType = OrderState,
    states = [Idle, Running],
    initial = Idle,
    transitions = [(Idle, Running)]
]
class OrderMachine {
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
