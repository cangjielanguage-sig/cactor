/*
 * 负例：states 里写了枚举里不存在的状态 —— 必须编译期报错。
 */
package neg_state_machine_unknown_state

import cactor_macros.*

enum OrderState {
    | Idle
    | Running
}

@StateMachine[
    stateType = OrderState,
    states = [Idle, NotExist],
    initial = Idle,
    transitions = [(Idle, NotExist)]
]
class OrderMachine {
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
