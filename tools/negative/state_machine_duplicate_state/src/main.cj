/*
 * 负例：states 里同一状态写两遍 —— 必须由宏在编译期报错（否则生成的 match 只会出现不可达分支告警）。
 */
package neg_state_machine_duplicate_state

import cactor_macros.*

enum OrderState {
    | Idle
    | Running
}

@StateMachine[
    stateType = OrderState,
    states = [Idle, Running, Idle],
    initial = Idle,
    transitions = [(Idle, Running)]
]
class OrderMachine {
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
