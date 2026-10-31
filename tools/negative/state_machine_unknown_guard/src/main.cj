/*
 * 负例：命名守卫（守卫表达式）引用了类体里不存在的方法 —— 必须在编译期报错。
 * 期望错误包含："守卫方法 gateOpen"
 */
package neg_state_machine_unknown_guard

import cactor_macros.*

enum OrderState {
    | Idle
    | Running
}

@StateMachine[
    stateType = OrderState,
    states = [Idle, Running],
    initial = Idle,
    transitions = [(Idle, Running, gateOpen)]
]
class OrderMachine {
}

main(): Int64 {
    println("不应编译通过")
    return 0
}
