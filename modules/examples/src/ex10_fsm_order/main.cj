/*
 * CActor Ex 10 - 订单状态机（对标 akka-sample-fsm 的 Buncher / protoactor-go 状态机示例）
 *
 * 业务约束（全部可判定）：
 * - 订单生命周期：new → created → paid → shipped；created/paid 可 → cancelled；
 * - 非法迁移必须被明确拒绝，且返回原因与当前状态（不静默忽略）；
 * - 下单后 300ms 未支付自动取消（超时是由定时器驱动的状态迁移）；
 * - 终态（shipped / cancelled）不能再迁移；
 * - 每个订单一个 Actor，状态互不影响。
 *
 * 组件：
 * - TypedBehaviors.withTimers + receive：状态即行为（state 作为行为参数，迁移返回新行为）
 * - TypedAsk：测试线程与订单 Actor 的请求-响应
 *
 * 自检（退出码 0/1）：
 * 1. new → created → paid → shipped 全程 ok，tracking 正确；
 * 2. 非法迁移被拒：未支付发货、重复支付、发货后取消；
 * 3. 未支付订单在超时后被自动取消；
 * 4. 两个订单互不影响。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex10_fsm_order
 */
package cactor_examples.ex10_fsm_order

import std.collection.*
import std.time.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.*

let ASK = Duration.second * 10
let PAY_TIMEOUT = Duration.millisecond * 300

// ======================= 协议 =======================

enum OrderCmd {
    | Place(String, TypedActorRef<String>)              // orderId
    | Pay(TypedActorRef<String>)
    | Ship(String, TypedActorRef<String>)               // tracking
    | Cancel(TypedActorRef<String>)
    | Status(TypedActorRef<String>)
    | Tick                                              // 支付超时定时器
}

// ======================= 状态机 =======================
//
//   new --Place--> created --Pay--> paid --Ship--> shipped
//                    |               |
//                    +---Cancel------+---> cancelled
//                    +---Tick(300ms)--+
//
// 状态放在行为参数里；每次迁移返回新的 behavior（无可变字段、无锁）。

func ok(state: String): String {
    "ok|${state}"
}

func reject(state: String, why: String): String {
    "reject|${why}|${state}"
}

func orderBehavior(timers: TypedTimerScheduler<OrderCmd>, state: String, tracking: String): TypedBehavior<OrderCmd> {
    TypedBehaviors.receive<OrderCmd>({ _, msg =>
        match (msg) {
            case OrderCmd.Place(orderId, reply) =>
                if (state != "new") {
                    reply.tell(reject(state, "already_placed"))
                    TypedBehaviors.same<OrderCmd>()
                } else {
                    // 下单即开始支付倒计时；支付或取消时取消它
                    timers.startSingleTimer("pay-timeout", OrderCmd.Tick, PAY_TIMEOUT)
                    reply.tell(ok("created"))
                    orderBehavior(timers, "created", tracking)
                }
            case OrderCmd.Pay(reply) =>
                if (state == "created") {
                    timers.cancel("pay-timeout")
                    reply.tell(ok("paid"))
                    orderBehavior(timers, "paid", tracking)
                } else if (state == "paid") {
                    reply.tell(reject(state, "already_paid"))
                    TypedBehaviors.same<OrderCmd>()
                } else if (state == "shipped") {
                    reply.tell(reject(state, "already_shipped"))
                    TypedBehaviors.same<OrderCmd>()
                } else if (state == "cancelled") {
                    reply.tell(reject(state, "already_cancelled"))
                    TypedBehaviors.same<OrderCmd>()
                } else {
                    reply.tell(reject(state, "cannot_pay"))
                    TypedBehaviors.same<OrderCmd>()
                }
            case OrderCmd.Ship(trk, reply) =>
                if (state == "paid") {
                    reply.tell(ok("shipped"))
                    orderBehavior(timers, "shipped", trk)
                } else if (state == "shipped") {
                    reply.tell(reject(state, "already_shipped"))
                    TypedBehaviors.same<OrderCmd>()
                } else {
                    reply.tell(reject(state, "not_paid"))
                    TypedBehaviors.same<OrderCmd>()
                }
            case OrderCmd.Cancel(reply) =>
                if (state == "created" || state == "paid") {
                    timers.cancel("pay-timeout")
                    reply.tell(ok("cancelled"))
                    orderBehavior(timers, "cancelled", tracking)
                } else if (state == "cancelled") {
                    reply.tell(reject(state, "already_cancelled"))
                    TypedBehaviors.same<OrderCmd>()
                } else {
                    reply.tell(reject(state, "already_shipped"))
                    TypedBehaviors.same<OrderCmd>()
                }
            case OrderCmd.Status(reply) =>
                reply.tell("${state}|${tracking}")
                TypedBehaviors.same<OrderCmd>()
            case OrderCmd.Tick =>
                // 只有还在等支付的订单会被超时取消；已支付/已取消的 Tick 是迟到的，忽略
                if (state == "created") {
                    orderBehavior(timers, "cancelled", "timeout")
                } else {
                    TypedBehaviors.same<OrderCmd>()
                }
        }
    })
}

func newOrder(system: CActorSystem, name: String): TypedActorRef<OrderCmd> {
    system.typedActorOf<OrderCmd>(
        TypedBehaviors.withTimers<OrderCmd>({ timers => orderBehavior(timers, "new", "") }), name)
}

// ======================= 调用辅助 =======================

func place(ref: TypedActorRef<OrderCmd>, id: String): String {
    TypedAsk.ask<OrderCmd, String>(ref, { r => OrderCmd.Place(id, r) }, ASK).get(ASK)
}

func pay(ref: TypedActorRef<OrderCmd>): String {
    TypedAsk.ask<OrderCmd, String>(ref, { r => OrderCmd.Pay(r) }, ASK).get(ASK)
}

func ship(ref: TypedActorRef<OrderCmd>, tracking: String): String {
    TypedAsk.ask<OrderCmd, String>(ref, { r => OrderCmd.Ship(tracking, r) }, ASK).get(ASK)
}

func cancel(ref: TypedActorRef<OrderCmd>): String {
    TypedAsk.ask<OrderCmd, String>(ref, { r => OrderCmd.Cancel(r) }, ASK).get(ASK)
}

func status(ref: TypedActorRef<OrderCmd>): String {
    TypedAsk.ask<OrderCmd, String>(ref, { r => OrderCmd.Status(r) }, ASK).get(ASK)
}

func waitState(ref: TypedActorRef<OrderCmd>, want: String, timeout: Duration): Bool {
    let deadline = MonoTime.now() + timeout
    while (MonoTime.now() < deadline) {
        if (status(ref).startsWith("${want}|")) {
            return true
        }
        sleep(Duration.millisecond * 20)
    }
    status(ref).startsWith("${want}|")
}

class Checker {
    var failures = 0

    func check(cond: Bool, what: String): Unit {
        if (cond) {
            println("  ✓ ${what}")
        } else {
            failures++
            println("  ✗ ${what}")
        }
    }
}

main(): Int64 {
    let system = CActorSystemFactory.create("ex10-fsm")
    let c = Checker()

    try {
        // ---- 1. 正常流程 ----
        println("\n[1] new → created → paid → shipped")
        let order = newOrder(system, "order-1")
        c.check(place(order, "o-1") == "ok|created", "Place → created")
        c.check(status(order) == "created|", "Status 反映 created")
        c.check(pay(order) == "ok|paid", "Pay → paid")
        c.check(ship(order, "SF-1001") == "ok|shipped", "Ship → shipped")
        c.check(status(order) == "shipped|SF-1001", "Status 反映 shipped 与运单号")

        // ---- 2. 非法迁移 ----
        println("\n[2] 非法迁移被明确拒绝")
        c.check(pay(order).startsWith("reject|already_shipped"), "发货后再支付被拒")
        c.check(cancel(order).startsWith("reject|already_shipped"), "发货后取消被拒")
        c.check(ship(order, "SF-1002").startsWith("reject|already_shipped"), "重复发货被拒")

        let fresh = newOrder(system, "order-2")
        c.check(ship(fresh, "SF-x").startsWith("reject|not_paid"), "未支付就发货被拒")
        c.check(place(fresh, "o-2") == "ok|created", "Place → created")
        c.check(place(fresh, "o-2").startsWith("reject|already_placed"), "重复下单被拒")
        c.check(pay(fresh) == "ok|paid", "Pay → paid")
        c.check(pay(fresh).startsWith("reject|already_paid"), "重复支付被拒")
        c.check(cancel(fresh) == "ok|cancelled", "paid 可以取消（退款）")
        c.check(cancel(fresh).startsWith("reject|already_cancelled"), "重复取消被拒")

        // ---- 3. 支付超时自动取消 ----
        println("\n[3] 未支付订单在 ${PAY_TIMEOUT.toMilliseconds()} ms 后自动取消")
        let slow = newOrder(system, "order-3")
        c.check(place(slow, "o-3") == "ok|created", "Place → created")
        c.check(status(slow).startsWith("created|"), "等待期内仍是 created")
        let timedOut = waitState(slow, "cancelled", Duration.second * 3)
        c.check(timedOut, "超时后状态变为 cancelled")
        c.check(status(slow) == "cancelled|timeout", "取消原因记为 timeout")
        c.check(pay(slow).startsWith("reject|already_cancelled"), "超时取消后不能再支付")

        // ---- 4. 订单之间互不影响 ----
        println("\n[4] 两个订单互不影响")
        let a = newOrder(system, "order-a")
        let b = newOrder(system, "order-b")
        place(a, "a")
        place(b, "b")
        pay(a)
        c.check(status(a).startsWith("paid|"), "a 已支付")
        c.check(status(b).startsWith("created|"), "b 仍待支付（a 的支付不影响 b）")
        ship(a, "TRK-A")
        c.check(status(a).startsWith("shipped|TRK-A"), "a 已发货")
        c.check(status(b).startsWith("created|"), "b 状态未被 a 影响")
    } finally {
        system.terminate()
    }

    println("\nex10：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
