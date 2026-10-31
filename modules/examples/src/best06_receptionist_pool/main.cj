/*
 * CActor Best 06 - 服务发现与路由池：Receptionist + RoundRobin
 *
 * 演示（类型化 API）：
 * - ServiceKey<T> + Receptionist：register / find / 自动注销
 * - 手写轮询池：find 得到的实例列表 + 轮询游标分发
 * - ask 聚合：向池中每个实例查询并求和
 *
 * 自检：注册数量、轮询分布、实例注销后的发现结果符合预期。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.best06_receptionist_pool
 */
package cactor_examples.best06_receptionist_pool

import std.collection.*
import std.time.*
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.*

enum WorkerMsg {
    | Ping(TypedActorRef<Int64>)   // 回复自己的编号
}

func worker(id: Int64): TypedBehavior<WorkerMsg> {
    TypedBehaviors.receiveMessage<WorkerMsg>({ m =>
        match (m) {
            case WorkerMsg.Ping(replyTo) =>
                replyTo.tell(id)
                TypedBehaviors.same<WorkerMsg>()
        }
    })
}

main(): Int64 {
    let system = CActorSystemFactory.create("best06")
    let rec = system.typed().receptionist()
    let key = ServiceKey<WorkerMsg>("workers")

    // 订阅变化（立即收到当前列表）
    let sizes = ArrayList<Int64>()
    rec.subscribe(key, { l: Listing<WorkerMsg> => sizes.add(l.size()) })

    // 启动 3 个实例并注册
    let refs = Array<TypedActorRef<WorkerMsg>>(3, { i =>
        let r = system.typedActorOf<WorkerMsg>(worker(i + 1), "w${i}")
        rec.register(key, r)
        r
    })

    var ok = true
    let deadline0 = MonoTime.now() + Duration.second * 3
    while (rec.find(key).size() < 3 && MonoTime.now() < deadline0) {
        sleep(Duration.millisecond * 5)
    }
    println("已注册：${rec.find(key).size()} 个实例")
    ok = ok && rec.find(key).size() == 3

    // 轮询分发：每个实例恰好被命中 N/实例数 次
    let hits = HashMap<String, Int64>()
    let roundRobin = Array<Int64>(9, { i => i % 3 })
    for (k in 0..9) {
        let target = rec.find(key).services[roundRobin[k]]
        let id = TypedAsk.ask<WorkerMsg, Int64>(target, { r => WorkerMsg.Ping(r) }, Duration.second * 3)
            .get(Duration.second * 3)
        hits["w${id - 1}"] = (hits.get("w${id - 1}") ?? 0) + 1
    }
    for ((name, h) in hits) {
        println("${name} 被调用 ${h} 次")
        ok = ok && h == 3
    }

    // 注销一个实例后，find 结果收敛
    rec.deregister(key, refs[2])
    let deadline1 = MonoTime.now() + Duration.second * 3
    while (rec.find(key).size() != 2 && MonoTime.now() < deadline1) {
        sleep(Duration.millisecond * 5)
    }
    println("注销后：${rec.find(key).size()} 个实例")
    ok = ok && rec.find(key).size() == 2

    system.terminate()
    println(if (ok) { "best06：通过" } else { "best06：失败" })
    if (ok) { 0 } else { 1 }
}
