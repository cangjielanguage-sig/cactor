/*
 * CActor Best 03 - 监督树与容错：策略、重启限次、死亡监视
 *
 * 演示（经典 API：监督树结构演示更直观）：
 * - TypedBehaviors.supervise(b).onFailure(restart().withLimit(n, within))
 * - ctx.watch + TypedTerminated（DeathPact）
 * - 子 Actor 失败 → 策略决定 Restart / Resume / Stop
 *
 * 自检：两次重启后第三次失败导致停止，watcher 收到 TypedTerminated。
 * 运行：cjpm build && ./target/release/bin/cactor_examples.best03_supervision_tree
 */
package cactor_examples.best03_supervision_tree

import std.sync.*
import std.time.*
import cactor.api.{CActorSystemFactory}
import cactor_core.typed.*

enum WorkerMsg {
    | Work(String)
    | Fail
    | StopYourself
}

enum WatcherMsg {
    | Report(String)
}

class Flaky <: Exception {
    init() { super("模拟故障") }
}

/** 每 3 次 Fail 中真正失败一次：1、2 次 Resume 继续，第 3 次失败导致超限停止 */
class Counter {
    var failures: Int64 = 0
}

func flakyWorker(): TypedBehavior<WorkerMsg> {
    TypedBehaviors.setup<WorkerMsg>({ _ =>
        let c = Counter()
        TypedBehaviors.supervise(
            TypedBehaviors.receive<WorkerMsg>({ ctx, m =>
                match (m) {
                    case WorkerMsg.Work(job) =>
                        println("处理 ${job}")
                        TypedBehaviors.same<WorkerMsg>()
                    case WorkerMsg.Fail =>
                        c.failures++
                        println("第 ${c.failures} 次失败")
                        throw Flaky()
                    case WorkerMsg.StopYourself => TypedBehaviors.stopped<WorkerMsg>()
                }
            })
        ).onFailure(TypedSupervisorStrategy.restart().withLimit(2, Duration.second * 10))
    })
}

func watcher(onDone: () -> Unit): TypedBehavior<WatcherMsg> {
    TypedBehaviors.setup<WatcherMsg>({ ctx =>
        let child = ctx.actorOf(flakyWorker(), "flaky")
        ctx.watch(child)
        child << WorkerMsg.Work("a") << WorkerMsg.Fail << WorkerMsg.Work("b") << WorkerMsg.Fail << WorkerMsg.Fail
        TypedBehaviors.receiveMessage<WatcherMsg>({ _ => TypedBehaviors.same<WatcherMsg>() })
            .receiveSignal({ _, sig =>
                if (sig is TypedTerminated) {
                    println("watcher：子 Actor 已停止（${sig.signalName()}）")
                    onDone()
                    TypedBehaviors.same<WatcherMsg>()
                } else {
                    TypedBehaviors.same<WatcherMsg>()
                }
            })
    })
}

main(): Int64 {
    let system = CActorSystemFactory.create("best03")
    let finished = AtomicBool(false)
    system.typedActorOf<WatcherMsg>(watcher({ => finished.store(true) }), "watcher")

    let deadline = MonoTime.now() + Duration.second * 10
    while (!finished.load() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 5)
    }
    let ok = finished.load()
    system.terminate()
    println(if (ok) { "best03：通过" } else { "best03：失败（子 Actor 未按预期停止）" })
    if (ok) { 0 } else { 1 }
}
