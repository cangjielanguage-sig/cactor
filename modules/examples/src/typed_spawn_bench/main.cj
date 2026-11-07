/*
 * CActor - 类型化 Actor 三条创建路径的消息热路径对比（T16 的验证基准）
 *
 *   A. system.typedActorOf：门面 @Frozen，热路径 hooks 在用户包构造（快路径）
 *   B. ctx.actorOf(setup { ... })：setup 是静态调用的 @Frozen 工厂，在用户包里顺带构造 hooks，子 Actor 取用
 *      （T16 之前子 Actor 全部走 core 内的默认 hooks）
 *   B2. ctx.actorOf(receiveMessage { ... })：行为没有携带 hooks，退回默认 hooks。
 *      ctx 是接口、actorOf 是虚调用，@Frozen 对虚调用无效，实现方无法在用户包里构造 hooks（已实测）
 *   C. TypedSpawn.props(behavior) 不带 hooks：core 内默认 hooks（慢路径基线）
 *   D. 用户代码直接调用 TypedSpawn.hooksFor（对照：静态调用 @Frozen 是否生效）
 *
 * 每条路径：actors 个计数 Actor，senders 个外部线程，每个线程向每个 Actor 各发 perPair 条，计时到全部处理完。
 * 三条路径交替跑 rounds 轮，每条取最好的一轮（减少噪声）。默认负载约 4×4×100k = 160 万条 / 路径 / 轮。
 *
 * 运行：cjpm build && CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.typed_spawn_bench [perPair] [rounds]
 * 注意：基准只在工作区整体构建（-O2）后、机器空闲时有意义
 */
package cactor_examples.typed_spawn_bench

import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import cactor.api.{CActor, CActorSystem}
import cactor_core.typed.*

enum Count {
    | Inc
}

enum ParentCmd {
    | Noop
}

func counter(done: AtomicInt64): TypedBehavior<Count> {
    TypedBehaviors.receiveMessage<Count> { _ =>
        done.fetchAdd(1)
        TypedBehaviors.same<Count>()
    }
}

/** 父 Actor 在 setup 里用 ctx.actorOf 创建子 Actor，把引用交给外部 */
class RefBox {
    let lock = Mutex()
    let refs = ArrayList<TypedActorRef<Count>>()

    func add(r: TypedActorRef<Count>): Unit {
        synchronized(lock) { refs.add(r) }
    }

    func snapshot(): Array<TypedActorRef<Count>> {
        synchronized(lock) { refs.toArray() }
    }
}

func parent(n: Int64, done: AtomicInt64, box: RefBox, viaSetup: Bool): TypedBehavior<ParentCmd> {
    TypedBehaviors.setup<ParentCmd> { ctx =>
        for (i in 0..n) {
            let child = if (viaSetup) {
                TypedBehaviors.setup<Count> { _ => counter(done) }
            } else {
                counter(done)
            }
            box.add(ctx.actorOf<Count>(child, "c${i}"))
        }
        TypedBehaviors.ignore<ParentCmd>()
    }
}

func waitUntil(cond: () -> Bool): Unit {
    let deadline = MonoTime.now() + Duration.second * 120
    while (!cond()) {
        if (MonoTime.now() > deadline) {
            throw IllegalStateException("基准等待超时")
        }
        sleep(Duration.millisecond)
    }
}

/** 返回每秒处理条数 */
func drive(refs: Array<TypedActorRef<Count>>, done: AtomicInt64, senders: Int64, perPair: Int64): Int64 {
    let total = Int64(refs.size) * senders * perPair
    let base = done.load()
    let start = MonoTime.now()
    let futures = ArrayList<Future<Unit>>()
    for (_ in 0..senders) {
        futures.add(spawn {
            for (_ in 0..perPair) {
                for (r in refs) {
                    r.tell(Count.Inc)
                }
            }
        })
    }
    for (f in futures) {
        f.get()
    }
    waitUntil({ => done.load() - base >= total })
    let elapsed = MonoTime.now() - start
    total * 1_000_000 / (elapsed.toMicroseconds() + 1)
}

main(args: Array<String>): Int64 {
    let perPair = if (args.size > 0) { Int64.parse(args[0]) } else { 100_000 }
    let rounds = if (args.size > 1) { Int64.parse(args[1]) } else { 3 }
    let actors = 4
    let senders = 4
    let system = CActor.system("typed-spawn-bench")

    // A：门面 typedActorOf
    let doneA = AtomicInt64(0)
    let refsA = Array<TypedActorRef<Count>>(actors, { i => system.typedActorOf<Count>(counter(doneA), "a${i}") })
    // B / B2：父 Actor 里 ctx.actorOf，初始行为分别以 setup 开头 / 直接是 receiveMessage
    let doneB = AtomicInt64(0)
    let box = RefBox()
    system.typedActorOf<ParentCmd>(parent(actors, doneB, box, true), "parent") |> ignoreRef
    waitUntil({ => box.snapshot().size == actors })
    let refsB = box.snapshot()
    let doneB2 = AtomicInt64(0)
    let box2 = RefBox()
    system.typedActorOf<ParentCmd>(parent(actors, doneB2, box2, false), "parent2") |> ignoreRef
    waitUntil({ => box2.snapshot().size == actors })
    let refsB2 = box2.snapshot()
    // C：不带 hooks 的 TypedSpawn.props（core 默认 hooks）
    let doneC = AtomicInt64(0)
    let refsC = Array<TypedActorRef<Count>>(actors, { i =>
        ClassicTypedRef<Count>(system.actorOf(TypedSpawn.props<Count>(counter(doneC)), "slow${i}"))
    })

    // D：用户代码直接（静态）调用 @Frozen 的 TypedSpawn.hooksFor 构造 hooks（对照：hooksFor 本身是否有效）
    let doneD = AtomicInt64(0)
    let refsD = Array<TypedActorRef<Count>>(actors, { i =>
        ClassicTypedRef<Count>(system.actorOf(
            TypedSpawn.props<Count>(counter(doneD), None<TypedActorSystem>, TypedProps(), Some(TypedSpawn.hooksFor<Count>())),
            "direct${i}"))
    })

    var bestA = 0
    var bestB = 0
    var bestB2 = 0
    var bestC = 0
    var bestD = 0
    for (round in 0..rounds) {
        let a = drive(refsA, doneA, senders, perPair)
        let b = drive(refsB, doneB, senders, perPair)
        let b2 = drive(refsB2, doneB2, senders, perPair)
        let c = drive(refsC, doneC, senders, perPair)
        let d = drive(refsD, doneD, senders, perPair)
        println("第 ${round + 1} 轮：A ${a} | B ${b} | B2 ${b2} | C ${c} | D ${d} msg/s")
        bestA = max(bestA, a)
        bestB = max(bestB, b)
        bestB2 = max(bestB2, b2)
        bestC = max(bestC, c)
        bestD = max(bestD, d)
    }
    system.terminate()
    println("最好成绩（${actors} Actor × ${senders} 发送线程 × 每对 ${perPair} 条）：")
    println("  A system.typedActorOf     ${bestA} msg/s")
    println("  B ctx.actorOf(setup {..}) ${bestB} msg/s（A 的 ${bestB * 100 / max(bestA, 1)}%）")
    println("  B2 ctx.actorOf(receive)   ${bestB2} msg/s（A 的 ${bestB2 * 100 / max(bestA, 1)}%）")
    println("  C TypedSpawn.props 无 hooks ${bestC} msg/s（A 的 ${bestC * 100 / max(bestA, 1)}%）")
    println("  D 用户代码直接 hooksFor   ${bestD} msg/s（A 的 ${bestD * 100 / max(bestA, 1)}%）")
    0
}

func ignoreRef(_: TypedActorRef<ParentCmd>): Unit {}
