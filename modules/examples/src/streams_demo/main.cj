/*
 * CActor Examples - 背压流（cactor_streams，对标 Akka Streams）
 *
 * 演示：
 *   1. 惰性蓝图 + 物化：同一张图运行两次
 *   2. 慢消费者 + 无限源 + async 边界：已生产 - 已消费 始终不超过边界缓冲区
 *   3. ask：以有界并行度询问类型化 Actor，按顺序得到回复
 *   4. Source.queue：从流外送入元素，溢出策略决定满时的行为
 *   5. kill switch：从流外结束一条无限流
 *   6. actorRefWithBackpressure：逐条确认地把元素交给 Actor
 *
 * 运行：cjpm build && CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.streams_demo
 */
package cactor_examples.streams_demo

import std.collection.*
import std.sync.*
import cactor_core.dsl.*
import cactor_core.typed.*
import cactor.api.CActor
import cactor_streams.*

enum PriceCmd {
    | Quote(String, TypedActorRef<Int64>)
}

enum Ledger {
    | Open(TypedActorRef<Unit>)
    | Entry(Int64, TypedActorRef<Unit>)
    | Close
    | Broken(String)
}

/** 账本 Actor：每记一笔就确认一次，流才会发下一笔 */
func ledger(total: AtomicInt64, closed: AtomicBool): TypedBehavior<Ledger> {
    TypedBehaviors.receive<Ledger>({ _, msg =>
        match (msg) {
            case Ledger.Open(ack) => ack << ()
            case Ledger.Entry(amount, ack) =>
                total.fetchAdd(amount)
                ack << ()
            case Ledger.Close => closed.store(true)
            case Ledger.Broken(reason) => println("账本收到失败：${reason}")
        }
        TypedBehaviors.same<Ledger>()
    })
}

main(): Int64 {
    let system = CActor.system("streams-demo")
    var ok = true

    // 1. 蓝图是惰性的、可重复运行的
    let squares = Sources.range(1, 5).map<Int64>({ x => x * x }).toMat(Sinks.seq<Int64>(), { _, r => r })
    let first = squares.run(system).get(3.seconds)
    let second = squares.run(system).get(3.seconds)
    println("1. 平方：${first} / 再运行一次：${second}")
    ok = ok && first == [1, 4, 9, 16, 25] && second == first

    // 2. 无限源 + 慢消费者：async 边界一次最多请求 16 个，消费者不拉取时生产就停下
    let produced = AtomicInt64(0)
    let mat = Materializer.create(system).withSettings(StreamSettings().withInputBufferSize(16))
    let pullQueue = Sources.repeat(1).map<Int64>({ x =>
        produced.fetchAdd(1)
        x
    }).async().runWith(Sinks.queue<Int64>(), mat)
    var consumed: Int64 = 0
    var maxGap: Int64 = 0
    for (_ in 0..100) {
        pullQueue.pull().get(3.seconds)
        consumed++
        let gap = produced.load() - consumed
        if (gap > maxGap) { maxGap = gap }
    }
    pullQueue.cancel()
    println("2. 消费 ${consumed} 个，生产与消费之差最大 ${maxGap}（上界 17）")
    ok = ok && maxGap <= 17

    // 3. 每个元素问一次报价 Actor，最多 4 个在途，回复按输入顺序排列
    let pricer = system.typedActorOf<PriceCmd>(TypedBehaviors.receive<PriceCmd>({ _, msg =>
        match (msg) {
            case PriceCmd.Quote(item, replyTo) => replyTo << item.size * 10
        }
        TypedBehaviors.same<PriceCmd>()
    }), "pricer")
    let prices = Sources.from<String>(["tea", "coffee", "water", "juice"])
        .ask<PriceCmd, Int64>(4, pricer, { item, replyTo => PriceCmd.Quote(item, replyTo) }, 3.seconds)
        .runWith(Sinks.seq<Int64>(), system).get(3.seconds)
    println("3. 报价：${prices}")
    ok = ok && prices == [30, 60, 50, 50]

    // 4. Source.queue：容量 2，DropNew 策略，满了的 offer 被丢弃（下游可能已取走一部分）
    let (queue, collected) = Sources.queue<String>(2, OverflowStrategy.DropNew)
        .toMat(Sinks.seq<String>(), { q, r => (q, r) }).run(system)
    let results = ArrayList<String>()
    for (word in ["a", "b", "c"]) {
        results.add(queue.offer(word).get(3.seconds).toString())
    }
    queue.complete()
    let got = collected.get(3.seconds)
    println("4. offer 结果：${results}，流收到：${got}")
    ok = ok && got.size >= 2

    // 5. kill switch：从流外结束一条无限流，汇正常完成
    let (killSwitch, done) = Sources.tick<String>(0.millis, 5.millis, "tick")
        .viaMat<String, UniqueKillSwitch, UniqueKillSwitch>(KillSwitches.single<String>(), { _, k => k })
        .toMat(Sinks.ignore<String>(), { k, d => (k, d) }).run(system)
    killSwitch.shutdown()
    done.get(3.seconds)
    println("5. kill switch 已关闭无限 tick 流")

    // 6. actorRefWithBackpressure：每笔记账都要确认，最后一笔确认后才发 Close
    let total = AtomicInt64(0)
    let closed = AtomicBool(false)
    let book = system.typedActorOf<Ledger>(ledger(total, closed), "ledger")
    let watched = Sources.range(1, 100).watchTermination()
    let finished = watched.toMat(Sinks.actorRefWithBackpressure<Int64, Ledger, Unit>(book,
        { ack, x => Ledger.Entry(x, ack) }, { ack => Ledger.Open(ack) }, Ledger.Close,
        { e => Ledger.Broken(e.message) }), { w, _ => w }).run(system)
    finished.get(3.seconds)
    var waited = 0
    while (!closed.load() && waited < 3000) {
        sleep(1.millis)
        waited++
    }
    println("6. 账本合计 ${total.load()}，已关闭：${closed.load()}")
    ok = ok && total.load() == 5050 && closed.load()

    system.terminate()
    println(if (ok) { "streams_demo：通过" } else { "streams_demo：失败" })
    if (ok) { 0 } else { 1 }
}
