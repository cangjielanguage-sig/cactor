/*
 * CActor - 并发安全加固（E35）
 *
 *   - 计数器 64 位（R19）：已处理 / 未处理数不再在 65535 饱和；多个发送方并发计死信不再丢数、不进位污染重启计数
 *   - 建停与抓取并发（R8、R15）：Actor 不停创建、停止子树（含合批停止）的同时，
 *     别的线程反复抓取 Prometheus 指标（遍历全部子表）并按路径 lookup 子 Actor，
 *     既不抛异常也不崩溃，停止后存活数回到 0（合批计数没有错乱）
 *
 * 压测时长默认 3 s，验收时用 CACTOR_CHURN_SECONDS=600 跑 10 分钟。
 */
package cactor_tests.unit.runtime.concurrency

import std.sync.*
import std.time.*
import std.env.*
import std.convert.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import std.runtime.{gc, getUsedHeapSize}
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_testkit.*

/** "u" 返回 Unhandled，其余正常处理 */
class HalfDeaf <: Actor {
    public prop name: String {
        get() { "half-deaf" }
    }

    public prop description: String {
        get() { "half-deaf" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage where s.getContent() == "u" => MessageResult.Unhandled
            case _ => MessageResult.Handled
        }
    }
}

class Quiet <: Actor {
    public prop name: String {
        get() { "quiet" }
    }

    public prop description: String {
        get() { "quiet" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

/** 收到 block 后占住线程直到 release（最多 30 s） */
class HoldGate <: Actor {
    let rec: EventRecorder
    let release: AtomicBool

    init(rec: EventRecorder, release: AtomicBool) {
        this.rec = rec
        this.release = release
    }

    public prop name: String {
        get() { "hold-gate" }
    }

    public prop description: String {
        get() { "hold-gate" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            if (s.getContent() == "block") {
                rec.record("blocking")
                let _ = awaitCondition(Duration.second * 30, { => release.load() })
            }
        }
        MessageResult.Handled
    }
}

/** 子 Actor：启动时再建一个孙 Actor，让它的停止走"有子 Actor 的合批停止"路径 */
class ChurnChild <: Actor {
    public prop name: String {
        get() { "churn-child" }
    }

    public prop description: String {
        get() { "churn-child" }
    }

    public func onStarted(context: ActorContext): Unit {
        context.actorOf({ => Quiet() }, "leaf") |> ignoreRef
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        MessageResult.Handled
    }
}

/**
 * 每轮建 8 个子 Actor（各带 1 个孙 Actor），watch 后全部停止；8 个 Terminated 都到齐才开始下一轮。
 * 在途 Actor 数有上界（不等完成就开下一轮时，每秒建停十几万个 Actor、分配约 300 MB/s，256 MB 默认堆的 GC 跟不上）
 */
class Churner <: Actor {
    let rounds: AtomicInt64
    let running: AtomicBool
    var outstanding: Int64 = 0

    init(rounds: AtomicInt64, running: AtomicBool) {
        this.rounds = rounds
        this.running = running
    }

    public prop name: String {
        get() { "churner" }
    }

    public prop description: String {
        get() { "churner" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: Terminated =>
                outstanding--
                if (outstanding == 0) {
                    rounds.fetchAdd(1)
                    context.self << StringMessage("churn")
                }
            case _ =>
                if (running.load()) {
                    for (_ in 0..8) {
                        let k = context.actorOf({ => ChurnChild() })
                        context.watch(k)
                        context.stop(k)
                        outstanding++
                    }
                }
        }
        MessageResult.Handled
    }
}

func metricValue(text: String, prefix: String): Option<Int64> {
    for (line in text.split("\n")) {
        if (line.startsWith(prefix + " ")) {
            return Int64.tryParse(line[(prefix.size + 1)..])
        }
    }
    None<Int64>
}

func envCount(name: String, dflt: Int64): Int64 {
    match (getVariable(name)) {
        case Some(v) => Int64.tryParse(v) ?? dflt
        case None => dflt
    }
}

func churnSeconds(): Int64 {
    match (getVariable("CACTOR_CHURN_SECONDS")) {
        case Some(v) => Int64.tryParse(v) ?? 3
        case None => 3
    }
}

@Test
public class ConcurrencyHardeningTest {
    @TestCase
    public func processedAndUnhandledCountersDoNotSaturate(): Unit {
        let kit = ActorTestKit("ctr64")
        let ref = kit.actorOf({ => HalfDeaf() }, "c")
        let n: Int64 = 70_000
        for (_ in 0..n) {
            ref << StringMessage("u")
        }
        let processed = "cactor_actor_processed_total{system=\"ctr64\",path=\"/user/c\"}"
        let unhandled = "cactor_actor_unhandled_total{system=\"ctr64\",path=\"/user/c\"}"
        // 此前两个计数都停在 65535
        @Expect(awaitCondition(Duration.second * 20, { => metricValue(kit.system.metricsText(true), processed) == Some(n) }),
            true)
        @Expect(metricValue(kit.system.metricsText(true), unhandled), Some(n))
        kit.shutdown()
    }

    @TestCase
    public func concurrentDeadLettersAreCountedExactly(): Unit {
        let kit = ActorTestKit("ctr-dead")
        let rec = EventRecorder()
        let release = AtomicBool(false)
        let ref = kit.actorOf(PropsFactory.create<Actor>({ => HoldGate(rec, release) })
            .withBoundedMailbox(1, MailboxOverflow.DropNew), "full")
        ref << StringMessage("block")
        @Expect(rec.await("blocking", Duration.second * 5), true)
        // 占满容量 1 的邮箱：之后的每条消息都被丢弃并计入死信
        ref << StringMessage("fill")
        let perThread: Int64 = 20_000
        let senders = ArrayList<Future<Unit>>()
        for (_ in 0..4) {
            senders.add(spawn {
                for (_ in 0..perThread) {
                    ref << StringMessage("x")
                }
            })
        }
        for (f in senders) {
            f.get()
        }
        let text = kit.system.metricsText(true)
        release.store(true)
        // 此前 16 位饱和于 65535；多个发送方并发"先读后加"还会进位到相邻的重启计数
        @Expect(metricValue(text, "cactor_actor_dead_letters_total{system=\"ctr-dead\",path=\"/user/full\"}"),
            Some(perThread * 4))
        @Expect(metricValue(text, "cactor_actor_restarts_total{system=\"ctr-dead\",path=\"/user/full\"}"), Some(0))
        kit.shutdown()
    }

    @TestCase
    public func scrapeAndLookupDuringChildChurn(): Unit {
        let kit = ActorTestKit("churn")
        let rounds = AtomicInt64(0)
        let running = AtomicBool(true)
        let churners = ArrayList<ActorRef>()
        for (i in 0..4) {
            let c = kit.actorOf({ => Churner(rounds, running) }, "churner${i}")
            c << StringMessage("churn")
            churners.add(c)
        }
        let deadline = MonoTime.now() + Duration.second * churnSeconds()
        let errors = AtomicInt64(0)
        let scrapes = AtomicInt64(0)
        let lookups = AtomicInt64(0)
        let workers = ArrayList<Future<Unit>>()
        let report = getVariable("CACTOR_CHURN_SECONDS").isSome()
        if (report) {
            // 验收模式：每 10 s 做一次 full GC 后报告存活堆，用来区分"泄漏"与"分配速率高"
            workers.add(spawn {
                var next = MonoTime.now() + Duration.second * 10
                while (MonoTime.now() < deadline) {
                    if (MonoTime.now() >= next) {
                        next = MonoTime.now() + Duration.second * 10
                        gc(heavy: true)
                        let live = metricValue(kit.system.metricsText(false), "cactor_actors_live") ?? -1
                        println("churn progress: rounds=${rounds.load()} live=${live} heapMB=${getUsedHeapSize() / 1048576} scrapes=${scrapes.load()} lookups=${lookups.load()}")
                    }
                    sleep(Duration.millisecond * 100)
                }
            })
        }
        // 抓取指标：遍历全部 Cell 的子表（此前跨线程无锁遍历父的 HashMap）
        for (_ in 0..envCount("CACTOR_CHURN_SCRAPERS", 2)) {
            workers.add(spawn {
                while (MonoTime.now() < deadline) {
                    try {
                        let text = kit.system.metricsText(true)
                        if (!text.contains("cactor_actors_live")) {
                            errors.fetchAdd(1)
                        }
                        scrapes.fetchAdd(1)
                    } catch (e: Exception) {
                        errors.fetchAdd(1)
                    }
                }
            })
        }
        // 按路径查子 Actor 和孙 Actor（匿名子 Actor 名为 $1、$2……）
        for (t in 0..envCount("CACTOR_CHURN_LOOKUPS", 2)) {
            workers.add(spawn {
                var k: Int64 = 1
                while (MonoTime.now() < deadline) {
                    try {
                        let c = (k + t) % 4
                        let _ = kit.system.lookup("/user/churner${c}/$${k % 4000}")
                        let _ = kit.system.lookup("/user/churner${c}/$${k % 4000}/leaf")
                        lookups.fetchAdd(1)
                    } catch (e: Exception) {
                        errors.fetchAdd(1)
                    }
                    k++
                }
            })
        }
        for (w in workers) {
            w.get()
        }
        running.store(false)
        if (getVariable("CACTOR_CHURN_SECONDS").isSome()) {
            // 验收模式：报告压测规模
            println("churn: seconds=${churnSeconds()} rounds=${rounds.load()} actors=${rounds.load() * 16} scrapes=${scrapes.load()} lookups=${lookups.load()} errors=${errors.load()}")
        }
        @Expect(errors.load(), 0)
        @Expect(rounds.load() > 0, true)
        @Expect(scrapes.load() > 0 || envCount("CACTOR_CHURN_SCRAPERS", 2) == 0, true)
        @Expect(lookups.load() > 0 || envCount("CACTOR_CHURN_LOOKUPS", 2) == 0, true)
        for (c in churners) {
            kit.system.stop(c)
        }
        // 全部停止后存活数归零：子树合批停止的计数没有错乱，没有 Cell 卡在 TERMINATING
        @Expect(awaitCondition(Duration.second * 20, {
            => metricValue(kit.system.metricsText(false), "cactor_actors_live") == Some(0)
        }), true)
        kit.shutdown()
    }

    @TestCase
    public func perActorSeriesAreCappedAndStable(): Unit {
        let kit = ActorTestKit("series-cap")
        for (i in 0..30) {
            kit.actorOf({ => Quiet() }, "a${i}") |> ignoreRef
        }
        @Expect(awaitCondition(Duration.second * 5, {
            => metricValue(kit.system.metricsText(false), "cactor_actors_live") == Some(30)
        }), true)
        let text = kit.system.metricsText(true, 10)
        var series: Int64 = 0
        for (line in text.split("\n")) {
            if (line.startsWith("cactor_actor_processed_total{")) {
                series++
            }
        }
        @Expect(series, 10)
        @Expect(metricValue(text, "cactor_actor_series_omitted"), Some(20))
        // 取路径排序后的前 10 个：多次抓取得到同一组序列
        @Expect(text.contains("path=\"/user/a0\""), true)
        @Expect(text.contains("path=\"/user/a17\""), true)
        @Expect(text.contains("path=\"/user/a9\""), false)
        kit.shutdown()
    }
}

func ignoreRef(_: ActorRef): Unit {}
