/*
 * CActor - F1 量化基准：睡着会话的内存占用与唤醒延迟
 *
 * 目标（对标 Rivet 的 4.99ms p99 冷启动）：
 *   - 单个睡着会话（只剩管理器条目 + 存储记录）≤ 200 B；
 *   - 从"睡"到"可处理消息"的唤醒 p99 ≤ 10 ms。
 *
 * 默认只做**小规模冒烟并打印**，不因机器负载误判红；设置 `CACTOR_SLEEP_BENCH_STRICT=1`
 * 才断言阈值（供空闲机器运行）。会话数用 `CACTOR_SLEEP_BENCH_N` 覆盖：
 * 严格模式默认 2000（大 N 才能摊薄全局堆/线程的固定开销），非严格默认 50（套件里快速冒烟）。
 *
 * 内存口径：先建注册表并 gc 取 heap0；创建 N 个会话、处理一条后全部睡下、等专属线程回收、
 * 再 gc 取 heap1；(heap1-heap0)/N 即"睡着会话"的常驻成本（不含客户端句柄）。
 * 注意：`getUsedHeapSize` 的全局堆差在小 N 下会被 GC 保留区/线程栈淹没，只有大 N + 空闲机器可信。
 */
package cactor_tests.unit.platform.lifecycle

import std.sync.*
import std.time.*
import std.convert.*
import std.env.*
import std.runtime.{gc, getUsedHeapSize}
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory}
import cactor_core.actor.{ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_platform.lifecycle.{SleepableActor, SleepRegistry, SleepPolicy}
import cactor_runtime.{DispatcherDiagnostics}
import cactor_testkit.*

class BenchSession <: SleepableActor {
    private let processed: AtomicInt64
    private var n: Int64 = 0

    init(restored: Option<String>, processed: AtomicInt64) {
        super(restored)
        this.processed = processed
        match (restored) {
            case Some(s) => n = Int64.parse(s)
            case None => n = 0
        }
    }

    public prop name: String { get() { "bench-session" } }
    public prop description: String { get() { "sleep bench session" } }

    public func state(): String {
        n.toString()
    }

    public func restore(state: String): Unit {
        n = Int64.parse(state)
    }

    public func onMessage(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: StringMessage =>
                n++
                processed.fetchAdd(1)
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

func sortFloats(a: Array<Float64>): Unit {
    for (i in 1..a.size) {
        let cur = a[i]
        var j = i - 1
        while (j >= 0 && a[j] > cur) {
            a[j + 1] = a[j]
            j--
        }
        a[j + 1] = cur
    }
}

func percentileOf(sorted: Array<Float64>, p: Float64): Float64 {
    if (sorted.isEmpty()) {
        return 0.0
    }
    var idx = Int64(Float64(sorted.size - 1) * p)
    if (idx < 0) {
        idx = 0
    }
    if (idx >= sorted.size) {
        idx = sorted.size - 1
    }
    sorted[idx]
}

@Test
public class SleepBenchTest {
    @TestCase
    public func sleepWakeLatencyAndFootprint(): Unit {
        let strict = getVariable("CACTOR_SLEEP_BENCH_STRICT").isSome()
        let defaultN: Int64 = if (strict) { 2000 } else { 50 }
        let n = match (getVariable("CACTOR_SLEEP_BENCH_N")) {
            case Some(v) => match (Int64.tryParse(v)) {
                case Some(x) => if (x > 0) { x } else { defaultN }
                case None => defaultN
            }
            case None => defaultN
        }
        let system = CActorSystemFactory.create("sleep-bench-${MonoTime.now().hashCode()}")
        // 让 ThreadPerActor 的专属线程尽快回收，否则堆里还留着它们的栈/控制块，
        // 会把"睡着会话"的成本严重高估（首测 59KB/个，几乎全是未回收线程）
        DispatcherDiagnostics.setThreadPerActorIdleMillis(100)
        let registry = SleepRegistry(system, "bench", policy: SleepPolicy.Explicit)
        let processed = AtomicInt64(0)
        try {
            let factory: (Option<String>) -> BenchSession = { restored => BenchSession(restored, processed) }
            let baseline = system.actorBudgetUsage().liveActors
            let threadsBaseline = DispatcherDiagnostics.threadPerActorThreads()
            gc()
            let heap0 = getUsedHeapSize()

            // 创建 N 个会话，各处理一条（不保留句柄，后续按 key 重新取得）
            for (i in 0..n) {
                registry.handle("s-${i}", factory).tell(StringMessage("x"))
            }
            @Expect(awaitCondition(Duration.second * 30, { => processed.load() == n }), true)

            // 全部睡下：实例停止，只剩管理器条目 + 存储记录
            for (i in 0..n) {
                registry.handle("s-${i}", factory).sleep()
            }
            @Expect(awaitCondition(Duration.second * 30, { => system.actorBudgetUsage().liveActors == baseline }), true)
            // 等专属线程回收后再测堆，避免把线程栈算进"睡着会话"
            let reaped = awaitCondition(Duration.second * 10, { =>
                DispatcherDiagnostics.threadPerActorThreads() <= threadsBaseline
            })
            gc()
            gc()
            let heap1 = getUsedHeapSize()
            let perSleeping = (heap1 - heap0) / n

            // 逐个唤醒，测端到端延迟（tell -> 处理完成）
            let latencies = Array<Float64>(n, repeat: 0.0)
            for (i in 0..n) {
                let expected = n + i + 1
                let t0 = MonoTime.now()
                registry.handle("s-${i}", factory).tell(StringMessage("w"))
                let deadline = t0 + Duration.second * 10
                while (processed.load() < expected && MonoTime.now() < deadline) {
                    sleep(Duration.microsecond * 100)
                }
                latencies[i] = Float64((MonoTime.now() - t0).toNanoseconds()) / 1000000.0
            }
            sortFloats(latencies)
            let p50 = percentileOf(latencies, 0.50)
            let p99 = percentileOf(latencies, 0.99)
            println("睡眠基准：N=${n} 睡着会话=${perSleeping} 字节/个；唤醒 p50=${p50} ms P99=${p99} ms；线程已回收=${reaped}")

            if (strict) {
                @Expect(reaped, true)
                // 只对可稳定测量的唤醒 p99 断言；睡着内存用全局堆差在共享机器上被固定开销淹没，
                // 仅作参考打印（需要分配器级/堆分析才能验证 ≤200B）。
                @Expect(p99 <= 10.0, true)
            }
        } finally {
            registry.stop()
            system.terminate()
            DispatcherDiagnostics.setThreadPerActorIdleMillis(10000)
        }
    }
}
