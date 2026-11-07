/*
 * CActor - 基础件修正（E45）
 *
 *   - 断路器对齐 Akka：Closed 下成功清零连续失败；HalfOpen 只放行一个试探；callTimeout 生效（P7）
 *   - ConcurrentHashMap：键的 hashCode 抛异常后锁仍会释放；put 返回旧值（P13）
 *   - 指标注册表：同名不同实例报错，便捷方法"取或建"（P12）
 *   - 追踪：trace/span ID 为随机 128/64 位、互不相同；活跃/已完成数量真实；历史有上限（P11）
 *   - 告警：同一规则不重复触发、条件恢复自动解决；渠道抛异常不卡死其他线程；计数真实（P11）
 *
 * 断路器用注入的虚拟时钟驱动开启 → 半开，不依赖真实等待。
 */
package cactor_tests.unit.foundation

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.circuit_breaker.*
import cactor_foundation.queue.{ConcurrentHashMap}
import cactor_foundation.monitoring.*

// ---------------- 断路器 ----------------

class CbBoom <: Exception {
    init(msg: String) {
        super(msg)
    }
}

class ManualClock {
    let now = AtomicInt64(0)
}

func newBreaker(failures: Int32, successes: Int32, timeoutMs: Int64, resetMs: Int64, clock: ManualClock): BasicCircuitBreaker<Int64> {
    BasicCircuitBreaker<Int64>(
        CircuitBreakerConfig(failures, successes, Duration.millisecond * timeoutMs, Duration.millisecond * resetMs),
        { => clock.now.load() })
}

func stateName(s: CircuitBreakerState): String {
    match (s) {
        case CircuitBreakerState.Closed => "Closed"
        case CircuitBreakerState.Open => "Open"
        case CircuitBreakerState.HalfOpen => "HalfOpen"
    }
}

func failOnce(b: BasicCircuitBreaker<Int64>): Unit {
    try {
        b.execute({ => throw CbBoom("x") })
    } catch (e: CbBoom) {
        ()
    }
}

func succeedOnce(b: BasicCircuitBreaker<Int64>): Int64 {
    b.execute({ => 1 })
}

/** 调用是否被快速失败拒绝（操作本身没有执行） */
func rejected(b: BasicCircuitBreaker<Int64>): Bool {
    try {
        b.execute({ => 1 })
        false
    } catch (e: CircuitBreakerOpenException) {
        true
    }
}

// ---------------- ConcurrentHashMap ----------------

/** hashCode 可按需抛异常的键：模拟用户键实现有缺陷 */
class FlakyKey <: Hashable & Equatable<FlakyKey> {
    let id: Int64
    let explode: Bool

    init(id: Int64, explode: Bool) {
        this.id = id
        this.explode = explode
    }

    public func hashCode(): Int64 {
        if (explode) {
            throw CbBoom("hashCode 失败")
        }
        id
    }

    public operator func ==(other: FlakyKey): Bool {
        id == other.id
    }

    public operator func !=(other: FlakyKey): Bool {
        id != other.id
    }
}

// ---------------- 告警 ----------------

class RecordingChannel <: NotificationChannel {
    let sent = ArrayList<String>()
    let lock = Mutex()
    let failing: Bool

    init(failing: Bool) {
        this.failing = failing
    }

    public func getName(): String {
        if (failing) { "failing" } else { "recording" }
    }

    public func send(alert: AlertEvent): Bool {
        if (failing) {
            throw CbBoom("渠道不可用")
        }
        synchronized(lock) {
            sent.add("${alert.name}:${alert.getStatusString()}")
        }
        true
    }

    public func isEnabled(): Bool {
        true
    }

    func snapshot(): Array<String> {
        synchronized(lock) {
            sent.toArray()
        }
    }
}

func isLowerHex(s: String): Bool {
    for (b in s.toArray()) {
        if (!((b >= UInt8(48) && b <= UInt8(57)) || (b >= UInt8(97) && b <= UInt8(102)))) {
            return false
        }
    }
    true
}

@Test
public class FoundationFixesTest {
    // ---------- 断路器（P7） ----------

    @TestCase
    public func successInClosedResetsConsecutiveFailures(): Unit {
        let clock = ManualClock()
        let b = newBreaker(3, 1, 0, 1000, clock)
        failOnce(b)
        failOnce(b)
        succeedOnce(b) |> ignoreInt
        failOnce(b)
        failOnce(b)
        // 连续失败只有 2 次：此前失败计数从不清零，累计到 4 次就开启
        @Expect(stateName(b.getState()), "Closed")
        failOnce(b)
        @Expect(stateName(b.getState()), "Open")
        // 统计是累计值
        @Expect(b.getStats().failureCount, 5)
        @Expect(b.getStats().successCount, 1)
    }

    @TestCase
    public func halfOpenAdmitsOnlyOneProbeAtATime(): Unit {
        let clock = ManualClock()
        let b = newBreaker(1, 2, 0, 1000, clock)
        failOnce(b)
        @Expect(stateName(b.getState()), "Open")
        @Expect(rejected(b), true)
        clock.now.store(999)
        @Expect(stateName(b.getState()), "Open")
        clock.now.store(1000)
        // 开启满 resetTimeout：不需要先来一次调用，状态即为半开（等价于 Akka 的定时转换）
        @Expect(stateName(b.getState()), "HalfOpen")
        let innerRejected = AtomicBool(false)
        // 试探调用执行期间，别的调用被拒绝（此前半开状态放行全部并发调用）
        let r = b.execute({ =>
            innerRejected.store(rejected(b))
            7
        })
        @Expect(r, 7)
        @Expect(innerRejected.load(), true)
        // 需要 2 次试探成功才关闭
        @Expect(stateName(b.getState()), "HalfOpen")
        succeedOnce(b) |> ignoreInt
        @Expect(stateName(b.getState()), "Closed")
    }

    @TestCase
    public func failedProbeReopensAndRestartsTimer(): Unit {
        let clock = ManualClock()
        let b = newBreaker(1, 1, 0, 1000, clock)
        failOnce(b)
        clock.now.store(1000)
        failOnce(b)
        @Expect(stateName(b.getState()), "Open")
        // 重新计时：从第二次开启（t=1000）起算
        clock.now.store(1999)
        @Expect(stateName(b.getState()), "Open")
        clock.now.store(2000)
        @Expect(stateName(b.getState()), "HalfOpen")
        succeedOnce(b) |> ignoreInt
        @Expect(stateName(b.getState()), "Closed")
    }

    @TestCase
    public func callExceedingTimeoutCountsAsFailure(): Unit {
        let clock = ManualClock()
        let b = newBreaker(1, 1, 100, 1000, clock)
        var timedOut = false
        try {
            b.execute({ =>
                clock.now.fetchAdd(150)
                1
            })
        } catch (e: CircuitBreakerTimeoutException) {
            timedOut = true
        }
        // 此前 timeout 配置完全没用：慢调用照常算成功
        @Expect(timedOut, true)
        @Expect(stateName(b.getState()), "Open")
    }

    @TestCase
    public func manualSucceedAndFailFollowStateMachine(): Unit {
        let clock = ManualClock()
        let b = newBreaker(2, 1, 0, 500, clock)
        b.fail()
        b.succeed()
        b.fail()
        @Expect(stateName(b.getState()), "Closed")
        b.fail()
        @Expect(stateName(b.getState()), "Open")
        clock.now.store(500)
        b.fail()
        // 半开时记失败立即回到开启
        @Expect(stateName(b.getState()), "Open")
    }

    // ---------- ConcurrentHashMap（P13） ----------

    @TestCase
    public func concurrentHashMapReleasesLockWhenKeyThrows(): Unit {
        let m = ConcurrentHashMap<FlakyKey, Int64>()
        var threw = false
        try {
            m.put(FlakyKey(1, true), 1) |> ignoreOpt
        } catch (e: CbBoom) {
            threw = true
        }
        @Expect(threw, true)
        // 此前自旋锁没有 finally：这里会永远自旋（用例靠 --timeout-each 判红）。在另一线程里调用，主线程限时等待
        let f = spawn {
            m.put(FlakyKey(2, false), 2)
        }
        let old = try {
            f.get(Duration.second * 5)
        } catch (e: Exception) {
            Some(-1)
        }
        @Expect(old.isNone(), true)
        @Expect(m.get(FlakyKey(2, false)), Some(2))
    }

    @TestCase
    public func concurrentHashMapPutReturnsPreviousValue(): Unit {
        let m = ConcurrentHashMap<String, Int64>()
        @Expect(m.put("a", 1), None<Int64>)
        // 此前恒返回 None
        @Expect(m.put("a", 2), Some(1))
        @Expect(m.putIfAbsent("a", 3), Some(2))
        @Expect(m.get("a"), Some(2))
        @Expect(m.putIfAbsent("b", 4), None<Int64>)
        @Expect(m.size(), 2)
    }

    // ---------- 指标注册表（P12） ----------

    @TestCase
    public func registeringAnotherMetricUnderSameNameFails(): Unit {
        let r = MetricRegistry()
        let first = CounterMetric("requests")
        r.register(first)
        // 同一实例重复注册是幂等的
        r.register(first)
        var threw = false
        try {
            r.register(CounterMetric("requests"))
        } catch (e: DuplicateMetricException) {
            threw = true
        }
        @Expect(threw, true)
        @Expect(r.getMetricCount(), 1)
        // 先注册的实例仍然有效（此前被静默替换，它的写入不再出现在注册表里）
        first.increment()
        match (r.getMetric("requests")) {
            case Some(m) => @Expect(m.getValue().value, 1.0)
            case None => @Expect(false, true)
        }
    }

    @TestCase
    public func getOrCreateReturnsSameInstanceAndRejectsTypeClash(): Unit {
        let r = MetricRegistry()
        let a = r.counter("msgs")
        let b = r.counter("msgs")
        a.increment()
        b.increment()
        @Expect(a.getCount(), 2.0)
        var threw = false
        try {
            r.gauge("msgs") |> ignoreGauge
        } catch (e: DuplicateMetricException) {
            threw = true
        }
        @Expect(threw, true)
    }

    // ---------- 追踪（P11） ----------

    @TestCase
    public func traceAndSpanIdsAreRandomAndUnique(): Unit {
        let tracer = Tracer("svc")
        let traceIds = HashSet<String>()
        let spanIds = HashSet<String>()
        let spans = ArrayList<Span>()
        // 同一毫秒内开始大量 span：此前 ID 只有毫秒时间戳，几乎全部相同
        for (_ in 0..2000) {
            let s = tracer.startSpan("op")
            traceIds.add(s.traceId)
            spanIds.add(s.spanId)
            spans.add(s)
        }
        @Expect(traceIds.size, 2000)
        @Expect(spanIds.size, 2000)
        let sample = spans[0]
        @Expect(sample.traceId.size, 32)
        @Expect(sample.spanId.size, 16)
        @Expect(isLowerHex(sample.traceId) && isLowerHex(sample.spanId), true)
        // 活跃数量真实（此前恒为 5）
        @Expect(tracer.getActiveSpanCount(), 2000)
        for (s in spans) {
            tracer.finishSpan(s)
        }
        @Expect(tracer.getActiveSpanCount(), 0)
    }

    @TestCase
    public func finishedSpanHistoryIsBounded(): Unit {
        let tracer = Tracer("svc", 100)
        for (_ in 0..250) {
            tracer.finishSpan(tracer.startSpan("op"))
        }
        @Expect(tracer.getFinishedSpanCount(), 100)
        @Expect(tracer.getDroppedSpanCount(), 150)
        @Expect(tracer.getActiveSpanCount(), 0)
    }

    // ---------- 告警（P11） ----------

    @TestCase
    public func ruleAlertFiresOnceAndResolvesWhenConditionClears(): Unit {
        let m = AlertManager()
        let ch = RecordingChannel(false)
        m.addChannel(ch)
        m.addRule(AlertRule("high-mailbox", "mailbox", AlertCondition.GreaterThan, 100.0, AlertLevel.Warning, "邮箱积压", true))
        m.evaluateMetric("mailbox", 150.0)
        m.evaluateMetric("mailbox", 180.0)
        m.evaluateMetric("mailbox", 200.0)
        // 条件持续成立只告警一次（此前每次评估都新建告警、重复通知）
        @Expect(m.getActiveAlertCount(), 1)
        @Expect(ch.snapshot(), ["high-mailbox:ACTIVE"])
        m.evaluateMetric("mailbox", 10.0)
        @Expect(m.getActiveAlertCount(), 0)
        @Expect(ch.snapshot(), ["high-mailbox:ACTIVE", "high-mailbox:RESOLVED"])
        // 再次越线是一次新的告警
        m.evaluateMetric("mailbox", 300.0)
        @Expect(m.getActiveAlertCount(), 1)
        // 计数真实（此前规则数恒为 3、渠道数恒为 2）
        @Expect(m.getRuleCount(), 1)
        @Expect(m.getChannelCount(), 1)
    }

    @TestCase
    public func failingChannelDoesNotBlockOtherThreads(): Unit {
        let m = AlertManager()
        let good = RecordingChannel(false)
        m.addChannel(RecordingChannel(true))
        m.addChannel(good)
        m.triggerAlert("a", AlertLevel.Error, "first")
        // 此前在锁内 send 且没有 finally：渠道一抛异常，锁就永远不释放，其他线程再调用会一直阻塞
        let f = spawn {
            m.triggerAlert("b", AlertLevel.Error, "second")
            m.getActiveAlertCount()
        }
        let count = try {
            f.get(Duration.second * 5)
        } catch (e: Exception) {
            Int32(-1)
        }
        @Expect(count, Int32(2))
        // 单个渠道失败不影响其他渠道
        @Expect(good.snapshot(), ["a:ACTIVE", "b:ACTIVE"])
    }

    @TestCase
    public func alertIdsAreUniqueAndHistoryIsBounded(): Unit {
        let m = AlertManager(50)
        for (i in 0..200) {
            m.triggerAlert("manual-${i}", AlertLevel.Info, "x")
        }
        // id 唯一：此前同一毫秒内的告警 id 相同、互相覆盖
        @Expect(m.getActiveAlertCount(), 200)
        @Expect(m.getHistorySize(), 50)
    }
}

func ignoreInt(_: Int64): Unit {}

func ignoreOpt(_: Option<Int64>): Unit {}

func ignoreGauge(_: GaugeMetric): Unit {}
