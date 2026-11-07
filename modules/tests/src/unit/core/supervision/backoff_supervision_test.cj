/*
 * CActor - E6 验收测试（docs/plan/20261106.md 阶段二）
 * Backoff 监督真实现：
 * - decide 返回 RestartAfter(delay)，退避间隔按指数增长且不超过 maxDelay
 * - 达到 maxRetries 后返回 Stop 并清理状态
 * - resetBackoff 清理单个 Actor 状态
 * - 并发 decide 无崩溃（此前 retryDelays/retryCounts 是普通 HashMap，并发有竞态）
 */
package cactor_tests.unit.core.supervision

import std.unittest.*
import std.unittest.testmacro.*
import std.time.*
import std.sync.*
import std.collection.*
import cactor_core.supervision.{BackoffSupervisionStrategy, ActorFailure}
import cactor_core.actor.{SupervisionDirective, ActorRef, ActorPath}
import cactor_core.message.{Message}
import cactor_core.context.{ActorContext}

class DummyActorRef6 <: ActorRef {
    private let p: ActorPath
    public init(pth: String) { this.p = ActorPath("sys", pth) }
    public prop path: ActorPath { get() { p } }
    public func tell(message: Message): Unit {}
    public func tell(message: Message, sender: Option<String>): Unit {}
    public func equals(other: ActorRef): Bool { false }
    public func hashCode(): Int32 { 0 }
}

class Ex6 <: Exception { public init() { super("boom") } }

/** 指令是否为 RestartAfter，是则取回延迟 */
private func asRestartAfter(d: SupervisionDirective): Option<Duration> {
    match (d) {
        case SupervisionDirective.RestartAfter(delay) => Some(delay)
        case _ => None<Duration>
    }
}

func failure6(path: String): ActorFailure {
    ActorFailure(DummyActorRef6(path), Ex6(), 1)
}

@Test
func testBackoffDecideReturnsRestartAfterWithGrowingDelay() {
    let s = BackoffSupervisionStrategy(Duration.millisecond * 10, Duration.second, 10)
    // 首次失败：延迟 = initialDelay
    match (asRestartAfter(s.decide(failure6("/a")))) {
        case Some(d) => @Assert(d.toMilliseconds() == 10)
        case None => @Assert(false)
    }
    // 第二次失败：延迟翻倍 = 20ms
    match (asRestartAfter(s.decide(failure6("/a")))) {
        case Some(d) => @Assert(d.toMilliseconds() == 20)
        case None => @Assert(false)
    }
    // 第三次：40ms
    match (asRestartAfter(s.decide(failure6("/a")))) {
        case Some(d) => @Assert(d.toMilliseconds() == 40)
        case None => @Assert(false)
    }
}

@Test
func testBackoffDelayCappedAtMax() {
    // initial=100ms, max=150ms：10 次 → 100,150,150,...
    let s = BackoffSupervisionStrategy(Duration.millisecond * 100, Duration.millisecond * 150, 20)
    match (asRestartAfter(s.decide(failure6("/cap")))) {
        case Some(d) => @Assert(d.toMilliseconds() == 100)
        case None => @Assert(false)
    }
    match (asRestartAfter(s.decide(failure6("/cap")))) {
        case Some(d) => @Assert(d.toMilliseconds() == 150)
        case None => @Assert(false)
    }
    match (asRestartAfter(s.decide(failure6("/cap")))) {
        case Some(d) => @Assert(d.toMilliseconds() == 150)
        case None => @Assert(false)
    }
}

@Test
func testBackoffStopsAfterMaxRetries() {
    let s = BackoffSupervisionStrategy(Duration.millisecond * 1, Duration.millisecond * 10, 2)
    @Assert(asRestartAfter(s.decide(failure6("/m"))).isSome())
    @Assert(asRestartAfter(s.decide(failure6("/m"))).isSome())
    // 第 3 次失败：达到 maxRetries=2，停止
    match (s.decide(failure6("/m"))) {
        case SupervisionDirective.Stop => @Assert(true)
        case other => @Assert(false)
    }
    // 停止后状态已清理：再次失败从 initialDelay 重新开始
    match (asRestartAfter(s.decide(failure6("/m")))) {
        case Some(d) => @Assert(d.toMilliseconds() == 1)
        case None => @Assert(false)
    }
}

@Test
func testBackoffResetPerActor() {
    let s = BackoffSupervisionStrategy(Duration.millisecond * 5, Duration.millisecond * 50, 10)
    @Assert(asRestartAfter(s.decide(failure6("/r1"))).isSome())
    @Assert(asRestartAfter(s.decide(failure6("/r1"))).isSome())
    @Assert(asRestartAfter(s.decide(failure6("/r2"))).isSome())
    @Assert(s.trackedActors() == 2)

    s.resetBackoff(DummyActorRef6("/r1"))
    @Assert(s.trackedActors() == 1)
    // 重置后 r1 从 initialDelay 重新开始
    match (asRestartAfter(s.decide(failure6("/r1")))) {
        case Some(d) => @Assert(d.toMilliseconds() == 5)
        case None => @Assert(false)
    }
    // r2 不受影响（已记录 1 次，下次应为 10ms）
    match (asRestartAfter(s.decide(failure6("/r2")))) {
        case Some(d) => @Assert(d.toMilliseconds() == 10)
        case None => @Assert(false)
    }
}

@Test
func testBackoffConcurrentDecideNoCrash() {
    // 修复前：普通 HashMap 并发读写有竞态崩溃风险；修复后锁保护
    let s = BackoffSupervisionStrategy(Duration.millisecond * 1, Duration.millisecond * 5, 100000)
    let threads = 32
    let perThread = 200
    let errors = AtomicInt64(0)
    let futures = ArrayList<Future<Unit>>()
    for (i in 0..threads) {
        futures.add(spawn {
            for (j in 0..perThread) {
                try {
                    // 4 个 key 交叉争用；只要不崩溃、不抛异常即可
                    let d = s.decide(failure6("/concurrent-${i % 4}"))
                    ()
                } catch (e: Exception) {
                    errors.fetchAdd(1)
                }
            }
        })
    }
    for (f in futures) {
        f.get()
    }
    @Assert(errors.load() == 0)
}
