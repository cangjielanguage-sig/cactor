/*
 * CActor TestKit - 虚拟时钟（对标 Akka ManualTime / ExplicitlyTriggeredScheduler，E52）
 *
 * 同一个 ManualTime 有两种用法：
 * - BehaviorTestKit 的虚拟时间（同步、单线程，见 behavior_testkit.cj）；
 * - 作为 ActorScheduler 注入真实 ActorSystem（ActorTestKit(name, time)）：Actor 定时器、接收超时、
 *   退避重启延迟、ctx.scheduleOnce、Actor 内 ctx.ask 的超时都只在 timePasses 时触发，
 *   带定时器的用例不再 sleep 真实时间，也不会因为机器繁忙而早到 / 晚到。
 *
 * 真实系统里定时任务只负责投递（把消息放进邮箱），处理仍由调度线程异步完成。所以：
 * - 推进前先确认定时器已经登记（awaitPending），否则 Actor 还没执行到 startTimer 时间就已经过去了；
 * - 推进后用 TestProbe.expectMsg 等待结果（等的是处理，不是定时器），无消息断言用 expectNoMessageFor。
 */
package cactor_testkit

import std.sync.*
import std.time.*
import std.collection.*
import cactor_core.actor.{ActorScheduler, ScheduledTask}
import cactor_core.typed.{TypedCancellable}

/** 虚拟时钟上的任务 */
public class ManualTask <: TypedCancellable {
    let id: Int64
    // 只在 ManualTime 的锁内读写
    var due: Duration
    let interval: Option<Duration>
    let action: () -> Unit
    // 任意线程都可能取消（Actor 线程 cancel 定时器、测试线程推进时间）
    private let done = AtomicBool(false)

    init(id: Int64, due: Duration, interval: Option<Duration>, action: () -> Unit) {
        this.id = id
        this.due = due
        this.interval = interval
        this.action = action
    }

    public func cancel(): Bool {
        done.compareAndSwap(false, true)
    }

    public func isCancelled(): Bool {
        done.load()
    }

    /** 一次性任务执行后视为已结束（之后 cancel 返回 false，与此前语义相同） */
    func finish(): Unit {
        done.store(true)
    }
}

/** ActorScheduler 句柄：ManualTask.cancel 返回 Bool，与 ScheduledTask.cancel(): Unit 签名不同，所以包一层 */
class ManualScheduledTask <: ScheduledTask {
    private let task: ManualTask

    init(task: ManualTask) {
        this.task = task
    }

    public func cancel(): Unit {
        task.cancel() |> ignoreCancelled
    }
}

func ignoreCancelled(_: Bool): Unit {}

/**
 * 虚拟时钟：只有调用 timePasses 时时间才前进，
 * 到期的任务按到期时间先后（相同时按登记先后）依次执行；周期任务会在同一次推进中多次执行。
 * 任务在调用 timePasses 的线程上执行（执行时不持锁，任务里可以再登记任务）；
 * 执行任务时 now() 等于该任务的到期时刻，因此任务内登记的后续任务按虚拟时间正确排队。
 *
 * 线程安全：任何线程都可以登记 / 取消任务；同一时刻只应有一个线程调用 timePasses。
 */
public class ManualTime <: ActorScheduler {
    private let lock = Mutex()
    // 虚拟单调时钟的起点：monotonicNow() = 起点 + 已推进的虚拟时间
    private let epoch = MonoTime.now()
    private var nowValue = Duration.Zero
    private var nextId: Int64 = 0
    private let tasks = ArrayList<ManualTask>()

    public init() {}

    /** 从创建开始经过的虚拟时间 */
    public func now(): Duration {
        synchronized(lock) { nowValue }
    }

    public func schedule(delay: Duration, action: () -> Unit): ManualTask {
        add(delay, None<Duration>, action)
    }

    public func scheduleRepeated(interval: Duration, action: () -> Unit): ManualTask {
        add(interval, Some(interval), action)
    }

    private func add(delay: Duration, interval: Option<Duration>, action: () -> Unit): ManualTask {
        synchronized(lock) {
            nextId++
            let d = if (delay < Duration.Zero) { Duration.Zero } else { delay }
            let t = ManualTask(nextId, nowValue + d, interval, action)
            tasks.add(t)
            t
        }
    }

    // ---------- ActorScheduler ----------

    public func monotonicNow(): MonoTime {
        epoch + now()
    }

    public func scheduleOnce(delay: Duration, task: () -> Unit): ScheduledTask {
        ManualScheduledTask(add(delay, None<Duration>, task))
    }

    /** 读时钟与登记在同一把锁内完成：测试线程并发推进时间也不会让到期时刻偏后 */
    public func scheduleOnceAt(deadline: MonoTime, task: () -> Unit): ScheduledTask {
        synchronized(lock) {
            ManualScheduledTask(add(deadline - epoch - nowValue, None<Duration>, task))
        }
    }

    /** 虚拟时间里任务执行不占时间，固定延迟与固定频率等价 */
    public func scheduleWithFixedDelay(initialDelay: Duration, delay: Duration, task: () -> Unit): ScheduledTask {
        ManualScheduledTask(add(initialDelay, Some(delay), task))
    }

    public func scheduleAtFixedRate(initialDelay: Duration, interval: Duration, task: () -> Unit): ScheduledTask {
        ManualScheduledTask(add(initialDelay, Some(interval), task))
    }

    // ---------- 推进与观察 ----------

    /** 尚未执行（且未取消）的任务数 */
    public func pendingCount(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for (t in tasks) {
                if (!t.isCancelled()) {
                    n++
                }
            }
            n
        }
    }

    /**
     * 等待（真实时间，最多 timeout）至少有 atLeast 个未执行的任务。
     * 驱动真实系统时，在 timePasses 之前用它确认 Actor 已经登记了定时器。
     */
    public func awaitPending(atLeast: Int64, timeout!: Duration = TestProbe.DEFAULT_TIMEOUT): Bool {
        awaitCondition(timeout, { => pendingCount() >= atLeast })
    }

    /** 推进虚拟时间，期间到期的任务依次执行 */
    public func timePasses(amount: Duration): Unit {
        if (amount < Duration.Zero) {
            throw IllegalArgumentException("timePasses 的时长不能为负: ${amount}")
        }
        let target = synchronized(lock) { nowValue + amount }
        while (true) {
            let next = synchronized(lock) {
                match (nextDue(target)) {
                    case Some(t) =>
                        nowValue = t.due
                        match (t.interval) {
                            case Some(i) where i > Duration.Zero => t.due = t.due + i
                            case _ => t.finish()
                        }
                        Some(t)
                    case None => None<ManualTask>
                }
            }
            match (next) {
                // 不持锁执行：任务可能登记新任务，或（真实系统里）触发别的线程登记 / 取消
                case Some(t) => t.action()
                case None => break
            }
        }
        synchronized(lock) {
            if (nowValue < target) {
                nowValue = target
            }
            tasks.removeIf({ t => t.isCancelled() })
        }
    }

    /**
     * 推进 amount 之后，断言各探针都没有收到消息（对标 Akka ManualTime.expectNoMessageFor）。
     * 定时任务只是把消息放进邮箱，处理是异步的，所以每个探针再等 settle（真实时间）确认没有迟到的消息。
     */
    public func expectNoMessageFor(amount: Duration, probes: Array<TestProbe>,
        settle!: Duration = Duration.millisecond * 100): Unit {
        timePasses(amount)
        for (p in probes) {
            p.expectNoMsg(settle)
        }
    }

    private func nextDue(target: Duration): Option<ManualTask> {
        var best: Option<ManualTask> = None
        for (t in tasks) {
            if (t.isCancelled() || t.due > target) {
                continue
            }
            match (best) {
                case Some(b) where b.due < t.due || (b.due == t.due && b.id < t.id) => ()
                case _ => best = Some(t)
            }
        }
        best
    }
}
