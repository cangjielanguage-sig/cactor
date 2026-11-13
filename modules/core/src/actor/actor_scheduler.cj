/*
 * CActor Core.Actor - 可注入的运行时调度器（E52，对标 Akka Scheduler / ExplicitlyTriggeredScheduler）
 *
 * 运行时里所有"过一段时间再做"的路径都经它安排：Actor 定时器（ctx.timers / TypedBehaviors.withTimers）、
 * 接收超时、退避重启的延迟、ctx.scheduleOnce / ctx.scheduler()、Actor 内 ctx.ask 的超时。
 * 默认实现 RealTimeScheduler 直接转给 std.sync.Timer，与引入本接口之前的行为完全相同；
 * 测试可以在创建系统时注入虚拟时钟（cactor_testkit.ManualTime），让这些路径只在推进虚拟时间时触发，
 * 带定时器的用例因此不必 sleep 真实时间。
 */
package cactor_core.actor

import std.time.*
import std.sync.*

/**
 * 已安排任务的句柄
 *
 * 取消只保证"之后不再触发"：已经开始执行（或已把消息放进邮箱）的那一次不会撤回，
 * 所以调用方仍需要像 Cell 定时器那样用代次过滤在途消息。
 */
public interface ScheduledTask {
    func cancel(): Unit
}

// std Timer 本身就满足句柄接口：真实调度路径不为句柄额外分配对象
extend Timer <: ScheduledTask {}

/**
 * 运行时调度器
 *
 * 实现必须线程安全：任何线程（Actor 线程、定时器线程、测试线程）都可能安排或取消任务。
 * 任务回调里只应做非阻塞的投递（tell / 系统消息），不要执行用户逻辑。
 */
public interface ActorScheduler {
    /**
     * 本调度器的"当前时刻"。接收超时等需要比较时刻的逻辑必须用它而不是 MonoTime.now()，
     * 否则虚拟时钟推进后，按真实时间计算的空闲时长永远不够，超时不会触发。
     */
    func monotonicNow(): MonoTime

    /** delay 之后执行一次 */
    func scheduleOnce(delay: Duration, task: () -> Unit): ScheduledTask

    /**
     * 在本调度器时钟的 deadline 执行一次（已过期则尽快执行）。
     * 按绝对时刻登记，避免"先读时钟算剩余时长、再登记"两步之间时钟被推进造成的偏差——
     * 虚拟时钟由测试线程推进，与 Actor 线程并发，这个偏差会让接收超时晚触发
     */
    func scheduleOnceAt(deadline: MonoTime, task: () -> Unit): ScheduledTask {
        let d = deadline - monotonicNow()
        scheduleOnce(if (d < Duration.Zero) { Duration.Zero } else { d }, task)
    }

    /** initialDelay 之后首次执行，此后每次执行结束到下一次开始至少间隔 delay（落后时不追赶） */
    func scheduleWithFixedDelay(initialDelay: Duration, delay: Duration, task: () -> Unit): ScheduledTask

    /** initialDelay 之后首次执行，此后按固定频率执行（落后时补发） */
    func scheduleAtFixedRate(initialDelay: Duration, interval: Duration, task: () -> Unit): ScheduledTask
}

/**
 * 默认调度器：std.sync.Timer + 真实单调时钟。无状态，全局共享一个实例。
 */
public class RealTimeScheduler <: ActorScheduler {
    public static let instance: RealTimeScheduler = RealTimeScheduler()

    private init() {}

    public func monotonicNow(): MonoTime {
        MonoTime.now()
    }

    public func scheduleOnce(delay: Duration, task: () -> Unit): ScheduledTask {
        Timer.once(delay, task)
    }

    public func scheduleWithFixedDelay(initialDelay: Duration, delay: Duration, task: () -> Unit): ScheduledTask {
        Timer.repeat(initialDelay, delay, task, style: CatchupStyle.Delay)
    }

    public func scheduleAtFixedRate(initialDelay: Duration, interval: Duration, task: () -> Unit): ScheduledTask {
        Timer.repeat(initialDelay, interval, task, style: CatchupStyle.Burst)
    }
}
