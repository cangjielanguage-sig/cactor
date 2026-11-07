/*
 * CActor Foundation - 限速日志闸门
 *
 * 对标 Akka 的 log-dead-letters = 10 + log-dead-letters-suspend-duration = 5min：
 * 每个时间窗口逐条记前 limit 条，第 limit 条附注"之后不再逐条记录"，其余只计数；
 * 窗口结束后第一条事件先补一行上个窗口略去的条数，再开始新窗口。
 * 用于死信、未处理消息、类型化协议不符这类"可能成千上万、但前几条最有诊断价值"的事件：
 * 逐条记会刷屏并拖慢热路径，完全不记则误投 / 协议错误静默消失。
 *
 * 热路径只有原子操作（无锁）：窗口切换用 CAS 选出一个线程做汇总，计数允许在切换瞬间有一两条的误差
 */
package cactor_foundation.logging

import std.sync.*
import std.time.*

let RATE_LIMIT_EPOCH: MonoTime = MonoTime.now()

public class LogRateLimiter {
    private let limit: Int64
    private let window: Duration
    private let windowNanos: Int64
    private let windowStart: AtomicInt64
    private let count = AtomicInt64(0)

    /** limit <= 0 表示完全不记 */
    public init(limit: Int64, window: Duration) {
        this.limit = limit
        this.window = window
        this.windowNanos = window.toNanoseconds()
        this.windowStart = AtomicInt64(nowNanos())
    }

    /**
     * 记一次事件：在配额内调用 emit(message())，否则只计数。
     * message 是闭包——被略去的事件不做字符串拼接
     */
    public func tryLog(emit: (String) -> Unit, message: () -> String): Unit {
        if (limit <= 0) {
            return
        }
        let now = nowNanos()
        let start = windowStart.load()
        if (now - start >= windowNanos && windowStart.compareAndSwap(start, now)) {
            let previous = count.swap(0)
            if (previous > limit) {
                emit("（上一个 ${window} 窗口另有 ${previous - limit} 条同类事件未记录）")
            }
        }
        let n = count.fetchAdd(1)
        if (n < limit - 1) {
            emit(message())
        } else if (n == limit - 1) {
            emit("${message()}（已记 ${limit} 条，${window} 内不再逐条记录）")
        }
    }

    /** 当前窗口内的事件数（含被略去的） */
    public func countInWindow(): Int64 {
        count.load()
    }

    private static func nowNanos(): Int64 {
        (MonoTime.now() - RATE_LIMIT_EPOCH).toNanoseconds()
    }
}
