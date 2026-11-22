/*
 * CActor Patterns.Backpressure - 显式背压引用（C2-b）
 *
 * 普通 tell 只有"邮箱满了才失败"的局部策略，上游看不到"下游现在忙不忙"。这里提供一个
 * 非阻塞的 offer：在途（目标邮箱积压）达到水位时返回 Backpressured(inFlight, limit)，
 * 由调用方自适应降速/落盘，而不是盲目重试或让内存无界增长。
 *
 * 语义与 cluster 的 SinkRefProducer.offer 一致（Enqueued / Backpressured / Closed），
 * 但作用在任意本地 ActorRef 上：不做隐式阻塞发送，不改变目标 Actor 的调度。
 *
 * 限制：远程/包装引用的 pendingCount 为 -1（无法本地观测）。这类目标请改用
 * StreamRef 的信用背压；本类对它们退化为"直接发送 + Enqueued"并在文档中写明。
 */
package cactor_patterns.backpressure

import std.time.*
import cactor_core.actor.{ActorRef}
import cactor_core.message.{Message}
import cactor_core.dsl.{AskInbox}

/** offer 的结果：容量信息随失败一起返回，便于上游按 limit 调整速率（对标 Rivet 的 QueueFull{limit,capacity}） */
public enum OfferResult {
    /** 已入队 */
    | Enqueued
    /** 在途已达水位：(当前在途, 上限)；消息未被接收 */
    | Backpressured(Int64, Int64)
    /** 目标已停止 */
    | Closed
}

extend OfferResult {
    public func isEnqueued(): Bool {
        match (this) {
            case Enqueued => true
            case _ => false
        }
    }

    public func isBackpressured(): Bool {
        match (this) {
            case Backpressured(_, _) => true
            case _ => false
        }
    }

    public func isClosed(): Bool {
        match (this) {
            case Closed => true
            case _ => false
        }
    }

    /** 在途消息数（仅 Backpressured 携带；其余为 0） */
    public func inFlight(): Int64 {
        match (this) {
            case Backpressured(n, _) => n
            case _ => 0
        }
    }

    /** 水位上限（仅 Backpressured 携带；其余为 0） */
    public func limit(): Int64 {
        match (this) {
            case Backpressured(_, l) => l
            case _ => 0
        }
    }

    /** 容量（与 limit 同义；F5 要求背压错误统一携带容量） */
    public func capacity(): Int64 {
        limit()
    }

    public func toString(): String {
        match (this) {
            case Enqueued => "Enqueued"
            case Backpressured(n, l) => "Backpressured(${n}/${l})"
            case Closed => "Closed"
        }
    }
}

/**
 * 有界在途的发送引用（C2-b）。
 *
 * ```cangjie
 * let bp = BackpressuredRef<MyMsg>(target, limit: 256)
 * var i = 0
 * while (i < n) {
 *     match (bp.offer(messages[i])) {
 *         case Enqueued => i++
 *         case Backpressured(_, _) => bp.whenWritable(1.second)   // 降速后重试同一条
 *         case Closed => break
 *     }
 * }
 * ```
 */
public class BackpressuredRef<T> where T <: Message {
    private let target: ActorRef
    private let limit: Int64

    public init(target: ActorRef, limit!: Int64 = 1024) {
        if (limit <= 0) {
            throw IllegalArgumentException("limit 必须为正数: ${limit}")
        }
        this.target = target
        this.limit = limit
    }

    /**
     * 尝试投递一条消息。不阻塞：
     * - 目标已停止 → Closed；
     * - 本地积压 >= limit → Backpressured(当前积压, limit)，消息未入队；
     * - 否则入队并返回 Enqueued。
     * 远程/包装引用无法观测积压（pendingCount < 0）时直接发送并返回 Enqueued。
     * 本地引用的 pendingCount 是跨线程近似计数（读侧可能看到滞后的消费进度），
     * 会短暂高估 1–2 条，水位判断因此是"保守"的（宁可早一点返回 Backpressured）。
     */
    public func offer(message: T): OfferResult {
        if (target.isTerminated()) {
            return OfferResult.Closed
        }
        let pending = target.pendingCount()
        if (pending < 0) {
            target.tell(message)
            return OfferResult.Enqueued
        }
        if (pending >= limit) {
            return OfferResult.Backpressured(pending, limit)
        }
        target.tell(message)
        OfferResult.Enqueued
    }

    /** 当前在途（目标邮箱积压；不可观测时为 0） */
    public func inFlight(): Int64 {
        let p = target.pendingCount()
        if (p < 0) { 0 } else { p }
    }

    /** 水位上限 */
    public func capacity(): Int64 {
        limit
    }

    /**
     * 等待到可写（在途 < limit 或目标停止）；超时返回 false。
     * 阻塞等待只能在 Actor 之外使用；Actor 内请用定时器/pipeToSelf。
     */
    public func whenWritable(timeout: Duration): Bool {
        let deadline = MonoTime.now() + timeout
        while (MonoTime.now() < deadline) {
            if (target.isTerminated()) {
                return true
            }
            if (inFlight() < limit) {
                return true
            }
            sleep(Duration.millisecond)
        }
        target.isTerminated() || inFlight() < limit
    }

    public func targetRef(): ActorRef {
        target
    }
}

/** 下游在 wait 之后仍处于背压水位，ask 未发送（区别于"发出后超时"） */
public class AskBackpressuredException <: Exception {
    public let inFlight: Int64
    public let limit: Int64

    public init(inFlight: Int64, limit: Int64) {
        super("下游忙（在途 ${inFlight}/${limit}），ask 未发送；请降速后重试，或对该目标改用 StreamRef 信用背压")
        this.inFlight = inFlight
        this.limit = limit
    }
}

/**
 * 带背压的经典 ask（C2 组合接口，与 `AskInbox.ask` 用法一致）：
 * 先按水位判断——在途未达 `limit`（或目标已停止）才把消息投给 target 并返回收件箱；
 * 水位已满时最多等 `writableTimeout`，仍不可写则抛 `AskBackpressuredException`，
 * 而不是让请求在邮箱里排队到 ask 超时（那时错误信息只剩"没回信"，看不出是背压）。
 *
 * ```cangjie
 * let reply = askWithBackpressure(worker, Query("x"), limit: 64).get(1.second)
 * ```
 *
 * 阻塞等待（`whenWritable` 与随后的 `get`）只能在 Actor 之外使用；Actor 内请用定时器/pipeToSelf。
 */
public func askWithBackpressure(target: ActorRef, message: Message,
    limit!: Int64 = 1024,
    writableTimeout!: Duration = Duration.second): AskInbox {
    let ref = BackpressuredRef<Message>(target, limit: limit)
    if (!ref.whenWritable(writableTimeout)) {
        throw AskBackpressuredException(ref.inFlight(), limit)
    }
    AskInbox.ask(target, message)
}
