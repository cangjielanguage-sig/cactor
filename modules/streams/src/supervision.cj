/*
 * CActor Streams - 流内监督、溢出策略与流异常
 *
 * 流内监督（对标 Akka Streams 的 Supervision.Decider）只作用于执行用户函数的阶段：
 * map / filter / mapConcat / collect / takeWhile / dropWhile / scan / fold / mapAsync / ask / Sink.foreach / Sink.fold。
 * - Stop：阶段失败，下游收到 onError，上游被取消（默认）
 * - Resume：丢弃出错的元素，继续处理后续元素，保留阶段状态
 * - Restart：丢弃出错的元素，并把有状态的阶段（scan / fold / grouped 等）重置为初始状态后继续
 */
package cactor_streams

/** 监督决定 */
public enum SupervisionDirective {
    | Stop
    | Resume
    | Restart

    public func toString(): String {
        match (this) {
            case Stop => "Stop"
            case Resume => "Resume"
            case Restart => "Restart"
        }
    }
}

/** 按异常决定如何处理（决定函数必须是纯函数，可能在任意岛线程被调用） */
public type Decider = (Exception) -> SupervisionDirective

/** 常用决定函数 */
public struct StreamSupervision {
    /** 任何异常都让阶段失败（默认） */
    public static let stoppingDecider: Decider = { _ => SupervisionDirective.Stop }

    /** 任何异常都跳过出错元素 */
    public static let resumingDecider: Decider = { _ => SupervisionDirective.Resume }

    /** 任何异常都跳过出错元素并重置阶段状态 */
    public static let restartingDecider: Decider = { _ => SupervisionDirective.Restart }
}

/**
 * 缓冲区满时的处理策略（buffer / Source.queue / Source.actorRef）
 */
public enum OverflowStrategy {
    /** 不再接收新元素，向上游施加背压（Source.queue 中表现为 offer 的 Promise 推迟完成） */
    | Backpressure
    /** 丢弃最旧的元素，为新元素腾位置 */
    | DropHead
    /** 丢弃最新入队的元素，为新元素腾位置 */
    | DropTail
    /** 丢弃整个缓冲区 */
    | DropBuffer
    /** 丢弃新元素 */
    | DropNew
    /** 以 BufferOverflowException 让流失败 */
    | Fail

    public func toString(): String {
        match (this) {
            case Backpressure => "Backpressure"
            case DropHead => "DropHead"
            case DropTail => "DropTail"
            case DropBuffer => "DropBuffer"
            case DropNew => "DropNew"
            case Fail => "Fail"
        }
    }
}

/** 缓冲区溢出（OverflowStrategy.Fail） */
public class BufferOverflowException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** 元素数超过了收集上限（Sink.seq(maxElements)） */
public class StreamLimitReachedException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** 流为空却需要一个元素（Sink.head） */
public class EmptyStreamException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** Source.queue / Source.actorRef 的 offer 结果 */
public enum QueueOfferResult {
    /** 已进入缓冲区（DropHead / DropTail / DropBuffer 腾位后入队也算） */
    | Enqueued
    /** 缓冲区满，新元素被丢弃（DropNew） */
    | Dropped
    /** 流已失败，或者这次 offer 本身导致流失败（Fail 策略、并发 offer 过多） */
    | QueueFailure(Exception)
    /** 队列已完成或下游已取消 */
    | QueueClosed

    public func isEnqueued(): Bool {
        match (this) {
            case Enqueued => true
            case _ => false
        }
    }

    public func toString(): String {
        match (this) {
            case Enqueued => "Enqueued"
            case Dropped => "Dropped"
            case QueueFailure(e) => "Failure(${e.message})"
            case QueueClosed => "QueueClosed"
        }
    }
}
