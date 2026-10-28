/*
 * CActor - PriorityMailbox (same package to avoid circular dep)
 */
package cactor_runtime.mailbox

import std.sync.*
import std.collection.*
import cactor_core.message.{Message, Envelope}
import std.collection.concurrent.*

/**
 * 简单优先级邮箱实现：
 * - 高优先级：SystemMessage 或 message.priority() > 0
 * - 低优先级：message.priority() < 0 或类型名含 "Low"
 * - 其他：普通优先级
 * 采用三个 FIFO 子队列，出队顺序：高 -> 普通 -> 低
 */
// 兼容代理：统一指向 queue.PriorityQueueMailbox 实现，避免重复代码
// 注意：为避免跨包循环依赖，这里不直接import queue包；而是内联一个轻量实现，语义保持一致
public class PriorityMailbox <: Mailbox {
  private let highQ: SimpleMailbox
  private let normQ: SimpleMailbox
  private let lowQ: SimpleMailbox

  public init() {
    this.highQ = SimpleMailbox(0)
    this.normQ = SimpleMailbox(0)
    this.lowQ = SimpleMailbox(0)
  }

  public func enqueue(envelope: Envelope): Bool {
    let msg = envelope.getMessage()
    if (ControlClassifier.isControl(msg)) {
      return highQ.enqueue(envelope)
    }
    let t = msg.messageType()
    if (t.contains("Low")) { return lowQ.enqueue(envelope) }
    return normQ.enqueue(envelope)
  }

  public func dequeue(): Option<Envelope> {
    match (highQ.dequeue()) { case Some(e) => return Some(e) case None => () }
    match (normQ.dequeue()) { case Some(e) => return Some(e) case None => () }
    lowQ.dequeue()
  }

  public func isEmpty(): Bool { highQ.isEmpty() && normQ.isEmpty() && lowQ.isEmpty() }
  public func size(): Int64 { highQ.size() + normQ.size() + lowQ.size() }
  public func clear(): Unit { highQ.clear(); normQ.clear(); lowQ.clear() }
}

/**
 * 优先级邮箱工厂
 * 用于创建基于队列的优先级邮箱，避免循环依赖
 */
public class PriorityMailboxFactory {
    /**
     * 创建基于队列的优先级邮箱
     * 直接实现queue/PriorityQueueMailbox的逻辑，避免循环依赖
     */
    public static func createQueueBasedPriorityMailbox(): Mailbox {
        QueueBasedPriorityMailbox()
    }
}

/**
 * 基于队列的优先级邮箱实现
 * 与queue/PriorityQueueMailbox逻辑完全一致，避免循环依赖
 */
public class QueueBasedPriorityMailbox <: Mailbox {
    private let highPriorityQueue: QueueMailbox
    private let normalPriorityQueue: QueueMailbox
    private let lowPriorityQueue: QueueMailbox
    private let closed: AtomicBool

    public init() {
        this.highPriorityQueue = QueueMailbox()
        this.normalPriorityQueue = QueueMailbox()
        this.lowPriorityQueue = QueueMailbox()
        this.closed = AtomicBool(false)
    }

    public func enqueue(envelope: Envelope): Bool {
        if (closed.load()) {
            return false
        }

        // 根据消息类型确定优先级（集中使用 ControlClassifier）
        let msg = envelope.getMessage()
        if (ControlClassifier.isControl(msg)) {
            return highPriorityQueue.enqueue(envelope)
        } else if (msg.messageType().contains("Low")) {
            return lowPriorityQueue.enqueue(envelope)
        } else {
            return normalPriorityQueue.enqueue(envelope)
        }
    }

    public func dequeue(): Option<Envelope> {
        // 按优先级顺序处理：高 -> 普通 -> 低
        match (highPriorityQueue.dequeue()) {
            case Some(envelope) => Some(envelope)
            case None =>
                match (normalPriorityQueue.dequeue()) {
                    case Some(envelope) => Some(envelope)
                    case None => lowPriorityQueue.dequeue()
                }
        }
    }

    public func isEmpty(): Bool {
        highPriorityQueue.isEmpty() &&
        normalPriorityQueue.isEmpty() &&
        lowPriorityQueue.isEmpty()
    }

    public func size(): Int64 {
        highPriorityQueue.size() +
        normalPriorityQueue.size() +
        lowPriorityQueue.size()
    }

    public func clear(): Unit {
        highPriorityQueue.clear()
        normalPriorityQueue.clear()
        lowPriorityQueue.clear()
    }

    public func isClosed(): Bool {
        closed.load()
    }

    public func close(): Unit {
        closed.store(true)
        highPriorityQueue.close()
        normalPriorityQueue.close()
        lowPriorityQueue.close()
    }
}

/**
 * 基于仓颉NonBlockingQueue的邮箱
 * 内联实现，避免循环依赖
 */
public class QueueMailbox <: Mailbox {
    private let queue: ConcurrentLinkedQueue<Envelope>
    private let closed: AtomicBool

    public init() {
        this.queue = ConcurrentLinkedQueue<Envelope>()
        this.closed = AtomicBool(false)
    }

    public func enqueue(envelope: Envelope): Bool {
        if (closed.load()) {
            return false
        }
        return queue.add(envelope)
    }

    public func dequeue(): Option<Envelope> {
        queue.remove()
    }

    public func isEmpty(): Bool {
        queue.size == 0
    }

    public func size(): Int64 {
        queue.size
    }

    public func clear(): Unit {
        while (!isEmpty()) {
            queue.remove()
        }
    }

    public func isClosed(): Bool {
        closed.load()
    }

    public func close(): Unit {
        closed.store(true)
    }
}

