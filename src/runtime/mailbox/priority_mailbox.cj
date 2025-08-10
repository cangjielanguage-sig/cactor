/*
 * CActor - PriorityMailbox (same package to avoid circular dep)
 */
package cactor.runtime.mailbox

import std.sync.*
import std.collection.*
import cactor.core.message.{Message, Envelope}

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

