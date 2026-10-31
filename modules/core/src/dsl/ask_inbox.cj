/*
 * CActor Core.DSL - 经典路径的 ask 临时收件箱（对照 docs/plan/dsl20261030.md §7.1「Ask 语法糖」）
 *
 * 设计取舍（为什么不是"包装消息的一行式 ask"）：
 *   patterns 的 Ask.ask 会把用户消息包进 AskMessage，要求目标 Actor 配合解包；
 *   这里提供零侵入的替代：请求消息自带 replyTo 字段（DSL 的标准写法），把收件箱
 *  当作普通 ActorRef 塞进 replyTo 即可，目标 Actor 不需要任何配合。
 *
 *   // 目标 Actor（@actor）：onQuery 里 m.replyTo << Answer(...)
 *   let inbox = askInbox()
 *   service << Query("今天天气如何", inbox)
 *   let answer = (inbox.get(3.seconds) as Answer).getOrThrow()
 *
 * 与另外两条 ask 主路径的关系（一条主路径，各管一段）：
 *   - 类型化进程外：TypedAsk.ask<Req, Res>(ref, factory, timeout)
 *   - 类型化 Actor 内：ctx.ask(target, factory, timeout) { outcome => ... }
 *   - 经典路径：askInbox()（本文件，零侵入）；patterns 的 Ask 保留兼容
 *
 * 超时统一抛 cactor_core.typed.TypedAskTimeoutException：三条路径共用一个超时异常，
 * 不再新增同名的 AskTimeoutException（cactor_patterns.ask 已有一个，经 cactor.* 门面
 * 一起导入时会产生名字歧义）。
 */
package cactor_core.dsl

import std.sync.*
import std.collection.*
import std.time.*
import cactor_core.actor.ActorPath
import cactor_core.actor.ActorRef
import cactor_core.message.Message
import cactor_core.typed.TypedAskTimeoutException

/** 收件箱的阻塞队列（Mutex + Condition，避免引入额外依赖） */
class InboxQueue {
    private let lock = Mutex()
    private let cond: Condition
    private let items = ArrayList<Message>()

    init() {
        this.cond = synchronized(lock) { lock.condition() }
    }

    func put(message: Message): Unit {
        synchronized(lock) {
            items.add(message)
            cond.notifyAll()
        }
    }

    /** 阻塞取一条；超时返回 None */
    func take(timeout: Duration): Option<Message> {
        // 单调时钟：墙钟被 NTP 回拨/跳变时不会提前超时或多等
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (items.isEmpty()) {
                let remaining = deadline - MonoTime.now()
                if (remaining <= Duration.Zero) {
                    return None<Message>
                }
                cond.wait(timeout: remaining)
            }
            return Some(items.remove(at: 0))
        }
    }

    func size(): Int64 {
        synchronized(lock) { Int64(items.size) }
    }
}

/**
 * ask 临时收件箱：一个进程内的"临时 ActorRef"，只收不发。
 * 把它填进请求消息的 replyTo 字段，然后 get(timeout) 等第一条回信。
 */
public class AskInbox <: ActorRef {
    // 进程内递增序号：equals/hashCode 按路径判等，路径必须唯一。
    // 不用时间戳——同一时钟刻度内创建的两个收件箱会撞路径，被当成同一个 ActorRef。
    private static let seq: AtomicInt64 = AtomicInt64(0)

    private let queue: InboxQueue
    private let actorPath: ActorPath

    public init() {
        this.queue = InboxQueue()
        this.actorPath = ActorPath("ask-inbox", "/temp/inbox-${seq.fetchAdd(1) + 1}")
    }

    public prop path: ActorPath {
        get() {
            actorPath
        }
    }

    /** 收到回复（由回复方经 `replyTo << message` 触发） */
    public func tell(message: Message): Unit {
        queue.put(message)
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        tell(message)
    }

    public func equals(other: ActorRef): Bool {
        other.path.getPath() == actorPath.getPath()
    }

    public func hashCode(): Int32 {
        Int32(actorPath.getPath().hashCode() & 0x7FFFFFFF)
    }

    public func toString(): String {
        actorPath.toString()
    }

    /** 已收到的回复数（测试与诊断用） */
    public func received(): Int64 {
        queue.size()
    }

    /** 阻塞等待第一条回复；超时抛 TypedAskTimeoutException */
    public func get(timeout: Duration): Message {
        match (queue.take(timeout)) {
            case Some(m) => m
            case None => throw TypedAskTimeoutException("ask 收件箱 ${actorPath.getPath()} 在 ${timeout} 内没有收到回复")
        }
    }

    /** 阻塞等待第一条回复；超时返回 None */
    public func tryGet(timeout: Duration): Option<Message> {
        queue.take(timeout)
    }
}

/** 创建一个 ask 临时收件箱 */
public func askInbox(): AskInbox {
    AskInbox()
}
