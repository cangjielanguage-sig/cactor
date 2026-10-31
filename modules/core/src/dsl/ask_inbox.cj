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
 */
package cactor_core.dsl

import std.sync.*
import std.collection.*
import std.time.*
import cactor_core.actor.ActorPath
import cactor_core.actor.ActorRef
import cactor_core.message.Message

/** ask 超时异常 */
public class AskTimeoutException <: Exception {
    public init(timeout: Duration) {
        super("ask 收件箱在 ${timeout} 内没有收到回复")
    }
}

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
        let deadline = DateTime.now() + timeout
        synchronized(lock) {
            while (items.isEmpty()) {
                let remaining = deadline - DateTime.now()
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
    private let queue: InboxQueue
    private let actorPath: ActorPath
    private let counter: AtomicInt64

    public init() {
        this.queue = InboxQueue()
        // 每个收件箱一个稳定且不与真实 Actor 冲突的路径前缀
        this.counter = AtomicInt64(DateTime.now().toUnixTimeStamp().toNanoseconds())
        this.actorPath = ActorPath("ask-inbox", "/temp/inbox-${counter.load()}")
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

    /** 阻塞等待第一条回复；超时抛 AskTimeoutException */
    public func get(timeout: Duration): Message {
        match (queue.take(timeout)) {
            case Some(m) => m
            case None => throw AskTimeoutException(timeout)
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
