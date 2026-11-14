/*
 * CActor Streams - 跨节点 ask 的占位回复引用
 *
 * ask 算子面向进程内 TypedAsk.ask：createRequest 拿到 replyTo 把请求消息建好，回复直接 tell 回来。
 * 跨节点时回复引用过不了线，改走 TypedAsk.askRemote（回复经信封 sender 回来），请求消息里没有引用字段。
 * 但 createRequest 的签名要求一个 TypedActorRef<Res>——跨节点场景下用户通常用 context.reply 应答，
 * 这个占位引用只是让请求能建出来；如果远端把它回发到本进程，它不指向任何存活 Actor，回复会按超时失败。
 */
package cactor_streams

import std.sync.*
import cactor_core.typed.*

/**
 * 永不投递的占位引用：tell 只计数后丢弃（跨节点场景说明远端没用 reply，属于使用错误，
 * 不能在远端抛异常炸掉它的 Actor——错误应该在发起方以 ask 超时呈现）。
 */
public class PromiseRefPlaceholder<T> <: TypedActorRef<T> {
    let lock = Mutex()
    var misdirected: Int64 = 0

    public func tell(message: T): Unit {
        synchronized(lock) {
            misdirected++
        }
    }

    /** 占位被回发的次数（不为 0 说明远端没用 context.reply，排障入口） */
    public func misdirectedCount(): Int64 {
        synchronized(lock) {
            misdirected
        }
    }

    public func pathString(): String {
        "/temp/stream-ask-placeholder"
    }

    public func equals(other: TypedActorRef<T>): Bool {
        other.pathString() == pathString()
    }
}
