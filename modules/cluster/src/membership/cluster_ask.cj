/*
 * CActor Cluster - 集群工具的 ask（E12′）
 *
 * 分片与单例透传 sender 之后（N16），ask 就是"以一次性回复引用为 sender 发出请求"：
 * 实体 / 单例照常 context.reply；目标在其他节点时，回复引用经远程层登记为回复入口，
 * 回复沿网络直接回到发起方（多跳转发也不经中间节点），Promise 完成后入口被清扫注销。
 */
package cactor_cluster.membership

import std.sync.*
import std.time.*
import cactor_core.actor.{ActorRef}
import cactor_core.message.{Message}
import cactor_core.typed.{TypedPromise, PromiseActorRef, TypedAskTimeoutException}

let clusterAskSeq = AtomicInt64(0)

func discardCompleted(_: Bool): Unit {}

/**
 * 以一次性回复引用为 sender 调用 send，返回回复的 Promise；timeout 内没有回复时以 TypedAskTimeoutException 失败。
 * what 只用于错误信息
 */
func askVia(what: String, timeout: Duration, send: (ActorRef) -> Unit): TypedPromise<Message> {
    let raw = TypedPromise<Any>()
    let replyTo = PromiseActorRef(raw, "/temp/cluster-ask-${clusterAskSeq.fetchAdd(1) + 1}")
    let timer = Timer.once(timeout, { =>
        raw.tryFailure(TypedAskTimeoutException("ask ${what} 在 ${timeout} 内没有回复")) |> discardCompleted
    })
    raw.onComplete({ _ => timer.cancel() })
    try {
        send(replyTo)
    } catch (e: Exception) {
        raw.tryFailure(e) |> discardCompleted
    }
    raw.map<Message>({ a =>
        match (a) {
            case m: Message => m
            case _ => throw IllegalStateException("ask ${what} 的回复不是 Message")
        }
    })
}
