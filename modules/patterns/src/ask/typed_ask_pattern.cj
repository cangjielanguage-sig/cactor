/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 类型化 ask 的结果与 Future（独立的完成值类型）
 *
 * C10 / E38（docs/plan/20261106.md）：本文件原有的 TypedAskPattern / TypedAsk 是"模拟发送"的实现——
 * 请求不等待回复，一律以超时结束，@Deprecated 之后仍被当作可用 API 留着。类型化 Actor 的回复走消息里的
 * replyTo，这一层根本拿不到回复，无法修成真的，因此删除。请改用：
 *   - Actor 外：cactor_core.typed.TypedAsk.ask<Req, Res>(ref, { r => Req(.., r) }, timeout)
 *   - Actor 内：ctx.ask(target, factory, timeout) { outcome => ... }
 *   - 经典 ActorRef：cactor_core.dsl.AskInbox.ask(target, message).get(timeout)
 * TypedResult / TypedAskFuture 是通用的完成值类型（线程安全、只完成一次），保留。
 */
package cactor_patterns.ask

import std.time.*

/**
 * 类型安全的Ask结果
 */
public enum TypedResult<T> {
    | Success(T)
    | Failure(Exception)
    | Timeout(String)
}



/**
 * 类型安全的Ask Future
 * 提供类型安全的异步结果处理
 */
public class TypedAskFuture<T> {
    private let requestId: String
    private let timeout: Duration
    private let startTime: MonoTime
    private let completion: Completion<TypedResult<T>>

    public init(requestId: String, timeout: Duration) {
        this.requestId = requestId
        this.timeout = timeout
        this.startTime = MonoTime.now()
        this.completion = Completion<TypedResult<T>>()
    }

    /**
     * 完成Future，设置成功结果（只有第一次完成生效）
     */
    public func complete(value: T): Unit {
        completion.tryComplete(TypedResult.Success(value))
    }

    /**
     * 完成Future，设置错误结果
     */
    public func completeWithError(error: Exception): Unit {
        completion.tryComplete(TypedResult.Failure(error))
    }

    /**
     * 完成Future，设置超时结果
     */
    public func completeWithTimeout(): Unit {
        completion.tryComplete(TypedResult.Timeout(requestId))
    }

    /**
     * 阻塞等待结果，直到完成或超时（Condition 通知）
     */
    public func get(): TypedResult<T> {
        let remaining = timeout - (MonoTime.now() - startTime)
        let wait = if (remaining > Duration.Zero) { remaining } else { Duration.Zero }
        match (completion.await(wait)) {
            case Some(res) => res
            case None => TypedResult.Timeout(requestId)
        }
    }

    /**
     * 注册完成回调；已完成时立即执行
     */
    public func onComplete(handler: (TypedResult<T>) -> Unit): Unit {
        completion.onComplete(handler)
    }

    public func isCompleted(): Bool {
        completion.isDone()
    }

    public func isTimedOut(): Bool {
        MonoTime.now() - startTime > timeout
    }

    public func getRequestId(): String {
        requestId
    }
}
