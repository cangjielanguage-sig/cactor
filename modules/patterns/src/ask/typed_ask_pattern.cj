/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 类型安全Ask模式（早期模拟实现，已废弃）
 *
 * P2 ask 收敛（dsl20261030.md §9 P2.3）：本文件里的 TypedAskPattern / TypedAsk 是
 * 早期"模拟发送"的实现（请求并不真正等待回复，一律以超时结束），与真正可用的
 * typed ask 语义不一致。为避免三套 ask 并存造成困惑：
 *   - 本文件的 TypedAskPattern / TypedAsk 标记为 @Deprecated；
 *   - 唯一推荐入口：cactor_core.typed.TypedAsk.ask（进程外）
 *     与 TypedActorContext.ask（Actor 内，响应回到自己的消息流）；
 *   - 经典 ActorRef 路径保留 cactor_patterns.ask.Ask / AskPatternManager。
 * 旧 API 行为保持不变（仅加标注），一个版本周期后再考虑移除。
 */
package cactor_patterns.ask

import std.time.*
import cactor_core.actor.{ActorRef, ActorPath}
import cactor_core.message.{Message}
import cactor_core.typed.{TypedActorRef}
import cactor_foundation.logging.{Logger, Logging}

let log_typed_ask_pattern: Logger = Logging.logger("cactor_patterns.ask.typed_ask_pattern")
// 避免循环依赖，直接使用相对导入

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

/**
 * 类型安全的Ask模式管理器
 * 简化实现（模拟发送），已被 cactor_core.typed.TypedAsk 取代
 */
@Deprecated["TypedAskPattern 是模拟实现，请改用 cactor_core.typed.TypedAsk.ask 或 ctx.ask", since: "1.2.0"]
public class TypedAskPattern {
    private let defaultTimeout: Duration

    public init() {
        this.defaultTimeout = Duration.second * 5
    }

    public init(defaultTimeout: Duration) {
        this.defaultTimeout = defaultTimeout
    }

    /**
     * 类型安全的Ask方法
     */
    @Deprecated["请改用 cactor_core.typed.TypedAsk.ask 或 ctx.ask", since: "1.2.0"]
    public func ask<T>(target: ActorRef, message: Message): TypedAskFuture<T> {
        askInternal<T>(target, message, defaultTimeout)
    }

    @Deprecated["请改用 cactor_core.typed.TypedAsk.ask 或 ctx.ask", since: "1.2.0"]
    public func ask<T>(target: ActorRef, message: Message, timeout: Duration): TypedAskFuture<T> {
        askInternal<T>(target, message, timeout)
    }

    /** 实际实现（不对外暴露，避免内部调用触发废弃告警） */
    func askInternal<T>(target: ActorRef, message: Message, timeout: Duration): TypedAskFuture<T> {
        let requestId = generateRequestId()
        let typedFuture = TypedAskFuture<T>(requestId, timeout)

        log_typed_ask_pattern.debug("[TYPED-ASK] 发送类型安全Ask请求: " + requestId)
        log_typed_ask_pattern.debug("[TYPED-ASK] 目标Actor: " + target.path.toString())
        log_typed_ask_pattern.debug("[TYPED-ASK] 消息类型: " + message.messageType())

        // 简化实现：模拟Ask模式发送请求
        // 实际实现中应该集成真正的AskPattern
        log_typed_ask_pattern.debug("[TYPED-ASK] 模拟发送Ask请求到Actor")

        // 模拟立即完成（避免复杂的类型转换）
        typedFuture.completeWithTimeout()  // 简化为超时，避免类型转换问题

        typedFuture
    }

    /**
     * 类型安全的TypedActorRef Ask方法
     */
    @Deprecated["请改用 cactor_core.typed.TypedAsk.ask 或 ctx.ask", since: "1.2.0"]
    public func askTyped<T, R>(target: TypedActorRef<T>, message: T): TypedAskFuture<R> {
        askTypedInternal<T, R>(target, message, defaultTimeout)
    }

    @Deprecated["请改用 cactor_core.typed.TypedAsk.ask 或 ctx.ask", since: "1.2.0"]
    public func askTyped<T, R>(target: TypedActorRef<T>, message: T, timeout: Duration): TypedAskFuture<R> {
        askTypedInternal<T, R>(target, message, timeout)
    }

    /** 实际实现（不对外暴露） */
    func askTypedInternal<T, R>(target: TypedActorRef<T>, message: T, timeout: Duration): TypedAskFuture<R> {
        let requestId = generateRequestId()
        let typedFuture = TypedAskFuture<R>(requestId, timeout)

        log_typed_ask_pattern.debug("[TYPED-ASK] 发送类型安全Ask请求: " + requestId)
        log_typed_ask_pattern.debug("[TYPED-ASK] 目标TypedActor")
        log_typed_ask_pattern.debug("[TYPED-ASK] 消息发送")

        // 使用TypedActorRef发送消息
        target.tell(message)

        // 模拟异步响应处理
        // 实际实现中应该通过TypedActorSystem处理响应
        typedFuture.completeWithTimeout()  // 简化为超时，避免类型转换问题

        typedFuture
    }

    /**
     * 处理类型安全的响应
     */
    public func handleTypedResponse<T>(requestId: String, response: T): Unit {
        log_typed_ask_pattern.debug("[TYPED-ASK] 处理类型安全响应: " + requestId)
        log_typed_ask_pattern.debug("[TYPED-ASK] 响应已处理")
        // 实际实现中应该查找并完成对应的Future
    }

    /**
     * 启动和停止
     */
    public func start(): Unit {
        log_typed_ask_pattern.debug("TypedAskPattern启动")
    }

    public func stop(): Unit {
        log_typed_ask_pattern.debug("TypedAskPattern停止")
        // 实际实现中应该清理待处理的请求
    }

    private func generateRequestId(): String {
        let timestamp = DateTime.now().toUnixTimeStamp().toMilliseconds()
        "typed-ask-" + timestamp.toString()
    }
}

/**
 * 类型安全Ask的静态工具方法（早期模拟实现，已废弃）
 */
@Deprecated["请改用 cactor_core.typed.TypedAsk.ask 或 ctx.ask", since: "1.2.0"]
public struct TypedAsk {
    private static let instance: TypedAskPattern = TypedAskPattern()

    /**
     * 静态Ask方法
     */
    @Deprecated["请改用 cactor_core.typed.TypedAsk.ask 或 ctx.ask", since: "1.2.0"]
    public static func ask<T>(target: ActorRef, message: Message): TypedAskFuture<T> {
        instance.askInternal<T>(target, message, Duration.second * 5)
    }

    @Deprecated["请改用 cactor_core.typed.TypedAsk.ask 或 ctx.ask", since: "1.2.0"]
    public static func ask<T>(target: ActorRef, message: Message, timeout: Duration): TypedAskFuture<T> {
        instance.askInternal<T>(target, message, timeout)
    }

    /**
     * 类型安全的TypedActor Ask
     */
    @Deprecated["请改用 cactor_core.typed.TypedAsk.ask 或 ctx.ask", since: "1.2.0"]
    public static func askTyped<T, R>(target: TypedActorRef<T>, message: T): TypedAskFuture<R> {
        instance.askTypedInternal<T, R>(target, message, Duration.second * 5)
    }

    @Deprecated["请改用 cactor_core.typed.TypedAsk.ask 或 ctx.ask", since: "1.2.0"]
    public static func askTyped<T, R>(target: TypedActorRef<T>, message: T, timeout: Duration): TypedAskFuture<R> {
        instance.askTypedInternal<T, R>(target, message, timeout)
    }
}
