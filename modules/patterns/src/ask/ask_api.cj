/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 改进的Ask模式API
 * Plan20阶段1：Ask模式完善 - 提供类型安全、高性能的Ask API
 */
package cactor_patterns.ask

import std.time.*
import cactor_core.actor.{ActorRef}
import cactor_core.message.{Message, StringMessage}

/**
 * Ask模式统一API - 参考Akka设计
 *
 * 提供类型安全的Ask操作，支持：
 * 1. 类型安全的请求-响应
 * 2. 超时控制
 * 3. 异步回调处理
 * 4. 错误处理和恢复
 */
public struct Ask {
    private static let defaultManager: AskPatternManager = AskPatternManager(Duration.second * 5)

    // 静态初始化（仓颉语言不支持static块，改为延迟初始化）
    private static var initialized: Bool = false

    private static func ensureInitialized(): Unit {
        if (!initialized) {
            defaultManager.start()
            initialized = true
        }
    }

    /**
     * 发送Ask请求（使用默认超时）
     *
     * @param target 目标Actor
     * @param message 请求消息
     * @return Future对象，可用于获取响应
     */
    public static func ask(target: ActorRef, message: Message): AskFuture<Message> {
        ensureInitialized()
        ask(target, message, Duration.second * 5)
    }

    /**
     * 发送Ask请求（指定超时）
     *
     * @param target 目标Actor
     * @param message 请求消息
     * @param timeout 超时时间
     * @return Future对象，可用于获取响应
     */
    public static func ask(target: ActorRef, message: Message, timeout: Duration): AskFuture<Message> {
        ensureInitialized()
        defaultManager.ask(target, message, timeout)
    }

    /**
     * 类型安全的Ask请求（简化版本）
     *
     * @param target 目标Actor
     * @param message 请求消息
     * @param timeout 超时时间
     * @return 普通的Future对象，用户需要自行进行类型检查
     */
    public static func askTyped(target: ActorRef, message: Message, timeout: Duration): AskFuture<Message> {
        // 简化实现：返回普通的Ask Future
        // 用户可以在获取响应后自行进行类型检查
        ensureInitialized()
        return ask(target, message, timeout)
    }

    /**
     * 便捷方法：等待Ask响应
     *
     * @param target 目标Actor
     * @param message 请求消息
     * @param timeout 超时时间
     * @return 响应结果
     */
    public static func askAndWait(target: ActorRef, message: Message, timeout: Duration): Result<Message> {
        ensureInitialized()
        let future = ask(target, message, timeout)
        return future.await(timeout)
    }

    /**
     * 便捷方法：异步Ask处理
     *
     * @param target 目标Actor
     * @param message 请求消息
     * @param timeout 超时时间
     * @param onSuccess 成功回调
     * @param onFailure 失败回调
     */
    public static func askAsync(
        target: ActorRef,
        message: Message,
        timeout: Duration,
        onSuccess: (Message) -> Unit,
        onFailure: (Exception) -> Unit
    ): Unit {
        ensureInitialized()
        let future = ask(target, message, timeout)
        future.onComplete { result =>
            match (result) {
                case Result.Success(response) => onSuccess(response)
                case Result.Failure(error) => onFailure(error)
            }
        }
    }

    /**
     * 生成请求ID
     */
    private static func generateRequestId(): String {
        let timestamp = DateTime.now().toUnixTimeStamp().toMilliseconds()
        let random = generateRandomString(8)
        "ask-${timestamp}-${random}"
    }

    /**
     * 生成随机字符串
     */
    private static func generateRandomString(length: Int32): String {
        let chars = "abcdefghijklmnopqrstuvwxyz0123456789"
        let result = StringBuilder()
        let timestamp = DateTime.now().toUnixTimeStamp().toMilliseconds()
        
        for (i in 0..length) {
            let index = Int32((timestamp + Int64(i)) % Int64(chars.size))
            result.append(chars[Int64(index)])
        }
        
        return result.toString()
    }

    /**
     * 关闭Ask管理器
     */
    public static func shutdown(): Unit {
        defaultManager.stop()
    }

    /**
     * 获取统计信息
     */
    public static func getStats(): AskStats {
        ensureInitialized()
        AskStats(0, Duration.second * 5)  // 简化实现
    }
}

/**
 * Ask统计信息
 */
public struct AskStats {
    public let pendingRequests: Int64
    public let defaultTimeout: Duration

    public init(pendingRequests: Int64, defaultTimeout: Duration) {
        this.pendingRequests = pendingRequests
        this.defaultTimeout = defaultTimeout
    }

    public func toString(): String {
        "AskStats(pending=${pendingRequests}, timeout=${defaultTimeout})"
    }
}

/**
 * 类型转换异常
 */
public class TypeCastException <: Exception {
    public init(message: String) {
        super(message)
    }
}
