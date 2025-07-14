/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 统一错误处理模块
 * Plan 0.20 Phase 2.2: 错误处理标准化 - 统一错误处理实现
 */
package cactor.core.error

import std.time.*
import std.collection.*

/**
 * CActor错误码枚举
 */
public enum CActorErrorCode {
    // 系统级错误 (1000-1999)
    | SystemInitializationError
    | SystemShutdownError
    | SystemOverloadError
    | SystemConfigurationError

    // Actor级错误 (2000-2999)
    | ActorCreationError
    | ActorNotFoundError
    | ActorTerminatedError
    | ActorMailboxFullError
    | ActorSupervisionError

    // 消息级错误 (3000-3999)
    | MessageSerializationError
    | MessageDeserializationError
    | MessageRoutingError
    | MessageTimeoutError
    | MessageFormatError

    // 网络级错误 (4000-4999)
    | NetworkConnectionError
    | NetworkTimeoutError
    | NetworkProtocolError
    | NetworkSecurityError

    // 持久化错误 (5000-5999)
    | PersistenceWriteError
    | PersistenceReadError
    | PersistenceCorruptionError
    | PersistenceStorageFullError

    // 集群错误 (6000-6999)
    | ClusterJoinError
    | ClusterLeaveError
    | ClusterSplitBrainError
    | ClusterNodeUnreachableError

    // 通用错误 (9000-9999)
    | UnknownError
    | ValidationError
    | ConfigurationError
    | ResourceExhaustionError
}

/**
 * 错误严重级别
 */
public enum ErrorSeverity {
    | Critical  // 严重错误，需要立即处理
    | High      // 高级错误，影响功能
    | Medium    // 中级错误，部分影响
    | Low       // 低级错误，轻微影响
    | Info      // 信息级别，仅记录
}

/**
 * CActor统一异常类
 */
public class CActorException <: Exception {
    private let errorCode: CActorErrorCode
    private let severity: ErrorSeverity
    private let timestamp: Int64
    private let context: HashMap<String, String>
    
    public init(code: CActorErrorCode, message: String, severity: ErrorSeverity) {
        super(message)
        this.errorCode = code
        this.severity = severity
        this.timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.context = HashMap<String, String>()
    }
    
    public init(code: CActorErrorCode, message: String) {
        super(message)
        this.errorCode = code
        this.severity = ErrorSeverity.Medium
        this.timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.context = HashMap<String, String>()
    }
    
    public func getErrorCode(): CActorErrorCode {
        errorCode
    }
    
    public func getSeverity(): ErrorSeverity {
        severity
    }
    
    public func getTimestamp(): Int64 {
        timestamp
    }
    
    public func addContext(key: String, value: String): Unit {
        context[key] = value
    }
    
    public func getContext(): HashMap<String, String> {
        context
    }
    
    public func getContextValue(key: String): Option<String> {
        context.get(key)
    }
    
    public func toString(): String {
        let contextStr = if (context.size > 0) {
            ", context=有${context.size}个上下文项"
        } else {
            ""
        }

        "CActorException(message=${message}, timestamp=${timestamp}${contextStr})"
    }
}

/**
 * 错误处理策略接口
 */
public interface ErrorHandlingStrategy {
    func handleError(error: CActorException): ErrorHandlingResult
    func canHandle(errorCode: CActorErrorCode): Bool
    func getName(): String
}

/**
 * 错误处理结果
 */
public enum ErrorHandlingResult {
    | Retry      // 重试操作
    | Escalate   // 升级处理
    | Ignore     // 忽略错误
    | Shutdown   // 关闭组件
    | Restart    // 重启组件
}

/**
 * 默认错误处理策略
 */
public class DefaultErrorHandlingStrategy <: ErrorHandlingStrategy {
    public func handleError(error: CActorException): ErrorHandlingResult {
        match (error.getSeverity()) {
            case ErrorSeverity.Critical => ErrorHandlingResult.Shutdown
            case ErrorSeverity.High => ErrorHandlingResult.Escalate
            case ErrorSeverity.Medium => ErrorHandlingResult.Retry
            case ErrorSeverity.Low => ErrorHandlingResult.Ignore
            case ErrorSeverity.Info => ErrorHandlingResult.Ignore
        }
    }
    
    public func canHandle(errorCode: CActorErrorCode): Bool {
        true  // 默认策略处理所有错误
    }
    
    public func getName(): String {
        "DefaultErrorHandlingStrategy"
    }
}

/**
 * Actor特定错误处理策略
 */
public class ActorErrorHandlingStrategy <: ErrorHandlingStrategy {
    public func handleError(error: CActorException): ErrorHandlingResult {
        match (error.getErrorCode()) {
            case CActorErrorCode.ActorCreationError => ErrorHandlingResult.Retry
            case CActorErrorCode.ActorNotFoundError => ErrorHandlingResult.Ignore
            case CActorErrorCode.ActorTerminatedError => ErrorHandlingResult.Restart
            case CActorErrorCode.ActorMailboxFullError => ErrorHandlingResult.Retry
            case CActorErrorCode.ActorSupervisionError => ErrorHandlingResult.Escalate
            case _ => ErrorHandlingResult.Escalate
        }
    }
    
    public func canHandle(errorCode: CActorErrorCode): Bool {
        match (errorCode) {
            case CActorErrorCode.ActorCreationError => true
            case CActorErrorCode.ActorNotFoundError => true
            case CActorErrorCode.ActorTerminatedError => true
            case CActorErrorCode.ActorMailboxFullError => true
            case CActorErrorCode.ActorSupervisionError => true
            case _ => false
        }
    }
    
    public func getName(): String {
        "ActorErrorHandlingStrategy"
    }
}

/**
 * 错误统计信息
 */
public struct ErrorStatistics {
    public let totalErrors: Int64
    public let criticalErrors: Int64
    public let highErrors: Int64
    public let mediumErrors: Int64
    public let lowErrors: Int64
    public let lastErrorTime: Int64

    public init() {
        this.totalErrors = 0
        this.criticalErrors = 0
        this.highErrors = 0
        this.mediumErrors = 0
        this.lowErrors = 0
        this.lastErrorTime = 0
    }

    public init(total: Int64, critical: Int64, high: Int64, medium: Int64, low: Int64, lastTime: Int64) {
        this.totalErrors = total
        this.criticalErrors = critical
        this.highErrors = high
        this.mediumErrors = medium
        this.lowErrors = low
        this.lastErrorTime = lastTime
    }

    public func toString(): String {
        "ErrorStatistics(total=${totalErrors}, critical=${criticalErrors}, high=${highErrors}, medium=${mediumErrors}, low=${lowErrors})"
    }
}

/**
 * 统一错误处理管理器
 */
public class ErrorHandlingManager {
    private static var instance: Option<ErrorHandlingManager> = None
    private let strategies: ArrayList<ErrorHandlingStrategy> = ArrayList<ErrorHandlingStrategy>()
    private var totalErrors: Int64 = 0
    private var criticalErrors: Int64 = 0
    private var highErrors: Int64 = 0
    private var mediumErrors: Int64 = 0
    private var lowErrors: Int64 = 0
    private var lastErrorTime: Int64 = 0
    
    private init() {
        // 注册默认策略
        strategies.add(ActorErrorHandlingStrategy())
        strategies.add(DefaultErrorHandlingStrategy())
    }
    
    public static func getInstance(): ErrorHandlingManager {
        if (instance.isNone()) {
            instance = Some(ErrorHandlingManager())
        }
        instance.getOrThrow()
    }
    
    public func registerStrategy(strategy: ErrorHandlingStrategy): Unit {
        strategies.add(strategy)
    }
    
    public func handleError(error: CActorException): ErrorHandlingResult {
        // 更新统计
        updateStatistics(error)

        // 查找合适的处理策略
        for (strategy in strategies) {
            if (strategy.canHandle(error.getErrorCode())) {
                let result = strategy.handleError(error)
                println("错误处理: ${strategy.getName()} 处理错误")
                return result
            }
        }

        // 如果没有找到合适的策略，使用默认处理
        println("错误处理: 使用默认策略处理错误")
        ErrorHandlingResult.Escalate
    }

    private func updateStatistics(error: CActorException): Unit {
        totalErrors += 1
        lastErrorTime = error.getTimestamp()

        // 更新按严重级别统计
        match (error.getSeverity()) {
            case ErrorSeverity.Critical => criticalErrors += 1
            case ErrorSeverity.High => highErrors += 1
            case ErrorSeverity.Medium => mediumErrors += 1
            case ErrorSeverity.Low => lowErrors += 1
            case ErrorSeverity.Info => lowErrors += 1
        }
    }

    public func getStatistics(): ErrorStatistics {
        ErrorStatistics(totalErrors, criticalErrors, highErrors, mediumErrors, lowErrors, lastErrorTime)
    }

    public func printStatistics(): Unit {
        println("\n📊 错误处理统计:")
        println("总错误数: ${totalErrors}")
        println("严重错误: ${criticalErrors}")
        println("高级错误: ${highErrors}")
        println("中级错误: ${mediumErrors}")
        println("低级错误: ${lowErrors}")
        println("最后错误时间: ${lastErrorTime}")
    }

    public func reset(): Unit {
        totalErrors = 0
        criticalErrors = 0
        highErrors = 0
        mediumErrors = 0
        lowErrors = 0
        lastErrorTime = 0
    }
}
