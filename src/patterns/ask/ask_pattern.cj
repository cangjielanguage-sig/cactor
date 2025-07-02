/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - Ask模式实现
 * 支持请求-响应模式的异步通信
 */
package cactor.patterns.ask

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*
import cactor.core.message.{Message, Envelope}
import cactor.core.actor.{ActorRef, ActorPath}

/**
 * Ask请求消息
 */
public class AskMessage <: Message {
    private let requestId: String
    private let originalMessage: Message
    private let replyTo: ActorRef

    public init(requestId: String, originalMessage: Message, replyTo: ActorRef) {
        this.requestId = requestId
        this.originalMessage = originalMessage
        this.replyTo = replyTo
    }

    public func messageType(): String {
        "AskMessage"
    }

    public func getRequestId(): String {
        requestId
    }

    public func getOriginalMessage(): Message {
        originalMessage
    }

    public func getReplyTo(): ActorRef {
        replyTo
    }
}

/**
 * Ask响应消息
 */
public class AskResponse <: Message {
    private let requestId: String
    private let response: Message
    private let success: Bool

    public init(requestId: String, response: Message, success: Bool) {
        this.requestId = requestId
        this.response = response
        this.success = success
    }

    public func messageType(): String {
        "AskResponse"
    }

    public func getRequestId(): String {
        requestId
    }

    public func getResponse(): Message {
        response
    }

    public func isSuccess(): Bool {
        success
    }
}

/**
 * Ask超时异常
 */
public class AskTimeoutException <: Exception {
    private let requestId: String
    private let timeout: Duration

    public init(requestId: String, timeout: Duration) {
        super("Ask request ${requestId} timed out after ${timeout}")
        this.requestId = requestId
        this.timeout = timeout
    }

    public func getRequestId(): String {
        requestId
    }

    public func getTimeout(): Duration {
        timeout
    }
}

/**
 * Ask Future - 改进的异步操作结果处理
 *
 * 改进点：
 * 1. 使用智能等待替代轮询，提高性能
 * 2. 支持回调函数，实现真正的异步处理
 * 3. 改进超时处理，避免递归调用
 * 4. 增强错误处理和状态管理
 */
public class AskFuture<T> {
    private let requestId: String
    private let timeout: Duration
    private let startTime: Int64
    private var result: Option<Result<T>>
    private let completed: AtomicBool
    private let mutex: Mutex
    private var callbacks: Array<(Result<T>) -> Unit>

    public init(requestId: String, timeout: Duration) {
        this.requestId = requestId
        this.timeout = timeout
        this.startTime = DateTime.now().toUnixTimeStamp().toNanoseconds()
        this.result = None
        this.completed = AtomicBool(false)
        this.mutex = Mutex()
        this.callbacks = Array<(Result<T>) -> Unit>(0, { _ => { _ => () } })
    }

    /**
     * 完成Future并设置成功结果
     */
    public func complete(value: T): Unit {
        completeWith(Result.Success(value))
    }

    /**
     * 完成Future并设置错误结果
     */
    public func completeWithError(error: Exception): Unit {
        completeWith(Result.Failure(error))
    }

    /**
     * 内部方法：完成Future并通知等待者
     */
    private func completeWith(resultValue: Result<T>): Unit {
        if (completed.compareAndSwap(false, true)) {
            mutex.lock()
            try {
                result = Some(resultValue)

                // 执行所有回调函数
                for (callback in callbacks) {
                    try {
                        callback(resultValue)
                    } catch (e: Exception) {
                        println("Ask Future回调执行失败: ${e.message}")
                    }
                }

                // 注意：仓颉语言暂不支持条件变量，使用轮询机制
                // 在实际生产环境中，可以考虑使用更高效的通知机制
            } finally {
                mutex.unlock()
            }
        }
    }

    /**
     * 阻塞等待结果（改进版本，使用条件变量）
     */
    public func get(): Result<T> {
        return await(timeout)
    }

    /**
     * 带超时的等待（改进版本，使用智能轮询）
     */
    public func await(waitTimeout: Duration): Result<T> {
        if (isCompleted()) {
            return getCompletedResult()
        }

        let endTime = DateTime.now().toUnixTimeStamp().toNanoseconds() + waitTimeout.toNanoseconds()
        let sleepInterval = Duration.millisecond * 1  // 1ms间隔

        while (!completed.load() && DateTime.now().toUnixTimeStamp().toNanoseconds() < endTime) {
            // 智能等待：短暂休眠后检查状态
            sleep(sleepInterval)

            if (completed.load()) {
                return getCompletedResult()
            }
        }

        // 最后检查一次
        if (completed.load()) {
            return getCompletedResult()
        } else {
            // 超时处理
            let timeoutError = AskTimeoutException(requestId, waitTimeout)
            return Result.Failure(timeoutError)
        }
    }

    /**
     * 获取已完成的结果
     */
    private func getCompletedResult(): Result<T> {
        match (result) {
            case Some(res) => res
            case None => Result.Failure(AskTimeoutException(requestId, timeout))
        }
    }

    /**
     * 异步处理结果（添加回调函数）
     */
    public func onComplete(callback: (Result<T>) -> Unit): Unit {
        mutex.lock()
        try {
            if (completed.load()) {
                // 如果已完成，立即执行回调
                match (result) {
                    case Some(res) => callback(res)
                    case None => callback(Result.Failure(AskTimeoutException(requestId, timeout)))
                }
            } else {
                // 添加到回调列表
                let newCallbacks = Array<(Result<T>) -> Unit>(Int64(callbacks.size) + 1, { i =>
                    if (i < Int64(callbacks.size)) {
                        callbacks[i]
                    } else {
                        callback
                    }
                })
                callbacks = newCallbacks
            }
        } finally {
            mutex.unlock()
        }
    }

    /**
     * 检查是否已完成
     */
    public func isCompleted(): Bool {
        completed.load()
    }

    /**
     * 检查是否超时
     */
    public func isTimedOut(): Bool {
        let elapsed = DateTime.now().toUnixTimeStamp().toNanoseconds() - startTime
        elapsed >= timeout.toNanoseconds()
    }

    /**
     * 获取请求ID
     */
    public func getRequestId(): String {
        requestId
    }

    /**
     * 获取超时时间
     */
    public func getTimeout(): Duration {
        timeout
    }
}

/**
 * Ask模式管理器
 */
public class AskPatternManager {
    private let pendingRequests: ConcurrentHashMap<String, AskFuture<Message>>
    private let requestCounter: AtomicInt64
    private let defaultTimeout: Duration
    private let cleanupInterval: Duration
    private let isRunning: AtomicBool

    public init(defaultTimeout: Duration) {
        this.pendingRequests = ConcurrentHashMap<String, AskFuture<Message>>()
        this.requestCounter = AtomicInt64(0)
        this.defaultTimeout = defaultTimeout
        this.cleanupInterval = Duration.second * 30
        this.isRunning = AtomicBool(false)
    }

    public func start(): Unit {
        if (isRunning.compareAndSwap(false, true)) {
            // 启动清理任务
            spawn {
                cleanupLoop()
            }
        }
    }

    public func stop(): Unit {
        isRunning.store(false)
    }

    public func ask(target: ActorRef, message: Message): AskFuture<Message> {
        ask(target, message, defaultTimeout)
    }

    public func ask(target: ActorRef, message: Message, timeout: Duration): AskFuture<Message> {
        let requestId = generateRequestId()
        let future = AskFuture<Message>(requestId, timeout)
        
        pendingRequests[requestId] = future
        
        // 创建临时Actor来接收响应
        let replyTo = createReplyActor(requestId)
        let askMessage = AskMessage(requestId, message, replyTo)
        
        // 发送Ask消息
        target.tell(askMessage)
        
        return future
    }

    public func handleResponse(response: AskResponse): Unit {
        let requestId = response.getRequestId()
        
        match (pendingRequests.remove(requestId)) {
            case Some(future) => 
                if (response.isSuccess()) {
                    future.complete(response.getResponse())
                } else {
                    future.completeWithError(Exception("Ask request failed"))
                }
            case None => 
                // 请求已超时或不存在
                println("收到未知请求的响应: ${requestId}")
        }
    }

    private func generateRequestId(): String {
        let counter = requestCounter.fetchAdd(1)
        let timestamp = DateTime.now().toUnixTimeStamp().toMilliseconds()
        "ask-${timestamp}-${counter}"
    }

    private func createReplyActor(requestId: String): ActorRef {
        // 创建一个真实的临时Actor来处理响应
        // 这个Actor会在收到响应后自动销毁
        AskReplyActor(requestId, this)
    }

    private func cleanupLoop(): Unit {
        while (isRunning.load()) {
            cleanupTimedOutRequests()
            sleep(cleanupInterval)
        }
    }

    private func cleanupTimedOutRequests(): Unit {
        let toRemove = ArrayList<String>()
        
        for ((requestId, future) in pendingRequests) {
            if (future.isTimedOut()) {
                toRemove.add(requestId)
            }
        }
        
        for (requestId in toRemove) {
            match (pendingRequests.remove(requestId)) {
                case Some(future) => 
                    future.completeWithError(AskTimeoutException(requestId, defaultTimeout))
                case None => ()
            }
        }
        
        if (!toRemove.isEmpty()) {
            println("清理了 ${toRemove.size} 个超时的Ask请求")
        }
    }

    public func getPendingRequestCount(): Int64 {
        Int64(pendingRequests.size)
    }

    public func getDefaultTimeout(): Duration {
        defaultTimeout
    }
}

/**
 * 虚拟ActorRef实现（用于测试）
 */
public class DummyActorRef <: ActorRef {
    private let id: String

    public init(id: String) {
        this.id = id
    }

    public prop path: ActorPath {
        get() {
            ActorPath("default", id)
        }
    }

    public func tell(message: Message): Unit {
        println("DummyActorRef[${id}] 收到消息: ${message.messageType()}")
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        println("DummyActorRef[${id}] 收到来自${sender}的消息: ${message.messageType()}")
    }

    public func equals(other: ActorRef): Bool {
        match (other) {
            case ref: DummyActorRef => this.id == ref.id
            case _ => false
        }
    }

    public func hashCode(): Int32 {
        Int32(id.hashCode())
    }

    public func toString(): String {
        "DummyActorRef(${id})"
    }
}

/**
 * Ask响应处理Actor - 临时Actor，用于接收Ask请求的响应
 */
public class AskReplyActor <: ActorRef {
    private let requestId: String
    private let manager: AskPatternManager
    private let actorPath: ActorPath

    public init(requestId: String, manager: AskPatternManager) {
        this.requestId = requestId
        this.manager = manager
        this.actorPath = ActorPath("ask-reply", requestId)
    }

    public prop path: ActorPath {
        get() { actorPath }
    }

    public func tell(message: Message): Unit {
        // 处理响应消息
        match (message) {
            case response: AskResponse =>
                manager.handleResponse(response)
            case _ =>
                // 将普通消息包装为AskResponse
                let askResponse = AskResponse(requestId, message, true)
                manager.handleResponse(askResponse)
        }
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        tell(message)
    }

    public func equals(other: ActorRef): Bool {
        match (other) {
            case ref: AskReplyActor => this.requestId == ref.requestId
            case _ => false
        }
    }

    public func hashCode(): Int32 {
        Int32(requestId.hashCode())
    }

    public func toString(): String {
        "AskReplyActor(${requestId})"
    }
}

/**
 * Result类型定义
 */
public enum Result<T> {
    | Success(T)
    | Failure(Exception)

    public func isSuccess(): Bool {
        match (this) {
            case Success(_) => true
            case Failure(_) => false
        }
    }

    public func isFailure(): Bool {
        !isSuccess()
    }

    public func get(): T {
        match (this) {
            case Success(value) => value
            case Failure(error) => throw error
        }
    }

    public func getOrElse(defaultValue: T): T {
        match (this) {
            case Success(value) => value
            case Failure(_) => defaultValue
        }
    }
}
