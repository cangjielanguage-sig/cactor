/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - Ask模式实现
 * 支持请求-响应模式的异步通信
 */
package cactor_patterns.ask

import std.sync.*
import std.time.*
import std.collection.*
import std.collection.concurrent.*
import cactor_core.message.{Message}
import cactor_core.actor.{ActorRef, ActorPath}

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
    private let startTime: MonoTime
    private let completion: Completion<Result<T>>

    public init(requestId: String, timeout: Duration) {
        this.requestId = requestId
        this.timeout = timeout
        this.startTime = MonoTime.now()
        this.completion = Completion<Result<T>>()
    }

    /**
     * 完成Future并设置成功结果（只有第一次完成生效）
     */
    public func complete(value: T): Unit {
        completion.tryComplete(Result.Success(value))
    }

    /**
     * 完成Future并设置错误结果
     */
    public func completeWithError(error: Exception): Unit {
        completion.tryComplete(Result.Failure(error))
    }

    /**
     * 阻塞等待结果，最长等待创建时指定的超时时间
     */
    public func get(): Result<T> {
        let remaining = timeout - (MonoTime.now() - startTime)
        return await(if (remaining > Duration.Zero) { remaining } else { Duration.Zero })
    }

    /**
     * 带超时的阻塞等待（Condition 通知，不轮询）
     */
    public func await(waitTimeout: Duration): Result<T> {
        match (completion.await(waitTimeout)) {
            case Some(res) => res
            case None => Result.Failure(AskTimeoutException(requestId, waitTimeout))
        }
    }

    /**
     * 注册完成回调；已完成时立即执行
     */
    public func onComplete(callback: (Result<T>) -> Unit): Unit {
        completion.onComplete(callback)
    }

    public func isCompleted(): Bool {
        completion.isDone()
    }

    public func isTimedOut(): Bool {
        MonoTime.now() - startTime >= timeout
    }

    public func getRequestId(): String {
        requestId
    }

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
    private let stopLock: Mutex
    private let stopCond: Condition

    public init(defaultTimeout: Duration) {
        this.stopLock = Mutex()
        this.stopCond = synchronized(stopLock) { stopLock.condition() }
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
        synchronized(stopLock) {
            stopCond.notifyAll()
        }
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
        "ask-${counter}"
    }

    private func createReplyActor(requestId: String): ActorRef {
        // 创建一个真实的临时Actor来处理响应
        // 这个Actor会在收到响应后自动销毁
        AskReplyActor(requestId, this)
    }

    private func cleanupLoop(): Unit {
        while (isRunning.load()) {
            cleanupTimedOutRequests()
            // 可被 stop() 立即唤醒的定时等待
            synchronized(stopLock) {
                if (isRunning.load()) {
                    stopCond.wait(timeout: cleanupInterval)
                }
            }
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
                    future.completeWithError(AskTimeoutException(requestId, future.getTimeout()))
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
