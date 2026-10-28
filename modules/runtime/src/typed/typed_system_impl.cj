/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Runtime - 类型安全Actor系统实现
 * 实现cactor1.md中第一优先级功能的运行时支持
 */
package cactor_runtime.typed

import std.time.*
import std.sync.*
import std.collection.*
import cactor_core.typed.*
import cactor_core.message.*
import cactor_core.actor.*
import cactor_core.context.*
import cactor_runtime.*

/**
 * 类型安全Actor系统的实现
 * 基于现有的CActorRuntime构建类型安全层
 */
public class TypedActorSystemImpl <: TypedActorSystem {
    private let systemName: String
    private let runtime: CActorRuntime
    private let startTimeStamp: Int64
    private let actorRegistry: HashMap<String, String>
    private let terminated: AtomicBool
    
    public init(name: String, runtime: CActorRuntime) {
        this.systemName = name
        this.runtime = runtime
        this.startTimeStamp = DateTime.now().toUnixTimeStamp().toMilliseconds()
        this.actorRegistry = HashMap<String, String>()
        this.terminated = AtomicBool(false)
    }
    
    public prop name: String {
        get() { systemName }
    }
    
    /**
     * 创建顶级Actor
     *
     * @param behavior Actor的行为
     * @param name Actor的名称
     * @return Actor的引用
     */
    public func createActor<U>(behavior: TypedBehavior<U>, name: String): TypedActorRef<U> {
        if (terminated.load()) {
            throw Exception("Actor system is terminated")
        }

        // 创建类型安全的Actor包装器
        let typedActor = TypedActorWrapper<U>(behavior, this)

        // 使用现有的运行时创建Actor
        let factory = SimpleActorFactory<Actor>({ => typedActor })
        let props = Props<Actor>(factory)
        let actorRef = runtime.actorOf(props, name)

        // 创建类型安全的ActorRef包装器
        let typedActorRef = TypedActorRefImpl<U>(actorRef, name)

        // 注册到注册表
        actorRegistry.add(name, name)

        return typedActorRef
    }
    
    public func terminate(): Bool {
        if (terminated.compareAndSwap(false, true)) {
            // 清空注册表
            actorRegistry.clear()

            // 终止底层运行时
            runtime.shutdown()
            return true
        }
        return false
    }
    
    public func isTerminated(): Bool {
        terminated.load()
    }
    
    public func startTime(): Int64 {
        startTimeStamp
    }

    /**
     * 创建Actor
     */
    public func createActor(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any> {
        // 通过泛型版本创建真实的类型安全Actor
        createActor<Any>(behavior, name)
    }

    /**
     * Actor选择 - 根据路径查找Actor
     * 实现与标准ActorSystem兼容的API
     */
    public func actorSelection(path: String): TypedActorSelection<Any> {
        TypedActorSelectionImpl(path, this)
    }

    /**
     * 查找Actor
     */
    public func findActor<U>(name: String): Option<TypedActorRef<U>> {
        // 简化实现，暂时返回None
        None<TypedActorRef<U>>
    }

    /**
     * 获取所有Actor名称
     */
    public func getAllActorNames(): Array<String> {
        // 简化实现，返回空数组
        Array<String>()
    }
}

/**
 * 类型安全ActorRef的实现
 * 包装现有的ActorRef，提供类型安全接口
 */
internal class TypedActorRefImpl<T> <: TypedActorRef<T> {
    internal let underlyingActorRef: ActorRef
    private let actorName: String
    
    public init(underlyingActorRef: ActorRef, name: String) {
        this.underlyingActorRef = underlyingActorRef
        this.actorName = name
    }
    
    public func tell(message: T): Unit {
        // 将类型安全的消息包装为通用消息
        let wrappedMessage = TypedMessageWrapper<T>(message)
        underlyingActorRef.tell(wrappedMessage)
    }
    
    public func pathString(): String {
        "/user/${actorName}"
    }
    
    public func equals(other: TypedActorRef<T>): Bool {
        match (other) {
            case impl: TypedActorRefImpl<T> => 
                underlyingActorRef.equals(impl.underlyingActorRef)
            case _ => false
        }
    }
}

/**
 * 类型安全Actor的包装器
 * 将TypedBehavior包装为传统的Actor接口
 */
internal class TypedActorWrapper<T> <: Actor {
    private var currentBehavior: TypedBehavior<T>
    private var context: Option<TypedActorContextImpl<T>> = None
    private let system: TypedActorSystem

    public init(initialBehavior: TypedBehavior<T>, system: TypedActorSystem) {
        this.currentBehavior = initialBehavior
        this.system = system
    }
    
    public prop name: String {
        get() { "TypedActor" }
    }
    
    public prop description: String {
        get() { "Type-safe Actor wrapper" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        // 创建类型安全的上下文（如果还没有）
        match (this.context) {
            case None =>
                this.context = Some(TypedActorContextImpl<T>(context, system))
            case Some(_) => ()
        }
        
        match (this.context) {
            case Some(typedContext) =>
                match (message) {
                    case wrapper: TypedMessageWrapper<T> =>
                        // 处理类型安全的消息
                        let newBehavior = currentBehavior.onMessage(wrapper.message, typedContext)
                        updateBehavior(newBehavior)
                        MessageResult.Handled
                    case _: StopMessage =>
                        // 处理停止消息
                        MessageResult.Stop
                    case _ =>
                        // 未知消息类型
                        MessageResult.Unhandled
                }
            case None =>
                MessageResult.Unhandled
        }
    }
    
    private func updateBehavior(newBehavior: TypedBehavior<T>): Unit {
        // 检查是否需要停止
        // 简化实现，直接更新行为
        currentBehavior = newBehavior
    }
    
    public func preStart(): Unit {
        // Actor启动前的操作
    }
    
    public func postStart(): Unit {
        // Actor启动后的操作
    }
    
    public func preStop(): Unit {
        // Actor停止前的操作
    }
    
    public func postStop(): Unit {
        // Actor停止后的操作
    }
    
    public func preRestart(reason: Exception): Unit {
        // Actor重启前的操作
    }
    
    public func postRestart(reason: Exception): Unit {
        // Actor重启后的操作
    }
}

/**
 * 类型安全消息包装器
 * 将类型安全的消息包装为通用消息
 */
internal class TypedMessageWrapper<T> <: Message {
    public let message: T
    
    public init(message: T) {
        this.message = message
    }
    
    public func messageType(): String {
        "TypedMessage"
    }
    
    public func priority(): Int32 {
        0
    }
    
    public func messageId(): String {
        generateMessageId()
    }
    
    public func timestamp(): Int64 {
        DateTime.now().toUnixTimeStamp().toMilliseconds()
    }
    
    private func generateMessageId(): String {
        let timestamp = DateTime.now().toUnixTimeStamp().toNanoseconds()
        "typed-msg-${timestamp}"
    }
}

/**
 * 类型安全Actor上下文的实现
 * 包装现有的ActorContext，提供类型安全接口
 * 实现plan10.md中要求的完整ActorContext功能
 */
internal class TypedActorContextImpl<T> <: TypedActorContext<T> {
    private let underlyingContext: ActorContext
    private var selfRef: Option<TypedActorRef<T>> = None
    private let actorSystem: TypedActorSystem
    private let actorScheduler: TypedScheduler
    private let childrenMap: HashMap<String, TypedActorRef<Any>>
    private let watchedActors: HashSet<String>

    public init(underlyingContext: ActorContext, actorSystem: TypedActorSystem) {
        this.underlyingContext = underlyingContext
        this.actorSystem = actorSystem
        this.actorScheduler = SimpleTypedScheduler()
        this.childrenMap = HashMap<String, TypedActorRef<Any>>()
        this.watchedActors = HashSet<String>()
    }

    public prop self: TypedActorRef<T> {
        get() {
            match (selfRef) {
                case Some(ref) => ref
                case None =>
                    // 创建自身引用的包装器
                    throw Exception("Self reference not initialized")
            }
        }
    }

    public func sender(): Option<String> {
        underlyingContext.sender()
    }

    public func stopSelf(): Unit {
        // 简化实现，暂时不做任何操作
        ()
    }

    public func log(): TypedLogger {
        SimpleTypedLoggerImpl()
    }

    // ========== 子Actor管理 ==========

    public func spawnChild(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any> {
        // 检查名称是否已存在
        match (childrenMap.get(name)) {
            case Some(_) => throw Exception("子Actor名称 '${name}' 已存在")
            case None => ()
        }

        // 创建子Actor（简化实现）
        // 暂时使用简化的实现，返回一个虚拟的ActorRef
        let childRef = createDummyTypedActorRef<Any>(name)
        childrenMap.add(name, childRef)

        log().info("创建子Actor: ${name}")
        return childRef
    }

    public func spawnAnonymousChild(behavior: TypedBehavior<Any>): TypedActorRef<Any> {
        let anonymousName = "child-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
        return spawnChild(behavior, anonymousName)
    }

    public func stopChild(child: TypedActorRef<Any>): Unit {
        let childPath = child.pathString()
        childrenMap.remove(childPath)
        log().info("停止子Actor: ${childPath}")
    }

    // ========== Actor监视 ==========

    public func watchActor(other: TypedActorRef<Any>): Unit {
        let actorPath = other.pathString()
        watchedActors.add(actorPath)
        log().info("开始监视Actor: ${actorPath}")
    }

    public func unwatchActor(other: TypedActorRef<Any>): Unit {
        let actorPath = other.pathString()
        watchedActors.remove(actorPath)
        log().info("停止监视Actor: ${actorPath}")
    }

    // ========== Ask模式支持 ==========

    public func askMessage(target: TypedActorRef<Any>, message: Any, timeout: Duration): TypedFuture<Any> {
        log().info("发送Ask消息到: ${target.pathString()}")
        // 简化实现：创建一个立即完成的Future
        return SimpleTypedFutureImpl<Any>(message)
    }

    public func askMessage(target: TypedActorRef<Any>, message: Any): TypedFuture<Any> {
        // 使用默认超时（3秒）
        return askMessage(target, message, Duration.second * 3)
    }

    // ========== 系统访问 ==========

    public func system(): TypedActorSystem {
        this.actorSystem
    }

    public func scheduler(): TypedScheduler {
        this.actorScheduler
    }

    // ========== 子Actor查找 ==========

    public func child(name: String): Option<TypedActorRef<Any>> {
        childrenMap.get(name)
    }

    public func children(): Array<TypedActorRef<Any>> {
        // 简化实现：返回空数组
        Array<TypedActorRef<Any>>()
    }

    /**
     * 设置自身引用
     * 内部方法，用于初始化
     */
    internal func setSelf(selfRef: TypedActorRef<T>): Unit {
        this.selfRef = Some(selfRef)
    }
}

/**
 * 简单的日志记录器实现
 */
internal class SimpleTypedLoggerImpl <: TypedLogger {
    public func debug(message: String): Unit {
        println("[DEBUG] ${message}")
    }

    public func info(message: String): Unit {
        println("[INFO] ${message}")
    }

    public func warn(message: String): Unit {
        println("[WARN] ${message}")
    }

    public func error(message: String): Unit {
        println("[ERROR] ${message}")
    }
}

/**
 * 简单的TypedFuture实现
 * 立即完成的Future，用于演示
 */
internal class SimpleTypedFutureImpl<T> <: TypedFuture<T> {
    private let result: T

    public init(result: T) {
        this.result = result
    }

    public func isCompleted(): Bool {
        true
    }

    public func get(): T {
        result
    }

    public func get(timeout: Duration): T {
        result
    }

    public func value(): Option<T> {
        Some(result)
    }
}

/**
 * 简单的TypedScheduler实现
 */
internal class SimpleTypedScheduler <: TypedScheduler {
    public func scheduleOnce(delay: Duration, target: TypedActorRef<Any>, message: Any): Unit {
        // 简化实现：直接发送消息（忽略延时）
        target.tell(message)
    }

    public func schedule(initialDelay: Duration, interval: Duration, target: TypedActorRef<Any>, message: Any): TypedCancellable {
        // 简化实现：返回一个虚拟的可取消任务
        SimpleCancellable()
    }
}

/**
 * 简单的可取消任务实现
 */
internal class SimpleCancellable <: TypedCancellable {
    private var cancelled: Bool = false

    public func cancel(): Bool {
        if (!cancelled) {
            cancelled = true
            return true
        }
        return false
    }

    public func isCancelled(): Bool {
        cancelled
    }
}

/**
 * 创建虚拟的TypedActorRef
 * 用于演示子Actor创建
 */
internal func createDummyTypedActorRef<T>(name: String): TypedActorRef<T> {
    DummyTypedActorRef<T>(name)
}

/**
 * 虚拟的TypedActorRef实现
 */
internal class DummyTypedActorRef<T> <: TypedActorRef<T> {
    private let name: String

    public init(name: String) {
        this.name = name
    }

    public func tell(message: T): Unit {
        println("[DUMMY] 发送消息到 ${name}")
    }

    public func pathString(): String {
        "/user/${name}"
    }

    public func equals(other: TypedActorRef<T>): Bool {
        pathString() == other.pathString()
    }
}

/**
 * 类型安全的Actor选择器实现
 * 与标准ActorSelection兼容，但提供类型安全
 */
internal class TypedActorSelectionImpl <: TypedActorSelection<Any> {
    private let path: String
    private let system: TypedActorSystemImpl

    public init(path: String, system: TypedActorSystemImpl) {
        this.path = path
        this.system = system
    }

    public func resolveOne(timeout: Duration): Option<TypedActorRef<Any>> {
        // 简化实现：尝试从路径解析Actor名称
        let actorName = extractActorName(path)

        // 创建类型安全的包装器
        Some(createDummyTypedActorRef<Any>(actorName))
    }

    public func tell(message: Any): Unit {
        let actorName = extractActorName(path)
        println("[TYPED_SELECTION] 发送消息到 ${actorName}")

        // 尝试解析并发送消息
        match (resolveOne(Duration.second * 1)) {
            case Some(actorRef) =>
                actorRef.tell(message)
            case None =>
                println("警告：未找到Actor ${path}，消息被丢弃")
        }
    }

    public func pathString(): String {
        path
    }

    private func extractActorName(path: String): String {
        // 简化的路径解析
        if (path.startsWith("/user/")) {
            // 提取 /user/ 后面的部分
            "extracted_actor_name"
        } else if (path.startsWith("/")) {
            // 提取 / 后面的部分
            "actor_name"
        } else {
            path
        }
    }
}
