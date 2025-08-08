/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 类型安全Actor系统
 * 实现cactor1.md中第一优先级功能：类型安全Actor系统
 * 基于Cangjie语言特性，参考Akka Typed设计
 */
package cactor.core.typed

import std.time.*
import cactor.core.message.*
import cactor.core.actor.*

/**
 * 类型安全的Actor行为接口
 * 这是CActor类型安全系统的核心接口，参考Akka Typed设计
 *
 * @param T 此Actor可以处理的消息类型
 */
public interface TypedBehavior<T> {
    /**
     * 处理接收到的消息
     *
     * @param message 接收到的消息，类型安全
     * @param context 类型安全的Actor上下文
     * @return 新的行为，可以是相同的行为或不同的行为
     */
    func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T>

    /**
     * 获取行为的描述信息
     * 用于调试和监控
     */
    func getDescription(): String {
        "TypedBehavior"
    }
}

/**
 * 类型安全的Actor引用接口
 * 提供类型安全的消息发送功能
 *
 * @param T 此ActorRef可以接收的消息类型
 */
public interface TypedActorRef<T> {
    /**
     * 发送消息给Actor (tell模式)
     * 这是异步的，不等待响应
     *
     * @param message 要发送的消息，类型安全
     */
    func tell(message: T): Unit

    /**
     * 获取Actor路径的字符串表示
     *
     * @return 路径字符串
     */
    func pathString(): String

    /**
     * 比较两个TypedActorRef是否相等
     *
     * @param other 另一个TypedActorRef
     * @return 是否相等
     */
    func equals(other: TypedActorRef<T>): Bool
}

/**
 * 类型安全的Actor上下文接口
 * 提供Actor运行时的环境和服务，所有操作都是类型安全的
 * 参考Akka Typed的ActorContext设计
 *
 * @param T 当前Actor可以处理的消息类型
 */
public interface TypedActorContext<T> {
    /**
     * 获取当前Actor的自身引用
     */
    prop self: TypedActorRef<T>

    /**
     * 获取当前消息的发送者路径
     *
     * @return 发送者的路径，如果没有则为None
     */
    func sender(): Option<String>

    /**
     * 停止自己
     */
    func stopSelf(): Unit

    /**
     * 获取日志记录器
     *
     * @return 日志记录器
     */
    func log(): TypedLogger

    // ========== 子Actor管理 ==========

    /**
     * 创建子Actor
     * 参考Akka的spawn方法
     * 注意：由于Cangjie接口限制，使用Any类型，实际使用时需要类型转换
     *
     * @param behavior 子Actor的行为
     * @param name 子Actor的名称
     * @return 子Actor的引用
     */
    func spawnChild(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any>

    /**
     * 创建匿名子Actor
     *
     * @param behavior 子Actor的行为
     * @return 子Actor的引用
     */
    func spawnAnonymousChild(behavior: TypedBehavior<Any>): TypedActorRef<Any>

    /**
     * 停止子Actor
     *
     * @param child 要停止的子Actor引用
     */
    func stopChild(child: TypedActorRef<Any>): Unit

    // ========== Actor监视 ==========

    /**
     * 监视另一个Actor
     * 当被监视的Actor终止时，会收到Terminated消息
     *
     * @param other 要监视的Actor引用
     */
    func watchActor(other: TypedActorRef<Any>): Unit

    /**
     * 取消监视另一个Actor
     *
     * @param other 要取消监视的Actor引用
     */
    func unwatchActor(other: TypedActorRef<Any>): Unit

    // ========== Ask模式支持 ==========

    /**
     * 发送消息并等待响应 (Ask模式)
     * 类型安全的请求-响应模式
     * 注意：由于Cangjie接口限制，使用Any类型
     *
     * @param target 目标Actor
     * @param message 要发送的消息
     * @param timeout 超时时间
     * @return Future包装的响应
     */
    func askMessage(target: TypedActorRef<Any>, message: Any, timeout: Duration): TypedFuture<Any>

    /**
     * Ask模式的简化版本，使用默认超时
     *
     * @param target 目标Actor
     * @param message 要发送的消息
     * @return Future包装的响应
     */
    func askMessage(target: TypedActorRef<Any>, message: Any): TypedFuture<Any>

    // ========== 系统访问 ==========

    /**
     * 获取Actor系统引用
     *
     * @return Actor系统
     */
    func system(): TypedActorSystem

    /**
     * 获取调度器
     * 用于延时消息和定时任务
     *
     * @return 调度器
     */
    func scheduler(): TypedScheduler

    // ========== 子Actor查找 ==========

    /**
     * 根据名称查找子Actor
     *
     * @param name 子Actor名称
     * @return 可能的子Actor引用
     */
    func child(name: String): Option<TypedActorRef<Any>>

    /**
     * 获取所有子Actor
     *
     * @return 子Actor引用列表
     */
    func children(): Array<TypedActorRef<Any>>
}

/**
 * 类型安全的Actor系统接口
 * 管理整个Actor系统的生命周期
 */
public interface TypedActorSystem {
    /**
     * 系统名称
     */
    prop name: String

    /**
     * 创建Actor
     * 注意：由于Cangjie接口限制，使用Any类型
     *
     * @param behavior Actor的行为
     * @param name Actor的名称
     * @return Actor的引用
     */
    func createActor(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any>

    /**
     * Actor选择 - 根据路径查找Actor
     * 实现与标准ActorSystem兼容的API
     *
     * @param path Actor路径
     * @return 类型安全的Actor选择器
     */
    func actorSelection(path: String): TypedActorSelection<Any>

    /**
     * 终止Actor系统
     *
     * @return 是否成功终止
     */
    func terminate(): Bool

    /**
     * 检查系统是否已终止
     *
     * @return 是否已终止
     */
    func isTerminated(): Bool

    /**
     * 获取系统启动时间
     *
     * @return 启动时间戳
     */
    func startTime(): Int64
}

/**
 * 类型安全的Actor选择器接口
 * 用于查找和选择Actor，与标准ActorSelection兼容
 *
 * @param T 此选择器可以处理的消息类型
 */
public interface TypedActorSelection<T> {
    /**
     * 解析选择器，返回单个TypedActorRef
     *
     * @param timeout 超时时间
     * @return 可能的TypedActorRef
     */
    func resolveOne(timeout: Duration): Option<TypedActorRef<T>>

    /**
     * 向选择的所有Actor发送消息
     *
     * @param message 要发送的消息
     */
    func tell(message: T): Unit

    /**
     * 获取选择器路径
     *
     * @return 路径字符串
     */
    func pathString(): String
}

/**
 * 简单的日志记录器接口
 * 用于Actor内部日志记录
 */
public interface TypedLogger {
    func debug(message: String): Unit
    func info(message: String): Unit
    func warn(message: String): Unit
    func error(message: String): Unit
}

/**
 * 类型安全的Future接口
 * 表示异步计算的结果
 */
public interface TypedFuture<T> {
    /**
     * 检查是否已完成
     * @return 是否已完成
     */
    func isCompleted(): Bool

    /**
     * 获取结果（阻塞等待）
     * @return 计算结果
     */
    func get(): T

    /**
     * 获取结果（带超时）
     * @param timeout 超时时间
     * @return 计算结果
     */
    func get(timeout: Duration): T

    /**
     * 获取当前值（如果已完成）
     * @return 可能的结果
     */
    func value(): Option<T>
}

/**
 * 类型安全的调度器接口
 * 用于延时消息和定时任务
 */
public interface TypedScheduler {
    /**
     * 延时发送消息
     *
     * @param delay 延时时间
     * @param target 目标Actor
     * @param message 要发送的消息
     */
    func scheduleOnce(delay: Duration, target: TypedActorRef<Any>, message: Any): Unit

    /**
     * 定期发送消息
     *
     * @param initialDelay 初始延时
     * @param interval 间隔时间
     * @param target 目标Actor
     * @param message 要发送的消息
     * @return 可取消的任务
     */
    func schedule(initialDelay: Duration, interval: Duration, target: TypedActorRef<Any>, message: Any): TypedCancellable
}

/**
 * 可取消的任务接口
 */
public interface TypedCancellable {
    /**
     * 取消任务
     * @return 是否成功取消
     */
    func cancel(): Bool

    /**
     * 检查是否已取消
     * @return 是否已取消
     */
    func isCancelled(): Bool
}

/**
 * 行为工厂类
 * 提供创建常用行为的静态方法，参考Akka的Behaviors
 */
public struct TypedBehaviors {

    /**
     * 创建一个接收消息的行为
     *
     * @param handler 消息处理函数
     * @return 新的行为
     */
    public static func receive<T>(handler: (TypedActorContext<T>, T) -> TypedBehavior<T>): TypedBehavior<T> {
        ReceiveBehavior<T>(handler)
    }

    /**
     * 创建一个相同的行为
     * 表示保持当前行为不变
     *
     * @return 相同的行为标记
     */
    public static func same<T>(): TypedBehavior<T> {
        SameBehavior<T>()
    }

    /**
     * 创建一个停止的行为
     * 表示Actor应该停止
     *
     * @return 停止行为
     */
    public static func stopped<T>(): TypedBehavior<T> {
        StoppedBehavior<T>()
    }

    /**
     * 创建一个忽略消息的行为
     * 接收但不处理任何消息
     *
     * @return 忽略行为
     */
    public static func ignore<T>(): TypedBehavior<T> {
        IgnoreBehavior<T>()
    }

    /**
     * 将外部消息类型 In 转换为内部类型 T 的行为
     * 等价于 Akka Typed 的 transformMessages 的简化版
     */
    public static func transformMessages<In, T>(inner: TypedBehavior<T>, f: (In) -> T): TypedBehavior<In> {
        TransformBehavior<In, T>(inner, f)
    }

    /**
     * 过滤消息，不满足条件的消息被忽略（Same 行为）
     */
    public static func filter<In>(inner: TypedBehavior<In>, p: (In) -> Bool): TypedBehavior<In> {
        FilterBehavior<In>(inner, p)
    }

    /**
     * 部分接收：仅当谓词为真时，调用处理器；否则保持行为不变
     */
    public static func receivePartial<T>(p: (T) -> Bool, handler: (TypedActorContext<T>, T) -> TypedBehavior<T>): TypedBehavior<T> {
        PartialBehavior<T>(p, handler)
    }

    /**
     * 命名别名：mapMessages/contramap 等价于 transformMessages
     */
    public static func mapMessages<In, T>(inner: TypedBehavior<T>, f: (In) -> T): TypedBehavior<In> { transformMessages(inner, f) }
    public static func contramap<In, T>(inner: TypedBehavior<T>, f: (In) -> T): TypedBehavior<In> { transformMessages(inner, f) }
}

/**
 * 接收行为实现
 * 处理接收到的消息
 */
internal class ReceiveBehavior<T> <: TypedBehavior<T> {
    private let handler: (TypedActorContext<T>, T) -> TypedBehavior<T>

    public init(handler: (TypedActorContext<T>, T) -> TypedBehavior<T>) {
        this.handler = handler
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        handler(context, message)
    }

    public func getDescription(): String {
        "ReceiveBehavior"
    }
}

/**
 * 相同行为标记
 * 表示保持当前行为不变
 */
internal class SameBehavior<T> <: TypedBehavior<T> {
    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        this
    }

    public func getDescription(): String {
        "SameBehavior"
    }
}

/**
 * 停止行为
 * 表示Actor应该停止
 */
internal class StoppedBehavior<T> <: TypedBehavior<T> {
    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        this
    }

    public func getDescription(): String {
        "StoppedBehavior"
    }
}

/**
 * Transform 行为实现
 */
internal class TransformBehavior<In, T> <: TypedBehavior<In> {
    private let inner: TypedBehavior<T>
    private let f: (In) -> T
    public init(inner: TypedBehavior<T>, f: (In) -> T) { this.inner = inner; this.f = f }
    public func onMessage(message: In, context: TypedActorContext<In>): TypedBehavior<In> {
        let next = inner.onMessage(f(message), AnyTransformContext<In, T>(context))
        TransformBehavior<In, T>(next, f)
    }
    public func getDescription(): String { "TransformBehavior" }
}

/**
 * Filter 行为实现
 */
internal class FilterBehavior<In> <: TypedBehavior<In> {
    private let inner: TypedBehavior<In>
    private let p: (In) -> Bool
    public init(inner: TypedBehavior<In>, p: (In) -> Bool) { this.inner = inner; this.p = p }
    public func onMessage(message: In, context: TypedActorContext<In>): TypedBehavior<In> {
        if (p(message)) { inner.onMessage(message, context) } else { inner }
    }
}

/**
 * 将 In 上下文适配到 T 的占位实现（最小化）
 */
internal class AnyTransformContext<In, T> <: TypedActorContext<T> {
    private let underlying: TypedActorContext<In>
    public init(underlying: TypedActorContext<In>) { this.underlying = underlying }
    public prop self: TypedActorRef<T> { get() { throw Exception("Not supported in minimal transform context") } }
    public func sender(): Option<String> { underlying.sender() }
    public func stopSelf(): Unit { underlying.stopSelf() }
    public func log(): TypedLogger { underlying.log() }
    public func spawnChild(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any> { underlying.spawnChild(behavior, name) }
    public func spawnAnonymousChild(behavior: TypedBehavior<Any>): TypedActorRef<Any> { underlying.spawnAnonymousChild(behavior) }
    public func stopChild(child: TypedActorRef<Any>): Unit { underlying.stopChild(child) }
    public func watchActor(other: TypedActorRef<Any>): Unit { underlying.watchActor(other) }
    public func unwatchActor(other: TypedActorRef<Any>): Unit { underlying.unwatchActor(other) }
    public func askMessage(target: TypedActorRef<Any>, message: Any, timeout: Duration): TypedFuture<Any> { underlying.askMessage(target, message, timeout) }
    public func askMessage(target: TypedActorRef<Any>, message: Any): TypedFuture<Any> { underlying.askMessage(target, message) }
    public func system(): TypedActorSystem { underlying.system() }
    public func scheduler(): TypedScheduler { underlying.scheduler() }
    public func child(name: String): Option<TypedActorRef<Any>> { underlying.child(name) }
    public func children(): Array<TypedActorRef<Any>> { underlying.children() }
}

/**
 * 部分接收行为：仅当谓词为真时调用处理器
 */
internal class PartialBehavior<T> <: TypedBehavior<T> {
    private let p: (T) -> Bool
    private let handler: (TypedActorContext<T>, T) -> TypedBehavior<T>
    public init(p: (T) -> Bool, handler: (TypedActorContext<T>, T) -> TypedBehavior<T>) { this.p = p; this.handler = handler }
    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        if (p(message)) { handler(context, message) } else { this }
    }
    public func getDescription(): String { "PartialBehavior" }
}

/**
 * 忽略行为
 * 接收但不处理任何消息
 */
internal class IgnoreBehavior<T> <: TypedBehavior<T> {
    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        this
    }

    public func getDescription(): String {
        "IgnoreBehavior"
    }
}

/**
 * 简单的日志记录器实现
 */
internal class SimpleTypedLogger <: TypedLogger {
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
