/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 类型安全 Actor（对标 Akka Typed）
 *
 * 行为（TypedBehavior<T>）是一个函数：收到消息后返回下一个行为。几个特殊的行为由运行时解释：
 * - same：保持当前行为
 * - unhandled：保持当前行为，并把消息计为未处理
 * - stopped：停止 Actor
 * - setup：延迟到 Actor 启动时（在 Actor 线程中，拿到上下文后）再创建真正的行为
 *
 * 运行时实现基于经典 Actor Cell（见 typed_adapter.cj），因此类型化 Actor 与经典 Actor 共享
 * 同一套监督树、死亡监视、定时器、邮箱与调度器。
 */
package cactor_core.typed

import std.time.*
import std.sync.*
import cactor_core.actor.{ActorRef, ActorContext, UnsupportedContextException}

/**
 * 类型安全的 Actor 行为
 *
 * @param T 此行为可以处理的消息类型
 */
public interface TypedBehavior<T> {
    /**
     * 处理一条消息，返回下一个行为（可以是 TypedBehaviors.same() 等特殊行为）
     */
    func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T>

    /**
     * 处理生命周期信号（PostStop / PreRestart / TypedTerminated）。默认不处理。
     * 未处理的 TypedTerminated 会让 Actor 以 DeathPactException 失败（与 Akka 一致）。
     */
    func onSignal(signal: TypedSignal, context: TypedActorContext<T>): TypedBehavior<T> {
        UnhandledBehavior<T>()
    }

    /**
     * 为当前行为附加信号处理函数
     */
    func receiveSignal(handler: (TypedActorContext<T>, TypedSignal) -> TypedBehavior<T>): TypedBehavior<T> {
        SignalBehavior<T>(this, handler)
    }

    /**
     * 行为的描述信息，用于调试和监控
     */
    func getDescription(): String {
        "TypedBehavior"
    }
}

/**
 * 类型安全的 Actor 引用
 *
 * @param T 此引用可以接收的消息类型
 */
public interface TypedActorRef<T> {
    /** 异步发送消息（tell） */
    func tell(message: T): Unit

    /** Actor 路径 */
    func pathString(): String

    func equals(other: TypedActorRef<T>): Bool

    /** 底层的经典引用（用于监视、与经典 Actor 互操作）；不是由 Actor 支撑的引用返回 None */
    func toClassic(): Option<ActorRef> {
        None<ActorRef>
    }

    /** ref << msg 等价于 ref.tell(msg)，可以连写 */
    operator func <<(message: T): TypedActorRef<T> {
        tell(message)
        this
    }
}

/**
 * 类型安全的 Actor 上下文（对标 Akka Typed ActorContext）
 *
 * 只有 self 必须实现；其余方法都有默认实现（抛出 UnsupportedContextException 或返回空值），
 * 运行时的实现提供全部能力。
 *
 * @param T 当前 Actor 可以处理的消息类型
 */
public interface TypedActorContext<T> {
    /** 自身引用 */
    prop self: TypedActorRef<T>

    /** 当前消息发送方的路径 */
    func sender(): Option<String> {
        None<String>
    }

    /** 停止自身（会先停止全部子 Actor） */
    func stopSelf(): Unit {
        throw UnsupportedContextException("stopSelf")
    }

    /** 日志记录器（来源为 Actor 路径） */
    func log(): TypedLogger {
        throw UnsupportedContextException("log")
    }

    // ========== 子 Actor ==========

    /**
     * 创建类型化子 Actor（spawn 是仓颉关键字，沿用 actorOf）
     * 名称在兄弟之间唯一，否则抛出 InvalidActorNameException
     */
    func actorOf<U>(behavior: TypedBehavior<U>, name: String): TypedActorRef<U> {
        throw UnsupportedContextException("actorOf")
    }

    /** 指定调度器 / 邮箱创建子 Actor */
    func actorOf<U>(behavior: TypedBehavior<U>, name: String, props: TypedProps): TypedActorRef<U> {
        throw UnsupportedContextException("actorOf")
    }

    /** 创建自动命名的子 Actor */
    func actorOf<U>(behavior: TypedBehavior<U>): TypedActorRef<U> {
        throw UnsupportedContextException("actorOf")
    }

    /** 停止子 Actor（只能停止自己的子 Actor 或自身） */
    func stop<U>(child: TypedActorRef<U>): Unit {
        throw UnsupportedContextException("stop")
    }

    /** 按名称查找子 Actor，并按调用方声明的类型 U 返回引用（类型由调用方保证，等价 Akka 的 unsafeUpcast） */
    func childAs<U>(name: String): Option<TypedActorRef<U>> {
        None<TypedActorRef<U>>
    }

    /** 根据名称查找子 Actor */
    func child(name: String): Option<TypedActorRef<Any>> {
        None<TypedActorRef<Any>>
    }

    /** 所有存活的子 Actor */
    func children(): Array<TypedActorRef<Any>> {
        Array<TypedActorRef<Any>>()
    }

    // ========== 死亡监视 ==========

    /** 监视目标；目标停止后本 Actor 收到 TypedTerminated 信号 */
    func watch<U>(other: TypedActorRef<U>): Unit {
        throw UnsupportedContextException("watch")
    }

    /** 监视目标；目标停止后本 Actor 收到 message（而不是 TypedTerminated 信号） */
    func watchWith<U>(other: TypedActorRef<U>, message: T): Unit {
        throw UnsupportedContextException("watchWith")
    }

    func unwatch<U>(other: TypedActorRef<U>): Unit {
        throw UnsupportedContextException("unwatch")
    }

    // ========== 请求-响应 ==========

    /**
     * 向 target 发起请求，响应（或超时失败）经 mapResponse 转换为本 Actor 的消息后投递给自己。
     * mapResponse 在本 Actor 线程中执行，可以安全访问 Actor 状态。
     */
    func ask<Req, Res>(target: TypedActorRef<Req>, createRequest: (TypedActorRef<Res>) -> Req, timeout: Duration,
        mapResponse: (AskOutcome<Res>) -> T): Unit {
        throw UnsupportedContextException("ask")
    }

    /** 异步结果完成后，经 map 转换为本 Actor 的消息投递给自己 */
    func pipeToSelf<V>(future: TypedFuture<V>, map: (AskOutcome<V>) -> T): Unit {
        throw UnsupportedContextException("pipeToSelf")
    }

    /**
     * 消息适配器：返回一个接收 U 的引用，发给它的消息经 f 转换后投递给本 Actor。
     * f 在本 Actor 线程中执行。常用于把其他 Actor 的响应协议转换为本 Actor 的协议。
     */
    func messageAdapter<U>(f: (U) -> T): TypedActorRef<U> {
        throw UnsupportedContextException("messageAdapter")
    }

    // ========== 定时 ==========

    /** 本 Actor 的定时器；Actor 停止或重启时自动取消 */
    prop timers: TypedTimerScheduler<T> {
        get() {
            throw UnsupportedContextException("timers")
        }
    }

    /** 空闲超过 timeout 没有收到消息时向自己投递 message */
    func setReceiveTimeout(timeout: Duration, message: T): Unit {
        throw UnsupportedContextException("setReceiveTimeout")
    }

    func cancelReceiveTimeout(): Unit {
        throw UnsupportedContextException("cancelReceiveTimeout")
    }

    /** 延迟向任意 Actor 发送消息 */
    func scheduleOnce<U>(delay: Duration, target: TypedActorRef<U>, message: U): TypedCancellable {
        throw UnsupportedContextException("scheduleOnce")
    }

    // ========== 系统访问 ==========

    func system(): TypedActorSystem {
        throw UnsupportedContextException("system")
    }

    func scheduler(): TypedScheduler {
        throw UnsupportedContextException("scheduler")
    }

    /** 底层经典上下文（互操作用） */
    func toClassic(): Option<ActorContext> {
        None<ActorContext>
    }

    // ========== 兼容旧接口（Any 版本，建议改用上面的泛型方法） ==========

    func spawnChild(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any> {
        actorOf<Any>(behavior, name)
    }

    func spawnAnonymousChild(behavior: TypedBehavior<Any>): TypedActorRef<Any> {
        actorOf<Any>(behavior)
    }

    func stopChild(child: TypedActorRef<Any>): Unit {
        stop<Any>(child)
    }

    func watchActor(other: TypedActorRef<Any>): Unit {
        watch<Any>(other)
    }

    func unwatchActor(other: TypedActorRef<Any>): Unit {
        unwatch<Any>(other)
    }

    /**
     * 旧版 ask：把 message 发给 target，target 用经典上下文 reply 回复。
     * 新代码请使用 ask<Req, Res>（显式 replyTo，类型安全）。
     */
    func askMessage(target: TypedActorRef<Any>, message: Any, timeout: Duration): TypedFuture<Any> {
        throw UnsupportedContextException("askMessage")
    }

    func askMessage(target: TypedActorRef<Any>, message: Any): TypedFuture<Any> {
        askMessage(target, message, Duration.second * 3)
    }
}

/**
 * 类型安全的 Actor 系统
 */
public interface TypedActorSystem {
    prop name: String

    /** 创建顶层类型化 Actor */
    func actorOf<U>(behavior: TypedBehavior<U>, name: String): TypedActorRef<U> {
        throw UnsupportedContextException("TypedActorSystem.actorOf")
    }

    /** 创建 Actor（Any 版本，兼容旧接口） */
    func createActor(behavior: TypedBehavior<Any>, name: String): TypedActorRef<Any>

    /** 按路径查找 Actor */
    func actorSelection(path: String): TypedActorSelection<Any>

    /** 服务注册与发现 */
    func receptionist(): Receptionist {
        throw UnsupportedContextException("receptionist")
    }

    func terminate(): Bool

    func isTerminated(): Bool

    func startTime(): Int64
}

/**
 * 类型安全的 Actor 选择器
 */
public interface TypedActorSelection<T> {
    func resolveOne(timeout: Duration): Option<TypedActorRef<T>>

    func tell(message: T): Unit

    func pathString(): String
}

/**
 * Actor 内使用的日志接口
 */
public interface TypedLogger {
    func debug(message: String): Unit
    func info(message: String): Unit
    func warn(message: String): Unit
    func error(message: String): Unit
}

/**
 * 异步结果
 */
public interface TypedFuture<T> {
    func isCompleted(): Bool

    /** 阻塞等待结果；失败时抛出失败原因 */
    func get(): T

    /** 带超时等待；超时抛出 TypedAskTimeoutException */
    func get(timeout: Duration): T

    /** 已完成时的结果 */
    func value(): Option<T>

    /** 完成时回调（默认实现在新线程中等待结果） */
    func onComplete(callback: (AskOutcome<T>) -> Unit): Unit {
        let fut = this
        spawn {
            let outcome = try {
                AskOutcome<T>.Replied(fut.get())
            } catch (e: Exception) {
                AskOutcome<T>.Failed(e)
            }
            callback(outcome)
        }
        ()
    }
}

/**
 * 调度器：延迟或周期性地向 Actor 发送消息
 */
public interface TypedScheduler {
    func scheduleOnce(delay: Duration, target: TypedActorRef<Any>, message: Any): Unit

    func schedule(initialDelay: Duration, interval: Duration, target: TypedActorRef<Any>, message: Any): TypedCancellable
}

/**
 * 可取消的任务
 */
public interface TypedCancellable {
    /** 取消；已取消过返回 false */
    func cancel(): Bool

    func isCancelled(): Bool
}

/**
 * 行为工厂（对标 Akka Behaviors）
 */
public struct TypedBehaviors {
    /** 接收消息（可以使用上下文） */
    public static func receive<T>(handler: (TypedActorContext<T>, T) -> TypedBehavior<T>): TypedBehavior<T> {
        ReceiveBehavior<T>(handler)
    }

    /** 同时提供消息与信号处理函数 */
    public static func receive<T>(handler: (TypedActorContext<T>, T) -> TypedBehavior<T>,
        signalHandler: (TypedActorContext<T>, TypedSignal) -> TypedBehavior<T>): TypedBehavior<T> {
        SignalBehavior<T>(ReceiveBehavior<T>(handler), signalHandler)
    }

    /** 只关心消息本身 */
    public static func receiveMessage<T>(handler: (T) -> TypedBehavior<T>): TypedBehavior<T> {
        ReceiveBehavior<T>({ _, m => handler(m) })
    }

    /** Actor 启动时（在 Actor 线程中）用上下文创建真正的行为；重启时会重新执行 */
    public static func setup<T>(factory: (TypedActorContext<T>) -> TypedBehavior<T>): TypedBehavior<T> {
        DeferredBehavior<T>(factory)
    }

    /** 保持当前行为 */
    public static func same<T>(): TypedBehavior<T> {
        SameBehavior<T>()
    }

    /** 保持当前行为，并把消息计为未处理 */
    public static func unhandled<T>(): TypedBehavior<T> {
        UnhandledBehavior<T>()
    }

    /** 停止 Actor */
    public static func stopped<T>(): TypedBehavior<T> {
        StoppedBehavior<T>(None<() -> Unit>)
    }

    /** 停止 Actor，并在停止完成后执行 postStop */
    public static func stopped<T>(postStop: () -> Unit): TypedBehavior<T> {
        StoppedBehavior<T>(Some(postStop))
    }

    /** 接收并丢弃所有消息 */
    public static func ignore<T>(): TypedBehavior<T> {
        IgnoreBehavior<T>()
    }

    /** 不处理任何消息（全部计为未处理） */
    public static func empty<T>(): TypedBehavior<T> {
        EmptyBehavior<T>()
    }

    /** 使用 Actor 级定时器 */
    public static func withTimers<T>(factory: (TypedTimerScheduler<T>) -> TypedBehavior<T>): TypedBehavior<T> {
        DeferredBehavior<T>({ ctx => factory(ctx.timers) })
    }

    /** 使用容量为 capacity 的暂存缓冲区 */
    public static func withStash<T>(capacity: Int64, factory: (TypedStashBuffer<T>) -> TypedBehavior<T>): TypedBehavior<T> {
        DeferredBehavior<T>({ ctx => factory(TypedStashBuffer<T>(capacity, ctx)) })
    }

    /** 为行为指定失败时的监督策略（未指定时与 Akka Typed 一致：失败即停止） */
    public static func supervise<T>(behavior: TypedBehavior<T>): SupervisedBehavior<T> {
        SupervisedBehavior<T>(behavior)
    }

    /** 将外部消息类型 In 转换为内部类型 T */
    public static func transformMessages<In, T>(inner: TypedBehavior<T>, f: (In) -> T): TypedBehavior<In> {
        TransformBehavior<In, T>(inner, f)
    }

    /** 过滤消息：谓词为假的消息计为未处理 */
    public static func filter<In>(inner: TypedBehavior<In>, p: (In) -> Bool): TypedBehavior<In> {
        FilterBehavior<In>(inner, p)
    }

    /** 部分接收：谓词为真时调用处理器，否则计为未处理 */
    public static func receivePartial<T>(p: (T) -> Bool, handler: (TypedActorContext<T>, T) -> TypedBehavior<T>): TypedBehavior<T> {
        PartialBehavior<T>(p, handler)
    }

    /** mapMessages / contramap 是 transformMessages 的别名 */
    public static func mapMessages<In, T>(inner: TypedBehavior<T>, f: (In) -> T): TypedBehavior<In> {
        transformMessages(inner, f)
    }

    public static func contramap<In, T>(inner: TypedBehavior<T>, f: (In) -> T): TypedBehavior<In> {
        transformMessages(inner, f)
    }

    // ---------- 供自定义拦截器 / 测试使用的判断 ----------

    public static func isSame<T>(b: TypedBehavior<T>): Bool {
        b is SameBehavior<T>
    }

    public static func isUnhandled<T>(b: TypedBehavior<T>): Bool {
        b is UnhandledBehavior<T>
    }

    public static func isStopped<T>(b: TypedBehavior<T>): Bool {
        b is StoppedBehavior<T>
    }

    public static func isDeferred<T>(b: TypedBehavior<T>): Bool {
        b is DeferredBehavior<T>
    }

    /** 展开 setup：反复执行工厂直到得到非延迟行为 */
    public static func start<T>(b: TypedBehavior<T>, ctx: TypedActorContext<T>): TypedBehavior<T> {
        var current = b
        while (let Some(d) <- (current as DeferredBehavior<T>)) {
            current = d.factory(ctx)
        }
        // 监督：登记规则并展开内部行为
        if (let Some(sb) <- (current as SupervisedBehavior<T>)) {
            current = sb.startIn(ctx)
        }
        // 组合行为（信号 / 转换 / 过滤）展开其内部的 setup
        if (let Some(w) <- (current as StartableBehavior<T>)) {
            current = w.startIn(ctx)
        }
        current
    }

    /**
     * 根据处理结果求下一个行为：same / unhandled 保持 current，setup 被展开，其余原样返回
     */
    public static func canonicalize<T>(next: TypedBehavior<T>, current: TypedBehavior<T>, ctx: TypedActorContext<T>): TypedBehavior<T> {
        if (isSame(next) || isUnhandled(next)) {
            current
        } else {
            start(next, ctx)
        }
    }

    /**
     * 用 behavior 依次处理 messages，返回最终行为（遇到 stopped 停止处理）
     */
    public static func interpretAll<T>(behavior: TypedBehavior<T>, messages: Array<T>, ctx: TypedActorContext<T>): TypedBehavior<T> {
        var current = start(behavior, ctx)
        for (m in messages) {
            if (isStopped(current)) {
                break
            }
            current = canonicalize(current.onMessage(m, ctx), current, ctx)
        }
        current
    }
}

// ======================= 内置行为 =======================

/** 包含内部行为的组合行为：启动时展开内部行为中的 setup */
internal interface StartableBehavior<T> {
    func startIn(ctx: TypedActorContext<T>): TypedBehavior<T>
}

/** 接收行为 */
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

/** 附加了信号处理函数的行为 */
internal class SignalBehavior<T> <: TypedBehavior<T> & StartableBehavior<T> {
    private let inner: TypedBehavior<T>
    private let handler: (TypedActorContext<T>, TypedSignal) -> TypedBehavior<T>

    public init(inner: TypedBehavior<T>, handler: (TypedActorContext<T>, TypedSignal) -> TypedBehavior<T>) {
        this.inner = inner
        this.handler = handler
    }

    public func startIn(ctx: TypedActorContext<T>): TypedBehavior<T> {
        SignalBehavior<T>(TypedBehaviors.start(inner, ctx), handler)
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        // 行为不变（same / unhandled）时保留信号处理函数；切换到新行为后随旧行为一起失效（与 Akka 一致）
        inner.onMessage(message, context)
    }

    public func onSignal(signal: TypedSignal, context: TypedActorContext<T>): TypedBehavior<T> {
        let next = handler(context, signal)
        if (TypedBehaviors.isUnhandled(next)) {
            // 信号处理函数不处理时交给内部行为
            inner.onSignal(signal, context)
        } else {
            next
        }
    }

    public func getDescription(): String {
        "SignalBehavior(${inner.getDescription()})"
    }
}

/** setup：启动时用上下文创建行为 */
internal class DeferredBehavior<T> <: TypedBehavior<T> {
    let factory: (TypedActorContext<T>) -> TypedBehavior<T>

    public init(factory: (TypedActorContext<T>) -> TypedBehavior<T>) {
        this.factory = factory
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        // 未经运行时展开就直接收到消息（例如作为组合行为的内部行为）：先展开再处理
        let started = TypedBehaviors.start<T>(this, context)
        TypedBehaviors.canonicalize(started.onMessage(message, context), started, context)
    }

    public func getDescription(): String {
        "DeferredBehavior"
    }
}

/** 保持当前行为 */
internal class SameBehavior<T> <: TypedBehavior<T> {
    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        this
    }

    public func getDescription(): String {
        "SameBehavior"
    }
}

/** 保持当前行为并计为未处理 */
internal class UnhandledBehavior<T> <: TypedBehavior<T> {
    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        this
    }

    public func getDescription(): String {
        "UnhandledBehavior"
    }
}

/** 停止 */
internal class StoppedBehavior<T> <: TypedBehavior<T> {
    let postStop: Option<() -> Unit>

    public init(postStop: Option<() -> Unit>) {
        this.postStop = postStop
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        UnhandledBehavior<T>()
    }

    public func getDescription(): String {
        "StoppedBehavior"
    }
}

/** 接收并丢弃 */
internal class IgnoreBehavior<T> <: TypedBehavior<T> {
    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        SameBehavior<T>()
    }

    public func getDescription(): String {
        "IgnoreBehavior"
    }
}

/** 全部计为未处理 */
internal class EmptyBehavior<T> <: TypedBehavior<T> {
    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        UnhandledBehavior<T>()
    }

    public func getDescription(): String {
        "EmptyBehavior"
    }
}

/** 消息转换 */
internal class TransformBehavior<In, T> <: TypedBehavior<In> & StartableBehavior<In> {
    private let inner: TypedBehavior<T>
    private let f: (In) -> T

    public init(inner: TypedBehavior<T>, f: (In) -> T) {
        this.inner = inner
        this.f = f
    }

    public func startIn(ctx: TypedActorContext<In>): TypedBehavior<In> {
        TransformBehavior<In, T>(TypedBehaviors.start(inner, TransformContext<In, T>(ctx)), f)
    }

    public func onMessage(message: In, context: TypedActorContext<In>): TypedBehavior<In> {
        let innerCtx = TransformContext<In, T>(context)
        let current = TypedBehaviors.start(inner, innerCtx)
        let next = current.onMessage(f(message), innerCtx)
        wrap(next, current, innerCtx)
    }

    public func onSignal(signal: TypedSignal, context: TypedActorContext<In>): TypedBehavior<In> {
        let innerCtx = TransformContext<In, T>(context)
        let current = TypedBehaviors.start(inner, innerCtx)
        wrap(current.onSignal(signal, innerCtx), current, innerCtx)
    }

    private func wrap(next: TypedBehavior<T>, current: TypedBehavior<T>, ctx: TypedActorContext<T>): TypedBehavior<In> {
        if (TypedBehaviors.isUnhandled(next)) {
            UnhandledBehavior<In>()
        } else if (TypedBehaviors.isSame(next)) {
            if (TypedBehaviors.isDeferred(inner)) {
                TransformBehavior<In, T>(current, f)
            } else {
                SameBehavior<In>()
            }
        } else if (let Some(s) <- (next as StoppedBehavior<T>)) {
            StoppedBehavior<In>(s.postStop)
        } else {
            TransformBehavior<In, T>(TypedBehaviors.start(next, ctx), f)
        }
    }

    public func getDescription(): String {
        "TransformBehavior"
    }
}

/** 过滤 */
internal class FilterBehavior<In> <: TypedBehavior<In> & StartableBehavior<In> {
    private let inner: TypedBehavior<In>
    private let p: (In) -> Bool

    public init(inner: TypedBehavior<In>, p: (In) -> Bool) {
        this.inner = inner
        this.p = p
    }

    public func startIn(ctx: TypedActorContext<In>): TypedBehavior<In> {
        FilterBehavior<In>(TypedBehaviors.start(inner, ctx), p)
    }

    public func onMessage(message: In, context: TypedActorContext<In>): TypedBehavior<In> {
        if (!p(message)) {
            return UnhandledBehavior<In>()
        }
        let current = TypedBehaviors.start(inner, context)
        let next = current.onMessage(message, context)
        if (TypedBehaviors.isUnhandled(next) || TypedBehaviors.isStopped(next)) {
            next
        } else if (TypedBehaviors.isSame(next)) {
            if (TypedBehaviors.isDeferred(inner)) {
                FilterBehavior<In>(current, p)
            } else {
                SameBehavior<In>()
            }
        } else {
            FilterBehavior<In>(TypedBehaviors.start(next, context), p)
        }
    }

    public func onSignal(signal: TypedSignal, context: TypedActorContext<In>): TypedBehavior<In> {
        inner.onSignal(signal, context)
    }

    public func getDescription(): String {
        "FilterBehavior"
    }
}

/** 部分接收 */
internal class PartialBehavior<T> <: TypedBehavior<T> {
    private let p: (T) -> Bool
    private let handler: (TypedActorContext<T>, T) -> TypedBehavior<T>

    public init(p: (T) -> Bool, handler: (TypedActorContext<T>, T) -> TypedBehavior<T>) {
        this.p = p
        this.handler = handler
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        if (p(message)) {
            handler(context, message)
        } else {
            UnhandledBehavior<T>()
        }
    }

    public func getDescription(): String {
        "PartialBehavior"
    }
}

/**
 * transformMessages 内部行为看到的上下文：除 self / timers / messageAdapter 等与消息类型 T 相关的能力外，
 * 其余全部委托给外层上下文
 */
internal class TransformContext<In, T> <: TypedActorContext<T> {
    private let underlying: TypedActorContext<In>

    public init(underlying: TypedActorContext<In>) {
        this.underlying = underlying
    }

    public prop self: TypedActorRef<T> {
        get() {
            throw UnsupportedContextException("self（transformMessages 内部行为请使用外层引用）")
        }
    }

    public func sender(): Option<String> { underlying.sender() }
    public func stopSelf(): Unit { underlying.stopSelf() }
    public func log(): TypedLogger { underlying.log() }
    public func actorOf<U>(behavior: TypedBehavior<U>, name: String): TypedActorRef<U> { underlying.actorOf<U>(behavior, name) }
    public func actorOf<U>(behavior: TypedBehavior<U>, name: String, props: TypedProps): TypedActorRef<U> { underlying.actorOf<U>(behavior, name, props) }
    public func actorOf<U>(behavior: TypedBehavior<U>): TypedActorRef<U> { underlying.actorOf<U>(behavior) }
    public func stop<U>(child: TypedActorRef<U>): Unit { underlying.stop<U>(child) }
    public func childAs<U>(name: String): Option<TypedActorRef<U>> { underlying.childAs<U>(name) }
    public func child(name: String): Option<TypedActorRef<Any>> { underlying.child(name) }
    public func children(): Array<TypedActorRef<Any>> { underlying.children() }
    public func watch<U>(other: TypedActorRef<U>): Unit { underlying.watch<U>(other) }
    public func unwatch<U>(other: TypedActorRef<U>): Unit { underlying.unwatch<U>(other) }
    public func scheduleOnce<U>(delay: Duration, target: TypedActorRef<U>, message: U): TypedCancellable { underlying.scheduleOnce<U>(delay, target, message) }
    public func system(): TypedActorSystem { underlying.system() }
    public func scheduler(): TypedScheduler { underlying.scheduler() }
    public func toClassic(): Option<ActorContext> { underlying.toClassic() }
    public func askMessage(target: TypedActorRef<Any>, message: Any, timeout: Duration): TypedFuture<Any> { underlying.askMessage(target, message, timeout) }
}
