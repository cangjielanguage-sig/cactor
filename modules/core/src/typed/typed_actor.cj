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

    /**
     * 保持当前行为（P2-20）：与 `TypedBehaviors.same<T>()` 语义相同，但返回本 Actor 缓存的
     * 同一实例，不产生堆分配。`same()` 在每个消息处理末尾都会调用，是类型化路径上的
     * 每消息第三次分配；泛型类静态单例在仓颉不可行（禁止引用 T），所以按"每 Actor 一个"
     * 缓存——实例只在 Actor 线程内创建与使用，无并发问题。
     */
    func same(): TypedBehavior<T> {
        TypedBehaviors.same<T>()
    }

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
        actorOfWithHooks<U>(behavior, Some(name), TypedProps())
    }

    /** 指定调度器 / 邮箱创建子 Actor */
    func actorOf<U>(behavior: TypedBehavior<U>, name: String, props: TypedProps): TypedActorRef<U> {
        actorOfWithHooks<U>(behavior, Some(name), props)
    }

    /** 创建自动命名的子 Actor */
    func actorOf<U>(behavior: TypedBehavior<U>): TypedActorRef<U> {
        actorOfWithHooks<U>(behavior, None<String>, TypedProps())
    }

    /**
     * 子 Actor 创建的实现入口：上下文实现覆写本方法，三个 actorOf 用默认实现。
     *
     * 热路径 hooks（T16）：ctx 是接口，actorOf 是虚调用，泛型方法在 core 内实例化——在这里构造的 hooks
     * 每条消息都要做运行时类型查找（实测子 Actor 吞吐只有顶层 typedActorOf 的 45–70%），@Frozen 对虚调用无效。
     * 能在调用方包里构造 hooks 的只有静态调用的 @Frozen 工厂：setup / supervise 在构造行为时顺带构造好 hooks，
     * 实现方用 carriedHooksOf(behavior) 取出。因此子 Actor 的初始行为以 setup（Akka 惯用写法）
     * 或 supervise(setup(...)) 开头即走快路径；直接传 receive 的子 Actor 退回 core 内的默认 hooks（正确，较慢）
     */
    func actorOfWithHooks<U>(behavior: TypedBehavior<U>, name: Option<String>, props: TypedProps): TypedActorRef<U> {
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
    // 下面构造行为的工厂函数标为 @Frozen：-O2 下函数体在调用方的包里展开，`SameBehavior<T>()` 等分配也在那里实例化。
    // 否则 core 的泛型代码以别的包的类型 T 实例化时，每次分配 C<T> 都要在运行时查找实例化类型的类型信息，
    // 实测 same() 单线程约 40ns、4 个线程并发时约 0.4µs 且互相阻塞；展开后约 20ns，不随线程数变化。
    // 用户每处理一条消息都会调用 same() / receive() 之一，这是类型化路径上最热的分配

    /** 接收消息（可以使用上下文） */
    @Frozen
    public static func receive<T>(handler: (TypedActorContext<T>, T) -> TypedBehavior<T>): TypedBehavior<T> {
        ReceiveBehavior<T>(handler)
    }

    /** 同时提供消息与信号处理函数 */
    @Frozen
    public static func receive<T>(handler: (TypedActorContext<T>, T) -> TypedBehavior<T>,
        signalHandler: (TypedActorContext<T>, TypedSignal) -> TypedBehavior<T>): TypedBehavior<T> {
        SignalBehavior<T>(ReceiveBehavior<T>(handler), signalHandler)
    }

    /** 只关心消息本身 */
    @Frozen
    public static func receiveMessage<T>(handler: (T) -> TypedBehavior<T>): TypedBehavior<T> {
        ReceiveBehavior<T>({ _, m => handler(m) })
    }

    /** Actor 启动时（在 Actor 线程中）用上下文创建真正的行为；重启时会重新执行 */
    @Frozen
    public static func setup<T>(factory: (TypedActorContext<T>) -> TypedBehavior<T>): TypedBehavior<T> {
        // @Frozen 在调用方包展开：顺带在这里构造热路径 hooks，ctx.actorOf 创建子 Actor 时取用（T16）。
        // 每次 setup 多 7 个小对象；setup 通常每个 Actor 只构造一次（重启重跑的是工厂，不是 setup 本身）
        DeferredBehavior<T>(factory, Some(TypedSpawn.hooksFor<T>()))
    }

    /** 保持当前行为。每消息场景推荐在处理函数里用 `ctx.same()`（复用每 Actor 缓存实例，零分配，P2-20） */
    @Frozen
    public static func same<T>(): TypedBehavior<T> {
        SameBehavior<T>()
    }

    /** 保持当前行为，并把消息计为未处理 */
    @Frozen
    public static func unhandled<T>(): TypedBehavior<T> {
        UnhandledBehavior<T>()
    }

    /** 停止 Actor */
    @Frozen
    public static func stopped<T>(): TypedBehavior<T> {
        StoppedBehavior<T>(None<() -> Unit>)
    }

    /** 停止 Actor，并在停止完成后执行 postStop */
    @Frozen
    public static func stopped<T>(postStop: () -> Unit): TypedBehavior<T> {
        StoppedBehavior<T>(Some(postStop))
    }

    /** 接收并丢弃所有消息 */
    @Frozen
    public static func ignore<T>(): TypedBehavior<T> {
        IgnoreBehavior<T>()
    }

    /** 不处理任何消息（全部计为未处理） */
    @Frozen
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
    @Frozen
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

    // 用 SpecialBehavior 的种类标记判断，而不是 `b is SameBehavior<T>`（原因见 SpecialBehavior）
    public static func isSame<T>(b: TypedBehavior<T>): Bool {
        specialKindOf(b) == SpecialBehavior.SAME
    }

    public static func isUnhandled<T>(b: TypedBehavior<T>): Bool {
        specialKindOf(b) == SpecialBehavior.UNHANDLED
    }

    public static func isStopped<T>(b: TypedBehavior<T>): Bool {
        specialKindOf(b) == SpecialBehavior.STOPPED
    }

    /** stopped 行为的 postStop 回调（非 stopped 返回 None）。供用户包的 @Frozen hooks 使用。 */
    public static func stoppedCallbackOf<T>(b: TypedBehavior<T>): Option<() -> Unit> {
        match (b as StoppedBehavior<T>) {
            case Some(sb) => sb.postStop
            case None => None<() -> Unit>
        }
    }

    public static func isDeferred<T>(b: TypedBehavior<T>): Bool {
        specialKindOf(b) == SpecialBehavior.DEFERRED
    }

    /** 展开 setup：反复执行工厂直到得到非延迟行为 */
    public static func start<T>(b: TypedBehavior<T>, ctx: TypedActorContext<T>): TypedBehavior<T> {
        // 按种类标记分派，而不是逐个 `as DeferredBehavior<T>` / `as StartableBehavior<T>`（原因见 SpecialBehavior）：
        // 每条消息返回的新行为绝大多数是普通行为，只做一次非泛型类检查就返回
        var current = b
        var kind = specialKindOf(current)
        while (kind == SpecialBehavior.DEFERRED) {
            current = (current as DeferredBehavior<T>).getOrThrow().factory(ctx)
            kind = specialKindOf(current)
        }
        // 监督：登记规则并展开内部行为
        if (kind == SpecialBehavior.SUPERVISED) {
            current = (current as SupervisedBehavior<T>).getOrThrow().startIn(ctx)
            kind = specialKindOf(current)
        }
        // 组合行为（信号 / 转换 / 过滤 / 拦截）展开其内部的 setup
        if (kind == SpecialBehavior.STARTABLE) {
            current = (current as StartableBehavior<T>).getOrThrow().startIn(ctx)
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
internal class SignalBehavior<T> <: SpecialBehavior & TypedBehavior<T> & StartableBehavior<T> {
    private let inner: TypedBehavior<T>
    private let handler: (TypedActorContext<T>, TypedSignal) -> TypedBehavior<T>

    public init(inner: TypedBehavior<T>, handler: (TypedActorContext<T>, TypedSignal) -> TypedBehavior<T>) {
        super(SpecialBehavior.STARTABLE)
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

    public override func wrappedBehavior(): Option<Any> {
        Some<Any>(inner)
    }

    public func getDescription(): String {
        "SignalBehavior(${inner.getDescription()})"
    }
}

/**
 * 需要特殊处理的行为（same / unhandled / stopped / setup / 监督 / 组合行为）的非泛型基类，用种类标记区分。
 * 每处理一条消息都要判断返回的是不是这几种行为、要不要展开。core 里的泛型代码以别的包的类型 T 实例化后，
 * `b is SameBehavior<T>` 这类泛型类型检查实测单线程约 30ns，4 个线程并发时约 0.4µs CPU 且互相阻塞；
 * 对非泛型基类做一次类检查再比较标记则不受影响。用户行为不继承它，标记为 0
 */
internal abstract class SpecialBehavior {
    static let SAME: Int64 = 1
    static let UNHANDLED: Int64 = 2
    static let STOPPED: Int64 = 3
    static let DEFERRED: Int64 = 4
    // SupervisedBehavior
    static let SUPERVISED: Int64 = 5
    // 实现 StartableBehavior 的组合行为
    static let STARTABLE: Int64 = 6
    // 运行中的监督包裹层（ActiveSupervision，SupervisedBehavior 启动后的形态）
    static let ACTIVE_SUPERVISION: Int64 = 7

    let specialKind: Int64

    init(specialKind: Int64) {
        this.specialKind = specialKind
    }

    /** 持续包裹内层行为的组合行为返回内层（类型擦除）：失败时沿它收集监督层（冷路径） */
    public open func wrappedBehavior(): Option<Any> {
        None<Any>
    }

    /** 监督包裹层返回本层规则（Array<SupervisionRule>，类型擦除：规则类型是包内的，不能出现在 open 方法签名里） */
    public open func supervisionLayer(): Option<Any> {
        None<Any>
    }

    /** 由 @Frozen 工厂在调用方包里构造的热路径 hooks（setup / supervise 携带，见 TypedActorContext.actorOfWithHooks） */
    public open func carriedHooks(): Option<TypedHooks> {
        None<TypedHooks>
    }
}

/** 行为携带的调用方包 hooks（没有则 None，创建方退回默认 hooks） */
func carriedHooksOf(behavior: Any): Option<TypedHooks> {
    match (behavior) {
        case s: SpecialBehavior => s.carriedHooks()
        case _ => None<TypedHooks>
    }
}

func specialKindOf(b: Any): Int64 {
    match (b) {
        case s: SpecialBehavior => s.specialKind
        case _ => 0
    }
}

/** setup：启动时用上下文创建行为 */
internal class DeferredBehavior<T> <: SpecialBehavior & TypedBehavior<T> {
    let factory: (TypedActorContext<T>) -> TypedBehavior<T>
    let hooks: Option<TypedHooks>

    public init(factory: (TypedActorContext<T>) -> TypedBehavior<T>) {
        this(factory, None<TypedHooks>)
    }

    public init(factory: (TypedActorContext<T>) -> TypedBehavior<T>, hooks: Option<TypedHooks>) {
        super(SpecialBehavior.DEFERRED)
        this.factory = factory
        this.hooks = hooks
    }

    public override func carriedHooks(): Option<TypedHooks> {
        hooks
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
internal class SameBehavior<T> <: SpecialBehavior & TypedBehavior<T> {
    public init() {
        super(SpecialBehavior.SAME)
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        this
    }

    public func getDescription(): String {
        "SameBehavior"
    }
}

/** 保持当前行为并计为未处理 */
internal class UnhandledBehavior<T> <: SpecialBehavior & TypedBehavior<T> {
    public init() {
        super(SpecialBehavior.UNHANDLED)
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        this
    }

    public func getDescription(): String {
        "UnhandledBehavior"
    }
}

/** 停止 */
internal class StoppedBehavior<T> <: SpecialBehavior & TypedBehavior<T> {
    let postStop: Option<() -> Unit>

    public init(postStop: Option<() -> Unit>) {
        super(SpecialBehavior.STOPPED)
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
internal class TransformBehavior<In, T> <: SpecialBehavior & TypedBehavior<In> & StartableBehavior<In> {
    private let inner: TypedBehavior<T>
    private let f: (In) -> T

    public init(inner: TypedBehavior<T>, f: (In) -> T) {
        super(SpecialBehavior.STARTABLE)
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
        } else if (TypedBehaviors.isStopped(next)) {
            StoppedBehavior<In>((next as StoppedBehavior<T>).getOrThrow().postStop)
        } else {
            TransformBehavior<In, T>(TypedBehaviors.start(next, ctx), f)
        }
    }

    public override func wrappedBehavior(): Option<Any> {
        Some<Any>(inner)
    }

    public func getDescription(): String {
        "TransformBehavior"
    }
}

/** 过滤 */
internal class FilterBehavior<In> <: SpecialBehavior & TypedBehavior<In> & StartableBehavior<In> {
    private let inner: TypedBehavior<In>
    private let p: (In) -> Bool

    public init(inner: TypedBehavior<In>, p: (In) -> Bool) {
        super(SpecialBehavior.STARTABLE)
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

    public override func wrappedBehavior(): Option<Any> {
        Some<Any>(inner)
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
internal class TransformContext<In, T> <: TypedActorContext<T> & SupervisionHost {
    private let underlying: TypedActorContext<In>

    public init(underlying: TypedActorContext<In>) {
        this.underlying = underlying
    }

    // transformMessages 内层的 supervise 登记到外层 Actor 的监督状态（此前内层上下文不是经典上下文，内层 supervise 完全不生效）
    public func pushStarting(rules: Array<SupervisionRule>): Unit {
        if (let Some(h) <- supervisionHostOf(underlying)) {
            h.pushStarting(rules)
        }
    }

    public func popStarting(): Unit {
        if (let Some(h) <- supervisionHostOf(underlying)) {
            h.popStarting()
        }
    }

    public prop self: TypedActorRef<T> {
        get() {
            throw UnsupportedContextException("self（transformMessages 内部行为请使用外层引用）")
        }
    }

    public func sender(): Option<String> { underlying.sender() }
    public func stopSelf(): Unit { underlying.stopSelf() }
    public func log(): TypedLogger { underlying.log() }
    public func actorOfWithHooks<U>(behavior: TypedBehavior<U>, name: Option<String>, props: TypedProps): TypedActorRef<U> {
        underlying.actorOfWithHooks<U>(behavior, name, props)
    }
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
