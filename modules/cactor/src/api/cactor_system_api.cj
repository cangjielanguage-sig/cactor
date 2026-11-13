/*
 * Copyright (c) 2024. All rights reserved.
 * CActor API - 参考Akka设计的统一Actor系统API
 * 按照plan10.md要求，提供统一的ActorSystem API，不直接导出core层
 * 设计理念：简洁、类型安全、隐藏实现细节
 */
package cactor.api

import cactor_core.actor.{Actor, ActorRef, Props, PropsFactory, ActorScheduler}
import cactor_core.config.{MailboxConfig as CoreMailboxConfig, DispatcherConfig as CoreDispatcherConfig, SupervisionConfig as CoreSupervisionConfig}
import cactor_core.typed.{TypedBehavior, TypedBehaviors, TypedActorRef, TypedActorSelection, TypedActorContext, ClassicTypedRef, TypedProps, TypedHooks, TypedSignal, NOT_MATCHED}
import std.sync.{Mutex}
import cactor_runtime.wrappers.{TypedActorRefWrapper, MessageBridge}
import cactor_runtime.{CActorRuntime, CActorRuntimeConfig}
import cactor_runtime.typed.{TypedActorSystemImpl}
import std.time.*
import cactor_core.message.{Message}

/**
 * CActorSystem - 参考Akka设计的统一Actor系统API
 * 提供类型安全、简洁的Actor创建接口
 */
public class CActorSystem {
    private let runtime: CActorRuntime
    private let typedLock = Mutex()
    private var typedValue: Option<TypedActorSystemImpl> = None<TypedActorSystemImpl>

    public init(name: String) {
        let config = CActorRuntimeConfig.createDefault()
        this.runtime = CActorRuntime(name, config)
        runtime.start()
    }

    public init(name: String, config: CActorRuntimeConfig) {
        this.runtime = CActorRuntime(name, config)
        runtime.start()
    }

    /**
     * 创建Actor - 参考Akka的actorOf方法
     * 使用类型擦除来处理泛型问题
     */
    public func actorOf(creator: () -> Actor, name: String): ActorRef {
        let props = PropsFactory.create(creator)
        runtime.actorOf(props, name)
    }

    /**
     * 创建Actor（自动命名）- 参考Akka的actorOf方法
     */
    public func actorOf(creator: () -> Actor): ActorRef {
        let props = PropsFactory.create(creator)
        runtime.actorOf(props)
    }

    /**
     * 使用ActorProps创建Actor
     */
    public func actorOf(actorProps: ActorProps, name: String): ActorRef {
        let creator = actorProps.getCreator()
        actorOf(creator, name)
    }

    /**
     * 使用ActorProps创建Actor（自动命名）
     */
    public func actorOf(actorProps: ActorProps): ActorRef {
        let creator = actorProps.getCreator()
        actorOf(creator)
    }

    /**
     * 使用Props创建Actor - 支持完整配置传递（类似Akka）
     */
    public func actorOf(props: Props<Actor>, name: String): ActorRef {
        runtime.actorOf(props, name)
    }

    /**
     * 使用Props创建Actor（自动命名）- 支持完整配置传递
     */
    public func actorOf(props: Props<Actor>): ActorRef {
        runtime.actorOf(props)
    }

    /**
     * 使用邮箱配置创建Actor - 类似Akka的withMailbox
     */
    public func actorOfWithMailbox(creator: () -> Actor, name: String, mailboxConfig: CoreMailboxConfig): ActorRef {
        let props = PropsFactory.create(creator).withMailbox(mailboxConfig.name)
        runtime.actorOf(props, name)
    }

    /**
     * 使用调度器配置创建Actor - 类似Akka的withDispatcher
     */
    public func actorOfWithDispatcher(creator: () -> Actor, name: String, dispatcherConfig: CoreDispatcherConfig): ActorRef {
        let props = PropsFactory.create(creator).withDispatcher(dispatcherConfig.name)
        runtime.actorOf(props, name)
    }

    /**
     * 使用完整配置创建Actor - 类似Akka的完整配置方式
     */
    public func actorOfConfigured(creator: () -> Actor,
                                 name: String,
                                 mailboxConfig: CoreMailboxConfig,
                                 dispatcherConfig: CoreDispatcherConfig,
                                 supervisionConfig: CoreSupervisionConfig): ActorRef {
        let props = PropsFactory.create(creator)
            .withMailbox(mailboxConfig.name)
            .withDispatcher(dispatcherConfig.name)
            .withSupervisionStrategy(supervisionConfig.name)
        runtime.actorOf(props, name)
    }

    /**
     * 获取系统名称
     */
    public func getName(): String {
        runtime.getRuntimeStats().systemName
    }

    /** 系统调度器（E52）：定时器、接收超时、退避重启、Actor 内 ask 超时都经它安排 */
    public func scheduler(): ActorScheduler {
        runtime.scheduler()
    }

    /**
     * 终止系统 - 参考Akka的terminate方法
     */
    public func terminate(): Unit {
        runtime.shutdown()
    }

    /**
     * 停止 Actor（对标 Akka `system.stop`）：处理完当前消息即停，邮箱中剩余的消息计为死信，子 Actor 先于它停止；
     * 名字保留到真正终止，终止前同名 actorOf 报"名称已存在"。顶层与子 Actor 都适用。
     * 需要"处理完已排队的消息再停"请用 gracefulStop（或发 PoisonPill）
     */
    public func stop(actorRef: ActorRef): Unit {
        runtime.stop(actorRef)
    }

    /**
     * 处理完已入队的消息后停止，并等待终止（对标 Akka `Patterns.gracefulStop`）：
     * 返回 true 表示已终止且名字已释放，超时返回 false。会阻塞，只能在 Actor 之外调用
     */
    public func gracefulStop(actorRef: ActorRef, timeout: Duration): Bool {
        runtime.gracefulStop(actorRef, timeout)
    }

    /**
     * 按路径查找存活的 Actor（包括子 Actor），例如 "/user/parent/child"
     */
    public func lookup(path: String): Option<ActorRef> {
        runtime.lookup(path)
    }

    /**
     * 订阅死信：投递失败的消息（目标已停止、有界邮箱已满、回复对象不存在等），
     * 以及包装成 UnhandledMessage 的未处理消息（对标 Akka AllDeadLetters；用 `is UnhandledMessage` 区分）。
     * 回调参数为消息和原目标路径；回调在发送方（或处理方）线程上执行，应尽快返回
     */
    public func subscribeDeadLetters(listener: (Message, String) -> Unit): Unit {
        runtime.subscribeDeadLetters(listener)
    }

    /** 系统内累计的死信数 */
    public func deadLetterCount(): Int64 {
        runtime.deadLetterCount()
    }

    /**
     * 把投递不出去的消息交给死信（计入 deadLetterCount，并通知 subscribeDeadLetters 的订阅者）。
     * recipient 为原目标的描述（例如 "sharding/cart/user-42"）。供自己做路由的组件使用（对标 Akka system.deadLetters）
     */
    public func publishDeadLetter(message: Message, recipient: String): Unit {
        runtime.publishDeadLetter(message, recipient)
    }

    /**
     * 系统内累计的未处理消息数（receive 返回 Unhandled、类型化 Actor 收到协议之外的消息；不含死信）。
     * 每条未处理消息同时以 UnhandledMessage 交给 subscribeDeadLetters 的订阅者（对标 Akka AllDeadLetters）
     */
    public func unhandledCount(): Int64 {
        runtime.unhandledCount()
    }

    /**
     * 检查是否已终止
     */
    public func isTerminated(): Bool {
        !runtime.getRuntimeStats().isRunning
    }

    /**
     * 获取底层运行时（用于高级功能）
     */
    public func getRuntime(): CActorRuntime {
        runtime
    }

    /**
     * Prometheus 文本格式的指标，可直接作为 /metrics 响应体
     * @param perActor 是否包含每个 Actor 的指标
     */
    public func metricsText(perActor: Bool): String {
        runtime.prometheusMetrics(perActor)
    }

    /**
     * @param maxActorSeries 每 Actor 指标最多覆盖的 Actor 数（标签基数上限，默认 DEFAULT_MAX_ACTOR_SERIES）
     */
    public func metricsText(perActor: Bool, maxActorSeries: Int64): String {
        runtime.prometheusMetrics(perActor, maxActorSeries)
    }

    // ========== 类型安全Actor支持 ==========

    /**
     * 创建类型安全Actor - 统一API
     *
     * @param behavior 类型安全的Actor行为
     * @param name Actor名称
     * @return 类型安全的Actor引用
     *
     * @Frozen：函数体在调用方（用户）包里展开，其中的桥接闭包 `any as T` 因此编译在用户包里
     * （具体类型转换 4–10ns），而不是 core 泛型代码里的运行时类型查找（多线程下互相阻塞）。
     * 消息路径见 core 的 TypedPayloadBox / TypedSpawn.props 说明。
     */
    @Frozen
    public func typedActorOf<T>(behavior: TypedBehavior<T>, name: String): TypedActorRef<T> {
        // 非泛型热路径闭包：全部在这里（用户包）编译，core 侧调用不触发运行时类型查找。
        // 实测：core 泛型代码调用涉及 T 的闭包约 700ms/600万次，纯 Any→Any 约 120ms（快 6 倍）。
        let unpack: (Any) -> Any = { a => match (a as T) { case Some(m) => m; case None => NOT_MATCHED } }
        let onMsg: (Any, Any, Any) -> Any = { b, m, cx =>
            (b as TypedBehavior<T>).getOrThrow().onMessage((m as T).getOrThrow(), (cx as TypedActorContext<T>).getOrThrow())
        }
        let onSig: (Any, Any, Any) -> Any = { b, sg, cx =>
            (b as TypedBehavior<T>).getOrThrow().onSignal((sg as TypedSignal).getOrThrow(), (cx as TypedActorContext<T>).getOrThrow())
        }
        let st: (Any, Any) -> Any = { b, cx =>
            TypedBehaviors.start<T>((b as TypedBehavior<T>).getOrThrow(), (cx as TypedActorContext<T>).getOrThrow())
        }
        let stoppedCb: (Any) -> Option<() -> Unit> = { b =>
            TypedBehaviors.stoppedCallbackOf<T>((b as TypedBehavior<T>).getOrThrow())
        }
        let desc: (Any) -> String = { b => (b as TypedBehavior<T>).getOrThrow().getDescription() }
        let hooks = TypedHooks(unpack, onMsg, onSig, st, stoppedCb, desc)
        typed().actorOfVia<T>(behavior, name, TypedProps(), Some(hooks))
    }

    /** 指定调度器 / 邮箱创建类型安全 Actor */
    @Frozen
    public func typedActorOf<T>(behavior: TypedBehavior<T>, name: String, props: TypedProps): TypedActorRef<T> {
        // 非泛型热路径闭包：全部在这里（用户包）编译，core 侧调用不触发运行时类型查找。
        // 实测：core 泛型代码调用涉及 T 的闭包约 700ms/600万次，纯 Any→Any 约 120ms（快 6 倍）。
        let unpack: (Any) -> Any = { a => match (a as T) { case Some(m) => m; case None => NOT_MATCHED } }
        let onMsg: (Any, Any, Any) -> Any = { b, m, cx =>
            (b as TypedBehavior<T>).getOrThrow().onMessage((m as T).getOrThrow(), (cx as TypedActorContext<T>).getOrThrow())
        }
        let onSig: (Any, Any, Any) -> Any = { b, sg, cx =>
            (b as TypedBehavior<T>).getOrThrow().onSignal((sg as TypedSignal).getOrThrow(), (cx as TypedActorContext<T>).getOrThrow())
        }
        let st: (Any, Any) -> Any = { b, cx =>
            TypedBehaviors.start<T>((b as TypedBehavior<T>).getOrThrow(), (cx as TypedActorContext<T>).getOrThrow())
        }
        let stoppedCb: (Any) -> Option<() -> Unit> = { b =>
            TypedBehaviors.stoppedCallbackOf<T>((b as TypedBehavior<T>).getOrThrow())
        }
        let desc: (Any) -> String = { b => (b as TypedBehavior<T>).getOrThrow().getDescription() }
        let hooks = TypedHooks(unpack, onMsg, onSig, st, stoppedCb, desc)
        typed().actorOfVia<T>(behavior, name, props, Some(hooks))
    }

    /**
     * 本系统的类型化视图（共享同一个运行时；提供 ask、receptionist、findActor 等）
     */
    public func typed(): TypedActorSystemImpl {
        synchronized(typedLock) {
            match (typedValue) {
                case Some(t) => t
                case None =>
                    let t = TypedActorSystemImpl(this.getName(), runtime)
                    typedValue = Some(t)
                    t
            }
        }
    }

    /**
     * 创建类型安全Actor（自动命名）
     *
     * @param behavior 类型安全的Actor行为
     * @return 类型安全的Actor引用
     */
    @Frozen
    public func typedActorOf<T>(behavior: TypedBehavior<T>): TypedActorRef<T> {
        let name = "typed-actor-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
        // 非泛型热路径闭包：全部在这里（用户包）编译，core 侧调用不触发运行时类型查找。
        // 实测：core 泛型代码调用涉及 T 的闭包约 700ms/600万次，纯 Any→Any 约 120ms（快 6 倍）。
        let unpack: (Any) -> Any = { a => match (a as T) { case Some(m) => m; case None => NOT_MATCHED } }
        let onMsg: (Any, Any, Any) -> Any = { b, m, cx =>
            (b as TypedBehavior<T>).getOrThrow().onMessage((m as T).getOrThrow(), (cx as TypedActorContext<T>).getOrThrow())
        }
        let onSig: (Any, Any, Any) -> Any = { b, sg, cx =>
            (b as TypedBehavior<T>).getOrThrow().onSignal((sg as TypedSignal).getOrThrow(), (cx as TypedActorContext<T>).getOrThrow())
        }
        let st: (Any, Any) -> Any = { b, cx =>
            TypedBehaviors.start<T>((b as TypedBehavior<T>).getOrThrow(), (cx as TypedActorContext<T>).getOrThrow())
        }
        let stoppedCb: (Any) -> Option<() -> Unit> = { b =>
            TypedBehaviors.stoppedCallbackOf<T>((b as TypedBehavior<T>).getOrThrow())
        }
        let desc: (Any) -> String = { b => (b as TypedBehavior<T>).getOrThrow().getDescription() }
        let hooks = TypedHooks(unpack, onMsg, onSig, st, stoppedCb, desc)
        typed().actorOfVia<T>(behavior, name, TypedProps(), Some(hooks))
    }

    /**
     * 类型安全Actor选择 - 根据路径查找Actor
     *
     * @param path Actor路径
     * @return 类型安全的Actor选择器
     */
    public func typedActorSelection<T>(path: String): TypedActorSelection<T> {
        typed().actorSelectionAs<T>(path)
    }

    /**
     * 获取系统启动时间
     *
     * @return 启动时间戳
     */
    public func getStartTime(): Int64 {
        // 简化实现：返回当前时间
        DateTime.now().toUnixTimeStamp().toMilliseconds()
    }

    // ========== Actor交互支持 ==========

    /**
     * 将标准ActorRef转换为类型安全ActorRef
     * 支持两种Actor之间的交互
     *
     * @param actorRef 标准Actor引用
     * @param name Actor名称
     * @return 类型安全的Actor引用
     */
    public func wrapAsTypedActor<T>(actorRef: ActorRef, name: String): TypedActorRef<T> {
        ClassicTypedRef<T>(actorRef)
    }

    /**
     * 从类型安全ActorRef获取底层标准ActorRef
     * 支持两种Actor之间的交互
     *
     * @param typedActorRef 类型安全Actor引用
     * @return 标准Actor引用
     */
    public func unwrapTypedActor<T>(typedActorRef: TypedActorRef<T>): ActorRef {
        match (typedActorRef) {
            case wrapper: TypedActorRefWrapper<T> => wrapper.getUnderlyingActorRef()
            case _ =>
                match (typedActorRef.toClassic()) {
                    case Some(c) => c
                    case None => throw IllegalArgumentException("${typedActorRef.pathString()} 不是由 Actor 支撑的引用")
                }
        }
    }

    /**
     * 创建混合消息桥接器
     * 允许标准Actor和类型安全Actor之间发送消息
     *
     * @param standardActor 标准Actor引用
     * @param typedActor 类型安全Actor引用
     * @return 消息桥接器
     */
    public func createMessageBridge<T>(standardActor: ActorRef, typedActor: TypedActorRef<T>): MessageBridge<T> {
        MessageBridge<T>(standardActor, typedActor)
    }
}



/**
 * CActorSystem工厂方法 - 参考Akka的ActorSystem.create()
 */
public struct CActorSystemFactory {
    /**
     * 创建默认的CActorRuntime系统
     */
    public static func create(): CActorSystem {
        create("default")
    }

    /**
     * 创建命名的CActorRuntime系统
     */
    public static func create(name: String): CActorSystem {
        CActorSystem(name)
    }

    /**
     * 创建配置的CActorRuntime系统
     */
    public static func create(name: String, config: CActorRuntimeConfig): CActorSystem {
        CActorSystem(name, config)
    }
}
