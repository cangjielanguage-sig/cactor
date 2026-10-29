/*
 * CActor Core.Typed - 类型化 Actor 的运行时桥接
 *
 * 类型化 Actor 运行在经典 Actor Cell 上：
 * - TypedActorRef<T>.tell(m) 把 m 包装成 TypedEnvelope<T> 投递给经典引用
 * - TypedActorAdapter<T> 是一个经典 Actor，持有当前行为并解释 same / unhandled / stopped / setup
 * - ClassicTypedContext<T> 用经典上下文实现类型化上下文（子 Actor、监视、定时器、ask、适配器等）
 * - setup 在 Actor.onStarted 中执行（Actor 线程内、处理第一条消息之前，重启后同样执行）
 *
 * 因此类型化 Actor 与经典 Actor 共享监督树、死亡监视、邮箱、调度器与死信等全部运行时能力。
 */
package cactor_core.typed

import std.time.*
import std.sync.*
import std.collection.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}
import cactor_foundation.logging.{Logger, Logging}

/** 可以取出任意类型载荷的消息（用于兼容载荷类型与目标协议是子类型关系的情况） */
interface PayloadCarrier {
    func anyPayload(): Any
}

/**
 * 类型化消息在经典邮箱中的信封
 */
public class TypedEnvelope<T> <: Message & PayloadCarrier {
    public let payload: T

    public init(payload: T) {
        this.payload = payload
    }

    public func messageType(): String {
        "TypedEnvelope"
    }

    public func anyPayload(): Any {
        payload
    }
}

/** 在目标 Actor 线程中才计算出消息（messageAdapter / pipeToSelf 使用） */
class AdaptedEnvelope<T> <: Message {
    let produce: () -> T

    init(produce: () -> T) {
        this.produce = produce
    }

    public func messageType(): String {
        "AdaptedEnvelope"
    }
}

/**
 * 由经典引用支撑的类型化引用
 */
public class ClassicTypedRef<T> <: TypedActorRef<T> {
    public let classic: ActorRef

    public init(classic: ActorRef) {
        this.classic = classic
    }

    public func tell(message: T): Unit {
        classic.tell(TypedEnvelope<T>(message))
    }

    /** 带发送方发送（接收方可以用经典上下文 reply） */
    public func tell(message: T, sender: ActorRef): Unit {
        classic.tell(TypedEnvelope<T>(message), sender)
    }

    public func pathString(): String {
        classic.path.getPath()
    }

    public func equals(other: TypedActorRef<T>): Bool {
        pathString() == other.pathString()
    }

    public func toClassic(): Option<ActorRef> {
        Some(classic)
    }

    public func toString(): String {
        "TypedActorRef(${pathString()})"
    }
}

/**
 * 类型化 Actor 的部署参数：调度器与邮箱
 */
public struct TypedProps {
    public let dispatcher: String
    public let mailboxCapacity: Int64

    public init() {
        this("default", 0)
    }

    init(dispatcher: String, mailboxCapacity: Int64) {
        this.dispatcher = dispatcher
        this.mailboxCapacity = mailboxCapacity
    }

    /** "default"（共享线程池）或 "blocking"（独立线程池，用于会阻塞的 Actor） */
    public func withDispatcher(name: String): TypedProps {
        TypedProps(name, mailboxCapacity)
    }

    /** 有界邮箱：超过容量的消息被丢弃并计为死信 */
    public func withBoundedMailbox(capacity: Int64): TypedProps {
        TypedProps(dispatcher, capacity)
    }
}

/**
 * 把类型化行为转换为经典 Props（供系统与上下文创建 Actor）
 */
public struct TypedSpawn {
    public static func props<U>(behavior: TypedBehavior<U>, system: Option<TypedActorSystem>, typedProps: TypedProps): Props<Actor> {
        let rules = SupervisionRules()
        var p = PropsFactory.create<Actor>({ => TypedActorAdapter<U>(behavior, rules, system) })
        p = p.withSupervisorStrategy(TypedDeciderStrategy(rules))
        if (typedProps.dispatcher != "default") {
            p = p.withDispatcher(typedProps.dispatcher)
        }
        if (typedProps.mailboxCapacity > 0) {
            p = p.withBoundedMailbox(typedProps.mailboxCapacity)
        }
        p
    }

    public static func props<U>(behavior: TypedBehavior<U>): Props<Actor> {
        props<U>(behavior, None<TypedActorSystem>, TypedProps())
    }
}

/**
 * 运行类型化行为的经典 Actor
 */
class TypedActorAdapter<T> <: Actor {
    private let initial: TypedBehavior<T>
    private let rules: SupervisionRules
    private let typedSystem: Option<TypedActorSystem>
    private var behavior: TypedBehavior<T>
    private var ctxValue: Option<ClassicTypedContext<T>> = None<ClassicTypedContext<T>>
    private var started: Bool = false
    private var stopping: Bool = false
    private var stoppedCallback: Option<() -> Unit> = None<() -> Unit>

    init(initial: TypedBehavior<T>, rules: SupervisionRules, typedSystem: Option<TypedActorSystem>) {
        this.initial = initial
        this.rules = rules
        this.typedSystem = typedSystem
        this.behavior = initial
    }

    public prop name: String {
        get() { "TypedActor" }
    }

    public prop description: String {
        get() { behavior.getDescription() }
    }

    private func ctxFor(classic: ActorContext): ClassicTypedContext<T> {
        match (ctxValue) {
            case Some(c) => c
            case None =>
                let c = ClassicTypedContext<T>(classic, rules, typedSystem)
                ctxValue = Some(c)
                c
        }
    }

    public func onStarted(context: ActorContext): Unit {
        if (started) {
            return
        }
        started = true
        let c = ctxFor(context)
        let result = applyNext(initial, c, true)
        match (result) {
            case MessageResult.Stop => c.stopSelf()
            case _ => ()
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let c = ctxFor(context)
        if (!started) {
            // 没有经过 onStarted 的运行时（例如测试桩）：在第一条消息前启动
            onStarted(context)
        }
        if (stopping) {
            return MessageResult.Unhandled
        }
        match (message) {
            case e: TypedEnvelope<T> => deliver(e.payload, c)
            case a: AdaptedEnvelope<T> => deliver(a.produce(), c)
            case t: Terminated =>
                match (c.watchMessages.remove(t.actor.path.getPath())) {
                    case Some(m) => deliver(m, c)
                    case None => signal(TypedTerminated(t.actor), c)
                }
            case _: ReceiveTimeout =>
                match (c.receiveTimeoutMessage) {
                    case Some(m) => deliver(m, c)
                    case None => MessageResult.Handled
                }
            case e: PayloadCarrier =>
                // 载荷类型与协议类型是子类型关系（例如用 TypedActorRef<Any> 发送）
                match (e.anyPayload() as T) {
                    case Some(m) => deliver(m, c)
                    case None => MessageResult.Unhandled
                }
            case _ =>
                // 经典消息：协议类型本身就是 Message 的子类型时直接处理
                match (message as T) {
                    case Some(m) => deliver(m, c)
                    case None => MessageResult.Unhandled
                }
        }
    }

    private func deliver(message: T, c: ClassicTypedContext<T>): MessageResult {
        applyNext(behavior.onMessage(message, c), c, false)
    }

    private func signal(sig: TypedSignal, c: ClassicTypedContext<T>): MessageResult {
        applyNext(behavior.onSignal(sig, c), c, false)
    }

    private func applyNext(next: TypedBehavior<T>, c: ClassicTypedContext<T>, initialStart: Bool): MessageResult {
        if (!initialStart) {
            if (TypedBehaviors.isUnhandled(next)) {
                return MessageResult.Unhandled
            }
            if (TypedBehaviors.isSame(next)) {
                return MessageResult.Handled
            }
        }
        let startedBehavior = TypedBehaviors.start(next, c)
        if (let Some(s) <- (startedBehavior as StoppedBehavior<T>)) {
            // 保留之前的行为，PostStop 信号交给它（除非 stopped 指定了回调）
            stopping = true
            stoppedCallback = s.postStop
            return MessageResult.Stop
        }
        if (TypedBehaviors.isSame(startedBehavior) || TypedBehaviors.isUnhandled(startedBehavior)) {
            // 初始行为不能是 same / unhandled
            if (initialStart) {
                throw IllegalStateException("初始行为不能是 same 或 unhandled")
            }
            return MessageResult.Handled
        }
        behavior = startedBehavior
        MessageResult.Handled
    }

    public func preRestart(reason: Exception): Unit {
        if (let Some(c) <- ctxValue) {
            try {
                behavior.onSignal(PreRestart(reason), c)
            } catch (e: Exception) {
                c.logger().warn("处理 PreRestart 信号失败: ${e}")
            }
        }
    }

    public func postStop(): Unit {
        if (let Some(c) <- ctxValue) {
            try {
                match (stoppedCallback) {
                    case Some(cb) => cb()
                    case None =>
                        behavior.onSignal(PostStop(), c)
                        ()
                }
            } catch (e: Exception) {
                c.logger().warn("处理 PostStop 信号失败: ${e}")
            }
        }
    }
}

/**
 * 用经典上下文实现的类型化上下文（每个 Actor 实例一个，只在 Actor 线程中使用）
 */
class ClassicTypedContext<T> <: TypedActorContext<T> {
    let classic: ActorContext
    let supervision: SupervisionRules
    let typedSystem: Option<TypedActorSystem>
    private let selfRef: ClassicTypedRef<T>
    private var timersValue: Option<TypedTimerScheduler<T>> = None<TypedTimerScheduler<T>>
    private var loggerValue: Option<ActorTypedLogger> = None<ActorTypedLogger>
    private var anonymousSeq: Int64 = 0
    /** watchWith 登记的消息：路径 -> 消息 */
    let watchMessages = HashMap<String, T>()
    var receiveTimeoutMessage: Option<T> = None<T>

    init(classic: ActorContext, supervision: SupervisionRules, typedSystem: Option<TypedActorSystem>) {
        this.classic = classic
        this.supervision = supervision
        this.typedSystem = typedSystem
        this.selfRef = ClassicTypedRef<T>(classic.self)
    }

    public prop self: TypedActorRef<T> {
        get() { selfRef }
    }

    public func sender(): Option<String> {
        classic.sender()
    }

    public func stopSelf(): Unit {
        classic.stop(classic.self)
    }

    func logger(): ActorTypedLogger {
        match (loggerValue) {
            case Some(l) => l
            case None =>
                let l = ActorTypedLogger(selfRef.pathString())
                loggerValue = Some(l)
                l
        }
    }

    public func log(): TypedLogger {
        logger()
    }

    // ---------- 子 Actor ----------

    public func actorOf<U>(behavior: TypedBehavior<U>, name: String): TypedActorRef<U> {
        actorOf<U>(behavior, name, TypedProps())
    }

    public func actorOf<U>(behavior: TypedBehavior<U>, name: String, props: TypedProps): TypedActorRef<U> {
        let ref = classic.actorOf(TypedSpawn.props<U>(behavior, typedSystem, props), name)
        ClassicTypedRef<U>(ref)
    }

    public func actorOf<U>(behavior: TypedBehavior<U>): TypedActorRef<U> {
        anonymousSeq++
        actorOf<U>(behavior, "$t${anonymousSeq}", TypedProps())
    }

    public func stop<U>(child: TypedActorRef<U>): Unit {
        classic.stop(classicOf(child, "stop"))
    }

    public func childAs<U>(name: String): Option<TypedActorRef<U>> {
        match (classic.child(name)) {
            case Some(r) => Some<TypedActorRef<U>>(ClassicTypedRef<U>(r))
            case None => None<TypedActorRef<U>>
        }
    }

    public func child(name: String): Option<TypedActorRef<Any>> {
        childAs<Any>(name)
    }

    public func children(): Array<TypedActorRef<Any>> {
        let cs = classic.children()
        Array<TypedActorRef<Any>>(cs.size, { i => ClassicTypedRef<Any>(cs[i]) })
    }

    // ---------- 死亡监视 ----------

    public func watch<U>(other: TypedActorRef<U>): Unit {
        classic.watch(classicOf(other, "watch"))
    }

    public func watchWith<U>(other: TypedActorRef<U>, message: T): Unit {
        let ref = classicOf(other, "watchWith")
        watchMessages.add(ref.path.getPath(), message)
        classic.watch(ref)
    }

    public func unwatch<U>(other: TypedActorRef<U>): Unit {
        let ref = classicOf(other, "unwatch")
        watchMessages.remove(ref.path.getPath())
        classic.unwatch(ref)
    }

    private func classicOf<U>(ref: TypedActorRef<U>, op: String): ActorRef {
        match (ref.toClassic()) {
            case Some(r) => r
            case None => throw IllegalArgumentException("${op}: ${ref.pathString()} 不是由 Actor 支撑的引用")
        }
    }

    // ---------- 请求-响应 ----------

    public func ask<Req, Res>(target: TypedActorRef<Req>, createRequest: (TypedActorRef<Res>) -> Req, timeout: Duration,
        mapResponse: (AskOutcome<Res>) -> T): Unit {
        pipeToSelf<Res>(TypedAsk.ask<Req, Res>(target, createRequest, timeout), mapResponse)
    }

    public func pipeToSelf<V>(future: TypedFuture<V>, map: (AskOutcome<V>) -> T): Unit {
        let me = classic.self
        future.onComplete({ outcome => me.tell(AdaptedEnvelope<T>({ => map(outcome) })) })
    }

    public func messageAdapter<U>(f: (U) -> T): TypedActorRef<U> {
        MessageAdapterTypedRef<U, T>(classic.self, f)
    }

    // ---------- 定时 ----------

    public prop timers: TypedTimerScheduler<T> {
        get() {
            match (timersValue) {
                case Some(t) => t
                case None =>
                    let t = TypedTimerScheduler<T>(classic.timers)
                    timersValue = Some(t)
                    t
            }
        }
    }

    public func setReceiveTimeout(timeout: Duration, message: T): Unit {
        receiveTimeoutMessage = Some(message)
        classic.setReceiveTimeout(timeout)
    }

    public func cancelReceiveTimeout(): Unit {
        receiveTimeoutMessage = None<T>
        classic.cancelReceiveTimeout()
    }

    public func scheduleOnce<U>(delay: Duration, target: TypedActorRef<U>, message: U): TypedCancellable {
        TimerCancellable(Timer.once(delay, { => target.tell(message) }))
    }

    // ---------- 系统 ----------

    public func system(): TypedActorSystem {
        match (typedSystem) {
            case Some(s) => s
            case None => throw UnsupportedContextException("system（该 Actor 不是由 TypedActorSystem 创建的）")
        }
    }

    public func scheduler(): TypedScheduler {
        TimerTypedScheduler()
    }

    public func toClassic(): Option<ActorContext> {
        Some(classic)
    }

    public func askMessage(target: TypedActorRef<Any>, message: Any, timeout: Duration): TypedFuture<Any> {
        let promise = TypedPromise<Any>()
        let replyTo = PromiseActorRef(promise, TypedAsk.nextPath())
        let timer = Timer.once(timeout, { =>
            promise.tryFailure(TypedAskTimeoutException("askMessage ${target.pathString()} 在 ${timeout} 内没有回复"))
            ()
        })
        promise.onComplete({ _ => timer.cancel() })
        match (target.toClassic()) {
            case Some(r) => r.tell(TypedEnvelope<Any>(message), replyTo)
            case None => target.tell(message)
        }
        promise
    }
}

/** messageAdapter 返回的引用：f 在拥有者 Actor 线程中执行 */
class MessageAdapterTypedRef<U, T> <: TypedActorRef<U> {
    private let owner: ActorRef
    private let f: (U) -> T

    init(owner: ActorRef, f: (U) -> T) {
        this.owner = owner
        this.f = f
    }

    public func tell(message: U): Unit {
        let fn = f
        owner.tell(AdaptedEnvelope<T>({ => fn(message) }))
    }

    public func pathString(): String {
        owner.path.getPath() + "/$adapter"
    }

    public func equals(other: TypedActorRef<U>): Bool {
        pathString() == other.pathString()
    }

    /** 适配器随拥有者一起停止，监视它等价于监视拥有者 */
    public func toClassic(): Option<ActorRef> {
        Some(owner)
    }
}

/** 基于 std Timer 的可取消任务 */
class TimerCancellable <: TypedCancellable {
    private let timer: Timer
    private let cancelled = AtomicBool(false)

    init(timer: Timer) {
        this.timer = timer
    }

    public func cancel(): Bool {
        if (cancelled.compareAndSwap(false, true)) {
            timer.cancel()
            true
        } else {
            false
        }
    }

    public func isCancelled(): Bool {
        cancelled.load()
    }
}

/** 基于 std Timer 的调度器 */
class TimerTypedScheduler <: TypedScheduler {
    public func scheduleOnce(delay: Duration, target: TypedActorRef<Any>, message: Any): Unit {
        Timer.once(delay, { => target.tell(message) })
        ()
    }

    public func schedule(initialDelay: Duration, interval: Duration, target: TypedActorRef<Any>, message: Any): TypedCancellable {
        TimerCancellable(Timer.repeat(initialDelay, interval, { => target.tell(message) }, style: CatchupStyle.Delay))
    }
}

/** 写入 CActor 日志系统的 Actor 日志（来源为 Actor 路径） */
class ActorTypedLogger <: TypedLogger {
    private let inner: Logger

    init(source: String) {
        this.inner = Logging.logger(source)
    }

    public func debug(message: String): Unit {
        inner.debug(message)
    }

    public func info(message: String): Unit {
        inner.info(message)
    }

    public func warn(message: String): Unit {
        inner.warn(message)
    }

    public func error(message: String): Unit {
        inner.error(message)
    }
}
