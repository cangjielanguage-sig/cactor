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
import cactor_foundation.logging.{Logger, Logging, LogRateLimiter}

// 类型化生命周期信号（PreRestart / PostStop）处理异常的日志（A10：此前静默吞掉）
let typedLifecycleLog: Logger = Logging.logger("cactor_core.typed.lifecycle")

// 类型化 Actor 收到协议之外的消息：这是编程错误（通常来自 childAs / findActor / lookup 的未检查转换，
// 或经典 ActorRef 直接 tell 给类型化 Actor），按 WARN 限速记录（每 5 分钟前 10 条），点明原因。
// 消息本身随后由 Cell 以 UnhandledMessage 交给死信订阅者（T11：此前只加一个计数，静默消失）
let protocolMismatchLog: LogRateLimiter = LogRateLimiter(10, Duration.minute * 5)

/** 可以取出任意类型载荷的消息（TypedPayloadBox / TypedEnvelope 等载体；用于跨协议投递与测试工具） */
public interface PayloadCarrier {
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

/**
 * 类型化消息的非泛型载体：热路径（每次 tell）上分配它而不是 TypedEnvelope<T>。
 *
 * core 的泛型代码以用户包的类型 T 实例化时，分配 C<T> 或做 `is C<T>` 都要在运行时查找类型信息，
 * 且多线程下互相阻塞（实测分配 60ns→300ns、匹配 ~30ns→0.4µs）；非泛型类没有这个成本（≈10ns）。
 * 载荷以 Any 携带，取出时的 `as T` 由 TypedSpawn.props 在调用方包里生成的桥接闭包完成
 * （用户包里编译的具体类型转换只需 4–10ns，见 P4-1）。
 */
public class TypedPayloadBox <: Message & PayloadCarrier {
    public let payload: Any
    /**
     * 类型期望（可选）：发布方声明的协议类型名。
     *
     * None = 没有声明：接收端沿用引入本机制之前的行为（`as T` 兜底，不匹配才算未处理）。
     * 声明了则接收端按名字比对，不符即拒收（计入未处理 + 限速告警）。
     */
    public let typeTag: Option<TypedTypeTag>

    public init(payload: Any, typeTag!: Option<TypedTypeTag> = None<TypedTypeTag>) {
        this.payload = payload
        this.typeTag = typeTag
    }

    public func messageType(): String {
        "TypedPayloadBox"
    }

    public func anyPayload(): Any {
        payload
    }
}

/**
 * 在目标 Actor 线程中才计算出消息（messageAdapter / pipeToSelf 使用）。
 * 载荷以 Any 携带（非泛型）：接收侧若写成 `case a: AdaptedEnvelope<T>`，
 * 每次都会触发运行时的泛型类型信息查找（profile 实测在多线程下互相阻塞，是 [7] 反缩放的主因）；
 * 取出后的 `as T` 交由用户包里编译的桥接闭包（unwrap）完成。
 */
class AdaptedEnvelope <: Message & PayloadCarrier {
    let produce: () -> Any

    init(produce: () -> Any) {
        this.produce = produce
    }

    public func messageType(): String {
        "AdaptedEnvelope"
    }

    public func anyPayload(): Any {
        produce()
    }
}

/**
 * 投递给自己的信号（pipeSignalToSelf 使用）：信号在目标 Actor 线程中才计算出来，交给行为的 onSignal，
 * 与普通消息同一邮箱、按到达顺序处理。不实现 PayloadCarrier——它不是 T 类型的消息，不能被当作消息解包。
 */
class SignalEnvelope <: Message {
    let produce: () -> TypedSignal

    init(produce: () -> TypedSignal) {
        this.produce = produce
    }

    public func messageType(): String {
        "SignalEnvelope"
    }
}

/**
 * 携带类型期望的类型化引用（非泛型接口）。
 *
 * 集群 Receptionist 等非泛型边界用它读取引用上声明的协议类型名（"注册时把 T 的类型名记下来"）。
 * 接口刻意不泛型：泛型接口的转换会在运行时查类型信息。
 */
public interface TypeExpectationCarrier {
    /** 本引用发出的消息声明的协议类型名（None = 未声明） */
    func typeExpectation(): Option<TypedTypeTag>
}

/**
 * 由经典引用支撑的类型化引用
 */
public class ClassicTypedRef<T> <: TypedActorRef<T> & TypeExpectationCarrier {
    public let classic: ActorRef
    /**
     * 类型期望（可选）：本引用发出的消息声明的协议类型名。
     *
     * 声明了就随消息带上（接收端据此校验），没声明就沿用旧行为。发送侧不做任何运行期类型查找，
     * 因此 tell 的开销与引入本机制之前相同。
     */
    public let typeTag: Option<TypedTypeTag>

    public init(classic: ActorRef, typeTag!: Option<TypedTypeTag> = None<TypedTypeTag>) {
        this.classic = classic
        this.typeTag = typeTag
    }

    public func tell(message: T): Unit {
        classic.tell(TypedPayloadBox(message, typeTag: typeTag))
    }

    /** 带发送方发送（接收方可以用经典上下文 reply） */
    public func tell(message: T, sender: ActorRef): Unit {
        classic.tell(TypedPayloadBox(message, typeTag: typeTag), sender)
    }

    public func typeExpectation(): Option<TypedTypeTag> {
        typeTag
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
    public let overflow: MailboxOverflow
    /**
     * 本 Actor 的协议类型名（可选）：声明后，收到的消息若带的类型名与之不符，一律拒收
     * （不投递、计入未处理、限速告警）。不声明 = 不做类型期望校验（旧行为）。
     */
    public let protocolTypeName: Option<String>

    public init() {
        this("default", 0, MailboxOverflow.DropNew, None<String>)
    }

    init(dispatcher: String, mailboxCapacity: Int64, overflow: MailboxOverflow) {
        this(dispatcher, mailboxCapacity, overflow, None<String>)
    }

    init(dispatcher: String, mailboxCapacity: Int64, overflow: MailboxOverflow, protocolTypeName: Option<String>) {
        this.dispatcher = dispatcher
        this.mailboxCapacity = mailboxCapacity
        this.overflow = overflow
        this.protocolTypeName = protocolTypeName
    }

    /** 声明本 Actor 的协议类型名：收到的消息类型名与之不符时拒收（类型期望校验） */
    public func withProtocolTypeName(name: String): TypedProps {
        if (name.isEmpty()) {
            throw IllegalArgumentException("协议类型名不能为空")
        }
        TypedProps(dispatcher, mailboxCapacity, overflow, Some(name))
    }

    /** "default"（共享线程池）或 "blocking"（独立线程池，用于会阻塞的 Actor） */
    public func withDispatcher(name: String): TypedProps {
        TypedProps(name, mailboxCapacity, overflow, protocolTypeName)
    }

    /** 有界邮箱：超过容量的消息被丢弃并计为死信 */
    public func withBoundedMailbox(capacity: Int64): TypedProps {
        TypedProps(dispatcher, capacity, MailboxOverflow.DropNew, protocolTypeName)
    }

    /** 有界邮箱，并指定邮箱满时的处理方式 */
    public func withBoundedMailbox(capacity: Int64, overflow: MailboxOverflow): TypedProps {
        TypedProps(dispatcher, capacity, overflow, protocolTypeName)
    }
}

/**
 * "未匹配"哨兵：非泛型边界用它表示"载荷不是协议类型 T"。
 * 不用 Option<T>，因为返回值类型一旦涉及泛型参数，core 侧调用就会触发运行时类型查找。
 */
public class TypedNotMatched {}

/** 全局单例哨兵 */
public let NOT_MATCHED: TypedNotMatched = TypedNotMatched()

/** 类型化上下文在非泛型边界上的载体（内部包一个 ClassicTypedContext<T>） */
public class TypedCtxBox {
    public let value: Any
    public init(value: Any) { this.value = value }
}

/**
 * 非泛型桥接：把适配器与"用户包类型 T"之间所有需要泛型的操作，都收进这几个
 * `Any → Any` 闭包里。闭包由 @Frozen 入口在**用户包**里构造，因此其中对 T 的
 * `as`/分配/接口调用都在用户包内实例化。
 *
 * 为什么必须是 `Any → Any`：实测（600 万次/4 线程）core 泛型代码里调用"涉及泛型参数"
 * 的闭包（即使闭包本身在用户包编译）每次都要运行时查类型信息——`(Any) -> Option<T>` 边界
 * 约 700ms，而全 `Any → Any` 边界约 120ms（快 6 倍）。所以连 Option<T> 都不能出现在签名里。
 */
public class TypedHooks {
    /** 载荷 → T（装箱为 Any）；不匹配返回 NOT_MATCHED */
    let unpack: (Any) -> Any
    /** (行为, 消息, 上下文) → 下一个行为（onMessage） */
    let onMessage: (Any, Any, Any) -> Any
    /** (行为, 信号, 上下文) → 下一个行为（onSignal） */
    let onSignal: (Any, Any, Any) -> Any
    /** (行为, 上下文) → 展开后的行为（TypedBehaviors.start） */
    let start: (Any, Any) -> Any
    /** 行为 → stopped 时的 postStop 回调（非 stopped 返回 None） */
    let stoppedCallback: (Any) -> Option<() -> Unit>
    /** 行为的描述文本 */
    let describe: (Any) -> String

    public init(unpack: (Any) -> Any, onMessage: (Any, Any, Any) -> Any, onSignal: (Any, Any, Any) -> Any,
        start: (Any, Any) -> Any, stoppedCallback: (Any) -> Option<() -> Unit>, describe: (Any) -> String) {
        this.unpack = unpack
        this.onMessage = onMessage
        this.onSignal = onSignal
        this.start = start
        this.stoppedCallback = stoppedCallback
        this.describe = describe
    }
}

/**
 * 冷路径桥接（每 Actor 一次，不进热路径）：建类型化上下文、取超时消息、停止自身。
 * 需要 internal 的 SupervisionRules，因此留在 core；泛型在 core 实例化，但只在建上下文/信号时发生。
 */
internal abstract class TypedColdHooks {
    public func makeCtx(classic: ActorContext, system: Option<TypedActorSystem>): Any
    public func timeoutMessage(c: Any): Any
    public func stopSelf(c: Any): Unit
}

/**
 * 冷路径的泛型实现（每 Actor 实例化一次，不进热路径）。
 * 适配器只持有抽象的 TypedColdHooks，因此适配器类本身完全非泛型——
 * 泛型类的方法序言会触发运行时类型查找（实测 receive 入口每消息一次）。
 */
internal class TypedColdHooksImpl<T> <: TypedColdHooks {
    let rules: SupervisionRules

    init(rules: SupervisionRules) {
        this.rules = rules
    }

    public func makeCtx(classic: ActorContext, system: Option<TypedActorSystem>): Any {
        ClassicTypedContext<T>(classic, rules, system)
    }

    public func timeoutMessage(c: Any): Any {
        let cc = (c as ClassicTypedContext<T>).getOrThrow()
        match (cc.receiveTimeoutMessage) {
            case Some(m) => m
            case None => NOT_MATCHED
        }
    }

    public func stopSelf(c: Any): Unit {
        (c as ClassicTypedContext<T>).getOrThrow().stopSelf()
    }
}

/**
 * 把类型化行为转换为经典 Props（供系统与上下文创建 Actor）
 */
public struct TypedSpawn {
    /**
     * hooks 由 @Frozen 入口（typedActorOf / ctx.actorOf）在用户包里构造（见 TypedHooks 说明）。
     * 未提供时退回 core 内构造的默认 hooks（正确，但每次消息会触发运行时类型查找）。
     */
    public static func props<U>(behavior: TypedBehavior<U>, system: Option<TypedActorSystem>, typedProps: TypedProps,
        hooks: Option<TypedHooks>): Props<Actor> {
        let rules = SupervisionRules()
        let h = match (hooks) {
            case Some(x) => x
            case None => defaultHooks<U>()
        }
        // 冷路径（每 Actor 一次）：上下文工厂留在 core，因为它需要 internal 的 SupervisionRules。
        // 热路径（每消息）全部经 h 的用户包闭包，不经 core 泛型代码。
        let cold = TypedColdHooksImpl<U>(rules)
        let protocolTag = typedTypeTagOf(typedProps.protocolTypeName)
        var p = PropsFactory.create<Actor>({ => TypedActorAdapter(h, cold, behavior, rules, system, protocolTag) })
        p = p.withSupervisorStrategy(TypedDeciderStrategy(rules))
        if (typedProps.dispatcher != "default") {
            p = p.withDispatcher(typedProps.dispatcher)
        }
        if (typedProps.mailboxCapacity > 0) {
            p = p.withBoundedMailbox(typedProps.mailboxCapacity, typedProps.overflow)
        }
        p
    }

    public static func props<U>(behavior: TypedBehavior<U>, system: Option<TypedActorSystem>, typedProps: TypedProps): Props<Actor> {
        props<U>(behavior, system, typedProps, None<TypedHooks>)
    }

    /**
     * 在**调用方包**里构造热路径 hooks（T16）：@Frozen 让函数体随调用方展开，闭包里对 U 的 `as` / 接口调用
     * 都在调用方包实例化，每条消息不再做运行时类型查找。只有**静态调用**链才有效（用户代码、setup 等 @Frozen 工厂）；
     * 经接口虚调用（如 ctx.actorOf 的实现）或在 core 的非 @Frozen 泛型代码里调用，等同于 defaultHooks
     */
    @Frozen
    public static func hooksFor<U>(): TypedHooks {
        let unpack: (Any) -> Any = { a => match (a as U) { case Some(m) => m; case None => NOT_MATCHED } }
        let onMsg: (Any, Any, Any) -> Any = { b, m, cx =>
            (b as TypedBehavior<U>).getOrThrow().onMessage((m as U).getOrThrow(), (cx as TypedActorContext<U>).getOrThrow())
        }
        let onSig: (Any, Any, Any) -> Any = { b, sg, cx =>
            (b as TypedBehavior<U>).getOrThrow().onSignal((sg as TypedSignal).getOrThrow(), (cx as TypedActorContext<U>).getOrThrow())
        }
        let st: (Any, Any) -> Any = { b, cx =>
            TypedBehaviors.start<U>((b as TypedBehavior<U>).getOrThrow(), (cx as TypedActorContext<U>).getOrThrow())
        }
        let stoppedCb: (Any) -> Option<() -> Unit> = { b =>
            TypedBehaviors.stoppedCallbackOf<U>((b as TypedBehavior<U>).getOrThrow())
        }
        let desc: (Any) -> String = { b => (b as TypedBehavior<U>).getOrThrow().getDescription() }
        TypedHooks(unpack, onMsg, onSig, st, stoppedCb, desc)
    }

    public static func props<U>(behavior: TypedBehavior<U>): Props<Actor> {
        props<U>(behavior, None<TypedActorSystem>, TypedProps())
    }

    /**
     * core 内构造的默认 hooks（泛型在 core 编译，慢路径）。
     * 只用于未经 @Frozen 入口创建的 Actor（例如 core 内部或旧调用方）；用户正常创建走用户包 hooks。
     */
    static func defaultHooks<U>(): TypedHooks {
        let unpack: (Any) -> Any = { a => match (a as U) { case Some(m) => m; case None => NOT_MATCHED } }
        let onMsg: (Any, Any, Any) -> Any = { b, m, c =>
            (b as TypedBehavior<U>).getOrThrow().onMessage((m as U).getOrThrow(), (c as TypedActorContext<U>).getOrThrow())
        }
        let onSig: (Any, Any, Any) -> Any = { b, s, c =>
            (b as TypedBehavior<U>).getOrThrow().onSignal((s as TypedSignal).getOrThrow(), (c as TypedActorContext<U>).getOrThrow())
        }
        let st: (Any, Any) -> Any = { b, c => TypedBehaviors.start<U>((b as TypedBehavior<U>).getOrThrow(), (c as TypedActorContext<U>).getOrThrow()) }
        let stopped: (Any) -> Option<() -> Unit> = { b => match (b as StoppedBehavior<U>) { case Some(sb) => sb.postStop; case None => None<() -> Unit> } }
        let desc: (Any) -> String = { b => (b as TypedBehavior<U>).getOrThrow().getDescription() }
        TypedHooks(unpack, onMsg, onSig, st, stopped, desc)
    }
}

/**
 * 运行类型化行为的经典 Actor（**非泛型**）。
 *
 * 行为对象、消息、上下文一律以 `Any` 持有/传递；所有需要 T 的操作都在 TypedHooks 的
 * 用户包闭包里完成。这样本类里没有任何以用户类型实例化的泛型方法调用，运行时不查类型信息。
 * （微基准：泛型适配器约 700ms / 600 万次，非泛型约 120ms。）
 */
class TypedActorAdapter <: Actor {
    private let hooks: TypedHooks
    private let cold: TypedColdHooks
    private let initialAny: Any
    private let rules: SupervisionRules
    private let typedSystem: Option<TypedActorSystem>
    private let protocolTag: Option<TypedTypeTag>
    private var behaviorAny: Any
    private var ctxValue: Option<Any> = None<Any>
    private var started: Bool = false
    private var stopping: Bool = false
    private var stoppedCallback: Option<() -> Unit> = None<() -> Unit>

    init(hooks: TypedHooks, cold: TypedColdHooks, initial: Any, rules: SupervisionRules, typedSystem: Option<TypedActorSystem>, protocolTag: Option<TypedTypeTag>) {
        this.hooks = hooks
        this.cold = cold
        this.initialAny = initial
        this.rules = rules
        this.typedSystem = typedSystem
        this.protocolTag = protocolTag
        this.behaviorAny = initial
    }

    public prop name: String {
        get() { "TypedActor" }
    }

    public prop description: String {
        get() { hooks.describe(behaviorAny) }
    }

    private func ctxFor(classic: ActorContext): Any {
        match (ctxValue) {
            case Some(c) => c
            case None =>
                let c = cold.makeCtx(classic, typedSystem)
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
        let result = try {
            applyNext(initialAny, c, true)
        } catch (e: Exception) {
            // 异常交给经典监督之前，在本 Actor 线程上快照当前生效的监督层（T9）
            rules.captureFailure(behaviorAny)
            throw e
        }
        match (result) {
            case MessageResult.Stop => cold.stopSelf(c)
            case _ => ()
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        // 启动失败已在 onStarted 里快照（含正在启动的层），不能在这里再覆盖一次
        if (!started) {
            onStarted(context)
        }
        try {
            receiveInner(message, context)
        } catch (e: Exception) {
            rules.captureFailure(behaviorAny)
            throw e
        }
    }

    private func receiveInner(message: Message, context: ActorContext): MessageResult {
        let c = ctxFor(context)
        if (stopping) {
            return MessageResult.Unhandled
        }
        match (message) {
            case p: TypedPayloadBox =>
                if (!acceptsProtocol(p)) {
                    return MessageResult.Unhandled
                }
                deliverAny(p.payload, c)
            // watchWith 的自定义消息由 Cell 换好、以 TypedPayloadBox 送达；这里只剩未登记消息的 Terminated 信号
            case t: Terminated =>
                let r = applyNext(hooks.onSignal(behaviorAny, TypedTerminated(t.actor), c), c, false)
                // 未处理的 Terminated 由 Cell 抛 DeathPactException：同样先快照监督层，规则可以处理它
                if (let MessageResult.Unhandled <- r) {
                    rules.captureFailure(behaviorAny)
                }
                r
            case s: SignalEnvelope =>
                // 行为不处理自定义信号时忽略（不计为未处理消息：它不是 T 类型的消息，也不进死信）
                match (applyNext(hooks.onSignal(behaviorAny, s.produce(), c), c, false)) {
                    case MessageResult.Unhandled => MessageResult.Handled
                    case r => r
                }
            case _: ReceiveTimeout =>
                let m = cold.timeoutMessage(c)
                if (m is TypedNotMatched) {
                    MessageResult.Handled
                } else {
                    applyNext(hooks.onMessage(behaviorAny, m, c), c, false)
                }
            case e: PayloadCarrier =>
                deliverAny(e.anyPayload(), c)
            case _ =>
                deliverAny(message, c)
        }
    }

    private func acceptsProtocol(box: TypedPayloadBox): Bool {
        let expected = match (protocolTag) {
            case Some(tag) => tag.typeName
            case None => return true
        }
        let actual = match (box.typeTag) {
            case Some(tag) => Some(tag.typeName)
            case None => match (box.payload as Message) {
                case Some(m) => Some(m.messageType())
                case None => TypedPayloadCodecs.typeNameOfValue(box.payload)
            }
        }
        match (actual) {
            case Some(name) =>
                if (name != expected) {
                    let detail = "类型化协议不匹配：期望 ${expected}，收到 ${name}"
                    TypedTypeExpectations.recordMismatch(detail)
                    protocolMismatchLog.tryLog({ s => typedLifecycleLog.warn(s) }, { => detail })
                    return false
                }
                TypedTypeExpectations.recordChecked()
            case None => TypedTypeExpectations.recordUnverified()
        }
        true
    }

    private func deliverAny(payload: Any, c: Any): MessageResult {
        let m = hooks.unpack(payload)
        if (m is TypedNotMatched) {
            protocolMismatchLog.tryLog({ s => typedLifecycleLog.warn(s) }, { => protocolMismatchText(payload) })
            return MessageResult.Unhandled
        }
        applyNext(hooks.onMessage(behaviorAny, m, c), c, false)
    }

    private func applyNext(next: Any, c: Any, initialStart: Bool): MessageResult {
        let nextKind = specialKindOf(next)
        if (!initialStart) {
            if (nextKind == SpecialBehavior.UNHANDLED) {
                return MessageResult.Unhandled
            }
            if (nextKind == SpecialBehavior.SAME) {
                return MessageResult.Handled
            }
        }
        if (nextKind == 0) {
            behaviorAny = next
            return MessageResult.Handled
        }
        let startedBehavior = hooks.start(next, c)
        let kind = specialKindOf(startedBehavior)
        if (kind == SpecialBehavior.STOPPED) {
            stopping = true
            stoppedCallback = hooks.stoppedCallback(startedBehavior)
            return MessageResult.Stop
        }
        if (kind == SpecialBehavior.SAME || kind == SpecialBehavior.UNHANDLED) {
            if (initialStart) {
                throw IllegalStateException("初始行为不能是 same 或 unhandled")
            }
            return MessageResult.Handled
        }
        behaviorAny = startedBehavior
        MessageResult.Handled
    }

    public func preRestart(reason: Exception): Unit {
        if (let Some(c) <- ctxValue) {
            try {
                hooks.onSignal(behaviorAny, PreRestart(reason), c) |> ignore
            } catch (e: Exception) {
                // 不影响重启，但不能静默吞掉（A10）
                typedLifecycleLog.warn("类型化 Actor 的 PreRestart 信号处理抛出异常（已忽略，继续重启）: ${e}")
            }
        }
    }

    public func postStop(): Unit {
        if (let Some(c) <- ctxValue) {
            try {
                match (stoppedCallback) {
                    case Some(cb) => cb()
                    case None => hooks.onSignal(behaviorAny, PostStop(), c) |> ignore
                }
            } catch (e: Exception) {
                typedLifecycleLog.warn("类型化 Actor 的 PostStop 信号处理抛出异常（已忽略，继续停止）: ${e}")
            }
        }
    }
}

func ignore(_: Any): Unit {}

/**
 * 用经典上下文实现的类型化上下文（每个 Actor 实例一个，只在 Actor 线程中使用）
 */
class ClassicTypedContext<T> <: TypedActorContext<T> & SupervisionHost {
    let classic: ActorContext
    let supervision: SupervisionRules
    let typedSystem: Option<TypedActorSystem>
    private let selfRef: ClassicTypedRef<T>
    private var timersValue: Option<TypedTimerScheduler<T>> = None<TypedTimerScheduler<T>>
    private var loggerValue: Option<ActorTypedLogger> = None<ActorTypedLogger>
    private var anonymousSeq: Int64 = 0
    var receiveTimeoutMessage: Option<T> = None<T>
    /** same() 的缓存实例（P2-20）：每 Actor 一个，只在 Actor 线程内使用，消除每消息分配 */
    private var sameBehaviorValue: Option<SameBehavior<T>> = None<SameBehavior<T>>

    public func pushStarting(rules: Array<SupervisionRule>): Unit {
        supervision.pushStarting(rules)
    }

    public func popStarting(): Unit {
        supervision.popStarting()
    }

    init(classic: ActorContext, supervision: SupervisionRules, typedSystem: Option<TypedActorSystem>) {
        this.classic = classic
        this.supervision = supervision
        this.typedSystem = typedSystem
        this.selfRef = ClassicTypedRef<T>(classic.self)
    }

    public prop self: TypedActorRef<T> {
        get() { selfRef }
    }

    public func same(): TypedBehavior<T> {
        match (sameBehaviorValue) {
            case Some(b) => b
            case None =>
                let b = SameBehavior<T>()
                sameBehaviorValue = Some(b)
                b
        }
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

    // 三个 actorOf 用接口默认实现，这里只实现创建本身。热路径 hooks 取初始行为携带的那份（T16）：
    // setup / supervise(setup) 在调用方包里构造好，子 Actor 与顶层 typedActorOf 同样走快路径；
    // 没有携带时退回 core 内的默认 hooks。此前这里构造了 unwrap 却没传下去，子 Actor 全部走慢路径
    public func actorOfWithHooks<U>(behavior: TypedBehavior<U>, name: Option<String>, props: TypedProps): TypedActorRef<U> {
        let childName = match (name) {
            case Some(n) => n
            case None =>
                anonymousSeq++
                "$t${anonymousSeq}"
        }
        let ref = classic.actorOf(TypedSpawn.props<U>(behavior, typedSystem, props, carriedHooksOf(behavior)), childName)
        ClassicTypedRef<U>(ref, typeTag: typedTypeTagOf(props.protocolTypeName))
    }

    public func stop<U>(child: TypedActorRef<U>): Unit {
        classic.stop(classicOf(child, "stop"))
    }

    /**
     * 按名字取子 Actor 并当作 TypedActorRef<U>（对应 Akka 的 child(name) + unsafeUpcast）：经典子引用不携带协议类型，
     * 这里无法在编译期或取引用时检查 U。用错时不会静默：对方收到协议之外的消息会限速 WARN，
     * 消息以 UnhandledMessage 交给死信订阅者（T11）
     */
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
        // 映射保存在 Cell 上（与 Akka 一致）：重启后的新实例收到的仍是 message，
        // 而不是没有登记的 Terminated 信号（此前保存在本上下文里，重启即丢失，进而触发 DeathPact，T3）
        classic.watchWith(classicOf(other, "watchWith"), TypedPayloadBox(message))
    }

    public func unwatch<U>(other: TypedActorRef<U>): Unit {
        classic.unwatch(classicOf(other, "unwatch"))
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
        // 超时走系统调度器（E52，与 Akka 相同）：注入虚拟时钟时，Actor 内 ask 的超时也只随虚拟时间触发
        pipeToSelf<Res>(TypedAsk.askWith<Req, Res>(target, createRequest, timeout, classic.scheduler(),
            None<AskCancellationToken>), mapResponse)
    }

    public func pipeToSelf<V>(future: TypedFuture<V>, map: (AskOutcome<V>) -> T): Unit {
        let me = classic.self
        future.onComplete({ outcome => me.tell(AdaptedEnvelope({ => map(outcome) })) })
    }

    public func pipeSignalToSelf<V>(future: TypedFuture<V>, map: (AskOutcome<V>) -> TypedSignal): Unit {
        let me = classic.self
        future.onComplete({ outcome => me.tell(SignalEnvelope({ => map(outcome) })) })
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
        TimerCancellable(classic.scheduler().scheduleOnce(delay, { => target.tell(message) }))
    }

    // ---------- 系统 ----------

    public func system(): TypedActorSystem {
        match (typedSystem) {
            case Some(s) => s
            case None => throw UnsupportedContextException("system（该 Actor 不是由 TypedActorSystem 创建的）")
        }
    }

    public func scheduler(): TypedScheduler {
        TimerTypedScheduler(classic.scheduler())
    }

    public func toClassic(): Option<ActorContext> {
        Some(classic)
    }

    public func askMessage(target: TypedActorRef<Any>, message: Any, timeout: Duration): TypedFuture<Any> {
        let promise = TypedPromise<Any>()
        let replyTo = PromiseActorRef(promise, TypedAsk.nextPath())
        let timer = classic.scheduler().scheduleOnce(timeout, { =>
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
        owner.tell(AdaptedEnvelope({ => fn(message) }))
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

/** 调度器任务句柄的可取消包装（E52 前直接包 std Timer） */
class TimerCancellable <: TypedCancellable {
    private let timer: ScheduledTask
    private let cancelled = AtomicBool(false)

    init(timer: ScheduledTask) {
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

/** 基于系统调度器（ActorScheduler）的类型化调度器 */
class TimerTypedScheduler <: TypedScheduler {
    private let underlying: ActorScheduler

    init(underlying: ActorScheduler) {
        this.underlying = underlying
    }

    public func scheduleOnce(delay: Duration, target: TypedActorRef<Any>, message: Any): Unit {
        underlying.scheduleOnce(delay, { => target.tell(message) })
        ()
    }

    public func schedule(initialDelay: Duration, interval: Duration, target: TypedActorRef<Any>, message: Any): TypedCancellable {
        TimerCancellable(underlying.scheduleWithFixedDelay(initialDelay, interval, { => target.tell(message) }))
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

/** 协议不符告警的正文（只在配额内拼接） */
func protocolMismatchText(payload: Any): String {
    let what = match (payload) {
        case m: Message => m.messageType()
        case t: ToString =>
            let runes = t.toString().toRuneArray()
            // 按字符截断：按字节切片可能切在多字节字符中间
            if (runes.size > 120) { "${String(runes[..120])}…" } else { String(runes) }
        case _ => "（无 toString 的载荷）"
    }
    "类型化 Actor 收到协议之外的消息 ${what}，按未处理消息交给死信订阅者。" +
        "常见原因：childAs / lookup 等未检查的类型转换用错了协议类型，或经典 ActorRef 直接 tell 给了类型化 Actor"
}
