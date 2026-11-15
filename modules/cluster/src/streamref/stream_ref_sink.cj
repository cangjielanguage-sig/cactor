/*
 * CActor Cluster.StreamRef - SinkRef（对标 Akka StreamRefs.sinkRef）
 *
 * 方向与 SourceRef 相反：本节点拥有 sink（消费端），远端生产方把元素推过来。
 *
 *   生产方（SinkRefProducer）                      Sink 拥有方（sinkRef）
 *     | -- Subscribe(流, 生产方位置, 版本) ------> |   单次挂接：第二个生产方收到 Rejected
 *     | <------------- OnSubscribe(流) ---------- |
 *     | <--------- Demand(流, 累计上限序号) ------ |   信用由 sink 按本地缓冲空位授予
 *     | -------- Element(流, 序号, 元素) --------> |   序号从 1 连续递增，且必须 ≤ 已授予需求
 *     | -------- Completed(流, 最后序号) --------> |   恰好一个终止信号
 *     | <--------------- Cancel(流, 原因) -------- |   本地取消/失败/属主停止
 *
 * 线上仍复用 StreamRefMsg 的 1..8 号帧（方向互换但不新增线上消息），因此新旧节点对帧格式
 * 的理解一致；只新增了本地控制消息 ConsumerPull，编码时直接抛异常，不影响旧帧的读取。
 *
 * 内存上界与 SourceRef 对称：
 * - sink 侧本地缓冲 ≤ bufferSize：授予的累计需求永远不超过"已交付给本地消费者 + bufferSize"；
 * - 生产方侧 offer 缓冲 ≤ bufferSize：先预留槽位，满了立即返回 Backpressured。
 * 因此慢消费者会把背压一路传回远端上游，而不是把两端内存吃光。
 *
 * 生命周期：完成/失败恰好一次；消费方 cancel 让生产方 termination() 变 Cancelled（可中止 LLM 调用）；
 * 生产方 fail/节点停止让本地 pull 以异常结束；owner（消费该流的本地 Actor）停止即结束整条流。
 */
package cactor_cluster.streamref

import std.collection.*
import std.sync.*
import cactor_core.actor.{ActorRef}
import cactor_core.message.{SerializationException, DeserializationException}
import cactor_core.typed.*
import cactor_foundation.reactive.{Subscriber, Subscription}
import cactor_remote.{RemoteActorRef}

private let SINK_REF_PREFIX: String = "sinkref:v1|"

// ================= SinkRef =================

/**
 * 指向 sink 阶段 Actor 的引用，可以放进请求消息交给远端生产方（对标 Akka SinkRef<T>）。
 *
 * - location：远端位置 "remote://host:port/name"；本节点没有远程层时为空（只能在本进程使用）。
 * - localRef：同进程时直接持有阶段引用；toWireString/fromWireString 之后为 None。
 * T 是幻象类型：线上只有位置与流标识，元素在生产端编码、在 sink 侧以 `as T` 还原，
 * 两端对 T 的理解不一致按协议违例失败，而不是悄悄丢弃。
 */
public class SinkRef<T> {
    public let streamId: String
    public let location: String
    public let localRef: Option<TypedActorRef<StreamRefMsg>>

    /** 一般由 StreamRefs.sinkRef 创建；直接构造用于自定义生产端（例如测试中的手写协议端点） */
    public init(streamId: String, location: String, localRef: Option<TypedActorRef<StreamRefMsg>>) {
        if (streamId.isEmpty() || streamId.contains("|")) {
            throw IllegalArgumentException("流标识不能为空且不能包含 '|'")
        }
        if (location.isEmpty() && localRef.isNone()) {
            throw IllegalArgumentException("SinkRef 至少需要远程位置或本地引用之一")
        }
        this.streamId = streamId
        this.location = location
        this.localRef = localRef
    }

    /** 能否跨进程传递（sink 所在节点有远程层） */
    public func isSerializable(): Bool {
        !location.isEmpty()
    }

    /** 过线文本，放进请求消息（StringMessage 或自定义编解码）即可；本地专用引用抛 SerializationException */
    public func toWireString(): String {
        if (location.isEmpty()) {
            throw SerializationException("SinkRef ${streamId} 只在本进程有效（sink 拥有方的 StreamRefs 没有远程层），不能跨节点传递")
        }
        "${SINK_REF_PREFIX}${streamId}|${location}"
    }

    /** 由 toWireString 的文本还原（不带本地引用） */
    public static func fromWireString(text: String): SinkRef<T> {
        if (!text.startsWith(SINK_REF_PREFIX)) {
            throw DeserializationException("不是 SinkRef 文本：${text}")
        }
        let rest = text[SINK_REF_PREFIX.size..]
        match (rest.indexOf("|")) {
            case Some(i) where i > 0 && i + 1 < rest.size =>
                SinkRef<T>(rest[0..i], rest[(i + 1)..], None<TypedActorRef<StreamRefMsg>>)
            case _ => throw DeserializationException("SinkRef 文本不完整：${text}")
        }
    }

    public func toString(): String {
        "SinkRef(${streamId}@${if (location.isEmpty()) { "local" } else { location }})"
    }
}

// ================= 本地消费句柄 =================

enum PullRegistration {
    | Accepted
    | Busy
    | Terminated(StreamTermination, Option<StreamRefException>)
}

enum DeliverResult {
    | Delivered
    | NoWaiter
    | TypeMismatch
}

/**
 * sink 阶段与本地消费句柄共享的状态。
 * pull 来自任意线程，交付发生在阶段 Actor 线程：锁只保护"挂起 promise"的所有权转移，
 * promise 的完成一律在锁外执行（回调里可能再次 pull）。
 */
class ConsumerShared<T> {
    let lock = Mutex()
    /**
     * 元素还原为 T 的闭包在 sinkRef<T>（泛型入口）里创建：阶段 Actor 的行为是闭包，
     * 在这里做 `value as T` 时运行时拿不到 T 的具体类型信息（会误判为类型不符）；
     * 入口处生成的闭包与 castElement<T> 一样能拿到调用方的具体类型。
     */
    let castF: (Any) -> Option<T>
    var pendingPull: Option<TypedPromise<Option<T>>> = None<TypedPromise<Option<T>>>
    var terminal: Option<StreamTermination> = None<StreamTermination>
    var terminalError: Option<StreamRefException> = None<StreamRefException>
    let terminationPromise = TypedPromise<StreamTermination>()
    let received = AtomicInt64(0)
    let delivered = AtomicInt64(0)
    let maxBufferedValue = AtomicInt64(0)
    let granted = AtomicInt64(0)
    let attached = AtomicBool(false)

    init(castF: (Any) -> Option<T>) {
        this.castF = castF
    }

    func registerPull(p: TypedPromise<Option<T>>): PullRegistration {
        synchronized(lock) {
            if (let Some(t) <- terminal) {
                PullRegistration.Terminated(t, terminalError)
            } else if (pendingPull.isSome()) {
                PullRegistration.Busy
            } else {
                pendingPull = Some(p)
                PullRegistration.Accepted
            }
        }
    }

    func hasPendingPull(): Bool {
        synchronized(lock) {
            pendingPull.isSome()
        }
    }

    /** 阶段线程：把元素交给等着的 pull；没人等则保留在阶段缓冲里 */
    func deliverAny(value: Any): DeliverResult {
        let p = synchronized(lock) {
            let x = pendingPull
            pendingPull = None<TypedPromise<Option<T>>>
            x
        }
        match (p) {
            case Some(pp) =>
                match (castF(value)) {
                    case Some(v) =>
                        pp.trySuccess(Some<T>(v))
                        DeliverResult.Delivered
                    case None =>
                        pp.tryFailure(StreamRefProtocolException("SinkRef 元素类型与声明的类型不符"))
                        DeliverResult.TypeMismatch
                }
            case None => DeliverResult.NoWaiter
        }
    }

    /** 恰好一次：记录终止并完成挂起的 pull（完成/取消 → None，失败 → 异常） */
    func finish(t: StreamTermination, err: Option<StreamRefException>): Bool {
        let (first, pending) = synchronized(lock) {
            if (terminal.isSome()) {
                return false
            }
            terminal = Some(t)
            terminalError = err
            let p = pendingPull
            pendingPull = None<TypedPromise<Option<T>>>
            (true, p)
        }
        if (first) {
            terminationPromise.trySuccess(t)
            if (let Some(pp) <- pending) {
                settlePull<T>(pp, t, err)
            }
        }
        first
    }
}

func settlePull<T>(p: TypedPromise<Option<T>>, t: StreamTermination, err: Option<StreamRefException>): Unit {
    match (t) {
        case StreamTermination.Failed(reason) =>
            match (err) {
                case Some(e) => p.tryFailure(e)
                case None => p.tryFailure(StreamRefRemoteFailureException(reason))
            }
        case _ => p.trySuccess(None<T>)
    }
}

/**
 * sink 侧的本地消费句柄（线程安全，任意线程可 pull）。
 *
 * pull 一次只取一个元素：本地缓冲空着时 Promise 挂起，元素到达后完成；流完成得到 None，
 * 失败得到对应异常。与 SinkQueue 一样，同一时刻只允许一个未完成的 pull——
 * 这既是背压的来源（不 pull = 缓冲填满 = 停止向远端授信），也让"已交付"计数没有歧义。
 *
 * ```
 * let (sinkRef, consumer) = refs.sinkRef<String>()
 * consumer.pull().onComplete { o => match (o) { ... } }
 * consumer.cancel()   // 远端生产方 termination() 变为 Cancelled
 * ```
 */
public class StreamConsumer<T> {
    let shared: ConsumerShared<T>
    private let lock = Mutex()
    private var stage: Option<TypedActorRef<StreamRefMsg>> = None<TypedActorRef<StreamRefMsg>>
    private let early = ArrayList<StreamRefMsg>()
    let cancelledFlag = AtomicBool(false)
    public let streamId: String

    init(streamId: String, shared: ConsumerShared<T>) {
        this.streamId = streamId
        this.shared = shared
    }

    /** 绑定阶段引用；绑定前的 pull/cancel 暂存，绑定时按序补发 */
    func bind(ref: TypedActorRef<StreamRefMsg>): Unit {
        let queued = synchronized(lock) {
            if (stage.isSome()) {
                return
            }
            stage = Some(ref)
            let q = early.toArray()
            early.clear()
            q
        }
        for (m in queued) {
            ref.tell(m)
        }
    }

    private func send(m: StreamRefMsg): Unit {
        let target = synchronized(lock) {
            match (stage) {
                case Some(s) => Some(s)
                case None =>
                    early.add(m)
                    None<TypedActorRef<StreamRefMsg>>
            }
        }
        if (let Some(s) <- target) {
            s.tell(m)
        }
    }

    /** 取下一个元素：Some 为元素，None 为正常完成/取消；失败时 Promise 以异常结束 */
    public func pull(): TypedPromise<Option<T>> {
        let p = TypedPromise<Option<T>>()
        match (shared.registerPull(p)) {
            case PullRegistration.Accepted => send(StreamRefMsg.ConsumerPull)
            case PullRegistration.Busy =>
                p.tryFailure(IllegalStateException("StreamConsumer 一次只允许一个未完成的 pull"))
            case PullRegistration.Terminated(t, err) => settlePull<T>(p, t, err)
        }
        p
    }

    /** 取消：远端生产方 termination() 变为 Cancelled；之后的 pull 得到 None */
    public func cancel(): Unit {
        cancel("消费方取消")
    }

    public func cancel(reason: String): Unit {
        if (cancelledFlag.compareAndSwap(false, true)) {
            send(StreamRefMsg.SubscriberCancel(reason))
        }
    }

    /** 流的结局，恰好完成一次：Completed / Cancelled / Failed */
    public func termination(): TypedPromise<StreamTermination> {
        shared.terminationPromise
    }

    public func isCancelled(): Bool {
        cancelledFlag.load()
    }

    /** 是否已有生产方挂接 */
    public func isAttached(): Bool {
        shared.attached.load()
    }

    /** 已从生产方收到的元素数（= 最后序号） */
    public func receivedCount(): Int64 {
        shared.received.load()
    }

    /** 已交付给本地 pull 的元素数 */
    public func deliveredCount(): Int64 {
        shared.delivered.load()
    }

    /** 已授予生产方的累计需求（允许发送的最大序号） */
    public func grantedDemand(): Int64 {
        shared.granted.load()
    }

    /** 观测到的最大"已收到未交付"数量：恒 ≤ bufferSize */
    public func maxBuffered(): Int64 {
        shared.maxBufferedValue.load()
    }

    /** 当前本地缓冲中的元素数 */
    public func bufferedCount(): Int64 {
        let n = shared.received.load() - shared.delivered.load()
        if (n < 0) { 0 } else { n }
    }
}

// ================= sink 阶段 =================

/** sink 阶段的可变状态：只在阶段 Actor 线程访问 */
class SinkStageState<T> {
    let streamId: String
    let exposedName: String
    let settings: StreamRefSettings
    let env: StreamRefEnv
    let shared: ConsumerShared<T>
    let consumer: StreamConsumer<T>
    let owner: Option<ActorRef>
    let buffer = ArrayDeque<Any>()
    var producer: Option<StreamPeer> = None<StreamPeer>
    var receivedSeq: Int64 = 0
    var grantedUpTo: Int64 = 0
    var deliveredCount: Int64 = 0
    var completedPending: Bool = false
    var terminal: Bool = false
    var cleaned: Bool = false

    init(streamId: String, exposedName: String, settings: StreamRefSettings, env: StreamRefEnv,
        shared: ConsumerShared<T>, consumer: StreamConsumer<T>, owner: Option<ActorRef>) {
        this.streamId = streamId
        this.exposedName = exposedName
        this.settings = settings
        this.env = env
        this.shared = shared
        this.consumer = consumer
        this.owner = owner
    }
}

func sinkStage<T>(st: SinkStageState<T>): TypedBehavior<StreamRefMsg> {
    TypedBehaviors.setup<StreamRefMsg>({ ctx =>
        st.consumer.bind(ctx.self)
        ctx.timers.startSingleTimer(KEY_SUBSCRIPTION_TIMEOUT, StreamRefMsg.SubscriptionTimeout,
            st.settings.subscriptionTimeout)
        if (let Some(o) <- st.owner) {
            // 属主（消费该流的本地 Actor）停止 = 整条流结束：不能让远端生产方继续生成
            if (let Some(c) <- ctx.toClassic()) {
                c.watch(o)
            }
        }
        TypedBehaviors.receive<StreamRefMsg>({ c, msg => sinkOnMessage<T>(st, c, msg) })
            .receiveSignal({ c, sig => sinkOnSignal<T>(st, c, sig) })
    })
}

func sinkOnMessage<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>,
    msg: StreamRefMsg): TypedBehavior<StreamRefMsg> {
    if (st.terminal) {
        return TypedBehaviors.same<StreamRefMsg>()
    }
    let sid = msg.streamIdOrEmpty()
    if (!sid.isEmpty() && sid != st.streamId) {
        streamRefLog.warn("SinkRef ${st.streamId} 收到其他流 ${sid} 的消息，忽略")
        return TypedBehaviors.same<StreamRefMsg>()
    }
    match (msg) {
        case StreamRefMsg.Subscribe(_, location, ver) =>
            match (st.env.resolveRemote(location)) {
                case Some(peer) => sinkOnProducerSubscribe<T>(st, ctx, peer, ver)
                case None =>
                    streamRefLog.warn("SinkRef ${st.streamId} 收到无法解析的生产方位置 ${location}，忽略")
                    TypedBehaviors.same<StreamRefMsg>()
            }
        case StreamRefMsg.SubscribeLocal(_, ref, ver) =>
            sinkOnProducerSubscribe<T>(st, ctx, StreamPeer(ref, None<RemoteActorRef>, ref.pathString()), ver)
        case StreamRefMsg.Element(_, seq, value) => sinkOnElement<T>(st, ctx, seq, value)
        case StreamRefMsg.Completed(_, lastSeq) => sinkOnCompleted<T>(st, ctx, lastSeq)
        case StreamRefMsg.Failed(_, reason) =>
            sinkTerminate<T>(st, ctx, StreamTermination.Failed(reason), Some(StreamRefRemoteFailureException(reason)),
                None<String>)
        case StreamRefMsg.ConsumerPull =>
            match (sinkPull<T>(st, ctx)) {
                case Some(b) => b
                case None =>
                    sinkSendDemand<T>(st, ctx)
                    TypedBehaviors.same<StreamRefMsg>()
            }
        case StreamRefMsg.SubscriberCancel(reason) =>
            sinkTerminate<T>(st, ctx, StreamTermination.Cancelled(reason), None<StreamRefException>,
                Some("消费方取消：${reason}"))
        case StreamRefMsg.SubscriptionTimeout =>
            if (st.producer.isNone()) {
                let reason = "subscription timeout：${st.settings.subscriptionTimeout} 内没有生产方挂接"
                sinkTerminate<T>(st, ctx, StreamTermination.Failed(reason),
                    Some(StreamRefSubscriptionTimeoutException(reason)), None<String>)
            } else {
                TypedBehaviors.same<StreamRefMsg>()
            }
        case StreamRefMsg.IdleTimeout =>
            if (st.grantedUpTo > st.receivedSeq) {
                let reason = "idle timeout：已授予需求至 #${st.grantedUpTo}，但生产方在 " +
                    "${st.settings.idleTimeout ?? Duration.Zero} 内没有发来任何消息"
                sinkTerminate<T>(st, ctx, StreamTermination.Failed(reason),
                    Some(StreamRefIdleTimeoutException(reason)), Some("消费方失败：${reason}"))
            } else {
                TypedBehaviors.same<StreamRefMsg>()
            }
        case StreamRefMsg.TerminationDeadline =>
            let reason = "生产方已终止，宽限期 ${st.settings.finalTerminationSignalDeadline} 内没有收到终止信号"
            sinkTerminate<T>(st, ctx, StreamTermination.Failed(reason),
                Some(StreamRefPeerTerminatedException(reason)), None<String>)
        case _ =>
            streamRefLog.debug("SinkRef ${st.streamId} 忽略消息 ${sid}")
            TypedBehaviors.same<StreamRefMsg>()
    }
}

func sinkOnProducerSubscribe<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>, peer: StreamPeer,
    ver: Int64): TypedBehavior<StreamRefMsg> {
    if (ver != STREAM_REF_PROTOCOL_VERSION) {
        peer.tellQuietly(StreamRefMsg.Rejected(st.streamId,
            "协议版本不符：sink ${STREAM_REF_PROTOCOL_VERSION}，生产方 ${ver}"))
        return TypedBehaviors.same<StreamRefMsg>()
    }
    match (st.producer) {
        case Some(existing) =>
            if (existing.key == peer.key) {
                // 同一个生产方重发 Subscribe（例如它以为前一条丢了）：幂等确认
                existing.tellQuietly(StreamRefMsg.OnSubscribe(st.streamId))
            } else {
                // 单次挂接：一个 SinkRef 只允许一个生产方，第二个明确拒绝，不影响第一个
                peer.tellQuietly(StreamRefMsg.Rejected(st.streamId,
                    "SinkRef 只能挂接一个生产方，已被 ${existing.key} 挂接"))
            }
            TypedBehaviors.same<StreamRefMsg>()
        case None =>
            st.producer = Some(peer)
            st.shared.attached.store(true)
            ctx.timers.cancel(KEY_SUBSCRIPTION_TIMEOUT)
            st.env.watchPeer(ctx, peer)
            peer.tellQuietly(StreamRefMsg.OnSubscribe(st.streamId))
            // 初始信用 = 本地缓冲容量；之后每交付 bufferSize/2 个再补一次
            sinkSendDemand<T>(st, ctx)
            TypedBehaviors.same<StreamRefMsg>()
    }
}

func sinkOnElement<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>, seq: Int64,
    value: Any): TypedBehavior<StreamRefMsg> {
    if (st.producer.isNone()) {
        return sinkProtocolFailure<T>(st, ctx, "挂接确认之前收到元素 #${seq}")
    }
    let expected = st.receivedSeq + 1
    if (seq < expected) {
        return sinkProtocolFailure<T>(st, ctx, "重复元素：收到 #${seq}，期望 #${expected}")
    }
    if (seq > expected) {
        return sinkProtocolFailure<T>(st, ctx, "元素缺号：收到 #${seq}，期望 #${expected}（中间有元素丢失）")
    }
    if (seq > st.grantedUpTo) {
        return sinkProtocolFailure<T>(st, ctx, "元素 #${seq} 超出已授予的需求 #${st.grantedUpTo}")
    }
    if (let Some(u) <- (value as StreamRefUndecodable)) {
        return sinkProtocolFailure<T>(st, ctx, "元素 #${seq} 无法解码：${u.reason}")
    }
    st.receivedSeq = seq
    st.shared.received.store(seq)
    st.buffer.addLast(value)
    let unconsumed = st.receivedSeq - st.deliveredCount
    if (unconsumed > st.shared.maxBufferedValue.load()) {
        st.shared.maxBufferedValue.store(unconsumed)
    }
    restartSinkIdleTimer(st, ctx)
    // 先交付给等着的 pull，再按空出的格子补信用
    match (sinkPull<T>(st, ctx)) {
        case Some(b) => return b
        case None => sinkSendDemand<T>(st, ctx)
    }
    TypedBehaviors.same<StreamRefMsg>()
}

/**
 * 消费方 pull：有挂起 pull 且缓冲非空时交付一个；排空且已收到 Completed 时完成流。
 * 返回 Some(behavior) 表示流已终止，调用方必须返回该 behavior（阶段已 stopped）。
 */
func sinkPull<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>): Option<TypedBehavior<StreamRefMsg>> {
    if (st.terminal) {
        return Some(TypedBehaviors.same<StreamRefMsg>())
    }
    if (!st.buffer.isEmpty() && st.shared.hasPendingPull()) {
        match (st.buffer.removeFirst()) {
            case Some(value) =>
                match (st.shared.deliverAny(value)) {
                    case DeliverResult.Delivered =>
                        st.deliveredCount++
                        st.shared.delivered.store(st.deliveredCount)
                    case DeliverResult.TypeMismatch =>
                        return Some(sinkProtocolFailure<T>(st, ctx, "元素类型与 SinkRef 声明的类型不符"))
                    case DeliverResult.NoWaiter =>
                        // 与 finish 竞争时理论上不会发生；放回队首，绝不丢元素
                        st.buffer.addFirst(value)
                }
            case None => ()
        }
    }
    if (st.completedPending && st.buffer.isEmpty()) {
        return Some(sinkTerminate<T>(st, ctx, StreamTermination.Completed, None<StreamRefException>, None<String>))
    }
    None<TypedBehavior<StreamRefMsg>>
}

func sinkOnCompleted<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>,
    lastSeq: Int64): TypedBehavior<StreamRefMsg> {
    if (st.producer.isNone()) {
        return sinkProtocolFailure<T>(st, ctx, "挂接确认之前收到 Completed")
    }
    if (lastSeq != st.receivedSeq) {
        return sinkProtocolFailure<T>(st, ctx,
            "完成序号不符：生产方最后序号 ${lastSeq}，本端只收到 ${st.receivedSeq}（有元素丢失）")
    }
    st.completedPending = true
    // 已收到终止信号：对端随后停止是正常现象，不能再按"没有终止信号"失败（缓冲可能还在慢慢排空）
    ctx.timers.cancel(KEY_TERMINATION_DEADLINE)
    if (st.buffer.isEmpty()) {
        return sinkTerminate<T>(st, ctx, StreamTermination.Completed, None<StreamRefException>, None<String>)
    }
    // 缓冲里还有元素：等本地消费方逐个 pull 完再发终止信号
    TypedBehaviors.same<StreamRefMsg>()
}

/** 预取窗口：目标 = 已交付 + bufferSize；空出至少一半窗口才补发，减少 Demand 消息数 */
func sinkSendDemand<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>): Unit {
    if (st.completedPending) {
        return
    }
    match (st.producer) {
        case Some(peer) =>
            let target = st.deliveredCount + st.settings.bufferSize
            let threshold = if (st.settings.bufferSize / 2 < 1) { 1 } else { st.settings.bufferSize / 2 }
            if (target - st.grantedUpTo >= threshold) {
                st.grantedUpTo = target
                st.shared.granted.store(target)
                peer.tellQuietly(StreamRefMsg.Demand(st.streamId, target))
                restartSinkIdleTimer(st, ctx)
            }
        case None => ()
    }
}

func restartSinkIdleTimer<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>): Unit {
    if (let Some(d) <- st.settings.idleTimeout) {
        if (st.producer.isSome() && st.grantedUpTo > st.receivedSeq) {
            ctx.timers.startSingleTimer(KEY_IDLE_TIMEOUT, StreamRefMsg.IdleTimeout, d)
        } else {
            ctx.timers.cancel(KEY_IDLE_TIMEOUT)
        }
    }
}

/** 本端协议违例：以协议异常结束，并告诉生产方取消（它继续发也只会被拒） */
func sinkProtocolFailure<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>,
    message: String): TypedBehavior<StreamRefMsg> {
    sinkTerminate<T>(st, ctx, StreamTermination.Failed(message), Some(StreamRefProtocolException(message)),
        Some("消费方失败：${message}"))
}

/**
 * 统一的终止路径：先记录恰好一次的结局，再按需通知生产方。
 * cancelReason 为 Some 时向生产方发 Cancel（生产方据此停止生成）。
 */
func sinkTerminate<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>, t: StreamTermination,
    err: Option<StreamRefException>, cancelReason: Option<String>): TypedBehavior<StreamRefMsg> {
    if (st.terminal) {
        return TypedBehaviors.same<StreamRefMsg>()
    }
    st.terminal = true
    st.shared.finish(t, err)
    if (let Some(reason) <- cancelReason) {
        if (let Some(p) <- st.producer) {
            p.tellQuietly(StreamRefMsg.Cancel(st.streamId, reason))
        }
    }
    sinkCleanup<T>(st, ctx)
    TypedBehaviors.stopped<StreamRefMsg>()
}

func sinkCleanup<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>): Unit {
    if (st.cleaned) {
        return
    }
    st.cleaned = true
    st.buffer.clear()
    ctx.timers.cancelAll()
    if (let Some(p) <- st.producer) {
        st.env.unwatchPeer(ctx, p)
    }
    st.env.unexpose(st.exposedName)
}

func sinkOnSignal<T>(st: SinkStageState<T>, ctx: TypedActorContext<StreamRefMsg>,
    sig: TypedSignal): TypedBehavior<StreamRefMsg> {
    match (sig) {
        case t: TypedTerminated =>
            if (st.terminal) {
                return TypedBehaviors.same<StreamRefMsg>()
            }
            if (isPeerRef(st.producer, t.ref)) {
                if (st.completedPending) {
                    // 终止信号已经收到，只是在等本地 pull 排空缓冲
                    return TypedBehaviors.same<StreamRefMsg>()
                }
                if (st.settings.finalTerminationSignalDeadline > Duration.Zero) {
                    // Completed/Failed 走普通消息通道、DEATH 走系统通道，后者可能先到（同进程也会）；
                    // 给终止信号一个宽限期，避免把正常完成误判成"对端消失"
                    ctx.timers.startSingleTimer(KEY_TERMINATION_DEADLINE, StreamRefMsg.TerminationDeadline,
                        st.settings.finalTerminationSignalDeadline)
                    return TypedBehaviors.same<StreamRefMsg>()
                }
                let reason = "生产方已终止且没有发出终止信号"
                return sinkTerminate<T>(st, ctx, StreamTermination.Failed(reason),
                    Some(StreamRefPeerTerminatedException(reason)), None<String>)
            }
            if (let Some(o) <- st.owner) {
                if (o.path.toString() == t.ref.path.toString()) {
                    let reason = "SinkRef 属主终止：${o.path.toString()}"
                    return sinkTerminate<T>(st, ctx, StreamTermination.Failed(reason),
                        Some(StreamRefPeerTerminatedException(reason)), Some("SinkRef 属主终止"))
                }
            }
            TypedBehaviors.same<StreamRefMsg>()
        case _: PostStop =>
            // 被外部停止（system.stop、系统关闭）而没有走终止路径：仍要恰好一个结局并取消生产方
            if (!st.terminal) {
                st.terminal = true
                let reason = "SinkRef 阶段已停止"
                st.shared.finish(StreamTermination.Failed(reason), Some(StreamRefPeerTerminatedException(reason)))
                if (let Some(p) <- st.producer) {
                    p.tellQuietly(StreamRefMsg.Cancel(st.streamId, reason))
                }
            }
            sinkCleanup<T>(st, ctx)
            TypedBehaviors.same<StreamRefMsg>()
        case _ => TypedBehaviors.same<StreamRefMsg>()
    }
}

// ================= 生产方（远端把元素推进来） =================

/** 生产方阶段的可变状态：只在阶段 Actor 线程访问 */
class SinkProducerState {
    let streamId: String
    let exposedName: String
    let settings: StreamRefSettings
    let shared: ProducerShared
    let env: StreamRefEnv
    let sink: StreamPeer
    let owner: Option<ActorRef>
    let pending = ArrayList<Any>()
    var attached: Bool = false
    var sentSeq: Int64 = 0
    var grantedUpTo: Int64 = 0
    var completing: Bool = false
    var terminal: Bool = false
    var cleaned: Bool = false

    init(streamId: String, exposedName: String, settings: StreamRefSettings, shared: ProducerShared,
        env: StreamRefEnv, sink: StreamPeer, owner: Option<ActorRef>) {
        this.streamId = streamId
        this.exposedName = exposedName
        this.settings = settings
        this.shared = shared
        this.env = env
        this.sink = sink
        this.owner = owner
    }
}

/**
 * SinkRef 的生产方句柄：既可以直接 offer（逐 token 推送），也可以直接作为 Reactive Streams
 * Subscriber 交给 `Sinks.fromSubscriber`（它只在本端有空位时向上游请求一个元素）。
 *
 * ```
 * let producer = refs.asSink<String>(sinkRef)     // 立即向远端 sink 挂接
 * producer.offer("Hello")                         // Enqueued / Backpressured / Closed
 * Sources.from(...).runWith(Sinks.fromSubscriber<String>(producer), system)
 * producer.termination().onComplete { t => ... }  // Cancelled：sink 取消/失败/属主停止
 * ```
 */
public class SinkRefProducer<T> <: StreamWriter<T> & Subscriber<T> {
    let shared: ProducerShared
    let stage: TypedActorRef<StreamRefMsg>
    private var adapter: Option<ProducerSubscriber<T>> = None<ProducerSubscriber<T>>

    init(shared: ProducerShared, stage: TypedActorRef<StreamRefMsg>, env: StreamRefEnv) {
        this.shared = shared
        this.stage = stage
        attachAdapter(env)
    }

    private func attachAdapter(env: StreamRefEnv): Unit {
        let a = ProducerSubscriber<T>(this)
        let pump = env.system.typedActorOf<RefPump>(producerSubscriberBehavior<T>(a),
            "streamref-pump-${env.nextStreamId()}")
        a.bind(pump)
        adapter = Some(a)
    }

    /** 非阻塞投递：Enqueued / Backpressured（缓冲满）/ Closed（流已结束） */
    public func offer(element: T): OfferResult {
        let r = shared.tryReserve()
        if (r.isEnqueued()) {
            stage.tell(StreamRefMsg.Offer(element))
        }
        r
    }

    /** 有空位时完成为 true；流已结束时完成为 false */
    public func whenReady(): TypedPromise<Bool> {
        shared.whenReady()
    }

    /** 已投递的元素全部发出后发送 Completed */
    public func complete(): Unit {
        stage.tell(StreamRefMsg.ProducerComplete)
    }

    /** 立即以 Failed(reason) 结束（未发出的元素丢弃），sink 侧收到 onError */
    public func fail(reason: String): Unit {
        stage.tell(StreamRefMsg.ProducerFail(reason))
    }

    /** 流的结局，恰好完成一次：Completed / Cancelled（sink 取消、失败或属主停止）/ Failed */
    public func termination(): TypedPromise<StreamTermination> {
        shared.terminationPromise
    }

    public func isClosed(): Bool {
        shared.isClosed()
    }

    /** 是否已被 sink 接受挂接 */
    public func isAttached(): Bool {
        shared.attached.load()
    }

    /** 已发到线上的元素数（= 最后序号） */
    public func sentCount(): Int64 {
        shared.sent.load()
    }

    /** sink 授予的累计需求（允许发送的最大序号） */
    public func grantedDemand(): Int64 {
        shared.granted.load()
    }

    /** 已 offer、尚未发上线的元素数（≤ bufferSize） */
    public func bufferedCount(): Int64 {
        shared.bufferedCount()
    }

    /** 发送时观测到的最大超发量 max(seq - grantedUpTo)：构造上恒为 0 */
    public func maxOverGrant(): Int64 {
        shared.maxOutstanding.load()
    }

    public func stageRef(): TypedActorRef<StreamRefMsg> {
        stage
    }

    // ---------- Reactive Streams Subscriber ----------
    // 委托给 ProducerSubscriber：只有本端有空位时才向上游 request(1)，不无界预读。

    public func onSubscribe(subscription: Subscription): Unit {
        adapter.getOrThrow().onSubscribe(subscription)
    }

    public func onNext(element: T): Unit {
        adapter.getOrThrow().onNext(element)
    }

    public func onError(cause: Exception): Unit {
        adapter.getOrThrow().onError(cause)
    }

    public func onComplete(): Unit {
        adapter.getOrThrow().onComplete()
    }
}

func sinkProducerStage(st: SinkProducerState): TypedBehavior<StreamRefMsg> {
    TypedBehaviors.setup<StreamRefMsg>({ ctx =>
        ctx.timers.startSingleTimer(KEY_SUBSCRIPTION_TIMEOUT, StreamRefMsg.SubscriptionTimeout,
            st.settings.subscriptionTimeout)
        if (let Some(o) <- st.owner) {
            if (let Some(c) <- ctx.toClassic()) {
                c.watch(o)
            }
        }
        // 先监视并暴露自己，再挂接：sink 的 OnSubscribe/Demand 要能回送到这里
        st.env.watchPeer(ctx, st.sink)
        var selfLocation = ""
        if (st.sink.isRemote()) {
            try {
                selfLocation = st.env.expose(st.exposedName, ctx.self)
            } catch (e: Exception) {
                return sinkProducerTerminate(st, ctx,
                    StreamTermination.Failed("SinkRef 生产方暴露失败：${e.message}"), false)
            }
        }
        let behavior = TypedBehaviors.receive<StreamRefMsg>({ c, msg => sinkProducerOnMessage(st, c, msg) })
            .receiveSignal({ c, sig => sinkProducerOnSignal(st, c, sig) })
        let attach = if (st.sink.isRemote()) {
            StreamRefMsg.Subscribe(st.streamId, selfLocation, STREAM_REF_PROTOCOL_VERSION)
        } else {
            StreamRefMsg.SubscribeLocal(st.streamId, ctx.self, STREAM_REF_PROTOCOL_VERSION)
        }
        try {
            st.sink.ref.tell(attach)
        } catch (e: Exception) {
            return sinkProducerTerminate(st, ctx,
                StreamTermination.Failed("无法向 SinkRef 发送挂接请求：${e.message}"), false)
        }
        behavior
    })
}

func sinkProducerOnMessage(st: SinkProducerState, ctx: TypedActorContext<StreamRefMsg>,
    msg: StreamRefMsg): TypedBehavior<StreamRefMsg> {
    if (st.terminal) {
        return TypedBehaviors.same<StreamRefMsg>()
    }
    let sid = msg.streamIdOrEmpty()
    if (!sid.isEmpty() && sid != st.streamId) {
        return TypedBehaviors.same<StreamRefMsg>()
    }
    match (msg) {
        case StreamRefMsg.OnSubscribe(_) =>
            if (!st.attached) {
                st.attached = true
                st.shared.attached.store(true)
                ctx.timers.cancel(KEY_SUBSCRIPTION_TIMEOUT)
                sinkProducerFlush(st, ctx)
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.Rejected(_, reason) =>
            sinkProducerTerminate(st, ctx, StreamTermination.Failed("挂接被拒绝：${reason}"), false)
        case StreamRefMsg.Demand(_, upTo) =>
            if (st.attached && upTo > st.grantedUpTo) {
                st.grantedUpTo = upTo
                st.shared.granted.store(upTo)
                return sinkProducerFlush(st, ctx)
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.Cancel(_, reason) =>
            sinkProducerTerminate(st, ctx, StreamTermination.Cancelled(reason), false)
        case StreamRefMsg.Offer(value) =>
            st.pending.add(value)
            sinkProducerFlush(st, ctx)
        case StreamRefMsg.ProducerComplete =>
            st.completing = true
            sinkProducerFlush(st, ctx)
        case StreamRefMsg.ProducerFail(reason) =>
            sinkProducerTerminate(st, ctx, StreamTermination.Failed(reason), true)
        case StreamRefMsg.SubscriptionTimeout =>
            if (!st.attached) {
                sinkProducerTerminate(st, ctx, StreamTermination.Failed(
                    "subscription timeout：${st.settings.subscriptionTimeout} 内没有收到 sink 的挂接确认"), false)
            } else {
                TypedBehaviors.same<StreamRefMsg>()
            }
        case StreamRefMsg.TerminationDeadline =>
            sinkProducerTerminate(st, ctx, StreamTermination.Cancelled("sink 已终止且没有发出终止信号"), false)
        case _ => TypedBehaviors.same<StreamRefMsg>()
    }
}

/** 在已授予需求内把排队元素发上线；完成请求且队列清空时发 Completed */
func sinkProducerFlush(st: SinkProducerState, ctx: TypedActorContext<StreamRefMsg>): TypedBehavior<StreamRefMsg> {
    if (!st.attached) {
        return TypedBehaviors.same<StreamRefMsg>()
    }
    var released: Int64 = 0
    while (!st.pending.isEmpty() && st.sentSeq < st.grantedUpTo) {
        let value = st.pending.remove(at: 0)
        released++
        let seq = st.sentSeq + 1
        try {
            st.sink.ref.tell(StreamRefMsg.Element(st.streamId, seq, value))
        } catch (e: Exception) {
            // 元素编码失败在发送侧大声失败，并告诉 sink 原因（不能静默丢元素）
            st.shared.release(released)
            let reason = "元素 #${seq} 发送失败：${e.message}"
            return sinkProducerTerminate(st, ctx, StreamTermination.Failed(reason), true)
        }
        st.sentSeq = seq
        st.shared.sent.store(seq)
        let over = seq - st.grantedUpTo
        if (over > st.shared.maxOutstanding.load()) {
            st.shared.maxOutstanding.store(over)
        }
    }
    if (released > 0) {
        st.shared.release(released)
    }
    if (st.completing && st.pending.isEmpty()) {
        st.sink.tellQuietly(StreamRefMsg.Completed(st.streamId, st.sentSeq))
        return sinkProducerTerminate(st, ctx, StreamTermination.Completed, false)
    }
    TypedBehaviors.same<StreamRefMsg>()
}

func sinkProducerTerminate(st: SinkProducerState, ctx: TypedActorContext<StreamRefMsg>,
    t: StreamTermination, notifySinkFailure: Bool): TypedBehavior<StreamRefMsg> {
    if (st.terminal) {
        return TypedBehaviors.same<StreamRefMsg>()
    }
    st.terminal = true
    let dropped = st.pending.size
    st.pending.clear()
    if (dropped > 0) {
        st.shared.release(dropped)
    }
    if (notifySinkFailure && st.attached) {
        st.sink.tellQuietly(StreamRefMsg.Failed(st.streamId, t.reason()))
    }
    st.shared.close(t)
    sinkProducerCleanup(st, ctx)
    TypedBehaviors.stopped<StreamRefMsg>()
}

func sinkProducerCleanup(st: SinkProducerState, ctx: TypedActorContext<StreamRefMsg>): Unit {
    if (st.cleaned) {
        return
    }
    st.cleaned = true
    ctx.timers.cancelAll()
    st.env.unwatchPeer(ctx, st.sink)
    st.env.unexpose(st.exposedName)
}

func sinkProducerOnSignal(st: SinkProducerState, ctx: TypedActorContext<StreamRefMsg>,
    sig: TypedSignal): TypedBehavior<StreamRefMsg> {
    match (sig) {
        case t: TypedTerminated =>
            if (st.terminal) {
                return TypedBehaviors.same<StreamRefMsg>()
            }
            if (isPeerRef(Some(st.sink), t.ref)) {
                if (st.settings.finalTerminationSignalDeadline > Duration.Zero) {
                    // Cancel / Completed 走普通通道、DEATH 走系统通道：给终止信号一个宽限期
                    ctx.timers.startSingleTimer(KEY_TERMINATION_DEADLINE, StreamRefMsg.TerminationDeadline,
                        st.settings.finalTerminationSignalDeadline)
                    return TypedBehaviors.same<StreamRefMsg>()
                }
                // sink 阶段或节点没了：等同取消，让上游（例如 LLM 调用）停下
                return sinkProducerTerminate(st, ctx,
                    StreamTermination.Cancelled("消费方终止：${t.ref.path.toString()}"), false)
            }
            if (let Some(o) <- st.owner) {
                if (o.path.toString() == t.ref.path.toString()) {
                    let reason = "生产方属主终止：${o.path.toString()}"
                    return sinkProducerTerminate(st, ctx, StreamTermination.Failed(reason), true)
                }
            }
            TypedBehaviors.same<StreamRefMsg>()
        case _: PostStop =>
            if (!st.terminal) {
                st.terminal = true
                let reason = "SinkRef 生产方阶段已停止"
                let dropped = st.pending.size
                st.pending.clear()
                if (dropped > 0) {
                    st.shared.release(dropped)
                }
                if (st.attached) {
                    st.sink.tellQuietly(StreamRefMsg.Failed(st.streamId, reason))
                }
                st.shared.close(StreamTermination.Failed(reason))
            }
            sinkProducerCleanup(st, ctx)
            TypedBehaviors.same<StreamRefMsg>()
        case _ => TypedBehaviors.same<StreamRefMsg>()
    }
}
