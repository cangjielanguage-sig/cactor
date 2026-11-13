/*
 * CActor Cluster.StreamRef - 消费方：Reactive Streams 形状的订阅者/订阅 + 消费方阶段 Actor
 *
 * 订阅者接口与 Reactive Streams 一致（onSubscribe / onNext / onComplete / onError + request(n) / cancel），
 * 以后 cactor_streams 可以直接把它适配成 Source.fromPublisher，不需要再理解线上协议。
 *
 * 信号规则（RS 1.x / 2.x）：
 * - onSubscribe 最先；onNext 次数 ≤ 订阅者 request 的总数；onComplete / onError 至多一个、之后无信号；
 * - cancel 之后不再有任何信号（包括终止信号）；request(n ≤ 0) 以 onError 结束并取消上游（RS 3.9）。
 * 全部回调在消费方阶段 Actor 线程串行执行。
 *
 * 预取窗口：向生产方授予的累计需求 ≤ 已交付 + bufferSize，并在窗口空出一半时补发（批量补信用，
 * 不是每个元素一条 Demand）。于是消费方缓冲 + 在途元素 ≤ bufferSize：慢消费者两端内存都有界。
 */
package cactor_cluster.streamref

import std.collection.*
import std.sync.*
import cactor_core.typed.*
import cactor_remote.{RemoteActorRef}

/** Reactive Streams 形状的订阅者 */
public interface StreamRefSubscriber<T> {
    func onSubscribe(subscription: StreamRefSubscription): Unit
    func onNext(element: T): Unit
    func onComplete(): Unit
    func onError(error: StreamRefException): Unit
}

/** 发给类型化 Actor 的订阅事件：Actor 自己决定何时 request，天然是"慢消费者按批请求" */
public enum StreamRefEvent<T> {
    | OnSubscribe(StreamRefSubscription)
    | OnNext(T)
    | OnComplete
    | OnError(StreamRefException)
}

/**
 * 订阅句柄（线程安全）：request / cancel 只向消费方阶段发消息，不在调用线程做任何协议工作。
 * 统计量供观测与测试断言（例如"在途 + 缓冲从不超过窗口"）。
 */
public class StreamRefSubscription {
    private let lock = Mutex()
    private var stage: Option<TypedActorRef<StreamRefMsg>> = None<TypedActorRef<StreamRefMsg>>
    private let early = ArrayList<StreamRefMsg>()
    let cancelledFlag = AtomicBool(false)
    let delivered = AtomicInt64(0)
    let received = AtomicInt64(0)
    let requestedUpTo = AtomicInt64(0)
    let maxUnconsumedValue = AtomicInt64(0)
    let subscribed = AtomicBool(false)
    let terminatedFlag = AtomicBool(false)
    public let streamId: String

    init(streamId: String) {
        this.streamId = streamId
    }

    /** 绑定阶段引用；绑定前的 request/cancel 暂存，绑定时按序补发 */
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

    /** 再要 n 个元素（累加，饱和到 Int64.Max）；n ≤ 0 按 RS 3.9 以 onError 结束 */
    public func request(n: Int64): Unit {
        if (cancelledFlag.load()) {
            return
        }
        send(StreamRefMsg.SubscriberRequest(n))
    }

    /** 取消：生产方收到 Cancel 后结束（termination() 为 Cancelled），本订阅者之后不再收到信号 */
    public func cancel(): Unit {
        cancel("消费方取消")
    }

    public func cancel(reason: String): Unit {
        if (cancelledFlag.compareAndSwap(false, true)) {
            send(StreamRefMsg.SubscriberCancel(reason))
        }
    }

    public func isCancelled(): Bool {
        cancelledFlag.load()
    }

    /** 是否已收到生产方的 OnSubscribe */
    public func isSubscribed(): Bool {
        subscribed.load()
    }

    /** 流是否已结束（完成、失败或取消） */
    public func isTerminated(): Bool {
        terminatedFlag.load()
    }

    /** 已交付给订阅者的元素数 */
    public func deliveredCount(): Int64 {
        delivered.load()
    }

    /** 已从生产方收到的元素数（= 最后序号） */
    public func receivedCount(): Int64 {
        received.load()
    }

    /** 已向生产方授予的累计需求 */
    public func requestedDemand(): Int64 {
        requestedUpTo.load()
    }

    /** 观测到的最大"已收到未交付"数量：恒 ≤ bufferSize */
    public func maxUnconsumed(): Int64 {
        maxUnconsumedValue.load()
    }
}

/** 订阅者的非泛型外壳：类型转换在用户调用的泛型入口里构造，阶段本身不泛型 */
class AnySubscriber {
    let onSubscribeF: (StreamRefSubscription) -> Unit
    let onNextF: (Any) -> Unit
    let onCompleteF: () -> Unit
    let onErrorF: (StreamRefException) -> Unit

    init(onSubscribeF: (StreamRefSubscription) -> Unit, onNextF: (Any) -> Unit, onCompleteF: () -> Unit,
        onErrorF: (StreamRefException) -> Unit) {
        this.onSubscribeF = onSubscribeF
        this.onNextF = onNextF
        this.onCompleteF = onCompleteF
        this.onErrorF = onErrorF
    }
}

/** 消费方阶段的可变状态：只在阶段 Actor 线程访问 */
class ConsumerStageState {
    let streamId: String
    let exposedName: String
    var selfLocation: String = ""
    let settings: StreamRefSettings
    let env: StreamRefEnv
    let producer: StreamPeer
    let subscriber: AnySubscriber
    let subscription: StreamRefSubscription
    let buffer = ArrayList<Any>()
    var subscribed: Bool = false
    var receivedSeq: Int64 = 0
    var requestedUpTo: Int64 = 0
    var deliveredCount: Int64 = 0
    var subscriberDemand: Int64 = 0
    var completedPending: Bool = false
    var terminal: Bool = false
    var cleaned: Bool = false

    init(streamId: String, exposedName: String, settings: StreamRefSettings, env: StreamRefEnv,
        producer: StreamPeer, subscriber: AnySubscriber, subscription: StreamRefSubscription) {
        this.streamId = streamId
        this.exposedName = exposedName
        this.settings = settings
        this.env = env
        this.producer = producer
        this.subscriber = subscriber
        this.subscription = subscription
    }
}

func consumerStage(st: ConsumerStageState): TypedBehavior<StreamRefMsg> {
    TypedBehaviors.setup<StreamRefMsg>({ ctx =>
        st.subscription.bind(ctx.self)
        if (!st.exposedName.isEmpty()) {
            // 先暴露再挂接：生产方的 OnSubscribe 可能在 Subscribe 发出后立刻回来
            st.selfLocation = st.env.expose(st.exposedName, ctx.self)
        }
        ctx.timers.startSingleTimer(KEY_SUBSCRIPTION_TIMEOUT, StreamRefMsg.SubscriptionTimeout, st.settings.subscriptionTimeout)
        // 先监视再挂接：生产方已不存在时，远程监视对未知暴露名立即回 DEATH，不必等订阅超时
        st.env.watchPeer(ctx, st.producer)
        let attach = if (st.producer.isRemote()) {
            StreamRefMsg.Subscribe(st.streamId, st.selfLocation, STREAM_REF_PROTOCOL_VERSION)
        } else {
            StreamRefMsg.SubscribeLocal(st.streamId, ctx.self, STREAM_REF_PROTOCOL_VERSION)
        }
        var attachError: Option<Exception> = None<Exception>
        try {
            st.producer.ref.tell(attach)
        } catch (e: Exception) {
            attachError = Some(e)
        }
        // RS 1.9：onSubscribe 必须是第一个信号（即使随后马上 onError）
        let behavior = TypedBehaviors.receive<StreamRefMsg>({ c, msg => consumerOnMessage(st, c, msg) })
            .receiveSignal({ c, sig => consumerOnSignal(st, c, sig) })
        try {
            st.subscriber.onSubscribeF(st.subscription)
        } catch (e: Exception) {
            return failAndCancel(st, ctx, StreamRefSubscriberException("onSubscribe 抛出异常：${e.message}"))
        }
        if (let Some(e) <- attachError) {
            return fail(st, ctx, StreamRefRejectedException("无法向生产方发送挂接请求：${e.message}"), false)
        }
        behavior
    })
}

func consumerOnMessage(st: ConsumerStageState, ctx: TypedActorContext<StreamRefMsg>, msg: StreamRefMsg): TypedBehavior<StreamRefMsg> {
    if (st.terminal) {
        return TypedBehaviors.same<StreamRefMsg>()
    }
    let sid = msg.streamIdOrEmpty()
    if (!sid.isEmpty() && sid != st.streamId) {
        streamRefLog.warn("StreamRef 消费方 ${st.streamId} 收到其他流 ${sid} 的消息，忽略")
        return TypedBehaviors.same<StreamRefMsg>()
    }
    match (msg) {
        case StreamRefMsg.OnSubscribe(_) =>
            if (!st.subscribed) {
                st.subscribed = true
                st.subscription.subscribed.store(true)
                ctx.timers.cancel(KEY_SUBSCRIPTION_TIMEOUT)
                sendDemand(st, ctx)
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.Rejected(_, reason) =>
            fail(st, ctx, StreamRefRejectedException("挂接被拒绝：${reason}"), false)
        case StreamRefMsg.Element(_, seq, value) => onElement(st, ctx, seq, value)
        case StreamRefMsg.Completed(_, lastSeq) =>
            if (!st.subscribed) {
                return failAndCancel(st, ctx, StreamRefProtocolException("挂接确认之前收到 Completed"))
            }
            if (lastSeq != st.receivedSeq) {
                return failAndCancel(st, ctx, StreamRefProtocolException(
                    "完成序号不符：生产方最后序号 ${lastSeq}，本端只收到 ${st.receivedSeq}（有元素丢失）"))
            }
            st.completedPending = true
            deliver(st, ctx)
        case StreamRefMsg.Failed(_, reason) =>
            // 失败立即传播，不先排空缓冲（与 RS / Akka 相同）
            fail(st, ctx, StreamRefRemoteFailureException(reason), false)
        case StreamRefMsg.SubscriberRequest(n) =>
            if (n <= 0) {
                return failAndCancel(st, ctx, StreamRefSubscriberException("request(${n}) 违反 Reactive Streams 3.9：n 必须为正"))
            }
            st.subscriberDemand = if (st.subscriberDemand > Int64.Max - n) { Int64.Max } else { st.subscriberDemand + n }
            deliver(st, ctx)
        case StreamRefMsg.SubscriberCancel(reason) =>
            st.terminal = true
            st.subscription.terminatedFlag.store(true)
            st.producer.tellQuietly(StreamRefMsg.Cancel(st.streamId, reason))
            consumerCleanup(st, ctx)
            TypedBehaviors.stopped<StreamRefMsg>()
        case StreamRefMsg.SubscriptionTimeout =>
            if (!st.subscribed) {
                return failAndCancel(st, ctx, StreamRefSubscriptionTimeoutException(
                    "subscription timeout：${st.settings.subscriptionTimeout} 内没有收到生产方 ${st.producer.key} 的挂接确认"))
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.IdleTimeout =>
            if (st.requestedUpTo > st.receivedSeq) {
                return failAndCancel(st, ctx, StreamRefIdleTimeoutException(
                    "idle timeout：已授予需求至 #${st.requestedUpTo}，但生产方在 ${st.settings.idleTimeout ?? Duration.Zero} 内没有发来任何消息"))
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.TerminationDeadline =>
            fail(st, ctx, StreamRefPeerTerminatedException(
                "生产方 ${st.producer.key} 已终止，宽限期 ${st.settings.finalTerminationSignalDeadline} 内没有收到终止信号"), false)
        case _ =>
            streamRefLog.debug("StreamRef 消费方 ${st.streamId} 忽略消息")
            TypedBehaviors.same<StreamRefMsg>()
    }
}

func onElement(st: ConsumerStageState, ctx: TypedActorContext<StreamRefMsg>, seq: Int64, value: Any): TypedBehavior<StreamRefMsg> {
    if (!st.subscribed) {
        return failAndCancel(st, ctx, StreamRefProtocolException("挂接确认之前收到元素 #${seq}"))
    }
    let expected = st.receivedSeq + 1
    if (seq < expected) {
        return failAndCancel(st, ctx, StreamRefProtocolException("重复元素：收到 #${seq}，期望 #${expected}"))
    }
    if (seq > expected) {
        return failAndCancel(st, ctx, StreamRefProtocolException("元素缺号：收到 #${seq}，期望 #${expected}（中间有元素丢失）"))
    }
    if (seq > st.requestedUpTo) {
        return failAndCancel(st, ctx, StreamRefProtocolException("元素 #${seq} 超出已授予的需求 #${st.requestedUpTo}"))
    }
    if (let Some(u) <- (value as StreamRefUndecodable)) {
        return failAndCancel(st, ctx, StreamRefProtocolException("元素 #${seq} 无法解码：${u.reason}"))
    }
    st.receivedSeq = seq
    st.subscription.received.store(seq)
    st.buffer.add(value)
    let unconsumed = st.receivedSeq - st.deliveredCount
    if (unconsumed > st.subscription.maxUnconsumedValue.load()) {
        st.subscription.maxUnconsumedValue.store(unconsumed)
    }
    restartIdleTimer(st, ctx)
    deliver(st, ctx)
}

/** 在订阅者需求内交付缓冲；排空且已收到 Completed 时发 onComplete */
func deliver(st: ConsumerStageState, ctx: TypedActorContext<StreamRefMsg>): TypedBehavior<StreamRefMsg> {
    while (st.subscriberDemand > 0 && !st.buffer.isEmpty()) {
        if (st.subscription.cancelledFlag.load()) {
            // cancel() 已调用但 SubscriberCancel 还在邮箱里：RS 要求尽快停止发信号
            return TypedBehaviors.same<StreamRefMsg>()
        }
        let value = st.buffer.remove(at: 0)
        st.deliveredCount++
        st.subscription.delivered.store(st.deliveredCount)
        if (st.subscriberDemand != Int64.Max) {
            st.subscriberDemand--
        }
        try {
            st.subscriber.onNextF(value)
        } catch (e: StreamRefException) {
            return failAndCancel(st, ctx, e)
        } catch (e: Exception) {
            return failAndCancel(st, ctx, StreamRefSubscriberException("onNext 抛出异常：${e.message}"))
        }
    }
    if (st.completedPending && st.buffer.isEmpty()) {
        if (st.subscription.cancelledFlag.load()) {
            return TypedBehaviors.same<StreamRefMsg>()
        }
        st.terminal = true
        st.subscription.terminatedFlag.store(true)
        try {
            st.subscriber.onCompleteF()
        } catch (e: Exception) {
            streamRefLog.warn("StreamRef 订阅者 onComplete 抛出异常：${e.message}")
        }
        consumerCleanup(st, ctx)
        return TypedBehaviors.stopped<StreamRefMsg>()
    }
    sendDemand(st, ctx)
    TypedBehaviors.same<StreamRefMsg>()
}

/** 预取窗口：目标 = 已交付 + bufferSize；空出至少一半窗口才补发，减少 Demand 消息数 */
func sendDemand(st: ConsumerStageState, ctx: TypedActorContext<StreamRefMsg>): Unit {
    if (!st.subscribed || st.completedPending) {
        return
    }
    let target = st.deliveredCount + st.settings.bufferSize
    let threshold = if (st.settings.bufferSize / 2 < 1) { 1 } else { st.settings.bufferSize / 2 }
    if (target - st.requestedUpTo >= threshold) {
        st.requestedUpTo = target
        st.subscription.requestedUpTo.store(target)
        st.producer.tellQuietly(StreamRefMsg.Demand(st.streamId, target))
        restartIdleTimer(st, ctx)
    }
}

func restartIdleTimer(st: ConsumerStageState, ctx: TypedActorContext<StreamRefMsg>): Unit {
    if (let Some(d) <- st.settings.idleTimeout) {
        if (st.requestedUpTo > st.receivedSeq) {
            ctx.timers.startSingleTimer(KEY_IDLE_TIMEOUT, StreamRefMsg.IdleTimeout, d)
        } else {
            ctx.timers.cancel(KEY_IDLE_TIMEOUT)
        }
    }
}

/** 本端判定的失败：通知订阅者，并告诉生产方取消（让它停止生成） */
func failAndCancel(st: ConsumerStageState, ctx: TypedActorContext<StreamRefMsg>, e: StreamRefException): TypedBehavior<StreamRefMsg> {
    fail(st, ctx, e, true)
}

func fail(st: ConsumerStageState, ctx: TypedActorContext<StreamRefMsg>, e: StreamRefException, cancelUpstream: Bool): TypedBehavior<StreamRefMsg> {
    if (!st.terminal) {
        st.terminal = true
        st.subscription.terminatedFlag.store(true)
        if (cancelUpstream) {
            st.producer.tellQuietly(StreamRefMsg.Cancel(st.streamId, "消费方失败：${e.message}"))
        }
        // 已 cancel 的订阅者不再收到任何信号（RS 3.6 之后的静默）
        if (!st.subscription.cancelledFlag.load()) {
            try {
                st.subscriber.onErrorF(e)
            } catch (ex: Exception) {
                streamRefLog.warn("StreamRef 订阅者 onError 抛出异常：${ex.message}")
            }
        }
    }
    consumerCleanup(st, ctx)
    TypedBehaviors.stopped<StreamRefMsg>()
}

func consumerCleanup(st: ConsumerStageState, ctx: TypedActorContext<StreamRefMsg>): Unit {
    if (st.cleaned) {
        return
    }
    st.cleaned = true
    st.buffer.clear()
    ctx.timers.cancelAll()
    st.env.unwatchPeer(ctx, st.producer)
    if (!st.exposedName.isEmpty()) {
        st.env.unexpose(st.exposedName)
    }
}

func consumerOnSignal(st: ConsumerStageState, ctx: TypedActorContext<StreamRefMsg>, sig: TypedSignal): TypedBehavior<StreamRefMsg> {
    match (sig) {
        case t: TypedTerminated =>
            if (st.terminal || !isPeerRef(Some(st.producer), t.ref)) {
                return TypedBehaviors.same<StreamRefMsg>()
            }
            if (st.producer.isRemote() && st.settings.finalTerminationSignalDeadline > Duration.Zero) {
                // Completed/Failed 走普通通道、DEATH 走系统通道，后者可能先到：给终止信号一个宽限期
                ctx.timers.startSingleTimer(KEY_TERMINATION_DEADLINE, StreamRefMsg.TerminationDeadline,
                    st.settings.finalTerminationSignalDeadline)
                return TypedBehaviors.same<StreamRefMsg>()
            }
            fail(st, ctx, StreamRefPeerTerminatedException("生产方 ${st.producer.key} 已终止且没有发出终止信号"), false)
        case _: PostStop =>
            if (!st.terminal) {
                // 被外部停止：告诉生产方取消（中止上游工作），订阅者收到唯一的 onError
                st.terminal = true
                st.subscription.terminatedFlag.store(true)
                st.producer.tellQuietly(StreamRefMsg.Cancel(st.streamId, "消费方阶段已停止"))
                if (!st.subscription.cancelledFlag.load()) {
                    try {
                        st.subscriber.onErrorF(StreamRefPeerTerminatedException("消费方阶段已停止"))
                    } catch (_: Exception) {}
                }
            }
            consumerCleanup(st, ctx)
            TypedBehaviors.same<StreamRefMsg>()
        case _ => TypedBehaviors.same<StreamRefMsg>()
    }
}
