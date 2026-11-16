/*
 * CActor Cluster.StreamRef - 生产方：StreamProducer<T> 句柄 + 生产方阶段 Actor
 *
 * 内存上界：
 * - 句柄侧：offer 先在共享计数里预留一个槽位，满 bufferSize 立即返回 Backpressured（非阻塞），
 *   所以"offer 了但还没发到线上"的元素永远 ≤ bufferSize；
 * - 线上：阶段只发送序号 ≤ 消费方累计需求的元素（sentSeq ≤ grantedUpTo 由构造保证），
 *   消费方的需求又不超过"已交付 + 预取窗口"。
 *
 * 取消要能真正中止上游工作（LLM 调用）：termination() 恰好完成一次，取消/消费方终止都会完成它，
 * 之后 offer 返回 Closed、whenReady 完成为 false。
 *
 * 阶段状态放在 setup 里的一个状态对象中，只在阶段 Actor 线程读写（与 Akka GraphStage 相同）：
 * 有界队列 + 一组计数，用"返回新行为"表达会让每条元素都复制一次队列，得不偿失。
 * 与句柄共享的部分（槽位、终止、统计）放在 ProducerShared，加锁或用原子量。
 */
package cactor_cluster.streamref

import std.collection.*
import std.sync.*
import std.time.*
import cactor_core.actor.{ActorRef}
import cactor_core.typed.*
import cactor_remote.{RemoteActorRef}

const KEY_SUBSCRIPTION_TIMEOUT: String = "streamref-subscription-timeout"
const KEY_IDLE_TIMEOUT: String = "streamref-idle-timeout"
const KEY_TERMINATION_DEADLINE: String = "streamref-termination-deadline"
const KEY_PRODUCER_RECONNECT: String = "streamref-producer-reconnect"
const KEY_COMPLETION_ACK: String = "streamref-completion-ack"

/** 句柄与阶段共享的状态 */
class ProducerShared {
    let lock = Mutex()
    let capacity: Int64
    var buffered: Int64 = 0
    var closed: Bool = false
    var waiters = ArrayList<TypedPromise<Bool>>()
    let terminationPromise = TypedPromise<StreamTermination>()
    let sent = AtomicInt64(0)
    let granted = AtomicInt64(0)
    let maxOutstanding = AtomicInt64(0)
    let attached = AtomicBool(false)
    /** 可靠模式：已发上线但未确认的元素数（诊断/测试用；恒 ≤ capacity） */
    let retainedCount = AtomicInt64(0)

    init(capacity: Int64) {
        this.capacity = capacity
    }

    func tryReserve(): OfferResult {
        synchronized(lock) {
            if (closed) {
                OfferResult.Closed
            } else if (buffered >= capacity) {
                OfferResult.Backpressured
            } else {
                buffered++
                OfferResult.Enqueued
            }
        }
    }

    /** 阶段把 n 个元素发上线（或丢弃）后归还槽位，唤醒等待者 */
    func release(n: Int64): Unit {
        let ready = synchronized(lock) {
            buffered -= n
            if (buffered < capacity && !waiters.isEmpty()) {
                let w = waiters
                waiters = ArrayList<TypedPromise<Bool>>()
                w
            } else {
                ArrayList<TypedPromise<Bool>>()
            }
        }
        for (p in ready) {
            p.trySuccess(true)
        }
    }

    func whenReady(): TypedPromise<Bool> {
        let p = TypedPromise<Bool>()
        let now = synchronized(lock) {
            if (closed) {
                Some(false)
            } else if (buffered < capacity) {
                Some(true)
            } else {
                waiters.add(p)
                None<Bool>
            }
        }
        if (let Some(v) <- now) {
            p.trySuccess(v)
        }
        p
    }

    /** 恰好一次：返回是否由本次调用关闭 */
    func close(t: StreamTermination): Bool {
        let w = synchronized(lock) {
            if (closed) {
                return false
            }
            closed = true
            let w = waiters
            waiters = ArrayList<TypedPromise<Bool>>()
            w
        }
        for (p in w) {
            p.trySuccess(false)
        }
        terminationPromise.trySuccess(t)
        true
    }

    func isClosed(): Bool {
        synchronized(lock) {
            closed
        }
    }

    func bufferedCount(): Int64 {
        synchronized(lock) {
            buffered
        }
    }
}

/**
 * 可写入的流句柄（SourceRef 的 StreamProducer 与 SinkRef 的 SinkRefProducer 共用）。
 * Reactive Streams 适配器只需要这三件事：投递、等待空位、观测结局。
 */
public interface StreamWriter<T> {
    /** 非阻塞投递：Enqueued / Backpressured（缓冲满）/ Closed（流已结束） */
    func offer(element: T): OfferResult

    /** 有空位时完成为 true；流已结束时完成为 false */
    func whenReady(): TypedPromise<Bool>

    /** 流的结局，恰好完成一次 */
    func termination(): TypedPromise<StreamTermination>

    /** 上游正常结束（已投递的元素全部发出后发送 Completed） */
    func complete(): Unit

    /** 上游失败/立即失败（未发出的元素丢弃） */
    func fail(reason: String): Unit
}

/**
 * 生产方句柄（线程安全，可以在任意线程/Actor 中使用）。
 *
 * 典型用法（Actor 内逐个生成 token）：
 * ```
 * match (producer.offer(token)) {
 *     case OfferResult.Enqueued => ctx.self << Gen(i + 1)
 *     case OfferResult.Backpressured => ctx.pipeToSelf(producer.whenReady(), { o => if (o.toOption() ?? false) { Gen(i) } else { Abort } })
 *     case OfferResult.Closed => ()   // 消费方取消：中止 LLM 调用
 * }
 * ```
 */
public class StreamProducer<T> <: StreamWriter<T> {
    let shared: ProducerShared
    let stage: TypedActorRef<StreamRefMsg>

    init(shared: ProducerShared, stage: TypedActorRef<StreamRefMsg>) {
        this.shared = shared
        this.stage = stage
    }

    /** 非阻塞投递：Enqueued / Backpressured（缓冲满）/ Closed（流已结束） */
    public func offer(element: T): OfferResult {
        let r = shared.tryReserve()
        if (r.isEnqueued()) {
            stage.tell(StreamRefMsg.Offer(element))
        }
        r
    }

    /** 有空位时完成为 true；流已结束时完成为 false（不阻塞线程，Actor 内配合 pipeToSelf 使用） */
    public func whenReady(): TypedPromise<Bool> {
        shared.whenReady()
    }

    /** 已投递的元素全部发出后发送 Completed */
    public func complete(): Unit {
        stage.tell(StreamRefMsg.ProducerComplete)
    }

    /** 立即以 Failed(reason) 结束（未发出的元素丢弃） */
    public func fail(reason: String): Unit {
        stage.tell(StreamRefMsg.ProducerFail(reason))
    }

    /** 流的结局，恰好完成一次：Completed / Cancelled（消费方取消或终止）/ Failed */
    public func termination(): TypedPromise<StreamTermination> {
        shared.terminationPromise
    }

    public func isClosed(): Bool {
        shared.isClosed()
    }

    /** 是否已有消费方挂接 */
    public func isAttached(): Bool {
        shared.attached.load()
    }

    /** 已发上线的元素数（= 最后序号） */
    public func sentCount(): Int64 {
        shared.sent.load()
    }

    /** 消费方授予的累计需求（允许发送的最大序号） */
    public func grantedDemand(): Int64 {
        shared.granted.load()
    }

    /** 已 offer、尚未发上线的元素数（≤ bufferSize） */
    public func bufferedCount(): Int64 {
        shared.bufferedCount()
    }

    /** 发送时观测到的最大超发量 max(seq - grantedUpTo)：构造上恒为 0，测试据此断言从不超出需求 */
    public func maxOverGrant(): Int64 {
        shared.maxOutstanding.load()
    }

    /** 可靠模式：已发上线但消费方尚未确认的元素数（受 bufferSize 约束；非可靠模式恒为 0） */
    public func retainedCount(): Int64 {
        shared.retainedCount.load()
    }

    /** 阶段 Actor 的引用（诊断、手工停止用） */
    public func stageRef(): TypedActorRef<StreamRefMsg> {
        stage
    }
}

/** 生产方阶段的可变状态：只在阶段 Actor 线程访问 */
class ProducerStageState {
    let streamId: String
    let exposedName: String
    let settings: StreamRefSettings
    let shared: ProducerShared
    let env: StreamRefEnv
    let owner: Option<ActorRef>
    var consumer: Option<StreamPeer> = None<StreamPeer>
    let pending = ArrayList<Any>()
    var sentSeq: Int64 = 0
    var grantedUpTo: Int64 = 0
    var completing: Bool = false
    // Publisher 可能在 SourceRef 返回前失败；保留首个原因至首次挂接或原订阅期限，避免只剩死亡通知。
    var pendingFailure: Option<String> = None<String>
    var terminal: Bool = false
    var cleaned: Bool = false
    // ---------- 可靠模式（跨节点 + reliable=true 时才生效；只在阶段线程访问） ----------
    /** 已发上线但消费方尚未确认的元素（序号单调），受 capacity 约束，禁止无界增长 */
    let retained = ArrayList<(Int64, Any)>()
    /** 消费方确认到的最大连续序号 */
    var lastAck: Int64 = 0
    /** 消费方在首次可靠挂接时带来的恢复令牌；恢复挂接必须携带同一个值 */
    var resumeToken: String = ""
    /** 是否已发出 Completed（发出后等 AckCompleted 再真正终止，避免终止信号在断连时丢失） */
    var completedSent: Bool = false
    /** 本阶段是否因消费方终止而进入重连等待 */
    var reconnecting: Bool = false
    var reconnectDeadline: Option<MonoTime> = None<MonoTime>
    /** 收到过对端的 Terminated（恢复挂接时才需要重新 watch，避免重复登记） */
    var peerDeathSeen: Bool = false
    /** 对端是远程且设置了 reliable：只有此时才走保留/确认/重发路径 */
    var reliableActive: Bool = false

    init(streamId: String, exposedName: String, settings: StreamRefSettings, shared: ProducerShared, env: StreamRefEnv,
        owner: Option<ActorRef>) {
        this.streamId = streamId
        this.exposedName = exposedName
        this.settings = settings
        this.shared = shared
        this.env = env
        this.owner = owner
    }
}

func producerStage(st: ProducerStageState): TypedBehavior<StreamRefMsg> {
    TypedBehaviors.setup<StreamRefMsg>({ ctx =>
        ctx.timers.startSingleTimer(KEY_SUBSCRIPTION_TIMEOUT, StreamRefMsg.SubscriptionTimeout, st.settings.subscriptionTimeout)
        if (let Some(o) <- st.owner) {
            // 属主（生成元素的 Actor）停止 = 生产方失败：不能让消费方一直等下去
            if (let Some(c) <- ctx.toClassic()) {
                c.watch(o)
            }
        }
        TypedBehaviors.receive<StreamRefMsg>({ c, msg => producerOnMessage(st, c, msg) })
            .receiveSignal({ c, sig => producerOnSignal(st, c, sig) })
    })
}

func producerOnMessage(st: ProducerStageState, ctx: TypedActorContext<StreamRefMsg>, msg: StreamRefMsg): TypedBehavior<StreamRefMsg> {
    if (st.terminal) {
        // 终止后（理论上已停止）迟到的消息：offer 预留的槽位已无意义，只需不再处理
        return TypedBehaviors.same<StreamRefMsg>()
    }
    match (msg) {
        case StreamRefMsg.Subscribe(sid, location, ver) =>
            match (st.env.resolveRemote(location)) {
                case Some(peer) => onSubscribe(st, ctx, sid, peer, ver)
                case None =>
                    streamRefLog.warn("StreamRef ${st.streamId} 收到无法解析的订阅方位置 ${location}，忽略")
                    TypedBehaviors.same<StreamRefMsg>()
            }
        case StreamRefMsg.SubscribeReliable(sid, location, ver, token, lastAck) =>
            if (!st.settings.reliable) {
                // 明确拒绝而不是半挂接：旧生产方即使解码了新帧，也不会静默按不可靠方式运行
                match (st.env.resolveRemote(location)) {
                    case Some(peer) => peer.tellQuietly(StreamRefMsg.Rejected(sid, "生产方未启用可靠模式（reliable=false）"))
                    case None => ()
                }
                return TypedBehaviors.same<StreamRefMsg>()
            }
            match (st.env.resolveRemote(location)) {
                case Some(peer) => onSubscribeReliable(st, ctx, sid, peer, ver, token, lastAck)
                case None =>
                    streamRefLog.warn("StreamRef ${st.streamId} 收到无法解析的可靠订阅方位置 ${location}，忽略")
                    TypedBehaviors.same<StreamRefMsg>()
            }
        case StreamRefMsg.SubscribeLocal(sid, ref, ver) =>
            onSubscribe(st, ctx, sid, StreamPeer(ref, None<RemoteActorRef>, ref.pathString()), ver)
        case StreamRefMsg.Demand(sid, upTo) =>
            if (sid == st.streamId && st.consumer.isSome() && upTo > st.grantedUpTo) {
                st.grantedUpTo = upTo
                st.shared.granted.store(upTo)
                return flush(st, ctx)
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.Ack(sid, seq) =>
            if (st.reliableActive && sid == st.streamId && st.consumer.isSome()) {
                if (seq > st.lastAck) {
                    st.lastAck = seq
                    dropAcked(st)
                }
                // 确认释放了保留名额：被信用窗口挡住的新元素可以继续发
                return flush(st, ctx)
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.AckCompleted(sid) =>
            if (st.reliableActive && sid == st.streamId && st.completedSent) {
                return terminate(st, ctx, StreamTermination.Completed)
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.Offer(value) =>
            if (st.pendingFailure.isSome()) {
                st.shared.release(1)
                return TypedBehaviors.same<StreamRefMsg>()
            }
            st.pending.add(value)
            flush(st, ctx)
        case StreamRefMsg.ProducerComplete =>
            if (st.pendingFailure.isSome()) { return TypedBehaviors.same<StreamRefMsg>() }
            st.completing = true
            flush(st, ctx)
        case StreamRefMsg.ProducerFail(reason) =>
            if (st.pendingFailure.isSome()) { return TypedBehaviors.same<StreamRefMsg>() }
            if (let Some(p) <- st.consumer) {
                p.tellQuietly(StreamRefMsg.Failed(st.streamId, reason))
                return terminate(st, ctx, StreamTermination.Failed(reason))
            }
            st.pendingFailure = Some(reason)
            let dropped = st.pending.size
            st.pending.clear()
            // 先关闭准入再归还槽位，不能在失败时唤醒上游继续请求。
            st.shared.close(StreamTermination.Failed(reason))
            st.shared.release(dropped)
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.Cancel(sid, reason) =>
            if (sid == st.streamId && st.consumer.isSome()) {
                return terminate(st, ctx, StreamTermination.Cancelled(reason))
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.SubscriptionTimeout =>
            if (st.consumer.isNone()) {
                let reason = st.pendingFailure ?? "subscription timeout：${st.settings.subscriptionTimeout} 内没有消费方挂接"
                return terminate(st, ctx, StreamTermination.Failed(reason))
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.ReconnectTick =>
            if (st.reliableActive && st.reconnecting) {
                match (st.reconnectDeadline) {
                    case Some(deadline) where MonoTime.now() >= deadline =>
                        return terminate(st, ctx, StreamTermination.Failed(
                            "重连窗口 ${st.settings.reconnectWindow} 内消费方没有重新挂接"))
                    case _ =>
                        // 继续等：消费方会带着恢复令牌重试，期间保留元素不释放
                        ctx.timers.startSingleTimer(KEY_PRODUCER_RECONNECT, StreamRefMsg.ReconnectTick,
                            st.settings.recoveryRetryInterval())
                }
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.CompletionAckTimeout =>
            // Completed 已发出但 AckCompleted 没回来（对端可能已收到；再等就会永久占住阶段）：
            // 按完成结束，消费方若真没收到会在自己的重连窗口内失败。
            if (st.reliableActive && st.completedSent) {
                return terminate(st, ctx, StreamTermination.Completed)
            }
            TypedBehaviors.same<StreamRefMsg>()
        case _ =>
            streamRefLog.debug("StreamRef 生产方 ${st.streamId} 忽略消息 ${msg.streamIdOrEmpty()}")
            TypedBehaviors.same<StreamRefMsg>()
    }
}

func onSubscribe(st: ProducerStageState, ctx: TypedActorContext<StreamRefMsg>, sid: String, peer: StreamPeer, ver: Int64): TypedBehavior<StreamRefMsg> {
    if (sid != st.streamId) {
        peer.tellQuietly(StreamRefMsg.Rejected(sid, "流标识不符：本阶段是 ${st.streamId}"))
        return TypedBehaviors.same<StreamRefMsg>()
    }
    if (ver == STREAM_REF_PROTOCOL_VERSION_RELIABLE) {
        // 版本 2 的首次探测：可靠生产方回 ReliableProbeOk，消费方再发带令牌的 SubscribeReliable。
        // 非可靠生产方（包括旧节点）走下面同一分支：明确 Rejected，而不是让新消费方干等超时。
        if (st.settings.reliable && peer.isRemote()) {
            peer.tellQuietly(StreamRefMsg.ReliableProbeOk(sid))
        } else {
            peer.tellQuietly(StreamRefMsg.Rejected(sid,
                "协议版本不符：生产方 ${STREAM_REF_PROTOCOL_VERSION}，消费方 ${ver}（可靠模式需要生产方 reliable=true）"))
        }
        return TypedBehaviors.same<StreamRefMsg>()
    }
    if (st.settings.reliable && peer.isRemote()) {
        // 可靠生产方不接受版本 1 的挂接：没有 ACK/恢复令牌就无法做重发，明确拒绝好过半可靠
        peer.tellQuietly(StreamRefMsg.Rejected(sid, "可靠模式需要协议版本 ${STREAM_REF_PROTOCOL_VERSION_RELIABLE}（SubscribeReliable）"))
        return TypedBehaviors.same<StreamRefMsg>()
    }
    if (ver != STREAM_REF_PROTOCOL_VERSION) {
        peer.tellQuietly(StreamRefMsg.Rejected(sid, "协议版本不符：生产方 ${STREAM_REF_PROTOCOL_VERSION}，消费方 ${ver}"))
        return TypedBehaviors.same<StreamRefMsg>()
    }
    attachFirst(st, ctx, sid, peer)
}

/** 首次挂接的公共部分（本地与不可靠远程共用）：单次挂接 + 幂等确认 + 立即 flush */
func attachFirst(st: ProducerStageState, ctx: TypedActorContext<StreamRefMsg>, sid: String, peer: StreamPeer): TypedBehavior<StreamRefMsg> {
    match (st.consumer) {
        case Some(existing) =>
            if (existing.key == peer.key) {
                // 同一个订阅者重发 Subscribe（例如它以为前一条丢了）：幂等确认
                existing.tellQuietly(StreamRefMsg.OnSubscribe(sid))
            } else {
                // 单次挂接：SourceRef 只能被物化一次（与 Akka 相同），第二个订阅者明确拒绝，不影响第一个
                peer.tellQuietly(StreamRefMsg.Rejected(sid, "SourceRef 只能挂接一个订阅者，已被 ${existing.key} 挂接"))
            }
            TypedBehaviors.same<StreamRefMsg>()
        case None =>
            st.consumer = Some(peer)
            st.shared.attached.store(true)
            ctx.timers.cancel(KEY_SUBSCRIPTION_TIMEOUT)
            st.env.watchPeer(ctx, peer)
            peer.tellQuietly(StreamRefMsg.OnSubscribe(sid))
            if (let Some(reason) <- st.pendingFailure) {
                peer.tellQuietly(StreamRefMsg.Failed(sid, reason))
                return terminate(st, ctx, StreamTermination.Failed(reason))
            }
            // 消费方之前已完成的请求在挂接后立即生效。
            flush(st, ctx)
    }
}

/**
 * 可靠挂接 / 恢复挂接（版本 2）。
 *
 * 首次挂接：记住消费方的恢复令牌，按普通挂接开始。
 * 恢复挂接（同一 streamId + 同一 token）：接受新的对端引用（对端可能换了暴露名/连接），
 * 用消费方报告的 lastAck 与生产方自己的 lastAck 取较大值丢弃保留元素，再从 lastAck+1 重发。
 * 重发不受信用窗口限制：这些元素本来就已经发过，只是消费方没确认；保留量不因此增长。
 */
func onSubscribeReliable(st: ProducerStageState, ctx: TypedActorContext<StreamRefMsg>, sid: String, peer: StreamPeer,
    ver: Int64, token: String, claimedAck: Int64): TypedBehavior<StreamRefMsg> {
    if (sid != st.streamId) {
        peer.tellQuietly(StreamRefMsg.Rejected(sid, "流标识不符：本阶段是 ${st.streamId}"))
        return TypedBehaviors.same<StreamRefMsg>()
    }
    if (ver != STREAM_REF_PROTOCOL_VERSION_RELIABLE) {
        peer.tellQuietly(StreamRefMsg.Rejected(sid, "协议版本不符：可靠生产方 ${STREAM_REF_PROTOCOL_VERSION_RELIABLE}，消费方 ${ver}"))
        return TypedBehaviors.same<StreamRefMsg>()
    }
    if (token.isEmpty()) {
        peer.tellQuietly(StreamRefMsg.Rejected(sid, "可靠挂接缺少恢复令牌"))
        return TypedBehaviors.same<StreamRefMsg>()
    }
    match (st.consumer) {
        case None =>
            st.consumer = Some(peer)
            st.reliableActive = true
            st.resumeToken = token
            st.lastAck = 0
            st.shared.attached.store(true)
            ctx.timers.cancel(KEY_SUBSCRIPTION_TIMEOUT)
            st.env.watchPeer(ctx, peer)
            peer.tellQuietly(StreamRefMsg.OnSubscribe(sid))
            if (let Some(reason) <- st.pendingFailure) {
                peer.tellQuietly(StreamRefMsg.Failed(sid, reason))
                return terminate(st, ctx, StreamTermination.Failed(reason))
            }
            flush(st, ctx)
        case Some(existing) =>
            if (token != st.resumeToken) {
                peer.tellQuietly(StreamRefMsg.Rejected(sid, "恢复令牌不符：不是同一条流"))
                return TypedBehaviors.same<StreamRefMsg>()
            }
            // 恢复：接受新引用，取消重连等待，按确认序号重发
            st.consumer = Some(peer)
            st.reconnecting = false
            st.reconnectDeadline = None<MonoTime>
            ctx.timers.cancel(KEY_PRODUCER_RECONNECT)
            if (st.peerDeathSeen) {
                // 之前收到过 DEATH，远程监视登记已被移除；恢复后重新登记
                st.peerDeathSeen = false
                st.env.watchPeer(ctx, peer)
            }
            if (claimedAck > st.lastAck) {
                st.lastAck = claimedAck
                dropAcked(st)
            }
            peer.tellQuietly(StreamRefMsg.OnSubscribe(sid))
            if (let Some(reason) <- st.pendingFailure) {
                peer.tellQuietly(StreamRefMsg.Failed(sid, reason))
                return terminate(st, ctx, StreamTermination.Failed(reason))
            }
            resendRetained(st, ctx)
    }
}

/** 丢弃 ≤ lastAck 的保留元素 */
func dropAcked(st: ProducerStageState): Unit {
    var dropped: Int64 = 0
    while (!st.retained.isEmpty()) {
        let (seq, _) = st.retained[0]
        if (seq <= st.lastAck) {
            st.retained.remove(at: 0)
            dropped++
        } else {
            break
        }
    }
    if (dropped > 0) {
        st.shared.retainedCount.store(Int64(st.retained.size))
    }
}

/** 把消费方尚未确认的元素按原序号重发；编码失败即大声失败 */
func resendRetained(st: ProducerStageState, ctx: TypedActorContext<StreamRefMsg>): TypedBehavior<StreamRefMsg> {
    let peer = match (st.consumer) {
        case Some(p) => p
        case None => return TypedBehaviors.same<StreamRefMsg>()
    }
    for ((seq, value) in st.retained) {
        if (seq <= st.lastAck) {
            continue
        }
        try {
            peer.ref.tell(StreamRefMsg.Element(st.streamId, seq, value))
        } catch (e: Exception) {
            let reason = "恢复时重发元素 #${seq} 失败：${e.message}"
            peer.tellQuietly(StreamRefMsg.Failed(st.streamId, reason))
            return terminate(st, ctx, StreamTermination.Failed(reason))
        }
    }
    if (st.completing && st.pending.isEmpty() && st.completedSent) {
        // Completed 也可能丢在断连期间：重发（消费方按序号与幂等处理）
        peer.tellQuietly(StreamRefMsg.Completed(st.streamId, st.sentSeq))
    }
    TypedBehaviors.same<StreamRefMsg>()
}

/** 在需求内把排队元素发上线；完成请求且队列清空时发 Completed */
func flush(st: ProducerStageState, ctx: TypedActorContext<StreamRefMsg>): TypedBehavior<StreamRefMsg> {
    if (st.reconnecting) {
        // 对端暂时不可达：不要往旧连接里灌元素，等恢复挂接后用 resendRetained 重发
        return TypedBehaviors.same<StreamRefMsg>()
    }
    let peer = match (st.consumer) {
        case Some(p) => p
        case None => return TypedBehaviors.same<StreamRefMsg>()
    }
    var released: Int64 = 0
    while (!st.pending.isEmpty() && st.sentSeq < st.grantedUpTo) {
        // 可靠模式：未确认元素也算在窗口里（sentSeq - lastAck = 保留量），
        // 这样即使 ACK 因单向断连全部丢失，保留量也不会超过 bufferSize。
        if (st.reliableActive && st.sentSeq - st.lastAck >= st.shared.capacity) {
            break
        }
        let value = st.pending.remove(at: 0)
        released++
        let seq = st.sentSeq + 1
        try {
            peer.ref.tell(StreamRefMsg.Element(st.streamId, seq, value))
        } catch (e: Exception) {
            // 元素编码失败（类型没有注册编解码器）或对端读不了：在发送侧大声失败，并告诉消费方原因
            st.shared.release(released)
            let reason = "元素 #${seq} 发送失败：${e.message}"
            peer.tellQuietly(StreamRefMsg.Failed(st.streamId, reason))
            return terminate(st, ctx, StreamTermination.Failed(reason))
        }
        st.sentSeq = seq
        if (st.reliableActive) {
            // 保留到消费方累计确认（含已完成但未确认的尾部元素）
            st.retained.add((seq, value))
            st.shared.retainedCount.store(Int64(st.retained.size))
        }
        st.shared.sent.store(seq)
        // 记录发送时的最大超发量（构造上恒为 ≤ 0；测试据此断言从不超出需求）
        let over = seq - st.grantedUpTo
        if (over > st.shared.maxOutstanding.load()) {
            st.shared.maxOutstanding.store(over)
        }
    }
    if (released > 0) {
        st.shared.release(released)
    }
    if (st.completing && st.pending.isEmpty()) {
        if (st.reliableActive) {
            if (!st.completedSent) {
                peer.tellQuietly(StreamRefMsg.Completed(st.streamId, st.sentSeq))
                st.completedSent = true
                // 等消费方确认收到 Completed；期间保持阶段存活以便断连后重发
                ctx.timers.startSingleTimer(KEY_COMPLETION_ACK, StreamRefMsg.CompletionAckTimeout,
                    st.settings.reconnectWindow)
            }
            return TypedBehaviors.same<StreamRefMsg>()
        }
        peer.tellQuietly(StreamRefMsg.Completed(st.streamId, st.sentSeq))
        return terminate(st, ctx, StreamTermination.Completed)
    }
    TypedBehaviors.same<StreamRefMsg>()
}

func terminate(st: ProducerStageState, ctx: TypedActorContext<StreamRefMsg>, t: StreamTermination): TypedBehavior<StreamRefMsg> {
    st.terminal = true
    let dropped = st.pending.size
    st.pending.clear()
    if (dropped > 0) {
        st.shared.release(dropped)
    }
    if (st.reliableActive) {
        st.retained.clear()
        st.shared.retainedCount.store(0)
    }
    st.shared.close(t)
    producerCleanup(st, ctx)
    TypedBehaviors.stopped<StreamRefMsg>()
}

func producerCleanup(st: ProducerStageState, ctx: TypedActorContext<StreamRefMsg>): Unit {
    if (st.cleaned) {
        return
    }
    st.cleaned = true
    ctx.timers.cancelAll()
    st.env.unexpose(st.exposedName)
    if (let Some(p) <- st.consumer) {
        st.env.unwatchPeer(ctx, p)
    }
}

func producerOnSignal(st: ProducerStageState, ctx: TypedActorContext<StreamRefMsg>, sig: TypedSignal): TypedBehavior<StreamRefMsg> {
    match (sig) {
        case t: TypedTerminated =>
            if (st.terminal) {
                return TypedBehaviors.same<StreamRefMsg>()
            }
            if (isPeerRef(st.consumer, t.ref)) {
                if (st.reliableActive) {
                    // 可靠模式：消费方阶段/节点暂时不可达不等于取消。保留元素，等它带恢复令牌回来；
                    // 超过重连窗口才结束（ReconnectTick 负责）。
                    st.peerDeathSeen = true
                    if (!st.reconnecting) {
                        st.reconnecting = true
                        st.reconnectDeadline = Some(MonoTime.now() + st.settings.reconnectWindow)
                        ctx.timers.startSingleTimer(KEY_PRODUCER_RECONNECT, StreamRefMsg.ReconnectTick,
                            st.settings.recoveryRetryInterval())
                    }
                    return TypedBehaviors.same<StreamRefMsg>()
                }
                // 消费方阶段或其节点没了：等同取消，让上游（LLM 调用）停下
                return terminate(st, ctx, StreamTermination.Cancelled("消费方终止：${t.ref.path.toString()}"))
            }
            if (let Some(o) <- st.owner) {
                if (o.path.toString() == t.ref.path.toString()) {
                    let reason = "生产方属主终止：${o.path.toString()}"
                    if (let Some(p) <- st.consumer) {
                        p.tellQuietly(StreamRefMsg.Failed(st.streamId, reason))
                    }
                    return terminate(st, ctx, StreamTermination.Failed(reason))
                }
            }
            TypedBehaviors.same<StreamRefMsg>()
        case _: PostStop =>
            // 被外部停止（system.stop、系统关闭）而没有走终止路径：仍要恰好一个终止信号
            if (!st.terminal) {
                st.terminal = true
                let reason = "生产方阶段已停止"
                if (let Some(p) <- st.consumer) {
                    p.tellQuietly(StreamRefMsg.Failed(st.streamId, reason))
                }
                let dropped = st.pending.size
                st.pending.clear()
                st.shared.release(dropped)
                st.shared.close(StreamTermination.Failed(reason))
            }
            producerCleanup(st, ctx)
            TypedBehaviors.same<StreamRefMsg>()
        case _ => TypedBehaviors.same<StreamRefMsg>()
    }
}
