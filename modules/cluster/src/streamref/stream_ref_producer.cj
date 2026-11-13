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
import cactor_core.actor.{ActorRef}
import cactor_core.typed.*
import cactor_remote.{RemoteActorRef}

const KEY_SUBSCRIPTION_TIMEOUT: String = "streamref-subscription-timeout"
const KEY_IDLE_TIMEOUT: String = "streamref-idle-timeout"
const KEY_TERMINATION_DEADLINE: String = "streamref-termination-deadline"

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
public class StreamProducer<T> {
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
    var terminal: Bool = false
    var cleaned: Bool = false

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
        case StreamRefMsg.SubscribeLocal(sid, ref, ver) =>
            onSubscribe(st, ctx, sid, StreamPeer(ref, None<RemoteActorRef>, ref.pathString()), ver)
        case StreamRefMsg.Demand(sid, upTo) =>
            if (sid == st.streamId && st.consumer.isSome() && upTo > st.grantedUpTo) {
                st.grantedUpTo = upTo
                st.shared.granted.store(upTo)
                return flush(st, ctx)
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.Offer(value) =>
            st.pending.add(value)
            flush(st, ctx)
        case StreamRefMsg.ProducerComplete =>
            st.completing = true
            flush(st, ctx)
        case StreamRefMsg.ProducerFail(reason) =>
            if (let Some(p) <- st.consumer) {
                p.tellQuietly(StreamRefMsg.Failed(st.streamId, reason))
            }
            terminate(st, ctx, StreamTermination.Failed(reason))
        case StreamRefMsg.Cancel(sid, reason) =>
            if (sid == st.streamId && st.consumer.isSome()) {
                return terminate(st, ctx, StreamTermination.Cancelled(reason))
            }
            TypedBehaviors.same<StreamRefMsg>()
        case StreamRefMsg.SubscriptionTimeout =>
            if (st.consumer.isNone()) {
                return terminate(st, ctx, StreamTermination.Failed(
                    "subscription timeout：${st.settings.subscriptionTimeout} 内没有消费方挂接"))
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
    if (ver != STREAM_REF_PROTOCOL_VERSION) {
        peer.tellQuietly(StreamRefMsg.Rejected(sid, "协议版本不符：生产方 ${STREAM_REF_PROTOCOL_VERSION}，消费方 ${ver}"))
        return TypedBehaviors.same<StreamRefMsg>()
    }
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
            // 消费方之前已完成/失败的请求在挂接后立即生效（completing 但还没人挂接的情况）
            flush(st, ctx)
    }
}

/** 在需求内把排队元素发上线；完成请求且队列清空时发 Completed */
func flush(st: ProducerStageState, ctx: TypedActorContext<StreamRefMsg>): TypedBehavior<StreamRefMsg> {
    let peer = match (st.consumer) {
        case Some(p) => p
        case None => return TypedBehaviors.same<StreamRefMsg>()
    }
    var released: Int64 = 0
    while (!st.pending.isEmpty() && st.sentSeq < st.grantedUpTo) {
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
