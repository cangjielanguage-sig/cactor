/*
 * CActor Cluster.StreamRef - SinkRef 的两个阶段 Actor（DA7，对标 Akka SinkRef）
 *
 * SinkRef 把方向反过来：消费方节点先暴露一个"接收阶段"，生产方节点创建生产阶段后发 SinkAttach。
 * 挂接完成后线上协议与 SourceRef 方向完全一致（Demand / Element / Completed / Cancel），
 * 因此信用、序号校验、取消传播全部复用 consumerStage 的既有逻辑，这里只写"方向反转"的部分：
 *
 *   SinkStage（消费方节点，暴露自己）        SinkProducerStage（生产方节点）
 *     | <-- SinkAttach(流, 生产方位置) ------ |   生产方先建阶段再回话（与 SourceRef 的 Subscribe 对称）
 *     | -------- OnSubscribe(流) ----------> |
 *     | -------- Demand(累计上限) ---------> |   信用由消费方授予
 *     | <-------- Element(序号, 元素) ------- |
 *     | <-- Completed / Failed / Cancel ---- |   与 SourceRef 相同
 *
 * 为什么不让 producerStage 直接处理 SinkAttach：既有 producerStage 的挂接语义是"收到 Subscribe 才确认"，
 * SinkRef 需要的是消费方阶段被动等生产方来回话。单独的薄阶段把 SinkAttach 翻译成初始的
 * OnSubscribe + 后续的既有消费行为，consumerStage 主体零改动。
 */
package cactor_cluster.streamref

import std.sync.*
import cactor_core.actor.{ActorRef}
import cactor_core.typed.*
import cactor_remote.{RemoteActorRef}

// ================= 消费方节点：SinkStage =================

/** SinkStage 的可变状态：只在阶段 Actor 线程访问 */
class SinkStageState {
    let streamId: String
    let exposedName: String
    let settings: StreamRefSettings
    let env: StreamRefEnv
    let subscriber: AnySubscriber
    let subscription: StreamRefSubscription
    var attached: Bool = false

    init(streamId: String, exposedName: String, settings: StreamRefSettings, env: StreamRefEnv,
        subscriber: AnySubscriber, subscription: StreamRefSubscription) {
        this.streamId = streamId
        this.exposedName = exposedName
        this.settings = settings
        this.env = env
        this.subscriber = subscriber
        this.subscription = subscription
    }
}

/**
 * SinkRef 的消费方阶段。收到生产方的 SinkAttach 后：
 * - 校验流标识与协议版本，回 OnSubscribe 确认（与 SourceRef 生产方的挂接确认对称）；
 * - 先调 subscriber.onSubscribe（RS 1.9：onSubscribe 必须是第一个信号），
 *   然后把行为切换成 consumerStage 的既有处理——Demand / Element / Completed / Cancel 全部复用。
 * sinkTimeout 内没有生产方来挂接就失败（对端没有远程层 / 不认识协议 / SinkAttach 丢失）。
 */
func sinkStage(st: SinkStageState): TypedBehavior<StreamRefMsg> {
    TypedBehaviors.setup<StreamRefMsg>({ ctx =>
        if (!st.exposedName.isEmpty()) {
            st.env.expose(st.exposedName, ctx.self)
        }
        // 用 sinkTimeout 而不是 subscriptionTimeout：生产方要先建好自己的阶段再回话，多一跳
        ctx.timers.startSingleTimer(KEY_SUBSCRIPTION_TIMEOUT, StreamRefMsg.SubscriptionTimeout,
            st.settings.sinkTimeout)
        TypedBehaviors.receive<StreamRefMsg>({ c, msg =>
            match (msg) {
                case StreamRefMsg.SinkAttach(sid, location, ver) =>
                    if (sid != st.streamId) {
                        // 流标识不符：忽略（可能别的流的迟到帧），本阶段继续等真正的 SinkAttach
                        streamRefLog.warn("SinkRef ${st.streamId} 收到其他流 ${sid} 的 SinkAttach，忽略")
                        return TypedBehaviors.same<StreamRefMsg>()
                    }
                    if (ver != STREAM_REF_PROTOCOL_VERSION) {
                        return failSinkStage(st, c, StreamRefRejectedException(
                            "SinkAttach 协议版本不符：本端 ${STREAM_REF_PROTOCOL_VERSION}，生产方 ${ver}"))
                    }
                    if (!st.attached) {
                        st.attached = true
                        ctx.timers.cancel(KEY_SUBSCRIPTION_TIMEOUT)
                        let peer = match (st.env.resolveRemote(location, reliable: st.settings.reliable)) {
                            case Some(p) => p
                            case None => return failSinkStage(st, c, StreamRefRejectedException(
                                "SinkRef ${st.streamId} 无法解析生产方位置 ${location}"))
                        }
                        // 切换成既有消费行为：收到 OnSubscribe 确认前先按已订阅处理（本消息就是确认的开始）
                        let cst = ConsumerStageState(st.streamId, "", st.settings, st.env, peer,
                            st.subscriber, st.subscription)
                        cst.subscribed = true
                        st.subscription.subscribed.store(true)
                        let behavior = TypedBehaviors.receive<StreamRefMsg>({ cc, m =>
                            consumerOnMessage(cst, cc, m)
                        }).receiveSignal({ cc, sig => consumerOnSignal(cst, cc, sig) })
                        st.env.watchPeer(ctx, cst.producer)
                        peer.tellQuietly(StreamRefMsg.OnSubscribe(st.streamId))
                        try {
                            st.subscriber.onSubscribeF(st.subscription)
                        } catch (e: Exception) {
                            return failSinkStage(st, c, StreamRefSubscriberException(
                                "onSubscribe 抛出异常：${e.message}"))
                        }
                        // 消费方可能已在 onSubscribe 里 request：立即把需求送出去
                        sendDemand(cst, c)
                        return behavior
                    }
                    TypedBehaviors.same<StreamRefMsg>()
                case StreamRefMsg.SubscriptionTimeout =>
                    if (!st.attached) {
                        return failSinkStage(st, c, StreamRefSinkTimeoutException(
                            "sink timeout：${st.settings.sinkTimeout} 内没有生产方挂接 SinkRef ${st.streamId}"))
                    }
                    TypedBehaviors.same<StreamRefMsg>()
                case _ => TypedBehaviors.same<StreamRefMsg>()
            }
        })
    })
}

/** SinkStage 挂接前的失败路径：订阅者收到唯一的 onError，清理暴露与定时器 */
func failSinkStage(st: SinkStageState, ctx: TypedActorContext<StreamRefMsg>, e: StreamRefException): TypedBehavior<StreamRefMsg> {
    st.subscription.terminatedFlag.store(true)
    try {
        st.subscriber.onErrorF(e)
    } catch (ex: Exception) {
        streamRefLog.warn("StreamRef 订阅者 onError 抛出异常：${ex.message}")
    }
    ctx.timers.cancelAll()
    if (!st.exposedName.isEmpty()) {
        st.env.unexpose(st.exposedName)
    }
    TypedBehaviors.stopped<StreamRefMsg>()
}

// ================= 生产方节点：SinkProducerStage =================

/** SinkProducerStage 的可变状态：只在阶段 Actor 线程访问 */
class SinkProducerStageState {
    let streamId: String
    let exposedName: String
    let settings: StreamRefSettings
    let shared: ProducerShared
    let env: StreamRefEnv
    /** SinkRef 的对端：SinkStage（消费方阶段） */
    let sink: StreamPeer
    var pendingFailure: Option<String> = None<String>

    init(streamId: String, exposedName: String, settings: StreamRefSettings, shared: ProducerShared,
        env: StreamRefEnv, sink: StreamPeer) {
        this.streamId = streamId
        this.exposedName = exposedName
        this.settings = settings
        this.shared = shared
        this.env = env
        this.sink = sink
    }
}

/**
 * SinkRef 的生产方阶段：创建后立即向 SinkStage 发 SinkAttach（带上自己的位置让对方回话），
 * 收到 OnSubscribe 确认后把行为切换成既有 producerStage——Offer / Demand / Completed / Cancel
 * 全部复用。SinkStage 在 sinkTimeout 内不确认则失败。
 */
func sinkProducerStage(st: SinkProducerStageState): TypedBehavior<StreamRefMsg> {
    TypedBehaviors.setup<StreamRefMsg>({ ctx =>
        ctx.timers.startSingleTimer(KEY_SUBSCRIPTION_TIMEOUT, StreamRefMsg.SubscriptionTimeout,
            st.settings.sinkTimeout)
        // 先监视对端再发 SinkAttach：对端已不存在时靠 DEATH / 节点不可达失败，而不是干等超时
        st.env.watchPeer(ctx, st.sink)
        let location = st.env.expose(st.exposedName, ctx.self)
        st.sink.tellQuietly(StreamRefMsg.SinkAttach(st.streamId, location, STREAM_REF_PROTOCOL_VERSION))
        TypedBehaviors.receive<StreamRefMsg>({ c, msg =>
            match (msg) {
                case StreamRefMsg.OnSubscribe(sid) =>
                    if (sid == st.streamId) {
                        ctx.timers.cancel(KEY_SUBSCRIPTION_TIMEOUT)
                        // 切换成既有生产行为：pst 从 0 计数，pendingFailure 保留挂接前的失败
                        let pst = ProducerStageState(st.streamId, st.exposedName, st.settings, st.shared,
                            st.env, None<ActorRef>)
                        pst.consumer = Some(st.sink)
                        st.shared.attached.store(true)
                        if (let Some(reason) <- st.pendingFailure) {
                            st.sink.tellQuietly(StreamRefMsg.Failed(st.streamId, reason))
                            return terminate(pst, c, StreamTermination.Failed(reason))
                        }
                        return flush(pst, c)
                    }
                    TypedBehaviors.same<StreamRefMsg>()
                case StreamRefMsg.ProducerFail(reason) =>
                    // 挂接前就失败：保留原因，等确认后带给对端；若已确认则直接终止
                    st.pendingFailure = Some(reason)
                    st.shared.close(StreamTermination.Failed(reason))
                    TypedBehaviors.same<StreamRefMsg>()
                case StreamRefMsg.SubscriptionTimeout =>
                    let reason = "sink attach timeout：${st.settings.sinkTimeout} 内没有收到 SinkRef 对端的挂接确认"
                    st.shared.close(StreamTermination.Failed(reason))
                    TypedBehaviors.stopped<StreamRefMsg>()
                case _ => TypedBehaviors.same<StreamRefMsg>()
            }
        }).receiveSignal({ c, sig =>
            match (sig) {
                case t: TypedTerminated =>
                    if (isPeerRef(Some(st.sink), t.ref)) {
                        st.shared.close(StreamTermination.Failed("SinkRef 对端终止：${t.ref.path.toString()}"))
                        return TypedBehaviors.stopped<StreamRefMsg>()
                    }
                    TypedBehaviors.same<StreamRefMsg>()
                case _: PostStop =>
                    if (!st.shared.isClosed()) {
                        st.shared.close(StreamTermination.Failed("生产方阶段已停止"))
                    }
                    st.env.unexpose(st.exposedName)
                    TypedBehaviors.same<StreamRefMsg>()
                case _ => TypedBehaviors.same<StreamRefMsg>()
            }
        })
    })
}
