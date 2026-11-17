/*
 * CActor Cluster.StreamRef - 入口：StreamRefs（对标 Akka StreamRefs.sourceRef）
 *
 * 每个节点建一个 StreamRefs（共享本节点的系统与远程层）：
 * ```
 * // 节点 B（生产方，例如 LLM Actor 收到请求时）
 * let (ref, producer) = streamRefs.sourceRef<String>(owner: ctx.self.toClassic())
 * classic.reply(StringMessage(ref.toWireString()))       // SourceRef 放进 ask 回复
 * producer.offer("Hello")                                 // Enqueued / Backpressured / Closed
 * producer.termination().onComplete { t => 若 Cancelled 则中止 LLM 调用 }
 *
 * // 节点 A（消费方）
 * let ref = SourceRef<String>.fromWireString(reply.getContent())
 * let sub = streamRefs.subscribe<String>(ref, onNext: { t => print(t) }, onComplete: { => }, onError: { e => })
 * sub.cancel()                                            // 生产方 termination() 变为 Cancelled
 * ```
 *
 * 跨节点时元素类型须能过线（Message、TypedPayloadCodecs 注册的类型或 String），否则生产方在发送侧
 * 大声失败、消费方收到 Failed；线上协议本身以命名载荷注册（ensureStreamRefCodecRegistered），
 * 两端都要使用本模块（未升级的对端解码不了 Subscribe，消费方以订阅超时失败）。
 */
package cactor_cluster.streamref

import cactor.api.{CActorSystem}
import cactor_core.actor.{ActorRef}
import cactor_core.message.{SerializationException, DeserializationException}
import cactor_core.typed.*
import cactor_foundation.reactive.*
import cactor_remote.{SimpleRemoteManager, RemoteActorRef}

public class StreamRefs {
    let env: StreamRefEnv

    /**
     * remote 为本节点的远程层：有它时 SourceRef 可以跨节点传递、消费方也会暴露自己以便生产方回送。
     * installRemoteWatch：为本节点开启 Actor 级远程监视（对端阶段被停止时立即收到 DEATH，而不只靠节点心跳）；
     * 本节点已由其他组件开启时传 false（重复开启会替换监视者 Actor）。
     */
    public init(system: CActorSystem, remote!: Option<SimpleRemoteManager> = None<SimpleRemoteManager>,
        installRemoteWatch!: Bool = true) {
        this.env = StreamRefEnv(system, remote)
        ensureStreamRefCodecRegistered()
        if (installRemoteWatch) {
            if (let Some(r) <- remote) {
                let tag = env.nodeTag
                r.enableRemoteWatch({ creator => system.actorOf(creator, "streamref-remote-watcher-${tag}") })
            }
        }
    }

    /**
     * 创建一条流的生产方：返回可以交给消费方的 SourceRef 与本地生产句柄。
     * owner：生成元素的 Actor（经典引用，类型化 Actor 用 `ctx.self.toClassic()`）；它停止时流以 Failed 结束，
     * 消费方立即收到 onError，而不是等到空闲超时。
     */
    public func sourceRef<T>(settings!: StreamRefSettings = StreamRefSettings(),
        owner!: Option<ActorRef> = None<ActorRef>): (SourceRef<T>, StreamProducer<T>) {
        ensureStreamRefCodecRegistered()
        let streamId = env.nextStreamId()
        let name = "streamref-p-${streamId}"
        let shared = ProducerShared(settings.bufferSize)
        let st = ProducerStageState(streamId, name, settings, shared, env, owner)
        let stage = env.spawnStage(producerStage(st), name)
        let location = env.expose(name, stage)
        (SourceRef<T>(streamId, location, Some(stage)), StreamProducer<T>(shared, stage))
    }

    /** 接入 Sources.fromPublisher；仍是单次挂接，需求与取消直接传给 StreamRef。 */
    public func asPublisher<T>(ref: SourceRef<T>, settings!: StreamRefSettings = StreamRefSettings()): Publisher<T> {
        RefPublisher<T>(this, ref, settings)
    }

    /**
     * 把 Publisher 接到有界 StreamRef；仅按生产端空位请求，禁止无界预读。
     * 远程元素类型仍须有线格式；上游失败转换成 StreamRef 的 Failed(reason)。
     */
    public func fromPublisher<T>(publisher: Publisher<T>, settings!: StreamRefSettings = StreamRefSettings(),
        owner!: Option<ActorRef> = None<ActorRef>): SourceRef<T> {
        let (ref, producer) = sourceRef<T>(settings: settings, owner: owner)
        let subscriber = asSubscriber<T>(producer)
        try {
            publisher.subscribe(subscriber)
        } catch (e: Exception) {
            subscriber.onError(e)
        }
        ref
    }

    /**
     * 接入 Sinks.fromSubscriber；此适配器必须独占 producer 的写入权。
     * 上游最多一个未回应请求，生产端缓冲与远程信用窗口分别由 StreamRefSettings 限定。
     */
    public func asSubscriber<T>(producer: StreamProducer<T>): Subscriber<T> {
        let subscriber = ProducerSubscriber<T>(producer)
        let pump = env.system.typedActorOf<RefPump>(producerSubscriberBehavior<T>(subscriber),
            "streamref-pump-${env.nextStreamId()}")
        subscriber.bind(pump)
        subscriber
    }

    /** 以 Reactive Streams 订阅者消费（订阅者自己 request） */
    public func subscribe<T>(ref: SourceRef<T>, subscriber: StreamRefSubscriber<T>,
        settings!: StreamRefSettings = StreamRefSettings()): StreamRefSubscription {
        let any = AnySubscriber({ s => subscriber.onSubscribe(s) }, { a => subscriber.onNext(castElement<T>(a)) },
            { => subscriber.onComplete() }, { e => subscriber.onError(e) })
        start(ref.streamId, ref.location, ref.localRef, any, settings)
    }

    /**
     * 回调式消费：订阅时请求无界需求，速率仍由预取窗口（bufferSize）限制——
     * 回调在阶段线程串行执行，回调慢 = 阶段不补需求 = 生产方被背压。
     */
    public func subscribe<T>(ref: SourceRef<T>, onNext!: (T) -> Unit, onComplete!: () -> Unit,
        onError!: (StreamRefException) -> Unit, settings!: StreamRefSettings = StreamRefSettings()): StreamRefSubscription {
        let any = AnySubscriber({ s => s.request(Int64.Max) }, { a => onNext(castElement<T>(a)) }, onComplete, onError)
        start(ref.streamId, ref.location, ref.localRef, any, settings)
    }

    /**
     * 把流事件投递给类型化 Actor：先收到 OnSubscribe(subscription)，之后由 Actor 自己 request(n)，
     * 适合"处理完一批再要下一批"的慢消费者。
     */
    public func subscribeActor<T>(ref: SourceRef<T>, target: TypedActorRef<StreamRefEvent<T>>,
        settings!: StreamRefSettings = StreamRefSettings()): StreamRefSubscription {
        let any = AnySubscriber({ s => target.tell(StreamRefEvent<T>.OnSubscribe(s)) },
            { a => target.tell(StreamRefEvent<T>.OnNext(castElement<T>(a))) },
            { => target.tell(StreamRefEvent<T>.OnComplete) },
            { e => target.tell(StreamRefEvent<T>.OnError(e)) })
        start(ref.streamId, ref.location, ref.localRef, any, settings)
    }

    /**
     * SinkRef 的消费侧入口：创建一个接收端并返回可交给生产方节点的 SinkRef 与订阅句柄。
     * 订阅语义与 subscribe<T>(ref, ...) 完全一致（信用窗口、序号校验、取消），只是方向相反——
     * 不是本节点去挂生产方，而是生产方节点拿着 SinkRef 来挂本节点。
     * onSubscriber 回调在挂接完成时收到 StreamRefSubscription，之后由消费方 request(n) 放行元素；
     * 生产方在 sinkTimeout 内没有回 SinkAttach 时以 StreamRefSinkTimeoutException 失败。
     */
    public func sinkRef<T>(onSubscriber: (StreamRefSubscription) -> Unit,
        settings!: StreamRefSettings = StreamRefSettings()): (SinkRef<T>, StreamRefSubscription) {
        sinkRefWithSubscriber<T>(AnySubscriber(onSubscriber, { _ => () }, { => () }, { _ => () }), settings)
    }

    /** sinkRef 的回调式便捷版本：订阅时请求无界需求（速率仍受预取窗口限制） */
    public func sinkRef<T>(onNext!: (T) -> Unit, onComplete!: () -> Unit,
        onError!: (StreamRefException) -> Unit, settings!: StreamRefSettings = StreamRefSettings()):
        (SinkRef<T>, StreamRefSubscription) {
        let any = AnySubscriber({ s => s.request(Int64.Max) }, { a => onNext(castElement<T>(a)) }, onComplete, onError)
        sinkRefWithSubscriber<T>(any, settings)
    }

    /** 把流事件投递给类型化 Actor 的 SinkRef 版本（Actor 自己决定何时 request） */
    public func sinkRefActor<T>(target: TypedActorRef<StreamRefEvent<T>>,
        settings!: StreamRefSettings = StreamRefSettings()): (SinkRef<T>, StreamRefSubscription) {
        let any = AnySubscriber({ s => target.tell(StreamRefEvent<T>.OnSubscribe(s)) },
            { a => target.tell(StreamRefEvent<T>.OnNext(castElement<T>(a))) },
            { => target.tell(StreamRefEvent<T>.OnComplete) },
            { e => target.tell(StreamRefEvent<T>.OnError(e)) })
        sinkRefWithSubscriber<T>(any, settings)
    }

    private func sinkRefWithSubscriber<T>(subscriber: AnySubscriber, settings: StreamRefSettings):
        (SinkRef<T>, StreamRefSubscription) {
        ensureStreamRefCodecRegistered()
        let streamId = env.nextStreamId()
        let name = "streamref-sk-${streamId}"
        let subscription = StreamRefSubscription(streamId)
        let exposedName = if (env.remote.isSome()) { name } else { "" }
        let st = SinkStageState(streamId, exposedName, settings, env, subscriber, subscription,
            localPath: "/user/${name}")
        let stage = env.spawnStage(sinkStage(st), name)
        // attachId：SinkStage 校验 SinkAttach 流标识用的值，必须与 asSink 发出的完全一致。
        // 跨进程给 streamId；同进程给本地引用路径（SinkStage 的 localPath 候选）
        let attachId = if (env.remote.isSome()) { streamId } else { "/user/${name}" }
        subscription.bind(stage)
        let location = env.expose(name, stage)
        (SinkRef<T>(streamId, location, Some(stage), attachId: attachId), subscription)
    }

    /**
     * SinkRef 的生产侧入口：在本节点把生产端接到远端 SinkRef 上。
     * 返回 StreamProducer（与 sourceRef 的句柄同形：offer / complete / fail / termination），
     * 内部创建本地生产阶段并向 SinkRef 位置发 SinkAttach；对端把它当作消费方挂接，
     * 此后的 Demand / Element / Cancel 走既有协议。onSubscriber 闭包的订阅者收到生产方的元素流。
     */
    public func asSink<T>(ref: SinkRef<T>, subscriber: StreamRefSubscriber<T>,
        settings!: StreamRefSettings = StreamRefSettings()): StreamProducer<T> {
        ensureStreamRefCodecRegistered()
        // SinkAttach 的流标识 = SinkRef 携带的 attachId（sinkRefWithSubscriber 生成，两端一定一致）
        let name = "streamref-ps-${env.nextStreamId()}"
        // 生产阶段挂接的对端不是"消费方位置"而是 SinkRef 的暴露名：构造一个指向对端阶段的 peer
        let peer = if (ref.localRef.isSome()) {
            StreamPeer(ref.localRef.getOrThrow(), None<RemoteActorRef>, ref.localRef.getOrThrow().pathString(), false)
        } else {
            match (env.resolveRemote(ref.location, reliable: settings.reliable)) {
                case Some(p) => p
                case None => throw IllegalArgumentException(
                    "无法解析 SinkRef 位置 ${ref.location}：本节点的 StreamRefs 没有远程层，或位置文本不合法")
            }
        }
        // 同进程挂接：生产阶段不暴露自己（location 为空），把引用登记到 env 供 SinkStage 取回
        if (ref.localRef.isSome()) {
            env.registerLocalSinkPeer(ref.attachId, peer)
        }
        let shared = ProducerShared(settings.bufferSize)
        let st = SinkProducerStageState(ref.attachId, name, settings, shared, env, peer)
        // 生产阶段先把 SinkAttach 交给对端，等 OnSubscribe 确认后照常按 Demand 发元素
        let stage = env.spawnStage(sinkProducerStage(st), name)
        // 用既有消费入口把元素递给 subscriber：消费阶段在本进程内挂接生产阶段（SubscribeLocal），
        // 流标识独立生成，与 SinkAttach 的 attachId 互不干扰
        let any = AnySubscriber({ s => subscriber.onSubscribe(s) }, { a => subscriber.onNext(castElement<T>(a)) },
            { => subscriber.onComplete() }, { e => subscriber.onError(e) })
        let cName = "streamref-sc-${env.nextStreamId()}"
        let cSt = ConsumerStageState(env.nextStreamId(), "", settings, env,
            StreamPeer(stage, None<RemoteActorRef>, name, false),
            any, StreamRefSubscription(env.nextStreamId()))
        env.spawnStage(consumerStage(cSt), cName)
        StreamProducer<T>(shared, stage)
    }

    private func start(streamId: String, location: String, localRef: Option<TypedActorRef<StreamRefMsg>>,
        subscriber: AnySubscriber, settings: StreamRefSettings): StreamRefSubscription {
        ensureStreamRefCodecRegistered()
        // 同进程的 SourceRef 直接用本地引用；否则按位置解析为远程引用（reliable 模式走可靠通道）
        let producer = match (localRef) {
            case Some(r) => StreamPeer(r, None<RemoteActorRef>, r.pathString(), false)
            case None =>
                match (env.resolveRemote(location, reliable: settings.reliable)) {
                    case Some(p) => p
                    case None => throw IllegalArgumentException(
                        "无法解析 SourceRef 位置 ${location}：本节点的 StreamRefs 没有远程层，或位置文本不合法")
                }
        }
        let subscription = StreamRefSubscription(streamId)
        let name = "streamref-c-${env.nextStreamId()}"
        // 远程生产方需要能回送：阶段在 setup 里先暴露自己再发 Subscribe（见 consumerStage）
        let exposedName = if (producer.isRemote()) { name } else { "" }
        let st = ConsumerStageState(streamId, exposedName, settings, env, producer, subscriber, subscription)
        subscription.bind(env.spawnStage(consumerStage(st), name))
        subscription
    }
}

// ================= SinkRef =================

private let SINK_REF_PREFIX: String = "sinkref:v1|"

/**
 * 指向消费方"接入点"的引用，可以交给生产方节点（对标 Akka SinkRef<T>）。
 *
 * 与 SourceRef 相反：消费方先创建 sinkRef 并把 SinkRef 文本交给生产方；生产方节点用
 * `StreamRefs.asSink` 解析后拿到一个 Subscriber，把自己的生产端接到它上面即可。
 * 消费方一侧照常收到 subscribe<T>(...) 语义的元素流（信用、序号、取消与 SourceRef 完全一致）。
 *
 * location 为空表示只能在本进程使用（消费方没有远程层）。
 */
public class SinkRef<T> {
    public let streamId: String
    public let location: String
    public let localRef: Option<TypedActorRef<StreamRefMsg>>
    /** SinkAttach 应使用的流标识：消费方 SinkStage 用它校验（同进程为本地引用路径，跨进程为 streamId） */
    public let attachId: String

    /** 一般由 StreamRefs.sinkRef 创建；直接构造用于自定义消费端（测试中的手写协议端点） */
    public init(streamId: String, location: String, localRef: Option<TypedActorRef<StreamRefMsg>>,
        attachId!: String = "") {
        if (streamId.isEmpty() || streamId.contains("|")) {
            throw IllegalArgumentException("流标识不能为空且不能包含 '|'")
        }
        if (location.isEmpty() && localRef.isNone()) {
            throw IllegalArgumentException("SinkRef 至少需要远程位置或本地引用之一")
        }
        this.streamId = streamId
        this.location = location
        this.localRef = localRef
        // fromWireString 还原（无本地引用）时 attachId 缺省为 streamId：跨进程流标识就是 streamId
        this.attachId = if (attachId.isEmpty()) { streamId } else { attachId }
    }

    /** 能否跨进程传递（消费方节点有远程层） */
    public func isSerializable(): Bool {
        !location.isEmpty()
    }

    /** 过线文本，放进请求消息即可；本地专用引用抛 SerializationException */
    public func toWireString(): String {
        if (location.isEmpty()) {
            throw SerializationException("SinkRef ${streamId} 只在本进程有效（消费方的 StreamRefs 没有远程层），不能跨节点传递")
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

/** 元素还原为 T：类型不符是协议违例（两端对 SourceRef<T> 的 T 理解不一致），按失败处理而不是悄悄丢弃 */
func castElement<T>(a: Any): T {
    match (a as T) {
        case Some(v) => v
        case None => throw StreamRefProtocolException("元素类型与 SourceRef 声明的类型不符")
    }
}
