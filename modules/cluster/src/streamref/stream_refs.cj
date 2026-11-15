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
     * 创建一条流的 sink：返回可以交给远端生产方的 SinkRef 与本地消费句柄。
     * owner：消费该流的 Actor（经典引用，类型化 Actor 用 `ctx.self.toClassic()`）；
     * 它停止时整条流以 Failed 结束，并通知生产方取消（停止生成）。
     *
     * 信用由 sink 侧按本地缓冲空位授予：不 pull 时本地缓冲填满 bufferSize，远端生产方随即被背压。
     */
    public func sinkRef<T>(settings!: StreamRefSettings = StreamRefSettings(),
        owner!: Option<ActorRef> = None<ActorRef>): (SinkRef<T>, StreamConsumer<T>) {
        ensureStreamRefCodecRegistered()
        let streamId = env.nextStreamId()
        let name = "streamref-s-${streamId}"
        let shared = ConsumerShared<T>({ a => a as T })
        let consumer = StreamConsumer<T>(streamId, shared)
        let st = SinkStageState<T>(streamId, name, settings, env, shared, consumer, owner)
        let stage = env.spawnStage(sinkStage<T>(st), name)
        consumer.bind(stage)
        let location = env.expose(name, stage)
        (SinkRef<T>(streamId, location, Some(stage)), consumer)
    }

    /**
     * 生产侧适配：把一个 SinkRef 变成可写入的句柄（同时是 Reactive Streams Subscriber）。
     * 可直接 `Sinks.fromSubscriber(refs.asSink<T>(ref))` 把本地流接到远端 sink；
     * 只在本端有空位且远端授予需求时向上游请求元素，不做无界预读。
     */
    public func asSink<T>(ref: SinkRef<T>, settings!: StreamRefSettings = StreamRefSettings(),
        owner!: Option<ActorRef> = None<ActorRef>): SinkRefProducer<T> {
        ensureStreamRefCodecRegistered()
        // 位置解析失败在调用线程同步报错：阶段 Actor 里抛异常只会变成一条难查的日志
        let sinkPeer = match (ref.localRef) {
            case Some(r) => StreamPeer(r, None<RemoteActorRef>, r.pathString())
            case None =>
                match (env.resolveRemote(ref.location)) {
                    case Some(p) => p
                    case None => throw IllegalArgumentException(
                        "无法解析 SinkRef 位置 ${ref.location}：本节点的 StreamRefs 没有远程层，或位置文本不合法")
                }
        }
        let name = "streamref-sp-${env.nextStreamId()}"
        let shared = ProducerShared(settings.bufferSize)
        let exposedName = if (sinkPeer.isRemote()) { name } else { "" }
        let st = SinkProducerState(ref.streamId, exposedName, settings, shared, env, sinkPeer, owner)
        let stage = env.spawnStage(sinkProducerStage(st), name)
        SinkRefProducer<T>(shared, stage, env)
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

    private func start(streamId: String, location: String, localRef: Option<TypedActorRef<StreamRefMsg>>,
        subscriber: AnySubscriber, settings: StreamRefSettings): StreamRefSubscription {
        ensureStreamRefCodecRegistered()
        // 同进程的 SourceRef 直接用本地引用；否则按位置解析为远程引用
        let producer = match (localRef) {
            case Some(r) => StreamPeer(r, None<RemoteActorRef>, r.pathString())
            case None =>
                match (env.resolveRemote(location)) {
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

/** 元素还原为 T：类型不符是协议违例（两端对 SourceRef<T> 的 T 理解不一致），按失败处理而不是悄悄丢弃 */
func castElement<T>(a: Any): T {
    match (a as T) {
        case Some(v) => v
        case None => throw StreamRefProtocolException("元素类型与 SourceRef 声明的类型不符")
    }
}
