/*
 * CActor Streams - 物化器
 *
 * 蓝图（Source / Flow / Sink / RunnableGraph）本身不执行任何东西；run 时由物化器为每个岛创建一个
 * 类型化 Actor，实例化全部阶段并启动。同一个蓝图可以反复物化，每次得到独立的阶段实例与物化值。
 *
 * 物化器只是"在哪里创建岛 Actor"的抽象：
 * - Materializer.create(system)：岛是系统的顶层 Actor
 * - Materializer.create(ctx)：岛是当前 Actor 的子 Actor，宿主 Actor 停止时流随之终止（物化值以失败结束）
 */
package cactor_streams

import std.sync.*
import cactor_core.typed.*
import cactor.api.CActorSystem

/**
 * 物化参数（不可变，withXxx 返回新实例）
 */
public struct StreamSettings {
    /** 异步边界与 fromPublisher 的输入缓冲区：一次最多向上游请求这么多元素，消耗一半后再补 */
    public let inputBufferSize: Int64
    /** 岛 Actor 每处理多少个事件让出一次调度线程 */
    public let syncProcessingLimit: Int64
    /** 岛 Actor 的名字前缀 */
    public let namePrefix: String
    /** 岛 Actor 的部署参数（调度器）；邮箱必须无界，岛之间的流量已由信用协议限制 */
    public let islandProps: TypedProps

    public init() {
        this(16, 1000, "stream", TypedProps())
    }

    init(inputBufferSize: Int64, syncProcessingLimit: Int64, namePrefix: String, islandProps: TypedProps) {
        this.inputBufferSize = inputBufferSize
        this.syncProcessingLimit = syncProcessingLimit
        this.namePrefix = namePrefix
        this.islandProps = islandProps
    }

    public func withInputBufferSize(size: Int64): StreamSettings {
        if (size <= 0) {
            throw IllegalArgumentException("inputBufferSize 必须为正数，收到 ${size}")
        }
        StreamSettings(size, syncProcessingLimit, namePrefix, islandProps)
    }

    public func withSyncProcessingLimit(limit: Int64): StreamSettings {
        if (limit <= 0) {
            throw IllegalArgumentException("syncProcessingLimit 必须为正数，收到 ${limit}")
        }
        StreamSettings(inputBufferSize, limit, namePrefix, islandProps)
    }

    public func withNamePrefix(prefix: String): StreamSettings {
        if (prefix.isEmpty()) {
            throw IllegalArgumentException("namePrefix 不能为空")
        }
        StreamSettings(inputBufferSize, syncProcessingLimit, prefix, islandProps)
    }

    /** 例如 TypedProps().withDispatcher("blocking")：流里有阻塞调用时放到独立线程池 */
    public func withIslandProps(props: TypedProps): StreamSettings {
        if (props.mailboxCapacity > 0) {
            throw IllegalArgumentException("岛 Actor 的邮箱必须无界：丢弃控制消息会破坏流协议")
        }
        StreamSettings(inputBufferSize, syncProcessingLimit, namePrefix, props)
    }
}

/**
 * 物化器：把蓝图运行起来
 */
public class Materializer {
    private static let seq = AtomicInt64(0)
    private let spawner: (TypedBehavior<IslandCmd>, String, TypedProps) -> TypedActorRef<IslandCmd>
    private let settingsValue: StreamSettings
    private let streamSeq = AtomicInt64(0)
    private let id: Int64

    init(spawner: (TypedBehavior<IslandCmd>, String, TypedProps) -> TypedActorRef<IslandCmd>, settings: StreamSettings) {
        this.spawner = spawner
        this.settingsValue = settings
        this.id = seq.fetchAdd(1) + 1
    }

    /** 岛作为系统的顶层 Actor */
    public static func create(system: CActorSystem): Materializer {
        Materializer({ b, n, p => system.typedActorOf<IslandCmd>(b, n, p) }, StreamSettings())
    }

    /** 岛作为类型化系统的顶层 Actor */
    public static func create(system: TypedActorSystem): Materializer {
        Materializer({ b, n, _ => system.actorOf<IslandCmd>(b, n) }, StreamSettings())
    }

    /**
     * 岛作为 ctx 所属 Actor 的子 Actor。只能在该 Actor 内部（处理消息时）物化；
     * 宿主停止时流被终止，物化值以 AbruptStageTerminationException 失败。
     */
    public static func create<T>(ctx: TypedActorContext<T>): Materializer {
        Materializer({ b, n, p => ctx.actorOf<IslandCmd>(b, n, p) }, StreamSettings())
    }

    public prop settings: StreamSettings {
        get() { settingsValue }
    }

    public func withSettings(settings: StreamSettings): Materializer {
        Materializer(spawner, settings)
    }

    func newIsland(): Island {
        let n = streamSeq.fetchAdd(1) + 1
        Island(this, "${settingsValue.namePrefix}-${id}-${n}")
    }

    /** 物化一张图：构建全部阶段，再为新岛创建 Actor 并启动 */
    func materialize<Mat>(build: (GraphBuilder) -> Mat): Mat {
        let first = newIsland()
        let builder = GraphBuilder(this, first, true)
        let mat = build(builder)
        launch(builder)
        mat
    }

    /** 启动构建器里新建的岛（运行中的子流也走这里） */
    func launch(builder: GraphBuilder): Unit {
        let islands = builder.newIslands.toArray()
        builder.newIslands.clear()
        // 先全部绑定 Actor 再统一 Start：岛之间的首批消息不会落空
        for (island in islands) {
            island.attach(spawner(island.behavior(), island.name, settingsValue.islandProps))
        }
        for (island in islands) {
            island.send(IslandCmd.Start)
        }
    }
}
