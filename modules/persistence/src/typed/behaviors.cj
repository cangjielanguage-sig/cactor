/*
 * CActor Persistence.Typed - 类型化持久化行为（对标 Akka Typed EventSourcedBehavior / DurableStateBehavior）
 *
 * ```
 * enum CounterCmd { | Add(Int64) | Get(TypedActorRef<Int64>) }
 * enum CounterEvt { | Added(Int64) }
 *
 * let counter = EventSourcedBehavior<CounterCmd, CounterEvt, Int64>(
 *     PersistenceId.of("Counter", "c-1"),
 *     emptyState: 0,
 *     commandHandler: { state, cmd, effect =>
 *         match (cmd) {
 *             case Add(n) => effect.persist(Added(n))
 *             case Get(replyTo) => effect.reply(replyTo, state)
 *         }
 *     },
 *     eventHandler: { state, evt => match (evt) { case Added(n) => state + n } },
 *     eventAdapter: counterEvents,          // 事件 ↔ 日志记录
 *     journal: FileEventStore("/var/lib/app/journal"))
 *     .withSnapshots(FileSnapshotStore("/var/lib/app/snapshots"), intCodec)
 *     .withRetention(RetentionCriteria.snapshotEvery(100, 2))
 * system.actorOf(counter.behavior(), "counter-c-1")
 * ```
 *
 * 与经典 EventSourcedActor 共用同一套日志（v2）：写入带期望序号，同一标识的第二个写者被拒绝（单写者）。
 */
package cactor_persistence.typed

import cactor_core.typed.*
import cactor_persistence.{EventStore, SnapshotStore, DurableStateStore}

/** Akka typed persistence 的默认暂存容量 */
let DEFAULT_STASH_CAPACITY: Int64 = 4096

/**
 * 事件溯源行为的定义（配置阶段用 withXxx 链式设置，最后 behavior() 得到可以 actorOf 的行为）
 *
 * @param C 命令类型
 * @param E 事件类型
 * @param S 状态类型（应当不可变：事件处理器返回新状态）
 */
public class EventSourcedBehavior<C, E, S> {
    public let persistenceId: PersistenceId
    let emptyState: S
    let commandHandler: (S, C, EffectFactory<E, S>) -> Effect<E, S>
    let eventHandler: (S, E) -> S
    let eventAdapter: EventAdapter<E>
    let journal: EventStore
    var snapshots: Option<(SnapshotStore, StateCodec<S>)> = None
    var retention: RetentionCriteria = RetentionCriteria.disabled()
    var snapshotPredicate: Option<(S, E, Int64) -> Bool> = None
    var signalHandler: Option<(S, TypedSignal) -> Unit> = None
    var stashCapacity: Int64 = DEFAULT_STASH_CAPACITY
    var stashOverflow: StashOverflowStrategy = StashOverflowStrategy.DropCommand

    /**
     * @param commandHandler (当前状态, 命令, 效果工厂) -> 效果；只读状态、不做 IO，副作用写在 thenRun / thenReply 里
     * @param eventHandler (状态, 事件) -> 新状态；纯函数，恢复时会被重放
     */
    public init(persistenceId: PersistenceId, emptyState!: S,
        commandHandler!: (S, C, EffectFactory<E, S>) -> Effect<E, S>, eventHandler!: (S, E) -> S,
        eventAdapter!: EventAdapter<E>, journal!: EventStore) {
        this.persistenceId = persistenceId
        this.emptyState = emptyState
        this.commandHandler = commandHandler
        this.eventHandler = eventHandler
        this.eventAdapter = eventAdapter
        this.journal = journal
    }

    /** 启用快照：恢复时先加载最新快照，再重放之后的事件 */
    public func withSnapshots(store: SnapshotStore, codec: StateCodec<S>): EventSourcedBehavior<C, E, S> {
        snapshots = Some((store, codec))
        this
    }

    /** 自动快照与删除旧快照 / 旧事件（需要 withSnapshots） */
    public func withRetention(criteria: RetentionCriteria): EventSourcedBehavior<C, E, S> {
        retention = criteria
        this
    }

    /** 某个事件应用后满足条件时保存快照（例如订单结账），可与 withRetention 同时使用 */
    public func snapshotWhen(predicate: (S, E, Int64) -> Bool): EventSourcedBehavior<C, E, S> {
        snapshotPredicate = Some(predicate)
        this
    }

    /** 信号处理：RecoveryCompleted / RecoveryFailed / SnapshotCompleted / … / PostStop / PreRestart / TypedTerminated */
    public func receiveSignal(handler: (S, TypedSignal) -> Unit): EventSourcedBehavior<C, E, S> {
        signalHandler = Some(handler)
        this
    }

    /** 恢复与写入期间暂存命令的容量（默认 4096）与满了之后的处理方式（默认丢弃并记日志） */
    public func withStash(capacity: Int64, overflow!: StashOverflowStrategy = StashOverflowStrategy.DropCommand): EventSourcedBehavior<C, E, S> {
        if (capacity <= 0) {
            throw IllegalArgumentException("暂存容量必须为正：${capacity}")
        }
        stashCapacity = capacity
        stashOverflow = overflow
        this
    }

    /**
     * 可以 actorOf 的行为。启动（以及监督重启）时从存储恢复，恢复完成前到达的命令暂存。
     * 写入失败或恢复失败时 Actor 以异常失败：默认停止；需要自动重试时用
     * TypedBehaviors.supervise(b.behavior()).onFailure(TypedSupervisorStrategy.restartWithBackoff(...))
     */
    public func behavior(): TypedBehavior<C> {
        if (retention.isEnabled() && snapshots.isNone()) {
            throw IllegalArgumentException("${persistenceId}：RetentionCriteria 需要先 withSnapshots")
        }
        let cfg = this
        TypedBehaviors.setup<C>({ ctx => EventSourcedRunning<C, E, S>(cfg, ctx).start() })
    }
}

/**
 * 持久状态行为的定义：每次持久化整个状态（带修订号），不保存事件历史
 *
 * @param C 命令类型
 * @param S 状态类型
 */
public class DurableStateBehavior<C, S> {
    public let persistenceId: PersistenceId
    let emptyState: S
    let commandHandler: (S, C, DurableEffectFactory<S>) -> DurableEffect<S>
    let codec: StateCodec<S>
    let store: DurableStateStore
    var signalHandler: Option<(S, TypedSignal) -> Unit> = None
    var stashCapacity: Int64 = DEFAULT_STASH_CAPACITY
    var stashOverflow: StashOverflowStrategy = StashOverflowStrategy.DropCommand

    public init(persistenceId: PersistenceId, emptyState!: S,
        commandHandler!: (S, C, DurableEffectFactory<S>) -> DurableEffect<S>, codec!: StateCodec<S>,
        store!: DurableStateStore) {
        this.persistenceId = persistenceId
        this.emptyState = emptyState
        this.commandHandler = commandHandler
        this.codec = codec
        this.store = store
    }

    public func receiveSignal(handler: (S, TypedSignal) -> Unit): DurableStateBehavior<C, S> {
        signalHandler = Some(handler)
        this
    }

    public func withStash(capacity: Int64, overflow!: StashOverflowStrategy = StashOverflowStrategy.DropCommand): DurableStateBehavior<C, S> {
        if (capacity <= 0) {
            throw IllegalArgumentException("暂存容量必须为正：${capacity}")
        }
        stashCapacity = capacity
        stashOverflow = overflow
        this
    }

    public func behavior(): TypedBehavior<C> {
        let cfg = this
        TypedBehaviors.setup<C>({ ctx => DurableStateRunning<C, S>(cfg, ctx).start() })
    }
}
