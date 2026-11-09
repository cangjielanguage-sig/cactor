/*
 * CActor Persistence.Typed - 持久化行为的运行时（EventSourcedBehavior 与 DurableStateBehavior 共用）
 *
 * 状态机（与 Akka Typed persistence 的 Recovering → Running ⇄ PersistingEvents 相同）：
 * - 恢复中：IO 线程读快照 / 日志并解码，结果以信号送回；期间到达的命令全部暂存；
 * - 运行中：命令处理器返回 Effect；
 * - 写入中：事件已提交给 IO 线程、尚未确认；期间到达的命令暂存，确认后先应用事件、执行副作用，
 *   再按到达顺序逐条处理暂存的命令（某条命令又触发写入时，其余命令继续留在缓冲区里）。
 * 因此命令严格按到达顺序处理，每条命令看到的都是之前所有命令的事件已落盘后的状态。
 *
 * 内部的 IO 完成通知走 ctx.pipeSignalToSelf，不占用户的命令协议 C。每个运行实例有自己的 token：
 * 监督重启后，旧实例在途 IO 的完成信号被识别并丢弃（新实例从存储重新恢复，不会重复应用）。
 */
package cactor_persistence.typed

import std.time.*
import std.collection.*
import cactor_core.typed.*
import cactor_persistence.{PersistenceException, EventStore, SnapshotStore, BasicSnapshot, BasicEvent, Event,
    DurableStateStore}

/** IO 完成后送回 Actor 的内部信号：在 Actor 线程中执行 complete，得到下一个行为 */
class IoDone<C> <: TypedSignal {
    let token: Object
    let complete: () -> TypedBehavior<C>

    init(token: Object, complete: () -> TypedBehavior<C>) {
        this.token = token
        this.complete = complete
    }

    public func signalName(): String {
        "PersistenceIoDone"
    }
}

abstract class PersistentEngine<C, E, S> <: TypedBehavior<C> {
    protected let ctx: TypedActorContext<C>
    protected let persistenceIdText: String
    protected let emptyState: S
    private let signalHandler: Option<(S, TypedSignal) -> Unit>
    private let stashCapacity: Int64
    private let overflow: StashOverflowStrategy
    private let token = Object()
    protected var state: S
    private var recovering = true
    private var writing = false
    private var stopRequested = false
    private var pendingSides: Array<SideEffect<S>> = []
    // 恢复与写入期间暂存的命令；用户 stash() 的命令单独保存，thenUnstashAll 时插到最前面
    private let stash = ArrayDeque<C>()
    private let userStash = ArrayDeque<C>()
    private var dropped: Int64 = 0

    init(ctx: TypedActorContext<C>, persistenceId: String, emptyState: S,
        signalHandler: Option<(S, TypedSignal) -> Unit>, stashCapacity: Int64, overflow: StashOverflowStrategy) {
        this.ctx = ctx
        this.persistenceIdText = persistenceId
        this.emptyState = emptyState
        this.state = emptyState
        this.signalHandler = signalHandler
        this.stashCapacity = stashCapacity
        this.overflow = overflow
    }

    // ---------- 子类实现 ----------

    /** 调用用户的命令处理器 */
    protected func commandEffect(cmd: C): Effect<E, S>

    /** 提交恢复 IO；完成后在 Actor 线程调用 finishRecovery / failRecovery */
    protected func startRecovery(): Unit

    /** 提交写入 IO；完成后在 Actor 线程更新状态并调用 finishWrite，失败调用 failWrite */
    protected func startWrite(events: Array<E>): Unit

    /** 提交删除 IO（只有 DurableStateBehavior 支持） */
    protected open func startDelete(): Unit {
        throw IllegalStateException("${persistenceIdText}：事件溯源行为不支持 delete")
    }

    // ---------- 供子类使用 ----------

    func start(): TypedBehavior<C> {
        startRecovery()
        this
    }

    /** 在 IO 线程执行 task，完成后在本 Actor 线程执行 onDone（旧实例的完成信号被丢弃） */
    protected func submit<V>(task: () -> V, onDone: (AskOutcome<V>) -> TypedBehavior<C>): Unit {
        let t = token
        ctx.pipeSignalToSelf<V>(PersistenceIo.submit<V>(task), { o => IoDone<C>(t, { => onDone(o) }) })
    }

    protected func notify(signal: TypedSignal): Unit {
        if (let Some(h) <- signalHandler) {
            h(state, signal)
        }
    }

    protected func finishRecovery(highestSequenceNr: Int64): TypedBehavior<C> {
        recovering = false
        notify(RecoveryCompleted(highestSequenceNr))
        drain()
    }

    protected func failRecovery(cause: Exception): TypedBehavior<C> {
        notify(RecoveryFailed(cause))
        throw PersistenceException("${persistenceIdText} 恢复失败：${cause.message}", cause)
    }

    protected func finishWrite(): TypedBehavior<C> {
        writing = false
        let sides = pendingSides
        pendingSides = []
        let b = runSides(sides)
        if (TypedBehaviors.isStopped(b)) {
            return b
        }
        drain()
    }

    /** 写入失败：Actor 以该异常失败，交给监督（默认停止；supervise(...).onFailure(restartWithBackoff) 可重启并重新恢复） */
    protected func failWrite(cause: Exception): TypedBehavior<C> {
        writing = false
        pendingSides = []
        throw cause
    }

    // ---------- 行为 ----------

    public func onMessage(message: C, context: TypedActorContext<C>): TypedBehavior<C> {
        if (recovering || writing) {
            if (stash.size >= stashCapacity) {
                return overflowed()
            }
            stash.addLast(message)
            return ctx.same()
        }
        let b = handle(message)
        if (TypedBehaviors.isStopped(b)) {
            return b
        }
        let d = drain()
        if (TypedBehaviors.isStopped(d)) {
            return d
        }
        b
    }

    public func onSignal(signal: TypedSignal, context: TypedActorContext<C>): TypedBehavior<C> {
        match (signal) {
            case d: IoDone<C> =>
                if (refEq(d.token, token)) {
                    d.complete()
                } else {
                    // 监督重启前的实例提交的 IO：新实例已从存储恢复，忽略
                    ctx.same()
                }
            case _ =>
                notify(signal)
                // 没有信号处理器时，未处理的 Terminated 与普通类型化 Actor 一样以 DeathPact 失败
                if (signalHandler.isNone() && (signal is TypedTerminated)) {
                    TypedBehaviors.unhandled<C>()
                } else {
                    ctx.same()
                }
        }
    }

    public func getDescription(): String {
        "PersistentBehavior(${persistenceIdText})"
    }

    // ---------- 内部 ----------

    private func handle(cmd: C): TypedBehavior<C> {
        let effect = commandEffect(cmd)
        match (effect.primary) {
            case PersistEvents(events) where events.size > 0 =>
                startWrite(events)
                writing = true
                pendingSides = effect.sideEffects
                ctx.same()
            case DeleteState =>
                startDelete()
                writing = true
                pendingSides = effect.sideEffects
                ctx.same()
            case UnhandledCommand =>
                let b = runSides(effect.sideEffects)
                if (TypedBehaviors.isStopped(b)) { b } else { TypedBehaviors.unhandled<C>() }
            case StashCommand =>
                if (userStash.size >= stashCapacity) {
                    let dropped = overflowed()
                    let b = runSides(effect.sideEffects)
                    if (TypedBehaviors.isStopped(b)) { b } else { dropped }
                } else {
                    userStash.addLast(cmd)
                    runSides(effect.sideEffects)
                }
            case _ => runSides(effect.sideEffects)
        }
    }

    private func runSides(sides: Array<SideEffect<S>>): TypedBehavior<C> {
        for (s in sides) {
            match (s) {
                case RunCallback(f) => f(state)
                case StopActor => stopRequested = true
                case UnstashAllCommands =>
                    // 用户暂存的命令排在已暂存的内部命令之前（它们到达得更早）
                    while (let Some(c) <- userStash.removeLast()) {
                        stash.addFirst(c)
                    }
            }
        }
        if (stopRequested) {
            TypedBehaviors.stopped<C>()
        } else {
            ctx.same()
        }
    }

    /** 逐条处理暂存的命令，直到又进入写入中或停止 */
    private func drain(): TypedBehavior<C> {
        while (!recovering && !writing && !stopRequested) {
            match (stash.removeFirst()) {
                case Some(cmd) =>
                    let b = handle(cmd)
                    if (TypedBehaviors.isStopped(b)) {
                        return b
                    }
                case None => break
            }
        }
        ctx.same()
    }

    /**
     * 暂存缓冲区满：FailActor 抛 StashOverflowException；DropCommand 返回 unhandled，
     * 被丢弃的命令作为未处理消息交给死信订阅者（对标 Akka 向事件流发布 Dropped），并限速记日志
     */
    private func overflowed(): TypedBehavior<C> {
        match (overflow) {
            case FailActor =>
                throw StashOverflowException("${persistenceIdText}：暂存缓冲区已满（容量 ${stashCapacity}）")
            case DropCommand =>
                dropped++
                // 每 1000 条记一次，避免日志风暴
                if (dropped % 1000 == 1) {
                    ctx.log().warn("${persistenceIdText}：暂存缓冲区已满（容量 ${stashCapacity}），已丢弃 ${dropped} 条命令")
                }
                TypedBehaviors.unhandled<C>()
        }
    }
}

// ======================= 事件溯源 =======================

/** 恢复读到的数据（IO 线程产生） */
class Recovered<E, S> {
    let snapshot: Option<(S, Int64)>
    let events: Array<E>
    let highest: Int64

    init(snapshot: Option<(S, Int64)>, events: Array<E>, highest: Int64) {
        this.snapshot = snapshot
        this.events = events
        this.highest = highest
    }
}

/** 快照 IO 的结果：删除旧快照 / 旧事件各自可能失败，不影响快照本身 */
class SnapshotOutcome {
    let deleteUpper: Int64
    let snapshotsDeleted: Option<Exception>
    let eventsDeleted: Option<Option<Exception>>

    init(deleteUpper: Int64, snapshotsDeleted: Option<Exception>, eventsDeleted: Option<Option<Exception>>) {
        this.deleteUpper = deleteUpper
        this.snapshotsDeleted = snapshotsDeleted
        this.eventsDeleted = eventsDeleted
    }
}

func loadEventSourced<E, S>(pid: String, journal: EventStore, snapshots: Option<(SnapshotStore, StateCodec<S>)>,
    adapter: EventAdapter<E>): Recovered<E, S> {
    var snap: Option<(S, Int64)> = None
    var from: Int64 = 0
    if (let Some((store, codec)) <- snapshots) {
        if (let Some(sn) <- store.getLatestSnapshot(pid)) {
            snap = Some((codec.decode(sn.getStateData()), sn.getSequenceNumber()))
            from = sn.getSequenceNumber()
        }
    }
    let highest = journal.highestSequenceNr(pid)
    let events = ArrayList<E>()
    // 序号必须从快照之后连续到最高序号：缺口说明事件被删除了却没有覆盖它们的快照（例如快照丢失），
    // 此时拒绝恢复——带着少了一段事件的状态继续运行比停下来更糟
    var expected = from + 1
    for (env in journal.replay(pid, from + 1, highest)) {
        if (env.sequenceNr != expected) {
            throw PersistenceException("${pid} 的日志缺少序号 ${expected}..${env.sequenceNr - 1}（快照序号 ${from}）")
        }
        events.add(all: adapter.fromJournal(env.event.getEventType(), env.event.getEventData()))
        expected++
    }
    if (expected <= highest) {
        throw PersistenceException("${pid} 的日志缺少序号 ${expected}..${highest}（快照序号 ${from}）")
    }
    Recovered<E, S>(snap, events.toArray(), highest)
}

class EventSourcedRunning<C, E, S> <: PersistentEngine<C, E, S> {
    private let cfg: EventSourcedBehavior<C, E, S>
    private let factory = EffectFactory<E, S>()
    private var seqNr: Int64 = 0
    private var snapshotInFlight = false
    private var pendingSnapshot: Option<(Int64, String)> = None

    init(cfg: EventSourcedBehavior<C, E, S>, ctx: TypedActorContext<C>) {
        super(ctx, cfg.persistenceId.id, cfg.emptyState, cfg.signalHandler, cfg.stashCapacity, cfg.stashOverflow)
        this.cfg = cfg
    }

    protected func commandEffect(cmd: C): Effect<E, S> {
        cfg.commandHandler(state, cmd, factory)
    }

    protected func startRecovery(): Unit {
        let pid = persistenceIdText
        let journal = cfg.journal
        let snapshots = cfg.snapshots
        let adapter = cfg.eventAdapter
        submit<Recovered<E, S>>({ => loadEventSourced<E, S>(pid, journal, snapshots, adapter) }, { o =>
            match (o) {
                case Replied(r) => recovered(r)
                case Failed(e) => failRecovery(e)
            }
        })
    }

    private func recovered(r: Recovered<E, S>): TypedBehavior<C> {
        var s = emptyState
        var snapSeq: Int64 = 0
        if (let Some((ss, seq)) <- r.snapshot) {
            s = ss
            snapSeq = seq
        }
        // 事件处理器在 Actor 线程执行（与 Akka 相同）；抛异常即恢复失败（同样先发 RecoveryFailed 信号）
        try {
            for (e in r.events) {
                s = cfg.eventHandler(s, e)
            }
        } catch (e: Exception) {
            return failRecovery(e)
        }
        state = s
        seqNr = max(r.highest, snapSeq)
        finishRecovery(seqNr)
    }

    protected func startWrite(events: Array<E>): Unit {
        let from = seqNr
        let pid = persistenceIdText
        let adapter = cfg.eventAdapter
        let ts = DateTime.now().toUnixTimeStamp().toMilliseconds()
        // 编码在 Actor 线程（适配器是纯函数）：编码失败时本条命令直接失败，不进入写入中
        let records = Array<Event>(events.size, { i =>
            let e = events[i]
            BasicEvent("${pid}-${from + 1 + i}", pid, adapter.manifest(e), adapter.toJournal(e), ts, adapter.tags(e))
        })
        let journal = cfg.journal
        submit<Int64>({ => journal.append(pid, records, from) }, { o =>
            match (o) {
                case Replied(_) => written(events, from)
                case Failed(e) => failWrite(e)
            }
        })
    }

    private func written(events: Array<E>, from: Int64): TypedBehavior<C> {
        var s = state
        var byPredicate = false
        for (i in 0..events.size) {
            s = cfg.eventHandler(s, events[i])
            if (let Some(p) <- cfg.snapshotPredicate) {
                byPredicate = byPredicate || p(s, events[i], from + 1 + i)
            }
        }
        state = s
        seqNr = from + events.size
        if (byPredicate || cfg.retention.crossesSnapshotPoint(from, seqNr)) {
            requestSnapshot()
        }
        finishWrite()
    }

    /**
     * 在当前序号保存快照。状态在 Actor 线程立即编码（之后状态继续变化，快照必须是此刻的值）。
     * 同一时刻最多一个快照在途：两个快照乱序完成会让"最新快照"倒退；在途期间又到了快照点，
     * 只保留最新的一个待写（更早的被它覆盖，恢复时只用最新快照，跳过中间的不影响正确性）
     */
    private func requestSnapshot(): Unit {
        let codec = match (cfg.snapshots) {
            case Some((_, c)) => c
            case None => return
        }
        let data = try {
            codec.encode(state)
        } catch (e: Exception) {
            // 编码失败只影响快照（恢复时多重放一些事件），不让已落盘的写入变成 Actor 失败
            notify(SnapshotFailed(seqNr, e))
            return
        }
        if (snapshotInFlight) {
            pendingSnapshot = Some((seqNr, data))
        } else {
            saveSnapshot(seqNr, data)
        }
    }

    private func saveSnapshot(seq: Int64, data: String): Unit {
        let store = match (cfg.snapshots) {
            case Some((st, _)) => st
            case None => return
        }
        snapshotInFlight = true
        let pid = persistenceIdText
        let retention = cfg.retention
        let journal = cfg.journal
        submit<SnapshotOutcome>({ =>
            store.saveSnapshot(BasicSnapshot(pid, seq, data))
            // 快照落盘之后才删旧的：任何时刻至少有一份可用的快照（或完整的事件）
            let upper = if (retention.isEnabled()) { retention.deleteUpperSequenceNr(seq) } else { 0 }
            var snapErr: Option<Exception> = None
            var eventsResult: Option<Option<Exception>> = None
            if (upper > 0) {
                try {
                    store.deleteSnapshotsTo(pid, upper)
                } catch (e: Exception) {
                    snapErr = Some(e)
                }
                if (retention.deleteEventsOnSnapshot) {
                    eventsResult = Some(try {
                        journal.deleteTo(pid, upper)
                        None<Exception>
                    } catch (e: Exception) {
                        Some(e)
                    })
                }
            }
            SnapshotOutcome(upper, snapErr, eventsResult)
        }, { o => snapshotDone(o, seq) })
    }

    private func snapshotDone(o: AskOutcome<SnapshotOutcome>, seq: Int64): TypedBehavior<C> {
        snapshotInFlight = false
        match (o) {
            case Failed(e) => notify(SnapshotFailed(seq, e))
            case Replied(r) =>
                notify(SnapshotCompleted(seq))
                if (r.deleteUpper > 0) {
                    match (r.snapshotsDeleted) {
                        case Some(e) => notify(DeleteFailed(r.deleteUpper, e))
                        case None => notify(DeleteSnapshotsCompleted(r.deleteUpper))
                    }
                    match (r.eventsDeleted) {
                        case Some(Some(e)) => notify(DeleteFailed(r.deleteUpper, e))
                        case Some(None) => notify(DeleteEventsCompleted(r.deleteUpper))
                        case None => ()
                    }
                }
        }
        if (let Some((nextSeq, data)) <- pendingSnapshot) {
            pendingSnapshot = None
            saveSnapshot(nextSeq, data)
        }
        ctx.same()
    }

    /** 当前最高序号（测试与诊断） */
    func lastSequenceNr(): Int64 {
        seqNr
    }
}

// ======================= 持久状态 =======================

class DurableStateRunning<C, S> <: PersistentEngine<C, S, S> {
    private let cfg: DurableStateBehavior<C, S>
    private let factory = DurableEffectFactory<S>()
    private var revision: Int64 = 0

    init(cfg: DurableStateBehavior<C, S>, ctx: TypedActorContext<C>) {
        super(ctx, cfg.persistenceId.id, cfg.emptyState, cfg.signalHandler, cfg.stashCapacity, cfg.stashOverflow)
        this.cfg = cfg
    }

    protected func commandEffect(cmd: C): Effect<S, S> {
        cfg.commandHandler(state, cmd, factory)
    }

    protected func startRecovery(): Unit {
        let pid = persistenceIdText
        let store = cfg.store
        let codec = cfg.codec
        submit<(Option<S>, Int64)>({ =>
            let s = match (store.get(pid)) {
                case Some(r) => Some(codec.decode(r.state))
                case None => None<S>
            }
            // 已删除时 get 为 None，修订号取墓碑的（不回退，P14）
            (s, store.revisionOf(pid))
        }, { o =>
            match (o) {
                case Replied((s, rev)) =>
                    state = s ?? emptyState
                    revision = rev
                    finishRecovery(rev)
                case Failed(e) => failRecovery(e)
            }
        })
    }

    protected func startWrite(states: Array<S>): Unit {
        // 一次命令只持久化最后一个状态（DurableEffectFactory.persist 只给一个）
        let newState = states[states.size - 1]
        let data = cfg.codec.encode(newState)
        let next = revision + 1
        let pid = persistenceIdText
        let store = cfg.store
        submit<Unit>({ => store.upsert(pid, next, data) }, { o =>
            match (o) {
                case Replied(_) =>
                    revision = next
                    state = newState
                    finishWrite()
                case Failed(e) => failWrite(e)
            }
        })
    }

    protected func startDelete(): Unit {
        let current = revision
        let pid = persistenceIdText
        let store = cfg.store
        submit<Unit>({ =>
            if (current > 0) {
                store.delete(pid, current)
            }
        }, { o =>
            match (o) {
                case Replied(_) =>
                    // 删除留下 current + 1 的墓碑；从未保存过时没有可删的
                    if (current > 0) {
                        revision = current + 1
                    }
                    state = emptyState
                    finishWrite()
                case Failed(e) => failWrite(e)
            }
        })
    }
}
