/*
 * CActor Streams - 异步与缓冲阶段：mapAsync / buffer / 子流（flatMapConcat、recoverWithRetries）
 *
 * mapAsync 在途数量不超过 parallelism：即使下游很慢，也只会有 parallelism 个 Promise 未完成、
 * 最多 parallelism 个结果在阶段里排队。Promise 的完成回调可能在任意线程，一律经 asyncCallback 回到岛内。
 */
package cactor_streams

import std.collection.*
import cactor_core.typed.*

/** 有序 mapAsync 的一个槽位：按上游顺序排队，完成后按顺序发出 */
class AsyncSlot<Out> {
    var outcome: Option<AskOutcome<Out>> = None<AskOutcome<Out>>
}

class MapAsyncLogic<In, Out> <: FlowLogic<In, Out> {
    let parallelism: Int64
    let f: (In) -> TypedPromise<Out>
    let decider: Decider
    let ordered: Bool
    let token: Option<AskCancellationToken>
    /** 有序：全部未发出的槽位（含已完成待发出的） */
    let slots = ArrayDeque<AsyncSlot<Out>>()
    /** 无序：已完成待发出的结果 */
    let completed = ArrayDeque<AskOutcome<Out>>()
    var inFlight: Int64 = 0
    var upstreamDone = false
    /** 观测用：同时未完成的 Promise 最大数量 */
    var peakInFlight: Int64 = 0

    init(island: Island, inlet: Conn<In>, parallelism: Int64, f: (In) -> TypedPromise<Out>, decider: Decider,
        ordered: Bool, token: Option<AskCancellationToken>) {
        super(island, inlet)
        this.parallelism = parallelism
        this.f = f
        this.decider = decider
        this.ordered = ordered
        this.token = token
    }

    /** 阶段里占用的名额：有序时排队的槽位都算，无序时在途 + 待发出 */
    private func occupied(): Int64 {
        if (ordered) { slots.size } else { inFlight + completed.size }
    }

    public override func preStart(): Unit {
        // 与 Akka 一致：不等下游需求，先把并行名额用满
        inlet.pull()
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        let p = try {
            f(e)
        } catch (ex: Exception) {
            if (superviseFailure(this, decider, ex, { => () })) {
                maybePull()
            }
            return
        }
        inFlight++
        if (inFlight > peakInFlight) {
            peakInFlight = inFlight
        }
        let logic = this
        if (ordered) {
            let slot = AsyncSlot<Out>()
            slots.addLast(slot)
            p.onComplete({ o => logic.asyncCallback({ => logic.onResult(Some(slot), o) }) })
        } else {
            p.onComplete({ o => logic.asyncCallback({ => logic.onResult(None<AsyncSlot<Out>>, o) }) })
        }
        maybePull()
    }

    func onResult(slot: Option<AsyncSlot<Out>>, o: AskOutcome<Out>): Unit {
        inFlight--
        match (slot) {
            case Some(s) => s.outcome = Some(o)
            case None => completed.addLast(o)
        }
        drain()
    }

    public override func onPull(): Unit {
        drain()
    }

    public override func onUpstreamFinish(): Unit {
        upstreamDone = true
        drain()
    }

    private func nextReady(): Option<AskOutcome<Out>> {
        if (ordered) {
            match (slots.first) {
                case Some(s) where s.outcome.isSome() =>
                    slots.removeFirst()
                    s.outcome
                case _ => None<AskOutcome<Out>>
            }
        } else {
            completed.removeFirst()
        }
    }

    private func drain(): Unit {
        while (outlet.isAvailable()) {
            match (nextReady()) {
                case Some(Replied(v)) =>
                    outlet.push(v)
                case Some(Failed(ex)) =>
                    if (!superviseFailure(this, decider, ex, { => () })) {
                        return
                    }
                case None => break
            }
        }
        if (upstreamDone && occupied() == 0) {
            completeStage()
            return
        }
        maybePull()
    }

    private func maybePull(): Unit {
        if (occupied() < parallelism && !inlet.hasBeenPulled() && !inlet.isClosedForIn()) {
            inlet.pull()
        }
    }

    public override func postStop(): Unit {
        // ask 变体：结束时取消尚未回复的 ask，不再占用定时器与回复引用
        if (let Some(t) <- token) {
            t.cancel()
        }
        slots.clear()
        completed.clear()
    }
}

/** buffer(size, strategy)：上游比下游快时最多缓存 size 个元素 */
class BufferLogic<T> <: FlowLogic<T, T> {
    let size: Int64
    let strategy: OverflowStrategy
    let buffer = ArrayDeque<T>()
    var upstreamDone = false
    var peakBuffered: Int64 = 0

    init(island: Island, inlet: Conn<T>, size: Int64, strategy: OverflowStrategy) {
        super(island, inlet)
        this.size = size
        this.strategy = strategy
    }

    public override func preStart(): Unit {
        inlet.pull()
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        if (buffer.size >= size) {
            match (strategy) {
                case Backpressure => throw IllegalStateException("背压缓冲区满时不应收到元素")
                case DropHead =>
                    buffer.removeFirst()
                    buffer.addLast(e)
                case DropTail =>
                    buffer.removeLast()
                    buffer.addLast(e)
                case DropBuffer =>
                    buffer.clear()
                    buffer.addLast(e)
                case DropNew => ()
                case Fail =>
                    failStage(BufferOverflowException("buffer 已满（${size}）"))
                    return
            }
        } else {
            buffer.addLast(e)
        }
        if (buffer.size > peakBuffered) {
            peakBuffered = buffer.size
        }
        if (outlet.isAvailable()) {
            outlet.push(buffer.removeFirst().getOrThrow())
        }
        pullIfAllowed()
    }

    private func pullIfAllowed(): Unit {
        let allowed = match (strategy) {
            case Backpressure => buffer.size < size
            case _ => true
        }
        if (allowed && !inlet.hasBeenPulled() && !inlet.isClosedForIn()) {
            inlet.pull()
        }
    }

    public override func onPull(): Unit {
        match (buffer.removeFirst()) {
            case Some(e) => outlet.push(e)
            case None => ()
        }
        if (upstreamDone) {
            if (buffer.isEmpty()) {
                completeStage()
            }
        } else {
            pullIfAllowed()
        }
    }

    public override func onUpstreamFinish(): Unit {
        upstreamDone = true
        if (buffer.isEmpty()) {
            completeStage()
        }
    }
}

// ========== 子流 ==========

/** 子流出口的回调：转给父阶段 */
class SubInletHandler<T> <: InHandler {
    let onPushF: () -> Unit
    let onFinishF: () -> Unit
    let onFailF: (Exception) -> Unit

    init(onPushF: () -> Unit, onFinishF: () -> Unit, onFailF: (Exception) -> Unit) {
        this.onPushF = onPushF
        this.onFinishF = onFinishF
        this.onFailF = onFailF
    }

    public func onPush(): Unit {
        onPushF()
    }

    public func onUpstreamFinish(): Unit {
        onFinishF()
    }

    public func onUpstreamFailure(cause: Exception): Unit {
        onFailF(cause)
    }
}

/**
 * 在父阶段所在的岛里物化一个子 Source，返回其出口连接（父阶段是它的下游）。
 * 子 Source 内的 async 边界照常开新岛。父阶段结束时子流随之被取消。
 */
func materializeSubSource<T>(parent: StageLogic, src: Source<T, Unit>, handler: InHandler): Conn<T> {
    let builder = GraphBuilder(parent.island.materializer, parent.island, false)
    let (built, _) = src.buildFn(builder)
    // 子 Source 以 async 结尾时出口在别的岛：再接一个边界回到父阶段所在的岛
    let conn = if (refEq(builder.current, parent.island)) {
        built
    } else {
        builder.asyncBoundaryInto<T>(built, parent.island.materializer.settings.inputBufferSize, parent.island)
    }
    conn.setIn(handler, parent)
    parent.inPorts.add(conn)
    parent.island.materializer.launch(builder)
    conn
}

func removePort(logic: StageLogic, conn: ConnBase): Unit {
    logic.inPorts.removeIf({ c => refEq(c, conn) })
}

/** flatMapConcat：每个元素展开成一个子 Source，依次（不并发）发出各子流的元素 */
class FlatMapConcatLogic<In, Out> <: FlowLogic<In, Out> {
    let f: (In) -> Source<Out, Unit>
    let decider: Decider
    var sub: Option<Conn<Out>> = None<Conn<Out>>
    var upstreamDone = false

    init(island: Island, inlet: Conn<In>, f: (In) -> Source<Out, Unit>, decider: Decider) {
        super(island, inlet)
        this.f = f
        this.decider = decider
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        let src = try {
            f(e)
        } catch (ex: Exception) {
            if (superviseFailure(this, decider, ex, { => () })) {
                onPull()
            }
            return
        }
        let logic = this
        let conn = materializeSubSource<Out>(this, src, SubInletHandler<Out>(
            { => logic.outlet.push(logic.sub.getOrThrow().grab()) },
            { => logic.onSubFinish() },
            { e => logic.failStage(e) }))
        sub = Some(conn)
        if (outlet.isAvailable()) {
            conn.pull()
        }
    }

    func onSubFinish(): Unit {
        if (let Some(c) <- sub) {
            removePort(this, c)
        }
        sub = None<Conn<Out>>
        if (upstreamDone) {
            completeStage()
        } else if (outlet.isAvailable() && !inlet.hasBeenPulled()) {
            inlet.pull()
        }
    }

    public override func onPull(): Unit {
        match (sub) {
            case Some(c) =>
                if (!c.hasBeenPulled() && !c.isClosedForIn()) {
                    c.pull()
                }
            case None =>
                if (!inlet.hasBeenPulled() && !inlet.isClosedForIn()) {
                    inlet.pull()
                }
        }
    }

    public override func onUpstreamFinish(): Unit {
        upstreamDone = true
        if (sub.isNone()) {
            completeStage()
        }
    }
}

/** recoverWithRetries：上游失败时切换到 f 给出的备用 Source；attempts < 0 表示不限次数 */
class RecoverWithLogic<T> <: FlowLogic<T, T> {
    let attempts: Int64
    let f: (Exception) -> Option<Source<T, Unit>>
    var used: Int64 = 0
    var active: Conn<T>

    init(island: Island, inlet: Conn<T>, attempts: Int64, f: (Exception) -> Option<Source<T, Unit>>) {
        super(island, inlet)
        this.attempts = attempts
        this.f = f
        this.active = inlet
    }

    public func onPush(): Unit {
        outlet.push(inlet.grab())
    }

    public override func onPull(): Unit {
        if (!active.hasBeenPulled() && !active.isClosedForIn()) {
            active.pull()
        }
    }

    public override func onUpstreamFailure(cause: Exception): Unit {
        switchOver(cause)
    }

    private func switchOver(cause: Exception): Unit {
        if (attempts >= 0 && used >= attempts) {
            failStage(cause)
            return
        }
        match (f(cause)) {
            case None => failStage(cause)
            case Some(src) =>
                used++
                removePort(this, active)
                let logic = this
                let conn = materializeSubSource<T>(this, src, SubInletHandler<T>(
                    { => logic.outlet.push(logic.active.grab()) },
                    { => logic.completeStage() },
                    { e => logic.switchOver(e) }))
                active = conn
                if (outlet.isAvailable()) {
                    conn.pull()
                }
        }
    }
}
