/*
 * CActor Streams - 执行引擎：连接、阶段逻辑与岛（island）解释器
 *
 * 设计（对标 Akka Streams 的 GraphInterpreter / ActorGraphInterpreter，按线性图简化）：
 * - 阶段之间用 Conn<T> 相连。下游 pull 一次、上游才能 push 一个元素，所以每条连接上最多只有
 *   1 个在途元素——内存上界由图的形状决定，与流的长度无关（这就是背压）。
 * - 同一个岛里的阶段融合执行：push / pull / 完成 / 取消都变成事件放进岛的 FIFO 队列，由解释器
 *   逐个派发。阶段之间从不直接相互调用，因此没有重入问题，调用栈深度与流长度无关。
 * - 每个岛由一个类型化 Actor 承载；岛内所有阶段状态只在这个 Actor 里读写，不需要锁。
 *   岛外的输入（异步边界上的元素与需求、mapAsync 的结果、定时器、队列 offer、kill switch）一律经
 *   invokeAsync 变成发给该 Actor 的消息。
 * - 每处理 syncProcessingLimit 个事件就让出 Actor（给自己发 Resume），无限流也不会独占调度线程，
 *   kill switch 等控制消息能及时得到处理。
 */
package cactor_streams

import std.collection.*
import std.sync.*
import std.time.*
import cactor_core.typed.*
import cactor_streams.reactive.*

// ========== 连接 ==========

let EV_PUSH: Int64 = 1
let EV_PULL: Int64 = 2
let EV_COMPLETE: Int64 = 3
let EV_FAIL: Int64 = 4
let EV_CANCEL: Int64 = 5

/** 输入端口的回调（由下游阶段实现） */
interface InHandler {
    func onPush(): Unit

    func onUpstreamFinish(): Unit

    func onUpstreamFailure(cause: Exception): Unit
}

/** 输出端口的回调（由上游阶段实现） */
interface OutHandler {
    func onPull(): Unit

    func onDownstreamFinish(): Unit
}

/** 与元素类型无关的连接状态；解释器只通过它派发事件 */
abstract class ConnBase {
    let island: Island
    var inHandler: Option<InHandler> = None<InHandler>
    var outHandler: Option<OutHandler> = None<OutHandler>
    var inOwner: Option<StageLogic> = None<StageLogic>
    var outOwner: Option<StageLogic> = None<StageLogic>
    /** 下游已 pull、上游尚未 push */
    var pulled = false
    /** 上游已完成或失败（端口对两侧都已关闭，但完成事件可能仍在队列里） */
    var upClosed = false
    /** 下游已取消 */
    var downClosed = false
    var failure: Option<Exception> = None<Exception>

    init(island: Island) {
        this.island = island
    }

    public func hasElement(): Bool

    public func dropElement(): Unit

    /** 下游取消。幂等；上游已关闭时不再通知上游 */
    func cancel(): Unit {
        if (downClosed) {
            return
        }
        downClosed = true
        pulled = false
        dropElement()
        if (!upClosed) {
            island.enqueue(this, EV_CANCEL)
        }
    }

    /** 上游完成。幂等；在途元素先于完成信号到达下游（事件 FIFO） */
    func complete(): Unit {
        if (upClosed) {
            return
        }
        upClosed = true
        if (!downClosed) {
            island.enqueue(this, EV_COMPLETE)
        }
    }

    func fail(cause: Exception): Unit {
        if (upClosed) {
            return
        }
        upClosed = true
        failure = Some(cause)
        if (!downClosed) {
            island.enqueue(this, EV_FAIL)
        }
    }

    /**
     * 下游请求一个元素。上游已关闭时是空操作（完成事件已在路上，与 Akka 一致）；
     * 重复 pull 或元素未取走就 pull 都是阶段实现的错误。
     */
    func pull(): Unit {
        if (downClosed) {
            throw IllegalStateException("不能 pull 已取消的端口")
        }
        if (upClosed) {
            return
        }
        if (pulled) {
            throw IllegalStateException("不能对同一端口重复 pull")
        }
        if (hasElement()) {
            throw IllegalStateException("上一个元素尚未 grab 就再次 pull")
        }
        pulled = true
        island.enqueue(this, EV_PULL)
    }

    /** 已 pull 且元素尚未取走（还没到、或已到但没 grab）：此时不能再次 pull */
    func hasBeenPulled(): Bool {
        pulled || hasElement()
    }

    /** 输出端可以 push（下游有需求且两侧都未关闭） */
    func isAvailable(): Bool {
        pulled && !downClosed && !upClosed
    }

    /** 从下游视角：上游已结束且没有待取元素，或自己已取消 */
    func isClosedForIn(): Bool {
        downClosed || (upClosed && !hasElement())
    }

    /** 从上游视角：自己已完成或下游已取消 */
    func isClosedForOut(): Bool {
        upClosed || downClosed
    }

    func setIn(handler: InHandler, owner: StageLogic): Unit {
        inHandler = Some(handler)
        inOwner = Some(owner)
    }

    func setOut(handler: OutHandler, owner: StageLogic): Unit {
        outHandler = Some(handler)
        outOwner = Some(owner)
    }
}

/** 类型化连接：最多保存 1 个已 push 未 grab 的元素 */
class Conn<T> <: ConnBase {
    var slot: Option<T> = None<T>

    init(island: Island) {
        super(island)
    }

    public func hasElement(): Bool {
        slot.isSome()
    }

    public func dropElement(): Unit {
        slot = None<T>
    }

    /** 向下游发出一个元素：必须先有 pull；下游已取消时元素被丢弃 */
    func push(element: T): Unit {
        if (downClosed) {
            return
        }
        if (upClosed) {
            throw IllegalStateException("不能 push 已完成的端口")
        }
        if (!pulled) {
            throw IllegalStateException("没有需求时不能 push（Reactive Streams 1.1）")
        }
        pulled = false
        slot = Some(element)
        island.enqueue(this, EV_PUSH)
    }

    /** 取走上游 push 来的元素 */
    func grab(): T {
        match (slot) {
            case Some(e) =>
                slot = None<T>
                e
            case None => throw IllegalStateException("端口上没有可取的元素")
        }
    }
}

struct StreamEvent {
    let conn: ConnBase
    let kind: Int64

    init(conn: ConnBase, kind: Int64) {
        this.conn = conn
        this.kind = kind
    }
}

// ========== 阶段逻辑 ==========

/** 岛被意外终止（承载的 Actor 停止、系统关闭）时，未结束的阶段以此失败 */
public class AbruptStageTerminationException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 一个阶段在某次物化中的运行实例。只在所属岛的 Actor 里被调用。
 */
abstract class StageLogic {
    let island: Island
    var id: Int64 = 0
    var finished = false
    /** 跨线程可读的结束标记（订阅者回调等在岛外判断是否还需要投递） */
    let finishedFlag = AtomicBool(false)
    let inPorts = ArrayList<ConnBase>()
    let outPorts = ArrayList<ConnBase>()
    let timers = HashMap<Int64, Timer>()
    private var timerSeq: Int64 = 0

    init(island: Island) {
        this.island = island
    }

    /** 连线：把自己登记为端口的处理者。由 GraphBuilder 在构造后调用（构造函数里不能让 this 逃逸） */
    public open func wire(): Unit {}

    /** 岛启动（或子流在运行中加入）时调用一次 */
    public open func preStart(): Unit {}

    /** 阶段结束时调用一次（正常完成、失败、取消或岛被终止） */
    public open func postStop(): Unit {}

    /** 岛被意外终止：物化值（Promise 等）应在这里以失败结束 */
    public open func onAbort(cause: Exception): Unit {}

    /** 正常结束：取消全部输入、完成全部输出 */
    func completeStage(): Unit {
        for (c in inPorts) {
            c.cancel()
        }
        for (c in outPorts) {
            c.complete()
        }
        finish()
    }

    /** 失败结束：取消全部输入、以 cause 失败全部输出 */
    func failStage(cause: Exception): Unit {
        for (c in inPorts) {
            c.cancel()
        }
        for (c in outPorts) {
            c.fail(cause)
        }
        finish()
    }

    func finish(): Unit {
        if (finished) {
            return
        }
        finished = true
        finishedFlag.store(true)
        for ((_, t) in timers) {
            t.cancel()
        }
        timers.clear()
        try {
            postStop()
        } catch (_: Exception) {
            // postStop 的异常不能阻止岛的收尾
        }
        island.stageFinished(this)
    }

    /**
     * 从任意线程把 f 投递回本阶段所在的岛执行。阶段已结束则丢弃；f 抛出的异常使本阶段失败。
     */
    func asyncCallback(f: () -> Unit): Unit {
        asyncCallbackOr(f, { => () })
    }

    /** 同上；阶段已结束（或岛已结束）时改为执行 orElse（在岛线程或调用方线程） */
    func asyncCallbackOr(f: () -> Unit, orElse: () -> Unit): Unit {
        let logic = this
        let delivered = island.invokeAsync({ =>
            if (logic.finished) {
                orElse()
            } else {
                try {
                    f()
                } catch (e: Exception) {
                    logic.failStage(e)
                }
            }
        })
        if (!delivered) {
            orElse()
        }
    }

    /** 在 delay 之后于岛内执行 f；阶段结束时自动取消 */
    func scheduleOnce(delay: Duration, f: () -> Unit): Unit {
        let logic = this
        timerSeq++
        let key = timerSeq
        // 回调在岛线程执行，此时 put 早已完成（同在岛线程），触发后即移除，长时间运行也不积累
        let t = Timer.once(delay, { =>
            logic.asyncCallback({ =>
                logic.timers.remove(key)
                f()
            })
        })
        timers[key] = t
    }

    /** 以固定间隔在岛内执行 f；阶段结束时自动取消 */
    func schedulePeriodically(initialDelay: Duration, interval: Duration, f: () -> Unit): Unit {
        let logic = this
        timerSeq++
        timers[timerSeq] = Timer.repeat(initialDelay, interval, { => logic.asyncCallback(f) }, style: CatchupStyle.Skip)
    }
}

/** 只有一个输出的阶段（Source） */
abstract class SourceLogic<Out> <: StageLogic & OutHandler {
    let outlet: Conn<Out>

    init(island: Island) {
        super(island)
        this.outlet = Conn<Out>(island)
    }

    public override func wire(): Unit {
        outlet.setOut(this, this)
        outPorts.add(outlet)
    }

    public open func onDownstreamFinish(): Unit {
        completeStage()
    }
}

/** 一进一出的阶段（Flow） */
abstract class FlowLogic<In, Out> <: StageLogic & InHandler & OutHandler {
    let inlet: Conn<In>
    let outlet: Conn<Out>

    init(island: Island, inlet: Conn<In>) {
        super(island)
        this.inlet = inlet
        this.outlet = Conn<Out>(island)
    }

    public override func wire(): Unit {
        inlet.setIn(this, this)
        outlet.setOut(this, this)
        inPorts.add(inlet)
        outPorts.add(outlet)
    }

    public open func onPull(): Unit {
        inlet.pull()
    }

    public open func onUpstreamFinish(): Unit {
        completeStage()
    }

    public open func onUpstreamFailure(cause: Exception): Unit {
        failStage(cause)
    }

    public open func onDownstreamFinish(): Unit {
        completeStage()
    }
}

/** 只有一个输入的阶段（Sink） */
abstract class SinkLogic<In> <: StageLogic & InHandler {
    let inlet: Conn<In>

    init(island: Island, inlet: Conn<In>) {
        super(island)
        this.inlet = inlet
    }

    public override func wire(): Unit {
        inlet.setIn(this, this)
        inPorts.add(inlet)
    }

    public open func onUpstreamFinish(): Unit {
        completeStage()
    }

    public open func onUpstreamFailure(cause: Exception): Unit {
        failStage(cause)
    }
}

// ========== 岛 ==========

/** 岛 Actor 的协议 */
enum IslandCmd {
    | Start
    | Exec(() -> Unit)
    | Resume
}

class IslandRefBox {
    let ref: TypedActorRef<IslandCmd>

    init(ref: TypedActorRef<IslandCmd>) {
        this.ref = ref
    }
}

/**
 * 一个融合执行单元：一组阶段 + 事件队列 + 承载它的 Actor。
 */
class Island {
    let materializer: Materializer
    let name: String
    private let refBox = AtomicOptionReference<IslandRefBox>()
    private let pendingLock = Mutex()
    private let pending = ArrayList<IslandCmd>()
    let finishedFlag = AtomicBool(false)
    private var started = false
    private var resumeScheduled = false
    private let events = ArrayDeque<StreamEvent>()
    private let logics = HashMap<Int64, StageLogic>()
    private var nextLogicId: Int64 = 0
    private let limit: Int64

    init(materializer: Materializer, name: String) {
        this.materializer = materializer
        this.name = name
        this.limit = materializer.settings.syncProcessingLimit
    }

    // ---------- 物化期（单线程） ----------

    func register(logic: StageLogic): Unit {
        nextLogicId++
        logic.id = nextLogicId
        logics[logic.id] = logic
        if (started) {
            // 运行中加入的子流阶段：立即启动
            runPreStart(logic)
        }
    }

    /** 绑定承载 Actor，并把绑定前投递的命令补发出去 */
    func attach(ref: TypedActorRef<IslandCmd>): Unit {
        let queued = synchronized(pendingLock) {
            refBox.store(IslandRefBox(ref))
            let copy = pending.toArray()
            pending.clear()
            copy
        }
        for (cmd in queued) {
            ref.tell(cmd)
        }
    }

    func behavior(): TypedBehavior<IslandCmd> {
        let island = this
        TypedBehaviors.receive<IslandCmd>({ _, cmd =>
            island.handle(cmd)
            if (island.isFinished()) {
                TypedBehaviors.stopped<IslandCmd>()
            } else {
                TypedBehaviors.same<IslandCmd>()
            }
        }).receiveSignal({ _, signal =>
            if (signal is PostStop) {
                island.abort(AbruptStageTerminationException("流的岛 ${island.name} 在阶段结束前被终止"))
            }
            TypedBehaviors.same<IslandCmd>()
        })
    }

    // ---------- 任意线程 ----------

    /** 投递到岛内执行；岛已结束返回 false（调用方据此走降级路径） */
    func invokeAsync(f: () -> Unit): Bool {
        if (finishedFlag.load()) {
            return false
        }
        send(IslandCmd.Exec(f))
        true
    }

    func send(cmd: IslandCmd): Unit {
        match (refBox.load()) {
            case Some(box) => box.ref.tell(cmd)
            case None =>
                let direct = synchronized(pendingLock) {
                    match (refBox.load()) {
                        case Some(box) => Some(box)
                        case None =>
                            pending.add(cmd)
                            None<IslandRefBox>
                    }
                }
                if (let Some(box) <- direct) {
                    box.ref.tell(cmd)
                }
        }
    }

    func isFinished(): Bool {
        finishedFlag.load()
    }

    // ---------- 岛线程 ----------

    func handle(cmd: IslandCmd): Unit {
        if (finishedFlag.load()) {
            return
        }
        ensureStarted()
        match (cmd) {
            case Start => ()
            case Exec(f) =>
                try {
                    f()
                } catch (_: Exception) {
                    // f 自带阶段级的异常处理（asyncCallback）；这里只防止岛 Actor 被意外打断
                }
            case Resume => resumeScheduled = false
        }
        drain()
    }

    private func ensureStarted(): Unit {
        if (started) {
            return
        }
        started = true
        // 按登记顺序（源 → 汇）启动；preStart 里只会排事件，不会直接回调其他阶段
        let initial = logics.values().toArray()
        sortById(initial)
        for (logic in initial) {
            runPreStart(logic)
        }
    }

    private func sortById(arr: Array<StageLogic>): Unit {
        // 插入排序：一个岛里的阶段数很少
        var i = 1
        while (i < arr.size) {
            let x = arr[i]
            var j = i - 1
            while (j >= 0 && arr[j].id > x.id) {
                arr[j + 1] = arr[j]
                j--
            }
            arr[j + 1] = x
            i++
        }
    }

    private func runPreStart(logic: StageLogic): Unit {
        if (logic.finished) {
            return
        }
        try {
            logic.preStart()
        } catch (e: Exception) {
            logic.failStage(e)
        }
    }

    func enqueue(conn: ConnBase, kind: Int64): Unit {
        events.addLast(StreamEvent(conn, kind))
    }

    private func drain(): Unit {
        var n: Int64 = 0
        while (n < limit) {
            match (events.removeFirst()) {
                case Some(ev) =>
                    dispatch(ev)
                    n++
                case None => break
            }
        }
        if (!events.isEmpty()) {
            // 让出 Actor：其他消息（取消、kill switch）可以插进来
            if (!resumeScheduled) {
                resumeScheduled = true
                send(IslandCmd.Resume)
            }
        } else if (logics.isEmpty()) {
            finishedFlag.store(true)
        }
    }

    private func dispatch(ev: StreamEvent): Unit {
        let c = ev.conn
        let toIn = ev.kind == EV_PUSH || ev.kind == EV_COMPLETE || ev.kind == EV_FAIL
        let owner = if (toIn) { c.inOwner } else { c.outOwner }
        let logic = match (owner) {
            case Some(l) => l
            case None => return
        }
        if (logic.finished) {
            return
        }
        try {
            if (ev.kind == EV_PUSH) {
                if (!c.downClosed) { c.inHandler.getOrThrow().onPush() }
            } else if (ev.kind == EV_PULL) {
                if (!c.upClosed && !c.downClosed) { c.outHandler.getOrThrow().onPull() }
            } else if (ev.kind == EV_COMPLETE) {
                if (!c.downClosed) { c.inHandler.getOrThrow().onUpstreamFinish() }
            } else if (ev.kind == EV_FAIL) {
                if (!c.downClosed) {
                    c.inHandler.getOrThrow().onUpstreamFailure(c.failure.getOrThrow())
                }
            } else if (ev.kind == EV_CANCEL) {
                c.outHandler.getOrThrow().onDownstreamFinish()
            }
        } catch (e: Exception) {
            logic.failStage(e)
        }
    }

    func stageFinished(logic: StageLogic): Unit {
        logics.remove(logic.id)
    }

    /** 承载 Actor 停止：结束所有仍在运行的阶段 */
    func abort(cause: Exception): Unit {
        if (finishedFlag.load() && logics.isEmpty()) {
            return
        }
        let remaining = logics.values().toArray()
        for (logic in remaining) {
            if (!logic.finished) {
                try {
                    logic.onAbort(cause)
                } catch (_: Exception) {}
                logic.failStage(cause)
            }
        }
        events.clear()
        finishedFlag.store(true)
    }
}

// ========== 图构建器 ==========

/**
 * 一次物化的构建上下文：把蓝图里的阶段实例化到岛里，遇到 async 边界时开新岛。
 */
class GraphBuilder {
    let materializer: Materializer
    var current: Island
    /** 本次构建新建、尚未启动的岛 */
    let newIslands = ArrayList<Island>()
    private let deciders = ArrayList<Decider>()

    init(materializer: Materializer, first: Island, firstIsNew: Bool) {
        this.materializer = materializer
        this.current = first
        if (firstIsNew) {
            newIslands.add(first)
        }
    }

    func add(logic: StageLogic): Unit {
        logic.wire()
        current.register(logic)
    }

    /** 当前生效的流内监督策略（最内层的 withSupervision 优先） */
    func decider(): Decider {
        if (deciders.isEmpty()) {
            StreamSupervision.stoppingDecider
        } else {
            deciders[deciders.size - 1]
        }
    }

    func pushDecider(d: Decider): Unit {
        deciders.add(d)
    }

    func popDecider(): Unit {
        deciders.remove(at: deciders.size - 1)
    }

    /**
     * 异步边界：conn 之前的阶段留在当前岛，之后的阶段放进新岛。
     * 两侧用 Reactive Streams 的发布者 / 订阅者连接，按 bufferSize 批量请求（信用制）。
     */
    func asyncBoundary<T>(conn: Conn<T>, bufferSize: Int64): Conn<T> {
        let next = materializer.newIsland()
        newIslands.add(next)
        asyncBoundaryInto<T>(conn, bufferSize, next)
    }

    /** 同上，但下游侧放进已有的岛 target（子流的出口要回到父阶段所在的岛） */
    func asyncBoundaryInto<T>(conn: Conn<T>, bufferSize: Int64, target: Island): Conn<T> {
        let upstream = PublisherSinkLogic<T>(current, conn, None<Subscriber<T>>)
        add(upstream)
        current = target
        let downstream = SubscriberSourceLogic<T>(current, bufferSize, Some<Publisher<T>>(upstream.publisher))
        add(downstream)
        downstream.outlet
    }
}
