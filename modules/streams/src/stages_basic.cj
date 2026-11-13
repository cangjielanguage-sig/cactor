/*
 * CActor Streams - 同步一进一出阶段
 *
 * 全部遵循同一套 pull / push 协议：下游 onPull 时向上游 pull，上游 onPush 时处理并（可能）push。
 * 执行用户函数的阶段按 Decider 处理异常；Resume / Restart 都丢弃出错元素并重新 pull，
 * Restart 额外把阶段状态重置为初始值。
 */
package cactor_streams

import std.collection.*
import std.time.*
import cactor_core.typed.*

/** 按决定处理用户函数的异常：Stop 时让阶段失败并返回 false；否则返回 true（调用方继续） */
func superviseFailure(logic: StageLogic, decider: Decider, e: Exception, onRestart: () -> Unit): Bool {
    let directive = try {
        decider(e)
    } catch (_: Exception) {
        SupervisionDirective.Stop
    }
    match (directive) {
        case Stop =>
            logic.failStage(e)
            false
        case Resume => true
        case Restart =>
            onRestart()
            true
    }
}

class MapLogic<In, Out> <: FlowLogic<In, Out> {
    let f: (In) -> Out
    let decider: Decider

    init(island: Island, inlet: Conn<In>, f: (In) -> Out, decider: Decider) {
        super(island, inlet)
        this.f = f
        this.decider = decider
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        let r = try {
            Some(f(e))
        } catch (ex: Exception) {
            if (superviseFailure(this, decider, ex, { => () })) {
                inlet.pull()
            }
            None<Out>
        }
        if (let Some(v) <- r) {
            outlet.push(v)
        }
    }
}

/** filter / collect：f 返回 None 的元素被跳过 */
class CollectLogic<In, Out> <: FlowLogic<In, Out> {
    let f: (In) -> Option<Out>
    let decider: Decider

    init(island: Island, inlet: Conn<In>, f: (In) -> Option<Out>, decider: Decider) {
        super(island, inlet)
        this.f = f
        this.decider = decider
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        let r = try {
            f(e)
        } catch (ex: Exception) {
            if (!superviseFailure(this, decider, ex, { => () })) {
                return
            }
            None<Out>
        }
        match (r) {
            case Some(v) => outlet.push(v)
            case None => inlet.pull()
        }
    }
}

/** mapConcat：一个元素展开成多个；展开结果按下游需求逐个发出 */
class MapConcatLogic<In, Out> <: FlowLogic<In, Out> {
    let f: (In) -> Iterable<Out>
    let decider: Decider
    var current: Option<Iterator<Out>> = None<Iterator<Out>>
    var upstreamDone = false

    init(island: Island, inlet: Conn<In>, f: (In) -> Iterable<Out>, decider: Decider) {
        super(island, inlet)
        this.f = f
        this.decider = decider
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        try {
            current = Some(f(e).iterator())
        } catch (ex: Exception) {
            current = None<Iterator<Out>>
            if (!superviseFailure(this, decider, ex, { => () })) {
                return
            }
        }
        emitNext()
    }

    public override func onPull(): Unit {
        emitNext()
    }

    public override func onUpstreamFinish(): Unit {
        upstreamDone = true
        if (current.isNone()) {
            completeStage()
        }
    }

    private func emitNext(): Unit {
        if (!outlet.isAvailable()) {
            return
        }
        if (let Some(it) <- current) {
            match (it.next()) {
                case Some(v) =>
                    outlet.push(v)
                    return
                case None => current = None<Iterator<Out>>
            }
        }
        if (upstreamDone) {
            completeStage()
        } else if (!inlet.hasBeenPulled()) {
            inlet.pull()
        }
    }
}

class TakeLogic<T> <: FlowLogic<T, T> {
    let n: Int64
    var count: Int64 = 0

    init(island: Island, inlet: Conn<T>, n: Int64) {
        super(island, inlet)
        this.n = n
    }

    public override func preStart(): Unit {
        if (n <= 0) {
            completeStage()
        }
    }

    public func onPush(): Unit {
        count++
        outlet.push(inlet.grab())
        if (count >= n) {
            // 取够了：完成下游、取消上游（完成事件排在刚 push 的元素之后）
            completeStage()
        }
    }
}

class TakeWhileLogic<T> <: FlowLogic<T, T> {
    let p: (T) -> Bool
    let inclusive: Bool
    let decider: Decider

    init(island: Island, inlet: Conn<T>, p: (T) -> Bool, inclusive: Bool, decider: Decider) {
        super(island, inlet)
        this.p = p
        this.inclusive = inclusive
        this.decider = decider
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        let ok = try {
            p(e)
        } catch (ex: Exception) {
            if (superviseFailure(this, decider, ex, { => () })) {
                inlet.pull()
            }
            return
        }
        if (ok) {
            outlet.push(e)
        } else {
            if (inclusive) {
                outlet.push(e)
            }
            completeStage()
        }
    }
}

class DropLogic<T> <: FlowLogic<T, T> {
    let n: Int64
    var dropped: Int64 = 0

    init(island: Island, inlet: Conn<T>, n: Int64) {
        super(island, inlet)
        this.n = n
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        if (dropped < n) {
            dropped++
            inlet.pull()
        } else {
            outlet.push(e)
        }
    }
}

class DropWhileLogic<T> <: FlowLogic<T, T> {
    let p: (T) -> Bool
    let decider: Decider
    var dropping = true

    init(island: Island, inlet: Conn<T>, p: (T) -> Bool, decider: Decider) {
        super(island, inlet)
        this.p = p
        this.decider = decider
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        if (!dropping) {
            outlet.push(e)
            return
        }
        let drop = try {
            p(e)
        } catch (ex: Exception) {
            if (superviseFailure(this, decider, ex, { => () })) {
                inlet.pull()
            }
            return
        }
        if (drop) {
            inlet.pull()
        } else {
            dropping = false
            outlet.push(e)
        }
    }
}

/**
 * 需要在上游结束后再发出最后一个元素的阶段的公共部分（grouped / fold / recover）
 */
abstract class EmitLastLogic<In, Out> <: FlowLogic<In, Out> {
    var pendingLast: Option<Out> = None<Out>

    init(island: Island, inlet: Conn<In>) {
        super(island, inlet)
    }

    /** 发出最后一个元素后完成；下游暂时没有需求就等到 onPull */
    func emitLastAndComplete(v: Option<Out>): Unit {
        match (v) {
            case Some(x) =>
                if (outlet.isAvailable()) {
                    outlet.push(x)
                    completeStage()
                } else {
                    pendingLast = Some(x)
                }
            case None => completeStage()
        }
    }

    public open override func onPull(): Unit {
        match (pendingLast) {
            case Some(x) =>
                pendingLast = None<Out>
                outlet.push(x)
                completeStage()
            case None => inlet.pull()
        }
    }
}

class GroupedLogic<T> <: EmitLastLogic<T, Array<T>> {
    let n: Int64
    var acc = ArrayList<T>()

    init(island: Island, inlet: Conn<T>, n: Int64) {
        super(island, inlet)
        this.n = n
    }

    public func onPush(): Unit {
        acc.add(inlet.grab())
        if (acc.size >= n) {
            let group = acc.toArray()
            acc = ArrayList<T>()
            outlet.push(group)
        } else {
            inlet.pull()
        }
    }

    public override func onUpstreamFinish(): Unit {
        if (acc.isEmpty()) {
            completeStage()
        } else {
            let group = acc.toArray()
            acc = ArrayList<T>()
            emitLastAndComplete(Some(group))
        }
    }
}

/** scan：先发出 zero，再发出每一步的累积值 */
class ScanLogic<In, Out> <: FlowLogic<In, Out> {
    let zero: Out
    let f: (Out, In) -> Out
    let decider: Decider
    var acc: Out
    var zeroEmitted = false
    var completeAfterZero = false

    init(island: Island, inlet: Conn<In>, zero: Out, f: (Out, In) -> Out, decider: Decider) {
        super(island, inlet)
        this.zero = zero
        this.acc = zero
        this.f = f
        this.decider = decider
    }

    public override func onPull(): Unit {
        if (!zeroEmitted) {
            zeroEmitted = true
            outlet.push(acc)
            if (completeAfterZero) {
                completeStage()
            }
        } else {
            inlet.pull()
        }
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        let logic = this
        try {
            acc = f(acc, e)
        } catch (ex: Exception) {
            if (superviseFailure(this, decider, ex, { => logic.acc = logic.zero })) {
                inlet.pull()
            }
            return
        }
        outlet.push(acc)
    }

    public override func onUpstreamFinish(): Unit {
        // 上游为空、zero 还没发出：等下游需求到来时发出 zero 再完成
        if (zeroEmitted) {
            completeStage()
        } else if (outlet.isAvailable()) {
            zeroEmitted = true
            outlet.push(acc)
            completeStage()
        } else {
            completeAfterZero = true
        }
    }
}

/** fold：上游结束时发出一个累积结果 */
class FoldLogic<In, Out> <: EmitLastLogic<In, Out> {
    let zero: Out
    let f: (Out, In) -> Out
    let decider: Decider
    var acc: Out

    init(island: Island, inlet: Conn<In>, zero: Out, f: (Out, In) -> Out, decider: Decider) {
        super(island, inlet)
        this.zero = zero
        this.acc = zero
        this.f = f
        this.decider = decider
    }

    public override func preStart(): Unit {
        // fold 只在结束时发出，不必等下游需求就开始消费上游
        inlet.pull()
    }

    public override func onPull(): Unit {
        match (pendingLast) {
            case Some(x) =>
                pendingLast = None<Out>
                outlet.push(x)
                completeStage()
            case None => ()
        }
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        let logic = this
        try {
            acc = f(acc, e)
        } catch (ex: Exception) {
            if (!superviseFailure(this, decider, ex, { => logic.acc = logic.zero })) {
                return
            }
        }
        inlet.pull()
    }

    public override func onUpstreamFinish(): Unit {
        emitLastAndComplete(Some(acc))
    }
}

/** recover：上游失败时，f 给出替代元素则发出它并正常完成，否则照常失败 */
class RecoverLogic<T> <: EmitLastLogic<T, T> {
    let f: (Exception) -> Option<T>

    init(island: Island, inlet: Conn<T>, f: (Exception) -> Option<T>) {
        super(island, inlet)
        this.f = f
    }

    public func onPush(): Unit {
        outlet.push(inlet.grab())
    }

    public override func onUpstreamFailure(cause: Exception): Unit {
        match (f(cause)) {
            case Some(v) => emitLastAndComplete(Some(v))
            case None => failStage(cause)
        }
    }
}

/** watchTermination：透传元素，流结束（完成 / 失败 / 下游取消）时完成 Promise */
class WatchTerminationLogic<T> <: FlowLogic<T, T> {
    let promise: TypedPromise<Unit>

    init(island: Island, inlet: Conn<T>, promise: TypedPromise<Unit>) {
        super(island, inlet)
        this.promise = promise
    }

    public func onPush(): Unit {
        outlet.push(inlet.grab())
    }

    // 先完成 Promise 再结束阶段：postStop 里的兜底 trySuccess 不能抢在失败之前
    public override func onUpstreamFinish(): Unit {
        promise.trySuccess(())
        completeStage()
    }

    public override func onUpstreamFailure(cause: Exception): Unit {
        promise.tryFailure(cause)
        failStage(cause)
    }

    public override func onDownstreamFinish(): Unit {
        promise.trySuccess(())
        completeStage()
    }

    public override func onAbort(cause: Exception): Unit {
        promise.tryFailure(cause)
    }

    public override func postStop(): Unit {
        // 本阶段因自身异常失败等其他路径结束时也要完成 Promise
        promise.trySuccess(())
    }
}

/** 节流：令牌桶，每 per 时间补 elements 个令牌，最多积攒 maximumBurst 个 */
class ThrottleLogic<T> <: FlowLogic<T, T> {
    let nanosPerToken: Int64
    let maximumBurst: Int64
    var tokens: Int64
    var lastRefill: Int64
    var held: Option<T> = None<T>
    var upstreamDone = false

    init(island: Island, inlet: Conn<T>, elements: Int64, perNanos: Int64, maximumBurst: Int64) {
        super(island, inlet)
        this.nanosPerToken = if (perNanos / elements > 0) { perNanos / elements } else { 1 }
        this.maximumBurst = maximumBurst
        this.tokens = maximumBurst
        this.lastRefill = monoNanos()
    }

    private func refill(): Unit {
        let now = monoNanos()
        let gained = (now - lastRefill) / nanosPerToken
        if (gained > 0) {
            tokens = if (tokens + gained > maximumBurst) { maximumBurst } else { tokens + gained }
            lastRefill += gained * nanosPerToken
        }
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        refill()
        if (tokens > 0) {
            tokens--
            outlet.push(e)
        } else {
            held = Some(e)
            let wait = nanosPerToken - (monoNanos() - lastRefill)
            let logic = this
            scheduleOnce(Duration.nanosecond * (if (wait > 0) { wait } else { 1 }), { => logic.release() })
        }
    }

    private func release(): Unit {
        refill()
        if (let Some(e) <- held) {
            if (tokens > 0) {
                tokens--
                held = None<T>
                // 持有元素期间没有 pull 新元素，下游需求仍在
                outlet.push(e)
                if (upstreamDone) {
                    completeStage()
                }
            } else {
                let logic = this
                scheduleOnce(Duration.nanosecond * nanosPerToken, { => logic.release() })
            }
        }
    }

    public override func onUpstreamFinish(): Unit {
        if (held.isNone()) {
            completeStage()
        } else {
            upstreamDone = true
        }
    }
}

let MONO_EPOCH = MonoTime.now()

func monoNanos(): Int64 {
    (MonoTime.now() - MONO_EPOCH).toNanoseconds()
}
