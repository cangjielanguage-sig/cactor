/*
 * CActor Streams - 汇阶段
 *
 * 汇是需求的源头：每处理完一个元素才再 pull 一个，所以慢汇会自然地把背压传到整条流。
 * 物化值多为 TypedPromise：流正常完成时成功，失败或岛被终止时以对应异常失败。
 */
package cactor_streams

import std.collection.*
import std.sync.*
import cactor_core.typed.*

class ForeachLogic<T> <: SinkLogic<T> {
    let f: (T) -> Unit
    let decider: Decider
    let promise: TypedPromise<Unit>

    init(island: Island, inlet: Conn<T>, f: (T) -> Unit, decider: Decider, promise: TypedPromise<Unit>) {
        super(island, inlet)
        this.f = f
        this.decider = decider
        this.promise = promise
    }

    public override func preStart(): Unit {
        inlet.pull()
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        try {
            f(e)
        } catch (ex: Exception) {
            if (!superviseFailure(this, decider, ex, { => () })) {
                promise.tryFailure(ex)
                return
            }
        }
        inlet.pull()
    }

    public override func onUpstreamFinish(): Unit {
        promise.trySuccess(())
        completeStage()
    }

    public override func onUpstreamFailure(cause: Exception): Unit {
        promise.tryFailure(cause)
        failStage(cause)
    }

    public override func onAbort(cause: Exception): Unit {
        promise.tryFailure(cause)
    }
}

class FoldSinkLogic<T, U> <: SinkLogic<T> {
    let zero: U
    let f: (U, T) -> U
    let decider: Decider
    let promise: TypedPromise<U>
    var acc: U

    init(island: Island, inlet: Conn<T>, zero: U, f: (U, T) -> U, decider: Decider, promise: TypedPromise<U>) {
        super(island, inlet)
        this.zero = zero
        this.acc = zero
        this.f = f
        this.decider = decider
        this.promise = promise
    }

    public override func preStart(): Unit {
        inlet.pull()
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        let logic = this
        try {
            acc = f(acc, e)
        } catch (ex: Exception) {
            if (!superviseFailure(this, decider, ex, { => logic.acc = logic.zero })) {
                promise.tryFailure(ex)
                return
            }
        }
        inlet.pull()
    }

    public override func onUpstreamFinish(): Unit {
        promise.trySuccess(acc)
        completeStage()
    }

    public override func onUpstreamFailure(cause: Exception): Unit {
        promise.tryFailure(cause)
        failStage(cause)
    }

    public override func onAbort(cause: Exception): Unit {
        promise.tryFailure(cause)
    }
}

/** 收集全部元素；maxElements 防止无界流把内存吃光 */
class SeqLogic<T> <: SinkLogic<T> {
    let maxElements: Int64
    let promise: TypedPromise<Array<T>>
    let acc = ArrayList<T>()

    init(island: Island, inlet: Conn<T>, maxElements: Int64, promise: TypedPromise<Array<T>>) {
        super(island, inlet)
        this.maxElements = maxElements
        this.promise = promise
    }

    public override func preStart(): Unit {
        inlet.pull()
    }

    public func onPush(): Unit {
        if (acc.size >= maxElements) {
            inlet.grab()
            let ex = StreamLimitReachedException("Sink.seq 收到的元素超过上限 ${maxElements}")
            promise.tryFailure(ex)
            failStage(ex)
            return
        }
        acc.add(inlet.grab())
        inlet.pull()
    }

    public override func onUpstreamFinish(): Unit {
        promise.trySuccess(acc.toArray())
        completeStage()
    }

    public override func onUpstreamFailure(cause: Exception): Unit {
        promise.tryFailure(cause)
        failStage(cause)
    }

    public override func onAbort(cause: Exception): Unit {
        promise.tryFailure(cause)
    }
}

/** 取第一个元素后取消上游 */
class HeadOptionLogic<T> <: SinkLogic<T> {
    let promise: TypedPromise<Option<T>>

    init(island: Island, inlet: Conn<T>, promise: TypedPromise<Option<T>>) {
        super(island, inlet)
        this.promise = promise
    }

    public override func preStart(): Unit {
        inlet.pull()
    }

    public func onPush(): Unit {
        promise.trySuccess(Some(inlet.grab()))
        completeStage()
    }

    public override func onUpstreamFinish(): Unit {
        promise.trySuccess(None<T>)
        completeStage()
    }

    public override func onUpstreamFailure(cause: Exception): Unit {
        promise.tryFailure(cause)
        failStage(cause)
    }

    public override func onAbort(cause: Exception): Unit {
        promise.tryFailure(cause)
    }
}

/** 立即取消上游 */
class CancelledSinkLogic<T> <: SinkLogic<T> {
    init(island: Island, inlet: Conn<T>) {
        super(island, inlet)
    }

    public override func preStart(): Unit {
        completeStage()
    }

    public func onPush(): Unit {}
}

// ========== actorRefWithBackpressure ==========

/** 目标 Actor 用来确认的轻量引用：收到任意 Ack 都算一次确认 */
class AckRef<Ack> <: TypedActorRef<Ack> {
    private static let seq = AtomicInt64(0)
    let onAck: () -> Unit
    let path: String

    init(onAck: () -> Unit) {
        this.onAck = onAck
        this.path = "/temp/stream-ack-${seq.fetchAdd(1) + 1}"
    }

    public func tell(message: Ack): Unit {
        onAck()
    }

    public func pathString(): String {
        path
    }

    public func equals(other: TypedActorRef<Ack>): Bool {
        other.pathString() == path
    }
}

/**
 * 一次只发一条：先发 onInit，收到确认后才 pull 下一个元素；目标处理不过来时整条流被背压。
 * 完成消息在最后一个元素被确认后发出；失败消息立即发出。
 */
class ActorRefBackpressureLogic<T, M, Ack> <: SinkLogic<T> {
    let target: TypedActorRef<M>
    let messageAdapter: (TypedActorRef<Ack>, T) -> M
    let onInitMessage: (TypedActorRef<Ack>) -> M
    let onCompleteMessage: M
    let onFailureMessage: (Exception) -> M
    var ackRef: Option<AckRef<Ack>> = None<AckRef<Ack>>
    var awaitingAck = false
    var upstreamDone = false

    init(island: Island, inlet: Conn<T>, target: TypedActorRef<M>, messageAdapter: (TypedActorRef<Ack>, T) -> M,
        onInitMessage: (TypedActorRef<Ack>) -> M, onCompleteMessage: M, onFailureMessage: (Exception) -> M) {
        super(island, inlet)
        this.target = target
        this.messageAdapter = messageAdapter
        this.onInitMessage = onInitMessage
        this.onCompleteMessage = onCompleteMessage
        this.onFailureMessage = onFailureMessage
    }

    public override func preStart(): Unit {
        let logic = this
        let ack = AckRef<Ack>({ => logic.asyncCallback({ => logic.onAck() }) })
        ackRef = Some(ack)
        awaitingAck = true
        target.tell(onInitMessage(ack))
    }

    func onAck(): Unit {
        if (!awaitingAck) {
            // 多余的确认（协议之外）忽略
            return
        }
        awaitingAck = false
        if (upstreamDone) {
            target.tell(onCompleteMessage)
            completeStage()
        } else if (!inlet.hasBeenPulled()) {
            inlet.pull()
        }
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        awaitingAck = true
        target.tell(messageAdapter(ackRef.getOrThrow(), e))
    }

    public override func onUpstreamFinish(): Unit {
        if (awaitingAck) {
            upstreamDone = true
        } else {
            target.tell(onCompleteMessage)
            completeStage()
        }
    }

    public override func onUpstreamFailure(cause: Exception): Unit {
        target.tell(onFailureMessage(cause))
        failStage(cause)
    }

    public override func onAbort(cause: Exception): Unit {
        target.tell(onFailureMessage(cause))
    }
}

// ========== Sink.queue ==========

class SinkQueueShared<T> {
    let lock = Mutex()
    var pendingPull: Option<TypedPromise<Option<T>>> = None<TypedPromise<Option<T>>>
    /** None = 运行中；Some(None) = 已完成；Some(Some(e)) = 已失败 */
    var terminal: Option<Option<Exception>> = None<Option<Exception>>
}

class SinkQueueLogic<T> <: SinkLogic<T> {
    let shared = SinkQueueShared<T>()

    init(island: Island, inlet: Conn<T>) {
        super(island, inlet)
    }

    /** 任意线程：一次只允许一个未完成的 pull */
    func userPull(): TypedPromise<Option<T>> {
        let p = TypedPromise<Option<T>>()
        // 0 = 投递回岛，1 = 并发 pull，2 = 已结束
        let (decision, term) = synchronized(shared.lock) {
            if (let Some(t) <- shared.terminal) {
                (2, t)
            } else if (shared.pendingPull.isSome()) {
                (1, None<Exception>)
            } else {
                shared.pendingPull = Some(p)
                (0, None<Exception>)
            }
        }
        if (decision == 1) {
            p.tryFailure(IllegalStateException("SinkQueue 一次只允许一个未完成的 pull"))
        } else if (decision == 2) {
            match (term) {
                case Some(e) => p.tryFailure(e)
                case None => p.trySuccess(None<T>)
            }
        } else {
            let logic = this
            asyncCallbackOr({ => logic.onUserPull() }, { => logic.settle() })
        }
        p
    }

    func onUserPull(): Unit {
        if (!inlet.hasBeenPulled() && !inlet.isClosedForIn()) {
            inlet.pull()
        }
    }

    public func onPush(): Unit {
        let e = inlet.grab()
        let p = synchronized(shared.lock) {
            let x = shared.pendingPull
            shared.pendingPull = None<TypedPromise<Option<T>>>
            x
        }
        if (let Some(pp) <- p) {
            pp.trySuccess(Some(e))
        }
    }

    func cancelFromOutside(): Unit {
        let logic = this
        asyncCallback({ => logic.completeStage() })
    }

    public override func onUpstreamFailure(cause: Exception): Unit {
        terminate(Some(cause))
        failStage(cause)
    }

    public override func postStop(): Unit {
        terminate(None<Exception>)
    }

    public override func onAbort(cause: Exception): Unit {
        terminate(Some(cause))
    }

    /** 记录终止并完成挂起的 pull（完成：None；失败：异常） */
    private func terminate(t: Option<Exception>): Unit {
        synchronized(shared.lock) {
            if (shared.terminal.isNone()) {
                shared.terminal = Some(t)
            }
        }
        settle()
    }

    func settle(): Unit {
        let (p, t) = synchronized(shared.lock) {
            let x = shared.pendingPull
            shared.pendingPull = None<TypedPromise<Option<T>>>
            (x, shared.terminal)
        }
        if (let Some(pp) <- p) {
            match (t) {
                case Some(Some(e)) => pp.tryFailure(e)
                case _ => pp.trySuccess(None<T>)
            }
        }
    }
}

/** Sinks.queue 的物化值：在流外按需逐个拉取元素 */
public class SinkQueue<T> {
    let logic: SinkQueueLogic<T>

    init(logic: SinkQueueLogic<T>) {
        this.logic = logic
    }

    /** 拉取下一个元素；流已完成时得到 None，流失败时 Promise 失败。必须等上一个 pull 完成再调用 */
    public func pull(): TypedPromise<Option<T>> {
        logic.userPull()
    }

    /** 不再需要元素：取消上游 */
    public func cancel(): Unit {
        logic.cancelFromOutside()
    }
}
