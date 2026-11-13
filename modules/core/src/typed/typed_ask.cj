/*
 * CActor Core.Typed - 请求-响应（对标 Akka Typed AskPattern）
 *
 * TypedAsk.ask 创建一个一次性的 replyTo 引用（不占用 Actor），目标 Actor 用它回复；
 * 超时或主动取消后 Promise 失败，之后到达的回复被丢弃；取消不撤回目标已收到的请求。
 */
package cactor_core.typed

import std.time.*
import std.sync.*
import std.collection.*
import cactor_core.actor.{ActorRef, ActorPath, isAffinityWorkerThread, ActorScheduler, RealTimeScheduler}
import cactor_core.message.{Message}

/** 异步结果：成功或失败 */
public enum AskOutcome<T> {
    | Replied(T)
    | Failed(Exception)

    public func isReplied(): Bool {
        match (this) {
            case Replied(_) => true
            case Failed(_) => false
        }
    }

    /** 成功时的值 */
    public func toOption(): Option<T> {
        match (this) {
            case Replied(v) => Some(v)
            case Failed(_) => None<T>
        }
    }

    /** 失败原因 */
    public func failure(): Option<Exception> {
        match (this) {
            case Replied(_) => None<Exception>
            case Failed(e) => Some(e)
        }
    }
}

/** ask 超时 */
public class TypedAskTimeoutException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/** 主动结束 ask 等待；不撤回已发出的消息，也不终止目标 Actor。 */
public class TypedAskCancelledException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * 可复用于一组 ask 的取消令牌。取消不可逆，订阅在 ask 完成时解除。
 * 锁只保护订阅集合，完成 Promise 和执行用户回调均在锁外进行。
 */
public class AskCancellationToken {
    private let lock = Mutex()
    private var cancelled = false
    private var sequence: Int64 = 0
    private let listeners = HashMap<Int64, () -> Unit>()

    public init() {}

    public func isCancellationRequested(): Bool {
        synchronized(lock) { cancelled }
    }

    /** 首次取消返回 true；重复取消返回 false。 */
    public func cancel(): Bool {
        let pending = synchronized(lock) {
            if (cancelled) { return false }
            cancelled = true
            let callbacks = listeners.values().toArray()
            listeners.clear()
            callbacks
        }
        for (callback in pending) { callback() }
        true
    }

    func bind<T>(promise: TypedPromise<T>, targetPath: String): Unit {
        let callback = { =>
            promise.tryFailure(TypedAskCancelledException("ask ${targetPath} 已取消"))
            ()
        }
        let id = synchronized(lock) {
            if (cancelled) { None<Int64> }
            else {
                sequence++
                listeners[sequence] = callback
                Some(sequence)
            }
        }
        match (id) {
            case None => callback()
            case Some(key) =>
                promise.onComplete({ _ =>
                    synchronized(lock) { listeners.remove(key) }
                    ()
                })
        }
    }
}

/**
 * 可完成一次的异步结果（线程安全）
 */
public class TypedPromise<T> <: TypedFuture<T> {
    private let lock = Mutex()
    private let cond: Condition
    private var outcomeValue: Option<AskOutcome<T>> = None<AskOutcome<T>>
    private let callbacks = ArrayList<(AskOutcome<T>) -> Unit>()

    public init() {
        this.cond = synchronized(lock) { lock.condition() }
    }

    /** 以成功完成；已完成过返回 false */
    public func trySuccess(value: T): Bool {
        complete(AskOutcome<T>.Replied(value))
    }

    /** 以失败完成；已完成过返回 false */
    public func tryFailure(cause: Exception): Bool {
        complete(AskOutcome<T>.Failed(cause))
    }

    /** 以给定结果完成（成功或失败）；已完成过返回 false */
    public func tryComplete(outcome: AskOutcome<T>): Bool {
        complete(outcome)
    }

    private func complete(outcome: AskOutcome<T>): Bool {
        let pending = synchronized(lock) {
            if (outcomeValue.isSome()) {
                return false
            }
            outcomeValue = Some(outcome)
            cond.notifyAll()
            let cbs = callbacks.toArray()
            callbacks.clear()
            cbs
        }
        // 回调在锁外执行，避免回调中再访问本 Promise 时死锁
        for (cb in pending) {
            runCallback(cb, outcome)
        }
        true
    }

    private func runCallback(cb: (AskOutcome<T>) -> Unit, outcome: AskOutcome<T>): Unit {
        try {
            cb(outcome)
        } catch (_: Exception) {
            // 回调异常不影响其他回调
        }
    }

    public func isCompleted(): Bool {
        synchronized(lock) {
            outcomeValue.isSome()
        }
    }

    /** 当前结果（未完成为 None） */
    public func outcome(): Option<AskOutcome<T>> {
        synchronized(lock) {
            outcomeValue
        }
    }

    /**
     * 未完成时阻塞等待。在亲和调度线程（Actor 的处理方法）里阻塞等待会卡住同线程上的全部 Actor，
     * 被等的 Actor 恰好在同一线程时就是死锁，因此直接报错（T15）；已完成的结果任何线程都可以取。
     */
    public func get(): T {
        checkMayBlock()
        let o = synchronized(lock) {
            while (outcomeValue.isNone()) {
                cond.wait()
            }
            outcomeValue.getOrThrow()
        }
        unwrap(o)
    }

    public func get(timeout: Duration): T {
        checkMayBlock()
        let deadline = MonoTime.now() + timeout
        let o = synchronized(lock) {
            while (outcomeValue.isNone()) {
                let left = deadline - MonoTime.now()
                if (left <= Duration.Zero) {
                    throw TypedAskTimeoutException("等待结果超时（${timeout}）")
                }
                cond.wait(timeout: left)
            }
            outcomeValue.getOrThrow()
        }
        unwrap(o)
    }

    private func checkMayBlock(): Unit {
        if (isAffinityWorkerThread() && synchronized(lock) { outcomeValue.isNone() }) {
            throw IllegalStateException(
                "不能在 Actor 处理方法内阻塞等待 TypedPromise：会占住亲和调度线程，被等的 Actor 若在同一线程上就是死锁。" +
                "请用 ctx.ask / pipeToSelf 在处理方法里接收结果，或让该 Actor 使用 blocking 调度器")
        }
    }

    private func unwrap(o: AskOutcome<T>): T {
        match (o) {
            case Replied(v) => v
            case Failed(e) => throw e
        }
    }

    public func value(): Option<T> {
        match (outcome()) {
            case Some(o) => o.toOption()
            case None => None<T>
        }
    }

    /** 完成时回调；已完成则立即在当前线程执行 */
    public func onComplete(callback: (AskOutcome<T>) -> Unit): Unit {
        let done = synchronized(lock) {
            if (outcomeValue.isNone()) {
                callbacks.add(callback)
            }
            outcomeValue
        }
        if (let Some(o) <- done) {
            runCallback(callback, o)
        }
    }

    /** 转换结果 */
    public func map<R>(f: (T) -> R): TypedPromise<R> {
        let p = TypedPromise<R>()
        onComplete({ o =>
            match (o) {
                case Replied(v) =>
                    try {
                        p.trySuccess(f(v))
                    } catch (e: Exception) {
                        p.tryFailure(e)
                    }
                case Failed(e) => p.tryFailure(e)
            }
            ()
        })
        p
    }

    /**
     * 串联下一个异步步骤（对标 Future.flatMap）：成功时 f 返回下一个 Promise，它的结果就是整条链的结果；
     * 本步失败或 f 抛异常时整条链以该异常失败。全程不阻塞，可以在 Actor 内组合（结果用 pipeToSelf 送回）
     */
    public func flatMap<R>(f: (T) -> TypedPromise<R>): TypedPromise<R> {
        let p = TypedPromise<R>()
        onComplete({ o =>
            match (o) {
                case Replied(v) =>
                    try {
                        f(v).onComplete({ r => p.tryComplete(r) |> ignoreCompleted })
                    } catch (e: Exception) {
                        p.tryFailure(e) |> ignoreCompleted
                    }
                case Failed(e) => p.tryFailure(e) |> ignoreCompleted
            }
            ()
        })
        p
    }

    /** 失败时用 f 给出替代值（对标 Future.recover）；成功原样传递，f 抛异常则以该异常失败 */
    public func recover(f: (Exception) -> T): TypedPromise<T> {
        let p = TypedPromise<T>()
        onComplete({ o =>
            match (o) {
                case Replied(v) => p.trySuccess(v) |> ignoreCompleted
                case Failed(e) =>
                    try {
                        p.trySuccess(f(e)) |> ignoreCompleted
                    } catch (e2: Exception) {
                        p.tryFailure(e2) |> ignoreCompleted
                    }
            }
            ()
        })
        p
    }

    /** 失败时换成另一个异步步骤（对标 Future.recoverWith），例如超时后改问备用服务 */
    public func recoverWith(f: (Exception) -> TypedPromise<T>): TypedPromise<T> {
        let p = TypedPromise<T>()
        onComplete({ o =>
            match (o) {
                case Replied(v) => p.trySuccess(v) |> ignoreCompleted
                case Failed(e) =>
                    try {
                        f(e).onComplete({ r => p.tryComplete(r) |> ignoreCompleted })
                    } catch (e2: Exception) {
                        p.tryFailure(e2) |> ignoreCompleted
                    }
            }
            ()
        })
        p
    }
}

func ignoreCompleted(_: Bool): Unit {}

/** sequence 的汇总状态：各结果按下标落位，剩余数归零时完成 */
class SequenceState<T> {
    let lock = Mutex()
    let slots: Array<Option<T>>
    var remaining: Int64

    init(n: Int64) {
        this.slots = Array<Option<T>>(n, repeat: None<T>)
        this.remaining = n
    }
}

/**
 * Promise 组合子（对标 Future.successful / failed / sequence）
 */
public struct TypedPromises {
    /** 已成功完成的 Promise */
    public static func successful<T>(value: T): TypedPromise<T> {
        let p = TypedPromise<T>()
        p.trySuccess(value) |> ignoreCompleted
        p
    }

    /** 已失败的 Promise */
    public static func failed<T>(cause: Exception): TypedPromise<T> {
        let p = TypedPromise<T>()
        p.tryFailure(cause) |> ignoreCompleted
        p
    }

    /**
     * 全部成功时按**输入顺序**给出结果数组（与完成先后无关）；任一失败立即以它失败，不等其余
     * （对标 Future.sequence）。空数组立即以空数组成功
     */
    public static func sequence<T>(promises: Array<TypedPromise<T>>): TypedPromise<Array<T>> {
        let result = TypedPromise<Array<T>>()
        let n = promises.size
        if (n == 0) {
            result.trySuccess(Array<T>()) |> ignoreCompleted
            return result
        }
        let state = SequenceState<T>(n)
        for (i in 0..n) {
            promises[i].onComplete({ o =>
                match (o) {
                    case Failed(e) => result.tryFailure(e) |> ignoreCompleted
                    case Replied(v) =>
                        let done = synchronized(state.lock) {
                            state.slots[i] = Some(v)
                            state.remaining--
                            state.remaining == 0
                        }
                        if (done) {
                            result.trySuccess(Array<T>(n, { k => state.slots[k].getOrThrow() })) |> ignoreCompleted
                        }
                }
            })
        }
        result
    }
}

/**
 * 一次性的回复引用：tell 即完成 Promise，不占用 Actor
 */
public class PromiseRef<T> <: TypedActorRef<T> {
    private let promise: TypedPromise<T>
    private let path: String

    public init(promise: TypedPromise<T>, path: String) {
        this.promise = promise
        this.path = path
    }

    public func tell(message: T): Unit {
        promise.trySuccess(message)
        ()
    }

    public func pathString(): String {
        path
    }

    public func equals(other: TypedActorRef<T>): Bool {
        path == other.pathString()
    }
}

/**
 * 经典引用形式的一次性回复引用：经典 Actor 用 context.reply 回复时完成 Promise。
 * 收到 TypedPayloadBox / TypedEnvelope 等载荷载体时取出其中的载荷。
 */
public class PromiseActorRef <: ActorRef {
    private let promise: TypedPromise<Any>
    private let actorPath: ActorPath

    public init(promise: TypedPromise<Any>, path: String) {
        this.promise = promise
        this.actorPath = ActorPath("temp", path)
    }

    public prop path: ActorPath {
        get() { actorPath }
    }

    public func tell(message: Message): Unit {
        let payload: Any = match (message) {
            case e: PayloadCarrier => e.anyPayload()
            case _ => message
        }
        promise.trySuccess(payload)
        ()
    }

    public func tell(message: Message, sender: Option<String>): Unit {
        tell(message)
    }

    public func equals(other: ActorRef): Bool {
        actorPath.getPath() == other.path.getPath()
    }

    public func hashCode(): Int32 {
        Int32(actorPath.getPath().hashCode() & 0x7FFFFFFF)
    }

    /** 远程管理器停止或发送失败时结束等待，仍遵循首次完成语义。 */
    public func fail(cause: Exception): Bool { promise.tryFailure(cause) }

    /** 生命周期通知在 Promise 锁外执行；已完成时立即调用。 */
    public func onComplete(callback: () -> Unit): Unit {
        promise.onComplete({ _ => callback() })
    }

    /**
     * Promise 已完成（收到回复、超时或取消）即视为终止（E12′）：一次性的回复引用不会再收消息，
     * 远程层据此注销为它登记的回复入口（经分片 / 单例的 ask 每次都会登记一个）
     */
    public func isTerminated(): Bool {
        promise.isCompleted()
    }
}

/**
 * 请求-响应
 */
public struct TypedAsk {
    private static let seq: AtomicInt64 = AtomicInt64(0)

    static func nextPath(): String {
        "/temp/ask-${seq.fetchAdd(1) + 1}"
    }

    /**
     * 向 target 发送 createRequest(replyTo) 生成的请求，返回响应的 Promise。
     * 在 timeout 内没有回复时 Promise 以 TypedAskTimeoutException 失败。
     * cancellation 可共享；取消以 TypedAskCancelledException 完成等待，不撤回已发送请求。
     * 取消与回复/超时竞争，以最先完成 Promise 的结果为准。
     *
     * 示例：
     *   let p = TypedAsk.ask<Cmd, Int64>(counter, { replyTo => GetCount(replyTo) }, Duration.second)
     *   let n = p.get(Duration.second)
     */
    public static func ask<Req, Res>(target: TypedActorRef<Req>, createRequest: (TypedActorRef<Res>) -> Req,
        timeout: Duration, cancellation!: Option<AskCancellationToken> = None<AskCancellationToken>): TypedPromise<Res> {
        askWith<Req, Res>(target, createRequest, timeout, RealTimeScheduler.instance, cancellation)
    }

    /**
     * 同 ask，超时由给定调度器安排（E52）：Actor 内的 ctx.ask 用所在系统的调度器，
     * 注入虚拟时钟时超时随虚拟时间触发；Actor 外的 ask 由调用线程阻塞等待，保持真实时间
     */
    static func askWith<Req, Res>(target: TypedActorRef<Req>, createRequest: (TypedActorRef<Res>) -> Req,
        timeout: Duration, scheduler: ActorScheduler, cancellation: Option<AskCancellationToken>): TypedPromise<Res> {
        let promise = TypedPromise<Res>()
        let targetPath = target.pathString()
        if (let Some(token) <- cancellation) { token.bind(promise, targetPath) }
        if (promise.isCompleted()) { return promise }
        let path = nextPath()
        let replyTo = PromiseRef<Res>(promise, path)
        let timer = scheduler.scheduleOnce(timeout, { =>
            promise.tryFailure(TypedAskTimeoutException("ask ${targetPath} 在 ${timeout} 内没有回复"))
            ()
        })
        promise.onComplete({ _ => timer.cancel() })
        try {
            if (!promise.isCompleted()) {
                let request = createRequest(replyTo)
                if (!promise.isCompleted()) { target.tell(request) }
            }
        } catch (e: Exception) {
            promise.tryFailure(e)
        }
        promise
    }

    private static let remoteSeq: AtomicInt64 = AtomicInt64(0)

    /**
     * 跨节点 ask：请求以 TypedPayloadBox 过线，回复引用走**信封的 sender**通道，
     * 所以请求消息本身不需要带任何引用（用户自定义的编解码器也就不需要认识 ref 字段）。
     *
     * 目标端照常回复即可：经典上下文用 `context.reply(响应)`；类型化上下文用
     * `context.toClassic().getOrThrow().reply(响应)`。响应在发起方被还原成 Res。
     *
     * 与 ask 的分工：ask 在进程内把 replyTo 塞进请求消息，最直接；跨进程时那个引用过不了线，
     * 因此用 askRemote。Res 必须是响应 Message 的类型本身（StringMessage 这类），
     * 或者是回复方用类型化引用回传、由 PromiseActorRef 自动解包的载荷。
     *
     * 示例：
     *   let p = TypedAsk.askRemote<StringMessage, StringMessage>(remote, StringMessage("ping"), Duration.second)
     *   let reply = p.get(Duration.second)
     */
    public static func askRemote<Req, Res>(target: TypedActorRef<Req>, request: Req,
        timeout: Duration, cancellation!: Option<AskCancellationToken> = None<AskCancellationToken>): TypedPromise<Res> {
        if (timeout <= Duration.Zero) { throw IllegalArgumentException("askRemote timeout 必须为正数") }
        let raw = TypedPromise<Any>()
        let targetPath = target.pathString()
        if (let Some(token) <- cancellation) { token.bind(raw, targetPath) }
        if (raw.isCompleted()) {
            return TypedPromises.failed<Res>(raw.outcome().getOrThrow().failure().getOrThrow())
        }
        let path = "/temp/remote-ask-${remoteSeq.fetchAdd(1) + 1}"
        let replyTo = PromiseActorRef(raw, path)
        try {
            let timer = Timer.once(timeout, { =>
                raw.tryFailure(TypedAskTimeoutException("跨节点 ask ${targetPath} 在 ${timeout} 内没有回复"))
                ()
            })
            raw.onComplete({ _ => timer.cancel() })
            match (target.toClassic()) {
                case Some(classic) =>
                    if (!raw.isCompleted()) {
                        classic.tell(TypedPayloadBox(request), replyTo)
                    }
                case None =>
                    raw.tryFailure(IllegalArgumentException("askRemote 的目标必须由 Actor 承载（toClassic() 为空）：${targetPath}"))
                    ()
            }
        } catch (e: Exception) {
            raw.tryFailure(e)
        }
        raw.map<Res>({ a =>
            match (a as Res) {
                case Some(v) => v
                case None => throw IllegalStateException("跨节点 ask ${targetPath} 的回复无法还原为期望类型；回复要么是 Res 本身，要么是类型化引用回传的一条消息")
            }
        })
    }
}
