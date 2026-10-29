/*
 * CActor Core.Typed - 请求-响应（对标 Akka Typed AskPattern）
 *
 * TypedAsk.ask 创建一个一次性的 replyTo 引用（不占用 Actor），目标 Actor 用它回复；
 * 超时后 Promise 以 TypedAskTimeoutException 失败，之后到达的回复被丢弃。
 */
package cactor_core.typed

import std.time.*
import std.sync.*
import std.collection.*
import cactor_core.actor.{ActorRef, ActorPath}
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

    public func get(): T {
        let o = synchronized(lock) {
            while (outcomeValue.isNone()) {
                cond.wait()
            }
            outcomeValue.getOrThrow()
        }
        unwrap(o)
    }

    public func get(timeout: Duration): T {
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
 * 收到 TypedEnvelope 时取出其中的载荷。
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
     *
     * 示例：
     *   let p = TypedAsk.ask<Cmd, Int64>(counter, { replyTo => GetCount(replyTo) }, Duration.second)
     *   let n = p.get(Duration.second)
     */
    public static func ask<Req, Res>(target: TypedActorRef<Req>, createRequest: (TypedActorRef<Res>) -> Req,
        timeout: Duration): TypedPromise<Res> {
        let promise = TypedPromise<Res>()
        let path = nextPath()
        let replyTo = PromiseRef<Res>(promise, path)
        let targetPath = target.pathString()
        let timer = Timer.once(timeout, { =>
            promise.tryFailure(TypedAskTimeoutException("ask ${targetPath} 在 ${timeout} 内没有回复"))
            ()
        })
        promise.onComplete({ _ => timer.cancel() })
        try {
            target.tell(createRequest(replyTo))
        } catch (e: Exception) {
            promise.tryFailure(e)
        }
        promise
    }
}
