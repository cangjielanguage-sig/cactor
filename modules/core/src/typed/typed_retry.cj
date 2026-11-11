/*
 * CActor Core.Typed - 可取消的异步退避重试
 *
 * 每次操作完成后才安排下一次，不阻塞调度线程；调用方负责请求的幂等语义。
 */
package cactor_core.typed

import std.time.*
import std.sync.*
import std.random.*
import cactor_core.supervision.{BackoffPolicy}

class RetryTimer <: TypedCancellable {
    private let timer: Timer
    private let cancelled = AtomicBool(false)
    init(delay: Duration, action: () -> Unit) { timer = Timer.once(delay, action) }
    public func cancel(): Bool {
        if (!cancelled.compareAndSwap(false, true)) { return false }
        timer.cancel()
        true
    }
    public func isCancelled(): Bool { cancelled.load() }
}

/** 先发布槽位再登记定时器：调度器可能在返回句柄前就执行回调。 */
class RetryDelay {
    private let lock = Mutex()
    private let closed = AtomicBool(false)
    private var handle = None<TypedCancellable>

    func install(value: TypedCancellable): Unit {
        let cancelNow = synchronized(lock) {
            if (closed.load()) { true }
            else { handle = Some(value); false }
        }
        if (cancelNow) { value.cancel() }
    }

    func fire(action: () -> Unit): Unit {
        if (closed.compareAndSwap(false, true)) {
            synchronized(lock) { handle = None<TypedCancellable> }
            action()
        }
    }

    func cancel(): Unit {
        closed.store(true)
        let old = synchronized(lock) {
            let value = handle
            handle = None<TypedCancellable>
            value
        }
        if (let Some(value) <- old) { value.cancel() }
    }
}

class RetryRun<T> {
    let result = TypedPromise<T>()
    private let operation: (Int64) -> TypedPromise<T>
    private let maxAttempts: Int64
    private let backoff: BackoffPolicy
    private let shouldRetry: (Exception) -> Bool
    private let schedule: (Duration, () -> Unit) -> TypedCancellable
    private let lock = Mutex()
    private let random = Random()
    private var pending = None<RetryDelay>

    init(operation: (Int64) -> TypedPromise<T>, maxAttempts: Int64, backoff: BackoffPolicy,
        shouldRetry: (Exception) -> Bool, schedule: (Duration, () -> Unit) -> TypedCancellable) {
        this.operation = operation
        this.maxAttempts = maxAttempts
        this.backoff = backoff
        this.shouldRetry = shouldRetry
        this.schedule = schedule
        result.onComplete({ _ =>
            let old = synchronized(lock) {
                let value = pending
                pending = None<RetryDelay>
                value
            }
            if (let Some(slot) <- old) { slot.cancel() }
        })
    }

    func attempt(number: Int64): Unit {
        if (result.isCompleted()) { return }
        try {
            operation(number).onComplete({ outcome =>
                match (outcome) {
                    case Replied(value) => result.trySuccess(value) |> ignoreCompleted
                    case Failed(cause) => failed(number, cause)
                }
            })
        } catch (cause: Exception) {
            failed(number, cause)
        }
    }

    private func failed(number: Int64, cause: Exception): Unit {
        if (result.isCompleted()) { return }
        if (number >= maxAttempts || (cause as TypedAskCancelledException).isSome()) {
            result.tryFailure(cause) |> ignoreCompleted
            return
        }
        try {
            if (!shouldRetry(cause)) {
                result.tryFailure(cause) |> ignoreCompleted
                return
            }
            let rnd = synchronized(lock) { random.nextFloat64() }
            let delay = backoff.delayFor(number - 1, rnd)
            let slot = RetryDelay()
            let accepted = synchronized(lock) {
                if (result.isCompleted()) { false }
                else { pending = Some(slot); true }
            }
            if (!accepted) { return }
            let handle = schedule(delay, { =>
                slot.fire({ =>
                    synchronized(lock) { pending = None<RetryDelay> }
                    attempt(number + 1)
                })
            })
            slot.install(handle)
        } catch (error: Exception) {
            result.tryFailure(error) |> ignoreCompleted
        }
    }
}

public struct TypedRetry {
    /**
     * attempt 从 1 计，maxAttempts 包括首次调用。仅使用 backoff 的延迟/抖动参数，
     * maxRestarts / resetBackoffAfter 属于监督语义，不影响本次请求的总尝试次数。
     * 取消停止后续尝试；要取消正在等待的 ask，应把同一令牌也传给该 ask。
     * operation 应迅速返回 Promise；不能在此回调里阻塞 get。
     */
    public static func run<T>(operation: (Int64) -> TypedPromise<T>, maxAttempts: Int64, backoff: BackoffPolicy,
        cancellation!: Option<AskCancellationToken> = None<AskCancellationToken>,
        shouldRetry!: (Exception) -> Bool = { _ => true },
        schedule!: (Duration, () -> Unit) -> TypedCancellable = { delay, action => RetryTimer(delay, action) }): TypedPromise<T> {
        if (maxAttempts <= 0) { throw IllegalArgumentException("maxAttempts 必须大于 0") }
        let run = RetryRun<T>(operation, maxAttempts, backoff, shouldRetry, schedule)
        if (let Some(token) <- cancellation) { token.bind(run.result, "retry") }
        run.attempt(1)
        run.result
    }
}
