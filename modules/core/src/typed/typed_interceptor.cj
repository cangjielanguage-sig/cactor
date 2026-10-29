/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 行为拦截器（对标 Akka Typed 的 BehaviorInterceptor / Behaviors.intercept / monitor / logMessages）
 */
package cactor_core.typed

/**
 * 行为拦截器：在内层行为处理消息和信号的前后插入逻辑。
 *
 * 拦截器可以：
 * - 调用 target 把消息（可以是修改后的消息）交给内层行为，并返回其结果；
 * - 不调用 target，直接返回 same / unhandled / stopped 等，从而丢弃、拒绝消息或停止 Actor。
 *
 * 内层行为切换（返回新行为）后拦截器仍然保留，直到 Actor 停止。
 */
public interface BehaviorInterceptor<T> {
    func aroundReceive(ctx: TypedActorContext<T>, message: T, target: (T) -> TypedBehavior<T>): TypedBehavior<T>

    func aroundSignal(ctx: TypedActorContext<T>, signal: TypedSignal, target: (TypedSignal) -> TypedBehavior<T>): TypedBehavior<T> {
        target(signal)
    }
}

/** 用函数构造的拦截器 */
public class FunctionInterceptor<T> <: BehaviorInterceptor<T> {
    private let f: (TypedActorContext<T>, T, (T) -> TypedBehavior<T>) -> TypedBehavior<T>

    public init(f: (TypedActorContext<T>, T, (T) -> TypedBehavior<T>) -> TypedBehavior<T>) {
        this.f = f
    }

    public func aroundReceive(ctx: TypedActorContext<T>, message: T, target: (T) -> TypedBehavior<T>): TypedBehavior<T> {
        f(ctx, message, target)
    }
}

internal class InterceptBehavior<T> <: TypedBehavior<T> & StartableBehavior<T> {
    private let interceptor: BehaviorInterceptor<T>
    private let inner: TypedBehavior<T>

    init(interceptor: BehaviorInterceptor<T>, inner: TypedBehavior<T>) {
        this.interceptor = interceptor
        this.inner = inner
    }

    public func startIn(ctx: TypedActorContext<T>): TypedBehavior<T> {
        let started = TypedBehaviors.start(inner, ctx)
        if (TypedBehaviors.isStopped(started)) {
            started
        } else {
            InterceptBehavior<T>(interceptor, started)
        }
    }

    public func onMessage(message: T, context: TypedActorContext<T>): TypedBehavior<T> {
        let current = TypedBehaviors.start(inner, context)
        let next = interceptor.aroundReceive(context, message, { m: T => current.onMessage(m, context) })
        wrap(next, current, context)
    }

    public func onSignal(signal: TypedSignal, context: TypedActorContext<T>): TypedBehavior<T> {
        let current = TypedBehaviors.start(inner, context)
        let next = interceptor.aroundSignal(context, signal, { s: TypedSignal => current.onSignal(s, context) })
        wrap(next, current, context)
    }

    private func wrap(next: TypedBehavior<T>, current: TypedBehavior<T>, ctx: TypedActorContext<T>): TypedBehavior<T> {
        if (TypedBehaviors.isUnhandled(next)) {
            next
        } else if (TypedBehaviors.isSame(next)) {
            if (TypedBehaviors.isDeferred(inner)) {
                InterceptBehavior<T>(interceptor, current)
            } else {
                next
            }
        } else if (TypedBehaviors.isStopped(next)) {
            next
        } else {
            // 内层切换了行为：保留拦截器（同一个拦截器重复包裹时不再叠加）
            let started = TypedBehaviors.start(next, ctx)
            if (TypedBehaviors.isStopped(started)) {
                started
            } else if (let Some(ib) <- (started as InterceptBehavior<T>)) {
                if (sameInterceptor(ib.interceptor, interceptor)) {
                    started
                } else {
                    InterceptBehavior<T>(interceptor, started)
                }
            } else {
                InterceptBehavior<T>(interceptor, started)
            }
        }
    }

    private static func sameInterceptor(a: BehaviorInterceptor<T>, b: BehaviorInterceptor<T>): Bool {
        match ((a as Object, b as Object)) {
            case (Some(x), Some(y)) => refEq(x, y)
            case _ => false
        }
    }

    public func getDescription(): String {
        "Intercept(${inner.getDescription()})"
    }
}

/** 把每条消息复制给监视者，再交给内层行为 */
class MonitorInterceptor<T> <: BehaviorInterceptor<T> {
    let monitor: TypedActorRef<T>

    init(monitor: TypedActorRef<T>) {
        this.monitor = monitor
    }

    public func aroundReceive(ctx: TypedActorContext<T>, message: T, target: (T) -> TypedBehavior<T>): TypedBehavior<T> {
        monitor.tell(message)
        target(message)
    }
}

/** 以 debug 级别记录每条消息与信号 */
class LogMessagesInterceptor<T> <: BehaviorInterceptor<T> {
    let show: (T) -> String

    init(show: (T) -> String) {
        this.show = show
    }

    public func aroundReceive(ctx: TypedActorContext<T>, message: T, target: (T) -> TypedBehavior<T>): TypedBehavior<T> {
        ctx.log().debug("received message ${show(message)}")
        target(message)
    }

    public func aroundSignal(ctx: TypedActorContext<T>, signal: TypedSignal, target: (TypedSignal) -> TypedBehavior<T>): TypedBehavior<T> {
        ctx.log().debug("received signal ${signal.signalName()}")
        target(signal)
    }
}

extend TypedBehaviors {
    /** 用拦截器包裹行为 */
    public static func intercept<T>(interceptor: BehaviorInterceptor<T>, behavior: TypedBehavior<T>): TypedBehavior<T> {
        InterceptBehavior<T>(interceptor, behavior)
    }

    /** 用函数拦截消息：f(ctx, message, target) */
    public static func intercept<T>(f: (TypedActorContext<T>, T, (T) -> TypedBehavior<T>) -> TypedBehavior<T>,
        behavior: TypedBehavior<T>): TypedBehavior<T> {
        InterceptBehavior<T>(FunctionInterceptor<T>(f), behavior)
    }

    /** 每条消息先复制给 monitor（常用于测试中观察 Actor 收到了什么） */
    public static func monitor<T>(monitor: TypedActorRef<T>, behavior: TypedBehavior<T>): TypedBehavior<T> {
        InterceptBehavior<T>(MonitorInterceptor<T>(monitor), behavior)
    }

    /** 以 debug 级别记录收到的消息和信号；show 把消息转为文本 */
    public static func logMessages<T>(behavior: TypedBehavior<T>, show: (T) -> String): TypedBehavior<T> {
        InterceptBehavior<T>(LogMessagesInterceptor<T>(show), behavior)
    }

    /** stopped 行为附带的停止回调（供测试工具执行 PostStop） */
    public static func stoppedCallback<T>(b: TypedBehavior<T>): Option<() -> Unit> {
        match (b as StoppedBehavior<T>) {
            case Some(s) => s.postStop
            case None => None<() -> Unit>
        }
    }
}
