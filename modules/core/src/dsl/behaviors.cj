/*
 * CActor Core.DSL - 行为层唯一推荐入口（对照 docs/plan/dsl20261030.md §7.2 / §7.3 / P3）
 *
 * 三档写法（推荐顺序）：
 *   形态 A（宏）：@actor[] class X { func onXxx(m: T, ctx) { ... } }   —— 经典 Actor + 编译期协议核对
 *   形态 B（lambda）：Behaviors.receive<T> { ctx, msg => ... }           —— 类型化 Actor，行为即函数
 *   形态 C（经典）：手写 receive + match                                —— 兜底写法
 *
 * 形态 A 与形态 B 共用同一套运行时语义：
 *   MessageResult.Handled   ↔ TypedBehaviors.same()
 *   MessageResult.Unhandled ↔ TypedBehaviors.unhandled()
 *   MessageResult.Stop      ↔ TypedBehaviors.stopped()
 *   MessageResult.Escalate  ↔ 交给监督者（typed 路径由 TypedBehaviors.supervise 承担）
 *   MessageResult.Restart   ↔ 交给监督者（typed 路径由 TypedBehaviors.supervise 承担）
 *
 * 本文件提供 Behaviors 门面（对标 Akka Typed 的 Behaviors 命名），内部仍由
 * TypedBehaviors 实现，避免再出现第三套行为 API。
 */
package cactor_core.dsl

import cactor_core.typed.{
    TypedBehavior, TypedActorContext, TypedSignal, TypedTimerScheduler, TypedStashBuffer,
    TypedActorRef, TypedBehaviors, BehaviorInterceptor, SupervisedBehavior
}
import cactor_core.message.MessageResult

/**
 * 行为工厂门面：DSL 文档与示例统一使用 Behaviors.xxx，
 * TypedBehaviors 保留为底层实现（不再新增第二套语义）。
 */
public struct Behaviors {
    /** 接收消息（可用上下文） */
    public static func receive<T>(handler: (TypedActorContext<T>, T) -> TypedBehavior<T>): TypedBehavior<T> {
        TypedBehaviors.receive<T>(handler)
    }

    /** 同时提供消息与信号处理函数 */
    public static func receive<T>(handler: (TypedActorContext<T>, T) -> TypedBehavior<T>,
        signalHandler: (TypedActorContext<T>, TypedSignal) -> TypedBehavior<T>): TypedBehavior<T> {
        TypedBehaviors.receive<T>(handler, signalHandler)
    }

    /** 只关心消息本身 */
    public static func receiveMessage<T>(handler: (T) -> TypedBehavior<T>): TypedBehavior<T> {
        TypedBehaviors.receiveMessage<T>(handler)
    }

    /** 启动时创建行为（重启会重新执行） */
    public static func setup<T>(factory: (TypedActorContext<T>) -> TypedBehavior<T>): TypedBehavior<T> {
        TypedBehaviors.setup<T>(factory)
    }

    /** 保持当前行为（等价于 MessageResult.Handled） */
    public static func same<T>(): TypedBehavior<T> {
        TypedBehaviors.same<T>()
    }

    /** 计为未处理（等价于 MessageResult.Unhandled） */
    public static func unhandled<T>(): TypedBehavior<T> {
        TypedBehaviors.unhandled<T>()
    }

    /** 停止 Actor（等价于 MessageResult.Stop） */
    public static func stopped<T>(): TypedBehavior<T> {
        TypedBehaviors.stopped<T>()
    }

    /** 停止 Actor 并在停止后执行 postStop */
    public static func stopped<T>(postStop: () -> Unit): TypedBehavior<T> {
        TypedBehaviors.stopped<T>(postStop)
    }

    /** 接收并丢弃所有消息 */
    public static func ignore<T>(): TypedBehavior<T> {
        TypedBehaviors.ignore<T>()
    }

    /** 不处理任何消息 */
    public static func empty<T>(): TypedBehavior<T> {
        TypedBehaviors.empty<T>()
    }

    /** 使用 Actor 级定时器 */
    public static func withTimers<T>(factory: (TypedTimerScheduler<T>) -> TypedBehavior<T>): TypedBehavior<T> {
        TypedBehaviors.withTimers<T>(factory)
    }

    /** 使用暂存缓冲区 */
    public static func withStash<T>(capacity: Int64, factory: (TypedStashBuffer<T>) -> TypedBehavior<T>): TypedBehavior<T> {
        TypedBehaviors.withStash<T>(capacity, factory)
    }

    /** 指定失败时的监督策略 */
    public static func supervise<T>(behavior: TypedBehavior<T>): SupervisedBehavior<T> {
        TypedBehaviors.supervise<T>(behavior)
    }

    /** 外部消息类型 In → 内部类型 T */
    public static func transformMessages<In, T>(inner: TypedBehavior<T>, f: (In) -> T): TypedBehavior<In> {
        TypedBehaviors.transformMessages<In, T>(inner, f)
    }

    /** mapMessages / contramap 是 transformMessages 的别名 */
    public static func mapMessages<In, T>(inner: TypedBehavior<T>, f: (In) -> T): TypedBehavior<In> {
        TypedBehaviors.transformMessages<In, T>(inner, f)
    }

    public static func contramap<In, T>(inner: TypedBehavior<T>, f: (In) -> T): TypedBehavior<In> {
        TypedBehaviors.transformMessages<In, T>(inner, f)
    }

    /** 过滤消息 */
    public static func filter<In>(inner: TypedBehavior<In>, p: (In) -> Bool): TypedBehavior<In> {
        TypedBehaviors.filter<In>(inner, p)
    }

    /** 部分接收 */
    public static func receivePartial<T>(p: (T) -> Bool,
        handler: (TypedActorContext<T>, T) -> TypedBehavior<T>): TypedBehavior<T> {
        TypedBehaviors.receivePartial<T>(p, handler)
    }

    /** 用拦截器包裹行为 */
    public static func intercept<T>(interceptor: BehaviorInterceptor<T>, behavior: TypedBehavior<T>): TypedBehavior<T> {
        TypedBehaviors.intercept<T>(interceptor, behavior)
    }

    /** 用函数拦截消息 */
    public static func intercept<T>(f: (TypedActorContext<T>, T, (T) -> TypedBehavior<T>) -> TypedBehavior<T>,
        behavior: TypedBehavior<T>): TypedBehavior<T> {
        TypedBehaviors.intercept<T>(f, behavior)
    }

    /** 每条消息先复制给 monitor */
    public static func monitor<T>(monitor: TypedActorRef<T>, behavior: TypedBehavior<T>): TypedBehavior<T> {
        TypedBehaviors.monitor<T>(monitor, behavior)
    }

    /** 以 debug 级别记录收到的消息 */
    public static func logMessages<T>(behavior: TypedBehavior<T>, show: (T) -> String): TypedBehavior<T> {
        TypedBehaviors.logMessages<T>(behavior, show)
    }
}

/**
 * 形态 A（MessageResult）→ 形态 B（TypedBehavior）的语义映射。
 * 让经典路径与类型化路径共享同一套"处理结果"语义。
 */
public func messageResultToBehavior<T>(result: MessageResult): TypedBehavior<T> {
    match (result) {
        case MessageResult.Handled => TypedBehaviors.same<T>()
        case MessageResult.Unhandled => TypedBehaviors.unhandled<T>()
        case MessageResult.Stop => TypedBehaviors.stopped<T>()
        case MessageResult.Restart => TypedBehaviors.same<T>()
        case MessageResult.Escalate => TypedBehaviors.unhandled<T>()
    }
}
