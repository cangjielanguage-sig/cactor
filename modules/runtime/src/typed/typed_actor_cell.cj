/*
 * CActor Runtime - Typed Actor Cell (minimal)
 */
package cactor_runtime.typed

import cactor_core.typed.*
import cactor_core.message.*
import cactor_core.actor.*


/**
 * 最小可用的 Typed Actor Cell：
 * - 管理当前 Behavior
 * - 支持 same/stopped 语义
 * - 与包装器交互
 */
public class TypedActorCell<T> {
    private var behavior: TypedBehavior<T>
    private let selfRef: TypedActorRef<T>
    private let context: TypedActorContext<T>
    private let stash: StashBuffer<T>
    private let timers: TypedTimers<T>

    public init(initial: TypedBehavior<T>, selfRef: TypedActorRef<T>, context: TypedActorContext<T>, stashCapacity: Int32) {
        this.behavior = initial
        this.selfRef = selfRef
        this.context = context
        this.stash = StashBuffer<T>(stashCapacity)
        this.timers = TypedTimers<T>(context.scheduler(), selfRef)
    }

    public func onMessage(msg: T): Unit {
        let next = behavior.onMessage(msg, context)
        updateBehavior(next)
    }

    public func updateBehavior(next: TypedBehavior<T>): Unit {
        // 简化：若是StoppedBehavior，忽略后续消息（不在此处停止 Actor，由上层包装器处理）
        behavior = next
    }

    public func stashMessage(msg: T): Bool { stash.stash(msg) }
    public func unstashAll(): Unit { stash.drainTo(selfRef) }
    public func getTimers(): TypedTimers<T> { timers }
}

