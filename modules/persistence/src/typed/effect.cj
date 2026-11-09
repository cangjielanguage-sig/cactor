/*
 * CActor Persistence.Typed - 命令处理的效果（对标 Akka Typed persistence 的 Effect / EffectBuilder）
 *
 * 命令处理器不直接修改状态，而是返回一个 Effect：一个主效果（持久化事件 / 什么都不做 / 未处理 / 暂存 / 删除状态）
 * 加上一串副作用（thenRun / thenReply / thenStop / thenUnstashAll）。主效果是持久化时，副作用在事件**落盘并应用到
 * 状态之后**才执行，拿到的是新状态——"先回复、后落盘"的竞态在类型层面就不存在。
 *
 * 仓颉不能从返回位置推断静态泛型函数的类型参数，因此效果由命令处理器的第三个参数（类型已确定的工厂）创建：
 *
 * ```
 * { state, cmd, effect =>
 *     match (cmd) {
 *         case Add(n, replyTo) => effect.persist(Added(n)).thenReply(replyTo, { s => s })
 *         case Get(replyTo) => effect.reply(replyTo, state)
 *     }
 * }
 * ```
 */
package cactor_persistence.typed

import std.collection.*
import cactor_core.typed.*

/** 主效果 */
enum PrimaryEffect<E> {
    | NoEffect
    | PersistEvents(Array<E>)
    | DeleteState
    | UnhandledCommand
    | StashCommand
}

/** 主效果完成后按顺序执行的副作用 */
enum SideEffect<S> {
    | RunCallback((S) -> Unit)
    | StopActor
    | UnstashAllCommands
}

/**
 * 命令处理的结果。不可变：thenXxx 返回追加了副作用的新效果
 *
 * @param E 事件类型（DurableStateBehavior 中是状态类型本身）
 * @param S 状态类型
 */
public class Effect<E, S> {
    let primary: PrimaryEffect<E>
    let sideEffects: Array<SideEffect<S>>

    init(primary: PrimaryEffect<E>, sideEffects: Array<SideEffect<S>>) {
        this.primary = primary
        this.sideEffects = sideEffects
    }

    private func append(side: SideEffect<S>): Effect<E, S> {
        let n = sideEffects.size
        Effect<E, S>(primary, Array<SideEffect<S>>(n + 1, { i => if (i < n) { sideEffects[i] } else { side } }))
    }

    /** 主效果完成后执行 callback（参数是此时的状态：持久化后为新状态） */
    public func thenRun(callback: (S) -> Unit): Effect<E, S> {
        append(SideEffect<S>.RunCallback(callback))
    }

    /** 主效果完成后用此时的状态计算回复并发给 replyTo */
    public func thenReply<R>(replyTo: TypedActorRef<R>, reply: (S) -> R): Effect<E, S> {
        append(SideEffect<S>.RunCallback({ s => replyTo.tell(reply(s)) }))
    }

    /** 全部副作用执行完后停止 Actor */
    public func thenStop(): Effect<E, S> {
        append(SideEffect<S>.StopActor)
    }

    /** 主效果完成后，把此前用 stash() 暂存的命令按原顺序取出处理 */
    public func thenUnstashAll(): Effect<E, S> {
        append(SideEffect<S>.UnstashAllCommands)
    }

    /** 要持久化的事件（不持久化时为空），供测试与诊断 */
    public func events(): Array<E> {
        match (primary) {
            case PersistEvents(es) => es
            case _ => Array<E>()
        }
    }

    /** 是否是 unhandled() */
    public func isUnhandled(): Bool {
        match (primary) {
            case UnhandledCommand => true
            case _ => false
        }
    }
}

/** DurableStateBehavior 的效果：事件就是新状态 */
public type DurableEffect<S> = Effect<S, S>

/** EventSourcedBehavior 命令处理器的第三个参数：创建效果 */
public class EffectFactory<E, S> {
    public init() {}

    /** 持久化一个事件 */
    public func persist(event: E): Effect<E, S> {
        Effect<E, S>(PrimaryEffect<E>.PersistEvents([event]), [])
    }

    /** 原子地持久化一批事件（一次写入、一次 fsync，要么全部落盘要么一条也不确认） */
    public func persistAll(events: Array<E>): Effect<E, S> {
        Effect<E, S>(PrimaryEffect<E>.PersistEvents(events), [])
    }

    /** 不持久化（只执行副作用） */
    public func none(): Effect<E, S> {
        Effect<E, S>(PrimaryEffect<E>.NoEffect, [])
    }

    /** 命令在当前状态下不处理：计为未处理（进死信） */
    public func unhandled(): Effect<E, S> {
        Effect<E, S>(PrimaryEffect<E>.UnhandledCommand, [])
    }

    /** 停止 Actor */
    public func stop(): Effect<E, S> {
        none().thenStop()
    }

    /** 暂存当前命令，之后由 unstashAll() / thenUnstashAll() 取出（例如等待某个条件满足） */
    public func stash(): Effect<E, S> {
        Effect<E, S>(PrimaryEffect<E>.StashCommand, [])
    }

    /** 取出全部暂存的命令 */
    public func unstashAll(): Effect<E, S> {
        none().thenUnstashAll()
    }

    /** 直接回复（不持久化） */
    public func reply<R>(replyTo: TypedActorRef<R>, message: R): Effect<E, S> {
        none().thenRun({ _ => replyTo.tell(message) })
    }
}

/** DurableStateBehavior 命令处理器的第三个参数 */
public class DurableEffectFactory<S> {
    public init() {}

    /** 持久化新状态（修订号加一） */
    public func persist(state: S): DurableEffect<S> {
        Effect<S, S>(PrimaryEffect<S>.PersistEvents([state]), [])
    }

    /** 删除已保存的状态：之后状态回到 emptyState，修订号不回退 */
    public func delete(): DurableEffect<S> {
        Effect<S, S>(PrimaryEffect<S>.DeleteState, [])
    }

    public func none(): DurableEffect<S> {
        Effect<S, S>(PrimaryEffect<S>.NoEffect, [])
    }

    public func unhandled(): DurableEffect<S> {
        Effect<S, S>(PrimaryEffect<S>.UnhandledCommand, [])
    }

    public func stop(): DurableEffect<S> {
        none().thenStop()
    }

    public func stash(): DurableEffect<S> {
        Effect<S, S>(PrimaryEffect<S>.StashCommand, [])
    }

    public func unstashAll(): DurableEffect<S> {
        none().thenUnstashAll()
    }

    public func reply<R>(replyTo: TypedActorRef<R>, message: R): DurableEffect<S> {
        none().thenRun({ _ => replyTo.tell(message) })
    }
}
