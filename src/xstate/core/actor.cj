/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 状态机Actor包
 * 简化的状态机Actor实现
 */
package cactor.xstate.core.actor

import std.collection.*
import std.time.*

/**
 * 简化的状态机Actor
 */
public struct SimpleStateMachineActor<TState, TEvent, TContext> where TContext <: Equatable {
    private var currentState: TState
    private var context: TContext

    public init(initialState: TState, initialContext: TContext) {
        this.currentState = initialState
        this.context = initialContext
    }

    /**
     * 获取当前状态
     */
    public func getCurrentState(): TState {
        currentState
    }

    /**
     * 获取当前上下文
     */
    public func getContext(): TContext {
        context
    }

    /**
     * 更新状态
     */
    public mut func updateState(newState: TState): Unit {
        currentState = newState
    }

    /**
     * 更新上下文
     */
    public mut func updateContext(newContext: TContext): Unit {
        context = newContext
    }
}
