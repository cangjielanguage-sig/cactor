/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 状态机定义包
 * 简化的状态机定义
 */
package cactor.xstate.core.machine

import std.collection.*
import std.time.*

/**
 * 简化的状态机定义
 */
public struct SimpleMachineDefinition<TState, TEvent, TContext> where TContext <: Equatable {
    public let id: String
    public let version: String
    public let initialState: TState
    public let initialContext: TContext

    public init(
        id: String,
        version: String,
        initialState: TState,
        initialContext: TContext
    ) {
        this.id = id
        this.version = version
        this.initialState = initialState
        this.initialContext = initialContext
    }

    /**
     * 验证状态机定义
     */
    public func validate(): Array<String> {
        let errors = ArrayList<String>()
        if (id.isEmpty()) {
            errors.add("状态机ID不能为空")
        }
        return errors.toArray()
    }
}
