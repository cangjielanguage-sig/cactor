/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 泛型支持包
 * 提供泛型相关的类型约束和辅助功能
 */
package cactor.xstate.lang.generics

import cactor.xstate.core.types.*

/**
 * 泛型类型约束辅助
 */
public struct GenericConstraints {
    /**
     * 验证状态类型约束
     */
    public static func validateStateType<TState>(): Bool where TState <: StateMachineState {
        true
    }
    
    /**
     * 验证事件类型约束
     */
    public static func validateEventType<TEvent>(): Bool where TEvent <: StateMachineEvent {
        true
    }
    
    /**
     * 验证上下文类型约束
     */
    public static func validateContextType<TContext>(): Bool where TContext <: Equatable {
        true
    }
}
