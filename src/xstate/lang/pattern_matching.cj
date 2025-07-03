/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 模式匹配支持包
 * 提供模式匹配相关的辅助功能
 */
package cactor.xstate.lang.pattern_matching

import cactor.xstate.core.types.*

/**
 * 模式匹配辅助工具
 */
public struct PatternMatchingUtils {
    /**
     * 状态模式匹配辅助
     */
    public static func matchState<TState>(
        state: TState,
        patterns: Array<String>
    ): Bool where TState <: StateMachineState {
        for (pattern in patterns) {
            if (state.getStateId() == pattern) {
                return true
            }
        }
        return false
    }
    
    /**
     * 事件模式匹配辅助
     */
    public static func matchEvent<TEvent>(
        event: TEvent,
        patterns: Array<String>
    ): Bool where TEvent <: StateMachineEvent {
        for (pattern in patterns) {
            if (event.matchesPattern(pattern)) {
                return true
            }
        }
        return false
    }
}
