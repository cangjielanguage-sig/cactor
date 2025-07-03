/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 核心类型包
 * 简化的类型定义，避免复杂的包依赖
 */
package cactor.xstate.core.types

import std.collection.*
import std.time.*
import cactor.core.message.{Message}

/**
 * 状态机状态基础接口
 */
public interface StateMachineState {
    func getStateId(): String
    func getDisplayName(): String { getStateId() }
    func equals(other: StateMachineState): Bool { getStateId() == other.getStateId() }
}

/**
 * 状态机事件基础接口
 */
public interface StateMachineEvent <: Message {
    func getEventType(): String
    func getEventData(): HashMap<String, Any> { HashMap<String, Any>() }
    func getSource(): Option<String> { None }
    func matchesType(eventType: String): Bool { getEventType() == eventType }
    func matchesPattern(pattern: String): Bool {
        let eventType = getEventType()
        // 简化的模式匹配，只支持精确匹配
        return eventType == pattern
    }
}
