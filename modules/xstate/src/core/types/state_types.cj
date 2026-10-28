/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 状态类型定义
 * 基于仓颉0.60.5语言特性的强类型状态系统
 */
package cactor_xstate.core.types

import std.collection.*
import cactor_core.message.{Message}

/**
 * 状态机状态基础接口
 * 利用仓颉接口系统定义状态的基本行为
 */
public interface StateMachineState {
    /**
     * 获取状态的唯一标识符
     */
    func getStateId(): String
    
    /**
     * 获取状态的显示名称
     */
    func getDisplayName(): String {
        getStateId()
    }
    
    /**
     * 检查状态是否相等
     */
    func equals(other: StateMachineState): Bool {
        getStateId() == other.getStateId()
    }
    
    /**
     * 检查是否为最终状态
     */
    func isFinal(): Bool {
        false
    }
    
    /**
     * 检查是否为初始状态
     */
    func isInitial(): Bool {
        false
    }
}

/**
 * 状态机事件基础接口
 * 继承CActor的Message接口，确保与Actor系统兼容
 */
public interface StateMachineEvent <: Message {
    /**
     * 获取事件类型
     */
    func getEventType(): String
    
    /**
     * 获取事件数据
     */
    func getEventData(): HashMap<String, Any> {
        HashMap<String, Any>()
    }
    
    /**
     * 获取事件来源
     */
    func getSource(): Option<String> {
        None
    }
    
    /**
     * 检查事件类型是否匹配
     */
    func matchesType(eventType: String): Bool {
        getEventType() == eventType
    }
}
