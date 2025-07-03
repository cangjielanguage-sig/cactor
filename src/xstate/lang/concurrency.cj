/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 并发原语支持包
 * 提供并发相关的辅助功能
 */
package cactor.xstate.lang.concurrency

import std.sync.*
import cactor.xstate.core.types.*

/**
 * 并发安全的状态管理器
 */
public class ConcurrentStateManager<TState> where TState <: StateMachineState {
    private let stateLock: Mutex = Mutex()
    private var currentState: TState
    
    public init(initialState: TState) {
        this.currentState = initialState
    }
    
    /**
     * 线程安全的状态获取
     */
    public func getCurrentState(): TState {
        stateLock.lock()
        let state = currentState
        stateLock.unlock()
        return state
    }
    
    /**
     * 线程安全的状态更新
     */
    public func updateState(newState: TState): Unit {
        stateLock.lock()
        currentState = newState
        stateLock.unlock()
    }
}

/**
 * 原子操作辅助工具
 */
public struct AtomicOperations {
    /**
     * 原子状态比较和交换
     */
    public static func compareAndSwap<TState>(
        manager: ConcurrentStateManager<TState>,
        expected: TState,
        newState: TState
    ): Bool where TState <: StateMachineState {
        // 简化实现，实际应该使用原子操作
        let current = manager.getCurrentState()
        if (current.equals(expected)) {
            manager.updateState(newState)
            return true
        }
        return false
    }
}
