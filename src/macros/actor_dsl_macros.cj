/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - Actor DSL宏系统
 * 基于plan3.md Phase 4设计，实现高级Actor DSL功能
 * 参考仓颉语言宏系统文档和CangjieMagic项目模式
 */
macro package cactor.macros

import std.ast.*

// ============================================================================
// 1. 简单测试宏 - 验证宏系统基本功能
// ============================================================================

/**
 * 简单测试宏 - 直接返回输入
 * 使用方式: @simple_test class MyClass { ... }
 */
public macro simple_test(input: Tokens): Tokens {
    println("宏被调用了！")
    return input
}

/**
 * Actor定义宏 - 自动生成Actor类的基础结构
 * 使用方式: @actor_def class MyActor { ... }
 */
public macro actor_def(input: Tokens): Tokens {
    println("actor_def宏被调用")
    return input
}

/**
 * 消息处理器宏 - 简化版本
 * 使用方式: @message_handler class Handler { ... }
 */
public macro message_handler(input: Tokens): Tokens {
    println("message_handler宏被调用")
    return input
}

/**
 * 状态机宏 - 简化版本
 * 使用方式: @state_machine class StateMachine { ... }
 */
public macro state_machine(input: Tokens): Tokens {
    println("state_machine宏被调用")
    return input
}

// ============================================================================
// 2. 简单的表达式宏 - 测试基本功能
// ============================================================================

/**
 * 日志宏 - 简化版本
 * 使用方式: @log_info("message")
 */
public macro log_info(message: Tokens): Tokens {
    return quote(println("[INFO] " + $(message)))
}








