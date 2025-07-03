/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 简单切换状态机示例
 * 展示基于仓颉语言特性的XState基本概念
 */
package cactor.examples.xstate_demo

import std.collection.*
import std.time.*
import cactor.xstate.core.types.{StateMachineState, StateMachineEvent}

/**
 * 切换状态枚举
 * 利用仓颉枚举实现强类型状态定义
 */
public enum ToggleState <: StateMachineState {
    | Inactive
    | Active

    public func getStateId(): String {
        match (this) {
            case Inactive => "inactive"
            case Active => "active"
        }
    }

    public func getDisplayName(): String {
        match (this) {
            case Inactive => "非活跃"
            case Active => "活跃"
        }
    }

    public func equals(other: StateMachineState): Bool {
        getStateId() == other.getStateId()
    }
}

/**
 * 切换事件枚举
 * 利用仓颉枚举实现强类型事件定义
 */
public enum ToggleEvent <: StateMachineEvent {
    | Toggle
    | Reset
    | GetCount

    public func getEventType(): String {
        match (this) {
            case Toggle => "toggle"
            case Reset => "reset"
            case GetCount => "get_count"
        }
    }

    public func getEventData(): HashMap<String, Any> {
        HashMap<String, Any>()
    }

    public func getSource(): Option<String> {
        Some("user")
    }

    // Message接口实现
    public func messageType(): String { "ToggleEvent" }
    public func priority(): Int32 { 0 }
    public func messageId(): String { "toggle-event" }
    public func timestamp(): Int64 { DateTime.now().toUnixTimeStamp().toNanoseconds() }
}

/**
 * 切换上下文
 * 利用仓颉结构体实现强类型上下文
 */
public struct ToggleContext {
    public let count: Int64
    public let lastToggleTime: Int64
    public let totalToggles: Int64

    public init(count: Int64, lastToggleTime: Int64, totalToggles: Int64) {
        this.count = count
        this.lastToggleTime = lastToggleTime
        this.totalToggles = totalToggles
    }

    /**
     * 默认构造函数
     */
    public static func create(): ToggleContext {
        ToggleContext(0, 0, 0)
    }

    /**
     * 相等性比较
     */
    public func equals(other: ToggleContext): Bool {
        count == other.count &&
        lastToggleTime == other.lastToggleTime &&
        totalToggles == other.totalToggles
    }

    /**
     * 不可变更新方法
     */
    public func incrementCount(): ToggleContext {
        ToggleContext(
            count + 1,
            DateTime.now().toUnixTimeStamp().toNanoseconds(),
            totalToggles + 1
        )
    }

    public func reset(): ToggleContext {
        ToggleContext(0, DateTime.now().toUnixTimeStamp().toNanoseconds(), totalToggles)
    }
}

/**
 * 简单切换状态机工厂
 * 展示如何创建基本的状态机
 */
public struct SimpleToggleMachineFactory {

    /**
     * 创建简单的切换状态机
     */
    public static func create(): String {
        "简单切换状态机已创建"
    }

    /**
     * 演示状态转换
     */
    public static func demonstrateTransition(): Unit {
        println("🎭 状态机转换演示:")

        // 创建初始状态和上下文
        var currentState = ToggleState.Inactive
        var context = ToggleContext.create()

        println("   初始状态: ${currentState.getDisplayName()}")
        println("   初始上下文: count=${context.count}")

        // 模拟第一次切换
        println("\n   发送Toggle事件...")
        currentState = ToggleState.Active
        context = context.incrementCount()
        println("   新状态: ${currentState.getDisplayName()}")
        println("   新上下文: count=${context.count}")

        // 模拟第二次切换
        println("\n   再次发送Toggle事件...")
        currentState = ToggleState.Inactive
        context = context.incrementCount()
        println("   新状态: ${currentState.getDisplayName()}")
        println("   新上下文: count=${context.count}")

        // 模拟重置
        println("\n   发送Reset事件...")
        currentState = ToggleState.Inactive
        context = context.reset()
        println("   新状态: ${currentState.getDisplayName()}")
        println("   新上下文: count=${context.count}")
    }
}

/**
 * 切换状态机演示
 * 展示基本的状态机概念
 */
public struct ToggleMachineDemo {

    /**
     * 运行演示
     */
    public static func run(): Unit {
        println("🎭 CActor XState - 简单切换状态机演示")
        println("=" * 50)

        try {
            println("✅ 开始XState基本概念演示")

            // 演示状态机工厂
            let result = SimpleToggleMachineFactory.create()
            println("✅ ${result}")

            // 演示状态转换
            println("\n🔄 状态转换演示:")
            SimpleToggleMachineFactory.demonstrateTransition()

            println("\n✅ 演示完成!")

        } catch (e: Exception) {
            println("❌ 演示失败: ${e.message}")
        }
    }
}

/**
 * 主函数 - 运行演示
 */
main(): Unit {
    ToggleMachineDemo.run()
}
