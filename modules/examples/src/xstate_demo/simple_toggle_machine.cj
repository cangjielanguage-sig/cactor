/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 简单切换状态机示例
 * 展示基于仓颉语言特性和CActor框架的XState实现
 */
package cactor_examples.xstate_demo

import std.collection.*
import std.time.*
import cactor_xstate.core.types.{StateMachineState, StateMachineEvent}
import cactor_xstate.core.machine.{StateMachineDefinition, StateMachineBuilder, StateNode}
import cactor_xstate.core.types.{TransitionDefinition, TransitionBuilder}
import cactor_xstate.core.engine.{PatternTransitionEngine}

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
    
    public func isFinal(): Bool {
        false
    }
    
    public func isInitial(): Bool {
        match (this) {
            case Inactive => true
            case _ => false
        }
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
    
    public func matchesType(eventType: String): Bool {
        getEventType() == eventType
    }
    
    // Message接口实现
    public func messageType(): String { "ToggleEvent" }
    public func priority(): Int32 { 0 }
    public func messageId(): String { "toggle-event-${DateTime.now().toUnixTimeStamp().toNanoseconds()}" }
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
 * 展示如何使用构建器模式创建状态机
 */
public struct SimpleToggleMachineFactory {
    
    /**
     * 创建简单的切换状态机定义
     */
    public static func create(): StateMachineDefinition {
        
        // 创建状态节点
        let inactiveState = StateNode.create("inactive")
        let activeState = StateNode.create("active")
        
        // 创建转换定义
        var transitionBuilder = TransitionBuilder()
        transitionBuilder.from("inactive")
        transitionBuilder.to("active")
        transitionBuilder.on("toggle")
        transitionBuilder.withPriority(1)
        let inactiveToActive = transitionBuilder.build()
        
        transitionBuilder = TransitionBuilder()
        transitionBuilder.from("active")
        transitionBuilder.to("inactive")
        transitionBuilder.on("toggle")
        transitionBuilder.withPriority(1)
        let activeToInactive = transitionBuilder.build()
        
        transitionBuilder = TransitionBuilder()
        transitionBuilder.from("active")
        transitionBuilder.to("inactive")
        transitionBuilder.on("reset")
        transitionBuilder.withPriority(2)
        let resetTransition = transitionBuilder.build()
        
        transitionBuilder = TransitionBuilder()
        transitionBuilder.from("inactive")
        transitionBuilder.to("inactive")
        transitionBuilder.on("reset")
        transitionBuilder.withPriority(2)
        let resetFromInactive = transitionBuilder.build()
        
        // 使用构建器创建状态机定义
        var machineBuilder = StateMachineBuilder()
        machineBuilder.withId("simple-toggle-machine")
        machineBuilder.withVersion("1.0.0")
        machineBuilder.withInitialState("inactive")
        machineBuilder.addState(inactiveState)
        machineBuilder.addState(activeState)
        machineBuilder.addTransition(inactiveToActive)
        machineBuilder.addTransition(activeToInactive)
        machineBuilder.addTransition(resetTransition)
        machineBuilder.addTransition(resetFromInactive)
        machineBuilder.addMeta("description", "简单的切换状态机示例")
        machineBuilder.addMeta("author", "CActor XState")
        
        return machineBuilder.build()
    }
    
    /**
     * 创建转换引擎
     */
    public static func createEngine(): PatternTransitionEngine {
        let machineDefinition = create()
        return PatternTransitionEngine(machineDefinition)
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
 * 展示如何使用状态机Actor
 */
public struct ToggleMachineDemo {
    
    /**
     * 运行演示
     */
    public static func run(): Unit {
        println("🎭 CActor XState - 简单切换状态机演示")
        println("=" * 50)
        
        try {
            println("✅ 开始XState核心功能演示")
            
            // 演示状态机定义创建
            let machineDefinition = SimpleToggleMachineFactory.create()
            println("✅ 状态机定义已创建: ${machineDefinition.id}")
            
            // 验证状态机定义
            let validationErrors = machineDefinition.validate()
            if (!validationErrors.isEmpty()) {
                println("❌ 状态机验证失败:")
                for (error in validationErrors) {
                    println("   - ${error}")
                }
                return
            }
            println("✅ 状态机定义验证通过")
            
            // 演示基本状态转换
            println("\n🔄 基本状态转换演示:")
            SimpleToggleMachineFactory.demonstrateTransition()
            
            // 演示转换引擎集成
            println("\n🎬 转换引擎集成演示:")
            demonstrateEngineIntegration()
            
            println("\n✅ 演示完成!")
            
        } catch (e: Exception) {
            println("❌ 演示失败: ${e.message}")
        }
    }
    
    /**
     * 演示转换引擎集成
     */
    private static func demonstrateEngineIntegration(): Unit {
        try {
            // 创建转换引擎
            let engine = SimpleToggleMachineFactory.createEngine()
            println("   ✅ 转换引擎已创建")

            // 获取引擎统计信息
            let stats = engine.getStatistics()
            println("   📊 引擎统计: 转换数=${stats.transitionCount}, 状态数=${stats.stateCount}")

            // 模拟状态转换
            println("   🔄 模拟状态转换...")
            let toggleEvent = ToggleEvent.Toggle
            println("   📨 事件类型: ${toggleEvent.getEventType()}")

            // 处理转换
            let result = engine.processTransition("inactive", toggleEvent)

            if (result.isSuccess()) {
                println("   ✅ 转换成功: inactive -> ${result.targetStateId}")
                println("   ⏱  执行时间: ${result.executionTime}ns")
            } else {
                println("   ❌ 转换失败: ${result.errorMessage}")
            }

            println("   ✅ 转换引擎集成演示完成")

        } catch (e: Exception) {
            println("   ❌ 转换引擎集成演示失败: ${e.message}")
        }
    }
}

