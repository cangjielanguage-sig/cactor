/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 简单功能测试
 * 验证XState基本功能
 */
package cactor.examples.xstate_demo

import std.collection.*
import std.time.*

/**
 * 简单的XState功能测试
 */
public struct SimpleXStateTest {
    
    /**
     * 运行基本测试
     */
    public static func runBasicTests(): Unit {
        println("🧪 CActor XState - 基本功能测试")
        println("=" * 50)
        
        var passedTests = 0
        var totalTests = 0
        
        // 测试1: 状态类型创建
        totalTests += 1
        if (testStateCreation()) {
            passedTests += 1
        }
        
        // 测试2: 事件类型创建
        totalTests += 1
        if (testEventCreation()) {
            passedTests += 1
        }
        
        // 测试3: 上下文创建
        totalTests += 1
        if (testContextCreation()) {
            passedTests += 1
        }
        
        // 测试4: 状态机定义创建
        totalTests += 1
        if (testStateMachineCreation()) {
            passedTests += 1
        }
        
        // 测试5: 转换引擎创建
        totalTests += 1
        if (testTransitionEngineCreation()) {
            passedTests += 1
        }
        
        println("\n📊 测试结果:")
        println("   通过: ${passedTests}/${totalTests}")
        println("   成功率: ${(passedTests * 100) / totalTests}%")
        
        if (passedTests == totalTests) {
            println("🎉 所有测试通过!")
        } else {
            println("⚠  有测试失败，请检查实现")
        }
    }
    
    /**
     * 测试状态创建
     */
    private static func testStateCreation(): Bool {
        println("🧪 测试状态创建...")
        
        try {
            let inactiveState = ToggleState.Inactive
            let activeState = ToggleState.Active
            
            if (inactiveState.getStateId() == "inactive" && 
                activeState.getStateId() == "active") {
                println("✅ 状态创建测试通过")
                return true
            } else {
                println("❌ 状态创建测试失败")
                return false
            }
        } catch (e: Exception) {
            println("❌ 状态创建测试异常: ${e.message}")
            return false
        }
    }
    
    /**
     * 测试事件创建
     */
    private static func testEventCreation(): Bool {
        println("🧪 测试事件创建...")
        
        try {
            let toggleEvent = ToggleEvent.Toggle
            let resetEvent = ToggleEvent.Reset
            
            if (toggleEvent.getEventType() == "toggle" && 
                resetEvent.getEventType() == "reset") {
                println("✅ 事件创建测试通过")
                return true
            } else {
                println("❌ 事件创建测试失败")
                return false
            }
        } catch (e: Exception) {
            println("❌ 事件创建测试异常: ${e.message}")
            return false
        }
    }
    
    /**
     * 测试上下文创建
     */
    private static func testContextCreation(): Bool {
        println("🧪 测试上下文创建...")
        
        try {
            let context = ToggleContext.create()
            
            if (context.count == 0) {
                println("✅ 上下文创建测试通过")
                return true
            } else {
                println("❌ 上下文创建测试失败")
                return false
            }
        } catch (e: Exception) {
            println("❌ 上下文创建测试异常: ${e.message}")
            return false
        }
    }
    
    /**
     * 测试状态机定义创建
     */
    private static func testStateMachineCreation(): Bool {
        println("🧪 测试状态机定义创建...")
        
        try {
            let machineDefinition = SimpleToggleMachineFactory.create()
            
            if (machineDefinition.id == "simple-toggle-machine" && 
                machineDefinition.initialStateId == "inactive") {
                println("✅ 状态机定义创建测试通过")
                return true
            } else {
                println("❌ 状态机定义创建测试失败")
                return false
            }
        } catch (e: Exception) {
            println("❌ 状态机定义创建测试异常: ${e.message}")
            return false
        }
    }
    
    /**
     * 测试转换引擎创建
     */
    private static func testTransitionEngineCreation(): Bool {
        println("🧪 测试转换引擎创建...")
        
        try {
            let machineDefinition = SimpleToggleMachineFactory.create()
            let engine = SimpleToggleMachineFactory.createEngine()
            
            let stats = engine.getStatistics()
            if (stats.transitionCount > 0) {
                println("✅ 转换引擎创建测试通过")
                return true
            } else {
                println("❌ 转换引擎创建测试失败")
                return false
            }
        } catch (e: Exception) {
            println("❌ 转换引擎创建测试异常: ${e.message}")
            return false
        }
    }
}

/**
 * 主函数
 */
main(): Unit {
    SimpleXStateTest.runBasicTests()
}
