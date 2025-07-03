/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 状态机功能验证
 * 验证XState实现的基本功能
 */
package cactor.examples.xstate_demo

import std.collection.*
import std.time.*

/**
 * XState功能验证器
 */
public struct XStateValidation {
    
    /**
     * 运行所有验证
     */
    public static func runAllValidations(): Unit {
        println("🧪 CActor XState - 功能验证")
        println("=" * 50)
        
        var passedTests = 0
        var totalTests = 0
        
        // 验证状态类型
        totalTests += 1
        if (validateStateTypes()) {
            passedTests += 1
        }
        
        // 验证事件类型
        totalTests += 1
        if (validateEventTypes()) {
            passedTests += 1
        }
        
        // 验证上下文类型
        totalTests += 1
        if (validateContextTypes()) {
            passedTests += 1
        }
        
        // 验证状态机工厂
        totalTests += 1
        if (validateStateMachineFactory()) {
            passedTests += 1
        }
        
        println("\n📊 验证结果:")
        println("   通过: ${passedTests}/${totalTests}")
        println("   成功率: ${(passedTests * 100) / totalTests}%")
        
        if (passedTests == totalTests) {
            println("🎉 所有验证通过!")
        } else {
            println("⚠️  有验证失败，请检查实现")
        }
    }
    
    /**
     * 验证状态类型定义
     */
    private static func validateStateTypes(): Bool {
        println("🧪 验证状态类型定义...")

        try {
            // 测试状态创建
            let inactiveState = ToggleState.Inactive
            let activeState = ToggleState.Active

            // 验证状态ID
            if (inactiveState.getStateId() != "inactive") {
                println("❌ 状态ID验证失败: expected 'inactive', got '${inactiveState.getStateId()}'")
                return false
            }

            if (activeState.getStateId() != "active") {
                println("❌ 状态ID验证失败: expected 'active', got '${activeState.getStateId()}'")
                return false
            }

            // 验证状态显示名称
            if (inactiveState.getDisplayName() != "非活跃") {
                println("❌ 状态显示名称验证失败")
                return false
            }

            // 验证状态相等性
            if (!inactiveState.equals(ToggleState.Inactive)) {
                println("❌ 状态相等性验证失败")
                return false
            }

            println("✅ 状态类型定义验证通过")
            return true

        } catch (e: Exception) {
            println("❌ 状态类型定义验证失败: ${e.message}")
            return false
        }
    }
    
    /**
     * 验证事件类型定义
     */
    private static func validateEventTypes(): Bool {
        println("🧪 验证事件类型定义...")
        
        try {
            // 测试事件创建
            let toggleEvent = ToggleEvent.Toggle
            let resetEvent = ToggleEvent.Reset
            
            // 验证事件类型
            if (toggleEvent.getEventType() != "toggle") {
                println("❌ 事件类型验证失败: expected 'toggle', got '${toggleEvent.getEventType()}'")
                return false
            }
            
            if (resetEvent.getEventType() != "reset") {
                println("❌ 事件类型验证失败: expected 'reset', got '${resetEvent.getEventType()}'")
                return false
            }
            
            // 验证消息接口
            if (toggleEvent.messageType() != "ToggleEvent") {
                println("❌ 消息类型验证失败")
                return false
            }
            
            println("✅ 事件类型定义验证通过")
            return true
            
        } catch (e: Exception) {
            println("❌ 事件类型定义验证失败: ${e.message}")
            return false
        }
    }
    
    /**
     * 验证上下文类型定义
     */
    private static func validateContextTypes(): Bool {
        println("🧪 验证上下文类型定义...")
        
        try {
            // 测试上下文创建
            let context1 = ToggleContext.create()
            let context2 = ToggleContext(5, 123456789, 10)
            
            // 验证默认值
            if (context1.count != 0) {
                println("❌ 上下文默认值验证失败: count应该是0")
                return false
            }
            
            // 验证自定义值
            if (context2.count != 5) {
                println("❌ 上下文自定义值验证失败: count应该是5")
                return false
            }
            
            // 验证相等性
            if (!context1.equals(ToggleContext.create())) {
                println("❌ 上下文相等性验证失败")
                return false
            }
            
            // 验证不可变更新
            let updatedContext = context1.incrementCount()
            if (updatedContext.count != 1) {
                println("❌ 上下文更新验证失败: count应该是1")
                return false
            }
            
            if (context1.count != 0) {
                println("❌ 上下文不可变性验证失败: 原对象应该不变")
                return false
            }
            
            println("✅ 上下文类型定义验证通过")
            return true
            
        } catch (e: Exception) {
            println("❌ 上下文类型定义验证失败: ${e.message}")
            return false
        }
    }
    
    /**
     * 验证状态机工厂
     */
    private static func validateStateMachineFactory(): Bool {
        println("🧪 验证状态机工厂...")

        try {
            // 创建状态机
            let result = SimpleToggleMachineFactory.create()

            // 验证返回结果
            if (result.isEmpty()) {
                println("❌ 状态机工厂验证失败: 返回结果为空")
                return false
            }

            if (!result.contains("状态机")) {
                println("❌ 状态机工厂验证失败: 返回结果不包含'状态机'")
                return false
            }

            println("✅ 状态机工厂验证通过")
            return true

        } catch (e: Exception) {
            println("❌ 状态机工厂验证失败: ${e.message}")
            return false
        }
    }
}

/**
 * 验证主函数
 */
main(): Unit {
    XStateValidation.runAllValidations()
}
