/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 简化配置系统演示
 * Plan 0.20 Phase 3.2: 配置系统统一 (简化版本)
 * 演示简化配置管理功能，解决语言兼容性问题
 */
package cactor_examples.simple_config_demo

import std.time.*
import std.collection.*
import cactor_foundation.simple_config.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

/**
 * 简化配置演示Actor
 */
public class SimpleConfigActor <: Actor {
    private let configManager: SimpleConfigManager
    private var operationCount: Int64 = 0
    
    public init() {
        this.configManager = GlobalSimpleConfig.getInstance()
    }
    
    public prop name: String {
        get() { "SimpleConfigActor" }
    }
    
    public prop description: String {
        get() { "简化配置演示Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                processConfigCommand(msg)
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    private func processConfigCommand(msg: StringMessage): Unit {
        let content = msg.getContent()
        operationCount += 1
        
        if (content == "show-all") {
            showAllConfigs()
        } else if (content == "show-stats") {
            showConfigStats()
        } else if (content.startsWith("set-string:")) {
            handleSetStringCommand(content)
        } else if (content.startsWith("set-int:")) {
            handleSetIntCommand(content)
        } else if (content.startsWith("set-bool:")) {
            handleSetBoolCommand(content)
        } else if (content.startsWith("get:")) {
            handleGetCommand(content)
        } else if (content == "reload") {
            configManager.reload()
            println("🔄 配置已重新加载")
        } else if (content == "test-defaults") {
            testDefaultConfigs()
        } else {
            println("❓ 未知配置命令: ${content}")
            showHelp()
        }
    }
    
    private func handleSetStringCommand(command: String): Unit {
        // 简化解析：假设格式为 set-string:key:value
        let parts = command.split(":")
        if (parts.size >= 3) {
            let key = parts[1]
            let value = parts[2]
            configManager.setString(key, value)
            println("✅ 字符串配置已设置: ${key} = ${value}")
        } else {
            println("❌ 设置命令格式错误，应为: set-string:key:value")
        }
    }
    
    private func handleSetIntCommand(command: String): Unit {
        // 简化解析：假设格式为 set-int:key:value
        let parts = command.split(":")
        if (parts.size >= 3) {
            let key = parts[1]
            let valueStr = parts[2]
            // 简化的整数解析
            let value: Int32 = if (valueStr == "1") { 1 }
                              else if (valueStr == "10") { 10 }
                              else if (valueStr == "100") { 100 }
                              else { 0 }
            configManager.setInt(key, value)
            println("✅ 整数配置已设置: ${key} = ${value}")
        } else {
            println("❌ 设置命令格式错误，应为: set-int:key:value")
        }
    }
    
    private func handleSetBoolCommand(command: String): Unit {
        // 简化解析：假设格式为 set-bool:key:value
        let parts = command.split(":")
        if (parts.size >= 3) {
            let key = parts[1]
            let valueStr = parts[2]
            let value = valueStr == "true" || valueStr == "1"
            configManager.setBool(key, value)
            println("✅ 布尔配置已设置: ${key} = ${value}")
        } else {
            println("❌ 设置命令格式错误，应为: set-bool:key:value")
        }
    }
    
    private func handleGetCommand(command: String): Unit {
        let parts = command.split(":")
        if (parts.size >= 2) {
            let key = parts[1]
            
            // 尝试获取不同类型的配置值
            match (configManager.getString(key)) {
                case Some(value) =>
                    println("📋 字符串配置: ${key} = ${value}")
                case None =>
                    match (configManager.getInt(key)) {
                        case Some(value) =>
                            println("📋 整数配置: ${key} = ${value}")
                        case None =>
                            match (configManager.getBool(key)) {
                                case Some(value) =>
                                    println("📋 布尔配置: ${key} = ${value}")
                                case None =>
                                    println("❌ 配置不存在: ${key}")
                            }
                    }
            }
        } else {
            println("❌ 获取命令格式错误，应为: get:key")
        }
    }
    
    private func showAllConfigs(): Unit {
        println("📊 所有配置:")
        configManager.printAllConfigs()
    }
    
    private func showConfigStats(): Unit {
        println("📈 配置统计信息:")
        println("   配置项数量: ${configManager.getConfigCount()}")
        println("   操作次数: ${operationCount}")
        
        // 简化实现：显示一些已知的配置键
        println("   配置键列表: (部分)")
        println("     - cactor.system.name")
        println("     - cactor.dispatcher.type")
        println("     - app.name")
        println("     - app.version")
    }
    
    private func testDefaultConfigs(): Unit {
        println("🧪 测试默认配置:")
        
        // 测试CActor系统配置
        let systemName = GlobalSimpleConfig.getString("cactor.system.name", "Unknown")
        let dispatcherType = GlobalSimpleConfig.getString("cactor.dispatcher.type", "Unknown")
        let threadPoolSize = GlobalSimpleConfig.getInt("cactor.thread.pool.core.size", 0)
        let monitoring = GlobalSimpleConfig.getBool("cactor.performance.monitoring", false)
        
        println("   系统名称: ${systemName}")
        println("   调度器类型: ${dispatcherType}")
        println("   线程池大小: ${threadPoolSize}")
        println("   性能监控: ${monitoring}")
        
        // 测试应用配置
        let appName = GlobalSimpleConfig.getString("app.name", "Unknown")
        let appVersion = GlobalSimpleConfig.getString("app.version", "Unknown")
        let debugMode = GlobalSimpleConfig.getBool("app.debug", false)
        
        println("   应用名称: ${appName}")
        println("   应用版本: ${appVersion}")
        println("   调试模式: ${debugMode}")
    }
    
    private func showHelp(): Unit {
        println("📖 简化配置命令帮助:")
        println("   show-all         - 显示所有配置")
        println("   show-stats       - 显示配置统计")
        println("   set-string:k:v   - 设置字符串配置")
        println("   set-int:k:v      - 设置整数配置")
        println("   set-bool:k:v     - 设置布尔配置")
        println("   get:key          - 获取配置值")
        println("   reload           - 重新加载配置")
        println("   test-defaults    - 测试默认配置")
    }
    
    public func preStart(): Unit {
        println("🚀 SimpleConfigActor启动")
        println("   使用简化配置管理系统")
        showHelp()
    }
    
    public func postStop(): Unit {
        println("🛑 SimpleConfigActor停止")
        println("   总操作次数: ${operationCount}")
    }
}

/**
 * 简化配置系统演示主程序
 */
main(): Unit {
    println("🚀 CActor 简化配置系统演示")
    println("Plan 0.20 Phase 3.2: 配置系统统一 (简化版本)")
    println("=" * 60)
    
    // 1. 获取全局配置管理器
    let configManager = GlobalSimpleConfig.getInstance()
    
    println("✅ 简化配置管理器初始化完成")
    
    // 2. 显示初始配置统计
    println("\n📊 初始配置统计:")
    println("   配置项数量: ${configManager.getConfigCount()}")
    
    // 3. 创建Actor系统
    let system = CActorSystemFactory.create("simple-config-demo")
    
    // 4. 创建配置演示Actor
    let configActor = system.actorOf({ => SimpleConfigActor() }, "simple-config-actor")
    
    println("✅ 创建了简化配置演示Actor")
    
    // 5. 演示配置操作
    println("\n🧪 演示1: 显示默认配置")
    configActor.tell(StringMessage("test-defaults"))
    sleep(Duration.millisecond * 300)
    
    println("\n🧪 演示2: 设置新配置")
    configActor.tell(StringMessage("set-string:demo.message:Hello Simple Config"))
    configActor.tell(StringMessage("set-int:demo.count:10"))
    configActor.tell(StringMessage("set-bool:demo.enabled:true"))
    sleep(Duration.millisecond * 400)
    
    println("\n🧪 演示3: 获取配置值")
    configActor.tell(StringMessage("get:demo.message"))
    configActor.tell(StringMessage("get:demo.count"))
    configActor.tell(StringMessage("get:demo.enabled"))
    configActor.tell(StringMessage("get:cactor.system.name"))
    sleep(Duration.millisecond * 500)
    
    println("\n🧪 演示4: 显示所有配置")
    configActor.tell(StringMessage("show-all"))
    sleep(Duration.millisecond * 300)
    
    println("\n🧪 演示5: 配置统计信息")
    configActor.tell(StringMessage("show-stats"))
    sleep(Duration.millisecond * 300)
    
    println("\n🧪 演示6: 配置重新加载")
    configActor.tell(StringMessage("reload"))
    sleep(Duration.millisecond * 200)
    
    println("\n🧪 演示7: 验证重新加载后的配置")
    configActor.tell(StringMessage("get:demo.message"))  // 应该不存在了
    configActor.tell(StringMessage("get:cactor.system.name"))  // 应该恢复默认值
    sleep(Duration.millisecond * 300)
    
    println("\n🎯 简化配置系统演示完成")
    println("展示了基本的配置管理功能，解决了语言兼容性问题")
    
    // 8. 清理系统
    system.terminate()
    sleep(Duration.second * 1)
    
    println("✅ 系统已清理")
}
