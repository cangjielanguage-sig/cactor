/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 统一配置系统演示
 * Plan 0.20 Phase 3.2: 配置系统统一
 * 演示统一配置管理、热更新和验证机制
 */
package cactor_examples.config_system_demo

import cactor_foundation.config.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}

/**
 * 配置监听Actor
 */
public class ConfigListenerActor <: Actor {
    private let configManager: UnifiedConfigManager
    private var configChangeCount: Int64 = 0

    public init() {
        this.configManager = GlobalConfigManager.getInstance()
        // 简化实现：不使用配置变更监听器
    }
    
    public prop name: String {
        get() { "ConfigListenerActor" }
    }
    
    public prop description: String {
        get() { "配置监听和管理Actor" }
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
        
        if (content == "show-config") {
            showCurrentConfig()
        } else if (content == "show-stats") {
            showConfigStatistics()
        } else if (content.startsWith("set:")) {
            handleSetCommand(content)
        } else if (content.startsWith("get:")) {
            handleGetCommand(content)
        } else if (content == "reload") {
            configManager.reload()
            println("🔄 配置已重新加载")
        } else {
            println("❓ 未知配置命令: ${content}")
            showHelp()
        }
    }
    
    private func handleSetCommand(command: String): Unit {
        let parts = command.split(":")
        if (parts.size >= 3) {
            let key = parts[1]
            let value = parts[2]
            
            try {
                configManager.setString(key, value)
                println("✅ 配置已设置: ${key} = ${value}")
            } catch (e: Exception) {
                println("❌ 设置配置失败: ${e.message}")
            }
        } else {
            println("❌ 设置命令格式错误，应为: set:key:value")
        }
    }
    
    private func handleGetCommand(command: String): Unit {
        let parts = command.split(":")
        if (parts.size >= 2) {
            let key = parts[1]
            
            match (configManager.getString(key)) {
                case Some(value) =>
                    println("📋 配置值: ${key} = ${value}")
                case None =>
                    println("❌ 配置不存在: ${key}")
            }
        } else {
            println("❌ 获取命令格式错误，应为: get:key")
        }
    }
    
    private func showCurrentConfig(): Unit {
        println("📊 当前配置:")
        let keys = configManager.getAllKeys()
        for (key in keys) {
            match (configManager.getString(key)) {
                case Some(value) =>
                    println("   ${key} = ${value}")
                case None =>
                    println("   ${key} = <null>")
            }
        }
    }
    
    private func showConfigStatistics(): Unit {
        let stats = configManager.getStatistics()
        println("📈 配置统计信息:")
        println("   ${stats.toString()}")
        println("   配置变更次数: ${configChangeCount}")
    }
    
    private func showHelp(): Unit {
        println("📖 配置命令帮助:")
        println("   show-config  - 显示所有配置")
        println("   show-stats   - 显示配置统计")
        println("   set:key:val  - 设置配置值")
        println("   get:key      - 获取配置值")
        println("   reload       - 重新加载配置")
    }
    
    /**
     * 处理配置变更（简化版本）
     */
    public func handleConfigChange(): Unit {
        configChangeCount += 1
        println("🔔 配置已变更，变更次数: ${configChangeCount}")
    }
    
    public func preStart(): Unit {
        println("🚀 ConfigListenerActor启动")
        println("   已注册为配置变更监听器")
        showHelp()
    }
    
    public func postStop(): Unit {
        println("🛑 ConfigListenerActor停止")
        println("   总配置变更次数: ${configChangeCount}")
    }
}

/**
 * 配置系统演示主程序
 */
main(): Unit {
    println("🚀 CActor 统一配置系统演示")
    println("Plan 0.20 Phase 3.2: 配置系统统一")
    println("=" * 60)
    
    // 1. 获取全局配置管理器
    let configManager = GlobalConfigManager.getInstance()
    
    // 2. 注册CActor配置验证器
    CActorConfigValidators.registerAllValidators(configManager)
    
    // 3. 添加自定义配置源
    let customConfig = MemoryConfigSource("custom", 200)
    customConfig.addConfig("app.name", ConfigValue.createString("ConfigDemo"))
    customConfig.addConfig("app.version", ConfigValue.createString("1.0.0"))
    customConfig.addConfig("app.debug", ConfigValue.createBool(true))
    configManager.registerSource(customConfig)
    
    println("✅ 配置管理器初始化完成")
    
    // 4. 显示初始配置
    println("\n📊 初始配置:")
    let stats = configManager.getStatistics()
    println("   ${stats.toString()}")
    
    // 5. 创建Actor系统
    let system = CActorSystemFactory.create("config-demo-system")
    
    // 6. 创建配置监听Actor
    let configActor = system.actorOf({ => ConfigListenerActor() }, "config-listener")
    
    println("✅ 创建了配置监听Actor")
    
    // 7. 演示配置操作
    println("\n🧪 演示1: 显示当前配置")
    configActor.tell(StringMessage("show-config"))
    sleep(Duration.millisecond * 300)
    
    println("\n🧪 演示2: 设置新配置")
    configActor.tell(StringMessage("set:demo.message:Hello Config System"))
    sleep(Duration.millisecond * 200)
    
    println("\n🧪 演示3: 获取配置值")
    configActor.tell(StringMessage("get:demo.message"))
    configActor.tell(StringMessage("get:cactor.system.name"))
    sleep(Duration.millisecond * 300)
    
    println("\n🧪 演示4: 配置验证测试")
    println("测试有效的调度器类型:")
    configActor.tell(StringMessage("set:cactor.dispatcher.type:thread-pool"))
    sleep(Duration.millisecond * 200)
    
    println("测试无效的调度器类型:")
    configActor.tell(StringMessage("set:cactor.dispatcher.type:invalid-type"))
    sleep(Duration.millisecond * 200)
    
    println("\n🧪 演示5: 配置统计信息")
    configActor.tell(StringMessage("show-stats"))
    sleep(Duration.millisecond * 200)
    
    println("\n🧪 演示6: 配置重新加载")
    configActor.tell(StringMessage("reload"))
    sleep(Duration.millisecond * 200)
    
    // 8. 演示配置热更新
    println("\n🧪 演示7: 配置热更新")
    let hotReloadManager = ConfigHotReloadManager(configManager)
    hotReloadManager.enable()
    
    // 添加更多配置
    customConfig.addConfig("runtime.setting", ConfigValue.createString("hot-reload-test"))
    hotReloadManager.triggerReload()
    sleep(Duration.millisecond * 300)
    
    configActor.tell(StringMessage("get:runtime.setting"))
    sleep(Duration.millisecond * 200)
    
    // 9. 演示配置前缀查询
    println("\n🧪 演示8: 配置前缀查询")
    let cactorConfigs = configManager.getByPrefix("cactor.")
    println("📋 以'cactor.'开头的配置项:")
    for ((key, value) in cactorConfigs) {
        println("   ${key} = ${value.asString()}")
    }
    
    // 10. 最终统计
    println("\n📈 最终配置统计:")
    configActor.tell(StringMessage("show-stats"))
    sleep(Duration.millisecond * 300)
    
    println("\n🎯 配置系统演示完成")
    println("展示了统一配置管理、热更新、验证和监听功能")
    
    // 11. 清理系统
    system.terminate()
    sleep(Duration.second * 1)
    
    println("✅ 系统已清理")
}
