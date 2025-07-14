/*
 * Copyright (c) 2024. All rights reserved.
 * CActor 依赖关系优化演示
 * Plan 0.20 Phase 3.1: 依赖关系优化
 * 演示依赖注入、抽象接口和模块契约的使用
 */
package cactor.examples.dependency_optimization_demo

import std.time.*
import std.collection.*
import cactor.foundation.di.*
import cactor.foundation.abstractions.*
import cactor.foundation.contracts.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor.core.actor.{Actor, ActorRef}
import cactor.core.message.{Message, MessageResult, StringMessage}
import cactor.core.context.{ActorContext}

/**
 * 服务接口示例
 */
public interface IMessageProcessor {
    func processMessage(message: String): String
    func getProcessorName(): String
}

/**
 * 日志服务接口
 */
public interface ILogService {
    func log(level: String, message: String): Unit
    func getLogLevel(): String
}

/**
 * 配置服务接口
 */
public interface IConfigService {
    func getValue(key: String): Option<String>
    func setValue(key: String, value: String): Unit
}

/**
 * 默认消息处理器实现
 */
public class DefaultMessageProcessor <: IMessageProcessor {
    private let processorName: String
    private let logService: ILogService
    
    public init(processorName: String, logService: ILogService) {
        this.processorName = processorName
        this.logService = logService
    }
    
    public func processMessage(message: String): String {
        logService.log("INFO", "处理消息: ${message}")
        let result = "已处理: ${message} (by ${processorName})"
        logService.log("INFO", "处理结果: ${result}")
        return result
    }
    
    public func getProcessorName(): String {
        processorName
    }
}

/**
 * 控制台日志服务实现
 */
public class ConsoleLogService <: ILogService {
    private let logLevel: String
    
    public init(logLevel: String) {
        this.logLevel = logLevel
    }
    
    public func log(level: String, message: String): Unit {
        let timestamp = DateTime.now().toString()
        println("[${timestamp}] [${level}] ${message}")
    }
    
    public func getLogLevel(): String {
        logLevel
    }
}

/**
 * 内存配置服务实现
 */
public class MemoryConfigService <: IConfigService {
    private let config: HashMap<String, String>
    
    public init() {
        this.config = HashMap<String, String>()
        // 设置默认配置
        config["processor.name"] = "DefaultProcessor"
        config["log.level"] = "INFO"
        config["system.name"] = "DependencyOptimizationDemo"
    }
    
    public func getValue(key: String): Option<String> {
        config.get(key)
    }
    
    public func setValue(key: String, value: String): Unit {
        config[key] = value
    }
}

/**
 * 依赖注入演示Actor
 */
public class DIActor <: Actor {
    private let messageProcessor: IMessageProcessor
    private let configService: IConfigService
    private var processedCount: Int64 = 0
    
    public init() {
        // 简化实现：直接创建依赖
        let logService = ConsoleLogService("INFO")
        this.messageProcessor = DefaultMessageProcessor("DIProcessor", logService)
        this.configService = MemoryConfigService()
    }
    
    public prop name: String {
        get() { "DIActor" }
    }
    
    public prop description: String {
        get() { "依赖注入演示Actor" }
    }
    
    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case msg: StringMessage =>
                processStringMessage(msg)
                MessageResult.Handled
            case _ =>
                MessageResult.Unhandled
        }
    }
    
    private func processStringMessage(msg: StringMessage): Unit {
        let content = msg.getContent()
        
        if (content == "get-stats") {
            showStatistics()
        } else if (content.startsWith("config:")) {
            handleConfigCommand(content)
        } else {
            // 使用注入的消息处理器
            let result = messageProcessor.processMessage(content)
            processedCount += 1
            println("📨 处理结果: ${result}")
            println("📊 已处理消息数: ${processedCount}")
        }
    }
    
    private func showStatistics(): Unit {
        println("📊 DIActor 统计信息:")
        println("   处理器名称: ${messageProcessor.getProcessorName()}")
        println("   已处理消息数: ${processedCount}")
        
        match (configService.getValue("system.name")) {
            case Some(systemName) =>
                println("   系统名称: ${systemName}")
            case None =>
                println("   系统名称: 未配置")
        }
    }
    
    private func handleConfigCommand(command: String): Unit {
        let parts = command.split(":")
        if (parts.size >= 3) {
            let key = parts[1]
            let value = parts[2]
            configService.setValue(key, value)
            println("✅ 配置已更新: ${key} = ${value}")
        } else {
            println("❌ 配置命令格式错误，应为: config:key:value")
        }
    }
    
    public func preStart(): Unit {
        println("🚀 DIActor启动 - 使用依赖注入")
        println("   消息处理器: ${messageProcessor.getProcessorName()}")
    }
    
    public func postStop(): Unit {
        println("🛑 DIActor停止 - 总处理数: ${processedCount}")
    }
}

/**
 * 模块契约实现示例
 */
public class DemoModuleContract <: ModuleContract {
    public prop moduleName: String {
        get() { "DependencyOptimizationDemo" }
    }
    
    public prop layer: ModuleLayer {
        get() { ModuleLayer.API }
    }
    
    public prop version: String {
        get() { "1.0.0" }
    }
    
    public prop description: String {
        get() { "依赖关系优化演示模块" }
    }
    
    public func getDependencies(): Array<String> {
        ["cactor.foundation.di", "cactor.foundation.abstractions", "cactor.api"]
    }
    
    public func getProvidedServices(): Array<String> {
        ["DependencyOptimizationDemo"]
    }
    
    public func getRequiredServices(): Array<String> {
        ["IMessageProcessor", "ILogService", "IConfigService"]
    }
    
    public func initialize(): Unit {
        println("🔧 初始化依赖关系优化演示模块")
        setupDependencyInjection()
    }
    
    public func dispose(): Unit {
        println("🧹 清理依赖关系优化演示模块")
        DIContainerProvider.reset()
    }
    
    public func validate(): Bool {
        let container = DIContainerProvider.getContainer()
        return container.isRegistered("IMessageProcessor") &&
               container.isRegistered("ILogService") &&
               container.isRegistered("IConfigService")
    }
    
    /**
     * 设置依赖注入
     */
    private func setupDependencyInjection(): Unit {
        // 注册服务
        let logService = ConsoleLogService("INFO")
        ServiceLocator.registerSingleton<ILogService>("ILogService", logService)
        
        let configService = MemoryConfigService()
        ServiceLocator.registerSingleton<IConfigService>("IConfigService", configService)
        
        // 简化实现：直接注册消息处理器
        let messageProcessor = DefaultMessageProcessor("DefaultProcessor", logService)
        ServiceLocator.registerSingleton<IMessageProcessor>("IMessageProcessor", messageProcessor)
        
        println("✅ 依赖注入配置完成")
    }
}

/**
 * 依赖关系优化演示主程序
 */
main(): Unit {
    println("🚀 CActor 依赖关系优化演示")
    println("Plan 0.20 Phase 3.1: 依赖关系优化")
    println("=" * 60)
    
    // 1. 注册模块契约
    let moduleRegistry = GlobalModuleRegistry.getInstance()
    let demoModule = DemoModuleContract()
    moduleRegistry.registerModule(demoModule)
    
    // 2. 初始化模块
    demoModule.initialize()
    
    // 3. 验证依赖注入
    if (demoModule.validate()) {
        println("✅ 依赖注入验证通过")
    } else {
        println("❌ 依赖注入验证失败")
        return
    }
    
    // 4. 创建Actor系统
    let system = CActorSystemFactory.create("dependency-optimization-demo")
    
    // 5. 创建使用依赖注入的Actor
    let diActor = system.actorOf({ => DIActor() }, "di-actor")
    
    println("✅ 创建了依赖注入演示Actor")
    
    // 6. 演示依赖注入功能
    println("\n🧪 演示1: 基本消息处理")
    diActor.tell(StringMessage("Hello, Dependency Injection!"))
    sleep(Duration.millisecond * 200)
    
    println("\n🧪 演示2: 配置更新")
    diActor.tell(StringMessage("config:processor.name:EnhancedProcessor"))
    sleep(Duration.millisecond * 200)
    
    println("\n🧪 演示3: 处理更多消息")
    diActor.tell(StringMessage("Message with new processor"))
    diActor.tell(StringMessage("Another message"))
    sleep(Duration.millisecond * 300)
    
    println("\n🧪 演示4: 获取统计信息")
    diActor.tell(StringMessage("get-stats"))
    sleep(Duration.millisecond * 200)
    
    // 7. 生成模块依赖关系报告
    println("\n📊 模块依赖关系报告:")
    let report = moduleRegistry.generateDependencyReport()
    println(report)
    
    // 8. 验证依赖关系
    let violations = moduleRegistry.validateDependencies()
    if (violations.size == 0) {
        println("✅ 依赖关系验证通过，无违反层次架构")
    } else {
        println("❌ 发现依赖关系违反:")
        for (violation in violations) {
            println("   - ${violation}")
        }
    }
    
    println("\n🎯 依赖关系优化演示完成")
    println("展示了依赖注入、抽象接口和模块契约的使用")
    
    // 9. 清理系统
    system.terminate()
    demoModule.dispose()
    sleep(Duration.second * 1)
    
    println("✅ 系统已清理")
}
