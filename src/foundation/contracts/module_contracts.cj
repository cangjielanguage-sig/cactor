/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Foundation - 模块契约定义
 * Plan 0.20 Phase 3.1: 依赖关系优化
 * 定义各层模块的边界和契约，确保清晰的架构层次
 */
package cactor.foundation.contracts

import std.collection.*
import cactor.foundation.abstractions.*

/**
 * 模块层次枚举
 */
public enum ModuleLayer {
    | Foundation   // 基础设施层
    | Core         // 核心层
    | Runtime      // 运行时层
    | Patterns     // 模式层
    | Distribution // 分布式层
    | Integration  // 集成层
    | API          // API层

    public func toString(): String {
        match (this) {
            case Foundation => "Foundation"
            case Core => "Core"
            case Runtime => "Runtime"
            case Patterns => "Patterns"
            case Distribution => "Distribution"
            case Integration => "Integration"
            case API => "API"
        }
    }

    public func hashCode(): Int64 {
        match (this) {
            case Foundation => 0
            case Core => 1
            case Runtime => 2
            case Patterns => 3
            case Distribution => 4
            case Integration => 5
            case API => 6
        }
    }
}

/**
 * 模块契约接口
 */
public interface ModuleContract {
    /**
     * 模块名称
     */
    prop moduleName: String
    
    /**
     * 模块层次
     */
    prop layer: ModuleLayer
    
    /**
     * 模块版本
     */
    prop version: String
    
    /**
     * 模块描述
     */
    prop description: String
    
    /**
     * 依赖的模块
     */
    func getDependencies(): Array<String>
    
    /**
     * 提供的服务
     */
    func getProvidedServices(): Array<String>
    
    /**
     * 需要的服务
     */
    func getRequiredServices(): Array<String>
    
    /**
     * 初始化模块
     */
    func initialize(): Unit
    
    /**
     * 清理模块
     */
    func dispose(): Unit
    
    /**
     * 验证模块状态
     */
    func validate(): Bool
}

/**
 * Foundation层契约
 */
public interface FoundationContract <: ModuleContract {
    /**
     * Foundation层提供基础设施服务
     * 不依赖任何其他层
     */
    
    /**
     * 提供内存管理服务
     */
    func getMemoryManager(): Any
    
    /**
     * 提供队列服务
     */
    func getQueueFactory(): Any
    
    /**
     * 提供网络服务
     */
    func getNetworkTransport(): Any
    
    /**
     * 提供序列化服务
     */
    func getSerializer(): Any
    
    /**
     * 提供依赖注入容器
     */
    func getDIContainer(): Any
}

/**
 * Core层契约
 */
public interface CoreContract <: ModuleContract {
    /**
     * Core层提供核心抽象
     * 只依赖Foundation层
     */
    
    /**
     * 提供Actor抽象
     */
    func getActorFactory(): Any
    
    /**
     * 提供消息系统
     */
    func getMessageSystem(): Any
    
    /**
     * 提供系统管理
     */
    func getSystemManager(): Any
    
    /**
     * 提供监督策略
     */
    func getSupervisionStrategy(): Any
}

/**
 * Runtime层契约
 */
public interface RuntimeContract <: ModuleContract {
    /**
     * Runtime层提供执行引擎
     * 依赖Foundation和Core层
     */
    
    /**
     * 提供调度器
     */
    func getDispatcherFactory(): Any
    
    /**
     * 提供邮箱系统
     */
    func getMailboxFactory(): Any
    
    /**
     * 提供生命周期管理
     */
    func getLifecycleManager(): Any
    
    /**
     * 提供监控系统
     */
    func getMonitoringSystem(): Any
}

/**
 * API层契约
 */
public interface APIContract <: ModuleContract {
    /**
     * API层提供用户接口
     * 依赖所有下层模块，但通过抽象接口
     */
    
    /**
     * 提供统一API
     */
    func getUnifiedAPI(): Any
    
    /**
     * 提供配置API
     */
    func getConfigurationAPI(): Any
    
    /**
     * 提供扩展API
     */
    func getExtensionAPI(): Any
}

/**
 * 模块注册表
 */
public class ModuleRegistry {
    private let modules: HashMap<String, ModuleContract>
    private let layerModules: HashMap<String, ArrayList<ModuleContract>>
    
    public init() {
        this.modules = HashMap<String, ModuleContract>()
        this.layerModules = HashMap<String, ArrayList<ModuleContract>>()

        // 初始化层次映射
        for (layerName in [
            "Foundation",
            "Core",
            "Runtime",
            "Patterns",
            "Distribution",
            "Integration",
            "API"
        ]) {
            layerModules[layerName] = ArrayList<ModuleContract>()
        }
    }
    
    /**
     * 注册模块
     */
    public func registerModule(module: ModuleContract): Unit {
        modules[module.moduleName] = module
        
        let layerName = getLayerName(module.layer)
        match (layerModules.get(layerName)) {
            case Some(moduleList) =>
                moduleList.add(module)
            case None =>
                let moduleList = ArrayList<ModuleContract>()
                moduleList.add(module)
                layerModules[layerName] = moduleList
        }
    }
    
    /**
     * 获取模块
     */
    public func getModule(moduleName: String): Option<ModuleContract> {
        modules.get(moduleName)
    }
    
    /**
     * 获取层次中的所有模块
     */
    public func getModulesInLayer(layer: ModuleLayer): Array<ModuleContract> {
        let layerName = getLayerName(layer)
        match (layerModules.get(layerName)) {
            case Some(moduleList) =>
                moduleList.toArray()
            case None =>
                Array<ModuleContract>()
        }
    }

    /**
     * 获取层次名称
     */
    private func getLayerName(layer: ModuleLayer): String {
        match (layer) {
            case ModuleLayer.Foundation => "Foundation"
            case ModuleLayer.Core => "Core"
            case ModuleLayer.Runtime => "Runtime"
            case ModuleLayer.Patterns => "Patterns"
            case ModuleLayer.Distribution => "Distribution"
            case ModuleLayer.Integration => "Integration"
            case ModuleLayer.API => "API"
        }
    }
    
    /**
     * 验证依赖关系
     */
    public func validateDependencies(): Array<String> {
        let violations = ArrayList<String>()
        
        for ((moduleName, module) in modules) {
            let moduleLayerIndex = getLayerIndex(module.layer)
            
            for (dependency in module.getDependencies()) {
                match (modules.get(dependency)) {
                    case Some(depModule) =>
                        let depLayerIndex = getLayerIndex(depModule.layer)
                        if (depLayerIndex > moduleLayerIndex) {
                            violations.add("模块 ${moduleName} 违反层次架构，依赖了上层模块 ${dependency}")
                        }
                    case None =>
                        violations.add("模块 ${moduleName} 依赖了不存在的模块 ${dependency}")
                }
            }
        }
        
        return violations.toArray()
    }
    
    /**
     * 获取层次索引
     */
    private func getLayerIndex(layer: ModuleLayer): Int32 {
        match (layer) {
            case ModuleLayer.Foundation => 0
            case ModuleLayer.Core => 1
            case ModuleLayer.Runtime => 2
            case ModuleLayer.Patterns => 3
            case ModuleLayer.Distribution => 4
            case ModuleLayer.Integration => 5
            case ModuleLayer.API => 6
        }
    }
    
    /**
     * 初始化所有模块
     */
    public func initializeAll(): Unit {
        // 按层次顺序初始化
        for (layer in [
            ModuleLayer.Foundation,
            ModuleLayer.Core,
            ModuleLayer.Runtime,
            ModuleLayer.Patterns,
            ModuleLayer.Distribution,
            ModuleLayer.Integration,
            ModuleLayer.API
        ]) {
            let moduleList = getModulesInLayer(layer)
            for (module in moduleList) {
                try {
                    module.initialize()
                    println("✅ 模块 ${module.moduleName} 初始化成功")
                } catch (e: Exception) {
                    println("❌ 模块 ${module.moduleName} 初始化失败: ${e.message}")
                }
            }
        }
    }
    
    /**
     * 清理所有模块
     */
    public func disposeAll(): Unit {
        // 按层次逆序清理
        for (layer in [
            ModuleLayer.API,
            ModuleLayer.Integration,
            ModuleLayer.Distribution,
            ModuleLayer.Patterns,
            ModuleLayer.Runtime,
            ModuleLayer.Core,
            ModuleLayer.Foundation
        ]) {
            let moduleList = getModulesInLayer(layer)
            for (module in moduleList) {
                try {
                    module.dispose()
                    println("✅ 模块 ${module.moduleName} 清理成功")
                } catch (e: Exception) {
                    println("❌ 模块 ${module.moduleName} 清理失败: ${e.message}")
                }
            }
        }
    }
    
    /**
     * 生成依赖关系报告
     */
    public func generateDependencyReport(): String {
        let report = StringBuilder()
        
        report.append("# CActor 模块依赖关系报告\n\n")
        report.append("**生成时间**: 当前时间\n")
        report.append("**模块总数**: ${modules.size}\n\n")
        
        // 按层次显示模块
        for (layer in [
            ModuleLayer.Foundation,
            ModuleLayer.Core,
            ModuleLayer.Runtime,
            ModuleLayer.Patterns,
            ModuleLayer.Distribution,
            ModuleLayer.Integration,
            ModuleLayer.API
        ]) {
            let moduleList = getModulesInLayer(layer)
            if (moduleList.size > 0) {
                report.append("## 模块层 (${moduleList.size} 个模块)\n\n")
                for (module in moduleList) {
                    report.append("### ${module.moduleName}\n")
                    report.append("- **版本**: ${module.version}\n")
                    report.append("- **描述**: ${module.description}\n")
                    report.append("- **依赖**: 依赖列表\n")
                    report.append("- **提供服务**: 服务列表\n")
                    report.append("- **需要服务**: 需要服务列表\n\n")
                }
            }
        }
        
        // 依赖关系验证
        let violations = validateDependencies()
        report.append("## 依赖关系验证\n\n")
        if (violations.size == 0) {
            report.append("✅ 所有依赖关系符合层次架构\n\n")
        } else {
            report.append("❌ 发现 ${violations.size} 个违反层次架构的依赖:\n\n")
            for (violation in violations) {
                report.append("- ${violation}\n")
            }
            report.append("\n")
        }
        
        return report.toString()
    }
}

/**
 * 全局模块注册表
 */
public struct GlobalModuleRegistry {
    private static var instance: Option<ModuleRegistry> = None<ModuleRegistry>
    
    public static func getInstance(): ModuleRegistry {
        match (instance) {
            case Some(registry) =>
                registry
            case None =>
                let registry = ModuleRegistry()
                instance = Some(registry)
                registry
        }
    }
    
    public static func reset(): Unit {
        match (instance) {
            case Some(registry) =>
                registry.disposeAll()
            case None =>
                println("无需处理") // 无需处理
        }
        instance = None<ModuleRegistry>
    }
}
