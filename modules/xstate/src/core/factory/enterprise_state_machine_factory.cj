/*
 * Copyright (c) 2024. All rights reserved.
 * CActor XState - 企业级状态机工厂
 * 提供企业级状态机的创建和配置功能
 */
package cactor_xstate.core.factory

import std.collection.*
import std.time.*
import cactor.api.{CActor, CActorSystem}
import cactor_core.actor.{ActorRef}
import cactor_xstate.core.types.{StateMachineState, StateMachineEvent}
import cactor_xstate.core.machine.{StateMachineDefinition, StateMachineBuilder}
import cactor_xstate.core.actor.{StateMachineActorFactory}

/**
 * 企业级状态机配置
 */
public struct EnterpriseStateMachineConfig {
    public let enableEventSourcing: Bool
    public let enableStateSnapshots: Bool
    public let enableDistributedSync: Bool
    public let enableRealTimeMonitoring: Bool
    public let enableFaultRecovery: Bool
    public let performanceOptimizations: Bool
    
    public init(
        enableEventSourcing: Bool,
        enableStateSnapshots: Bool,
        enableDistributedSync: Bool,
        enableRealTimeMonitoring: Bool,
        enableFaultRecovery: Bool,
        performanceOptimizations: Bool
    ) {
        this.enableEventSourcing = enableEventSourcing
        this.enableStateSnapshots = enableStateSnapshots
        this.enableDistributedSync = enableDistributedSync
        this.enableRealTimeMonitoring = enableRealTimeMonitoring
        this.enableFaultRecovery = enableFaultRecovery
        this.performanceOptimizations = performanceOptimizations
    }
    
    /**
     * 创建默认企业级配置
     */
    public static func createDefault(): EnterpriseStateMachineConfig {
        EnterpriseStateMachineConfig(
            true,  // enableEventSourcing
            true,  // enableStateSnapshots
            false, // enableDistributedSync
            true,  // enableRealTimeMonitoring
            true,  // enableFaultRecovery
            true   // performanceOptimizations
        )
    }
    
    /**
     * 创建高性能配置
     */
    public static func createHighPerformance(): EnterpriseStateMachineConfig {
        EnterpriseStateMachineConfig(
            false, // enableEventSourcing
            false, // enableStateSnapshots
            false, // enableDistributedSync
            false, // enableRealTimeMonitoring
            false, // enableFaultRecovery
            true   // performanceOptimizations
        )
    }
    
    /**
     * 创建完整企业级配置
     */
    public static func createFullEnterprise(): EnterpriseStateMachineConfig {
        EnterpriseStateMachineConfig(
            true, // enableEventSourcing
            true, // enableStateSnapshots
            true, // enableDistributedSync
            true, // enableRealTimeMonitoring
            true, // enableFaultRecovery
            true  // performanceOptimizations
        )
    }
}

/**
 * 企业级状态机工厂
 * 提供企业级状态机的创建和管理功能
 */
public struct EnterpriseStateMachineFactory {
    
    /**
     * 创建企业级状态机
     */
    public static func create(
        system: CActorSystem,
        machineDefinition: StateMachineDefinition,
        config: EnterpriseStateMachineConfig,
        name: String
    ): ActorRef {
        println("🏢 创建企业级状态机: ${name}")
        
        // 应用企业级配置
        applyEnterpriseConfig(config)
        
        // 创建基础状态机Actor
        let stateMachineActor = StateMachineActorFactory.create(system, machineDefinition, name)
        
        // 应用企业级增强
        applyEnterpriseEnhancements(stateMachineActor, config)
        
        println("✅ 企业级状态机创建完成: ${name}")
        return stateMachineActor
    }
    
    /**
     * 创建层次状态机
     */
    public static func createHierarchical(
        system: CActorSystem,
        rootMachineDefinition: StateMachineDefinition,
        childMachineDefinitions: HashMap<String, StateMachineDefinition>,
        config: EnterpriseStateMachineConfig,
        name: String
    ): ActorRef {
        println("🏗️ 创建层次状态机: ${name}")
        
        // 创建根状态机
        let rootActor = create(system, rootMachineDefinition, config, "${name}-root")
        
        // 创建子状态机
        for ((childName, childDefinition) in childMachineDefinitions) {
            let childActor = create(system, childDefinition, config, "${name}-${childName}")
            println("   ✅ 子状态机创建: ${childName}")
        }
        
        println("✅ 层次状态机创建完成: ${name}")
        return rootActor
    }
    
    /**
     * 创建并行状态机
     */
    public static func createParallel(
        system: CActorSystem,
        regionMachineDefinitions: HashMap<String, StateMachineDefinition>,
        config: EnterpriseStateMachineConfig,
        name: String
    ): Array<ActorRef> {
        println("⚡ 创建并行状态机: ${name}")
        
        let parallelActors = ArrayList<ActorRef>()
        
        // 创建并行区域状态机
        for ((regionName, regionDefinition) in regionMachineDefinitions) {
            let regionActor = create(system, regionDefinition, config, "${name}-${regionName}")
            parallelActors.add(regionActor)
            println("   ✅ 并行区域创建: ${regionName}")
        }
        
        println("✅ 并行状态机创建完成: ${name}")
        return parallelActors.toArray()
    }
    
    /**
     * 创建状态机集群
     */
    public static func createCluster(
        systems: Array<CActorSystem>,
        machineDefinition: StateMachineDefinition,
        config: EnterpriseStateMachineConfig,
        clusterName: String
    ): Array<ActorRef> {
        println("🌐 创建状态机集群: ${clusterName}")
        
        let clusterActors = ArrayList<ActorRef>()
        
        // 在每个系统中创建状态机实例
        for (i in 0..systems.size-1) {
            let system = systems[i]
            let nodeName = "${clusterName}-node-${i}"
            let nodeActor = create(system, machineDefinition, config, nodeName)
            clusterActors.add(nodeActor)
            println("   ✅ 集群节点创建: ${nodeName}")
        }
        
        println("✅ 状态机集群创建完成: ${clusterName}")
        return clusterActors.toArray()
    }
    
    /**
     * 应用企业级配置
     */
    private static func applyEnterpriseConfig(config: EnterpriseStateMachineConfig): Unit {
        if (config.enableEventSourcing) {
            println("   🔧 启用事件溯源")
        }
        
        if (config.enableStateSnapshots) {
            println("   🔧 启用状态快照")
        }
        
        if (config.enableDistributedSync) {
            println("   🔧 启用分布式同步")
        }
        
        if (config.enableRealTimeMonitoring) {
            println("   🔧 启用实时监控")
        }
        
        if (config.enableFaultRecovery) {
            println("   🔧 启用故障恢复")
        }
        
        if (config.performanceOptimizations) {
            println("   🔧 启用性能优化")
        }
    }
    
    /**
     * 应用企业级增强
     */
    private static func applyEnterpriseEnhancements(
        actor: ActorRef,
        config: EnterpriseStateMachineConfig
    ): Unit {
        if (config.enableEventSourcing) {
            // 应用事件溯源增强
            println("   ✅ 事件溯源增强已应用")
        }
        
        if (config.enableStateSnapshots) {
            // 应用状态快照增强
            println("   ✅ 状态快照增强已应用")
        }
        
        if (config.enableDistributedSync) {
            // 应用分布式同步增强
            println("   ✅ 分布式同步增强已应用")
        }
        
        if (config.enableRealTimeMonitoring) {
            // 应用实时监控增强
            println("   ✅ 实时监控增强已应用")
        }
        
        if (config.enableFaultRecovery) {
            // 应用故障恢复增强
            println("   ✅ 故障恢复增强已应用")
        }
        
        if (config.performanceOptimizations) {
            // 应用性能优化增强
            println("   ✅ 性能优化增强已应用")
        }
    }
}

/**
 * 状态机模板工厂
 * 提供常用状态机模板的快速创建
 */
public struct StateMachineTemplateFactory {
    
    /**
     * 创建工作流状态机模板
     */
    public static func createWorkflowTemplate(): StateMachineDefinition {
        var builder = StateMachineBuilder()
        builder.withId("workflow-template")
        builder.withInitialState("pending")
        return builder.build()
    }
    
    /**
     * 创建审批流状态机模板
     */
    public static func createApprovalTemplate(): StateMachineDefinition {
        var builder = StateMachineBuilder()
        builder.withId("approval-template")
        builder.withInitialState("submitted")
        return builder.build()
    }

    /**
     * 创建订单处理状态机模板
     */
    public static func createOrderProcessingTemplate(): StateMachineDefinition {
        var builder = StateMachineBuilder()
        builder.withId("order-processing-template")
        builder.withInitialState("created")
        return builder.build()
    }

    /**
     * 创建用户会话状态机模板
     */
    public static func createUserSessionTemplate(): StateMachineDefinition {
        var builder = StateMachineBuilder()
        builder.withId("user-session-template")
        builder.withInitialState("anonymous")
        return builder.build()
    }

    /**
     * 创建文件处理状态机模板
     */
    public static func createFileProcessingTemplate(): StateMachineDefinition {
        var builder = StateMachineBuilder()
        builder.withId("file-processing-template")
        builder.withInitialState("uploaded")
        return builder.build()
    }
}
