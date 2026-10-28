/*
 * Copyright (c) 2024. All rights reserved.
 * CActor API - 参考Akka设计的统一Actor系统API
 * 按照plan10.md要求，提供统一的ActorSystem API，不直接导出core层
 * 设计理念：简洁、类型安全、隐藏实现细节
 */
package cactor.api

import cactor_core.actor.{Actor, ActorRef, Props, PropsFactory}
import cactor_core.config.{MailboxConfig as CoreMailboxConfig, DispatcherConfig as CoreDispatcherConfig, SupervisionConfig as CoreSupervisionConfig}
import cactor_core.typed.{TypedBehavior, TypedActorRef, TypedActorSelection}
import cactor_runtime.wrappers.{TypedActorRefWrapper, TypedActorSelectionWrapper, MessageBridge}
import cactor_runtime.{CActorRuntime, CActorRuntimeConfig}
import cactor_runtime.typed.{TypedActorSystemImpl}
import std.time.*

/**
 * CActorSystem - 参考Akka设计的统一Actor系统API
 * 提供类型安全、简洁的Actor创建接口
 */
public class CActorSystem {
    private let runtime: CActorRuntime

    public init(name: String) {
        let config = CActorRuntimeConfig.createDefault()
        this.runtime = CActorRuntime(name, config)
        runtime.start()
    }

    public init(name: String, config: CActorRuntimeConfig) {
        this.runtime = CActorRuntime(name, config)
        runtime.start()
    }

    /**
     * 创建Actor - 参考Akka的actorOf方法
     * 使用类型擦除来处理泛型问题
     */
    public func actorOf(creator: () -> Actor, name: String): ActorRef {
        let props = PropsFactory.create(creator)
        runtime.actorOf(props, name)
    }

    /**
     * 创建Actor（自动命名）- 参考Akka的actorOf方法
     */
    public func actorOf(creator: () -> Actor): ActorRef {
        let props = PropsFactory.create(creator)
        runtime.actorOf(props)
    }

    /**
     * 使用ActorProps创建Actor
     */
    public func actorOf(actorProps: ActorProps, name: String): ActorRef {
        let creator = actorProps.getCreator()
        actorOf(creator, name)
    }

    /**
     * 使用ActorProps创建Actor（自动命名）
     */
    public func actorOf(actorProps: ActorProps): ActorRef {
        let creator = actorProps.getCreator()
        actorOf(creator)
    }

    /**
     * 使用Props创建Actor - 支持完整配置传递（类似Akka）
     */
    public func actorOf(props: Props<Actor>, name: String): ActorRef {
        runtime.actorOf(props, name)
    }

    /**
     * 使用Props创建Actor（自动命名）- 支持完整配置传递
     */
    public func actorOf(props: Props<Actor>): ActorRef {
        runtime.actorOf(props)
    }

    /**
     * 使用邮箱配置创建Actor - 类似Akka的withMailbox
     */
    public func actorOfWithMailbox(creator: () -> Actor, name: String, mailboxConfig: CoreMailboxConfig): ActorRef {
        let props = PropsFactory.create(creator).withMailbox(mailboxConfig.name)
        runtime.actorOf(props, name)
    }

    /**
     * 使用调度器配置创建Actor - 类似Akka的withDispatcher
     */
    public func actorOfWithDispatcher(creator: () -> Actor, name: String, dispatcherConfig: CoreDispatcherConfig): ActorRef {
        let props = PropsFactory.create(creator).withDispatcher(dispatcherConfig.name)
        runtime.actorOf(props, name)
    }

    /**
     * 使用完整配置创建Actor - 类似Akka的完整配置方式
     */
    public func actorOfConfigured(creator: () -> Actor,
                                 name: String,
                                 mailboxConfig: CoreMailboxConfig,
                                 dispatcherConfig: CoreDispatcherConfig,
                                 supervisionConfig: CoreSupervisionConfig): ActorRef {
        let props = PropsFactory.create(creator)
            .withMailbox(mailboxConfig.name)
            .withDispatcher(dispatcherConfig.name)
            .withSupervisionStrategy(supervisionConfig.name)
        runtime.actorOf(props, name)
    }

    /**
     * 获取系统名称
     */
    public func getName(): String {
        runtime.getRuntimeStats().systemName
    }

    /**
     * 终止系统 - 参考Akka的terminate方法
     */
    public func terminate(): Unit {
        runtime.shutdown()
    }

    /**
     * 检查是否已终止
     */
    public func isTerminated(): Bool {
        !runtime.getRuntimeStats().isRunning
    }

    /**
     * 获取底层运行时（用于高级功能）
     */
    public func getRuntime(): CActorRuntime {
        runtime
    }

    // ========== 类型安全Actor支持 ==========

    /**
     * 创建类型安全Actor - 统一API
     *
     * @param behavior 类型安全的Actor行为
     * @param name Actor名称
     * @return 类型安全的Actor引用
     */
    public func typedActorOf<T>(behavior: TypedBehavior<T>, name: String): TypedActorRef<T> {
        // 通过类型安全系统创建，避免Dummy包装
        let typedSystem = TypedActorSystemImpl(this.getName(), this.getRuntime())
        typedSystem.createActor<T>(behavior, name)
    }

    /**
     * 创建类型安全Actor（自动命名）
     *
     * @param behavior 类型安全的Actor行为
     * @return 类型安全的Actor引用
     */
    public func typedActorOf<T>(behavior: TypedBehavior<T>): TypedActorRef<T> {
        let name = "typed-actor-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
        typedActorOf(behavior, name)
    }

    /**
     * 类型安全Actor选择 - 根据路径查找Actor
     *
     * @param path Actor路径
     * @return 类型安全的Actor选择器
     */
    public func typedActorSelection<T>(path: String): TypedActorSelection<T> {
        TypedActorSelectionWrapper<T>(path)
    }

    /**
     * 获取系统启动时间
     *
     * @return 启动时间戳
     */
    public func getStartTime(): Int64 {
        // 简化实现：返回当前时间
        DateTime.now().toUnixTimeStamp().toMilliseconds()
    }

    // ========== Actor交互支持 ==========

    /**
     * 将标准ActorRef转换为类型安全ActorRef
     * 支持两种Actor之间的交互
     *
     * @param actorRef 标准Actor引用
     * @param name Actor名称
     * @return 类型安全的Actor引用
     */
    public func wrapAsTypedActor<T>(actorRef: ActorRef, name: String): TypedActorRef<T> {
        TypedActorRefWrapper<T>(actorRef, name)
    }

    /**
     * 从类型安全ActorRef获取底层标准ActorRef
     * 支持两种Actor之间的交互
     *
     * @param typedActorRef 类型安全Actor引用
     * @return 标准Actor引用
     */
    public func unwrapTypedActor<T>(typedActorRef: TypedActorRef<T>): ActorRef {
        match (typedActorRef) {
            case wrapper: TypedActorRefWrapper<T> =>
                wrapper.getUnderlyingActorRef()
            case _ =>
                throw Exception("不支持的TypedActorRef类型")
        }
    }

    /**
     * 创建混合消息桥接器
     * 允许标准Actor和类型安全Actor之间发送消息
     *
     * @param standardActor 标准Actor引用
     * @param typedActor 类型安全Actor引用
     * @return 消息桥接器
     */
    public func createMessageBridge<T>(standardActor: ActorRef, typedActor: TypedActorRef<T>): MessageBridge<T> {
        MessageBridge<T>(standardActor, typedActor)
    }
}



/**
 * CActorSystem工厂方法 - 参考Akka的ActorSystem.create()
 */
public struct CActorSystemFactory {
    /**
     * 创建默认的CActorRuntime系统
     */
    public static func create(): CActorSystem {
        create("default")
    }

    /**
     * 创建命名的CActorRuntime系统
     */
    public static func create(name: String): CActorSystem {
        CActorSystem(name)
    }

    /**
     * 创建配置的CActorRuntime系统
     */
    public static func create(name: String, config: CActorRuntimeConfig): CActorSystem {
        CActorSystem(name, config)
    }
}
