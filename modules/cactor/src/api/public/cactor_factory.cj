/*
 * Copyright (c) 2024. All rights reserved.
 * CActor API - 统一工厂接口
 * 提供简洁的Actor系统创建接口，隐藏实现细节
 */
package cactor.api.public

import cactor_core.actor.{Actor, Props, PropsFactory}
import cactor_core.system.{ActorSystem}

/**
 * CActor系统工厂
 *
 * 提供统一的Actor系统创建接口，隐藏底层实现细节。
 * 这是创建CActor系统的主要入口点，支持多种配置选项。
 *
 * @since 1.0.0
 * @author CActor Team
 */
public struct CActorFactory {
    /**
     * 创建默认Actor系统
     *
     * 使用默认配置创建一个新的Actor系统，系统名称为"default"。
     * 适用于简单的应用场景和快速原型开发。
     *
     * @return 新创建的Actor系统实例
     * @throws Exception 当系统创建失败时抛出
     */
    public static func createSystem(): ActorSystem {
        createSystem("default")
    }

    /**
     * 创建命名Actor系统
     *
     * 使用指定名称创建一个新的Actor系统。系统名称用于标识
     * 不同的Actor系统实例，在分布式环境中尤其重要。
     *
     * @param name Actor系统的名称，必须是有效的标识符
     * @return 新创建的Actor系统实例
     * @throws Exception 当系统创建失败或名称无效时抛出
     */
    public static func createSystem(name: String): ActorSystem {
        // 这里需要引用具体的实现类
        // 暂时返回一个简单的实现
        throw Exception("Not implemented yet")
    }

    /**
     * 创建Actor Props
     *
     * 创建用于Actor实例化的Props配置对象。Props包含了
     * 创建Actor所需的所有信息，包括工厂函数、调度器配置等。
     *
     * @param creator Actor实例创建函数
     * @return 配置好的Props对象
     * @throws Exception 当Props创建失败时抛出
     */
    public static func createProps<T>(creator: () -> T): Props<T> where T <: Actor {
        PropsFactory.create(creator)
    }
}

/**
 * CActor系统构建器
 *
 * 提供流式API构建Actor系统，支持链式调用配置各种参数。
 * 使用构建器模式可以更灵活地配置Actor系统的各个组件。
 *
 * 使用示例：
 * ```cangjie
 * let system = CActorSystemBuilder()
 *     .withName("MySystem")
 *     .withDispatcher("thread-pool")
 *     .withMailbox("bounded")
 *     .build()
 * ```
 *
 * @since 1.0.0
 * @author CActor Team
 */
public class CActorSystemBuilder {
    private var systemName: String = "default"
    private var dispatcherType: String = "default"
    private var mailboxType: String = "unbounded"

    /**
     * 构造函数
     *
     * 创建一个新的系统构建器实例，使用默认配置。
     */
    public init() {}

    /**
     * 设置系统名称
     *
     * @param name 系统名称，用于标识Actor系统
     * @return 当前构建器实例，支持链式调用
     */
    public func withName(name: String): CActorSystemBuilder {
        this.systemName = name
        this
    }

    /**
     * 设置调度器类型
     *
     * @param dispatcher 调度器类型，如"thread-pool"、"work-stealing"等
     * @return 当前构建器实例，支持链式调用
     */
    public func withDispatcher(dispatcher: String): CActorSystemBuilder {
        this.dispatcherType = dispatcher
        this
    }

    /**
     * 设置邮箱类型
     *
     * @param mailbox 邮箱类型，如"bounded"、"unbounded"、"priority"等
     * @return 当前构建器实例，支持链式调用
     */
    public func withMailbox(mailbox: String): CActorSystemBuilder {
        this.mailboxType = mailbox
        this
    }

    /**
     * 构建Actor系统
     *
     * 根据之前设置的配置参数创建Actor系统实例。
     *
     * @return 配置好的Actor系统实例
     * @throws Exception 当系统创建失败时抛出
     */
    public func build(): ActorSystem {
        CActorFactory.createSystem(systemName)
    }
}
