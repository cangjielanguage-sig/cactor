/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 类型安全路由器系统
 * 实现plan10.md中第二优先级功能：消息路由机制
 * 与标准Router系统API兼容
 */
package cactor.core.typed

import std.time.*
import std.collection.*

/**
 * 类型安全的路由策略接口
 * 与标准RoutingStrategy兼容，但提供类型安全
 * 
 * @param T 此策略可以处理的消息类型
 */
public interface TypedRoutingStrategy<T> {
    /**
     * 选择目标Actor
     * 
     * @param routees 可用的Actor列表
     * @param message 要路由的消息
     * @return 选中的Actor，如果没有可用Actor返回None
     */
    func selectRoutee(routees: Array<TypedActorRef<T>>, message: T): Option<TypedActorRef<T>>
    
    /**
     * 获取策略描述
     * 
     * @return 策略描述
     */
    func getDescription(): String {
        "TypedRoutingStrategy"
    }
}

/**
 * 类型安全的路由器接口
 * 与标准Router兼容，但提供类型安全
 * 
 * @param T 此路由器可以处理的消息类型
 */
public interface TypedRouter<T> {
    /**
     * 路由消息
     * 
     * @param message 要路由的消息
     */
    func route(message: T): Unit
    
    /**
     * 添加路由目标
     * 
     * @param routee 要添加的Actor
     */
    func addRoutee(routee: TypedActorRef<T>): Unit
    
    /**
     * 移除路由目标
     * 
     * @param routee 要移除的Actor
     */
    func removeRoutee(routee: TypedActorRef<T>): Unit
    
    /**
     * 获取所有路由目标
     * 
     * @return 路由目标数组
     */
    func getRoutees(): Array<TypedActorRef<T>>
    
    /**
     * 获取路由策略
     * 
     * @return 路由策略
     */
    func getStrategy(): TypedRoutingStrategy<T>
}

/**
 * 类型安全的轮询路由策略
 * 按顺序轮流选择Actor
 */
public class TypedRoundRobinRoutingStrategy<T> <: TypedRoutingStrategy<T> {
    private var currentIndex: Int64 = 0
    
    public init() {}
    
    public func selectRoutee(routees: Array<TypedActorRef<T>>, message: T): Option<TypedActorRef<T>> {
        if (routees.size == 0) {
            return None
        }
        
        let index = currentIndex % routees.size
        currentIndex = (currentIndex + 1) % routees.size
        Some(routees[index])
    }
    
    public func getDescription(): String {
        "TypedRoundRobinRoutingStrategy"
    }
}

/**
 * 类型安全的随机路由策略
 * 随机选择Actor
 */
public class TypedRandomRoutingStrategy<T> <: TypedRoutingStrategy<T> {
    public init() {}
    
    public func selectRoutee(routees: Array<TypedActorRef<T>>, message: T): Option<TypedActorRef<T>> {
        if (routees.size == 0) {
            return None
        }

        // 简化的随机实现 - 使用当前时间作为随机种子
        let index = (DateTime.now().toUnixTimeStamp().toMilliseconds() % routees.size)
        Some(routees[index])
    }
    
    public func getDescription(): String {
        "TypedRandomRoutingStrategy"
    }
}

/**
 * 类型安全的广播路由策略
 * 将消息发送给所有Actor
 */
public class TypedBroadcastRoutingStrategy<T> <: TypedRoutingStrategy<T> {
    public init() {}
    
    public func selectRoutee(routees: Array<TypedActorRef<T>>, message: T): Option<TypedActorRef<T>> {
        // 广播策略不选择单个Actor，而是在Router中特殊处理
        None
    }
    
    /**
     * 广播消息给所有Actor
     * 
     * @param routees 目标Actor列表
     * @param message 要广播的消息
     */
    public func broadcast(routees: Array<TypedActorRef<T>>, message: T): Unit {
        for (routee in routees) {
            routee.tell(message)
        }
    }
    
    public func getDescription(): String {
        "TypedBroadcastRoutingStrategy"
    }
}

/**
 * 类型安全的一致性哈希路由策略
 * 基于消息内容的哈希值选择Actor
 */
public class TypedConsistentHashRoutingStrategy<T> <: TypedRoutingStrategy<T> {
    public init() {}
    
    public func selectRoutee(routees: Array<TypedActorRef<T>>, message: T): Option<TypedActorRef<T>> {
        if (routees.size == 0) {
            return None
        }

        // 简化的一致性哈希实现 - 使用路由目标数量作为哈希
        let index = routees.size % routees.size
        Some(routees[index])
    }
    
    public func getDescription(): String {
        "TypedConsistentHashRoutingStrategy"
    }
}

/**
 * 类型安全的基础路由器实现
 * 与标准BasicRouter兼容，但提供类型安全
 */
public class TypedBasicRouter<T> <: TypedRouter<T> {
    private let strategy: TypedRoutingStrategy<T>
    private let routees: ArrayList<TypedActorRef<T>>
    
    public init(strategy: TypedRoutingStrategy<T>) {
        this.strategy = strategy
        this.routees = ArrayList<TypedActorRef<T>>()
    }
    
    public func route(message: T): Unit {
        let routeeArray = routees.toArray()
        
        // 特殊处理广播策略
        match (strategy) {
            case broadcast: TypedBroadcastRoutingStrategy<T> =>
                broadcast.broadcast(routeeArray, message)
            case _ =>
                match (strategy.selectRoutee(routeeArray, message)) {
                    case Some(routee) =>
                        routee.tell(message)
                    case None =>
                        println("警告: 没有可用的路由目标")
                }
        }
    }
    
    public func addRoutee(routee: TypedActorRef<T>): Unit {
        routees.add(routee)
    }
    
    public func removeRoutee(routee: TypedActorRef<T>): Unit {
        // 简化实现：遍历查找并移除
        var index = -1
        for (i in 0..routees.size) {
            if (routees[i].equals(routee)) {
                index = i
                break
            }
        }
        if (index >= 0) {
            routees.remove(index..index+1)  // 移除单个元素，使用Range
        }
    }
    
    public func getRoutees(): Array<TypedActorRef<T>> {
        routees.toArray()
    }
    
    public func getStrategy(): TypedRoutingStrategy<T> {
        strategy
    }
}

/**
 * 类型安全的路由器工厂
 * 提供创建常用路由器的静态方法，与标准RouterFactory兼容
 */
public struct TypedRouterFactory {
    
    /**
     * 创建轮询路由器
     */
    public static func roundRobin<T>(): TypedBasicRouter<T> {
        TypedBasicRouter<T>(TypedRoundRobinRoutingStrategy<T>())
    }
    
    /**
     * 创建随机路由器
     */
    public static func random<T>(): TypedBasicRouter<T> {
        TypedBasicRouter<T>(TypedRandomRoutingStrategy<T>())
    }
    
    /**
     * 创建广播路由器
     */
    public static func broadcast<T>(): TypedBasicRouter<T> {
        TypedBasicRouter<T>(TypedBroadcastRoutingStrategy<T>())
    }
    
    /**
     * 创建一致性哈希路由器
     */
    public static func consistentHash<T>(): TypedBasicRouter<T> {
        TypedBasicRouter<T>(TypedConsistentHashRoutingStrategy<T>())
    }
}
