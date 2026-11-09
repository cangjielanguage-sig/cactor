/*
 * Copyright (c) 2024. All rights reserved.
 * SimpleActorRegistry - 简化的Actor注册表
 * 基于plan10.md设计，提供基本的Actor注册和查找服务
 */
protected package cactor_runtime.registry

import std.sync.*
import std.collection.*
import std.time.*
import cactor_core.actor.{ActorRef}

/**
 * 简化的Actor注册表
 * 
 * 职责：
 * 1. 维护Actor的注册信息
 * 2. 提供基本的Actor查找服务
 * 3. 支持Actor路径解析
 */
public class SimpleActorRegistry {
    // 名称 -> Actor（顶层 Actor 的唯一索引）。
    // 为什么不再维护"完整路径 -> 条目"和"路径 -> 名称"两张附加表：
    // 顶层路径恒为 "/user/<名字>"，路径与名称一一对应；原来的 pathToActor + nameToPath
    // 在百万级顶层 Actor 时每个 Actor 多占一份完整路径 String、一个包装条目与两个表项，
    // 实测合计约 500–700 B/Actor（对标 Akka 的 spawn 基准就是顶层 Actor，这一项影响最大）。
    private let nameToActor: HashMap<String, ActorRef>
    
    // 统计信息
    private let totalActors: AtomicInt64
    private let registrationCount: AtomicInt64
    // 保护 nameToActor 与统计的复合操作
    private let lock: Mutex
    
    public init() {
        this.nameToActor = HashMap<String, ActorRef>()
        this.totalActors = AtomicInt64(0)
        this.registrationCount = AtomicInt64(0)
        this.lock = Mutex()
    }
    
    /**
     * 注册Actor
     */
    public func register(actorRef: ActorRef, parentPath: Option<String>): Unit {
        synchronized(lock) {
            let name = extractActorName(actorRef.path.toString())
            // 已经注册（同名同引用）则忽略；同名的是已终止、注销还没执行的旧实例时直接替换（总数不变）
            let replacing = match (nameToActor.get(name)) {
                case Some(existing) where existing.equals(actorRef) => return
                case Some(_) => true
                case None => false
            }
            nameToActor[name] = actorRef
            if (!replacing) {
                totalActors.fetchAdd(1)
            }
            registrationCount.fetchAdd(1)
        }
    }
    
    /**
     * 注销Actor
     */
    public func unregister(actorRef: ActorRef): Unit {
        // 只注销这个实例：同名的新实例可能已经替换了它（旧实例迟到的注销不能删掉新实例的登记）
        synchronized(lock) {
            let name = extractActorName(actorRef.path.toString())
            match (nameToActor.get(name)) {
                case Some(existing) where existing.equals(actorRef) =>
                    nameToActor.remove(name)
                    totalActors.fetchAdd(-1)
                case _ => ()
            }
        }
    }
    
    /**
     * 通过路径注销Actor
     */
    public func unregisterByPath(path: String): Unit {
        synchronized(lock) {
            let name = extractActorName(path)
            if (nameToActor.remove(name).isSome()) {
                totalActors.fetchAdd(-1)
            }
        }
    }
    
    /**
     * 通过路径查找Actor
     */
    public func findByPath(path: String): Option<ActorRef> {
        findByName(extractActorName(path))
    }
    
    /**
     * 通过名称查找Actor
     */
    public func findByName(name: String): Option<ActorRef> {
        synchronized(lock) {
            match (nameToActor.get(name)) {
                case Some(ref) => Some(ref)
                case None => None<ActorRef>
            }
        }
    }
    
    /**
     * 检查Actor是否已注册
     */
    public func isRegistered(actorRef: ActorRef): Bool {
        synchronized(lock) {
            match (nameToActor.get(extractActorName(actorRef.path.toString()))) {
                case Some(existing) => existing.equals(actorRef)
                case None => false
            }
        }
    }
    
    /**
     * 获取注册的Actor总数
     */
    public func getTotalActorCount(): Int64 {
        totalActors.load()
    }
    
    /**
     * 获取注册次数
     */
    public func getRegistrationCount(): Int64 {
        registrationCount.load()
    }
    
    /**
     * 获取所有注册的Actor
     */
    public func getAllActors(): Array<ActorRef> {
        synchronized(lock) {
            nameToActor.values().toArray()
        }
    }
    
    /**
     * 清空注册表
     */
    public func clear(): Unit {
        synchronized(lock) {
            nameToActor.clear()
            totalActors.store(0)
        }
    }
    
    /**
     * 打印注册表统计信息
     */
    public func printStats(): Unit {
        println("=== SimpleActorRegistry统计信息 ===")
        println("总Actor数量: ${getTotalActorCount()}")
        println("注册次数: ${getRegistrationCount()}")
        println("名称索引大小: ${countNames()}")
    }

    private func countNames(): Int32 {
        Int32(nameToActor.size)
    }
    
    /**
     * 从路径中提取Actor名称
     */
    private func extractActorName(path: String): String {
        // 取路径最后一段作为名称。旧实现直接返回整条路径，导致 nameToPath 与 pathToActor
        // 是两张同键表（白占一份索引），且 validateActorCreation 用裸名字 findByName 永远查不到
        // → 顶层重名检测形同虚设。改为真正的末段提取后，两处语义都正确。
        match (path.lastIndexOf("/")) {
            case Some(i) => path[(i + 1)..]
            case None => path
        }
    }
    
}
