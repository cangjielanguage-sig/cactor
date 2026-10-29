/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 核心Actor接口
 * 基于仓颉语言特性的高性能Actor系统
 */
package cactor_core.actor

import std.time.*
import cactor_core.message.*

/**
 * Actor生命周期状态
 */
public enum ActorLifecycleState {
    | Created      // 已创建，未启动
    | Starting     // 启动中
    | Running      // 运行中
    | Stopping     // 停止中
    | Stopped      // 已停止
    | Failed       // 失败状态
    | Restarting   // 重启中
    
    public func toInt64(): Int64 {
        match (this) {
            case Created => 0
            case Starting => 1
            case Running => 2
            case Stopping => 3
            case Stopped => 4
            case Failed => 5
            case Restarting => 6
        }
    }
    
    public static func fromInt64(value: Int64): ActorLifecycleState {
        match (value) {
            case 0 => Created
            case 1 => Starting
            case 2 => Running
            case 3 => Stopping
            case 4 => Stopped
            case 5 => Failed
            case 6 => Restarting
            case _ => Stopped
        }
    }
}

/**
 * Actor基础接口
 * 定义Actor的核心行为和生命周期
 */
public interface Actor {
    /**
     * Actor的唯一名称
     */
    prop name: String

    /**
     * Actor的功能描述
     */
    prop description: String

    /**
     * 处理接收到的消息
     * @param message 接收到的消息
     * @param context Actor上下文
     * @return 消息处理结果
     */
    func receive(message: Message, context: ActorContext): MessageResult

    /**
     * Actor启动前的初始化操作
     * 在Actor开始处理消息之前调用
     */
    func preStart(): Unit {
        // 默认空实现
    }

    /**
     * Actor启动后的操作
     * 在Actor开始处理消息后调用
     */
    func postStart(): Unit {
        // 默认空实现
    }

    /**
     * Actor停止前的清理操作
     * 在Actor停止处理消息前调用
     */
    func preStop(): Unit {
        // 默认空实现
    }

    /**
     * Actor停止后的清理操作
     * 在Actor完全停止后调用
     */
    func postStop(): Unit {
        // 默认空实现
    }

    /**
     * Actor重启前的操作
     * 当Actor因异常需要重启时调用
     */
    func preRestart(reason: Exception): Unit {
        preStop()
    }

    /**
     * Actor重启后的操作
     * 当Actor重启完成后调用
     */
    func postRestart(reason: Exception): Unit {
        postStart()
    }

    /**
     * 在 Actor 自己的执行线程中、处理第一条消息之前调用（重启后的新实例同样会调用）。
     * 与 preStart 不同，这里可以使用完整的上下文：创建子 Actor、启动定时器、监视其他 Actor 等。
     * 抛出异常按消息处理失败交给监督策略处理。
     */
    func onStarted(context: ActorContext): Unit {
        // 默认空实现
    }

    /**
     * 本 Actor 作为父 Actor 时，对其子 Actor 使用的监督策略（对标 Akka 的 supervisorStrategy）。
     * 返回 None 时使用系统默认策略：OneForOne，异常时重启，1 分钟内最多 10 次。
     * 子 Actor 的 Props 若通过 withSupervisorStrategy 指定了策略，以 Props 为准。
     */
    func supervisorStrategy(): Option<SupervisionStrategy> {
        None<SupervisionStrategy>
    }
}



/**
 * Actor路径
 * 唯一标识Actor在系统中的位置
 */
public struct ActorPath {
    private let system: String
    private let path: String

    public init(system: String, path: String) {
        this.system = system
        this.path = path
    }

    public func getSystem(): String {
        system
    }

    public func getPath(): String {
        path
    }

    public func toString(): String {
        "cactor://${system}${path}"
    }

    public func parent(): Option<ActorPath> {
        // 简化实现，暂时返回None
        // 实际实现需要字符串处理功能
        None
    }

    public func child(name: String): ActorPath {
        ActorPath(system, "${path}/${name}")
    }

    public func equals(other: ActorPath): Bool {
        system == other.system && path == other.path
    }

    public func hashCode(): Int32 {
        // 简化的哈希计算，截取低32位
        Int32(system.hashCode() & 0x7FFFFFFF)
    }
}

/**
 * Actor工厂接口
 * 用于创建Actor实例
 */
public interface ActorFactory<T> where T <: Actor {
    /**
     * 创建Actor实例
     * @return 新的Actor实例
     */
    func create(): T
}

/**
 * 简单Actor工厂实现
 */
public class SimpleActorFactory<T> <: ActorFactory<T> where T <: Actor {
    private let creator: () -> T

    public init(creator: () -> T) {
        this.creator = creator
    }

    public func create(): T {
        creator()
    }
}

/**
 * Props - Actor创建配置
 * 参考Akka的Props设计
 */
/**
 * 有界邮箱满时的处理方式（对标 Akka BoundedMailbox 的 pushTimeout 与 Reactive Streams 的溢出策略）
 */
public enum MailboxOverflow {
    /** 丢弃新消息并计为死信（默认，发送方不受影响） */
    | DropNew
    /** 丢弃新消息并向发送方抛出 MailboxFullException */
    | Fail
    /** 发送方等待空位，最多等待给定时长；超时后按 DropNew 处理。不要在同一调度器的 Actor 之间使用，以免占住工作线程 */
    | Block(Duration)
}

/** 邮箱已满（MailboxOverflow.Fail） */
public class MailboxFullException <: Exception {
    public init(message: String) {
        super(message)
    }
}

public struct Props<T> where T <: Actor {
    private let factory: ActorFactory<T>
    private let dispatcher: String
    private let mailboxType: String
    private let supervisionStrategy: String
    private let mailboxCapacity: Int64
    private let strategy: Option<SupervisionStrategy>
    private let overflow: MailboxOverflow

    public init(factory: ActorFactory<T>) {
        this.factory = factory
        this.dispatcher = "default"
        this.mailboxType = "unbounded"
        this.supervisionStrategy = "default"
        this.mailboxCapacity = 0
        this.strategy = None<SupervisionStrategy>
        this.overflow = MailboxOverflow.DropNew
    }

    private init(factory: ActorFactory<T>, dispatcher: String, mailboxType: String, supervisionStrategy: String,
        mailboxCapacity: Int64, strategy: Option<SupervisionStrategy>, overflow: MailboxOverflow) {
        this.factory = factory
        this.dispatcher = dispatcher
        this.mailboxType = mailboxType
        this.supervisionStrategy = supervisionStrategy
        this.mailboxCapacity = mailboxCapacity
        this.strategy = strategy
        this.overflow = overflow
    }

    /**
     * 调度器："default"（共享的亲和调度线程池）或 "blocking"（独立线程池，用于会阻塞的 Actor）
     */
    public func withDispatcher(dispatcher: String): Props<T> {
        Props<T>(factory, dispatcher, mailboxType, supervisionStrategy, mailboxCapacity, strategy, overflow)
    }

    /**
     * 邮箱类型："unbounded"（默认，无锁）、"bounded"（需配合 withBoundedMailbox 指定容量）、
     * "priority"（按 Message.priority() 从高到低处理，同优先级先进先出）
     */
    public func withMailbox(mailboxType: String): Props<T> {
        Props<T>(factory, this.dispatcher, mailboxType, supervisionStrategy, mailboxCapacity, strategy, overflow)
    }

    /** 有界邮箱：超过容量的新消息被丢弃并计为死信 */
    public func withBoundedMailbox(capacity: Int64): Props<T> {
        Props<T>(factory, dispatcher, "bounded", supervisionStrategy, capacity, strategy, MailboxOverflow.DropNew)
    }

    /** 有界邮箱，并指定邮箱满时的处理方式（背压） */
    public func withBoundedMailbox(capacity: Int64, overflow: MailboxOverflow): Props<T> {
        Props<T>(factory, dispatcher, "bounded", supervisionStrategy, capacity, strategy, overflow)
    }

    public func getMailboxOverflow(): MailboxOverflow {
        overflow
    }

    public func withSupervisionStrategy(strategy: String): Props<T> {
        Props<T>(factory, dispatcher, mailboxType, strategy, mailboxCapacity, this.strategy, overflow)
    }

    /** 为该 Actor 指定监督策略（优先于父 Actor 的 supervisorStrategy） */
    public func withSupervisorStrategy(strategy: SupervisionStrategy): Props<T> {
        Props<T>(factory, dispatcher, mailboxType, supervisionStrategy, mailboxCapacity, Some(strategy), overflow)
    }

    public func getMailboxCapacity(): Int64 {
        mailboxCapacity
    }

    public func getSupervisorStrategy(): Option<SupervisionStrategy> {
        strategy
    }

    public func create(): T {
        factory.create()
    }

    public func getDispatcher(): String {
        dispatcher
    }

    public func getMailboxType(): String {
        mailboxType
    }

    public func getSupervisionStrategy(): String {
        supervisionStrategy
    }

    public func getDispatcherName(): String {
        dispatcher
    }
}

/**
 * Props工厂方法
 */
public struct PropsFactory {
    /**
     * 创建Props
     */
    public static func create<T>(creator: () -> T): Props<T> where T <: Actor {
        Props<T>(SimpleActorFactory<T>(creator))
    }

    /**
     * 创建带调度器的Props
     */
    public static func createWithDispatcher<T>(creator: () -> T, dispatcher: String): Props<T> where T <: Actor {
        Props<T>(SimpleActorFactory<T>(creator)).withDispatcher(dispatcher)
    }
}
