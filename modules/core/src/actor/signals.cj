/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Core - 生命周期信号与特殊消息（对标 Akka 的 Terminated / PoisonPill / Kill / ReceiveTimeout）
 */
package cactor_core.actor

import cactor_core.message.{Message, AutoReceivedMessage}

/**
 * 被监视的 Actor 已停止。通过 context.watch 订阅，作为普通消息交给 receive 处理。
 * 监视者未处理（返回 Unhandled）时视为 DeathPactException 失败，与 Akka 一致。
 */
public class Terminated <: Message {
    public let actor: ActorRef

    public init(actor: ActorRef) {
        this.actor = actor
    }

    public func messageType(): String {
        "Terminated"
    }
}

/**
 * 不影响接收超时计时的消息（例如定时心跳），对标 Akka NotInfluenceReceiveTimeout
 */
public interface NotInfluenceReceiveTimeout <: Message {}

/**
 * 处理到这条消息时停止 Actor：排在它之前的消息会先被处理完
 */
public class PoisonPill <: AutoReceivedMessage {
    public init() {}

    public func messageType(): String {
        "PoisonPill"
    }

    /** 与普通消息同一优先级：排在已入队的消息之后处理 */
    public func priority(): Int32 {
        0
    }
}

/**
 * 处理到这条消息时让 Actor 以 ActorKilledException 失败，交由监督策略处理
 */
public class Kill <: AutoReceivedMessage {
    public init() {}

    public func messageType(): String {
        "Kill"
    }

    /** 与普通消息同一优先级：排在已入队的消息之后处理 */
    public func priority(): Int32 {
        0
    }
}

/**
 * 空闲超过 setReceiveTimeout 设定的时长
 */
public class ReceiveTimeout <: NotInfluenceReceiveTimeout {
    public init() {}

    public func messageType(): String {
        "ReceiveTimeout"
    }
}

/** 由 Kill 触发的失败 */
public class ActorKilledException <: Exception {
    public init(path: String) {
        super("Actor ${path} 收到 Kill")
    }
}

/** 监视者没有处理 Terminated 时抛出 */
public class DeathPactException <: Exception {
    public init(path: String) {
        super("未处理被监视 Actor ${path} 的 Terminated")
    }
}

/**
 * Actor 启动（onStarted / 类型化 setup）时抛出的异常，原异常见 cause。
 *
 * 与 Akka 的 ActorInitializationException 相同：默认监督策略对它返回 Stop。
 * 否则"启动依赖不可用"会让默认的不限次重启陷入无限循环（顶层 Actor 曾因此同栈递归到 StackOverflowError）。
 * 类型化监督会先解开它，再按原异常匹配 onFailureOf 规则（setup 失败仍可按规则重启）。
 */
public class ActorInitializationException <: Exception {
    public let path: String
    public let cause: Exception

    public init(path: String, cause: Exception) {
        super("Actor ${path} 启动失败: ${cause}")
        this.path = path
        this.cause = cause
    }
}

/**
 * 消息处理中抛出的 Error（如递归过深的 StackOverflowError），包装成异常交给监督。
 * 原先 Error 会逃出工作线程的 catch(Exception) 并杀死线程，使亲和到该线程的 Actor 全部失联。
 */
public class ActorErrorException <: Exception {
    public let path: String
    public let error: Error

    public init(path: String, error: Error) {
        super("Actor ${path} 处理消息时发生 Error: ${error}")
        this.path = path
        this.error = error
    }
}

/** 创建子 Actor 时名称冲突或非法 */
public class InvalidActorNameException <: Exception {
    public init(message: String) {
        super(message)
    }
}

/**
 * Actor 没有处理的消息（receive 返回 Unhandled，或类型化 Actor 收到协议之外的消息）。
 * 对标 Akka 的 UnhandledMessage：和 DeadLetter 一样属于 AllDeadLetters，包装后交给死信订阅者，
 * 订阅者据 `is UnhandledMessage` 区分"没人收"和"收了但没处理"。
 * 不计入 deadLetterCount（那是"没有送达"的计数），另有 unhandledCount
 */
public class UnhandledMessage <: Message {
    /** 原消息 */
    public let message: Message
    /** 没有处理它的 Actor 路径 */
    public let recipient: String

    public init(message: Message, recipient: String) {
        this.message = message
        this.recipient = recipient
    }

    public func messageType(): String {
        "UnhandledMessage"
    }
}
