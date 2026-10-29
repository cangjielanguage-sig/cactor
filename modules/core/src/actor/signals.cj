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

/** 创建子 Actor 时名称冲突或非法 */
public class InvalidActorNameException <: Exception {
    public init(message: String) {
        super(message)
    }
}
