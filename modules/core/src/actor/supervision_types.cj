/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Core - 监督核心类型
 * 放在 actor 包中，使 Actor（supervisorStrategy）和 Props（withSupervisorStrategy）可以直接引用；
 * cactor_core.supervision 重导出这些类型，原有导入不受影响。
 */
package cactor_core.actor

import std.time.*

/**
 * 监督指令
 * 定义当Actor失败时应该采取的行动
 */
public enum SupervisionDirective {
    | Resume      // 恢复Actor，忽略失败
    | Restart     // 重启Actor
    | Stop        // 停止Actor
    | Escalate    // 将失败上报给上级监督者
}

/**
 * Actor失败信息
 */
public struct ActorFailure {
    public let actor: ActorRef
    public let cause: Exception
    public let timestamp: Int64
    public let failureCount: Int64

    public init(actor: ActorRef, cause: Exception, failureCount: Int64) {
        this.actor = actor
        this.cause = cause
        this.timestamp = DateTime.now().toUnixTimeStamp().toMilliseconds()
        this.failureCount = failureCount
    }
}

/**
 * 监督策略接口
 */
public interface SupervisionStrategy {
    /**
     * 决定如何处理Actor失败
     */
    func decide(failure: ActorFailure): SupervisionDirective

    /**
     * 指令是否作用于所有兄弟 Actor（OneForAll 语义）。
     * 默认 false：只作用于失败的那个子 Actor（OneForOne 语义）。
     */
    func appliesToAllChildren(): Bool {
        false
    }
}

