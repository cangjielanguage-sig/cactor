/*
 * CActor Core.Typed - 生命周期信号（对标 Akka Typed Signal）
 */
package cactor_core.typed

import cactor_core.actor.{ActorRef}

/**
 * 生命周期信号，由 TypedBehavior.onSignal 处理
 */
public interface TypedSignal {
    func signalName(): String
}

/** Actor 已停止（所有子 Actor 已停止之后投递） */
public class PostStop <: TypedSignal {
    public init() {}

    public func signalName(): String {
        "PostStop"
    }
}

/** Actor 即将因失败而重启（旧行为收到） */
public class PreRestart <: TypedSignal {
    public let cause: Exception

    public init(cause: Exception) {
        this.cause = cause
    }

    public func signalName(): String {
        "PreRestart"
    }
}

/** 被监视的 Actor 已停止 */
public class TypedTerminated <: TypedSignal {
    public let ref: ActorRef

    public init(ref: ActorRef) {
        this.ref = ref
    }

    public func signalName(): String {
        "Terminated"
    }

    /** 已停止 Actor 的路径 */
    public func pathString(): String {
        ref.path.getPath()
    }

    /** 是否是 other 停止了 */
    public func isFor<U>(other: TypedActorRef<U>): Bool {
        pathString() == other.pathString()
    }
}
