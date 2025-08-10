/*
 * CActor Core.Typed - Typed Protocols & MessageAdapter
 */
package cactor.core.typed

/**
 * 将外部消息类型 In 适配为 Actor 内部消息类型 T
 */
public interface TypedMessageAdapter<In, T> {
    func adapt(message: In): T
}

/**
 * 适配器工具：基于目标 TypedActorRef<T> 暴露一个 TypedActorRef<In>
 * 所有 In 消息将通过适配函数转换为 T 并转发到目标
 */
public struct TypedAdapters {
    public static func messageAdapter<In, T>(target: TypedActorRef<T>, f: (In) -> T): TypedActorRef<In> {
        MessageAdapterRef<In, T>(target, f)
    }
}

internal class MessageAdapterRef<In, T> <: TypedActorRef<In> {
    private let target: TypedActorRef<T>
    private let fn: (In) -> T

    public init(target: TypedActorRef<T>, fn: (In) -> T) {
        this.target = target
        this.fn = fn
    }

    public func tell(message: In): Unit {
        let adapted = fn(message)
        target.tell(adapted)
    }

    public func pathString(): String { target.pathString() + "/adapter" }

    public func equals(other: TypedActorRef<In>): Bool { this.pathString() == other.pathString() }
}

