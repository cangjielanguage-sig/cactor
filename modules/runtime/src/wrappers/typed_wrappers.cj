/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Runtime - 经典引用与类型化引用的互操作
 *
 * 类型化 Actor 的真实实现在 cactor_core.typed（typed_adapter.cj）。这里只保留两个互操作工具：
 * - TypedActorRefWrapper：把经典 ActorRef 当作 TypedActorRef<T> 使用（消息以 TypedEnvelope<T> 投递）
 * - MessageBridge：同时持有经典与类型化引用，便于两侧互发消息
 */
package cactor_runtime.wrappers

import cactor_core.actor.{ActorRef}
import cactor_core.message.{Message}
import cactor_core.typed.{TypedActorRef, ClassicTypedRef}

/**
 * 以协议 T 包装经典引用
 */
public class TypedActorRefWrapper<T> <: TypedActorRef<T> {
    private let underlying: ClassicTypedRef<T>

    public init(actorRef: ActorRef, name: String) {
        this.underlying = ClassicTypedRef<T>(actorRef)
    }

    public func tell(message: T): Unit {
        underlying.tell(message)
    }

    public func pathString(): String {
        underlying.pathString()
    }

    public func equals(other: TypedActorRef<T>): Bool {
        pathString() == other.pathString()
    }

    public func toClassic(): Option<ActorRef> {
        Some(underlying.classic)
    }

    public func getUnderlyingActorRef(): ActorRef {
        underlying.classic
    }
}

/**
 * 经典 Actor 与类型化 Actor 之间的消息桥
 */
public class MessageBridge<T> {
    private let standardActor: ActorRef
    private let typedActor: TypedActorRef<T>

    public init(standardActor: ActorRef, typedActor: TypedActorRef<T>) {
        this.standardActor = standardActor
        this.typedActor = typedActor
    }

    /** 向类型化 Actor 发送 */
    public func sendToTyped(message: T): Unit {
        typedActor.tell(message)
    }

    /** 向经典 Actor 发送 */
    public func sendToStandard(message: Message): Unit {
        standardActor.tell(message)
    }

    public func getStandardActor(): ActorRef {
        standardActor
    }

    public func getTypedActor(): TypedActorRef<T> {
        typedActor
    }
}
