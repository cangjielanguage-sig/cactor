/*
 * CActor Core.Typed - TypedActorRef helpers & selection basics
 */
package cactor.core.typed

import std.collection.*
import std.time.*

/**
 * 简单的TypedActorRef 工具与选择器骨架
 */
public struct TypedActorRefs {
    /** 判断相等（基于路径） */
    public static func equals<T>(a: TypedActorRef<T>, b: TypedActorRef<T>): Bool { a.pathString() == b.pathString() }
}

/**
 * 简单的基于路径的选择器（默认不解析通配符，仅保留骨架）
 */
public class SimpleTypedActorSelection<T> <: TypedActorSelection<T> {
    private let path: String
    private let candidates: Array<TypedActorRef<T>>

    public init(path: String, candidates: Array<TypedActorRef<T>>) {
        this.path = path
        this.candidates = candidates
    }

    public func resolveOne(timeout: Duration): Option<TypedActorRef<T>> {
        if (candidates.size > 0) { Some(candidates[0]) } else { None }
    }

    public func tell(message: T): Unit {
        for (ref in candidates) { ref.tell(message) }
    }

    public func pathString(): String { path }
}

