/*
 * CActor Core.Typed - TypedActorRef helpers & selection basics
 */
package cactor_core.typed

import std.collection.*

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
        // 返回第一个匹配的候选
        for (ref in candidates) {
            if (matches(path, ref.pathString())) { return Some(ref) }
        }
        None
    }

    public func tell(message: T): Unit {
        // 仅向匹配的候选发送
        for (ref in candidates) {
            if (matches(path, ref.pathString())) { ref.tell(message) }
        }
    }

    public func pathString(): String { path }

    // =============== 路径匹配：支持 * 与 ** ===============
    private func splitPath(p: String): Array<String> {
        // 按 '/' 分割然后手动移除空段
        let parts = p.split("/")
        let list = ArrayList<String>()
        for (s in parts) { if (s != "") { list.add(s) } }
        let n = Int64(list.size)
        Array<String>(n, { i => list[i] })
    }

    private func matches(pattern: String, candidate: String): Bool {
        let pSegs = splitPath(pattern)
        let cSegs = splitPath(candidate)
        matchSegments(pSegs, Int64(0), cSegs, Int64(0))
    }

    private func matchSegments(pSegs: Array<String>, pi: Int64, cSegs: Array<String>, ci: Int64): Bool {
        if (pi == pSegs.size && ci == cSegs.size) { return true }
        if (pi == pSegs.size) { return false }
        let p = pSegs[pi]
        if (p == "**") {
            // 匹配零个或多个段
            if (matchSegments(pSegs, pi + 1, cSegs, ci)) { return true }
            if (ci < cSegs.size && matchSegments(pSegs, pi, cSegs, ci + 1)) { return true }
            return false
        } else {
            if (ci >= cSegs.size) { return false }
            if (p == "*" || p == cSegs[ci]) {
                return matchSegments(pSegs, pi + 1, cSegs, ci + 1)
            } else {
                return false
            }
        }
    }
}

