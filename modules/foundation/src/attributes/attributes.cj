/*
 * CActor Foundation - 类型安全的属性表
 * 以 AttributeKey<T> 的唯一 id 为键，取值时按 T 向下转型；供 ApplicationCall、插件、ActorSystem 共用。
 */
package cactor_foundation.attributes

import std.sync.*
import std.collection.*

let keySequence = AtomicInt64(0)

public class AttributeKey<T> {
    public let name: String
    public let id: Int64

    public init(name: String) {
        this.name = name
        this.id = keySequence.fetchAdd(1)
    }

    public func toString(): String {
        "AttributeKey(${name})"
    }
}

public class Attributes {
    private let lock = Mutex()
    private let values = HashMap<Int64, Any>()

    public init() {}

    public func put<T>(key: AttributeKey<T>, value: T): Unit {
        synchronized(lock) {
            values.add(key.id, value)
        }
    }

    public func get<T>(key: AttributeKey<T>): Option<T> {
        synchronized(lock) {
            match (values.get(key.id)) {
                case Some(v) => v as T
                case None => None<T>
            }
        }
    }

    public func getOrElse<T>(key: AttributeKey<T>, fallback: T): T {
        get(key) ?? fallback
    }

    public func contains<T>(key: AttributeKey<T>): Bool {
        synchronized(lock) { values.contains(key.id) }
    }

    public func remove<T>(key: AttributeKey<T>): Unit {
        synchronized(lock) {
            values.remove(key.id)
        }
    }

    /** 不存在时用 create 计算并保存（原子） */
    public func computeIfAbsent<T>(key: AttributeKey<T>, create: () -> T): T {
        synchronized(lock) {
            match (values.get(key.id)) {
                case Some(v) => match (v as T) {
                    case Some(t) => t
                    case None => throw IllegalStateException("属性类型不匹配: ${key.name}")
                }
                case None =>
                    let t = create()
                    values.add(key.id, t)
                    t
            }
        }
    }
}
