/*
 * Copyright (c) 2024. All rights reserved.
 * Foundation Layer - 互斥锁保护的 HashMap
 *
 * 此前用 AtomicBool 自旋锁（P13）：既不让出 CPU，也没有 finally——键的 hashCode/== 抛异常时锁永远不释放，
 * 之后所有调用方空转到死；put 恒返回 None。现在用 synchronized(Mutex)，异常时自动释放；
 * put 返回旧值。高并发读写请直接用 std.collection.concurrent.ConcurrentHashMap。
 */
package cactor_foundation.queue

import std.sync.*
import std.collection.*

/**
 * 线程安全的 HashMap（单把互斥锁）
 */
public class ConcurrentHashMap<K, V> where K <: Hashable & Equatable<K> {
    private let map: HashMap<K, V>
    private let lock: Mutex

    public init() {
        this.map = HashMap<K, V>()
        this.lock = Mutex()
    }

    /**
     * 插入键值对；返回被替换的旧值（键原先不存在时为 None）
     */
    public func put(key: K, value: V): Option<V> {
        synchronized(lock) {
            map.add(key, value)
        }
    }

    /**
     * 键不存在时才插入；返回已存在的值（此时不插入），插入成功返回 None
     */
    public func putIfAbsent(key: K, value: V): Option<V> {
        synchronized(lock) {
            match (map.get(key)) {
                case Some(existing) => Some(existing)
                case None =>
                    map.add(key, value)
                    None<V>
            }
        }
    }

    /**
     * 获取值
     */
    public func get(key: K): Option<V> {
        synchronized(lock) {
            map.get(key)
        }
    }

    /**
     * 移除键值对；返回被移除的值
     */
    public func remove(key: K): Option<V> {
        synchronized(lock) {
            map.remove(key)
        }
    }

    /**
     * 检查是否包含键
     */
    public func containsKey(key: K): Bool {
        synchronized(lock) {
            map.contains(key)
        }
    }

    /**
     * 清空所有元素
     */
    public func clear(): Unit {
        synchronized(lock) {
            map.clear()
        }
    }

    /**
     * 获取大小
     */
    public func size(): Int64 {
        synchronized(lock) {
            map.size
        }
    }

    /**
     * 检查是否为空
     */
    public func isEmpty(): Bool {
        synchronized(lock) {
            map.isEmpty()
        }
    }
}
