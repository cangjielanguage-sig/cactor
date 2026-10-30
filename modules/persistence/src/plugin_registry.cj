/*
 * Copyright (c) 2024. All rights reserved.
 * CActor Persistence - 存储插件注册表
 *
 * 用一个 URI 选择存储实现，使部署时可以通过配置（例如环境变量）切换存储，而不用改代码：
 *   "memory:"            内存（测试用）
 *   "file:/var/lib/app"  文件存储，冒号后为目录
 * 其他实现（数据库等）用 registerJournal / registerSnapshotStore / registerDurableStateStore 注册自己的方案名。
 */
package cactor_persistence

import std.sync.*
import std.collection.*

public class PersistencePlugins {
    private static let lock = Mutex()
    private static let journals = HashMap<String, (String) -> EventStore>()
    private static let snapshotStores = HashMap<String, (String) -> SnapshotStore>()
    private static let stateStores = HashMap<String, (String) -> DurableStateStore>()
    private static var defaultsRegistered = false

    private static func ensureDefaults(): Unit {
        if (defaultsRegistered) {
            return
        }
        defaultsRegistered = true
        journals["memory"] = { _ => MemoryEventStore() }
        journals["file"] = { p => FileEventStore(requirePath("file", p)) }
        snapshotStores["memory"] = { _ => MemorySnapshotStore() }
        snapshotStores["file"] = { p => FileSnapshotStore(requirePath("file", p)) }
        stateStores["memory"] = { _ => MemoryDurableStateStore() }
        stateStores["file"] = { p => FileDurableStateStore(requirePath("file", p)) }
    }

    private static func requirePath(scheme: String, p: String): String {
        if (p.isEmpty()) {
            throw IllegalArgumentException("${scheme}: 方案需要目录，例如 ${scheme}:/var/lib/app")
        }
        p
    }

    /** 拆分 "方案:参数" */
    private static func split(uri: String): (String, String) {
        match (uri.indexOf(":")) {
            case Some(i) => (uri[0..i], uri[(i + 1)..])
            case None => throw IllegalArgumentException("存储 URI 需要形如 方案:参数，例如 file:/var/lib/app，实际为 ${uri}")
        }
    }

    public static func registerJournal(scheme: String, factory: (String) -> EventStore): Unit {
        synchronized(lock) {
            ensureDefaults()
            journals[scheme] = factory
        }
    }

    public static func registerSnapshotStore(scheme: String, factory: (String) -> SnapshotStore): Unit {
        synchronized(lock) {
            ensureDefaults()
            snapshotStores[scheme] = factory
        }
    }

    public static func registerDurableStateStore(scheme: String, factory: (String) -> DurableStateStore): Unit {
        synchronized(lock) {
            ensureDefaults()
            stateStores[scheme] = factory
        }
    }

    public static func journal(uri: String): EventStore {
        let (scheme, arg) = split(uri)
        let f = synchronized(lock) {
            ensureDefaults()
            journals.get(scheme)
        }
        match (f) {
            case Some(factory) => factory(arg)
            case None => throw IllegalArgumentException("未注册的事件日志方案: ${scheme}")
        }
    }

    public static func snapshotStore(uri: String): SnapshotStore {
        let (scheme, arg) = split(uri)
        let f = synchronized(lock) {
            ensureDefaults()
            snapshotStores.get(scheme)
        }
        match (f) {
            case Some(factory) => factory(arg)
            case None => throw IllegalArgumentException("未注册的快照存储方案: ${scheme}")
        }
    }

    public static func durableStateStore(uri: String): DurableStateStore {
        let (scheme, arg) = split(uri)
        let f = synchronized(lock) {
            ensureDefaults()
            stateStores.get(scheme)
        }
        match (f) {
            case Some(factory) => factory(arg)
            case None => throw IllegalArgumentException("未注册的状态存储方案: ${scheme}")
        }
    }
}
