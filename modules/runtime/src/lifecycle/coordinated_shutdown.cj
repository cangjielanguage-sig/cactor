/*
 * CActor Runtime - CoordinatedShutdown (minimal, real implementation)
 */
package cactor_runtime.lifecycle

import std.collection.*
import std.sync.*
import std.time.*

/**
 * 一个最小可用的协调关闭实现：
 * - 支持按阶段注册任务
 * - run() 时按阶段顺序依次执行
 * - 每个阶段中的任务按注册顺序执行
 * - 失败不阻塞其它任务（记录日志并继续）
 */
public class CoordinatedShutdown {
    private static let registry: HashMap<String, CoordinatedShutdown> = HashMap<String, CoordinatedShutdown>()
    private static let regMutex: Mutex = Mutex()

    public static func get(systemKey: String): CoordinatedShutdown {
        regMutex.lock()
        match (registry.get(systemKey)) {
            case Some(cs) => regMutex.unlock(); cs
            case None =>
                let cs = CoordinatedShutdown(systemKey)
                registry.add(systemKey, cs)
                regMutex.unlock()
                cs
        }
    }

    private let systemKey: String
    private let phases: ArrayList<String>
    private let tasks: HashMap<String, ArrayList<() -> Unit>>
    private let mutex: Mutex

    public init(systemKey: String) {
        this.systemKey = systemKey
        this.mutex = Mutex()
        this.phases = ArrayList<String>()
        this.tasks = HashMap<String, ArrayList<() -> Unit>>()
        // 默认阶段顺序（对齐 Akka 的简化版）
        addPhase("before-service-unbind")
        addPhase("service-unbind")
        addPhase("service-stop")
        addPhase("actor-system-terminate")
        addPhase("system-terminate")
    }

    /** 新增阶段（若已存在则忽略） */
    public func addPhase(phase: String): Unit {
        mutex.lock()
        if (!containsPhase(phase)) {
            phases.add(phase)
            tasks.add(phase, ArrayList<() -> Unit>())
        }
        mutex.unlock()
    }

    private func containsPhase(phase: String): Bool {
        for (p in phases) { if (p == phase) { return true } }
        false
    }

    /** 在指定阶段注册任务 */
    public func registerTask(phase: String, task: () -> Unit): Unit {
        mutex.lock()
        if (!containsPhase(phase)) { addPhase(phase) }
        match (tasks.get(phase)) {
            case Some(list) => list.add(task)
            case None =>
                let list = ArrayList<() -> Unit>()
                list.add(task)
                tasks.add(phase, list)
        }
        mutex.unlock()
    }

    /** 执行所有阶段任务 */
    public func run(): Unit {
        // 顺序执行各阶段
        for (phase in phases) {
            executePhase(phase)
        }
    }

    private func executePhase(phase: String): Unit {
        mutex.lock()
        let list = match (tasks.get(phase)) { case Some(l) => l.toArray(); case None => Array<() -> Unit>() }
        mutex.unlock()
        for (t in list) {
            try { t() } catch (e: Exception) {
                println("[CoordinatedShutdown] phase='${phase}' task failed: ${e.message}")
            }
        }
    }
}

