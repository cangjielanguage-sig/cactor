/*
 * CActor Runtime - CoordinatedShutdown（计划 E34，对标 Akka CoordinatedShutdown）
 *
 * 按阶段关闭系统：阶段按依赖顺序串行执行，同一阶段内的任务并行执行（各在一个轻量线程上），
 * 每个阶段有超时，超时或任务异常只记日志、不阻塞后续阶段（Akka 的 recover = on）。
 *
 * 与旧实现的区别（R17）：
 * - 由 CActorRuntime 实例持有：旧实现是按系统名索引的静态表，表里的闭包永不释放，
 *   同名系统反复创建时还会把上一个系统的任务带到下一个系统里执行；
 * - 用户 Actor 在倒数第二个阶段 actor-system-terminate 中停止：此前先停止全部 Actor 再执行各阶段，
 *   before-service-unbind 等阶段里的任务已无法与 Actor 交互；
 * - 每阶段有超时，一个卡住的任务不会让关闭永远结束不了；
 * - run 只执行一次：并发或重复调用都等待同一次执行完成。
 */
package cactor_runtime.lifecycle

import std.collection.*
import std.sync.*
import std.time.*
import cactor_foundation.logging.{Logger, Logging}

let log_coordinated_shutdown: Logger = Logging.logger("cactor_runtime.lifecycle.coordinated_shutdown")

/** 一个阶段：名称 + 超时 + 任务 */
class ShutdownPhase {
    let name: String
    let timeout: Duration
    let tasks = ArrayList<(String, () -> Unit)>()

    init(name: String, timeout: Duration) {
        this.name = name
        this.timeout = timeout
    }
}

public class CoordinatedShutdown {
    // ---- 内置阶段（名称、顺序与 Akka 相同；集群相关阶段供 cluster 模块注册任务） ----
    public static let PhaseBeforeServiceUnbind = "before-service-unbind"
    public static let PhaseServiceUnbind = "service-unbind"
    public static let PhaseServiceRequestsDone = "service-requests-done"
    public static let PhaseServiceStop = "service-stop"
    public static let PhaseBeforeClusterShutdown = "before-cluster-shutdown"
    public static let PhaseClusterLeave = "cluster-leave"
    public static let PhaseClusterShutdown = "cluster-shutdown"
    public static let PhaseBeforeActorSystemTerminate = "before-actor-system-terminate"
    /** 停止全部用户 Actor（倒数第二个阶段） */
    public static let PhaseActorSystemTerminate = "actor-system-terminate"
    /** 释放运行时资源（最后一个阶段，此时已没有存活的用户 Actor） */
    public static let PhaseSystemTerminate = "system-terminate"

    /** 阶段默认超时（与 Akka default-phase-timeout 相同） */
    public static let DEFAULT_PHASE_TIMEOUT: Duration = Duration.second * 5

    private static let registry: HashMap<String, CoordinatedShutdown> = HashMap<String, CoordinatedShutdown>()
    private static let regMutex: Mutex = Mutex()

    /**
     * 按名称取全局实例（旧 API）。
     * 静态表永不释放其中的闭包，同名系统之间还会共享任务；运行时请用 CActorRuntime.getCoordinatedShutdown()。
     */
    @Deprecated["请改用 CActorRuntime.getCoordinatedShutdown()（实例持有，不泄漏、不跨系统共享任务）", since: "1.2.0"]
    public static func get(systemKey: String): CoordinatedShutdown {
        synchronized(regMutex) {
            match (registry.get(systemKey)) {
                case Some(cs) => cs
                case None =>
                    let cs = CoordinatedShutdown(systemKey)
                    registry[systemKey] = cs
                    cs
            }
        }
    }

    private let systemKey: String
    private let mutex = Mutex()
    private let phases = ArrayList<ShutdownPhase>()
    private var runFuture: Option<Future<Unit>> = None<Future<Unit>>

    public init(systemKey: String) {
        this.systemKey = systemKey
        for (p in [PhaseBeforeServiceUnbind, PhaseServiceUnbind, PhaseServiceRequestsDone, PhaseServiceStop,
            PhaseBeforeClusterShutdown, PhaseClusterLeave, PhaseClusterShutdown, PhaseBeforeActorSystemTerminate]) {
            phases.add(ShutdownPhase(p, DEFAULT_PHASE_TIMEOUT))
        }
        // 停止 Actor 需要等全部 postStop 完成，给更长的时限
        phases.add(ShutdownPhase(PhaseActorSystemTerminate, Duration.second * 15))
        phases.add(ShutdownPhase(PhaseSystemTerminate, DEFAULT_PHASE_TIMEOUT))
    }

    /** 已注册的阶段名，按执行顺序 */
    public func phaseNames(): Array<String> {
        synchronized(mutex) {
            Array<String>(phases.size, { i => phases[i].name })
        }
    }

    /** 新增阶段，追加在最后（已存在则忽略） */
    public func addPhase(phase: String): Unit {
        addPhase(phase, DEFAULT_PHASE_TIMEOUT)
    }

    /** 新增阶段并指定超时，追加在最后（已存在则忽略） */
    public func addPhase(phase: String, timeout: Duration): Unit {
        synchronized(mutex) {
            if (findPhase(phase).isNone()) {
                phases.add(ShutdownPhase(phase, timeout))
            }
        }
    }

    private func findPhase(phase: String): Option<ShutdownPhase> {
        for (p in phases) {
            if (p.name == phase) {
                return Some(p)
            }
        }
        None
    }

    /** 在指定阶段注册任务（阶段不存在时追加为新阶段） */
    public func registerTask(phase: String, task: () -> Unit): Unit {
        registerTask(phase, "task", task)
    }

    /** 在指定阶段注册具名任务（名称用于超时 / 失败日志）；关闭已开始后注册的任务不会执行 */
    public func registerTask(phase: String, name: String, task: () -> Unit): Unit {
        synchronized(mutex) {
            if (runFuture.isSome()) {
                log_coordinated_shutdown.warn("[CoordinatedShutdown] ${systemKey} 已在关闭，忽略阶段 ${phase} 的任务 ${name}")
                return
            }
            let p = match (findPhase(phase)) {
                case Some(x) => x
                case None =>
                    let x = ShutdownPhase(phase, DEFAULT_PHASE_TIMEOUT)
                    phases.add(x)
                    x
            }
            p.tasks.add((name, task))
        }
    }

    /** 是否已开始关闭 */
    public func isRunning(): Bool {
        synchronized(mutex) {
            runFuture.isSome()
        }
    }

    /** 异步执行全部阶段；只执行一次，重复调用返回同一个 Future */
    public func runAsync(): Future<Unit> {
        synchronized(mutex) {
            match (runFuture) {
                case Some(f) => f
                case None =>
                    let snapshot = phases.toArray()
                    let key = systemKey
                    let f = spawn {
                        for (p in snapshot) {
                            runPhase(key, p)
                        }
                    }
                    runFuture = Some(f)
                    f
            }
        }
    }

    /** 执行全部阶段并等待结束；只执行一次，并发调用都等待同一次执行完成 */
    public func run(): Unit {
        runAsync().get()
    }
}

/** 并行执行一个阶段的全部任务，最多等待阶段超时；异常与超时只记日志 */
func runPhase(systemKey: String, phase: ShutdownPhase): Unit {
    if (phase.tasks.isEmpty()) {
        return
    }
    let deadline = MonoTime.now() + phase.timeout
    let running = ArrayList<(String, Future<Unit>)>()
    for ((name, task) in phase.tasks) {
        running.add((name, spawn {
            try {
                task()
            } catch (e: Exception) {
                log_coordinated_shutdown.error("[CoordinatedShutdown] ${systemKey} 阶段 ${phase.name} 的任务 ${name} 失败: ${e}")
            }
        }))
    }
    for ((name, f) in running) {
        let remaining = deadline - MonoTime.now()
        if (remaining <= Duration.Zero) {
            log_coordinated_shutdown.warn("[CoordinatedShutdown] ${systemKey} 阶段 ${phase.name} 超时（${phase.timeout}），任务 ${name} 未完成，继续下一阶段")
            continue
        }
        try {
            f.get(remaining)
        } catch (e: TimeoutException) {
            log_coordinated_shutdown.warn("[CoordinatedShutdown] ${systemKey} 阶段 ${phase.name} 超时（${phase.timeout}），任务 ${name} 未完成，继续下一阶段")
        }
    }
}
