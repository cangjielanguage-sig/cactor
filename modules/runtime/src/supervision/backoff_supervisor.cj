/*
 * CActor Runtime - Backoff Supervisor (minimal runtime scheduler)
 */
package cactor_runtime.supervision

import std.sync.*
import std.time.*
import std.collection.*
import cactor_core.actor.{ActorRef}
import cactor_core.supervision.{ActorFailure, BackoffSupervisorStrategy}
import cactor_core.guardian.{GuardianActor}

/**
 * 运行时Backoff监督器：
 * - 使用 BackoffSupervisorStrategy 的 backoff 参数计算下一次重启延迟
 * - 通过 std.sync.Timer.once 安排 guardian.restartChild 的调用
 * - 维护每个子Actor的重试计数（与 core 策略内部计数解耦，最小改动）
 *
 * 注意：目前未强制 maxRetries 上限，由策略决定是否还应重启；
 * 若需要，可在策略中暴露上限或在此处附加阈值判断。
 */
public class BackoffSupervisor {
    private let strategy: BackoffSupervisorStrategy
    private let guardian: GuardianActor
    private let retryCounts: HashMap<String, Int64>
    private let maxRetries: Int64
    private let mutex: Mutex

    public init(strategy: BackoffSupervisorStrategy, guardian: GuardianActor) {
        this.strategy = strategy
        this.guardian = guardian
        this.retryCounts = HashMap<String, Int64>()
        // 从策略推断最大重试上限（策略默认10，可通过重载构造设置）
        this.maxRetries = strategy.getMaxRetries()
        this.mutex = Mutex()
    }

    /**
     * 处理失败：超过最大重试则停止，否则按退避调度重启
     */
    public func handleFailure(failure: ActorFailure): Unit {
        let path = failure.actor.path.toString()
        var shouldStop = false
        var nextDelay = Duration.millisecond * 0

        mutex.lock()
        let current = match (retryCounts.get(path)) { case Some(v) => v; case None => 0 }
        if (current >= maxRetries) {
            shouldStop = true
        } else {
            nextDelay = strategy.calculateBackoffTime(current)
            retryCounts.add(path, current + 1)
        }
        mutex.unlock()

        if (shouldStop) {
            // 超上限：停止并清理计数
            cleanup(path)
            guardian.stopChild(failure.actor)
            return
        }

        // 安排延迟重启
        if (nextDelay.toMilliseconds() <= 0) {
            guardian.restartChild(failure.actor, failure.cause)
        } else {
            Timer.once(nextDelay, { =>
                guardian.restartChild(failure.actor, failure.cause)
            })
        }
    }

    /** 成功后清理重试状态 */
    public func cleanup(path: String): Unit {
        mutex.lock(); retryCounts.remove(path); mutex.unlock()
    }
}

