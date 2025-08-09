/*
 * CActor Runtime - Backoff Supervisor (minimal runtime scheduler)
 */
package cactor.runtime.supervision

import std.sync.*
import std.time.*
import std.collection.*
import cactor.core.actor.{ActorRef}
import cactor.core.supervision.{ActorFailure, BackoffSupervisorStrategy}
import cactor.core.guardian.{GuardianActor}

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
    private let mutex: Mutex

    public init(strategy: BackoffSupervisorStrategy, guardian: GuardianActor) {
        this.strategy = strategy
        this.guardian = guardian
        this.retryCounts = HashMap<String, Int64>()
        this.mutex = Mutex()
    }

    public func handleFailure(failure: ActorFailure): Unit {
        let path = failure.actor.path.toString()
        mutex.lock()
        let count = match (retryCounts.get(path)) { case Some(v) => v; case None => 0 }
        let delay = strategy.calculateBackoffTime(count)
        retryCounts.add(path, count + 1)
        mutex.unlock()

        // 安排延迟重启
        if (delay.toMilliseconds() <= 0) {
            guardian.restartChild(failure.actor, failure.cause)
        } else {
            Timer.once(delay, { =>
                guardian.restartChild(failure.actor, failure.cause)
            })
        }
    }
}

