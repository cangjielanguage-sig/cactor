/*
 * CActor - A1b 端到端：经管理端点 /metrics 抓取按调度模式区分的运行时长直方图
 *
 * thread_per_actor_test 已经单测过 DispatcherDiagnostics.runDurationPrometheus()；
 * 这里补的是**端到端**那一半：真实 TcpServerSocket 上 GET /metrics，断言
 * 三种调度模式（default / blocking / thread-per-actor）的直方图序列与
 * thread-per-actor 专用线程 gauge 都出现在响应体里。
 */
package cactor_tests.unit.observability

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory, ManagementServer}
import cactor_core.actor.{Actor, ActorContext, PropsFactory, DispatcherMode}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_testkit.*

/** 收到消息就计数；用于让三种调度模式各产生至少一次运行（直方图在批边界记录） */
class MetricsProbeActor <: Actor {
    private let handled: AtomicInt64

    init(handled: AtomicInt64) {
        this.handled = handled
    }

    public prop name: String {
        get() { "metrics-probe" }
    }

    public prop description: String {
        get() { "metrics probe" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: StringMessage => handled.fetchAdd(1)
            case _ => ()
        }
        MessageResult.Handled
    }
}

@Test
public class MetricsE2ETest {
    @TestCase
    public func metricsExposeRunDurationAndThreadPerActorOverHttp(): Unit {
        let system = CActorSystemFactory.create("metrics-e2e-${MonoTime.now().hashCode()}")
        let defaultCount = AtomicInt64(0)
        let blockingCount = AtomicInt64(0)
        let tpaCount = AtomicInt64(0)
        let mgmt = ManagementServer(system, 0).start()
        try {
            let d = system.actorOf(PropsFactory.create<Actor>({ => MetricsProbeActor(defaultCount) }), "e2e-default")
            let b = system.actorOf(PropsFactory.create<Actor>({ => MetricsProbeActor(blockingCount) })
                .withDispatcherMode(DispatcherMode.Blocking), "e2e-blocking")
            let t = system.actorOf(PropsFactory.create<Actor>({ => MetricsProbeActor(tpaCount) })
                .withDispatcherMode(DispatcherMode.ThreadPerActor), "e2e-tpa")
            for (_ in 0..3) {
                d.tell(StringMessage("x"))
                b.tell(StringMessage("x"))
                t.tell(StringMessage("x"))
            }
            @Expect(awaitCondition(Duration.second * 10, { =>
                defaultCount.load() == 3 && blockingCount.load() == 3 && tpaCount.load() == 3
            }), true)

            // 直方图在 Cell 批边界记录；轮询直到三种 mode 都出现（否则读到的可能是上一批之前的快照）
            let holder = Array<String>(1, repeat: "")
            let seen = awaitCondition(Duration.second * 10, { =>
                let (status, metrics) = httpRequest(mgmt.port(), "GET", "/metrics")
                holder[0] = metrics
                status == 200 && metrics.contains("mode=\"default\"") && metrics.contains("mode=\"blocking\"") &&
                    metrics.contains("mode=\"thread-per-actor\"")
            })
            @Expect(seen, true)
            let body = holder[0]
            @Expect(body.contains("cactor_actor_run_duration_seconds_count{mode=\"default\"}"), true)
            @Expect(body.contains("cactor_actor_run_duration_seconds_count{mode=\"blocking\"}"), true)
            @Expect(body.contains("cactor_actor_run_duration_seconds_count{mode=\"thread-per-actor\"}"), true)
            @Expect(body.contains("cactor_thread_per_actor_threads"), true)
        } finally {
            mgmt.stop()
            system.terminate()
        }
    }
}
