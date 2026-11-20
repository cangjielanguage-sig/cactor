/*
 * CActor - 调度模式 A1 / 阻塞可观测 A1b
 *
 * 验证三件事：
 * 1. ThreadPerActor 的阻塞隔离：默认亲和池的工作线程全部被阻塞的 Actor 占住时，
 *    落在 ThreadPerActor 上的 Actor 仍能及时处理消息（同样的 Actor 落在默认池上会被饿住）；
 * 2. 空闲线程回收：大量短命 Actor 各自用掉一根专属线程，空闲后线程数回落到基线；
 * 3. 运行时长直方图按调度模式区分（A1b），并出现在 Prometheus 文本里。
 */
package cactor_tests.unit.runtime.scheduling

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.typed.*
import cactor_core.message.{Message, MessageResult}
import cactor_runtime.*
import cactor_testkit.*

class ModePing <: Message {
    public func messageType(): String {
        "ModePing"
    }
}

/** 收到消息就计数；用于观测"何时被调度到" */
class CountingActor <: Actor {
    let handled: AtomicInt64

    init(handled: AtomicInt64) {
        this.handled = handled
    }

    public prop name: String {
        get() { "counting" }
    }

    public prop description: String {
        get() { "counting actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case _: ModePing => handled.fetchAdd(1)
            case _ => ()
        }
        MessageResult.Handled
    }
}

/** 收到消息后占住自己的调度线程，直到 release（最多 30 s） */
class HoldActor <: Actor {
    let started: AtomicInt64
    let release: AtomicBool

    init(started: AtomicInt64, release: AtomicBool) {
        this.started = started
        this.release = release
    }

    public prop name: String {
        get() { "hold" }
    }

    public prop description: String {
        get() { "hold actor" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        started.fetchAdd(1)
        let _ = awaitCondition(Duration.second * 30, { => release.load() })
        MessageResult.Handled
    }
}

@Test
public class ThreadPerActorTest {
    @TestCase
    public func dispatcherModeMapsToDispatcherName(): Unit {
        let p = PropsFactory.create<Actor>({ => CountingActor(AtomicInt64(0)) })
            .withDispatcherMode(DispatcherMode.Affinity)
        @Expect(p.getDispatcher(), "default")
        let tp = PropsFactory.create<Actor>({ => CountingActor(AtomicInt64(0)) })
            .withDispatcherMode(DispatcherMode.ThreadPerActor)
        @Expect(tp.getDispatcher(), "thread-per-actor")
        let bp = PropsFactory.create<Actor>({ => CountingActor(AtomicInt64(0)) })
            .withDispatcherMode(DispatcherMode.Blocking)
        @Expect(bp.getDispatcher(), "blocking")
        @Expect(TypedProps().withDispatcherMode(DispatcherMode.Blocking).dispatcher, "blocking")
    }

    @TestCase
    public func threadPerActorIsolatesBlocking(): Unit {
        let kit = ActorTestKit("thread-per-actor-isolation")
        let starter = AtomicInt64(0)
        let starterRef = kit.actorOf(PropsFactory.create<Actor>({ => CountingActor(starter) }), "starter")
        starterRef << ModePing()
        @Expect(awaitCondition(Duration.second * 5, { => starter.load() == 1 }), true)
        let workers = DispatcherDiagnostics.affinityWorkerCount()
        @Expect(workers > 0, true)

        // 用阻塞 Actor 占满默认调度器的全部工作线程（各占一根，3 s 后释放）
        let started = AtomicInt64(0)
        let release = AtomicBool(false)
        for (i in 0..workers) {
            let ref = kit.actorOf(PropsFactory.create<Actor>({ => HoldActor(started, release) }), "hold${i}")
            ref << ModePing()
        }
        @Expect(awaitCondition(Duration.second * 10, { => started.load() == workers }), true)

        // 默认池：所有 worker 都被占住，新 Actor 在 400 ms 内得不到执行
        let slow = AtomicInt64(0)
        let slowRef = kit.actorOf(PropsFactory.create<Actor>({ => CountingActor(slow) }), "slow")
        slowRef << ModePing()
        @Expect(awaitCondition(Duration.millisecond * 400, { => slow.load() == 1 }), false)

        // ThreadPerActor：自己的专属线程，不受默认池阻塞影响
        let fast = AtomicInt64(0)
        let fastRef = kit.actorOf(
            PropsFactory.create<Actor>({ => CountingActor(fast) })
                .withDispatcherMode(DispatcherMode.ThreadPerActor), "fast")
        fastRef << ModePing()
        @Expect(awaitCondition(Duration.second * 2, { => fast.load() == 1 }), true)

        release.store(true)
        kit.shutdown()
    }

    @TestCase
    public func threadPerActorReapsIdleThreads(): Unit {
        let kit = ActorTestKit("thread-per-actor-reap")
        // 缩短空闲回收阈值，避免用例等待默认的 10 s
        DispatcherDiagnostics.setThreadPerActorIdleMillis(100)
        try {
            let n: Int64 = 1000
            let handled = AtomicInt64(0)
            let createdBefore = DispatcherDiagnostics.threadPerActorCreated()
            for (i in 0..n) {
                let ref = kit.actorOf(
                    PropsFactory.create<Actor>({ => CountingActor(handled) })
                        .withDispatcherMode(DispatcherMode.ThreadPerActor), "reap${i}")
                ref << ModePing()
            }
            @Expect(awaitCondition(Duration.second * 30, { => handled.load() == n }), true)
            // 每个 Actor 至少创建过一根专属线程
            @Expect(DispatcherDiagnostics.threadPerActorCreated() - createdBefore >= n, true)
            // 空闲 100 ms 后线程退出：线程数回落到 0，且确有回收发生
            let reapedBefore = DispatcherDiagnostics.threadPerActorReaped()
            @Expect(awaitCondition(Duration.second * 30, { => DispatcherDiagnostics.threadPerActorThreads() == 0 }), true)
            @Expect(DispatcherDiagnostics.threadPerActorReaped() > reapedBefore, true)
        } finally {
            DispatcherDiagnostics.setThreadPerActorIdleMillis(10000)
            kit.shutdown()
        }
    }

    @TestCase
    public func runDurationHistogramSplitsByMode(): Unit {
        let kit = ActorTestKit("run-duration-modes")
        let defaultCount = AtomicInt64(0)
        let blockingCount = AtomicInt64(0)
        let tpaCount = AtomicInt64(0)
        let a = kit.actorOf(PropsFactory.create<Actor>({ => CountingActor(defaultCount) }), "hist-default")
        let b = kit.actorOf(PropsFactory.create<Actor>({ => CountingActor(blockingCount) })
            .withDispatcherMode(DispatcherMode.Blocking), "hist-blocking")
        let c = kit.actorOf(PropsFactory.create<Actor>({ => CountingActor(tpaCount) })
            .withDispatcherMode(DispatcherMode.ThreadPerActor), "hist-tpa")
        a << ModePing()
        b << ModePing()
        c << ModePing()
        @Expect(awaitCondition(Duration.second * 10, { =>
            defaultCount.load() == 1 && blockingCount.load() == 1 && tpaCount.load() == 1
        }), true)
        @Expect(DispatcherDiagnostics.runCount("default") > 0, true)
        @Expect(DispatcherDiagnostics.runCount("blocking") > 0, true)
        @Expect(DispatcherDiagnostics.runCount("thread-per-actor") > 0, true)
        let text = DispatcherDiagnostics.runDurationPrometheus()
        @Expect(text.contains("cactor_actor_run_duration_seconds_bucket"), true)
        @Expect(text.contains("mode=\"thread-per-actor\""), true)
        @Expect(text.contains("mode=\"blocking\""), true)
        @Expect(text.contains("le=\"+Inf\""), true)
        kit.shutdown()
    }
}
