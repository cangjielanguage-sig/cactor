/*
 * CActor - CoordinatedShutdown 语义（E34，对照 Akka CoordinatedShutdown）
 *
 *   - 阶段按 Akka 的顺序执行，任务失败不影响其他任务和后续阶段
 *   - 用户 Actor 在 actor-system-terminate 阶段才停止：之前各阶段的任务仍能与 Actor 交互（R17）
 *   - run 只执行一次（重复、并发调用都只执行一次任务）
 *   - 阶段超时：卡住的任务不阻塞后续阶段
 *   - 实例互不共享任务（旧实现按系统名放在静态表中）
 */
package cactor_tests.unit.runtime.lifecycle

import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_runtime.lifecycle.{CoordinatedShutdown}
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult}
import cactor_testkit.*

class SeqHolder {
    private let lock = Mutex()
    private var seq: String = ""

    public func append(s: String): Unit {
        synchronized(lock) { seq = seq + s }
    }

    public func value(): String {
        synchronized(lock) { seq }
    }
}

class CsPing <: Message {
    let replyTo: ActorRef

    init(replyTo: ActorRef) {
        this.replyTo = replyTo
    }

    public func messageType(): String {
        "CsPing"
    }
}

class CsPong <: Message {
    public func messageType(): String {
        "CsPong"
    }
}

class Ponger <: Actor {
    let stopped: AtomicBool

    init(stopped: AtomicBool) {
        this.stopped = stopped
    }

    public prop name: String {
        get() { "ponger" }
    }

    public prop description: String {
        get() { "ponger" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(p) <- (message as CsPing)) {
            p.replyTo.tell(CsPong())
        }
        MessageResult.Handled
    }

    public func postStop(): Unit {
        stopped.store(true)
    }
}

@Test
public class CoordinatedShutdownPhaseTestSuite {
    @TestCase
    public func phaseOrderAndFailureIsolation(): Unit {
        let cs = CoordinatedShutdown("cs-order")
        let holder = SeqHolder()
        // 倒序注册，验证执行顺序由阶段决定而不是注册顺序
        cs.registerTask(CoordinatedShutdown.PhaseSystemTerminate, { => holder.append("F") })
        cs.registerTask(CoordinatedShutdown.PhaseActorSystemTerminate, { => holder.append("E") })
        cs.registerTask(CoordinatedShutdown.PhaseServiceStop, { => throw Exception("boom") })
        cs.registerTask(CoordinatedShutdown.PhaseServiceStop, { => holder.append("C") })
        cs.registerTask(CoordinatedShutdown.PhaseClusterLeave, { => holder.append("D") })
        cs.registerTask(CoordinatedShutdown.PhaseServiceUnbind, { => holder.append("B") })
        cs.registerTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, { => holder.append("A") })
        cs.run()
        @Expect(holder.value(), "ABCDEF")
        let names = cs.phaseNames()
        @Expect(names[names.size - 2], CoordinatedShutdown.PhaseActorSystemTerminate)
        @Expect(names[names.size - 1], CoordinatedShutdown.PhaseSystemTerminate)
    }

    @TestCase
    public func actorsAreAliveUntilActorSystemTerminatePhase(): Unit {
        let kit = ActorTestKit("cs-alive")
        let stopped = AtomicBool(false)
        let ponger = kit.actorOf({ => Ponger(stopped) }, "ponger")
        let probe = kit.createTestProbe()
        let pr = probe.ref
        let rec = EventRecorder()
        let cs = kit.system.getRuntime().getCoordinatedShutdown()
        // 早期阶段：Actor 必须仍能应答（此前所有 Actor 在任何阶段执行之前就已停止）
        cs.registerTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, { =>
            ponger.tell(CsPing(pr))
            let alive = try {
                probe.expectMsg(Duration.second * 3) is CsPong
            } catch (e: Exception) {
                false
            }
            rec.record("before-service-unbind:alive=${alive}")
        })
        cs.registerTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, { =>
            rec.record("before-actor-system-terminate:stopped=${stopped.load()}")
        })
        cs.registerTask(CoordinatedShutdown.PhaseSystemTerminate, { =>
            rec.record("system-terminate:stopped=${stopped.load()}")
        })
        kit.shutdown()
        @Expect(rec.snapshot(), ["before-service-unbind:alive=true", "before-actor-system-terminate:stopped=false",
            "system-terminate:stopped=true"])
        @Expect(kit.system.isTerminated(), true)
    }

    @TestCase
    public func runExecutesTasksOnlyOnce(): Unit {
        let cs = CoordinatedShutdown("cs-once")
        let n = AtomicInt64(0)
        cs.registerTask(CoordinatedShutdown.PhaseServiceStop, { => n.fetchAdd(1) |> ignoreInt })
        let futures = ArrayList<Future<Unit>>()
        for (_ in 0..4) {
            futures.add(spawn { cs.run() })
        }
        for (f in futures) {
            f.get()
        }
        cs.run()
        @Expect(n.load(), 1)
        @Expect(cs.isRunning(), true)
        // 关闭开始后注册的任务不再执行
        cs.registerTask(CoordinatedShutdown.PhaseSystemTerminate, { => n.fetchAdd(100) |> ignoreInt })
        cs.run()
        @Expect(n.load(), 1)
    }

    @TestCase
    public func phaseTimeoutDoesNotBlockLaterPhases(): Unit {
        let cs = CoordinatedShutdown("cs-timeout")
        let holder = SeqHolder()
        let release = AtomicBool(false)
        cs.addPhase("slow", Duration.millisecond * 100)
        cs.addPhase("after-slow")
        cs.registerTask("slow", { =>
            // 卡住的任务（最多 10 s）
            let _ = awaitCondition(Duration.second * 10, { => release.load() })
        })
        cs.registerTask("after-slow", { => holder.append("after") })
        let start = MonoTime.now()
        cs.run()
        let elapsed = (MonoTime.now() - start).toMilliseconds()
        release.store(true)
        @Expect(holder.value(), "after")
        @Expect(elapsed < 3000, true)
    }

    @TestCase
    public func instancesDoNotShareTasks(): Unit {
        let a = CoordinatedShutdown("same-name")
        let b = CoordinatedShutdown("same-name")
        let holder = SeqHolder()
        a.registerTask(CoordinatedShutdown.PhaseServiceStop, { => holder.append("a") })
        b.run()
        @Expect(holder.value(), "")
        a.run()
        @Expect(holder.value(), "a")
    }
}

func ignoreInt(_: Int64): Unit {}
