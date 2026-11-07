/*
 * CActor - 调度诊断与 Block 背压（E39）
 *
 *   - 饥饿检测：默认调度器上的 Actor 在处理方法里阻塞超过阈值（默认 1000 ms），运行时告警一次并点名该 Actor
 *     （此前没有任何提示，亲和到同一线程的其他 Actor 静默停摆）
 *   - Block 背压在 Actor 内发送时不等待：直接按超时计死信，不占住工作线程
 *     （此前在发送方线程上轮询 sleep，最长占住整个 Block 超时，接收方若在同一线程上就互相等待）
 *   - Actor 之外的生产者照常等待空位，接收方出队后被唤醒
 */
package cactor_tests.unit.runtime.scheduling

import std.sync.*
import std.time.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.*
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_runtime.{DispatcherDiagnostics}
import cactor_testkit.*

/** 第一条消息阻塞到 gate 打开，记录收到的每条消息 */
class GatedSink <: Actor {
    let rec: EventRecorder
    let gate: AtomicBool

    init(rec: EventRecorder, gate: AtomicBool) {
        this.rec = rec
        this.gate = gate
    }

    public prop name: String {
        get() { "gated-sink" }
    }

    public prop description: String {
        get() { "gated-sink" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            rec.record("b:${s.getContent()}")
            if (s.getContent() == "m1") {
                let _ = awaitCondition(Duration.second * 10, { => gate.load() })
            }
        }
        MessageResult.Handled
    }
}

/** 收到 "go" 时向 target 发送 m3，记录发送花了多久 */
class Producer <: Actor {
    let rec: EventRecorder
    let target: ActorRef

    init(rec: EventRecorder, target: ActorRef) {
        this.rec = rec
        this.target = target
    }

    public prop name: String {
        get() { "producer" }
    }

    public prop description: String {
        get() { "producer" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let start = MonoTime.now()
        target << StringMessage("m3")
        rec.record("sent-ms:${(MonoTime.now() - start).toMilliseconds()}")
        MessageResult.Handled
    }
}

func sentMs(rec: EventRecorder): Int64 {
    for (e in rec.snapshot()) {
        if (e.startsWith("sent-ms:")) {
            return Int64.parse(e["sent-ms:".size..])
        }
    }
    -1
}

/** 处理方法里一直阻塞到 release */
class Hog <: Actor {
    let release: AtomicBool

    init(release: AtomicBool) {
        this.release = release
    }

    public prop name: String {
        get() { "hog" }
    }

    public prop description: String {
        get() { "hog" }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let _ = awaitCondition(Duration.second * 20, { => release.load() })
        MessageResult.Handled
    }
}

func gatedProps(rec: EventRecorder, gate: AtomicBool, timeout: Duration): Props<Actor> {
    PropsFactory.create<Actor>({ => GatedSink(rec, gate) }).withBoundedMailbox(1, MailboxOverflow.Block(timeout))
}

@Test
public class StarvationAndBlockTest {
    @TestCase
    public func starvationDetectorNamesTheBlockingActor(): Unit {
        let kit = ActorTestKit("starvation")
        let release = AtomicBool(false)
        let before = DispatcherDiagnostics.starvationWarnings()
        let hog = kit.actorOf({ => Hog(release) }, "hog-in-default-dispatcher")
        hog << StringMessage("block")
        // 默认阈值 1000 ms，检测线程每 250 ms 看一次
        @Expect(awaitCondition(Duration.second * 5, { => DispatcherDiagnostics.starvationWarnings() > before }), true)
        let named = DispatcherDiagnostics.lastStarvationPath() ?? ""
        @Expect(named.contains("hog-in-default-dispatcher"), true)
        release.store(true)
        kit.shutdown()
    }

    @TestCase
    public func blockOverflowInsideActorDoesNotStallWorker(): Unit {
        let kit = ActorTestKit("block-in-actor")
        let rec = EventRecorder()
        let gate = AtomicBool(false)
        let sink = kit.actorOf(gatedProps(rec, gate, Duration.second * 2), "sink")
        sink << StringMessage("m1")
        @Expect(rec.await("b:m1", Duration.second * 5), true)
        // 接收方卡在 m1 上，m2 占满容量 1
        sink << StringMessage("m2")
        let producer = kit.actorOf({ => Producer(rec, sink) }, "producer")
        producer << StringMessage("go")
        @Expect(awaitCondition(Duration.second * 5, { => sentMs(rec) >= 0 }), true)
        // 在 Actor 内发送：不等待 2 s 的 Block 超时（此前占住工作线程整整 2 s）
        @Expect(sentMs(rec) < 1000, true)
        gate.store(true)
        @Expect(rec.await("b:m2", Duration.second * 5), true)
        // m3 按超时处理（计死信），没有进邮箱
        @Expect(rec.contains("b:m3"), false)
        kit.shutdown()
    }

    @TestCase
    public func blockOverflowFromOutsideWaitsUntilDequeue(): Unit {
        let kit = ActorTestKit("block-outside")
        let rec = EventRecorder()
        let gate = AtomicBool(false)
        let sink = kit.actorOf(gatedProps(rec, gate, Duration.second * 5), "sink")
        sink << StringMessage("m1")
        @Expect(rec.await("b:m1", Duration.second * 5), true)
        sink << StringMessage("m2")
        let returned = AtomicBool(false)
        let sender = spawn {
            sink << StringMessage("m3")
            returned.store(true)
        }
        // 外部生产者在等空位
        @Expect(awaitCondition(Duration.millisecond * 200, { => returned.load() }), false)
        gate.store(true)
        // 接收方出队后被唤醒、m3 入队并被处理
        @Expect(awaitCondition(Duration.second * 3, { => returned.load() }), true)
        @Expect(rec.await("b:m3", Duration.second * 5), true)
        sender.get()
        kit.shutdown()
    }
}
