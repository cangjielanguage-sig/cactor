/*
 * CActor - 持久化失败语义（E8′ 的 P3 部分；E31 先行用例转正）
 *
 *   - 事件已落库、处理器抛异常：抛 EventHandlerException（PersistenceException 的子类），序号不回退，
 *     后续写入接在已落库的事件之后（此前回退序号，下一次 persist 与存储版本冲突，这个 Actor 永远写不进去）
 *   - 存储写入失败（版本冲突）：抛 PersistenceException，序号回退，不影响再次写入
 *   - persistAll 消费存储返回的 Future：版本冲突要报错（此前未 get()，冲突被静默吞掉、序号照样前进）
 *   - 端到端（P4）：真实 ActorSystem 中处理器失败 → 默认监督重启 → 新实例在 preStart 从存储恢复，
 *     已落库的事件被重放、后续写入照常（此前没有恢复钩子，重启后序号为 0，首次 persist 即冲突）
 */
package cactor_tests.unit.persistence

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_persistence.{AbstractPersistentActor, EnhancedEventStore, SnapshotStore, MemoryEnhancedEventStore,
    MemorySnapshotStore, SimpleEventPayload, PersistenceException, EventHandlerException, PersistentEvent}
import cactor_testkit.*

class P3Actor <: AbstractPersistentActor {
    init(es: EnhancedEventStore, ss: SnapshotStore) {
        super(es, ss)
    }

    public override func persistenceId(): String {
        "p3-actor"
    }
}

func ignoreCount(_: Int64): Unit {}

/** 计数器：inc 持久化一个事件；boom 持久化后处理器抛异常；get 回复 "count=N seq=M" */
class RestartingCounter <: AbstractPersistentActor {
    var count: Int64 = 0
    let starts: AtomicInt64

    init(es: EnhancedEventStore, ss: SnapshotStore, starts: AtomicInt64) {
        super(es, ss)
        this.starts = starts
    }

    public override func persistenceId(): String {
        "e2e-counter"
    }

    public override func preStart(): Unit {
        starts.fetchAdd(1) |> ignoreCount
        super.preStart()
    }

    protected override func onRecoveryEvent(event: PersistentEvent): Unit {
        count++
    }

    public override func receiveCommand(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage where s.getContent() == "inc" =>
                persist(SimpleEventPayload("inc", "1"), { _ => count++ })
            case s: StringMessage where s.getContent() == "boom" =>
                persist(SimpleEventPayload("inc", "1"), { _ => throw Exception("处理器失败") })
            case s: StringMessage where s.getContent() == "get" =>
                context.reply(StringMessage("count=${count} seq=${getLastSequenceNr()}"))
            case _ => return MessageResult.Unhandled
        }
        MessageResult.Handled
    }
}

@Test
public class PersistFailureSemanticsTest {
    @TestCase
    public func handlerFailureDoesNotRollBackPersistedSequence(): Unit {
        let store = MemoryEnhancedEventStore()
        let actor = P3Actor(store, MemorySnapshotStore())
        var handlerFailure: Option<EventHandlerException> = None
        try {
            actor.persist(SimpleEventPayload("e", "1"), { _ => throw Exception("处理器失败") })
        } catch (e: EventHandlerException) {
            handlerFailure = Some(e)
        }
        @Expect(handlerFailure.isSome(), true)
        @Expect(handlerFailure.getOrThrow().persistedSequenceNr, 1)
        @Expect(actor.getLastSequenceNr(), 1)
        // 第一条事件已经落库：后续写入接在它后面
        var secondOk = true
        try {
            actor.persist(SimpleEventPayload("e", "2"), { _ => () })
        } catch (e: PersistenceException) {
            secondOk = false
        }
        @Expect(secondOk, true)
        @Expect(actor.getLastSequenceNr(), 2)
        @Expect(store.getHighestSequenceNr("p3-actor").get(), 2)
    }

    @TestCase
    public func storeConflictRollsBackAndIsNotAHandlerFailure(): Unit {
        let store = MemoryEnhancedEventStore()
        let first = P3Actor(store, MemorySnapshotStore())
        first.persist(SimpleEventPayload("e", "a"), { _ => () })
        // 同一 persistenceId 的第二个实例没有恢复，按版本 0 写入：冲突
        let second = P3Actor(store, MemorySnapshotStore())
        var storeFailure = false
        let handlerRan = AtomicBool(false)
        try {
            second.persist(SimpleEventPayload("e", "b"), { _ => handlerRan.store(true) })
        } catch (e: EventHandlerException) {
            storeFailure = false
        } catch (e: PersistenceException) {
            storeFailure = true
        }
        @Expect(storeFailure, true)
        @Expect(handlerRan.load(), false)
        @Expect(second.getLastSequenceNr(), 0)
        @Expect(store.getHighestSequenceNr("p3-actor").get(), 1)
    }

    @TestCase
    public func persistAllReportsStoreConflict(): Unit {
        let store = MemoryEnhancedEventStore()
        let first = P3Actor(store, MemorySnapshotStore())
        first.persistAll([SimpleEventPayload("e", "a1"), SimpleEventPayload("e", "a2")], { _ => () })
        @Expect(first.getLastSequenceNr(), 2)
        let second = P3Actor(store, MemorySnapshotStore())
        let handled = AtomicInt64(0)
        var failed = false
        try {
            second.persistAll([SimpleEventPayload("e", "b1"), SimpleEventPayload("e", "b2")], { _ => handled.fetchAdd(1) |> ignoreCount })
        } catch (e: PersistenceException) {
            failed = true
        }
        @Expect(failed, true)
        @Expect(handled.load(), 0)
        @Expect(second.getLastSequenceNr(), 0)
        @Expect(store.getHighestSequenceNr("p3-actor").get(), 2)
        // 冲突后第一个实例照常写入
        first.persistAll([SimpleEventPayload("e", "a3")], { _ => () })
        @Expect(first.getLastSequenceNr(), 3)
    }

    @TestCase
    public func handlerFailureRestartsAndReplaysPersistedEvents(): Unit {
        let kit = ActorTestKit("p3-e2e")
        let es = MemoryEnhancedEventStore()
        let ss = MemorySnapshotStore()
        let starts = AtomicInt64(0)
        let ref = kit.actorOf({ => RestartingCounter(es, ss, starts) }, "counter")
        let probe = kit.createTestProbe()
        probe.send(ref, StringMessage("inc"))
        probe.send(ref, StringMessage("inc"))
        // 第 3 个事件落库后处理器失败：默认监督重启，新实例恢复出 3 个事件
        probe.send(ref, StringMessage("boom"))
        probe.send(ref, StringMessage("inc"))
        probe.send(ref, StringMessage("get"))
        let reply = probe.expectMsgType<StringMessage>(Duration.second * 5)
        @Expect(reply.getContent(), "count=4 seq=4")
        @Expect(starts.load(), 2)
        @Expect(es.getHighestSequenceNr("e2e-counter").get(), 4)
        kit.shutdown()
    }
}
