/*
 * CActor - 类型化持久化（E44：EventSourcedBehavior / DurableStateBehavior，对标 Akka Typed persistence）
 *
 *   - 计数器写文件日志，整个 ActorSystem 关闭后新系统恢复出相同状态；再重启一次仍相同（重放幂等），之后的写入序号接续
 *   - 写入期间到达的命令暂存，按到达顺序逐条处理：每条命令看到的都是之前所有事件已落盘后的状态
 *   - thenReply 在事件落盘、应用之后才执行：收到回复时日志里已经有这条事件
 *   - 用户 stash() / thenUnstashAll()：锁定期间的命令暂存，解锁后按原顺序处理
 *   - RetentionCriteria.snapshotEvery(10, 2).withDeleteEventsOnSnapshot()：只保留最近两份快照、删除更早的事件，
 *     恢复从快照开始
 *   - EventAdapter 的 schema 演进：旧格式拆成多个事件、废弃的事件跳过；写入序号按日志序号接续（不按适配后的事件数）
 *   - 日志数据损坏或事件处理器重放时抛异常：恢复失败、Actor 停止（不带着错误状态运行）；事件被删除却没有覆盖它们的快照（序号缺口）同样恢复失败
 *   - 快照按序号取最新：迟到落盘的旧快照不会被当成最新（否则恢复会跳过已被删除的一段事件）
 *   - 另一个写者插入事件：本 Actor 写入冲突而失败，监督重启后从日志恢复（含别人的事件），之后正常写入
 *   - 重启前在途的 IO（快照）完成后，完成信号送到新实例：按实例 token 识别并丢弃
 *   - 暂存缓冲区满：DropCommand 丢弃并作为未处理消息上报；FailActor 让 Actor 失败
 *   - DurableStateBehavior：持久化、删除、重启恢复；删除后修订号不回退（P14），之后的写入接在墓碑之后
 *   - P14 存储层：删除留下墓碑，持有旧修订号的写者被拒绝（此前删除后修订号回到 0，旧写者能把已删除的状态写回来）
 */
package cactor_tests.integration.persistence

import std.fs.*
import std.sync.*
import std.time.*
import std.convert.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.*
import cactor_persistence.{EventStore, MemoryEventStore, FileEventStore, FileSnapshotStore, MemorySnapshotStore,
    SnapshotStore, Snapshot, BasicSnapshot, BasicEvent, Event, EventEnvelope, DurableStateStore, MemoryDurableStateStore,
    FileDurableStateStore, DurableStateConflictException}
import cactor_persistence.typed.*
import cactor_testkit.*

let TP_T: Duration = Duration.second * 5

func tpDir(tag: String): String {
    let d = "/tmp/cactor-typed-persistence/${tag}-${MonoTime.now().hashCode()}"
    if (exists(d)) {
        remove(d, recursive: true)
    }
    d
}

// ---------------- 计数器协议 ----------------

enum TpCmd {
    | Add(Int64)
    | AddAndGet(Int64, TypedActorRef<Int64>)
    | Get(TypedActorRef<Int64>)
    | Lock
    | Unlock
}

enum TpEvt {
    | Added(Int64)
    | Locked
    | Unlocked
}

struct TpState {
    let total: Int64
    let locked: Bool

    init(total: Int64, locked: Bool) {
        this.total = total
        this.locked = locked
    }
}

/** 日志格式：Added → "Added"/"n"；旧版本 "Added.v0"/"a;b;c" 一条记录装多个增量；"Obsolete" 已废弃 */
class TpAdapter <: EventAdapter<TpEvt> {
    public func manifest(event: TpEvt): String {
        match (event) {
            case TpEvt.Added(_) => "Added"
            case TpEvt.Locked => "Locked"
            case TpEvt.Unlocked => "Unlocked"
        }
    }

    public func toJournal(event: TpEvt): String {
        match (event) {
            case TpEvt.Added(n) => "${n}"
            case _ => ""
        }
    }

    public func fromJournal(manifest: String, data: String): Array<TpEvt> {
        match (manifest) {
            case "Added" => [TpEvt.Added(Int64.parse(data))]
            case "Added.v0" => data.split(";").map({ p => TpEvt.Added(Int64.parse(p)) }).toArray()
            case "Obsolete" => []
            case "Locked" => [TpEvt.Locked]
            case "Unlocked" => [TpEvt.Unlocked]
            case other => throw IllegalArgumentException("未知事件类型：${other}")
        }
    }
}

let tpCodec = FunctionStateCodec<TpState>(encode: { s => "${s.total}|${s.locked}" }, decode: { d =>
    let parts = d.split("|")
    TpState(Int64.parse(parts[0]), parts[1] == "true")
})

func tpCounter(id: String, journal: EventStore, rec: EventRecorder): EventSourcedBehavior<TpCmd, TpEvt, TpState> {
    EventSourcedBehavior<TpCmd, TpEvt, TpState>(PersistenceId.of("Counter", id), emptyState: TpState(0, false),
        commandHandler: { s, cmd, effect =>
            match (cmd) {
                case TpCmd.Add(_) where s.locked => effect.stash()
                case TpCmd.Add(n) => effect.persist(TpEvt.Added(n)).thenRun({ ns => rec.record("total:${ns.total}") })
                case TpCmd.AddAndGet(n, replyTo) => effect.persist(TpEvt.Added(n)).thenReply(replyTo, { ns => ns.total })
                case TpCmd.Get(replyTo) => effect.reply(replyTo, s.total)
                case TpCmd.Lock => effect.persist(TpEvt.Locked)
                case TpCmd.Unlock => effect.persist(TpEvt.Unlocked).thenUnstashAll()
            }
        },
        eventHandler: { s, e =>
            match (e) {
                case TpEvt.Added(n) => TpState(s.total + n, s.locked)
                case TpEvt.Locked => TpState(s.total, true)
                case TpEvt.Unlocked => TpState(s.total, false)
            }
        }, eventAdapter: TpAdapter(), journal: journal).receiveSignal({ s, signal =>
        match (signal) {
            case r: RecoveryCompleted => rec.record("recovered:${r.highestSequenceNr}:${s.total}")
            case _: RecoveryFailed => rec.record("recovery-failed")
            case c: SnapshotCompleted => rec.record("snapshot:${c.sequenceNr}")
            case f: SnapshotFailed => rec.record("snapshot-failed:${f.sequenceNr}:${f.cause.message}")
            case d: DeleteSnapshotsCompleted => rec.record("snapshots-deleted:${d.toSequenceNr}")
            case d: DeleteEventsCompleted => rec.record("events-deleted:${d.toSequenceNr}")
            case d: DeleteFailed => rec.record("delete-failed:${d.toSequenceNr}:${d.cause.message}")
            case _ => ()
        }
    })
}

func tpGet(ref: TypedActorRef<TpCmd>): Int64 {
    TypedAsk.ask<TpCmd, Int64>(ref, { r => TpCmd.Get(r) }, TP_T).get(TP_T)
}

func tpAddAndGet(ref: TypedActorRef<TpCmd>, n: Int64): Int64 {
    TypedAsk.ask<TpCmd, Int64>(ref, { r => TpCmd.AddAndGet(n, r) }, TP_T).get(TP_T)
}

func tpPid(id: String): String {
    PersistenceId.of("Counter", id).id
}

/** 日志里该标识的全部事件数据（按序号） */
func tpJournalData(journal: EventStore, pid: String): Array<String> {
    journal.replay(pid, 1, Int64.Max).map({ env => env.event.getEventData() }).toArray()
}

/** replay / append 可以被闸门挡住的日志：用来确定性地制造"恢复中 / 写入中" */
class GatedEventStore <: EventStore {
    let inner = MemoryEventStore()
    let replayOpen = AtomicBool(true)
    let replayCalls = AtomicInt64(0)

    private func waitReplay(): Unit {
        replayCalls.fetchAdd(1)
        while (!replayOpen.load()) {
            sleep(Duration.millisecond)
        }
    }

    public func saveEvent(event: Event): Unit {
        inner.saveEvent(event)
    }

    public func getEvents(actorId: String): Array<Event> {
        inner.getEvents(actorId)
    }

    public func getEventsAfter(actorId: String, timestamp: Int64): Array<Event> {
        inner.getEventsAfter(actorId, timestamp)
    }

    public func deleteEvents(actorId: String): Unit {
        inner.deleteEvents(actorId)
    }

    public func close(): Unit {
        inner.close()
    }

    public func highestSequenceNr(persistenceId: String): Int64 {
        inner.highestSequenceNr(persistenceId)
    }

    public func append(persistenceId: String, events: Array<Event>, expectedSeq: Int64): Int64 {
        inner.append(persistenceId, events, expectedSeq)
    }

    public func replay(persistenceId: String, fromSeq: Int64, toSeq: Int64): Array<EventEnvelope> {
        waitReplay()
        inner.replay(persistenceId, fromSeq, toSeq)
    }
}

/** saveSnapshot 可以被闸门挡住的快照存储 */
class GatedSnapshotStore <: SnapshotStore {
    let inner = MemorySnapshotStore()
    let saveOpen = AtomicBool(true)
    let saveCalls = AtomicInt64(0)
    let saved = AtomicInt64(0)

    public func saveSnapshot(snapshot: Snapshot): Unit {
        saveCalls.fetchAdd(1)
        while (!saveOpen.load()) {
            sleep(Duration.millisecond)
        }
        inner.saveSnapshot(snapshot)
        saved.fetchAdd(1)
    }

    public func getLatestSnapshot(actorId: String): Option<Snapshot> {
        inner.getLatestSnapshot(actorId)
    }

    public func getSnapshot(actorId: String, sequenceNumber: Int64): Option<Snapshot> {
        inner.getSnapshot(actorId, sequenceNumber)
    }

    public func deleteSnapshots(actorId: String): Unit {
        inner.deleteSnapshots(actorId)
    }

    public func close(): Unit {
        inner.close()
    }

    public func deleteSnapshotsTo(actorId: String, maxSequenceNr: Int64): Unit {
        inner.deleteSnapshotsTo(actorId, maxSequenceNr)
    }
}

// ---------------- 持久状态协议 ----------------

enum DsCmd {
    | Put(String, TypedActorRef<String>)
    | Clear(TypedActorRef<String>)
    | Read(TypedActorRef<String>)
}

let dsCodec = FunctionStateCodec<String>(encode: { s => s }, decode: { d => d })

func dsBehavior(id: String, store: DurableStateStore, rec: EventRecorder): DurableStateBehavior<DsCmd, String> {
    DurableStateBehavior<DsCmd, String>(PersistenceId.of("Profile", id), emptyState: "<empty>",
        commandHandler: { s, cmd, effect =>
            match (cmd) {
                case DsCmd.Put(v, replyTo) => effect.persist(v).thenReply(replyTo, { ns => ns })
                case DsCmd.Clear(replyTo) => effect.delete().thenReply(replyTo, { ns => ns })
                case DsCmd.Read(replyTo) => effect.reply(replyTo, s)
            }
        }, codec: dsCodec, store: store).receiveSignal({ s, signal =>
        match (signal) {
            case r: RecoveryCompleted => rec.record("recovered:${r.highestSequenceNr}:${s}")
            case _ => ()
        }
    })
}

func dsAsk(ref: TypedActorRef<DsCmd>, f: (TypedActorRef<String>) -> DsCmd): String {
    TypedAsk.ask<DsCmd, String>(ref, f, TP_T).get(TP_T)
}

@Test
public class TypedPersistenceTest {
    @TestCase
    func counterRecoversAcrossSystemRestart(): Unit {
        let dir = tpDir("restart")
        let pid = tpPid("c1")
        let kit1 = ActorTestKit("tp-restart-1")
        let rec1 = EventRecorder()
        let journal1 = FileEventStore("${dir}/journal")
        let ref1 = kit1.system.typed().actorOf(tpCounter("c1", journal1, rec1).behavior(), "c1")
        for (i in 1..=10) {
            ref1.tell(TpCmd.Add(i))
        }
        @Expect(tpGet(ref1), 55)
        @Expect(rec1.contains("recovered:0:0"), true)
        kit1.shutdown()
        journal1.close()
        // 新系统、新的存储实例：只能从磁盘恢复
        for (round in 0..2) {
            let kit = ActorTestKit("tp-restart-${round + 2}")
            let rec = EventRecorder()
            let journal = FileEventStore("${dir}/journal")
            let ref = kit.system.typed().actorOf(tpCounter("c1", journal, rec).behavior(), "c1")
            @Expect(tpGet(ref), 55 + round * 5)
            @Expect(rec.contains("recovered:${10 + round}:${55 + round * 5}"), true)
            // 写入序号接续：第一轮写 11、第二轮写 12，没有冲突
            @Expect(tpAddAndGet(ref, 5), 60 + round * 5)
            @Expect(journal.highestSequenceNr(pid), 11 + round)
            kit.shutdown()
            journal.close()
        }
    }

    @TestCase
    func commandsDuringPersistAreProcessedInArrivalOrder(): Unit {
        let kit = ActorTestKit("tp-order")
        let rec = EventRecorder()
        let journal = MemoryEventStore()
        let ref = kit.system.typed().actorOf(tpCounter("order", journal, rec).behavior(), "order")
        for (i in 1..=100) {
            ref.tell(TpCmd.Add(i))
        }
        @Expect(rec.await("total:5050", TP_T), true)
        // 每条命令的 thenRun 看到的是前缀和：命令没有乱序、没有重复、没有基于旧状态
        let totals = rec.snapshot().filter({ e => e.startsWith("total:") }).toArray()
        @Expect(totals.size, 100)
        var expected = 0
        for (i in 0..100) {
            expected += i + 1
            @Expect(totals[i], "total:${expected}")
        }
        let data = tpJournalData(journal, tpPid("order"))
        @Expect(data.size, 100)
        for (i in 0..100) {
            @Expect(data[i], "${i + 1}")
        }
        kit.shutdown()
    }

    @TestCase
    func thenReplyRunsAfterEventIsPersisted(): Unit {
        let kit = ActorTestKit("tp-reply")
        let journal = MemoryEventStore()
        let ref = kit.system.typed().actorOf(tpCounter("reply", journal, EventRecorder()).behavior(), "reply")
        for (i in 1..=20) {
            // 收到回复时日志里一定已经有这条事件
            @Expect(tpAddAndGet(ref, 7), 7 * i)
            @Expect(journal.highestSequenceNr(tpPid("reply")), i)
        }
        kit.shutdown()
    }

    @TestCase
    func userStashAndUnstashAllKeepOrder(): Unit {
        let kit = ActorTestKit("tp-stash")
        let rec = EventRecorder()
        let ref = kit.system.typed().actorOf(tpCounter("stash", MemoryEventStore(), rec).behavior(), "stash")
        ref << TpCmd.Lock << TpCmd.Add(1) << TpCmd.Add(2) << TpCmd.Add(3)
        // 锁定期间 Add 被用户 stash，Get 照常回复
        @Expect(tpGet(ref), 0)
        ref.tell(TpCmd.Unlock)
        @Expect(rec.await("total:6", TP_T), true)
        let totals = rec.snapshot().filter({ e => e.startsWith("total:") }).toArray()
        @Expect(totals, ["total:1", "total:3", "total:6"])
        @Expect(tpGet(ref), 6)
        kit.shutdown()
    }

    @TestCase
    func retentionKeepsRecentSnapshotsAndDeletesOldEvents(): Unit {
        let dir = tpDir("retention")
        let pid = tpPid("ret")
        let snapshots = FileSnapshotStore("${dir}/snapshots", 100)
        let journal = FileEventStore("${dir}/journal")
        let rec = EventRecorder()
        let kit = ActorTestKit("tp-retention")
        let b = tpCounter("ret", journal, rec).withSnapshots(snapshots, tpCodec).withRetention(
            RetentionCriteria.snapshotEvery(10, 2).withDeleteEventsOnSnapshot())
        let ref = kit.system.typed().actorOf(b.behavior(), "ret")
        for (i in 1..=45) {
            ref.tell(TpCmd.Add(i))
        }
        @Expect(rec.await("total:1035", TP_T), true)
        // 序号 40 的快照之后删除 ≤ 40 − 2×10 = 20 的快照与事件
        @Expect(rec.await("events-deleted:20", TP_T), true)
        @Expect(rec.contains("snapshots-deleted:20"), true)
        @Expect(rec.snapshot().filter({ e => e.startsWith("snapshot-failed") || e.startsWith("delete-failed") }).isEmpty(),
            true)
        // 快照只在快照点上（在途期间到达的快照点只保留最新的一个，因此 10/20/30 可能被跳过，40 一定有）
        for (e in rec.snapshot()) {
            if (e.startsWith("snapshot:")) {
                let seq = Int64.parse(e.removePrefix("snapshot:"))
                @Expect(seq % 10, 0)
            }
        }
        @Expect(snapshots.getSnapshot(pid, 10).isNone(), true)
        @Expect(snapshots.getSnapshot(pid, 20).isNone(), true)
        @Expect(snapshots.getSnapshot(pid, 30).isSome(), rec.contains("snapshot:30"))
        @Expect(snapshots.getLatestSnapshot(pid).getOrThrow().getSequenceNumber(), 40)
        let remaining = journal.replay(pid, 1, Int64.Max)
        @Expect(remaining.size, 25)
        @Expect(remaining[0].sequenceNr, 21)
        @Expect(journal.highestSequenceNr(pid), 45)
        kit.shutdown()
        journal.close()

        // 恢复：快照 40 + 事件 41..45
        let kit2 = ActorTestKit("tp-retention-2")
        let rec2 = EventRecorder()
        let journal2 = FileEventStore("${dir}/journal")
        let b2 = tpCounter("ret", journal2, rec2).withSnapshots(FileSnapshotStore("${dir}/snapshots", 100), tpCodec)
            .withRetention(RetentionCriteria.snapshotEvery(10, 2).withDeleteEventsOnSnapshot())
        let ref2 = kit2.system.typed().actorOf(b2.behavior(), "ret")
        @Expect(tpGet(ref2), 1035)
        @Expect(rec2.contains("recovered:45:1035"), true)
        kit2.shutdown()
        journal2.close()
    }

    @TestCase
    func eventAdapterEvolvesOldSchema(): Unit {
        let pid = tpPid("evo")
        let journal = MemoryEventStore()
        // 旧版本写下的日志：一条记录装三个增量、一条废弃事件、一条新格式
        journal.append(pid, [BasicEvent("e1", pid, "Added.v0", "1;2;3"), BasicEvent("e2", pid, "Obsolete", "x"),
            BasicEvent("e3", pid, "Added", "10")], 0)
        let kit = ActorTestKit("tp-evo")
        let rec = EventRecorder()
        let ref = kit.system.typed().actorOf(tpCounter("evo", journal, rec).behavior(), "evo")
        @Expect(tpGet(ref), 16)
        // 序号按日志（3 条记录），不是适配后的事件数（4 个事件）：否则下一次写入期望序号 4 而冲突
        @Expect(rec.contains("recovered:3:16"), true)
        @Expect(tpAddAndGet(ref, 1), 17)
        @Expect(journal.highestSequenceNr(pid), 4)
        kit.shutdown()
    }

    @TestCase
    func corruptJournalFailsRecoveryAndStopsActor(): Unit {
        let pid = tpPid("bad")
        let journal = MemoryEventStore()
        journal.append(pid, [BasicEvent("e1", pid, "Added", "1"), BasicEvent("e2", pid, "Garbage", "?")], 0)
        let kit = ActorTestKit("tp-bad")
        let rec = EventRecorder()
        kit.system.typed().actorOf(tpCounter("bad", journal, rec).behavior(), "bad")
        @Expect(rec.await("recovery-failed", TP_T), true)
        @Expect(awaitCondition(TP_T, { => kit.system.lookup("/user/bad").isNone() }), true)
        @Expect(rec.snapshot().filter({ e => e.startsWith("recovered") }).isEmpty(), true)
        kit.shutdown()
    }

    @TestCase
    func eventHandlerFailureDuringReplayFailsRecovery(): Unit {
        // 日志能解码，但事件处理器在重放时抛异常（例如新版本的不变式检查）：同样发 RecoveryFailed 并停止
        let pid = tpPid("handler")
        let journal = MemoryEventStore()
        journal.append(pid, [BasicEvent("e1", pid, "Added", "-1")], 0)
        let kit = ActorTestKit("tp-handler")
        let rec = EventRecorder()
        let b = EventSourcedBehavior<TpCmd, TpEvt, Int64>(PersistenceId.of("Counter", "handler"), emptyState: 0,
            commandHandler: { s, cmd, effect => effect.none() }, eventHandler: { s, e =>
                match (e) {
                    case TpEvt.Added(n) where n < 0 => throw IllegalStateException("负数增量")
                    case TpEvt.Added(n) => s + n
                    case _ => s
                }
            }, eventAdapter: TpAdapter(), journal: journal).receiveSignal({ _, signal =>
            if (signal is RecoveryFailed) {
                rec.record("recovery-failed")
            }
        })
        kit.system.typed().actorOf(b.behavior(), "handler")
        @Expect(rec.await("recovery-failed", TP_T), true)
        @Expect(awaitCondition(TP_T, { => kit.system.lookup("/user/handler").isNone() }), true)
        kit.shutdown()
    }

    @TestCase
    func journalGapWithoutCoveringSnapshotFailsRecovery(): Unit {
        // 事件 1..30，≤ 20 已删除，却没有快照覆盖它们：恢复必须失败，而不是从 21 开始算出少了一段的状态
        let pid = tpPid("gap")
        let journal = MemoryEventStore()
        journal.append(pid, Array<Event>(30, { i => BasicEvent("g${i + 1}", pid, "Added", "1") }), 0)
        journal.deleteTo(pid, 20)
        let kit = ActorTestKit("tp-gap")
        let rec = EventRecorder()
        kit.system.typed().actorOf(tpCounter("gap", journal, rec).withSnapshots(MemorySnapshotStore(), tpCodec).behavior(),
            "gap")
        @Expect(rec.await("recovery-failed", TP_T), true)
        @Expect(awaitCondition(TP_T, { => kit.system.lookup("/user/gap").isNone() }), true)
        kit.shutdown()
    }

    @TestCase
    func lateOlderSnapshotDoesNotBecomeLatest(): Unit {
        // 重启前提交的旧快照可能比新实例的快照晚落盘：最新快照必须按序号，而不是按保存顺序
        let stores: Array<SnapshotStore> = [MemorySnapshotStore(), FileSnapshotStore(tpDir("snap-order"), 3)]
        for (store in stores) {
            store.saveSnapshot(BasicSnapshot("p", 30, "s30"))
            store.saveSnapshot(BasicSnapshot("p", 20, "s20"))
            @Expect(store.getLatestSnapshot("p").getOrThrow().getStateData(), "s30")
            store.saveSnapshot(BasicSnapshot("p", 40, "s40"))
            store.saveSnapshot(BasicSnapshot("p", 40, "s40b"))
            store.saveSnapshot(BasicSnapshot("p", 10, "s10"))
            @Expect(store.getLatestSnapshot("p").getOrThrow().getStateData(), "s40b")
            @Expect(store.getSnapshot("p", 30).getOrThrow().getStateData(), "s30")
            store.deleteSnapshotsTo("p", 30)
            @Expect(store.getSnapshot("p", 30).isNone(), true)
            @Expect(store.getLatestSnapshot("p").getOrThrow().getSequenceNumber(), 40)
        }
    }

    @TestCase
    func journalConflictFailsActorAndRestartRecovers(): Unit {
        let pid = tpPid("conflict")
        let journal = MemoryEventStore()
        let kit = ActorTestKit("tp-conflict")
        let rec = EventRecorder()
        let b = TypedBehaviors.supervise(tpCounter("conflict", journal, rec).behavior()).onFailure(
            TypedSupervisorStrategy.restart())
        let ref = kit.system.typed().actorOf(b, "conflict")
        @Expect(tpAddAndGet(ref, 1), 1)
        // 另一个写者（例如脑裂时的第二个实例）在日志里插了一条事件
        journal.append(pid, [BasicEvent("other-2", pid, "Added", "100")], 1)
        // 本 Actor 的下一次写入期望序号 1 → 冲突 → 失败 → 重启 → 从日志恢复出 101
        ref.tell(TpCmd.Add(5))
        @Expect(rec.await("recovered:2:101", TP_T), true)
        @Expect(tpAddAndGet(ref, 5), 106)
        @Expect(tpJournalData(journal, pid), ["1", "100", "5"])
        kit.shutdown()
    }

    @TestCase
    func staleIoCompletionFromPreviousIncarnationIsIgnored(): Unit {
        let pid = tpPid("stale")
        let journal = MemoryEventStore()
        let snapshots = GatedSnapshotStore()
        snapshots.saveOpen.store(false)
        let kit = ActorTestKit("tp-stale")
        let rec = EventRecorder()
        let b = TypedBehaviors.supervise(tpCounter("stale", journal, rec).withSnapshots(snapshots, tpCodec)
            .withRetention(RetentionCriteria.snapshotEvery(10, 2)).behavior()).onFailure(TypedSupervisorStrategy.restart())
        let ref = kit.system.typed().actorOf(b, "stale")
        for (i in 1..=10) {
            ref.tell(TpCmd.Add(i))
        }
        // 序号 10 的快照已提交给 IO 线程、被挡住
        @Expect(rec.await("total:55", TP_T), true)
        @Expect(awaitCondition(TP_T, { => snapshots.saveCalls.load() == 1 }), true)
        // 另一个写者插入事件 → 下一次写入冲突 → 重启，新实例从日志恢复
        journal.append(pid, [BasicEvent("other-11", pid, "Added", "100")], 10)
        ref.tell(TpCmd.Add(1))
        @Expect(rec.await("recovered:11:155", TP_T), true)
        // 放行旧实例的快照：它的完成信号到达的是新实例，必须丢弃（否则旧实例的状态机会在新实例的线程里继续运行）
        snapshots.saveOpen.store(true)
        @Expect(awaitCondition(TP_T, { => snapshots.saved.load() == 1 }), true)
        @Expect(tpAddAndGet(ref, 1), 156)
        @Expect(tpAddAndGet(ref, 1), 157)
        @Expect(rec.contains("snapshot:10"), false)
        kit.shutdown()
    }

    @TestCase
    func stashOverflowDropsCommandsAsUnhandled(): Unit {
        let journal = GatedEventStore()
        journal.replayOpen.store(false)
        let kit = ActorTestKit("tp-overflow-drop")
        let rec = EventRecorder()
        let b = tpCounter("drop", journal, rec).withStash(2)
        let ref = kit.system.typed().actorOf(b.behavior(), "drop")
        @Expect(awaitCondition(TP_T, { => journal.replayCalls.load() == 1 }), true)
        // 恢复被挡住：前两条暂存，后三条超出容量被丢弃（作为未处理消息上报）
        for (i in 1..=5) {
            ref.tell(TpCmd.Add(i))
        }
        @Expect(awaitCondition(TP_T, { => kit.system.unhandledCount() == 3 }), true)
        journal.replayOpen.store(true)
        @Expect(rec.await("total:3", TP_T), true)
        @Expect(tpGet(ref), 3)
        kit.shutdown()
    }

    @TestCase
    func stashOverflowCanFailActor(): Unit {
        let journal = GatedEventStore()
        journal.replayOpen.store(false)
        let kit = ActorTestKit("tp-overflow-fail")
        let b = tpCounter("fail", journal, EventRecorder()).withStash(2, overflow: StashOverflowStrategy.FailActor)
        let ref = kit.system.typed().actorOf(b.behavior(), "fail")
        for (i in 1..=3) {
            ref.tell(TpCmd.Add(i))
        }
        @Expect(awaitCondition(TP_T, { => kit.system.lookup("/user/fail").isNone() }), true)
        journal.replayOpen.store(true)
        kit.shutdown()
    }

    @TestCase
    func retentionWithoutSnapshotsIsRejected(): Unit {
        var rejected = false
        try {
            tpCounter("x", MemoryEventStore(), EventRecorder()).withRetention(RetentionCriteria.snapshotEvery(10, 2))
                .behavior()
        } catch (e: IllegalArgumentException) {
            rejected = true
        }
        @Expect(rejected, true)
    }

    @TestCase
    func durableStatePersistsDeletesAndRecovers(): Unit {
        let dir = tpDir("durable")
        let pid = PersistenceId.of("Profile", "p1").id
        let kit1 = ActorTestKit("tp-ds-1")
        let rec1 = EventRecorder()
        let store1 = FileDurableStateStore(dir)
        let ref1 = kit1.system.typed().actorOf(dsBehavior("p1", store1, rec1).behavior(), "p1")
        @Expect(rec1.await("recovered:0:<empty>", TP_T), true)
        @Expect(dsAsk(ref1, { r => DsCmd.Put("a", r) }), "a")
        @Expect(dsAsk(ref1, { r => DsCmd.Put("b", r) }), "b")
        @Expect(store1.revisionOf(pid), 2)
        // 删除：状态回到 emptyState，存储留下修订号 3 的墓碑
        @Expect(dsAsk(ref1, { r => DsCmd.Clear(r) }), "<empty>")
        @Expect(store1.get(pid).isNone(), true)
        @Expect(store1.revisionOf(pid), 3)
        @Expect(dsAsk(ref1, { r => DsCmd.Put("c", r) }), "c")
        @Expect(store1.revisionOf(pid), 4)
        @Expect(dsAsk(ref1, { r => DsCmd.Clear(r) }), "<empty>")
        kit1.shutdown()

        // 删除之后重启：修订号从墓碑（5）接续，不回到 0（P14）
        let kit2 = ActorTestKit("tp-ds-2")
        let rec2 = EventRecorder()
        let store2 = FileDurableStateStore(dir)
        let ref2 = kit2.system.typed().actorOf(dsBehavior("p1", store2, rec2).behavior(), "p1")
        @Expect(dsAsk(ref2, { r => DsCmd.Read(r) }), "<empty>")
        @Expect(rec2.contains("recovered:5:<empty>"), true)
        @Expect(dsAsk(ref2, { r => DsCmd.Put("d", r) }), "d")
        @Expect(store2.revisionOf(pid), 6)
        kit2.shutdown()

        let kit3 = ActorTestKit("tp-ds-3")
        let rec3 = EventRecorder()
        let ref3 = kit3.system.typed().actorOf(dsBehavior("p1", FileDurableStateStore(dir), rec3).behavior(), "p1")
        @Expect(dsAsk(ref3, { r => DsCmd.Read(r) }), "d")
        @Expect(rec3.contains("recovered:6:d"), true)
        kit3.shutdown()
    }

    @TestCase
    func deletedStateRejectsStaleWriter(): Unit {
        let dir = tpDir("p14")
        let stores: Array<DurableStateStore> = [MemoryDurableStateStore(), FileDurableStateStore(dir)]
        for (store in stores) {
            store.upsert("k", 1, "a")
            store.upsert("k", 2, "b")
            store.upsert("other", 1, "o")
            store.delete("k", 2)
            @Expect(store.get("k").isNone(), true)
            @Expect(store.revisionOf("k"), 3)
            @Expect(store.persistenceIds(), ["other"])
            // 删除之前读到修订号 0 / 2 的旧写者都被拒绝（此前删除后修订号回到 0，upsert(k, 1) 会把状态写回来）
            for (rev in [1, 3]) {
                var conflict = false
                try {
                    store.upsert("k", rev, "stale")
                } catch (e: DurableStateConflictException) {
                    conflict = true
                }
                @Expect(conflict, true)
            }
            @Expect(store.get("k").isNone(), true)
            store.upsert("k", 4, "new")
            @Expect(store.get("k").getOrThrow().state, "new")
            @Expect(store.revisionOf("k"), 4)
        }
        // 文件存储重新打开：墓碑与修订号都在磁盘上
        let reopened = FileDurableStateStore(dir)
        @Expect(reopened.revisionOf("k"), 4)
        reopened.delete("k", 4)
        let again = FileDurableStateStore(dir)
        @Expect(again.revisionOf("k"), 5)
        @Expect(again.get("k").isNone(), true)
        @Expect(again.persistenceIds(), ["other"])
    }
}
