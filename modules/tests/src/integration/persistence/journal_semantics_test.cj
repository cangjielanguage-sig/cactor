/*
 * CActor - E8′ 日志语义端到端：
 * - 同一持久化标识的两个 Actor 实例双写：后写者被拒绝（JournalConflictException），监督重启后从日志恢复，日志不交错（P10）；
 * - 插件按序号重放快照之后的事件、恢复后的最高序号正确（P5）；
 * - persistAll 一批事件一次落盘；快照后 deleteEventsTo，重启后状态与序号都对。
 */
package cactor_tests.integration.persistence

import std.fs.*
import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}
import cactor_persistence.*

func freshBase(tag: String): String {
    let d = "/tmp/cactor-journal-it/${tag}-${MonoTime.now().hashCode()}"
    if (exists(d)) {
        remove(d, recursive: true)
    }
    d
}

func dataOf(store: EventStore, id: String): String {
    let sb = StringBuilder()
    for (env in store.replay(id, 1, Int64.Max)) {
        if (sb.size > 0) {
            sb.append(",")
        }
        sb.append("${env.sequenceNr}:${env.event.getEventData()}")
    }
    sb.toString()
}

/** 命令 "batch:<n>"：一次 persistAll n 条 "1"；"trim"：保存快照并删除快照覆盖的事件 */
class Ledger <: EventSourcedActor {
    private var total: Int64 = 0
    private let observed: AtomicInt64

    init(events: EventStore, snapshots: SnapshotStore, observed: AtomicInt64) {
        super(events, snapshots, 0)
        this.observed = observed
    }

    public prop name: String { get() { "Ledger" } }
    public prop description: String { get() { "Ledger" } }

    public func persistenceId(): String { "ledger" }

    public func handleCommand(message: Message, context: ActorContext): MessageResult {
        if (let Some(m) <- (message as StringMessage)) {
            let parts = m.getContent().split(":")
            match (parts[0]) {
                case "batch" =>
                    let n = Int64.parse(parts[1])
                    persistAll(Array<(String, String)>(n, { _ => ("Added", "1") }))
                case "trim" =>
                    saveSnapshot()
                    deleteEventsTo(lastSequenceNr())
                case _ => ()
            }
            observed.store(total)
        }
        MessageResult.Handled
    }

    public func applyEvent(event: Event): Unit {
        total += Int64.parse(event.getEventData())
    }

    public func snapshotState(): Option<String> { Some("${total}") }

    public func restoreSnapshot(state: String): Unit {
        total = Int64.parse(state)
    }

    public func onRecoveryCompleted(): Unit {
        observed.store(total)
    }
}

@Test
public class JournalSemanticsTest {
    @TestCase
    func secondWriterForSameIdIsRejectedAndRecovers(): Unit {
        let base = freshBase("dual")
        let system = CActorSystemFactory.create("journal-dual-${MonoTime.now().hashCode()}")
        let events = FileEventStore("${base}/journal")
        let snaps = FileSnapshotStore("${base}/snapshots")
        let balanceA = AtomicInt64(-1)
        let balanceB = AtomicInt64(-1)
        let a = system.actorOf({ => AccountActor("dual", events, snaps, balanceA, AtomicInt64(0)) }, "a")
        a.tell(StringMessage("deposit:10"))
        a.tell(StringMessage("deposit:20"))
        @Expect(eventually(Duration.second * 5, { => balanceA.load() == 30 }), true)
        // 同一标识的第二个实例（例如分片移交时旧实例还没停）：从日志恢复到 30 后写入
        let b = system.actorOf({ => AccountActor("dual", events, snaps, balanceB, AtomicInt64(0)) }, "b")
        b.tell(StringMessage("deposit:5"))
        @Expect(eventually(Duration.second * 5, { => balanceB.load() == 35 }), true)
        // a 的状态已过期：它的写入被拒绝（此前直接写进日志，两个实例的事件交错、各自的余额分叉）
        a.tell(StringMessage("deposit:1"))
        // 被拒绝的 a 由监督重启，从日志恢复出 b 写入的事件
        @Expect(eventually(Duration.second * 5, { => balanceA.load() == 35 }), true)
        a.tell(StringMessage("deposit:2"))
        @Expect(eventually(Duration.second * 5, { => balanceA.load() == 37 }), true)
        @Expect(dataOf(events, "account-dual"), "1:10,2:20,3:5,4:2")
        @Expect(dataOf(FileEventStore("${base}/journal"), "account-dual"), "1:10,2:20,3:5,4:2")
        system.terminate()
        remove(base, recursive: true)
    }

    @TestCase
    func pluginReplaysBySequenceNotTimestamp(): Unit {
        let plugin = DefaultPersistencePlugin(PersistenceConfig.createEnabled("memory", "memory"))
        // 同一毫秒内连续写入：按时间戳筛选"快照之后"的事件会漏掉它们
        for (i in 1..=3) {
            plugin.persistEvent("acc", "Deposited", "${i}")
        }
        plugin.takeSnapshot("acc", "after-3")
        for (i in 4..=5) {
            plugin.persistEvent("acc", "Deposited", "${i}")
        }
        let replayed = ArrayList<String>()
        let snapSeq = AtomicInt64(-1)
        plugin.recoverActor("acc", { e => replayed.add(e.getEventData()) }, { s => snapSeq.store(s.getSequenceNumber()) })
        @Expect(snapSeq.load(), 3)
        @Expect(String.join(replayed.toArray(), delimiter: ","), "4,5")

        // 没有快照时恢复：最高序号取日志的（此前恒为 1，之后的快照序号全错）
        let fresh = DefaultPersistencePlugin(PersistenceConfig.createEnabled("memory", "memory"))
        for (i in 1..=5) {
            fresh.persistEvent("b", "Deposited", "${i}")
        }
        fresh.recoverActor("b", { _ => () }, { _ => () })
        fresh.takeSnapshot("b", "after-5")
        let seq = AtomicInt64(-1)
        let after = ArrayList<String>()
        fresh.recoverActor("b", { e => after.add(e.getEventData()) }, { s => seq.store(s.getSequenceNumber()) })
        @Expect(seq.load(), 5)
        @Expect(after.size, 0)
    }

    @TestCase
    func persistAllAndDeleteToSurviveRestart(): Unit {
        let base = freshBase("ledger")
        let observed = AtomicInt64(-1)
        let system1 = CActorSystemFactory.create("journal-ledger-1-${MonoTime.now().hashCode()}")
        let events1 = FileEventStore("${base}/journal")
        let ledger = system1.actorOf({ => Ledger(events1, FileSnapshotStore("${base}/snapshots"), observed) }, "ledger")
        ledger.tell(StringMessage("batch:5"))
        ledger.tell(StringMessage("batch:3"))
        @Expect(eventually(Duration.second * 5, { => observed.load() == 8 }), true)
        ledger.tell(StringMessage("trim"))
        @Expect(eventually(Duration.second * 5, { => events1.replay("ledger", 1, Int64.Max).size == 0 }), true)
        @Expect(events1.highestSequenceNr("ledger"), 8)
        system1.terminate()

        // 重启：快照恢复到 8，序号从 8 继续（日志已删空，但最高序号保留）
        let recovered = AtomicInt64(-1)
        let system2 = CActorSystemFactory.create("journal-ledger-2-${MonoTime.now().hashCode()}")
        let events2 = FileEventStore("${base}/journal")
        let l2 = system2.actorOf({ => Ledger(events2, FileSnapshotStore("${base}/snapshots"), recovered) }, "ledger")
        @Expect(eventually(Duration.second * 5, { => recovered.load() == 8 }), true)
        l2.tell(StringMessage("batch:2"))
        @Expect(eventually(Duration.second * 5, { => recovered.load() == 10 }), true)
        @Expect(dataOf(events2, "ledger"), "9:1,10:1")
        system2.terminate()
        remove(base, recursive: true)
    }
}
