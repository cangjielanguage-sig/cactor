/*
 * CActor - R6 测试：持久化查询（ReadJournal）、持久化状态（Durable State）、存储插件注册表
 */
package cactor_tests.integration.persistence

import std.fs.*
import std.sync.*
import std.time.*
import std.convert.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.{CActorSystemFactory}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.context.{ActorContext}
import cactor_persistence.{EventStore, MemoryEventStore, FileEventStore, FileSnapshotStore, BasicEvent, ReadJournal,
    DurableStateActor, DurableStateStore, MemoryDurableStateStore, FileDurableStateStore, DurableStateConflictException,
    PersistencePlugins}

func freshDir(tag: String): String {
    let d = "/tmp/cactor-query-test/${tag}-${MonoTime.now().hashCode()}"
    if (exists(d)) {
        remove(d, recursive: true)
    }
    d
}

func ev(id: String, n: Int64, eventType: String, ts: Int64): BasicEvent {
    BasicEvent("${id}-${n}", id, eventType, "${n}", ts)
}

/** 持久化状态计数器：命令 "inc" / "reset" */
class StateCounter <: DurableStateActor<Int64> {
    let id: String
    let observed: AtomicInt64

    init(id: String, store: DurableStateStore, observed: AtomicInt64) {
        super(store)
        this.id = id
        this.observed = observed
    }

    public prop name: String { get() { "state-counter" } }
    public prop description: String { get() { "state counter" } }

    public func persistenceId(): String { "counter-${id}" }
    public func emptyState(): Int64 { 0 }
    public func encodeState(s: Int64): String { "${s}" }
    public func decodeState(s: String): Int64 { Int64.parse(s) }

    public func handleCommand(message: Message, context: ActorContext): MessageResult {
        if (let Some(m) <- (message as StringMessage)) {
            match (m.getContent()) {
                case "inc" => persistState(state + 1)
                case "reset" => deleteState()
                case _ => ()
            }
            observed.store(state)
        }
        MessageResult.Handled
    }
}

@Test
public class ReadJournalTest {
    @TestCase
    func currentEventsAndRanges(): Unit {
        let store = MemoryEventStore()
        for (n in 1..=5) {
            store.saveEvent(ev("p1", n, "T", n))
        }
        let rj = ReadJournal(store)
        let all = rj.currentEventsByPersistenceId("p1")
        @Expect(all.size, 5)
        for (i in 0..5) {
            @Expect(all[i].sequenceNr, i + 1)
            @Expect(all[i].event.getEventData(), "${i + 1}")
        }
        let mid = rj.currentEventsByPersistenceId("p1", 2, 4)
        @Expect(mid.size, 3)
        @Expect(mid[0].sequenceNr, 2)
        @Expect(mid[2].sequenceNr, 4)
        @Expect(rj.currentEventsByPersistenceId("p1", 6, 10).size, 0)
        @Expect(rj.currentEventsByPersistenceId("nobody").size, 0)
    }

    @TestCase
    func liveStreamDeliversEachEventOnceInOrderAndResumes(): Unit {
        let store = FileEventStore(freshDir("live"))
        for (n in 1..=3) {
            store.saveEvent(ev("live", n, "T", n))
        }
        let rj = ReadJournal(store, Duration.millisecond * 10)
        let lock = Mutex()
        let got = ArrayList<Int64>()
        let sub = rj.eventsByPersistenceId("live", 2, { e => synchronized(lock) { got.add(e.sequenceNr) } })
        // 订阅期间另一个线程持续追加
        let writer = spawn {
            for (n in 4..=50) {
                store.saveEvent(ev("live", n, "T", n))
                if (n % 10 == 0) {
                    sleep(Duration.millisecond * 5)
                }
            }
        }
        writer.get()
        @Expect(eventually(Duration.second * 5, { => sub.lastSequenceNr() == 50 }), true)
        sub.cancel()
        let snapshot = synchronized(lock) { got.toArray() }
        @Expect(snapshot.size, 49)
        for (i in 0..snapshot.size) {
            @Expect(snapshot[i], i + 2)
        }
        // 取消后不再交付；从保存的偏移量继续只拿到新事件
        sleep(Duration.millisecond * 50)
        for (n in 51..=55) {
            store.saveEvent(ev("live", n, "T", n))
        }
        sleep(Duration.millisecond * 50)
        @Expect(synchronized(lock) { got.size }, 49)
        let resumed = ArrayList<Int64>()
        let sub2 = rj.eventsByPersistenceId("live", sub.lastSequenceNr() + 1, { e => synchronized(lock) { resumed.add(e.sequenceNr) } })
        @Expect(eventually(Duration.second * 5, { => sub2.delivered() == 5 }), true)
        sub2.cancel()
        @Expect(synchronized(lock) { resumed.toArray() }, [51, 52, 53, 54, 55])
    }

    @TestCase
    func callbackFailureEndsSubscription(): Unit {
        let store = MemoryEventStore()
        for (n in 1..=5) {
            store.saveEvent(ev("f", n, "T", n))
        }
        let errors = AtomicInt64(0)
        let sub = ReadJournal(store, Duration.millisecond * 10).eventsByPersistenceId("f", 1, { e =>
            if (e.sequenceNr == 3) {
                throw Exception("投影失败")
            }
        }, { _ => errors.fetchAdd(1) })
        @Expect(eventually(Duration.second * 3, { => errors.load() == 1 }), true)
        @Expect(sub.isCancelled(), true)
        // 失败的那条不算交付，可以从 lastSequenceNr + 1 重试
        @Expect(sub.lastSequenceNr(), 2)
    }

    @TestCase
    func persistenceIdsAndEventsByType(): Unit {
        let ids = ["订单/1", "a b", "x:y", ""]
        let stores: Array<EventStore> = [MemoryEventStore(), FileEventStore(freshDir("ids"))]
        for (store in stores) {
            var ts: Int64 = 100
            for (id in ids) {
                store.saveEvent(ev(id, 1, "Created", ts))
                store.saveEvent(ev(id, 2, "Paid", ts + 10))
                ts -= 1
            }
            let rj = ReadJournal(store)
            let found = rj.currentPersistenceIds()
            @Expect(found.size, 4)
            for (id in ids) {
                @Expect(found.contains(id), true)
            }
            let created = rj.currentEventsByType("Created")
            @Expect(created.size, 4)
            // 按时间戳排序：最后写入的标识时间戳最小
            @Expect(created[0].persistenceId, "")
            @Expect(created[3].persistenceId, "订单/1")
            for (e in created) {
                @Expect(e.sequenceNr, 1)
            }
            @Expect(rj.currentEventsByType("Paid").size, 4)
        }
    }

    /** 读侧投影：写侧是真实 Actor 系统中的事件溯源账户，读侧经实时查询独立计算余额，结果一致 */
    @TestCase
    func projectionFollowsEventSourcedActor(): Unit {
        let base = freshDir("projection")
        let events = FileEventStore("${base}/journal")
        let snaps = FileSnapshotStore("${base}/snapshots")
        let balance = AtomicInt64(-1)
        let rejected = AtomicInt64(0)
        let projected = AtomicInt64(0)
        let sub = ReadJournal(events, Duration.millisecond * 10).eventsByPersistenceId("account-7", 1, { e =>
            let amount = Int64.parse(e.event.getEventData())
            match (e.event.getEventType()) {
                case "Deposited" => projected.fetchAdd(amount)
                case "Withdrawn" => projected.fetchSub(amount)
                case _ => 0
            }
            ()
        })
        let system = CActorSystemFactory.create("projection-${MonoTime.now().hashCode()}")
        let acc = system.actorOf({ => AccountActor("7", events, snaps, balance, rejected) }, "account")
        for (i in 1..=40) {
            acc.tell(StringMessage("deposit:${i}"))
            if (i % 5 == 0) {
                acc.tell(StringMessage("withdraw:7"))
            }
        }
        @Expect(eventually(Duration.second * 5, { => balance.load() == 820 - 56 }), true)
        @Expect(eventually(Duration.second * 5, { => projected.load() == balance.load() }), true)
        @Expect(sub.lastSequenceNr(), 48)
        sub.cancel()
        system.terminate()
    }
}

@Test
public class DurableStateTest {
    @TestCase
    func stateSurvivesSystemRestart(): Unit {
        let dir = freshDir("state")
        let observed = AtomicInt64(-1)
        let system1 = CActorSystemFactory.create("ds-1-${MonoTime.now().hashCode()}")
        let c1 = system1.actorOf({ => StateCounter("a", FileDurableStateStore(dir), observed) }, "counter")
        for (_ in 0..10) {
            c1.tell(StringMessage("inc"))
        }
        @Expect(eventually(Duration.second * 5, { => observed.load() == 10 }), true)
        system1.terminate()
        // 新进程：全新的存储实例
        let store2 = FileDurableStateStore(dir)
        let rec = store2.get("counter-a").getOrThrow()
        @Expect(rec.revision, 10)
        @Expect(rec.state, "10")
        let observed2 = AtomicInt64(-1)
        let system2 = CActorSystemFactory.create("ds-2-${MonoTime.now().hashCode()}")
        let c2 = system2.actorOf({ => StateCounter("a", store2, observed2) }, "counter")
        c2.tell(StringMessage("inc"))
        @Expect(eventually(Duration.second * 5, { => observed2.load() == 11 }), true)
        c2.tell(StringMessage("reset"))
        @Expect(eventually(Duration.second * 5, { => observed2.load() == 0 }), true)
        @Expect(store2.get("counter-a").isNone(), true)
        system2.terminate()
    }

    @TestCase
    func staleRevisionIsRejected(): Unit {
        let stores: Array<DurableStateStore> = [MemoryDurableStateStore(), FileDurableStateStore(freshDir("conflict"))]
        for (store in stores) {
            store.upsert("k", 1, "a")
            store.upsert("k", 2, "b")
            var conflicts = 0
            for (rev in [1, 2, 4]) {
                try {
                    store.upsert("k", rev, "x")
                } catch (e: DurableStateConflictException) {
                    conflicts++
                }
            }
            @Expect(conflicts, 3)
            @Expect(store.get("k").getOrThrow().state, "b")
            var deleteConflict = false
            try {
                store.delete("k", 1)
            } catch (e: DurableStateConflictException) {
                deleteConflict = true
            }
            @Expect(deleteConflict, true)
            store.delete("k", 2)
            @Expect(store.get("k").isNone(), true)
        }
    }

    /** 8 个线程各做 100 次“读 → 加一 → 按修订号写，冲突则重读重试”：乐观并发下一次也不丢 */
    @TestCase
    func concurrentWritersWithRetryLoseNothing(): Unit {
        let stores: Array<DurableStateStore> = [MemoryDurableStateStore(), FileDurableStateStore(freshDir("race"))]
        for (store in stores) {
            let conflicts = AtomicInt64(0)
            let workers = ArrayList<Future<Unit>>()
            for (_ in 0..8) {
                workers.add(spawn {
                    for (_ in 0..100) {
                        while (true) {
                            let (rev, v) = match (store.get("hot")) {
                                case Some(r) => (r.revision, Int64.parse(r.state))
                                case None => (0, 0)
                            }
                            try {
                                store.upsert("hot", rev + 1, "${v + 1}")
                                break
                            } catch (_: DurableStateConflictException) {
                                conflicts.fetchAdd(1)
                            }
                        }
                    }
                })
            }
            for (w in workers) {
                w.get()
            }
            let r = store.get("hot").getOrThrow()
            @Expect(r.state, "800")
            @Expect(r.revision, 800)
            @Expect(conflicts.load() > 0, true)
        }
    }

    @TestCase
    func strayTempFileIsIgnored(): Unit {
        let dir = freshDir("tmpfile")
        let store = FileDurableStateStore(dir)
        store.upsert("会话/1", 1, "旧状态")
        // 模拟写入临时文件后、rename 之前进程崩溃
        File.writeTo("${dir}/garbage.state.tmp", "半截数据".toArray())
        let reopened = FileDurableStateStore(dir)
        @Expect(reopened.get("会话/1").getOrThrow().state, "旧状态")
        @Expect(reopened.persistenceIds(), ["会话/1"])
    }
}

@Test
public class PersistencePluginsTest {
    @TestCase
    func storesAreSelectedByUri(): Unit {
        let dir = freshDir("plugins")
        let j = PersistencePlugins.journal("file:${dir}/journal")
        j.saveEvent(ev("p", 1, "T", 1))
        @Expect(FileEventStore("${dir}/journal").getEvents("p").size, 1)
        @Expect(PersistencePlugins.journal("memory:").getEvents("p").size, 0)
        let st = PersistencePlugins.durableStateStore("file:${dir}/state")
        st.upsert("s", 1, "v")
        @Expect(FileDurableStateStore("${dir}/state").get("s").getOrThrow().state, "v")
        PersistencePlugins.snapshotStore("memory:")
        var errors = 0
        for (bad in ["nosuchscheme:x", "file:", "no-colon"]) {
            try {
                PersistencePlugins.journal(bad)
            } catch (e: IllegalArgumentException) {
                errors++
            }
        }
        @Expect(errors, 3)
        // 自定义方案
        let created = AtomicInt64(0)
        PersistencePlugins.registerJournal("counting", { _ =>
            created.fetchAdd(1)
            MemoryEventStore()
        })
        PersistencePlugins.journal("counting:anything")
        @Expect(created.load(), 1)
    }
}
