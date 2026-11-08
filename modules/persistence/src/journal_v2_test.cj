/*
 * 日志 v2（E8′）：期望序号拒绝双写（P10）、deleteTo 保留最高序号、零填充尾部截断（P6）、
 * 中间损坏拒绝启动、v1 旧日志迁移、组提交与批量 fsync、tags、句柄 LRU
 */
package cactor_persistence

import std.fs.*
import std.sync.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*

func ev(pid: String, n: Int64): Event {
    BasicEvent("${pid}-${n}", pid, "T", "data-${n}", 1000 + n)
}

func journalFileOf(dir: String, pid: String): String {
    "${dir}/${safeFileName(pid)}.journal"
}

func seqsOf(envs: Array<EventEnvelope>): String {
    let sb = StringBuilder()
    for (i in 0..envs.size) {
        if (i > 0) {
            sb.append(",")
        }
        sb.append(envs[i].sequenceNr)
    }
    sb.toString()
}

func joined(xs: Array<String>): String {
    String.join(xs, delimiter: ",")
}

@Test
public class JournalV2Test {
    @TestCase
    func appendRejectsStaleWriter(): Unit {
        let dir = freshDir("conflict")
        let store = FileEventStore(dir)
        @Expect(store.append("p", [ev("p", 1), ev("p", 2)], 0), 2)
        // 期望序号过期：一条也不写
        @ExpectThrows[JournalConflictException](store.append("p", [ev("p", 3)], 1))
        @Expect(store.highestSequenceNr("p"), 2)
        // 另一个存储实例（另一个写者）追加后，本实例按文件大小发现变化，旧期望序号被拒绝
        let other = FileEventStore(dir)
        @Expect(other.append("p", [ev("p", 3)], 2), 3)
        @ExpectThrows[JournalConflictException](store.append("p", [ev("p", 4)], 2))
        @Expect(store.append("p", [ev("p", 4)], 3), 4)
        @Expect(seqsOf(FileEventStore(dir).replay("p", 1, Int64.Max)), "1,2,3,4")
        remove(dir, recursive: true)
    }

    @TestCase
    func deleteToKeepsHighestSequenceNr(): Unit {
        let dir = freshDir("deleteto")
        let store = FileEventStore(dir)
        store.append("p", [ev("p", 1), ev("p", 2), ev("p", 3), ev("p", 4), ev("p", 5)], 0)
        store.deleteTo("p", 3)
        @Expect(seqsOf(store.replay("p", 1, Int64.Max)), "4,5")
        @Expect(store.getEvents("p").size, 2)
        // 重启后仍是同样的结果，序号按记录而不是按位置
        let reopened = FileEventStore(dir)
        @Expect(reopened.highestSequenceNr("p"), 5)
        @Expect(reopened.replay("p", 5, 5)[0].event.getEventData(), "data-5")
        reopened.deleteTo("p", 100)
        @Expect(reopened.replay("p", 1, Int64.Max).size, 0)
        @Expect(reopened.highestSequenceNr("p"), 5)
        @Expect(FileEventStore(dir).append("p", [ev("p", 6)], 5), 6)
        @Expect(exists(journalFileOf(dir, "p") + ".tmp"), false)
        remove(dir, recursive: true)
    }

    @TestCase
    func zeroFilledTailIsTruncated(): Unit {
        let dir = freshDir("zerotail")
        let store = FileEventStore(dir)
        store.append("p", [ev("p", 1), ev("p", 2), ev("p", 3)], 0)
        let path = journalFileOf(dir, "p")
        let good = FileInfo(path).size
        // 断电后文件尾被零填充（P6：此前长度为 0 的记录被当成合法记录，恢复报损坏，Actor 永远起不来）
        let f = File(path, OpenMode.Append)
        f.write(Array<UInt8>(4096, repeat: 0))
        f.close()
        let reopened = FileEventStore(dir)
        @Expect(seqsOf(reopened.replay("p", 1, Int64.Max)), "1,2,3")
        @Expect(FileInfo(path).size, good)
        @Expect(reopened.append("p", [ev("p", 4)], 3), 4)
        @Expect(FileEventStore(dir).getEvents("p").size, 4)
        remove(dir, recursive: true)
    }

    @TestCase
    func midFileCorruptionIsRefusedNotTruncated(): Unit {
        let dir = freshDir("midcorrupt")
        let store = FileEventStore(dir)
        store.append("p", [ev("p", 1), ev("p", 2), ev("p", 3)], 0)
        let path = journalFileOf(dir, "p")
        let bytes = File.readFrom(path)
        // 翻转第 2 条记录负载中的一个字节：其后第 3 条记录仍完整，说明不是尾部残缺
        let first = journalHeaderEnd(bytes).getOrThrow()
        let second = first + RECORD_HEADER_SIZE + getU32(bytes, first)
        bytes[second + RECORD_HEADER_SIZE + 12] = bytes[second + RECORD_HEADER_SIZE + 12] ^ 0xFF
        File.writeTo(path, bytes)
        @ExpectThrows[PersistenceCorruptionException](FileEventStore(dir).replay("p", 1, Int64.Max))
        // 不截断：已确认的第 3 条事件仍在文件里，等待人工处理
        @Expect(FileInfo(path).size, bytes.size)
        remove(dir, recursive: true)
    }

    @TestCase
    func legacyV1JournalIsMigrated(): Unit {
        let dir = freshDir("legacy")
        Directory.create(dir, recursive: true)
        let path = journalFileOf(dir, "acct")
        let v1 = ArrayList<UInt8>()
        for (i in 1..=3) {
            v1.add(all: RecordWriter().putString("e${i}").putString("acct").putString("Deposited")
                .putString("amount=${i}").putInt64(500 + i).toRecord())
        }
        // v1 的残缺尾部也一并丢弃
        v1.add(all: [0, 0, 0, 40, 1, 2])
        File.writeTo(path, v1.toArray())
        let store = FileEventStore(dir)
        let envs = store.replay("acct", 1, Int64.Max)
        @Expect(seqsOf(envs), "1,2,3")
        @Expect(envs[2].event.getEventData(), "amount=3")
        @Expect(envs[0].event.getTimestamp(), 501)
        @Expect(hasJournalMagic(File.readFrom(path)), true)
        @Expect(store.append("acct", [ev("acct", 4)], 3), 4)
        @Expect(FileEventStore(dir).getEvents("acct").size, 4)
        remove(dir, recursive: true)
    }

    @TestCase
    func batchIsOneFsyncAndConcurrentWritersShareFsyncs(): Unit {
        let dir = freshDir("groupcommit")
        let store = FileEventStore(dir)
        store.append("p", [ev("p", 1)], 0)
        // 一批 100 条：一次写入、一次 fsync
        let before = persistenceFsyncCount()
        let batch = Array<Event>(100, { i => ev("p", i + 2) })
        @Expect(store.append("p", batch, 1), 101)
        @Expect(persistenceFsyncCount() - before, 1)
        // 8 个线程各追加 50 条到同一日志：fsync 次数少于追加次数（组提交）
        let start = persistenceFsyncCount()
        let futures = Array(8, { t =>
            spawn {
                for (i in 0..50) {
                    store.saveEvent(BasicEvent("t${t}-${i}", "shared", "T", "x", i))
                }
            }
        })
        for (f in futures) {
            f.get()
        }
        let used = persistenceFsyncCount() - start
        @Expect(store.highestSequenceNr("shared"), 400)
        @Expect(used < 400, true)
        @Expect(FileEventStore(dir).replay("shared", 1, Int64.Max).size, 400)
        remove(dir, recursive: true)
    }

    @TestCase
    func tagsSurviveAndCanBeQueried(): Unit {
        let dir = freshDir("tags")
        let store = FileEventStore(dir)
        store.append("a", [BasicEvent("a1", "a", "T", "x", 1).withTags(["vip", "eu"])], 0)
        store.append("b", [BasicEvent("b1", "b", "T", "y", 2).withTags(["vip"]), BasicEvent("b2", "b", "T", "z", 3)], 0)
        let reopened = FileEventStore(dir)
        @Expect(joined(reopened.replay("a", 1, 1)[0].event.getTags()), "vip,eu")
        let vip = ReadJournal(reopened).currentEventsByTag("vip")
        @Expect(vip.size, 2)
        @Expect(vip[0].persistenceId, "a")
        @Expect(vip[1].event.getEventId(), "b1")
        @Expect(ReadJournal(reopened).currentEventsByTag("eu").size, 1)
        remove(dir, recursive: true)
    }

    @TestCase
    func openHandlesAreBoundedByLru(): Unit {
        let dir = freshDir("lru")
        let store = FileEventStore(dir, fsync: false, maxOpenFiles: 4)
        for (i in 0..12) {
            store.append("p${i}", [ev("p${i}", 1)], 0)
        }
        @Expect(store.openFileCount() <= 4, true)
        // 被关闭句柄的日志照常可写可读
        @Expect(store.append("p0", [ev("p0", 2)], 1), 2)
        @Expect(store.openFileCount() <= 4, true)
        for (i in 0..12) {
            @Expect(FileEventStore(dir).highestSequenceNr("p${i}"), if (i == 0) { 2 } else { 1 })
        }
        store.close()
        @Expect(store.openFileCount(), 0)
        remove(dir, recursive: true)
    }

    @TestCase
    func longPersistenceIdsUseHashedFileNames(): Unit {
        let dir = freshDir("longid")
        let longId = "tenant/" + String(Array<Rune>(300, repeat: r'长'))
        let store = FileEventStore(dir)
        @Expect(store.append(longId, [ev(longId, 1)], 0), 1)
        @Expect(safeFileName(longId).size < 100, true)
        let reopened = FileEventStore(dir)
        @Expect(reopened.highestSequenceNr(longId), 1)
        @Expect(reopened.persistenceIds().contains(longId), true)
        reopened.deleteTo(longId, 1)
        @Expect(FileEventStore(dir).persistenceIds().contains(longId), true)
        let states = FileDurableStateStore("${dir}/state")
        states.upsert(longId, 1, "s")
        @Expect(states.persistenceIds().contains(longId), true)
        @Expect(states.get(longId).getOrThrow().state, "s")
        remove(dir, recursive: true)
    }

    @TestCase
    func memoryStoreHasTheSameSemantics(): Unit {
        let store = MemoryEventStore()
        store.append("p", [ev("p", 1), ev("p", 2), ev("p", 3)], 0)
        @ExpectThrows[JournalConflictException](store.append("p", [ev("p", 4)], 2))
        store.deleteTo("p", 2)
        @Expect(seqsOf(store.replay("p", 1, Int64.Max)), "3")
        @Expect(store.highestSequenceNr("p"), 3)
        @Expect(store.append("p", [ev("p", 4)], 3), 4)
        @ExpectThrows[IllegalArgumentException](store.append("p", [ev("other", 1)], 4))
    }
}
