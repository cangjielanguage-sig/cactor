/*
 * 持久化测试：基于真实文件系统
 */
package cactor_persistence

import std.fs.*
import std.sync.*
import std.time.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*

func freshDir(tag: String): String {
    let dir = "/tmp/cactor-persistence-test/${tag}-${MonoTime.now().hashCode()}"
    if (exists(dir)) {
        remove(dir, recursive: true)
    }
    dir
}

@Test
public class FilePersistenceTest {
    @TestCase
    func eventsSurviveRestart(): Unit {
        let dir = freshDir("restart")
        let store1 = FileEventStore(dir)
        for (i in 0..100) {
            store1.saveEvent(BasicEvent("e${i}", "account-1", "Deposited", "amount=${i};备注=第${i}笔", 1000 + i))
        }
        store1.saveEvent(BasicEvent("x", "account-2", "Opened", "", 5))
        store1.close()

        // 模拟进程重启：新的存储实例读取同一目录
        let store2 = FileEventStore(dir)
        let events = store2.getEvents("account-1")
        @Expect(events.size, 100)
        @Expect(events[0].getEventId(), "e0")
        @Expect(events[99].getEventData(), "amount=99;备注=第99笔")
        @Expect(events[42].getTimestamp(), 1042)
        @Expect(store2.getEvents("account-2").size, 1)
        @Expect(store2.getEvents("account-2")[0].getEventData(), "")
        @Expect(store2.getEventsAfter("account-1", 1097).size, 2)
        @Expect(store2.getEvents("missing").size, 0)
        store2.deleteEvents("account-1")
        @Expect(store2.getEvents("account-1").size, 0)
        remove(dir, recursive: true)
    }

    @TestCase
    func tornTailIsIgnoredAndRepaired(): Unit {
        let dir = freshDir("torn")
        let store = FileEventStore(dir)
        store.saveEvent(BasicEvent("e1", "a", "T", "one", 1))
        store.saveEvent(BasicEvent("e2", "a", "T", "two", 2))
        // 模拟崩溃：在日志末尾留下半条记录
        let journal = "${dir}/${safeFileName("a")}.journal"
        let f = File(journal, OpenMode.Append)
        f.write([0, 0, 0, 50, 1, 2, 3])
        f.close()
        @Expect(store.getEvents("a").size, 2)
        // 继续追加：残缺尾部被截断，新记录可以正常读出
        store.saveEvent(BasicEvent("e3", "a", "T", "three", 3))
        let events = FileEventStore(dir).getEvents("a")
        @Expect(events.size, 3)
        @Expect(events[2].getEventData(), "three")
        remove(dir, recursive: true)
    }

    @TestCase
    func actorIdCannotEscapeDirectory(): Unit {
        let dir = freshDir("escape")
        let store = FileEventStore(dir)
        store.saveEvent(BasicEvent("e", "../../etc/evil", "T", "x", 1))
        @Expect(exists("${dir}/../../etc/evil.journal"), false)
        @Expect(store.getEvents("../../etc/evil").size, 1)
        // 所有文件都在存储目录内
        var count = 0
        for (_ in Directory.readFrom(dir)) {
            count++
        }
        @Expect(count, 1)
        remove(dir, recursive: true)
    }

    @TestCase
    func concurrentAppendsAreNotCorrupted(): Unit {
        let dir = freshDir("concurrent")
        let store = FileEventStore(dir)
        let futures = Array(8, { t =>
            spawn {
                for (i in 0..50) {
                    store.saveEvent(BasicEvent("t${t}-${i}", "shared", "T", "payload-${t}-${i}", i))
                }
            }
        })
        for (f in futures) {
            f.get()
        }
        let events = FileEventStore(dir).getEvents("shared")
        @Expect(events.size, 400)
        let ids = HashSet<String>()
        for (e in events) {
            ids.add(e.getEventId())
        }
        @Expect(ids.size, 400)
        remove(dir, recursive: true)
    }

    @TestCase
    func snapshotsKeepLatestAndSurviveRestart(): Unit {
        let dir = freshDir("snapshot")
        let store = FileSnapshotStore(dir, 3)
        @Expect(store.getLatestSnapshot("cart").isNone(), true)
        for (seq in 1..=5) {
            store.saveSnapshot(BasicSnapshot("cart", seq, "{\"items\":${seq},\"名称\":\"购物车\"}", 100 + seq))
        }
        let reopened = FileSnapshotStore(dir, 3)
        match (reopened.getLatestSnapshot("cart")) {
            case Some(sn) =>
                @Expect(sn.getSequenceNumber(), 5)
                @Expect(sn.getStateData(), "{\"items\":5,\"名称\":\"购物车\"}")
                @Expect(sn.getTimestamp(), 105)
            case None => @Fail("快照丢失")
        }
        // 只保留最新 3 个
        @Expect(reopened.getSnapshot("cart", 2).isNone(), true)
        @Expect(reopened.getSnapshot("cart", 3).isSome(), true)
        @Expect(exists("${dir}/${safeFileName("cart")}.snapshots.tmp"), false)
        reopened.deleteSnapshots("cart")
        @Expect(reopened.getLatestSnapshot("cart").isNone(), true)
        remove(dir, recursive: true)
    }

    @TestCase
    func corruptedRecordIsReported(): Unit {
        let dir = freshDir("corrupt")
        Directory.create(dir, recursive: true)
        // 长度完整但内容非法的记录
        File.writeTo("${dir}/${safeFileName("bad")}.journal", [0, 0, 0, 4, 0, 0, 0, 99])
        @ExpectThrows[PersistenceCorruptionException](FileEventStore(dir).getEvents("bad"))
        remove(dir, recursive: true)
    }
}
