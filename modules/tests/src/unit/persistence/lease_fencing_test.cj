/*
 * CActor - 文件租约与存储侧 fencing（DA4）
 *
 * 文件租约（共享目录即协调点）：
 *   - 两个持有者互斥；释放后对方立即获取；token 跨对象（模拟进程重启）单调递增
 *   - 多个竞争者同时获取同一个编号：link 原子性保证恰好一个赢家
 *   - 竞争者认为已过期（注入时钟偏差，违反假设）而接手时，原持有者下一次心跳发现更大编号、回调 lost
 * fencing：
 *   - 旧持有者的 token 在新持有者写过之后被拒绝（持久状态 / 事件日志）；同样的写入绕过 fencing 会覆盖新持有者的数据
 *   - 文件 fencing 记录跨实例保留，重启后旧 token 仍被拒绝
 *   - 租约已失去时经租约取 token 的写入直接被拒绝
 */
package cactor_tests.unit.persistence

import std.fs.*
import std.sync.*
import std.time.*
import std.random.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.coordination.*
import cactor_persistence.*
import cactor_testkit.*

func da4TempDir(tag: String): String {
    let d = "/tmp/cactor-da4-${tag}-${Random().nextUInt64()}"
    Directory.create(d, recursive: true)
    d
}

func da4Cleanup(dir: String): Unit {
    try {
        remove(dir, recursive: true)
    } catch (_: Exception) {}
}

func fileLeaseSettings(owner: String): LeaseSettings {
    LeaseSettings("singleton-ledger", owner, Duration.millisecond * 2000, Duration.millisecond * 100,
        Duration.millisecond * 200)
}

/** 时间偏移的墙钟（模拟竞争者时钟超前） */
func skewedWall(offsetMs: Int64): () -> Int64 {
    { => wallClockMillis() + offsetMs }
}

func fileLease(dir: String, owner: String): FileLease {
    FileLease(dir, fileLeaseSettings(owner), wallClock: wallClockMillis, monoClock: realMono, sync: false)
}

let monoStart = MonoTime.now()

func realMono(): Int64 {
    (MonoTime.now() - monoStart).toMilliseconds()
}

@Test
public class FileLeaseTest {
    @TestCase
    public func exclusiveReleaseAndTokensSurviveRestart(): Unit {
        let dir = da4TempDir("lease")
        let a = fileLease(dir, "a")
        let b = fileLease(dir, "b")
        @Expect(a.acquire({ _ => () }), true)
        @Expect(a.fencingToken(), Some(1))
        @Expect(b.acquire({ _ => () }), false)
        @Expect(b.fencingToken(), None<Int64>)
        @Expect(a.release(), true)
        @Expect(b.acquire({ _ => () }), true)
        @Expect(b.fencingToken(), Some(2))
        @Expect(b.release(), true)
        // 新对象 = 进程重启：编号来自目录，不回退；同一持有者名也拿新 token
        let a2 = fileLease(dir, "a")
        @Expect(a2.acquire({ _ => () }), true)
        @Expect(a2.fencingToken(), Some(3))
        @Expect(a2.latestIssuedToken(), 3)
        a2.release()
        @Expect(a.lostCount() + b.lostCount() + a2.lostCount(), 0)
        da4Cleanup(dir)
    }

    @TestCase
    public func concurrentContendersHaveExactlyOneWinner(): Unit {
        let dir = da4TempDir("race")
        let n = 8
        let wins = AtomicInt64(0)
        let leases = Array<FileLease>(n, { i => fileLease(dir, "c${i}") })
        let go = AtomicBool(false)
        let finished = AtomicInt64(0)
        for (i in 0..n) {
            let l = leases[i]
            spawn {
                while (!go.load()) {
                    () // 自旋等待同时起跑
                }
                try {
                    if (l.acquire({ _ => () })) {
                        wins.fetchAdd(1)
                    }
                } finally {
                    finished.fetchAdd(1)
                }
            }
        }
        go.store(true)
        @Expect(awaitCondition(Duration.second * 30, { => finished.load() == n }), true)
        @Expect(wins.load(), 1)
        var holders = 0
        for (l in leases) {
            if (l.checkLease()) {
                holders++
                @Expect(l.fencingToken(), Some(1))
                l.release()
            }
        }
        @Expect(holders, 1)
        da4Cleanup(dir)
    }

    @TestCase
    public func takeoverIsDetectedByOldHolderHeartbeat(): Unit {
        let dir = da4TempDir("takeover")
        let a = fileLease(dir, "a")
        let lost = AtomicInt64(0)
        @Expect(a.acquire({ _ => lost.fetchAdd(1) }), true)
        // 竞争者墙钟超前 10 s（超出 clockSkewMargin，违反部署假设）：它认为 a 的记录已过期而接手
        let b = FileLease(dir, fileLeaseSettings("b"), wallClock: skewedWall(10000), monoClock: realMono, sync: false)
        @Expect(b.acquire({ _ => () }), true)
        @Expect(b.fencingToken(), Some(2))
        // a 的下一次心跳看到更大的编号：续约失败、回调 lost、不再持有
        @Expect(awaitCondition(Duration.second * 10, { => lost.load() == 1 }), true)
        @Expect(a.checkLease(), false)
        @Expect(a.fencingToken(), None<Int64>)
        @Expect(b.checkLease(), true)
        b.release()
        da4Cleanup(dir)
    }
}

@Test
public class FencingTest {
    @TestCase
    public func staleDurableStateWriterIsRejectedAfterNewOwnerWrites(): Unit {
        let dir = da4TempDir("fence-state")
        let inner = FileDurableStateStore("${dir}/state")
        let fence = FileFencingTokenStore("${dir}/fence", sync: false)
        let leases = InMemoryLeaseStore()
        let leaseA = InMemoryLease(leases, LeaseSettings("shard-7", "a"))
        let leaseB = InMemoryLease(leases, LeaseSettings("shard-7", "b"))
        @Expect(leaseA.acquire({ _ => () }), true)
        // A 在检查租约时拿到 token，随后"停顿"：这个 token 代表一条在途的写入
        let tokenA = leaseA.fencingToken()
        let storeA = FencedDurableStateStore(inner, fence, "shard-7", { => tokenA })
        storeA.upsert("cart-1", 1, "a:1")

        // A 的租约被撤销（等价于停顿期间过期），B 获取并写入
        leases.revoke("shard-7")
        @Expect(leaseB.acquire({ _ => () }), true)
        @Expect(leaseB.fencingToken(), Some(2))
        let storeB = FencedDurableStateStore(inner, fence, leaseB)
        storeB.upsert("cart-1", 2, "b:2")

        // A 醒来，重新读取修订号（乐观并发拦不住），带旧 token 写入：被 fencing 拒绝，B 的数据不变
        var stale = false
        try {
            storeA.upsert("cart-1", inner.revisionOf("cart-1") + 1, "a:stale")
        } catch (e: StaleFencingTokenException) {
            stale = e.token == 1 && e.highestSeen == 2
        }
        @Expect(stale, true)
        @Expect(inner.get("cart-1").getOrThrow().state, "b:2")
        // 删除同样受保护
        var staleDelete = false
        try {
            storeA.delete("cart-1", 2)
        } catch (_: StaleFencingTokenException) {
            staleDelete = true
        }
        @Expect(staleDelete, true)

        // 经租约取 token：A 已失去租约，写入直接被拒绝
        var notHeld = false
        try {
            FencedDurableStateStore(inner, fence, leaseA).upsert("cart-1", 3, "a:x")
        } catch (_: LeaseNotHeldException) {
            notHeld = true
        }
        @Expect(notHeld, true)

        // 重启后（新的 fencing 存储实例）最大 token 仍是 2，旧 token 仍被拒绝
        let fence2 = FileFencingTokenStore("${dir}/fence", sync: false)
        @Expect(fence2.highestToken("shard-7"), 2)
        var staleAfterRestart = false
        try {
            FencedDurableStateStore(inner, fence2, "shard-7", { => tokenA }).upsert("cart-1", 3, "a:again")
        } catch (_: StaleFencingTokenException) {
            staleAfterRestart = true
        }
        @Expect(staleAfterRestart, true)

        // 对照：同一条旧写入绕过 fencing 直接写内层存储会成功并覆盖新所有者的数据——这正是 DA4 要堵的洞
        inner.upsert("cart-1", inner.revisionOf("cart-1") + 1, "a:stale")
        @Expect(inner.get("cart-1").getOrThrow().state, "a:stale")
        leaseB.release()
        da4Cleanup(dir)
    }

    @TestCase
    public func staleJournalWriterIsRejected(): Unit {
        let dir = da4TempDir("fence-journal")
        let inner = FileEventStore("${dir}/journal", fsync: false)
        let fence = MemoryFencingTokenStore()
        let tokenA: Option<Int64> = Some(1)
        let journalA = FencedEventStore(inner, fence, "entity-x", { => tokenA })
        @Expect(journalA.append("p1", [BasicEvent("e1", "p1", "T", "a1")], 0), 1)
        @Expect(journalA.append("p1", [BasicEvent("e2", "p1", "T", "a2")], 1), 2)
        let journalB = FencedEventStore(inner, fence, "entity-x", { => Some(2) })
        @Expect(journalB.append("p1", [BasicEvent("e3", "p1", "T", "b1")], 2), 3)
        // A 从日志恢复出最新序号后再写（序号检查通过）：只有 token 能拦住它
        var stale = false
        try {
            journalA.append("p1", [BasicEvent("e4", "p1", "T", "a3")], inner.highestSequenceNr("p1"))
        } catch (_: StaleFencingTokenException) {
            stale = true
        }
        @Expect(stale, true)
        @Expect(inner.highestSequenceNr("p1"), 3)
        var staleDelete = false
        try {
            journalA.deleteTo("p1", 3)
        } catch (_: StaleFencingTokenException) {
            staleDelete = true
        }
        @Expect(staleDelete, true)
        // 读操作不受 fencing 约束
        @Expect(journalA.replay("p1", 1, 3).size, 3)
        @Expect(fence.highestToken("entity-x"), 2)
        inner.close()
        da4Cleanup(dir)
    }
}
