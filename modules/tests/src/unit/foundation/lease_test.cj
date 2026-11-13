/*
 * CActor - 租约语义（DA4，cactor_foundation.coordination）
 *
 *   - 互斥：同名租约同一时刻只有一个持有者，其他人获取失败（计入 deniedCount）
 *   - token：每次成功获取严格递增；续约不改变 token；同一持有者重新获取也拿到新 token
 *   - 释放：释放后他人立即可获取；释放不触发 lost 回调
 *   - 过期：持有者停止续约（手动时钟跨过 ttl）后本地 checkLease 先失效，后端判定过期，竞争者接手；
 *     原持有者的心跳发现过期并回调 onLost（恰好一次）
 *   - 撤销：后端撤销立即回调 onLost
 *   - 后端不可用：acquire 抛 LeaseException
 * 等待异步回调一律用 awaitCondition，不用固定 sleep。
 */
package cactor_tests.unit.foundation

import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.coordination.*
import cactor_testkit.*

func leaseSettings(owner: String): LeaseSettings {
    LeaseSettings("res", owner, Duration.millisecond * 1000, Duration.millisecond * 50, Duration.millisecond * 100)
}

@Test
public class LeaseSemanticsTest {
    @TestCase
    public func exclusiveAcquireAndMonotonicTokens(): Unit {
        let store = InMemoryLeaseStore()
        let a = InMemoryLease(store, leaseSettings("a"))
        let b = InMemoryLease(store, leaseSettings("b"))
        @Expect(a.acquire({ _ => () }), true)
        @Expect(a.fencingToken(), Some(1))
        @Expect(b.acquire({ _ => () }), false)
        @Expect(b.checkLease(), false)
        @Expect(b.fencingToken(), None<Int64>)
        @Expect(store.deniedCount("res"), 1)
        // 已持有时再次 acquire：仍持有、token 不变
        @Expect(a.acquire({ _ => () }), true)
        @Expect(a.fencingToken(), Some(1))
        // 释放后他人立即可获取，token 递增
        @Expect(a.release(), true)
        @Expect(a.checkLease(), false)
        @Expect(a.release(), false)
        @Expect(b.acquire({ _ => () }), true)
        @Expect(b.fencingToken(), Some(2))
        @Expect(b.release(), true)
        // 同一持有者再次获取也是新 token（重启后的实例必须能 fence 掉旧实例在途的写入）
        @Expect(a.acquire({ _ => () }), true)
        @Expect(a.fencingToken(), Some(3))
        @Expect(store.lastToken("res"), 3)
        a.release()
        // 主动释放从不触发 lost
        @Expect(a.lostCount() + b.lostCount(), 0)
    }

    @TestCase
    public func heartbeatKeepsLeaseAliveWithSameToken(): Unit {
        let now = AtomicInt64(0)
        let store = InMemoryLeaseStore({ => now.load() })
        let a = InMemoryLease(store, leaseSettings("a"))
        @Expect(a.acquire({ _ => () }), true)
        // 时间分几步推进，总量远超 ttl；每一步都小于有效期，且等心跳把到期时间续上后再推进
        for (_ in 0..5) {
            now.fetchAdd(400)
            let target = now.load() + 1000
            // 续约后后端到期时间 = 当前时刻 + ttl
            @Expect(awaitCondition(Duration.second * 5, { => store.owner("res") == Some("a") && renewedTo(store, target) }),
                true)
        }
        @Expect(a.checkLease(), true)
        @Expect(a.fencingToken(), Some(1))
        @Expect(a.lostCount(), 0)
        a.release()
    }

    @TestCase
    public func expiryHandsOverAndNotifiesOldHolderOnce(): Unit {
        let now = AtomicInt64(0)
        let store = InMemoryLeaseStore({ => now.load() })
        let a = InMemoryLease(store, leaseSettings("a"))
        let b = InMemoryLease(store, leaseSettings("b"))
        let lost = AtomicInt64(0)
        @Expect(a.acquire({ _ => lost.fetchAdd(1) }), true)
        // 模拟持有者长停顿：时间一次跨过 ttl，期间没有续约机会
        now.fetchAdd(1500)
        // 本地视角立即失效（单调时钟已过 validUntil），不必等心跳
        @Expect(a.checkLease(), false)
        @Expect(a.fencingToken(), None<Int64>)
        @Expect(store.owner("res"), None<String>)
        @Expect(b.acquire({ _ => () }), true)
        @Expect(b.fencingToken(), Some(2))
        @Expect(awaitCondition(Duration.second * 5, { => lost.load() == 1 }), true)
        // 心跳在 lost 之后退出：再推进时间也不会重复回调；b 仍是唯一持有者
        @Expect(awaitCondition(Duration.second * 5, { => store.owner("res") == Some("b") }), true)
        @Expect(lost.load(), 1)
        @Expect(a.lostCount(), 1)
        b.release()
    }

    @TestCase
    public func revokeNotifiesHolderImmediately(): Unit {
        let store = InMemoryLeaseStore()
        let a = InMemoryLease(store, leaseSettings("a"))
        let reason = AtomicBool(false)
        @Expect(a.acquire({ r => reason.store(r.isSome()) }), true)
        store.revoke("res")
        @Expect(awaitCondition(Duration.second * 5, { => a.lostCount() == 1 }), true)
        @Expect(reason.load(), true)
        @Expect(a.checkLease(), false)
    }

    @TestCase
    public func unavailableStoreFailsAcquire(): Unit {
        let store = InMemoryLeaseStore()
        let a = InMemoryLease(store, leaseSettings("a"))
        store.setAvailable(false)
        var threw = false
        try {
            a.acquire({ _ => () })
        } catch (_: LeaseException) {
            threw = true
        }
        @Expect(threw, true)
        @Expect(a.checkLease(), false)
        store.setAvailable(true)
        @Expect(a.acquire({ _ => () }), true)
        a.release()
    }

    @TestCase
    public func settingsRejectUnsafeTimings(): Unit {
        var rejected = 0
        // 心跳不小于 ttl - 余量：到期前没有续约机会
        try {
            LeaseSettings("r", "o", Duration.second, Duration.millisecond * 950, Duration.millisecond * 100)
        } catch (_: IllegalArgumentException) {
            rejected++
        }
        // 余量不小于 ttl：本地有效期为零
        try {
            LeaseSettings("r", "o", Duration.second, Duration.millisecond * 10, Duration.second)
        } catch (_: IllegalArgumentException) {
            rejected++
        }
        @Expect(rejected, 2)
        let s = LeaseSettings("r", "o").withTtl(Duration.second * 20).withOwnerName("p")
        @Expect(s.ownerName, "p")
        @Expect(s.validityMillis(), 19000)
    }
}

/** 后端记录的到期时间是否已续到 target（心跳已在当前手动时刻完成一次续约） */
func renewedTo(store: InMemoryLeaseStore, target: Int64): Bool {
    store.expiresAt("res") >= target
}
