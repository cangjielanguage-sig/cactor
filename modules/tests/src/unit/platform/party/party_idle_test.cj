/*
 * CActor Platform.Party - SetPartyToIdle（F2 扩展）
 *
 * 覆盖：空闲标记幂等、概要暴露 idle/idleSinceMillis、成员活动（加入/广播/离开/异步通知）自动唤醒。
 */
package cactor_tests.unit.platform.party

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_platform.party.*
import cactor_testkit.*

@Test
public class PartyIdleTest {
    @TestCase
    public func setIdleMarksPartyAndIsIdempotent(): Unit {
        let kit = ActorTestKit("party-idle-set")
        let party = PartyHost.start(kit.system, "party-idle-1")
        try {
            let probe = PartyProbeActor()
            let r1 = kit.actorOf({ => probe }, "party-idle-m1")
            @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)

            let before = party.summary()
            @Expect(before.idle, false)
            @Expect(before.idleSinceMillis, 0)

            let first = party.setIdle()
            @Expect(first.ok, true)
            @Expect(first.reason, "")
            let after = party.summary()
            @Expect(after.idle, true)
            @Expect(after.idleSinceMillis > 0, true)
            @Expect(party.isIdle(), true)

            // 幂等：再次标记成功，但明确说明已经是空闲
            let again = party.setIdle()
            @Expect(again.ok, true)
            @Expect(again.reason, "already idle")
            // 空闲时刻不被覆盖
            @Expect(party.summary().idleSinceMillis, after.idleSinceMillis)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func memberActivityWakesParty(): Unit {
        let kit = ActorTestKit("party-idle-wake")
        let party = PartyHost.start(kit.system, "party-idle-2")
        try {
            let p1 = PartyProbeActor()
            let p2 = PartyProbeActor()
            let r1 = kit.actorOf({ => p1 }, "party-idle-w1")
            let r2 = kit.actorOf({ => p2 }, "party-idle-w2")
            @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)

            // 加入唤醒
            @Expect(party.setIdle().ok, true)
            @Expect(party.isIdle(), true)
            @Expect(party.join(PartyMember.create("b", "", r2)).accepted, true)
            @Expect(party.isIdle(), false)
            @Expect(party.summary().idleSinceMillis, 0)

            // 广播唤醒
            @Expect(party.setIdle().ok, true)
            @Expect(party.broadcast("hi"), 2)
            @Expect(party.isIdle(), false)

            // 非阻塞通知唤醒（异步投递，用条件等待）
            @Expect(party.setIdle().ok, true)
            party.notify("async")
            @Expect(awaitCondition(Duration.second * 5, { => !party.isIdle() }), true)

            // 离开唤醒
            @Expect(party.setIdle().ok, true)
            @Expect(party.leave("b"), true)
            @Expect(party.isIdle(), false)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func emptyPartyCanBeIdleAndJoinWakesIt(): Unit {
        let kit = ActorTestKit("party-idle-empty")
        let party = PartyHost.start(kit.system, "party-idle-3")
        try {
            @Expect(party.isIdle(), false)
            @Expect(party.setIdle().ok, true)
            @Expect(party.isIdle(), true)
            @Expect(party.memberCount(), 0)

            let probe = PartyProbeActor()
            let r1 = kit.actorOf({ => probe }, "party-idle-e1")
            @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)
            @Expect(party.isIdle(), false)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }
}
