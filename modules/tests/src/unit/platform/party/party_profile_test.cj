/*
 * CActor Platform.Party - Profile / SelfProfile / SelfSummary（F2 扩展）
 *
 * 覆盖：GetPartyProfile 的公开元信息、GetPartySelfProfile 的成员身份（含主持人转移）、
 * GetPartySelfSummary 的"概要 + 自己"组合，以及非成员返回空。
 */
package cactor_tests.unit.platform.party

import std.unittest.*
import std.unittest.testmacro.*
import cactor_platform.party.*
import cactor_testkit.*

@Test
public class PartyProfileTest {
    @TestCase
    public func profileReflectsPartyState(): Unit {
        let kit = ActorTestKit("party-profile")
        let party = PartyHost.start(kit.system, "party-prof-1")
        try {
            let empty = party.profile()
            @Expect(empty.path.contains("party-prof-1"), true)
            @Expect(empty.hostId, "")
            @Expect(empty.memberCount, 0)
            @Expect(empty.isPublic, false)
            @Expect(empty.idle, false)
            @Expect(empty.createdAtMillis > 0, true)

            let p1 = PartyProbeActor()
            let p2 = PartyProbeActor()
            let r1 = kit.actorOf({ => p1 }, "party-prof-m1")
            let r2 = kit.actorOf({ => p2 }, "party-prof-m2")
            @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)
            @Expect(party.join(PartyMember.create("b", "", r2)).accepted, true)
            @Expect(party.setPublicity(true), true)

            let full = party.profile()
            @Expect(full.memberCount, 2)
            @Expect(full.hostId, "a")
            @Expect(full.isPublic, true)
            // 创建时间不随活动变化
            @Expect(full.createdAtMillis, empty.createdAtMillis)

            @Expect(party.setIdle().ok, true)
            @Expect(party.profile().idle, true)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func selfProfileAndSummary(): Unit {
        let kit = ActorTestKit("party-self")
        let party = PartyHost.start(kit.system, "party-prof-2")
        try {
            let p1 = PartyProbeActor()
            let p2 = PartyProbeActor()
            let r1 = kit.actorOf({ => p1 }, "party-prof-s1")
            let r2 = kit.actorOf({ => p2 }, "party-prof-s2")
            @Expect(party.join(PartyMember.create("a", "host", r1)).accepted, true)
            @Expect(party.join(PartyMember.create("b", "member", r2)).accepted, true)

            let a = party.selfProfile("a").getOrThrow()
            @Expect(a.memberId, "a")
            @Expect(a.role, "host")
            @Expect(a.joinedAtMillis > 0, true)
            @Expect(a.isHost, true)

            let b = party.selfProfile("b").getOrThrow()
            @Expect(b.role, "member")
            @Expect(b.isHost, false)

            @Expect(party.selfProfile("nobody").isNone(), true)

            let selfSummary = party.selfSummary("a")
            @Expect(selfSummary.summary.memberIds.size, 2)
            @Expect(selfSummary.self.isSome(), true)
            @Expect(selfSummary.self.getOrThrow().memberId, "a")

            // 非成员：概要仍可读，self 为空
            let outsider = party.selfSummary("nobody")
            @Expect(outsider.summary.memberIds.size, 2)
            @Expect(outsider.self.isNone(), true)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func selfProfileTracksHostTransfer(): Unit {
        let kit = ActorTestKit("party-self-host")
        let party = PartyHost.start(kit.system, "party-prof-3")
        try {
            let p1 = PartyProbeActor()
            let p2 = PartyProbeActor()
            let r1 = kit.actorOf({ => p1 }, "party-prof-h1")
            let r2 = kit.actorOf({ => p2 }, "party-prof-h2")
            @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)
            @Expect(party.join(PartyMember.create("b", "", r2)).accepted, true)
            @Expect(party.selfProfile("a").getOrThrow().isHost, true)

            @Expect(party.transferOwnership("b").ok, true)
            @Expect(party.selfProfile("b").getOrThrow().isHost, true)
            @Expect(party.selfProfile("a").getOrThrow().isHost, false)
            @Expect(party.profile().hostId, "b")
        } finally {
            party.stop()
            kit.shutdown()
        }
    }
}
