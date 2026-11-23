/*
 * CActor Platform.Party - 管理操作（F2 扩展）验证
 *
 * 覆盖：显式转移主持人（非成员拒绝）、主持人踢人（非主持人/踢自己/目标不存在拒绝）、
 * 公开性设置与概要（成员/主持人/公开性/未撤销邀请数）；主持人离开后覆盖失效并回退。
 */
package cactor_tests.unit.platform.party

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_platform.party.*
import cactor_testkit.*

@Test
public class PartyAdminTest {
    @TestCase
    public func transferAndKickManageMembers(): Unit {
        let kit = ActorTestKit("party-admin")
        let party = PartyHost.start(kit.system, "party-admin")
        let ra = kit.actorOf({ => PartyProbeActor() }, "adm-a")
        let rb = kit.actorOf({ => PartyProbeActor() }, "adm-b")
        let rc = kit.actorOf({ => PartyProbeActor() }, "adm-c")
        try {
            @Expect(party.join(PartyMember.create("a", "host", ra)).accepted, true)
            @Expect(party.join(PartyMember.create("b", "member", rb)).accepted, true)
            @Expect(party.join(PartyMember.create("c", "member", rc)).accepted, true)
            @Expect(party.hostId(), "a")

            // 显式转移主持人
            let moved = party.transferOwnership("c")
            @Expect(moved.ok, true)
            @Expect(party.hostId(), "c")
            let badTransfer = party.transferOwnership("nobody")
            @Expect(badTransfer.ok, false)
            @Expect(badTransfer.reason, "target is not a member")

            // 踢人：非主持人、踢自己、目标不存在都拒绝
            let notHost = party.kick("a", "b")
            @Expect(notHost.ok, false)
            @Expect(notHost.reason, "only host can kick")
            let selfKick = party.kick("c", "c")
            @Expect(selfKick.ok, false)
            @Expect(selfKick.reason, "cannot kick self")
            let missing = party.kick("c", "nobody")
            @Expect(missing.ok, false)
            @Expect(missing.reason, "member not found")

            let kicked = party.kick("c", "b")
            @Expect(kicked.ok, true)
            @Expect(party.memberCount(), 2)
            var hasB = false
            for (id in party.memberIds()) {
                if (id == "b") {
                    hasB = true
                }
            }
            @Expect(hasB, false)

            // 主持人离开后覆盖失效，回退到最早加入的存活成员
            @Expect(party.leave("c"), true)
            @Expect(party.hostId(), "a")
        } finally {
            party.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func publicityAndSummary(): Unit {
        let kit = ActorTestKit("party-summary")
        let party = PartyHost.start(kit.system, "party-summary", secret: "sum-secret")
        let ra = kit.actorOf({ => PartyProbeActor() }, "sum-a")
        let rb = kit.actorOf({ => PartyProbeActor() }, "sum-b")
        try {
            @Expect(party.join(PartyMember.create("a", "host", ra)).accepted, true)
            @Expect(party.join(PartyMember.create("b", "member", rb)).accepted, true)

            let before = party.summary()
            @Expect(before.memberCount(), 2)
            @Expect(before.hostId, "a")
            @Expect(before.isPublic, false)
            @Expect(before.inviteCount, 0)

            @Expect(party.setPublicity(true), true)
            let invite = party.createInvite("a", "guest", Duration.minute * 5).invite.getOrThrow()
            let after = party.summary()
            @Expect(after.isPublic, true)
            @Expect(after.inviteCount, 1)

            @Expect(party.revokeInvite(invite.id), true)
            @Expect(party.summary().inviteCount, 0)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }
}
