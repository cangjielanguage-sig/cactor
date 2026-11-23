/*
 * CActor Platform.Party - 邀请（F2 扩展）验证
 *
 * 覆盖：成员签发邀请 → 按邀请查询/加入；撤销后不可加入且幂等返回 false；
 * 邀请过期后拒绝；未配置密钥时明确失败；非成员不能签发。
 */
package cactor_tests.unit.platform.party

import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_platform.party.*
import cactor_testkit.*

@Test
public class PartyInviteTest {
    @TestCase
    public func inviteJoinRevokeAndExpire(): Unit {
        let kit = ActorTestKit("party-invite")
        let party = PartyHost.start(kit.system, "party-inv", secret: "party-secret-123")
        let hostRef = kit.actorOf({ => PartyProbeActor() }, "inv-host")
        let guestRef = kit.actorOf({ => PartyProbeActor() }, "inv-guest")
        let lateRef = kit.actorOf({ => PartyProbeActor() }, "inv-late")
        try {
            @Expect(party.join(PartyMember.create("host", "host", hostRef)).accepted, true)

            let created = party.createInvite("host", "guest-1", Duration.minute * 5)
            @Expect(created.invite.isSome(), true)
            let invite = created.invite.getOrThrow()
            @Expect(invite.partyPath.size > 0, true)
            // GetPartyFromInvite：有效邀请能查到 Party 路径
            @Expect(party.partyOfInvite(invite.id).isSome(), true)

            // SendJoinRequest：按邀请加入
            let joined = party.joinByInvite(invite.id, PartyMember.create("guest", "member", guestRef))
            @Expect(joined.accepted, true)
            @Expect(party.memberCount(), 2)

            // RevokePartyInvite：撤销后不可再加入，且重复撤销返回 false
            @Expect(party.revokeInvite(invite.id), true)
            @Expect(party.revokeInvite(invite.id), false)
            @Expect(party.partyOfInvite(invite.id).isNone(), true)
            let rejected = party.joinByInvite(invite.id, PartyMember.create("late", "member", lateRef))
            @Expect(rejected.accepted, false)
            @Expect(rejected.reason.contains("revoked"), true)
            @Expect(party.memberCount(), 2)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func inviteExpiresAndRequiresMember(): Unit {
        let kit = ActorTestKit("party-invite-exp")
        let party = PartyHost.start(kit.system, "party-inv2", secret: "s3")
        let hostRef = kit.actorOf({ => PartyProbeActor() }, "exp-host")
        let guestRef = kit.actorOf({ => PartyProbeActor() }, "exp-guest")
        try {
            // 非成员不能签发
            let denied = party.createInvite("nobody", "x", Duration.minute)
            @Expect(denied.invite.isNone(), true)
            @Expect(denied.reason, "inviter is not a member")

            @Expect(party.join(PartyMember.create("host", "host", hostRef)).accepted, true)
            let created = party.createInvite("host", "g", Duration.millisecond * 60)
            @Expect(created.invite.isSome(), true)
            let inviteId = created.invite.getOrThrow().id

            sleep(Duration.millisecond * 160)
            @Expect(party.partyOfInvite(inviteId).isNone(), true)
            let expired = party.joinByInvite(inviteId, PartyMember.create("g", "member", guestRef))
            @Expect(expired.accepted, false)
            @Expect(expired.reason.contains("expired"), true)
            @Expect(party.memberCount(), 1)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func invitesDisabledWithoutSecret(): Unit {
        let kit = ActorTestKit("party-nosecret")
        let party = PartyHost.start(kit.system, "party-nosecret")
        let hostRef = kit.actorOf({ => PartyProbeActor() }, "ns-host")
        try {
            @Expect(party.join(PartyMember.create("host", "host", hostRef)).accepted, true)
            let r = party.createInvite("host", "x", Duration.minute)
            @Expect(r.invite.isNone(), true)
            @Expect(r.reason.contains("disabled"), true)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }
}
