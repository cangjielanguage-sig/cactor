/*
 * CActor Platform.Party × Matchmaker - 联动（F2 × F3）
 *
 * 覆盖：Party 代表全体成员提交匹配票，配对/过期时成员都收到非阻塞通知；
 * findLobbyForParty 与 requestPlayer 使用不同通知前缀。
 */
package cactor_tests.unit.platform.party

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_platform.party.*
import cactor_platform.matchmaker.*
import cactor_testkit.*

/** 成员收到的通知里是否有一条以 prefix 开头（文本含动态票 id，只能按前缀判断） */
func sawPrefix(rec: EventRecorder, prefix: String): Bool {
    for (e in rec.snapshot()) {
        if (e.startsWith(prefix)) {
            return true
        }
    }
    false
}

func attrs(region: String): HashMap<String, String> {
    let m = HashMap<String, String>()
    m["region"] = region
    m
}

@Test
public class PartyMatchmakerTest {
    @TestCase
    public func matchedTicketNotifiesAllMembers(): Unit {
        let kit = ActorTestKit("party-mm-matched")
        let party = PartyHost.start(kit.system, "party-mm-1")
        let mm = Matchmaker.create(sweepInterval: Duration.millisecond * 20)
        try {
            let p1 = PartyProbeActor()
            let p2 = PartyProbeActor()
            let r1 = kit.actorOf({ => p1 }, "party-mm-m1")
            let r2 = kit.actorOf({ => p2 }, "party-mm-m2")
            @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)
            @Expect(party.join(PartyMember.create("b", "", r2)).accepted, true)

            // Party 提交一张票，另一名玩家直接提交兼容票
            PartyMatchmaker.findLobbyForParty(mm, party, MatchTicket("t-party", "1v1", attrs("cn")),
                ttl: Duration.second * 5)
            mm.submit(MatchTicket("t-other", "1v1", attrs("cn")), ttl: Duration.second * 5)

            @Expect(awaitCondition(Duration.second * 5, { =>
                sawPrefix(p1.seen, "party.lobby.matched") && sawPrefix(p2.seen, "party.lobby.matched")
            }), true)
            // 通知里带上对手票 id
            var sawPartner = false
            for (e in p1.seen.snapshot()) {
                if (e.startsWith("party.lobby.matched") && e.contains("partner=t-other")) {
                    sawPartner = true
                }
            }
            @Expect(sawPartner, true)
        } finally {
            mm.stop()
            party.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func expiredTicketNotifiesMembers(): Unit {
        let kit = ActorTestKit("party-mm-expired")
        let party = PartyHost.start(kit.system, "party-mm-2")
        let mm = Matchmaker.create(sweepInterval: Duration.millisecond * 20)
        try {
            let p1 = PartyProbeActor()
            let r1 = kit.actorOf({ => p1 }, "party-mm-e1")
            @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)

            PartyMatchmaker.findLobbyForParty(mm, party, MatchTicket("t-solo", "solo", attrs("cn")),
                ttl: Duration.millisecond * 150)

            @Expect(awaitCondition(Duration.second * 5, { =>
                sawPrefix(p1.seen, "party.lobby.expired")
            }), true)
        } finally {
            mm.stop()
            party.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func requestPlayerUsesOwnPrefix(): Unit {
        let kit = ActorTestKit("party-mm-player")
        let party = PartyHost.start(kit.system, "party-mm-3")
        let mm = Matchmaker.create(sweepInterval: Duration.millisecond * 20)
        try {
            let p1 = PartyProbeActor()
            let r1 = kit.actorOf({ => p1 }, "party-mm-p1")
            @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)

            PartyMatchmaker.requestPlayer(mm, party, MatchTicket("t-req", "coop", attrs("eu")),
                ttl: Duration.second * 5)
            mm.submit(MatchTicket("t-peer", "coop", attrs("eu")), ttl: Duration.second * 5)

            @Expect(awaitCondition(Duration.second * 5, { =>
                sawPrefix(p1.seen, "party.player.matched")
            }), true)
        } finally {
            mm.stop()
            party.stop()
            kit.shutdown()
        }
    }
}
