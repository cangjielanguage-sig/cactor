/*
 * CActor Platform.Party × Matchmaker Lobby - JoinMatchmakerLobbyForParty（F2 × F3）
 *
 * 覆盖：Party 全员加入大厅并广播结果；大厅容量不足时回滚，不留下半个 Party。
 */
package cactor_tests.unit.platform.party

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_platform.party.*
import cactor_platform.matchmaker.*
import cactor_testkit.*

func lobbyAttrsOf(region: String): HashMap<String, String> {
    let m = HashMap<String, String>()
    m["region"] = region
    m
}

@Test
public class PartyLobbyTest {
    @TestCase
    public func joinLobbyForPartyAddsAllMembers(): Unit {
        let kit = ActorTestKit("party-lobby-join")
        let party = PartyHost.start(kit.system, "party-lobby-1")
        try {
            let p1 = PartyProbeActor()
            let p2 = PartyProbeActor()
            let r1 = kit.actorOf({ => p1 }, "party-lobby-m1")
            let r2 = kit.actorOf({ => p2 }, "party-lobby-m2")
            @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)
            @Expect(party.join(PartyMember.create("b", "", r2)).accepted, true)

            let reg = LobbyRegistry()
            let lobby = reg.createLobby("1v1", lobbyAttrsOf("cn"), capacity: 3)
            let out = PartyMatchmaker.joinLobbyForParty(reg, party, lobby.id)
            @Expect(out.ok, true)
            @Expect(out.lobby.getOrThrow().playerCount(), 2)

            var sawA = false
            var sawB = false
            for (player in out.lobby.getOrThrow().players) {
                if (player.playerId == "a") { sawA = true }
                if (player.playerId == "b") { sawB = true }
            }
            @Expect(sawA, true)
            @Expect(sawB, true)

            @Expect(awaitCondition(Duration.second * 5, { =>
                sawPrefix(p1.seen, "party.lobby.joined") && sawPrefix(p2.seen, "party.lobby.joined")
            }), true)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }

    @TestCase
    public func joinLobbyForPartyRollsBackOnFull(): Unit {
        let kit = ActorTestKit("party-lobby-rollback")
        let party = PartyHost.start(kit.system, "party-lobby-2")
        try {
            let p1 = PartyProbeActor()
            let p2 = PartyProbeActor()
            let r1 = kit.actorOf({ => p1 }, "party-lobby-r1")
            let r2 = kit.actorOf({ => p2 }, "party-lobby-r2")
            @Expect(party.join(PartyMember.create("a", "", r1)).accepted, true)
            @Expect(party.join(PartyMember.create("b", "", r2)).accepted, true)

            // 容量只够一个人：第二个成员加入失败后，第一个应被回滚
            let reg = LobbyRegistry()
            let lobby = reg.createLobby("1v1", lobbyAttrsOf("cn"), capacity: 1)
            let out = PartyMatchmaker.joinLobbyForParty(reg, party, lobby.id)
            @Expect(out.ok, false)
            @Expect(out.reason, "lobby is full")
            @Expect(reg.get(lobby.id).getOrThrow().playerCount(), 0)

            @Expect(awaitCondition(Duration.second * 5, { =>
                sawPrefix(p1.seen, "party.lobby.join_failed") &&
                sawPrefix(p2.seen, "party.lobby.join_failed")
            }), true)
        } finally {
            party.stop()
            kit.shutdown()
        }
    }
}
