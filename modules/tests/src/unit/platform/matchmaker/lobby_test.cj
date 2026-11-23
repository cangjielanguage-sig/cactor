/*
 * CActor Platform.Matchmaker - Lobby（F3 扩展）
 *
 * 覆盖：创建/加入/就绪、满员/关闭/重复加入的明确拒绝、连接状态、FindLobby 的属性与容量过滤。
 */
package cactor_tests.unit.platform.matchmaker

import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_platform.matchmaker.*

func lobbyAttrs(region: String): HashMap<String, String> {
    let m = HashMap<String, String>()
    m["region"] = region
    m
}

@Test
public class LobbyRegistryTest {
    @TestCase
    public func createJoinReadyAndFind(): Unit {
        let reg = LobbyRegistry()
        let lobby = reg.createLobby("1v1", lobbyAttrs("cn"), capacity: 2)
        @Expect(lobby.playerCount(), 0)
        @Expect(lobby.ready, false)
        @Expect(lobby.closed, false)
        @Expect(lobby.capacity, 2)

        let j1 = reg.join(lobby.id, "p1")
        @Expect(j1.ok, true)
        @Expect(j1.lobby.getOrThrow().playerCount(), 1)
        // 未满时可被 FindLobby 找到；满员后不再返回
        @Expect(reg.find("1v1", lobbyAttrs("cn")).isSome(), true)
        // 当前成员全部就绪即 ready（不要求满员）
        @Expect(reg.setReady(lobby.id, "p1").lobby.getOrThrow().ready, true)

        // 新成员加入后不再全员就绪
        let j2 = reg.join(lobby.id, "p2")
        @Expect(j2.ok, true)
        @Expect(j2.lobby.getOrThrow().ready, false)
        @Expect(reg.setReady(lobby.id, "p1").lobby.getOrThrow().ready, false)
        let allReady = reg.setReady(lobby.id, "p2").lobby.getOrThrow()
        @Expect(allReady.ready, true)
        @Expect(allReady.playerCount(), 2)

        // 满员：FindLobby 跳过，模式不同也找不到
        @Expect(reg.find("1v1", lobbyAttrs("cn")).isNone(), true)
        @Expect(reg.find("2v2", lobbyAttrs("cn")).isNone(), true)

        // 关闭后拒绝新玩家，列表中默认只剩未关闭的
        @Expect(reg.close(lobby.id).ok, true)
        let rejected = reg.join(lobby.id, "p3")
        @Expect(rejected.ok, false)
        @Expect(rejected.reason, "lobby is closed")
        @Expect(reg.list().size, 0)
        @Expect(reg.list(openOnly: false).size, 1)

        @Expect(reg.remove(lobby.id), true)
        @Expect(reg.remove(lobby.id), false)
        @Expect(reg.count(), 0)
    }

    @TestCase
    public func rejectsFullClosedDuplicateAndUnknown(): Unit {
        let reg = LobbyRegistry()
        let lobby = reg.createLobby("1v1", lobbyAttrs("cn"), capacity: 1)
        @Expect(reg.join(lobby.id, "p1").ok, true)

        let full = reg.join(lobby.id, "p2")
        @Expect(full.ok, false)
        @Expect(full.reason, "lobby is full")
        let duplicate = reg.join(lobby.id, "p1")
        @Expect(duplicate.ok, false)
        @Expect(duplicate.reason, "player already in lobby")

        let notJoined = reg.setReady(lobby.id, "p2")
        @Expect(notJoined.ok, false)
        @Expect(notJoined.reason, "player not in lobby")
        let notConnected = reg.playerConnected(lobby.id, "p2", connected: false)
        @Expect(notConnected.ok, false)

        @Expect(reg.join("no-such-lobby", "p1").reason, "lobby not found")
        @Expect(reg.close("no-such-lobby").ok, false)
        @Expect(reg.get("no-such-lobby").isNone(), true)

        // 离开后空位可再用
        @Expect(reg.leave(lobby.id, "p1").ok, true)
        @Expect(reg.join(lobby.id, "p2").ok, true)

        // 容量必须为正
        var threw = false
        try {
            reg.createLobby("1v1", lobbyAttrs("cn"), capacity: 0)
        } catch (e: IllegalArgumentException) {
            threw = true
        }
        @Expect(threw, true)
    }

    @TestCase
    public func connectionStateTracksPlayers(): Unit {
        let reg = LobbyRegistry()
        let lobby = reg.createLobby("coop", lobbyAttrs("eu"))
        @Expect(reg.join(lobby.id, "p1").ok, true)

        let offline = reg.playerConnected(lobby.id, "p1", connected: false).lobby.getOrThrow()
        @Expect(offline.players[0].connected, false)
        // 断线不改变成员关系，也不清就绪之外的字段
        @Expect(offline.playerCount(), 1)

        let online = reg.playerConnected(lobby.id, "p1", connected: true).lobby.getOrThrow()
        @Expect(online.players[0].connected, true)
    }

    @TestCase
    public func findSkipsIncompatibleFullAndClosed(): Unit {
        let reg = LobbyRegistry()
        let cn = reg.createLobby("1v1", lobbyAttrs("cn"), capacity: 1)
        @Expect(reg.find("1v1", lobbyAttrs("cn")).isSome(), true)
        @Expect(reg.find("1v1", lobbyAttrs("us")).isNone(), true)

        // 满了就不再被 FindLobby 返回
        @Expect(reg.join(cn.id, "p1").ok, true)
        @Expect(reg.find("1v1", lobbyAttrs("cn")).isNone(), true)

        // 关闭的同属性大厅也不返回，新大厅重新可见
        let other = reg.createLobby("1v1", lobbyAttrs("us"), capacity: 1)
        @Expect(reg.find("1v1", lobbyAttrs("us")).isSome(), true)
        @Expect(reg.close(other.id).ok, true)
        @Expect(reg.find("1v1", lobbyAttrs("us")).isNone(), true)
    }
}
