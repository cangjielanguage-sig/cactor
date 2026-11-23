/*
 * CActor Platform.Party - Party 与 Matchmaker 联动（F2 × F3）
 *
 * 对标 Rivet 的 FindMatchmakerLobbyForParty / RequestMatchmakerPlayer：
 * 由 Party 代表全体成员提交一张匹配票，匹配完成（配对/过期/失败）后把结果
 * 非阻塞地广播给所有成员，调用方无需轮询。
 *
 * 只做胶水：票的排队与配对仍由 Matchmaker 负责，通知复用 PartyHost.notify 的 tell 通道，
 * 因此不会在匹配回调线程上阻塞，也不会拖慢匹配本身。
 */
package cactor_platform.party

import std.collection.*
import std.time.*
import cactor_core.typed.{AskOutcome, TypedPromise}
import cactor_platform.matchmaker.*

/** Party 与 Matchmaker 的联动入口 */
public class PartyMatchmaker {
    /**
     * 为 Party 找一个匹配大厅（FindMatchmakerLobbyForParty）：提交票，结果广播给全体成员。
     * 返回的 Promise 与票本身一致，调用方也可自行等待。
     */
    public static func findLobbyForParty(mm: Matchmaker, party: PartyHost, ticket: MatchTicket,
        ttl!: Duration = Duration.second * 5): TypedPromise<MatchResult> {
        notifyOnResult(mm.submit(ticket, ttl: ttl), party, "party.lobby", ticket.mode)
    }

    /** 为 Party 请求一名匹配玩家（RequestMatchmakerPlayer）：语义同上，通知前缀不同便于区分来源 */
    public static func requestPlayer(mm: Matchmaker, party: PartyHost, ticket: MatchTicket,
        ttl!: Duration = Duration.second * 5): TypedPromise<MatchResult> {
        notifyOnResult(mm.submit(ticket, ttl: ttl), party, "party.player", ticket.mode)
    }

    /**
     * Party 整体加入一个已存在的大厅（JoinMatchmakerLobbyForParty）：每个成员作为一个玩家加入。
     * 任一成员加入失败则回滚已加入的成员（大厅保持加入前的状态），并把原因广播给 Party。
     */
    public static func joinLobbyForParty(registry: LobbyRegistry, party: PartyHost,
        lobbyId: String): LobbyOutcome {
        let members = party.memberIds()
        if (members.size == 0) {
            let out = LobbyOutcome(false, "party has no members", None<LobbyInfo>)
            party.notify("party.lobby.join_failed lobby=${lobbyId} reason=${out.reason}")
            return out
        }
        let joined = ArrayList<String>()
        var last = LobbyOutcome(false, "lobby not found", None<LobbyInfo>)
        for (memberId in members) {
            let r = registry.join(lobbyId, memberId)
            if (!r.ok) {
                // 回滚：组队加入要么全进、要么都不进，避免大厅里留下半个 Party
                for (id in joined) {
                    registry.leave(lobbyId, id)
                }
                party.notify("party.lobby.join_failed lobby=${lobbyId} player=${memberId} reason=${r.reason}")
                return r
            }
            joined.add(memberId)
            last = r
        }
        match (last.lobby) {
            case Some(info) =>
                party.notify("party.lobby.joined lobby=${lobbyId} players=${info.playerCount()}")
            case None => ()
        }
        last
    }

    /** 在票完成时按 kind 前缀广播结果；回调可能在匹配线程上执行，notify 只用 tell 不阻塞 */
    private static func notifyOnResult(promise: TypedPromise<MatchResult>, party: PartyHost,
        kind: String, mode: String): TypedPromise<MatchResult> {
        promise.onComplete({ outcome =>
            match (outcome) {
                case Replied(r) =>
                    match (r) {
                        case Matched(me, partner) =>
                            party.notify("${kind}.matched id=${me.id} mode=${mode} partner=${partner.id}")
                        case Expired(t) =>
                            party.notify("${kind}.expired id=${t.id} mode=${mode}")
                    }
                case Failed(e) =>
                    party.notify("${kind}.failed mode=${mode} reason=${e.message}")
            }
            ()
        })
        promise
    }
}
