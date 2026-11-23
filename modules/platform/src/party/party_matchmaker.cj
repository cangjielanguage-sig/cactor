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
