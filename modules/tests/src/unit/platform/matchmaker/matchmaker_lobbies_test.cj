/*
 * CActor Platform.Matchmaker - ListLobbies 观测面（F3 扩展）验证
 *
 * 按 mode 的等待票快照：只有仍有等待票的 mode 出现；已配对的 mode 不出现。
 */
package cactor_tests.unit.platform.matchmaker

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_platform.matchmaker.*

@Test
public class MatchmakerLobbiesTest {
    @TestCase
    public func waitingLobbiesReflectPendingTickets(): Unit {
        let mm = Matchmaker.create()
        try {
            // duel 单张等待；raid 两张兼容票立即配对
            let duel = mm.submit(MatchTicket("a", "duel", HashMap<String, String>()), ttl: Duration.second * 10)
            let raid1 = mm.submit(MatchTicket("b", "raid", HashMap<String, String>()), ttl: Duration.second * 10)
            let raid2 = mm.submit(MatchTicket("c", "raid", HashMap<String, String>()), ttl: Duration.second * 10)

            @Expect(mm.waitingCount(), 1)
            @Expect(mm.waitingCountOf("duel"), 1)
            @Expect(mm.waitingCountOf("raid"), 0)

            let byMode = mm.waitingByMode()
            @Expect(byMode.get("duel"), Some<Int64>(1))
            @Expect(byMode.get("raid").isNone(), true)

            // raid 已配对：两个结果都是 Matched
            match (raid1.get(Duration.second * 3)) {
                case MatchResult.Matched(_, _) => ()
                case MatchResult.Expired(_) => @Expect("Matched", "Expired")
            }
            match (raid2.get(Duration.second * 3)) {
                case MatchResult.Matched(_, _) => ()
                case MatchResult.Expired(_) => @Expect("Matched", "Expired")
            }

            // 停止时仍在等待的 duel 以 Expired 完成，调用方不会永久挂起
            mm.stop()
            match (duel.get(Duration.second * 3)) {
                case MatchResult.Expired(_) => ()
                case MatchResult.Matched(_, _) => @Expect("Expired", "Matched")
            }
            @Expect(mm.waitingCount(), 0)
        } finally {
            mm.stop()
        }
    }
}
