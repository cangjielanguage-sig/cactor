/*
 * CActor Platform.Matchmaker - 按属性匹配（F3 / AR9）
 *
 * 覆盖：兼容属性配对、属性冲突不配对、TTL 到期 Expired、并发提交下每张票只匹配一次。
 */
package cactor_tests.unit.platform.matchmaker

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.typed.{TypedPromise}
import cactor_platform.matchmaker.*

func attrsOf(pairs: Array<(String, String)>): HashMap<String, String> {
    let m = HashMap<String, String>()
    for ((k, v) in pairs) {
        m[k] = v
    }
    m
}

@Test
public class MatchmakerTest {
    @TestCase
    public func matchmakerPairsCompatibleTickets(): Unit {
        let maker = Matchmaker.create()
        let t1 = MatchTicket("t1", "duel", attrsOf([("region", "cn"), ("level", "gold")]))
        let t2 = MatchTicket("t2", "duel", attrsOf([("region", "cn"), ("team", "red")]))
        let p1 = maker.submit(t1, ttl: Duration.second * 2)
        let p2 = maker.submit(t2, ttl: Duration.second * 2)
        match (p1.get(Duration.second * 2)) {
            case Matched(a, b) =>
                let ids = HashSet<String>()
                ids.add(a.id)
                ids.add(b.id)
                @Expect(ids.contains("t1"), true)
                @Expect(ids.contains("t2"), true)
            case Expired(_) => @Fail("兼容的票应匹配")
        }
        match (p2.get(Duration.second * 2)) {
            case Matched(_, _) => ()
            case Expired(_) => @Fail("兼容的票应匹配")
        }
        // 属性冲突：不应与已存在的票匹配，超时 Expired
        let p3 = maker.submit(MatchTicket("t3", "duel", attrsOf([("level", "silver")])), ttl: Duration.millisecond * 150)
        match (p3.get(Duration.second * 2)) {
            case Expired(t) => @Expect(t.id, "t3")
            case Matched(_, _) => @Fail("属性冲突不应匹配")
        }
        maker.stop()
    }

    @TestCase
    public func matchmakerExpiresUnmatchedTicket(): Unit {
        let maker = Matchmaker.create()
        let p = maker.submit(MatchTicket("solo", "duel", attrsOf([("region", "cn")])),
            ttl: Duration.millisecond * 150)
        match (p.get(Duration.second * 2)) {
            case Expired(t) => @Expect(t.id, "solo")
            case Matched(_, _) => @Fail("单张票不应匹配")
        }
        @Expect(maker.waitingCount(), 0)
        maker.stop()
    }

    @TestCase
    public func matchmakerHandlesConcurrentSubmissions(): Unit {
        let maker = Matchmaker.create()
        let total = 200
        let workers = 8
        var futures = ArrayList<Future<Array<TypedPromise<MatchResult>>>>()
        for (w in 0..workers) {
            let start = w * (total / workers)
            let end = if (w == workers - 1) { total } else { start + (total / workers) }
            futures.add(spawn {
                let ps = ArrayList<TypedPromise<MatchResult>>()
                for (i in start..end) {
                    let group = if (i % 2 == 0) { "a" } else { "b" }
                    let ticket = MatchTicket("t${i}", "duel", attrsOf([("group", group)]))
                    ps.add(maker.submit(ticket, ttl: Duration.second * 5))
                }
                ps.toArray()
            })
        }
        let promises = ArrayList<TypedPromise<MatchResult>>()
        for (f in futures) {
            for (p in f.get()) {
                promises.add(p)
            }
        }
        var matched = 0
        let ids = HashSet<String>()
        for (p in promises) {
            match (p.get(Duration.second * 5)) {
                case Matched(a, b) =>
                    matched++
                    ids.add(a.id)
                    ids.add(b.id)
                case Expired(_) => @Fail("并发提交的票应在 TTL 内匹配")
            }
        }
        @Expect(matched, total)
        @Expect(Int64(ids.size), Int64(total))
        @Expect(maker.waitingCount(), 0)
        maker.stop()
    }
}
