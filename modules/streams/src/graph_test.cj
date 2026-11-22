/*
 * CActor Streams - GraphDSL 扇出编排测试（差距 4.8）
 *
 * 覆盖：
 *   - broadcast：一个源发给 3 个汇，内容一致；
 *   - partitionBy：同 key 恒定落到同一分区；
 *   - 全部出口取消后上游被取消（无限源也能结束）。
 */
package cactor_streams

import std.collection.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor.api.CActor
import cactor_core.dsl.*
import cactor_core.typed.*

@Test
public class GraphDslTest {
    @TestCase
    public func broadcastFansOutToAllSubscribers(): Unit {
        let system = CActor.system("graph-broadcast")
        let graph = GraphDSL.broadcast<Int64, Unit, TypedPromise<Array<Int64>>>(Sources.range(1, 10),
            [Sinks.seq<Int64>(), Sinks.seq<Int64>(), Sinks.seq<Int64>()])
        let (_, mats) = graph.run(system)
        @Expect(mats.size, 3)
        for (m in mats) {
            @Expect(m.get(3.seconds), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        }
        system.terminate()
    }

    @TestCase
    public func partitionByRoutesByKey(): Unit {
        let system = CActor.system("graph-partition")
        let graph = GraphDSL.partitionBy<Int64, Unit, TypedPromise<Array<Int64>>>(Sources.range(1, 10), 2,
            { x => x % 2 }, [Sinks.seq<Int64>(), Sinks.seq<Int64>()])
        let (_, mats) = graph.run(system)
        @Expect(mats[0].get(3.seconds), [2, 4, 6, 8, 10])
        @Expect(mats[1].get(3.seconds), [1, 3, 5, 7, 9])
        system.terminate()
    }

    @TestCase
    public func allOutletsCancelStopsUpstream(): Unit {
        let system = CActor.system("graph-cancel")
        let graph = GraphDSL.broadcast<Int64, Unit, TypedPromise<Option<Int64>>>(
            Sources.iterate<Int64>(1, { x => x + 1 }),
            [Sinks.headOption<Int64>(), Sinks.headOption<Int64>()])
        let (_, mats) = graph.run(system)
        @Expect(mats[0].get(3.seconds), Some<Int64>(1))
        @Expect(mats[1].get(3.seconds), Some<Int64>(1))
        system.terminate()
    }

    @TestCase
    public func balanceDistributesAcrossSinks(): Unit {
        let system = CActor.system("graph-balance")
        let graph = GraphDSL.balance<Int64, Unit, TypedPromise<Array<Int64>>>(Sources.range(1, 10),
            [Sinks.seq<Int64>(), Sinks.seq<Int64>(), Sinks.seq<Int64>()])
        let (_, mats) = graph.run(system)
        let seen = HashSet<Int64>()
        var total: Int64 = 0
        var nonEmpty: Int64 = 0
        for (m in mats) {
            let xs = m.get(3.seconds)
            if (!xs.isEmpty()) {
                nonEmpty++
            }
            for (x in xs) {
                total++
                seen.add(x)
            }
        }
        @Expect(total, 10)
        @Expect(seen.size, 10)
        @Expect(nonEmpty, 3)
        system.terminate()
    }

    @TestCase
    public func zipPairsByPosition(): Unit {
        let system = CActor.system("graph-zip")
        let graph = GraphDSL.zip<Int64, String, Unit, Unit, TypedPromise<Array<(Int64, String)>>>(
            Sources.range(1, 5), Sources.from<String>(["a", "b", "c", "d", "e"]),
            Sinks.seq<(Int64, String)>())
        let (_, _, m) = graph.run(system)
        let pairs = m.get(3.seconds)
        @Expect(pairs.size, 5)
        let letters = ["a", "b", "c", "d", "e"]
        for (i in 0..pairs.size) {
            let (a, b) = pairs[i]
            @Expect(a, Int64(i + 1))
            @Expect(b, letters[i])
        }
        // 长度不等时按较短的结束
        let g2 = GraphDSL.zip<Int64, String, Unit, Unit, TypedPromise<Array<(Int64, String)>>>(
            Sources.range(1, 2), Sources.from<String>(["x", "y", "z"]), Sinks.seq<(Int64, String)>())
        let (_, _, m2) = g2.run(system)
        let pairs2 = m2.get(3.seconds)
        @Expect(pairs2.size, 2)
        let (a0, b0) = pairs2[0]
        let (a1, b1) = pairs2[1]
        @Expect(a0, 1)
        @Expect(b0, "x")
        @Expect(a1, 2)
        @Expect(b1, "y")
        system.terminate()
    }

    /** 汇里带 async 边界时，后续汇仍必须接在 junction 所在的岛（否则跨岛并发调用同一 junction） */
    @TestCase
    public func asyncSinksStayInJunctionIsland(): Unit {
        let system = CActor.system("graph-async-sinks")
        let graph = GraphDSL.broadcast<Int64, Unit, TypedPromise<Array<Int64>>>(Sources.range(1, 5),
            [Sinks.seq<Int64>().async(), Sinks.seq<Int64>()])
        let (_, mats) = graph.run(system)
        @Expect(mats[0].get(3.seconds), [1, 2, 3, 4, 5])
        @Expect(mats[1].get(3.seconds), [1, 2, 3, 4, 5])
        system.terminate()
    }

    /** 一侧以 async 边界结尾时，两侧仍在同一岛内配对 */
    @TestCase
    public func zipSupportsAsyncSide(): Unit {
        let system = CActor.system("graph-async-zip")
        let graph = GraphDSL.zip<Int64, String, Unit, Unit, TypedPromise<Array<(Int64, String)>>>(
            Sources.range(1, 3).async(), Sources.from<String>(["a", "b", "c"]), Sinks.seq<(Int64, String)>())
        let (_, _, m) = graph.run(system)
        let pairs = m.get(3.seconds)
        @Expect(pairs.size, 3)
        let (a0, b0) = pairs[0]
        let (a2, b2) = pairs[2]
        @Expect(a0, 1)
        @Expect(b0, "a")
        @Expect(a2, 3)
        @Expect(b2, "c")
        system.terminate()
    }
}
