/*
 * CActor Cluster - Gossip 合并在"同一地址换实例"时的性质（N4）：
 * 墓碑让迟到的旧视图不能复活旧实例；合并满足交换律、幂等律；可达性行按实例区分；编码往返保留 uid 与墓碑；
 * 被移除的实例只以墓碑存在、不留成员记录，墓碑合并取较大的移除时间（E12′）
 */
package cactor_cluster.membership

import std.collection.*
import std.sort.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_cluster.ddata.{FieldReader}

func gossipOf(ms: Array<Member>, tomb: Array<String>): Gossip {
    let t = HashMap<String, Int64>()
    for (x in tomb) {
        t[x] = 1000
    }
    gossipWithTimes(ms, t)
}

func gossipWithTimes(ms: Array<Member>, t: HashMap<String, Int64>): Gossip {
    let m = HashMap<String, Member>()
    for (x in ms) {
        m[x.address] = x
    }
    let seen = HashSet<String>()
    seen.add("h:2")
    Gossip(m, seen, HashMap<String, ReachRow>(), t)
}

func describe(g: Gossip): String {
    let keys = ArrayList<String>()
    for ((k, _) in g.members) {
        keys.add(k)
    }
    let arr = keys.toArray()
    sort(arr)
    let sb = StringBuilder()
    for (k in arr) {
        sb.append(g.members[k].toString())
        sb.append(";")
    }
    let ts = g.tombstones.keys().toArray()
    sort(ts)
    for (t in ts) {
        sb.append("+${t}@${g.tombstones[t]}")
    }
    sb.toString()
}

@Test
public class GossipIncarnationTest {
    @TestCase
    func staleViewCannotResurrectReplacedInstance(): Unit {
        let other = Member("h:2", MemberStatus.Up, 1, 5)
        // 迟到的旧视图：旧实例 #11 仍是 Up
        let stale = gossipOf([Member("h:1", MemberStatus.Up, 2, 11), other], [])
        // 新视图：旧实例已被替换（墓碑），新实例 #22 正在加入
        let fresh = gossipOf([Member("h:1", MemberStatus.Joining, 0, 22), other], ["h:1#11"])
        let ab = Gossip.merge(stale, fresh)
        let ba = Gossip.merge(fresh, stale)
        @Expect(describe(ab), describe(ba))
        let m = ab.members["h:1"]
        @Expect(m.uid, 22u64)
        @Expect(m.status, MemberStatus.Joining)
        // 新实例不继承旧实例的 upNumber
        @Expect(m.upNumber, 0)
        @Expect(ab.tombstones.contains("h:1#11"), true)
        // 幂等
        @Expect(describe(Gossip.merge(ab, ab)), describe(ab))
        @Expect(describe(Gossip.merge(ab, stale)), describe(ab))
    }

    @TestCase
    func buriedInstanceIsRemovedEvenWithoutNewRecord(): Unit {
        // 只有墓碑传到了：旧实例的记录被删除，只留墓碑（E12′：此前保留一条 Removed 记录，永不删除）
        let stale = gossipOf([Member("h:1", MemberStatus.Up, 2, 11)], [])
        let onlyTomb = gossipOf([], ["h:1#11"])
        let merged = Gossip.merge(stale, onlyTomb)
        @Expect(merged.members.contains("h:1"), false)
        @Expect(merged.tombstones.contains("h:1#11"), true)
        @Expect(describe(merged), describe(Gossip.merge(onlyTomb, stale)))
    }

    @TestCase
    func removedRecordsAreNotKeptAndTombstoneTimesMergeByMax(): Unit {
        // 旧格式里的 Removed 记录同样不保留；同一墓碑取较大的移除时间，合并仍满足交换律
        let a = gossipWithTimes([Member("h:1", MemberStatus.Removed, 2, 11), Member("h:2", MemberStatus.Up, 1, 5)],
            HashMap<String, Int64>([("h:1#11", 100), ("h:7#1", 50)]))
        let b = gossipWithTimes([Member("h:2", MemberStatus.Up, 1, 5)], HashMap<String, Int64>([("h:1#11", 300)]))
        let ab = Gossip.merge(a, b)
        @Expect(describe(ab), describe(Gossip.merge(b, a)))
        @Expect(ab.members.size, 1)
        @Expect(ab.tombstones["h:1#11"], 300)
        @Expect(ab.tombstones["h:7#1"], 50)
        @Expect(describe(Gossip.merge(ab, a)), describe(ab))
    }

    @TestCase
    func sameInstanceStillMergesByStatusRank(): Unit {
        let a = gossipOf([Member("h:1", MemberStatus.Up, 3, 11)], [])
        let b = gossipOf([Member("h:1", MemberStatus.Leaving, 2, 11)], [])
        let m = Gossip.merge(a, b).members["h:1"]
        @Expect(m.status, MemberStatus.Leaving)
        @Expect(m.upNumber, 2)
        @Expect(m.uid, 11u64)
    }

    @TestCase
    func reachabilityRowsFollowTheCurrentInstance(): Unit {
        let oldRow = HashMap<String, ReachRow>()
        let subjects = HashSet<String>()
        subjects.add("h:3")
        // 旧实例版本号 7，新实例从 1 重新计数：不能被旧行压住
        oldRow["h:1"] = ReachRow(7, subjects, 11)
        let newRow = HashMap<String, ReachRow>()
        newRow["h:1"] = ReachRow(1, HashSet<String>(), 22)
        let members = HashMap<String, Member>()
        members["h:1"] = Member("h:1", MemberStatus.Up, 4, 22)
        let rows = Gossip.mergeRows(oldRow, newRow, members)
        @Expect(rows["h:1"].uid, 22u64)
        @Expect(rows["h:1"].version, 1)
        @Expect(rows["h:1"].subjects.size, 0)
    }

    @TestCase
    func replacingAnInstanceDropsItsReachabilityRow(): Unit {
        let subjects = HashSet<String>()
        subjects.add("h:2")
        let g = gossipOf([Member("h:1", MemberStatus.Removed, 2, 11), Member("h:2", MemberStatus.Up, 1, 5)], [])
            .withRow("h:1", ReachRow(7, subjects, 11))
        @Expect(g.rows.contains("h:1"), true)
        let next = HashMap<String, Member>(g.members)
        next["h:1"] = Member("h:1", MemberStatus.Joining, 0, 22)
        let tomb = HashMap<String, Int64>([("h:1#11", 1000)])
        // 旧实例 #11 报告的"h:2 不可达"不能再算作新实例 #22 的观察
        @Expect(g.withMembers(next, "h:2", tomb).rows.contains("h:1"), false)
    }

    @TestCase
    func encodingKeepsUidsAndTombstones(): Unit {
        let g = gossipOf([Member("h:1", MemberStatus.Joining, 0, 18446744073709551615u64),
            Member("h:2", MemberStatus.Up, 1, 5)], ["h:1#11", "h:9#3"])
        let back = Gossip.decodeFrom(FieldReader(g.encode()))
        @Expect(describe(back), describe(g))
        @Expect(back.members["h:1"].uid, 18446744073709551615u64)
    }
}
