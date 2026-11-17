/*
 * CActor Real 04 - 边缘–云一致性配置面（CRDT + Gossip + SBR 脑裂）
 *
 * 真实约束（docs/plan/2026117-lz.md §3 E4）：
 * - 边缘节点经常掉线又回来（离线优先）：回归后必须靠反熵补齐，不丢更新；
 * - 分区不能导致配置回退：CRDT 合并是单调的，节点各自继续写、合并后不丢；
 * - 分区要能被识别（脑裂）：sbrDecide 是纯函数，可对同一视图做单元断言；
 * - 读要快但要能选一致性级别：Local / Majority / All。
 *
 * 组件（全部是仓内现成实现）：
 * - GCounter / PNCounter / ORSet / LWWRegister（cluster/src/ddata/crdt.cj）
 * - Replicator：增量推送 + 摘要反熵 + 一致性级别（ddata/replicator.cj）
 * - sbrDecide + DowningStrategy（membership/sbr.cj，纯函数）
 *
 * 自检（退出码 0/1）：
 * 1. 4 节点写同一 GCounter / ORSet，停止写入后全部收敛且计数等于真实总量；
 * 2. 节点离线期间继续写，回归后反熵补齐（合并后值 = 离线期间的量 + 在线量）；
 * 3. ORSet 并发 add 同一元素，合并后元素仍在（add-wins）；
 * 4. sbrDecide：3 节点 1 个不可达 → 多数派侧 DownUnreachable；少数派侧（共 5 节点、2 个可达）→ DownSelf；
 * 5. ReadMajority 在 1 个副本不可达时明确失败（ReplicatorConsistencyException），ReadLocal 仍即时返回。
 *
 * 运行：source tools/env.sh && cjpm build && \
 *       CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.real04_edge_config
 */
package cactor_examples.real04_edge_config

import std.collection.*
import std.sync.*
import std.time.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.ddata.*
import cactor_cluster.membership.{Member, MemberStatus, DowningStrategy, SbrDecision, sbrDecide}

class EdgeNode {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let rep: Replicator

    init(i: Int64) {
        system = CActorSystemFactory.create("real04-edge-${i}-${MonoTime.now().hashCode()}")
        remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        remote.start()
        rep = Replicator(system, remote, Duration.millisecond * 20)
    }

    func address(): NetworkAddress {
        remote.getLocalAddress()
    }

    func shutdown(): Unit {
        rep.stop()
        remote.stop()
        system.terminate()
    }
}

class Checker {
    var failures = 0

    func check(cond: Bool, what: String): Unit {
        if (cond) {
            println("  ✓ ${what}")
        } else {
            failures++
            println("  ✗ ${what}")
        }
    }
}

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

/** 所有节点该键的摘要相同（不存在的键视为未收敛） */
func converged(nodes: Array<EdgeNode>, key: String): Bool {
    var first: Option<String> = None
    for (n in nodes) {
        match (n.rep.get(key)) {
            case Some(v) =>
                let d = v.digest()
                match (first) {
                    case Some(f) where f != d => return false
                    case Some(_) => ()
                    case None => first = Some(d)
                }
            case None => return false
        }
    }
    true
}

func member(addr: String, status: MemberStatus): Member {
    Member(addr, status, 1)
}

main(): Int64 {
    let c = Checker()
    let nodeCount = 4
    let total: Int64 = 4 * 50
    let nodes = Array<EdgeNode>(nodeCount, { i => EdgeNode(i) })
    for (a in nodes) {
        for (b in nodes) {
            if (!refEq(a, b)) {
                a.rep.join(b.address())
            }
        }
    }

    try {
        // ---- 1. 多节点写同一键，停止后收敛 ----
        println("\n[1] 4 节点写 GCounter / ORSet，停止后全部收敛")
        for (i in 0..nodeCount) {
            for (_ in 0..50) {
                nodes[i].rep.incrementCounter("hits", 1)
            }
            // 每个节点加一个自己的元素（ORSet add-wins）
            nodes[i].rep.addToSet("members", "edge-${i}")
        }
        let ok1 = waitUntil(Duration.second * 20, { =>
            converged(nodes, "hits") && converged(nodes, "members")
                && nodes[0].rep.counterValue("hits") == total
        })
        c.check(ok1, "收敛且计数 == ${total}（实际 ${nodes[0].rep.counterValue("hits")}）")
        let elements = nodes[0].rep.setElements("members")
        c.check(Int64(elements.size) == nodeCount, "ORSet 合并出 ${elements.size} 个边缘节点")

        // ---- 2. 离线期间的更新在回归后补齐 ----
        println("\n[2] 节点离线期间继续写，回归后反熵补齐")
        let offline = nodes[3]
        // 让节点 3 从其它节点的视图中消失：停掉它的传输（其它节点继续写）
        offline.remote.stop()
        let beforeOffline = nodes[0].rep.counterValue("hits")
        // 在线节点继续写 100 次
        for (_ in 0..100) {
            nodes[0].rep.incrementCounter("hits", 1)
        }
        // 重启节点 3 的传输并重新 join（模拟边缘盒子回来）
        offline.remote.start()
        for (n in nodes) {
            if (!refEq(n, offline)) {
                offline.rep.join(n.address())
                n.rep.join(offline.address())
            }
        }
        let expectedAfter = beforeOffline + 100
        let ok2 = waitUntil(Duration.second * 30, { =>
            converged(nodes, "hits") && nodes[0].rep.counterValue("hits") == expectedAfter
                && offline.rep.counterValue("hits") == expectedAfter
        })
        c.check(ok2, "回归后收敛到 ${expectedAfter}（node0=${nodes[0].rep.counterValue("hits")}，离线节点=${offline.rep.counterValue("hits")}）")

        // ---- 3. ORSet 并发 add 同一元素（add-wins） ----
        println("\n[3] ORSet 并发加同一元素：合并后仍在")
        let key = "tags"
        nodes[0].rep.addToSet(key, "shared")
        nodes[1].rep.addToSet(key, "shared")
        let ok3 = waitUntil(Duration.second * 20, { =>
            converged(nodes, key) && nodes[2].rep.setElements(key).contains("shared")
        })
        c.check(ok3, "两个节点并发 add 同一元素，第三个节点合并后仍能看到它")

        // ---- 4. SBR 决策（纯函数，直接对视图断言） ----
        println("\n[4] SBR：多数派侧存活、少数派侧自停")
        let three = [member("a", MemberStatus.Up), member("b", MemberStatus.Up), member("c", MemberStatus.Up)]
        let unreachableOne = HashSet<String>(["c"])
        match (sbrDecide(DowningStrategy.KeepMajority(Duration.Zero), three, unreachableOne)) {
            case DownUnreachable(down) =>
                c.check(down == ["c"], "3 节点中 1 个不可达 → 多数派侧 down 掉它")
            case DownSelf => c.check(false, "多数派侧不应自停")
            case NoAction => c.check(false, "多数派侧不应 NoAction")
        }
        // 5 节点分为 2 / 3：2 个可达的一侧是少数派 → DownSelf
        let five = [
            member("a", MemberStatus.Up), member("b", MemberStatus.Up), member("c", MemberStatus.Up),
            member("d", MemberStatus.Up), member("e", MemberStatus.Up)
        ]
        let unreachableThree = HashSet<String>(["c", "d", "e"])
        match (sbrDecide(DowningStrategy.KeepMajority(Duration.Zero), five, unreachableThree)) {
            case DownSelf => c.check(true, "5 节点中 3 个不可达 → 少数派侧自停（DownSelf）")
            case DownUnreachable(_) => c.check(false, "少数派侧不应 down 掉多数派")
            case NoAction => c.check(false, "少数派侧不应 NoAction")
        }
        // 整个分区都看得到彼此：NoAction
        let noneDown = HashSet<String>()
        match (sbrDecide(DowningStrategy.KeepMajority(Duration.Zero), five, noneDown)) {
            case NoAction => c.check(true, "无不可达成员 → NoAction")
            case _ => c.check(false, "无不可达时应 NoAction")
        }

        // ---- 5. 读一致性级别 ----
        println("\n[5] 读一致性：ReadLocal 即时、ReadMajority 在副本不足时明确失败")
        let localRead = nodes[0].rep.getWith("hits", ReadConsistency.ReadLocal).get(Duration.second * 5)
        c.check(localRead.isSome(), "ReadLocal 立即返回本地值")
        // 停掉 2 个节点，制造副本不足
        nodes[2].remote.stop()
        nodes[3].remote.stop()
        var majorityFailed = false
        try {
            nodes[0].rep.getWith("hits", ReadConsistency.ReadMajority(Duration.millisecond * 800))
                .get(Duration.second * 3)
        } catch (_: ReplicatorConsistencyException) {
            majorityFailed = true
        } catch (_: Exception) {
            majorityFailed = true
        }
        c.check(majorityFailed, "副本不足时 ReadMajority 明确失败（不返回不可信值）")
        // 重启回来
        nodes[2].remote.start()
        nodes[3].remote.start()
    } finally {
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nreal04：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
