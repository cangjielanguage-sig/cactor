/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - 分布式数据（Replicator）并发压测（E43：N0 定位与回归）
 *
 *   cactor_examples.ddata_stress [秒数=10] [节点数=3] [每节点写线程=2] [每节点读线程=2]
 *
 * 多个节点在本机回环 TCP 上全连接，gossip 间隔 20 ms：
 * - 写线程不停地对 ORSet 增删（异步与同步交替，经 Replicator Actor 串行执行 add / remove / merge）、
 *   同步递增一个共享 PNCounter、偶尔写 LWW 寄存器；
 * - 读线程同时 get / setElements / keys / stats / counterValue / registerValue；
 * - 远程状态推送、摘要反熵、差异回送在 Actor 线程里与上述读写并发。
 * 时间到后停止写入并校验：各节点每个键的摘要一致（收敛），计数器等于全部递增之和，解码失败为 0。
 *
 * 结合运行时参数切换 GC 行为（例如 cjGCInterval=1ms cjGCThreshold=4MB 让 GC 极其频繁），
 * 用于确认高分配压力 + 并发读写下没有 SIGSEGV（tools/ddata_stress.sh 连跑多轮）。
 * 退出码：0 通过，1 未收敛或有解码失败。
 */
package cactor_examples.ddata_stress

import std.sync.*
import std.time.*
import std.collection.*
import std.convert.*
import cactor.api.{CActorSystemFactory, CActorSystem}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.ddata.*

let KEYS = 12

class StressNode {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let rep: Replicator

    init(i: Int64) {
        system = CActorSystemFactory.create("ddata-stress-${i}-${MonoTime.now().hashCode()}")
        remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        remote.start()
        rep = Replicator(system, remote, Duration.millisecond * 20)
    }

    func shutdown(): Unit {
        rep.stop()
        remote.stop()
        system.terminate()
    }
}

func argOr(args: Array<String>, i: Int64, d: Int64): Int64 {
    if (args.size > i) { Int64.parse(args[i]) } else { d }
}

/** 所有节点上该键的摘要都相同（不存在的键视为不同） */
func converged(nodes: Array<StressNode>, key: String): Bool {
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

main(args: Array<String>): Int64 {
    let secs = argOr(args, 0, 10)
    let nodeCount = argOr(args, 1, 3)
    let writers = argOr(args, 2, 2)
    let readers = argOr(args, 3, 2)
    let nodes = Array<StressNode>(nodeCount, { i => StressNode(i) })
    for (a in nodes) {
        for (b in nodes) {
            if (!refEq(a, b)) {
                a.rep.join(b.remote.getLocalAddress())
            }
        }
    }

    let stop = AtomicBool(false)
    let writes = AtomicInt64(0)
    let reads = AtomicInt64(0)
    let increments = AtomicInt64(0)
    let failures = AtomicInt64(0)
    let futures = ArrayList<Future<Unit>>()
    for (n in 0..nodeCount) {
        let rep = nodes[n].rep
        for (w in 0..writers) {
            futures.add(spawn {
                var i: Int64 = 0
                while (!stop.load()) {
                    let key = "set-${(i + w) % KEYS}"
                    try {
                        // 异步与同步交替：每个写线程最多领先 Replicator 一条异步更新。
                        // 全部异步时写线程远快于 Replicator Actor，邮箱无限积压，排在后面的同步更新 10 s 超时
                        // （负载高 + 4 KB region 时实测一次），那是压测本身没有背压，不是被测对象的问题
                        if (i % 7 == 6) {
                            rep.removeFromSet(key, "n${n}-w${w}-e${(i / 7) % 50}")
                        } else if (i % 2 == 0) {
                            rep.addToSetAsync(key, "n${n}-w${w}-e${i % 50}")
                        } else {
                            rep.addToSet(key, "n${n}-w${w}-e${i % 50}")
                        }
                        if (i % 10 == 0) {
                            // 同步更新：等 Replicator Actor 执行完（也让写入速度受 Actor 吞吐约束）
                            rep.incrementCounter("hits", 1)
                            increments.fetchAdd(1)
                        }
                        if (i % 50 == 0) {
                            rep.setRegister("leader", "n${n}-w${w}-${i}")
                        }
                    } catch (e: Exception) {
                        failures.fetchAdd(1)
                        eprintln("写入失败: ${e}")
                    }
                    writes.fetchAdd(1)
                    i++
                }
            })
        }
        for (r in 0..readers) {
            futures.add(spawn {
                var i: Int64 = r
                while (!stop.load()) {
                    let key = "set-${i % KEYS}"
                    match (rep.get(key)) {
                        case Some(v) => v.digest()
                        case None => ""
                    }
                    rep.setElements(key)
                    rep.keys()
                    rep.stats()
                    rep.counterValue("hits")
                    rep.registerValue("leader")
                    reads.fetchAdd(1)
                    i++
                }
            })
        }
    }

    let start = MonoTime.now()
    while (MonoTime.now() - start < Duration.second * secs) {
        sleep(Duration.millisecond * 100)
    }
    stop.store(true)
    for (f in futures) {
        f.get()
    }

    // 收敛：所有键的摘要一致、计数器等于全部递增之和
    let keys = HashSet<String>()
    for (n in nodes) {
        keys.add(all: n.rep.keys())
    }
    let expected = increments.load()
    let t0 = MonoTime.now()
    var ok = false
    while (!ok && MonoTime.now() - t0 < Duration.second * 30) {
        ok = true
        for (k in keys) {
            if (!converged(nodes, k)) {
                ok = false
                break
            }
        }
        for (n in nodes) {
            if (n.rep.counterValue("hits") != expected) {
                ok = false
            }
        }
        if (!ok) {
            sleep(Duration.millisecond * 50)
        }
    }
    var decodeErrors: Int64 = 0
    var merges: Int64 = 0
    for (n in nodes) {
        let (_, m, d) = n.rep.stats()
        merges += m
        decodeErrors += d
    }
    println("写 ${writes.load()} 次、读 ${reads.load()} 次、远程合并 ${merges} 次、计数器递增 ${expected} 次，键 ${keys.size} 个")
    println("收敛 ${if (ok) { "是" } else { "否" }}（停止写入后 ${(MonoTime.now() - t0).toMilliseconds()} ms），解码失败 ${decodeErrors}，写入异常 ${failures.load()}")
    for (n in nodes) {
        n.shutdown()
    }
    if (ok && decodeErrors == 0 && failures.load() == 0) {
        println("ddata_stress 通过")
        0
    } else {
        println("ddata_stress 失败")
        1
    }
}
