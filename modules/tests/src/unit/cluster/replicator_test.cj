/*
 * Copyright (c) 2024. All rights reserved.
 * CActor - R6 测试：Replicator 在三个真实节点（本机回环 TCP）间同步 CRDT
 * 覆盖：并发计数收敛、ORSet 并发增删（add 优先）收敛、LWW 寄存器、晚加入节点获得全量状态、
 * 订阅通知、30% 丢包下靠反熵收敛、编码往返
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.random.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_foundation.network.{NetworkAddress, NetworkTransport, TcpTransport, TransportProtocol}
import cactor_remote.*
import cactor_cluster.ddata.*
import cactor_testkit.*

let GOSSIP = Duration.millisecond * 50
let CONVERGE = Duration.second * 10

class DropSome <: NetworkTransport {
    private let inner = TcpTransport(NetworkAddress("127.0.0.1", 0))
    private let rate: Float64
    private let lock = Mutex()
    private let rnd: Random
    let dropped = AtomicInt64(0)

    init(rate: Float64, seed: UInt64) {
        this.rate = rate
        this.rnd = Random(seed)
    }

    public func start(): Unit { inner.start() }
    public func stop(): Unit { inner.stop() }
    public func setDataHandler(handler: (Array<UInt8>) -> Unit): Unit { inner.setDataHandler(handler) }
    public func getLocalAddress(): NetworkAddress { inner.getLocalAddress() }
    public func isActive(): Bool { inner.isActive() }

    public func send(address: NetworkAddress, data: Array<UInt8>): Unit {
        if (synchronized(lock) { rnd.nextFloat64() < rate }) {
            dropped.fetchAdd(1)
            return
        }
        inner.send(address, data)
    }
}

class Node {
    let kit: ActorTestKit
    let remote: SimpleRemoteManager
    let rep: Replicator

    init(name: String, transport: Option<NetworkTransport>) {
        kit = ActorTestKit(name)
        remote = match (transport) {
            case Some(t) => SimpleRemoteManager(t)
            case None => SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        }
        remote.start()
        rep = Replicator(kit.system, remote, GOSSIP)
    }

    func addr(): NetworkAddress {
        remote.getLocalAddress()
    }

    func shutdown(): Unit {
        rep.stop()
        remote.stop()
        kit.shutdown()
    }
}

/** 全连接 */
func mesh(nodes: Array<Node>): Unit {
    for (a in nodes) {
        for (b in nodes) {
            if (!refEq(a, b)) {
                a.rep.join(b.addr())
            }
        }
    }
}

func cluster(n: Int64, name: String): Array<Node> {
    let nodes = Array<Node>(n, { i => Node("${name}-${i}", None<NetworkTransport>) })
    mesh(nodes)
    nodes
}

func shutdownAll(nodes: Array<Node>): Unit {
    for (n in nodes) {
        n.shutdown()
    }
}

@Test
public class ReplicatorTest {
    @TestCase
    public func testConcurrentCountersConverge(): Unit {
        let nodes = cluster(3, "rep-counter")
        // 三个节点各自在独立线程中并发增减
        let futures = ArrayList<Future<Unit>>()
        for (i in 0..3) {
            let n = nodes[i]
            futures.add(spawn {
                for (_ in 0..100) {
                    n.rep.incrementCounter("hits", 2)
                }
                for (_ in 0..(10 * i)) {
                    n.rep.incrementCounter("hits", -1)
                }
            })
        }
        for (f in futures) {
            f.get()
        }
        // 600 - (0 + 10 + 20)
        for (n in nodes) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.counterValue("hits") == 570 }), true)
        }
        shutdownAll(nodes)
    }

    @TestCase
    public func testORSetConcurrentAddRemoveConverges(): Unit {
        let nodes = cluster(3, "rep-orset")
        let (a, b, c) = (nodes[0], nodes[1], nodes[2])
        a.rep.addToSet("cart", "apple")
        a.rep.addToSet("cart", "pear")
        @Expect(awaitCondition(CONVERGE, { => b.rep.setElements("cart") == ["apple", "pear"] }), true)
        // 多个节点几乎同时增删：最终所有节点得到同一个结果（真正分区下的 add 优先见 testAddWinsOverConcurrentRemove）
        b.rep.removeFromSet("cart", "pear")
        c.rep.removeFromSet("cart", "apple")
        c.rep.addToSet("cart", "apple")
        b.rep.addToSet("cart", "kiwi")
        for (n in nodes) {
            @Expect(awaitCondition(CONVERGE, { => n.rep.setElements("cart") == ["apple", "kiwi"] }), true)
        }
        shutdownAll(nodes)
    }

    @TestCase
    public func testAddWinsOverConcurrentRemove(): Unit {
        // 两个节点在不连通时各自操作，连通后合并
        let a = Node("rep-addwins-a", None<NetworkTransport>)
        let b = Node("rep-addwins-b", None<NetworkTransport>)
        a.rep.join(b.addr())
        b.rep.join(a.addr())
        a.rep.addToSet("s", "x")
        @Expect(awaitCondition(CONVERGE, { => b.rep.setElements("s") == ["x"] }), true)
        // 分区
        a.rep.leave(b.addr())
        b.rep.leave(a.addr())
        a.rep.removeFromSet("s", "x")
        b.rep.addToSet("s", "x")
        b.rep.addToSet("s", "y")
        @Expect(a.rep.setElements("s"), Array<String>())
        // 恢复
        a.rep.join(b.addr())
        b.rep.join(a.addr())
        @Expect(awaitCondition(CONVERGE, { => a.rep.setElements("s") == ["x", "y"] }), true)
        @Expect(awaitCondition(CONVERGE, { => b.rep.setElements("s") == ["x", "y"] }), true)
        a.shutdown()
        b.shutdown()
    }

    @TestCase
    public func testRegisterLastWriteWinsAndSubscription(): Unit {
        let nodes = cluster(2, "rep-lww")
        let (a, b) = (nodes[0], nodes[1])
        let seen = EventRecorder()
        b.rep.subscribe("leader", { v =>
            match (v) {
                case Register(r) => seen.record(r.value)
                case _ => ()
            }
        })
        a.rep.setRegister("leader", "node-a")
        @Expect(seen.await("node-a", CONVERGE), true)
        sleep(Duration.millisecond * 5)
        b.rep.setRegister("leader", "node-b")
        @Expect(awaitCondition(CONVERGE, { => a.rep.registerValue("leader") == Some("node-b") }), true)
        // 订阅只在值变化时触发：反熵重复送达同一个值不会重复通知
        sleep(GOSSIP * 6)
        @Expect(seen.count("node-a"), 1)
        @Expect(seen.count("node-b"), 1)
        shutdownAll(nodes)
    }

    @TestCase
    public func testLateJoinerReceivesFullState(): Unit {
        let nodes = cluster(2, "rep-late")
        for (i in 0..20) {
            nodes[0].rep.incrementCounter("k${i}", i)
        }
        nodes[1].rep.addToSet("members", "a")
        let late = Node("rep-late-new", None<NetworkTransport>)
        // 只有老节点知道新节点：反熵把全部键推给它（20 个计数器 + 从 nodes[1] 同步来的 members）
        nodes[0].rep.join(late.addr())
        @Expect(awaitCondition(CONVERGE, { => late.rep.keys().size == 21 && late.rep.counterValue("k19") == 19 }), true)
        nodes[1].rep.join(late.addr())
        @Expect(awaitCondition(CONVERGE, { => late.rep.setElements("members") == ["a"] }), true)
        late.shutdown()
        shutdownAll(nodes)
    }

    @TestCase
    public func testConvergesUnderThirtyPercentLoss(): Unit {
        let transports = Array<DropSome>(3, { i => DropSome(0.3, UInt64(42 + i)) })
        let nodes = Array<Node>(3, { i => Node("rep-lossy-${i}", Some<NetworkTransport>(transports[i])) })
        mesh(nodes)
        for (round in 0..50) {
            for (i in 0..3) {
                nodes[i].rep.incrementCounter("c", 1)
                nodes[i].rep.addToSet("s", "n${i}-${round % 5}")
            }
        }
        for (n in nodes) {
            @Expect(awaitCondition(CONVERGE * 2, { => n.rep.counterValue("c") == 150 && n.rep.setElements("s").size == 15 }), true)
        }
        var dropped: Int64 = 0
        for (t in transports) {
            dropped += t.dropped.load()
        }
        @Expect(dropped > 0, true)
        shutdownAll(nodes)
    }

    @TestCase
    public func testCodecRoundTrip(): Unit {
        var s = ORSet().add("a", "x|y:z").add("b", "中文").add("a", "").remove("x|y:z")
        let values = [
            ReplicatedValue.Counter(GCounter().increment("n1", 5).increment("n:2", 7)),
            ReplicatedValue.PNCounterValue(PNCounter().increment("n1", 10).decrement("n2", 3)),
            ReplicatedValue.GSetValue(GSet().add("1:2").add("")),
            ReplicatedValue.ORSetValue(s),
            ReplicatedValue.Register(LWWRegister<String>("v:1|2", 42, "node"))
        ]
        for (v in values) {
            let back = ReplicatedValue.decode(v.encode())
            @Expect(back.kind(), v.kind())
            @Expect(back.sameValue(v), true)
            // 合并自身幂等
            @Expect(back.merge(v).sameValue(v), true)
        }
        var mismatch = false
        try {
            values[0].merge(values[1])
        } catch (_: IllegalArgumentException) {
            mismatch = true
        }
        @Expect(mismatch, true)
        var corrupt = false
        try {
            ReplicatedValue.decode("7:GCounter|garbage")
        } catch (_: Exception) {
            corrupt = true
        }
        @Expect(corrupt, true)
        s = s.add("c", "w")
        @Expect(ReplicatedValue.decode(ReplicatedValue.ORSetValue(s).encode()).sameValue(ReplicatedValue.ORSetValue(s)), true)
    }
}
