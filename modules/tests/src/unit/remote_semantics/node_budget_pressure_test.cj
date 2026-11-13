/* CActor Remote - 全节点预算的慢消费者 / 重连压力矩阵（DA3）。
 * 真实 TCP：
 *   1. 不读不答的对端（握手一直超时、反复重连）+ 多通道洪泛：出站字节与未确认消息全程不超预算，
 *      隔离与停机后归零。
 *   2. 黑洞 / 强制重连 / 恢复多轮循环：每轮把未确认预算打满，恢复后两端预算都排空到 0，
 *      接收端恰好收到被准入的消息、没有重复。
 * 等待一律用 awaitCondition；采样协程只观测峰值，不靠固定时长判断结果。
 */
package cactor_tests.unit.remote_semantics

import std.collection.*
import std.net.{TcpSocket, TcpServerSocket, IPSocketAddress}
import std.sync.*
import std.time.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{StringMessage}
import cactor_foundation.network.{NetworkAddress, TcpTransport, TcpTransportLimits, BlackholeTransport}
import cactor_remote.*
import cactor_testkit.*

let T20S: Duration = Duration.second * 20

/** 只 accept、从不读写的对端：握手等不到应答，出站帧只能在本节点排队 */
class SilentPeer {
    private let server: TcpServerSocket
    private let lock = Mutex()
    private let sockets = ArrayList<TcpSocket>()
    private let running = AtomicBool(true)
    let accepted = AtomicInt64(0)

    init() {
        server = TcpServerSocket(bindAt: 0)
        server.bind()
        spawn {
            while (running.load()) {
                try {
                    let s = server.accept()
                    synchronized(lock) { sockets.add(s) }
                    accepted.fetchAdd(1)
                } catch (_: Exception) {
                    ()
                }
            }
        }
    }

    func address(): NetworkAddress {
        NetworkAddress("127.0.0.1", Int32((server.localAddress as IPSocketAddress).getOrThrow().port))
    }

    func close(): Unit {
        running.store(false)
        try { server.close() } catch (_: Exception) { () }
        let all = synchronized(lock) { sockets.toArray() }
        for (s in all) {
            try { s.close() } catch (_: Exception) { () }
        }
    }
}

/** 后台采样峰值：洪泛期间任何时刻越过预算都会被记下 */
class PeakSampler {
    private let running = AtomicBool(true)
    private let stopped = AtomicBool(false)
    let peaks: Array<AtomicInt64>

    init(probes: Array<() -> Int64>) {
        peaks = Array<AtomicInt64>(probes.size, { _ => AtomicInt64(0) })
        spawn {
            while (running.load()) {
                for (i in 0..probes.size) {
                    let v = probes[i]()
                    if (v > peaks[i].load()) {
                        peaks[i].store(v)
                    }
                }
                sleep(Duration.millisecond)
            }
            stopped.store(true)
        }
    }

    func finish(): Unit {
        running.store(false)
        awaitCondition(Duration.second * 5, { => stopped.load() }) |> discardAwait
    }

    func peak(i: Int64): Int64 { peaks[i].load() }
}

func discardAwait(_: Bool): Unit {}

@Test
public class RemoteNodeBudgetPressureTest {
    @TestCase
    public func floodToSilentPeerStaysWithinBudgetsAcrossReconnects(): Unit {
        let peer = SilentPeer()
        let pendingLimit: Int64 = 64 * 1024
        let controlLimit: Int64 = 16 * 1024
        // 握手 200 ms 超时：对端不答，关联反复握手失败、退避重连
        let tcp = TcpTransport(NetworkAddress("127.0.0.1", 0), Duration.millisecond * 200, limits: TcpTransportLimits(
            maxPendingBytes: pendingLimit, maxControlPendingBytes: controlLimit, idleRetireAfter: Duration.hour))
        let m = nodeWith(tcp, RemotingSettings.defaults().withNodeBudget(300, 100))
        let target = peer.address()
        let body = String(Array<Rune>(1024, repeat: r'x'))
        let sampler = PeakSampler([{ => tcp.getPendingBytes() }, { => tcp.getControlPendingBytes() },
            { => m.reliableBudgetUsage().unacked }])
        try {
            // 4 个协程、40 个通道、共 1000 条 1 KB 消息
            let done = AtomicInt64(0)
            for (w in 0..4) {
                spawn {
                    for (i in 0..250) {
                        m.tellReliable(m.remoteActorOf(target, "sink${(w * 250 + i) % 40}"), StringMessage(body))
                    }
                    done.fetchAdd(1)
                }
            }
            @Expect(awaitCondition(T20S, { => done.load() == 4 }), true)
            @Expect(m.reliableDeliveryStats().unacked, 300)
            @Expect(m.reliableDeliveryStats().nodeBudgetDropped, 700)
            @Expect(budgetConsistent(m, 300, 0), true)
            // 重发持续往同一目标灌帧，字节预算必然被打满；同时要看到多次重连
            @Expect(awaitCondition(T20S, { => tcp.getByteCapacityRejections() > 0 && tcp.getHandshakeFailures() >= 3 }),
                true)
            sampler.finish()
            @Expect(sampler.peak(0) <= pendingLimit, true)
            @Expect(sampler.peak(1) <= controlLimit, true)
            @Expect(sampler.peak(2) <= 300, true)
            // 隔离放弃该对端：消息预算归零
            m.quarantine(target, "silent peer")
            @Expect(budgetConsistent(m, 0, 0), true)
            m.stop()
            @Expect(awaitCondition(T5S, { => tcp.getPendingBytes() == 0 && tcp.getControlPendingBytes() == 0 }), true)
        } finally {
            sampler.finish()
            m.stop()
            peer.close()
        }
    }

    @TestCase
    public func blackholeAndReconnectCyclesDrainBudgetsWithoutDuplicates(): Unit {
        let kit = ActorTestKit("node-budget-pressure")
        let tcpB = TcpTransport(NetworkAddress("127.0.0.1", 0), limits: TcpTransportLimits(
            maxInboundPendingBytes: 1024 * 1024, idleRetireAfter: Duration.hour))
        let b = nodeWith(tcpB, RemotingSettings.defaults().withNodeBudget(1000, 100))
        let tcpA = TcpTransport(NetworkAddress("127.0.0.1", 0), limits: TcpTransportLimits(
            maxPendingBytes: 256 * 1024, idleRetireAfter: Duration.hour))
        let hole = BlackholeTransport(tcpA)
        let a = nodeWith(hole, RemotingSettings.defaults().withNodeBudget(200, 100))
        let sink = Collect()
        let sinkRef = kit.actorOf({ => sink }, "sink")
        for (c in 0..10) {
            b.exposeActor("sink${c}", sinkRef)
        }
        let addrB = b.getLocalAddress()
        let sampler = PeakSampler([{ => a.reliableBudgetUsage().unacked }, { => b.reliableBudgetUsage().inboundBuffered },
            { => tcpA.getPendingBytes() }, { => tcpB.getInboundPendingBytes() }])
        try {
            var admitted: Int64 = 0
            for (round in 0..3) {
                hole.blackhole(addrB)
                // 10 个通道 × 30 条：节点预算只准入前 200 条
                for (i in 0..30) {
                    for (c in 0..10) {
                        a.tellReliable(a.remoteActorOf(addrB, "sink${c}"), StringMessage("r${round}-c${c}-${i}"))
                    }
                }
                admitted += 200
                @Expect(a.reliableDeliveryStats().unacked, 200)
                @Expect(a.reliableDeliveryStats().nodeBudgetDropped, 100 * (round + 1))
                @Expect(budgetConsistent(a, 200, 0), true)
                // 强制重连：旧关联连同排队帧一起丢掉，靠可靠层重发补齐
                tcpA.resetOutbound(addrB)
                hole.heal(addrB)
                let expected = admitted
                @Expect(awaitCondition(T20S, { =>
                    a.reliableDeliveryStats().unacked == 0 && sink.snapshot().size == expected
                }), true)
                @Expect(budgetConsistent(a, 0, 0), true)
                @Expect(awaitCondition(T5S, { => budgetConsistent(b, 0, 0) }), true)
            }
            sampler.finish()
            @Expect(sampler.peak(0) <= 200, true)
            @Expect(sampler.peak(1) <= 100, true)
            @Expect(sampler.peak(2) <= 256 * 1024, true)
            @Expect(sampler.peak(3) <= 1024 * 1024, true)
            // 恰好一次：每条准入的消息只到一次
            let seen = HashSet<String>(sink.snapshot())
            @Expect(seen.size, 600)
            @Expect(sink.snapshot().size, 600)
            a.stop()
            b.stop()
            @Expect(awaitCondition(T5S, { => tcpA.getPendingBytes() == 0 && tcpB.getInboundPendingBytes() == 0 }), true)
        } finally {
            sampler.finish()
            a.stop()
            b.stop()
            kit.shutdown()
        }
    }
}
