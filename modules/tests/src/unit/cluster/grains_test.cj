/*
 * CActor - R6 测试：虚拟 Actor（Grain）——按需激活、单一激活与回合制、异常与超时、空闲停用、宕机后重新激活
 * 全部在本机回环 TCP 上的真实节点之间进行。
 */
package cactor_tests.unit.cluster

import std.sync.*
import std.time.*
import std.convert.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.message.{Message, StringMessage}
import cactor_cluster.membership.*
import cactor_persistence.{DurableStateStore, MemoryDurableStateStore}
import cactor_testkit.*

/** 计数 Grain：inc 加一并返回新值；boom 抛异常；slow 睡 500ms；where 返回所在节点 */
class CounterGrain <: Grain {
    let store: Option<DurableStateStore>
    let events: EventRecorder
    var count: Int64 = 0
    var revision: Int64 = 0

    init(store: Option<DurableStateStore>, events: EventRecorder) {
        this.store = store
        this.events = events
    }

    public func onActivate(ctx: GrainContext): Unit {
        if (let Some(st) <- store) {
            if (let Some(r) <- st.get("grain-${ctx.id}")) {
                count = Int64.parse(r.state)
                revision = r.revision
            }
        }
        events.record("activate:${ctx.id}@${ctx.node}")
    }

    public func onDeactivate(ctx: GrainContext): Unit {
        events.record("deactivate:${ctx.id}")
    }

    public func onCall(request: Message, ctx: GrainContext): Message {
        let cmd = (request as StringMessage).getOrThrow().getContent()
        match (cmd) {
            case "inc" =>
                count++
                if (let Some(st) <- store) {
                    revision++
                    st.upsert("grain-${ctx.id}", revision, "${count}")
                }
                StringMessage("${count}")
            case "boom" => throw IllegalStateException("计数器拒绝了请求")
            case "slow" =>
                sleep(Duration.millisecond * 500)
                StringMessage("slow")
            case "where" => StringMessage(ctx.node)
            case _ => StringMessage("?")
        }
    }
}

func grainsOn(nodes: Array<CNode>, store: Option<DurableStateStore>, events: EventRecorder, idle: Duration): Array<Grains> {
    Array<Grains>(nodes.size, { i =>
        let n = nodes[i]
        Grains(n.cluster, n.kit.system, n.remote, "counter", { _ => CounterGrain(store, events) }, idle)
    })
}

func text(m: Message): String {
    (m as StringMessage).getOrThrow().getContent()
}

@Test
public class GrainsTest {
    @TestCase
    public func testSingleActivationAndTurnBasedCallsFromAllNodes(): Unit {
        let nodes = startCluster(3, "grain-turn", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let events = EventRecorder()
        let grains = grainsOn(nodes, None<DurableStateStore>, events, Duration.second * 30)
        let lock = Mutex()
        let replies = ArrayList<Int64>()
        let workers = ArrayList<Future<Unit>>()
        for (i in 0..3) {
            let g = grains[i]
            workers.add(spawn {
                for (_ in 0..100) {
                    let v = Int64.parse(text(g.call("g1", StringMessage("inc"), Duration.second * 10)))
                    synchronized(lock) {
                        replies.add(v)
                    }
                }
            })
        }
        for (w in workers) {
            w.get()
        }
        // 三个节点并发 300 次调用：同一个激活逐个处理，应答恰好是 1..300 各一次
        let seen = HashSet<Int64>()
        for (v in replies) {
            seen.add(v)
        }
        @Expect(replies.size, 300)
        @Expect(seen.size, 300)
        @Expect(seen.contains(1) && seen.contains(300), true)
        var activations: Int64 = 0
        for (g in grains) {
            activations += g.stats()[0]
        }
        @Expect(activations, 1)
        for (g in grains) {
            g.stop()
        }
        closeAll(nodes)
    }

    @TestCase
    public func testGrainsSpreadAndStayPut(): Unit {
        let nodes = startCluster(3, "grain-spread", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let grains = grainsOn(nodes, None<DurableStateStore>, EventRecorder(), Duration.second * 30)
        let hosts = HashMap<String, String>()
        for (k in 0..30) {
            hosts["c${k}"] = text(grains[0].call("c${k}", StringMessage("where"), Duration.second * 10))
        }
        let distinct = HashSet<String>()
        for ((_, h) in hosts) {
            distinct.add(h)
        }
        @Expect(distinct.size, 3)
        // 从其他节点调用同一标识：仍在同一个激活上
        for (k in 0..30) {
            @Expect(text(grains[(k % 2) + 1].call("c${k}", StringMessage("where"), Duration.second * 10)), hosts["c${k}"])
        }
        var active: Int64 = 0
        for (g in grains) {
            active += g.activeIds().size
        }
        @Expect(active, 30)
        for (g in grains) {
            g.stop()
        }
        closeAll(nodes)
    }

    @TestCase
    public func testExceptionsAndTimeoutsReachTheCaller(): Unit {
        let nodes = startCluster(2, "grain-err", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let grains = grainsOn(nodes, None<DurableStateStore>, EventRecorder(), Duration.second * 30)
        @Expect(text(grains[0].call("e", StringMessage("inc"), Duration.second * 10)), "1")
        var error = ""
        try {
            grains[1].call("e", StringMessage("boom"), Duration.second * 10)
        } catch (ex: GrainCallException) {
            error = ex.message
        }
        @Expect(error.contains("计数器拒绝了请求"), true)
        // Grain 继续可用，状态没有丢
        @Expect(text(grains[0].call("e", StringMessage("inc"), Duration.second * 10)), "2")
        var timedOut = false
        try {
            grains[0].call("e", StringMessage("slow"), Duration.millisecond * 100)
        } catch (_: GrainTimeoutException) {
            timedOut = true
        }
        @Expect(timedOut, true)
        // 超时后到达的应答被丢弃并计数；之后的调用照常
        @Expect(awaitCondition(Duration.second * 5, { => grains[0].stats()[3] == 1 }), true)
        @Expect(text(grains[0].call("e", StringMessage("inc"), Duration.second * 10)), "3")
        // 单向发送：同一节点随后的调用排在它之后（同一发送方经同一路径，保持顺序）
        grains[1].tell("e", StringMessage("inc"))
        @Expect(text(grains[1].call("e", StringMessage("inc"), Duration.second * 10)), "5")
        for (g in grains) {
            g.stop()
        }
        closeAll(nodes)
    }

    @TestCase
    public func testIdleDeactivationAndReactivationWithState(): Unit {
        let nodes = startCluster(1, "grain-idle", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        let events = EventRecorder()
        let store = MemoryDurableStateStore()
        let n = nodes[0]
        let volatile = Grains(n.cluster, n.kit.system, n.remote, "volatile", { _ => CounterGrain(None<DurableStateStore>, events) },
            Duration.millisecond * 200)
        let durable = Grains(n.cluster, n.kit.system, n.remote, "durable", { _ => CounterGrain(Some<DurableStateStore>(store), events) },
            Duration.millisecond * 200)
        for (g in [volatile, durable]) {
            g.call("x", StringMessage("inc"), Duration.second * 5)
            g.call("x", StringMessage("inc"), Duration.second * 5)
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => volatile.stats()[1] == 1 && durable.stats()[1] == 1 }), true)
        @Expect(volatile.activeIds().size + durable.activeIds().size, 0)
        // 重新激活：没有持久化的从 0 开始，持久化的从存储恢复
        @Expect(text(volatile.call("x", StringMessage("inc"), Duration.second * 5)), "1")
        @Expect(text(durable.call("x", StringMessage("inc"), Duration.second * 5)), "3")
        @Expect(volatile.stats()[0], 2)
        @Expect(durable.stats()[0], 2)
        volatile.stop()
        durable.stop()
        closeAll(nodes)
    }

    @TestCase
    public func testGrainIsReactivatedElsewhereAfterCrash(): Unit {
        let nodes = startCluster(3, "grain-crash", FAST_AUTODOWN)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let store = MemoryDurableStateStore()
        let grains = grainsOn(nodes, Some<DurableStateStore>(store), EventRecorder(), Duration.second * 30)
        // 找一个不在调用节点（node0）上的标识
        var id = ""
        var host = ""
        for (k in 0..50) {
            let h = text(grains[0].call("acct-${k}", StringMessage("where"), Duration.second * 10))
            if (h != nodes[0].address()) {
                id = "acct-${k}"
                host = h
                break
            }
        }
        @Expect(id.size > 0, true)
        for (_ in 0..5) {
            grains[0].call(id, StringMessage("inc"), Duration.second * 10)
        }
        var victim = 1
        if (nodes[2].address() == host) {
            victim = 2
        }
        nodes[victim].crash()
        let survivors = [nodes[0], nodes[3 - victim]]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(survivors, 2) }), true)
        // 下一次调用在存活节点上重新激活，状态从存储恢复
        @Expect(text(grains[0].call(id, StringMessage("inc"), Duration.second * 10)), "6")
        @Expect(text(grains[0].call(id, StringMessage("where"), Duration.second * 10)) != host, true)
        grains[0].stop()
        grains[3 - victim].stop()
        nodes[0].close()
        nodes[3 - victim].close()
    }
}
