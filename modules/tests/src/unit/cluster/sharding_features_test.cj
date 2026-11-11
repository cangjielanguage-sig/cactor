/*
 * CActor - R6 测试：分片的空闲钝化、remember-entities、事件溯源实体在移交 / 宕机后恢复状态
 * 全部在本机回环 TCP 上的真实节点之间进行。
 */
package cactor_tests.unit.cluster

import std.fs.*
import std.sync.*
import std.time.*
import std.random.*
import std.convert.*
import std.collection.*
import std.unittest.*
import std.unittest.testmacro.*
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_cluster.membership.*
import cactor_cluster.ddata.{Replicator}
import cactor_persistence.{EventSourcedActor, Event, EventStore, FileEventStore}
import cactor_testkit.*

/** 记录实体在哪个节点启动过（preStart），不依赖消息 */
class StartLog {
    let lock = Mutex()
    let starts = HashMap<String, ArrayList<String>>()

    func record(id: String, node: String): Unit {
        synchronized(lock) {
            match (starts.get(id)) {
                case Some(l) => l.add(node)
                case None =>
                    let l = ArrayList<String>()
                    l.add(node)
                    starts[id] = l
            }
        }
    }

    func startsOf(id: String): Array<String> {
        synchronized(lock) {
            match (starts.get(id)) {
                case Some(l) => l.toArray()
                case None => Array<String>()
            }
        }
    }
}

/** 收到 "stop" 时自己停止，其余与 CountingEntity 相同 */
class RememberedEntity <: Actor {
    let id: String
    let node: String
    let ledger: Ledgers
    let startLog: StartLog
    var total: Int64 = 0

    init(id: String, node: String, ledger: Ledgers, startLog: StartLog) {
        this.id = id
        this.node = node
        this.ledger = ledger
        this.startLog = startLog
    }

    public prop name: String { get() { "remembered" } }
    public prop description: String { get() { "remembered" } }

    public func preStart(): Unit {
        startLog.record(id, node)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            if (s.getContent() == "stop") {
                context.stop(context.self)
                return MessageResult.Handled
            }
            total++
            ledger.record(id, node, total, s.getContent())
        }
        MessageResult.Handled
    }
}

/** 每条消息处理 30ms 的实体：Region 投递完一批后，邮箱里仍积压着消息 */
class SlowEntity <: Actor {
    let id: String
    let ledger: Ledgers
    var total: Int64 = 0

    init(id: String, ledger: Ledgers) {
        this.id = id
        this.ledger = ledger
    }

    public prop name: String { get() { "slow" } }
    public prop description: String { get() { "slow" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        sleep(Duration.millisecond * 30)
        total++
        ledger.record(id, "", total, "")
        MessageResult.Handled
    }
}

/** 事件溯源计数器：每条消息持久化一个事件，计数在任何节点上重建都从日志恢复 */
class PersistentCounter <: EventSourcedActor {
    let id: String
    let node: String
    let ledger: Ledgers
    var total: Int64 = 0

    init(id: String, node: String, ledger: Ledgers, store: EventStore) {
        super(store)
        this.id = id
        this.node = node
        this.ledger = ledger
    }

    public prop name: String { get() { "persistent-counter" } }
    public prop description: String { get() { "persistent counter" } }

    public func persistenceId(): String {
        "counter-${id}"
    }

    public func handleCommand(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            persist("Incremented", s.getContent())
            ledger.record(id, node, total, s.getContent())
        }
        MessageResult.Handled
    }

    public func applyEvent(event: Event): Unit {
        total++
    }
}

func freshDir(name: String): String {
    let path = "/tmp/cactor-test-${name}-${DateTime.now().toUnixTimeStamp().toNanoseconds()}"
    if (exists(path)) {
        removeIfExists(path, recursive: true)
    }
    Directory.create(path, recursive: true)
    path
}

func totalLocal(regions: Array<ClusterSharding>): Int64 {
    var n: Int64 = 0
    for (r in regions) {
        n += r.localEntityCount()
    }
    n
}

func passivationDiag(what: String, regions: Array<ClusterSharding>, ledger: Ledgers): Unit {
    var local: Int64 = 0
    var passivated: Int64 = 0
    for (i in 0..regions.size) {
        local += regions[i].localEntityCount()
        passivated += regions[i].passivatedCount()
    }
    println("DIAG ${what}：本地实体 ${local}、钝化计数 ${passivated}、写入条数 ${ledger.count()}" +
        "（逐区域 ${regions.map({ r => "${r.localEntityCount()}/${r.passivatedCount()}" }).toArray()}）")
}

/** 等条件成立；超时先打印诊断，再交回调用方断言（失败时日志能区分"没收敛"与"计数时序"） */
func awaitPassivation(limit: Duration, cond: () -> Bool, diag: () -> Unit): Bool {
    let ok = awaitCondition(limit, cond)
    if (!ok) {
        diag()
    }
    ok
}

// 钝化用例统一关掉按负载的自动再平衡：这里验证“空闲即钝化”，而再平衡会把分片（连同实体）移交到别的节点，
// 移交时停止的实体记在 handedOff 上、不计入 passivatedCount（20 个实体只剩 18 次钝化）。再平衡本身由
// ShardingRebalanceTest 覆盖；库的默认值仍然是开启。
@Test
public class ShardingPassivationTest {
    @TestCase
    public func testIdleEntitiesArePassivatedAndRecreatedOnDemand(): Unit {
        let nodes = startCluster(2, "shp-idle", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let ledger = Ledgers()
        let regions = Array<ClusterSharding>(2, { i =>
            let n = nodes[i]
            let addr = n.address()
            ClusterSharding(n.cluster, n.kit.system, n.remote, "idle", 32, { id => CountingEntity(id, addr, ledger) },
                ShardingSettings.passivating(Duration.millisecond * 300).withRebalance(RebalancePolicy.disabled()))
        })
        for (e in 0..20) {
            regions[e % 2].tell("p${e}", StringMessage("a"))
        }
        @Expect(awaitPassivation(CLUSTER_WAIT, { => ledger.count() == 20 }, { => passivationDiag("首轮消息", regions, ledger) }), true)
        // 空闲后全部被钝化。注意顺序：localEntityCount 在 stopEntity 里先归零，钝化计数稍后才递增
        // （同一个 Actor 轮次内、锁外），所以"本地实体为 0"早于"计数到 20"是正常的中间态；
        // 断言等计数到位，语义是"20 个实体全部被钝化过"，不是"某一瞬间恰好数到 20"。
        @Expect(awaitPassivation(CLUSTER_WAIT, { => totalLocal(regions) == 0 }, { => passivationDiag("钝化后", regions, ledger) }), true)
        @Expect(awaitPassivation(CLUSTER_WAIT, { => regions[0].passivatedCount() + regions[1].passivatedCount() == 20 }, { => passivationDiag("钝化计数", regions, ledger) }), true)
        // 再发消息：重新创建，内存状态从 0 开始（证明确实是新实例）
        for (e in 0..20) {
            regions[(e + 1) % 2].tell("p${e}", StringMessage("b"))
        }
        @Expect(awaitPassivation(CLUSTER_WAIT, { => ledger.count() == 40 }, { => passivationDiag("重建消息", regions, ledger) }), true)
        for (e in 0..20) {
            @Expect(ledger.latestOf("p${e}"), 1)
            @Expect(ledger.hostsOf("p${e}").size, 1)
        }
        for (r in regions) {
            r.stop()
        }
        closeAll(nodes)
    }

    @TestCase
    public func testBusyEntityIsNotPassivatedAndNoMessageIsLostAroundPassivation(): Unit {
        let nodes = startCluster(1, "shp-busy", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        let ledger = Ledgers()
        let addr = nodes[0].address()
        // 持续活跃的实体（每 30ms 一条，阈值 1s）不应被钝化
        let steady = ClusterSharding(nodes[0].cluster, nodes[0].kit.system, nodes[0].remote, "steady", 8,
            { id => CountingEntity(id, addr, ledger) },
            ShardingSettings.passivating(Duration.second).withRebalance(RebalancePolicy.disabled()))
        for (k in 0..20) {
            steady.tell("busy", StringMessage("${k}"))
            sleep(Duration.millisecond * 30)
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 20 }), true)
        @Expect(ledger.latestOf("busy"), 20)
        @Expect(steady.passivatedCount(), 0)
        steady.stop()
        let region = ClusterSharding(nodes[0].cluster, nodes[0].kit.system, nodes[0].remote, "busy", 8,
            { id => CountingEntity(id, addr, ledger) },
            ShardingSettings.passivating(Duration.millisecond * 100).withRebalance(RebalancePolicy.disabled()))
        // 间隔在钝化阈值附近随机抖动：不少消息会在实体停止过程中到达，要求一条不丢
        let rnd = Random(7)
        var sent: Int64 = 0
        for (k in 0..60) {
            for (e in 0..5) {
                region.tell("j${e}", StringMessage("${k}"))
                sent++
            }
            sleep(Duration.millisecond * (60 + Int64(rnd.nextUInt8() % 100)))
        }
        let expected = 20 + sent
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == expected }), true)
        @Expect(region.passivatedCount() > 0, true)
        @Expect(region.stats()[3], 0)
        region.stop()
        closeAll(nodes)
    }

    @TestCase
    public func testPassivationLetsQueuedMessagesFinish(): Unit {
        let nodes = startCluster(1, "shp-slow", FAST)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 1) }), true)
        let ledger = Ledgers()
        // 阈值 100ms，而 10 条消息需要 300ms 才能处理完：钝化发生时邮箱中仍有积压
        let region = ClusterSharding(nodes[0].cluster, nodes[0].kit.system, nodes[0].remote, "slow", 8,
            { id => SlowEntity(id, ledger) },
            ShardingSettings.passivating(Duration.millisecond * 100).withRebalance(RebalancePolicy.disabled()))
        for (k in 0..10) {
            region.tell("s", StringMessage("${k}"))
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => region.passivatedCount() == 1 && region.localEntityCount() == 0 }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 10 }), true)
        // 同一个实例处理完全部积压消息后才停止
        @Expect(ledger.latestOf("s"), 10)
        region.stop()
        closeAll(nodes)
    }

    @TestCase
    public func testRememberAndPassivateAreExclusive(): Unit {
        let nodes = startCluster(1, "shp-ex", FAST)
        let rep = Replicator(nodes[0].kit.system, nodes[0].remote, Duration.millisecond * 50)
        var threw = false
        try {
            ShardingSettings(100, Some(Duration.second), Some(rep))
        } catch (e: IllegalArgumentException) {
            threw = true
        }
        @Expect(threw, true)
        rep.stop()
        closeAll(nodes)
    }
}

@Test
public class ShardingRememberEntitiesTest {
    @TestCase
    public func testRememberedEntitiesRestartElsewhereWithoutNewMessages(): Unit {
        let nodes = startCluster(3, "shr-crash", FAST_AUTODOWN)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 3) }), true)
        let ledger = Ledgers()
        let startLog = StartLog()
        let reps = Array<Replicator>(3, { i => Replicator(nodes[i].kit.system, nodes[i].remote, Duration.millisecond * 50) })
        let regions = Array<ClusterSharding>(3, { i =>
            let n = nodes[i]
            let addr = n.address()
            ClusterSharding(n.cluster, n.kit.system, n.remote, "rem", 32,
                { id => RememberedEntity(id, addr, ledger, startLog) },
                // 本例精确核对宕机恢复次数；自动负载迁移会额外启动实体，另由再平衡用例覆盖。
                ShardingSettings.remembering(reps[i]).withRebalance(RebalancePolicy.disabled()))
        })
        for (e in 0..30) {
            regions[0].tell("r${e}", StringMessage("x"))
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 30 }), true)
        // 等实体 ID 复制到幸存节点
        let rememberedEverywhere = { =>
            var all = true
            for (i in 0..2) {
                var n: Int64 = 0
                for (s in 0..32) {
                    n += reps[i].setElements(rememberedEntitiesKey("rem", s)).size
                }
                all = all && n == 30
            }
            all
        }
        @Expect(awaitCondition(CLUSTER_WAIT, rememberedEverywhere), true)
        let victimIds = regions[2].localEntityIds()
        @Expect(victimIds.size > 0, true)
        let victim = nodes[2].address()
        nodes[2].crash()
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp([nodes[0], nodes[1]], 2) }), true)
        // 不发任何消息：宕机节点上的实体在新拥有者上被主动启动
        @Expect(awaitCondition(CLUSTER_WAIT, { => totalLocal([regions[0], regions[1]]) == 30 }), true)
        @Expect(regions[0].restartedCount() + regions[1].restartedCount(), victimIds.size)
        // 区域登记实体是同步的，preStart（写启动记录）在实体自己的线程上稍后执行：
        // 并行跑套件、负载高时，上面的计数可能先于最后几条启动记录成立
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            var all = true
            for (id in victimIds) {
                all = all && startLog.startsOf(id).size >= 2
            }
            all
        }), true)
        for (id in victimIds) {
            let starts = startLog.startsOf(id)
            @Expect(starts.size, 2)
            @Expect(starts[0], victim)
            @Expect(starts[1] != victim, true)
        }
        // 实体自己停止后从集合中删除，不会再被恢复
        regions[0].tell("r0", StringMessage("stop"))
        let key0 = rememberedEntitiesKey("rem", shardOf("r0", 32))
        @Expect(awaitCondition(CLUSTER_WAIT, { =>
            !reps[0].setElements(key0).contains("r0") && !reps[1].setElements(key0).contains("r0")
        }), true)
        @Expect(awaitCondition(CLUSTER_WAIT, { => totalLocal([regions[0], regions[1]]) == 29 }), true)
        @Expect(awaitGossipRounds([nodes[0].cluster, nodes[1].cluster], 2, CLUSTER_WAIT), true)
        @Expect(totalLocal([regions[0], regions[1]]), 29)
        for (i in 0..2) {
            regions[i].stop()
            reps[i].stop()
        }
        nodes[0].close()
        nodes[1].close()
    }
}

@Test
public class ShardingPersistentEntityTest {
    @TestCase
    public func testEventSourcedEntityKeepsStateAcrossCrashAndHandoff(): Unit {
        let dir = freshDir("sharding-es")
        let store = FileEventStore(dir)
        let nodes = startCluster(2, "she", FAST_AUTODOWN)
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(nodes, 2) }), true)
        let ledger = Ledgers()
        let regions = ArrayList<ClusterSharding>()
        for (n in nodes) {
            let addr = n.address()
            regions.add(ClusterSharding(n.cluster, n.kit.system, n.remote, "pc", 32,
                { id => PersistentCounter(id, addr, ledger, store) }))
        }
        for (k in 0..5) {
            for (e in 0..20) {
                regions[0].tell("a${e}", StringMessage("${k}"))
            }
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 100 }), true)
        // 1) 第三个节点加入：部分实体移交过去，计数继续累加
        let third = CNode("she-2").join([nodes[0].addr()], FAST_AUTODOWN)
        let all = [nodes[0], nodes[1], third]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(all, 3) }), true)
        let thirdAddr = third.address()
        regions.add(ClusterSharding(third.cluster, third.kit.system, third.remote, "pc", 32,
            { id => PersistentCounter(id, thirdAddr, ledger, store) }))
        @Expect(awaitCondition(CLUSTER_WAIT, { => regions[0].stats()[2] + regions[1].stats()[2] > 0 }), true)
        for (e in 0..20) {
            regions[1].tell("a${e}", StringMessage("after-join"))
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 120 }), true)
        @Expect(regions[2].localEntityCount() > 0, true)
        for (e in 0..20) {
            @Expect(ledger.latestOf("a${e}"), 6)
        }
        // 2) 两个初始节点中实体较多的一个宕机：其上的实体在幸存节点上从日志恢复。
        // 分片是哈希分配的，固定选 1 号时它有真实概率（实测约 5%，第三节点加入后部分分片移交）
        // 一个实体都不剩，此时 victimIds 为空、"在别处恢复"无从验证——属测试设计问题，
        // 受害者应由实际分布决定（与 testEntitiesOfCrashedNodeAreRecreatedElsewhere 的做法一致）。
        let victimIdx: Int64 = if (regions[0].localEntityCount() >= regions[1].localEntityCount()) { 0 } else { 1 }
        let victimIds = regions[victimIdx].localEntityIds()
        @Expect(victimIds.size > 0, true)
        let survivorIdx: Int64 = if (victimIdx == 0) { 1 } else { 0 }
        nodes[victimIdx].crash()
        let survivors = [nodes[survivorIdx], third]
        @Expect(awaitCondition(CLUSTER_WAIT, { => allUp(survivors, 2) }), true)
        for (e in 0..20) {
            regions[survivorIdx].tell("a${e}", StringMessage("after-crash"))
        }
        @Expect(awaitCondition(CLUSTER_WAIT, { => ledger.count() == 140 }), true)
        for (e in 0..20) {
            @Expect(ledger.latestOf("a${e}"), 7)
        }
        for (id in victimIds) {
            @Expect(store.countEvents("counter-${id}"), 7)
        }
        regions[survivorIdx].stop()
        regions[2].stop()
        nodes[survivorIdx].close()
        third.close()
        removeIfExists(dir, recursive: true)
    }
}
