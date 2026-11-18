/*
 * CActor Ex 23 - 分片实体的空闲钝化与恢复（对标 Akka Cluster Sharding passivation）
 *
 * 真实约束：
 * - 大量低频实体常驻内存会撑爆堆：空闲一段时间后必须停用（钝化）；
 * - 钝化后实体状态不能丢：再次访问时从持久化状态恢复（DurableStateStore）；
 * - 同一实体在任意时刻只在一个节点激活（由分片路由保证）；
 * - 钝化必须真的发生（不能用"永远不会空闲"骗过测试）。
 *
 * 组件：
 * - ClusterSharding + ShardingSettings.passivating(300ms)
 * - 实体 preStart 从 FileDurableStateStore 读取，inc 时按修订号写入
 * - Lou：GET /count?e=、POST /inc?e=
 *
 * 自检（退出码 0/1）：
 * 1. e1 累加 3、e2 累加 1（独立实体互不影响）；
 * 2. 两个节点上都不再持有实体（钝化确实发生）；
 * 3. 再次读取 e1 → 3、e2 → 1（从持久化状态恢复）；
 * 4. Lou 端点一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex23_sharding_passivation
 *   ./target/release/bin/cactor_examples.ex23_sharding_passivation serve 18108
 */
package cactor_examples.ex23_sharding_passivation

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterSharding, ShardingSettings}
import cactor_persistence.{DurableStateStore, FileDurableStateStore}

let ASK = Duration.second * 15
let CLUSTER_WAIT = Duration.second * 25
let IDLE = Duration.millisecond * 300

/** 分片实体：状态在内存 + DurableStateStore；preStart 恢复 */
class CounterEntity <: Actor {
    let id: String
    let store: DurableStateStore
    var count: Int64 = 0
    var revision: Int64 = 0

    init(id: String, store: DurableStateStore) {
        this.id = id
        this.store = store
    }

    public prop name: String { get() { "counter-${id}" } }
    public prop description: String { get() { "passivating shard entity" } }

    public func preStart(): Unit {
        match (store.get(id)) {
            case Some(r) =>
                count = Int64.parse(r.state)
                revision = r.revision
            case None => ()
        }
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            match (s.getContent()) {
                case "inc" =>
                    count++
                    revision++
                    store.upsert(id, revision, "${count}")
                    context.reply(StringMessage("${id}|${count}"))
                case "get" => context.reply(StringMessage("${id}|${count}"))
                case _ => context.reply(StringMessage("${id}|?|${count}"))
            }
        }
        MessageResult.Handled
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.index = index
        this.system = CActorSystemFactory.create("ex23-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            cluster.shutdown()
            remote.stop()
            system.terminate()
        }
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

func ask(shard: ClusterSharding, id: String, cmd: String): String {
    match (shard.ask(id, StringMessage(cmd), ASK).get(ASK) as StringMessage) {
        case Some(s) => s.getContent()
        case None => "?"
    }
}

func localEntities(shards: ArrayList<ClusterSharding>): Int64 {
    var n: Int64 = 0
    for (s in shards) {
        n += Int64(s.localEntityIds().size)
    }
    n
}

func buildApp(shard: ClusterSharding, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/count", { ctx =>
        let id = ctx.query("e") ?? "e1"
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"value\":\"${ask(shard, id, "get")}\"}")
    })
    app.post("/inc", { ctx =>
        let id = ctx.query("e") ?? "e1"
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"value\":\"${ask(shard, id, "inc")}\"}")
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
    app
}

func startServer(app: Lou): UInt16 {
    spawn { app.listen() }
    for (_ in 0..500) {
        if (let Some(s) <- app.getStdServer()) {
            if (s.port != 0u16) {
                return s.port
            }
        }
        sleep(Duration.millisecond * 20)
    }
    0u16
}

func readBody(response: HttpResponse): String {
    let out = ArrayList<UInt8>()
    let buf = Array<UInt8>(4096, repeat: 0)
    var n = response.body.read(buf)
    while (n > 0) {
        for (i in 0..n) {
            out.add(buf[i])
        }
        n = response.body.read(buf)
    }
    String.fromUtf8(out.toArray())
}

main(args: Array<String>): Int64 {
    let dir = "/tmp/cactor-ex23-${MonoTime.now().hashCode()}"
    let store = FileDurableStateStore("${dir}/state")
    let nodeCount = 2
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.second * 2, Some(Duration.second * 3))
    let nodes = ArrayList<Node>()
    var seed: Option<NetworkAddress> = None<NetworkAddress>
    for (i in 0..nodeCount) {
        let n = Node(i, seed, settings)
        seed = Some(n.remote.getLocalAddress())
        nodes.add(n)
    }
    let shardSettings = ShardingSettings.passivating(IDLE)
    let shards = ArrayList<ClusterSharding>()
    for (i in 0..nodeCount) {
        shards.add(ClusterSharding(nodes[i].cluster, nodes[i].system, nodes[i].remote, "counter", 8,
            { id => CounterEntity(id, store) }, shardSettings))
    }

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(shards[0], port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex23：端口绑定失败")
            return 1
        }
        println("ex23 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    var formed = true
    for (n in nodes) {
        formed = formed && waitUntil(CLUSTER_WAIT, { =>
            n.cluster.state().upCount() == nodeCount && n.cluster.state().converged
        })
    }
    c.check(formed, "两节点集群形成并收敛")

    try {
        // ---- 1. 累加 ----
        println("\n[1] e1 累加 3 次、e2 累加 1 次")
        var last = ""
        for (_ in 0..3) {
            last = ask(shards[0], "e1", "inc")
        }
        c.check(last == "e1|3", "e1 == 3（${last}）")
        let e2 = ask(shards[1], "e2", "inc")
        c.check(e2 == "e2|1", "e2 == 1（${e2}）")
        c.check(localEntities(shards) >= 1, "钝化前实体在本地激活（${localEntities(shards)} 个）")

        // ---- 2. 空闲钝化 ----
        println("\n[2] 空闲 ${IDLE.toMilliseconds()} ms 后钝化")
        let passivated = waitUntil(Duration.second * 15, { => localEntities(shards) == 0 })
        c.check(passivated, "两个节点都不再持有实体（实际 ${localEntities(shards)} 个）")

        // ---- 3. 重新激活并恢复状态 ----
        println("\n[3] 再次访问：从持久化状态恢复")
        let restored1 = ask(shards[0], "e1", "get")
        let restored2 = ask(shards[0], "e2", "get")
        c.check(restored1 == "e1|3", "e1 恢复为 3（${restored1}）")
        c.check(restored2 == "e2|1", "e2 恢复为 1（${restored2}）")

        // ---- 5. Lou ----
        println("\n[4] Lou 端点")
        let app = buildApp(shards[0], 0u16)
        let port = startServer(app)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 20).build()
        let count = client.get("http://127.0.0.1:${port}/count?e=e1")
        c.check(readBody(count).contains("e1|3"), "GET /count 返回 3")
        let inc = client.post("http://127.0.0.1:${port}/inc?e=e1", "")
        c.check(readBody(inc).contains("e1|4"), "POST /inc 之后为 4")
        client.close()
        app.stop()
    } finally {
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex23：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
