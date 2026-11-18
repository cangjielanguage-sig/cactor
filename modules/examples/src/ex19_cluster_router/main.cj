/*
 * CActor Ex 19 - 集群感知路由（对标 Akka cluster-aware router，差距 4.5 的应用层补法）
 *
 * 真实约束：
 * - 目标集合来自服务发现（Receptionist），而不是静态配置的地址；
 * - 每次路由按**实时负载**选目标（最闲的），负载低的目标优先；
 * - 目标节点掉线/注销后不再被选中；
 * - 没有可用目标时**明确失败**，不能静默丢消息。
 *
 * 说明：CActor 目前没有内建的集群感知路由器（差距 4.5）。本示例用
 * `ClusterReceptionist.find` + 远程 ask 读取负载，在网关层实现等效路由；
 * 生产上应并行 ask（本示例为可读性串行，注释里标了替换点）。
 *
 * 组件：
 * - ClusterReceptionist：跨节点发现 worker
 * - AskInbox：本地/远程统一的请求-响应
 * - Lou：GET /workers、POST /route、POST /set?worker=&load=
 *
 * 自检（退出码 0/1）：
 * 1. 三个 worker，负载 5/1/3 → 选中负载最低的；
 * 2. 改变负载 → 选择随之改变（不是轮询）；
 * 3. 停掉一个 worker → 不再被选中；
 * 4. 全部注销 → 明确失败（不静默丢）；
 * 5. Lou 端点与内存状态一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex19_cluster_router
 *   ./target/release/bin/cactor_examples.ex19_cluster_router serve 18104
 */
package cactor_examples.ex19_cluster_router

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.dsl.AskInbox
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterReceptionist}
import cactor_cluster.ddata.Replicator

let SERVICE = "ex19-svc"
let ASK = Duration.second * 5
let CLUSTER_WAIT = Duration.second * 25

class SvcWorker <: Actor {
    let tag: String
    let lock = Mutex()
    var load: Int64 = 0
    var done: Int64 = 0

    init(tag: String) {
        this.tag = tag
    }

    public prop name: String { get() { "svc-${tag}" } }
    public prop description: String { get() { "cluster router target" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let text = s.getContent()
            if (text.startsWith("set|")) {
                synchronized(lock) {
                    load = Int64.parse(text[4..])
                }
                context.reply(StringMessage("ok"))
            } else if (text == "peek") {
                let l = synchronized(lock) { load }
                context.reply(StringMessage("${l}"))
            } else if (text == "work") {
                let d = synchronized(lock) {
                    done++
                    if (load > 0) {
                        load--
                    }
                    done
                }
                context.reply(StringMessage("${tag}|${d}"))
            } else {
                context.reply(StringMessage("?"))
            }
        }
        MessageResult.Handled
    }

    func currentLoad(): Int64 {
        synchronized(lock) {
            load
        }
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    let replicator: Replicator
    let receptionist: ClusterReceptionist
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.index = index
        this.system = CActorSystemFactory.create("ex19-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        this.replicator = Replicator(system, remote, Duration.millisecond * 50)
        this.receptionist = ClusterReceptionist(cluster, system, remote, replicator)
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

/** 网关侧的路由器：发现目标 → 读负载 → 选最闲 → 转发 */
class ClusterRouter {
    let receptionist: ClusterReceptionist

    init(receptionist: ClusterReceptionist) {
        this.receptionist = receptionist
    }

    func targets(): Array<ActorRef> {
        receptionist.find(SERVICE)
    }

    func loadOf(ref: ActorRef): Int64 {
        Int64.parse(textOf(AskInbox.ask(ref, StringMessage("peek")).get(ASK)))
    }

    func loads(): Array<(String, Int64)> {
        let out = ArrayList<(String, Int64)>()
        for (r in targets()) {
            out.add((r.path.toString(), loadOf(r)))
        }
        out.toArray()
    }

    /** 返回 (目标路径, 回复)；没有目标时抛出明确异常 */
    func route(): (String, String) {
        let refs = targets()
        if (refs.isEmpty()) {
            throw IllegalStateException("no_candidates")
        }
        var best: Option<ActorRef> = None<ActorRef>
        var bestLoad: Int64 = Int64.Max
        // 生产上应并行 ask 所有目标；示例串行以便阅读
        for (r in refs) {
            let l = loadOf(r)
            if (l < bestLoad) {
                bestLoad = l
                best = Some(r)
            }
        }
        let targetRef = best.getOrThrow()
        let reply = textOf(AskInbox.ask(targetRef, StringMessage("work")).get(ASK))
        (targetRef.path.toString(), reply)
    }
}

func textOf(m: Message): String {
    match (m as StringMessage) {
        case Some(s) => s.getContent()
        case None => "?"
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

func workersJson(router: ClusterRouter): String {
    let sb = StringBuilder()
    sb.append("{\"workers\":[")
    var first = true
    for ((path, load) in router.loads()) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("{\"worker\":\"${path}\",\"load\":${load}}")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(router: ClusterRouter, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/workers", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(workersJson(router))
    })
    app.post("/route", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        try {
            let (worker, reply) = router.route()
            ctx.writeString("{\"worker\":\"${worker}\",\"reply\":\"${reply}\"}")
        } catch (_: IllegalStateException) {
            ctx.writeStringWithCode(503u16, "{\"error\":\"no_candidates\"}")
        } catch (e: Exception) {
            ctx.writeStringWithCode(500u16, "{\"error\":\"route_failed\"}")
        }
    })
    app.post("/set", { ctx =>
        // 按"当前可见目标排序后的下标"设置负载，便于外部复现实验
        let idx = ctx.queryInt("worker") ?? 0
        let load = ctx.queryInt("load") ?? 0
        let refs = router.targets()
        ctx.setSingleHeader("Content-Type", "application/json")
        if (idx < 0 || idx >= Int64(refs.size)) {
            ctx.writeStringWithCode(400u16, "{\"error\":\"bad_worker\"}")
        } else {
            AskInbox.ask(refs[idx], StringMessage("set|${load}")).get(ASK)
            ctx.writeString("{\"worker\":\"${refs[idx].path.toString()}\",\"load\":${load}}")
        }
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

/** 按路径排序后设置负载，返回选中的标签 */
func setLoads(router: ClusterRouter, values: Array<Int64>): Array<ActorRef> {
    let refs = router.targets()
    for (i in 0..refs.size) {
        if (i < values.size) {
            AskInbox.ask(refs[i], StringMessage("set|${values[i]}")).get(ASK)
        }
    }
    refs
}

main(args: Array<String>): Int64 {
    let nodeCount = 3
    let settings = ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.second * 2, Some(Duration.second * 3))
    let nodes = ArrayList<Node>()
    var seed: Option<NetworkAddress> = None<NetworkAddress>
    for (i in 0..nodeCount) {
        let n = Node(i, seed, settings)
        seed = Some(n.remote.getLocalAddress())
        nodes.add(n)
    }
    let workerRefs = ArrayList<ActorRef>()
    for (i in 0..nodeCount) {
        let ref = nodes[i].system.actorOf({ => SvcWorker("w${i}") }, "svc-w${i}")
        nodes[i].receptionist.register(SERVICE, ref)
        workerRefs.add(ref)
    }
    let router = ClusterRouter(nodes[0].receptionist)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(router, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex19：端口绑定失败")
            return 1
        }
        println("ex19 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
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
    c.check(formed, "三节点集群形成并收敛")
    c.check(waitUntil(CLUSTER_WAIT, { => router.targets().size == 3 }), "路由发现 3 个目标")
    let app = buildApp(router, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1/2. 按负载选择 ----
        println("\n[1] 按实时负载选择最闲目标")
        let refs = setLoads(router, [5, 1, 3])
        let (w1, r1) = router.route()
        c.check(w1 == refs[1].path.toString(), "负载 1 的目标被选中（${w1}）")
        setLoads(router, [1, 9, 3])
        let (w2, _) = router.route()
        c.check(w2 == refs[0].path.toString(), "负载变化后改选另一个目标（${w2}）")
        setLoads(router, [9, 9, 2])
        let (w3, _) = router.route()
        c.check(w3 == refs[2].path.toString(), "再次改选负载 2 的目标（${w3}）")

        // ---- 3. 停掉一个目标 ----
        println("\n[3] 目标停用后不再被选中")
        nodes[2].receptionist.deregister(SERVICE, workerRefs[2])
        nodes[2].system.stop(workerRefs[2])
        c.check(waitUntil(CLUSTER_WAIT, { => router.targets().size == 2 }), "目标集合降为 2")
        setLoads(router, [0, 0])
        var pickedDead = false
        for (_ in 0..6) {
            let (w, _) = router.route()
            if (w == workerRefs[2].path.toString()) {
                pickedDead = true
            }
        }
        c.check(!pickedDead, "已停用目标从未被选中")

        // ---- 4. 无候选明确失败 ----
        println("\n[4] 全部注销 → 明确失败")
        for (i in 0..2) {
            nodes[i].receptionist.deregister(SERVICE, workerRefs[i])
            nodes[i].system.stop(workerRefs[i])
        }
        c.check(waitUntil(CLUSTER_WAIT, { => router.targets().size == 0 }), "目标集合为空")
        var noCandidate = false
        try {
            router.route()
        } catch (e: IllegalStateException) {
            noCandidate = e.message.contains("no_candidates")
        }
        c.check(noCandidate, "无候选时抛出 no_candidates")

        // ---- 5. Lou ----
        println("\n[5] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let route = client.post("http://127.0.0.1:${port}/route", "")
        c.check(route.status == 503 && readBody(route).contains("no_candidates"), "POST /route 在无目标时返回 503")
        let workers = client.get("http://127.0.0.1:${port}/workers")
        c.check(workers.status == 200 && readBody(workers).contains("\"workers\":[]"), "GET /workers 为空列表")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex19：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
