/*
 * CActor Ex 55 - 按角色路由（对标 Akka cluster-aware router 的 role 过滤，差距 4.3/4.5）
 *
 * 真实约束：
 * - 网关节点只做接入，不该抢业务计算；worker 角色才处理任务；
 * - 角色要随成员视图自动传播：新加入的 worker 立刻可分到任务，节点离开后自动剔除；
 * - 没有任何 worker 时必须明确失败，而不是把任务发给网关或悄悄丢弃。
 *
 * 组件：
 * - `Cluster(..., roles: [...])` + `ClusterSnapshot.membersWithRole`（本轮新增）
 * - `ClusterRouter.withRole("worker")`（本轮新增）：候选只含具备该角色的 Up 成员
 * - Lou：POST /route（投递任务并统计来自哪些节点）、GET /members（地址与角色）
 *
 * 自检（退出码 0/1）：
 * 1. 3 节点收敛，角色在全部节点可见；
 * 2. 角色路由候选只有两台 worker，网关被排除；
 * 3. 6 个任务全部由 worker 处理，网关 0 次；
 * 4. Lou /members 与 /route 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex55_role_routing
 *   ./target/release/bin/cactor_examples.ex55_role_routing serve 18200
 */
package cactor_examples.ex55_role_routing

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings, Member, MemberStatus}
import cactor_cluster.routing.{ClusterRouter, ClusterRouterStrategy}

let WAIT = Duration.second * 25

/** 结果统计：来自哪些节点、各几次 */
class Tally {
    private let lock = Mutex()
    private let hits = HashMap<String, Int64>()

    func hit(node: String): Unit {
        synchronized(lock) {
            hits[node] = (hits.get(node) ?? 0) + 1
        }
    }

    func total(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for ((_, v) in hits) {
                n += v
            }
            n
        }
    }

    func at(node: String): Int64 {
        synchronized(lock) {
            hits.get(node) ?? 0
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{")
            var first = true
            for ((k, v) in hits) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${k}\":${v}")
            }
            sb.append("}")
            sb.toString()
        }
    }
}

/** worker：把任务回给 sender，内容带自己的节点地址 */
class WorkerActor <: Actor {
    let node: String

    init(node: String) {
        this.node = node
    }

    public prop name: String { get() { "role-worker" } }
    public prop description: String { get() { "role worker" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            context.reply(StringMessage("${node}|${s.getContent()}"))
        }
        MessageResult.Handled
    }
}

/** 收集 worker 回复（网关本地） */
class Collector <: Actor {
    let tally: Tally
    let lock = Mutex()
    let last = ArrayList<String>()

    init(tally: Tally) {
        this.tally = tally
    }

    public prop name: String { get() { "role-collector" } }
    public prop description: String { get() { "role collector" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let parts = s.getContent().split("|")
            if (parts.size >= 2) {
                tally.hit(parts[0])
                synchronized(lock) {
                    last.add(parts[1])
                }
            }
        }
        MessageResult.Handled
    }

    func lastJobs(): Array<String> {
        synchronized(lock) {
            last.toArray()
        }
    }
}

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings, roles: Array<String>) {
        this.system = CActorSystemFactory.create("ex55-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings, roles: roles)
    }

    func address(): String {
        remote.getLocalAddress().toString()
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

func membersJson(cluster: Cluster): String {
    let sb = StringBuilder()
    sb.append("[")
    var first = true
    for (m in cluster.state().members) {
        if (m.status != MemberStatus.Up) {
            continue
        }
        if (!first) {
            sb.append(",")
        }
        first = false
        let roles = StringBuilder()
        roles.append("[")
        var rf = true
        for (r in m.roles) {
            if (!rf) {
                roles.append(",")
            }
            rf = false
            roles.append("\"${r}\"")
        }
        roles.append("]")
        sb.append("{\"address\":\"${m.address}\",\"roles\":${roles.toString()}}")
    }
    sb.append("]")
    sb.toString()
}

func buildApp(router: ClusterRouter, collector: ActorRef, tally: Tally, cluster: Cluster, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/route", { ctx =>
        let jobs = ctx.queryInt("jobs") ?? 6
        let before = tally.total()
        for (i in 0..jobs) {
            router.tell(StringMessage("job-${i}"), collector)
        }
        let ok = waitUntil(WAIT, { => tally.total() >= before + jobs })
        ctx.setSingleHeader("Content-Type", "application/json")
        if (ok) {
            ctx.writeString("{\"done\":${tally.total()},\"by_node\":${tally.json()}}")
        } else {
            ctx.writeString("{\"error\":\"timeout\",\"done\":${tally.total()}}")
        }
    })
    app.get("/members", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(membersJson(cluster))
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

func clusterSettings(): ClusterSettings {
    ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.millisecond * 300, Duration.second * 2).withSeedNodeTimeout(Duration.second * 2)
}

main(args: Array<String>): Int64 {
    let settings = clusterSettings()
    let nodes = ArrayList<Node>()
    nodes.add(Node(0, None<NetworkAddress>, settings, ["gateway"]))
    nodes.add(Node(1, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings, ["worker"]))
    nodes.add(Node(2, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings, ["worker"]))

    // worker 暴露同名服务；网关只做接入
    for (i in 1..3) {
        let addr = nodes[i].address()
        let ref = nodes[i].system.actorOf({ => WorkerActor(addr) }, "role-worker")
        nodes[i].remote.exposeActor("worker", ref)
    }
    let tally = Tally()
    let collector = nodes[0].system.actorOf({ => Collector(tally) }, "role-collector")
    let router = ClusterRouter(nodes[0].cluster, nodes[0].remote, "worker",
        strategy: ClusterRouterStrategy.RoundRobin).withRole("worker")

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let _ = waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 3 })
        let server = buildApp(router, collector, tally, nodes[0].cluster, port)
        let actual = startServer(server)
        if (actual == 0u16) {
            println("ex55：端口绑定失败")
            return 1
        }
        println("ex55 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    try {
        println("\n[1] 角色传播")
        c.check(waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 3 }), "3 节点 Up")
        c.check(waitUntil(WAIT, { =>
            nodes[0].cluster.state().membersWithRole("worker").size == 2 &&
                nodes[0].cluster.state().membersWithRole("gateway").size == 1
        }), "网关视图：2 worker + 1 gateway")
        c.check(nodes[2].cluster.state().hasRole(nodes[1].address(), "worker"), "角色在 worker 之间互见")

        println("\n[2] 角色路由")
        let candidates = router.candidates()
        c.check(candidates.size == 2, "候选只有 2 台 worker")
        c.check(!candidates.contains(nodes[0].address()), "网关被排除在候选之外")
        for (i in 0..6) {
            router.tell(StringMessage("job-${i}"), collector)
        }
        c.check(waitUntil(WAIT, { => tally.total() == 6 }), "6 个任务全部完成")
        c.check(tally.at(nodes[0].address()) == 0, "网关处理 0 个任务：${tally.json()}")
        c.check(tally.at(nodes[1].address()) > 0 && tally.at(nodes[2].address()) > 0,
            "两台 worker 都参与：${tally.json()}")

        println("\n[3] Lou 端点")
        let server = buildApp(router, collector, tally, nodes[0].cluster, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let members = readBody(client.get("http://127.0.0.1:${port}/members"))
            c.check(members.contains("\"gateway\"") && members.contains("\"worker\""), "GET /members：${members}")
            let body = readBody(client.post("http://127.0.0.1:${port}/route?jobs=4", ""))
            c.check(body.contains("\"done\":10"), "POST /route：${body}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }
    } finally {
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex55：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
