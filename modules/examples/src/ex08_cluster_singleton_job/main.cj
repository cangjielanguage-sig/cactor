/*
 * CActor Ex 08 - 集群单例 + 定时任务 + 故障接管（对标 akka-sample-cluster-singleton）
 *
 * 真实约束：
 * - 集群里同一时刻只有一个实例在跑（"每小时对账只跑一次"这类任务）；
 * - 任意节点都能把命令发给它，回复里能看出实例在哪个节点；
 * - 实例自己按固定间隔跑 tick（定时任务）；
 * - 宿主节点宕机后，另一个节点在有限时间内接管，任务继续；
 *   接管后内存计数从 0 开始（单例状态不持久化，这点必须说清楚）。
 *
 * 组件：
 * - Cluster + ClusterSingleton（每个节点一个句柄，factory 在当选节点上创建实例）
 * - JobActor：计数 + onStarted 里 startTimerWithFixedDelay
 * - Lou：GET /job/status、POST /job/inc（挂在 0 号节点）
 *
 * 自检（退出码 0/1）：
 * 1. 恰好一个节点 isRunningHere，三个句柄的 location() 一致；
 * 2. 从三个节点各 ask 一次 inc → 计数 1/2/3，且都在同一节点；
 * 3. tick 计数在等待期内增长（定时器生效）；
 * 4. 停掉宿主节点 → 另一个节点接管，inc 仍可用且实例节点变了、计数从头开始。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex08_cluster_singleton_job
 *   ./target/release/bin/cactor_examples.ex08_cluster_singleton_job serve 18094
 */
package cactor_examples.ex08_cluster_singleton_job

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
import cactor_remote.{SimpleRemoteManager}
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterSingleton}

let NAME = "reconcile-job"
let ASK = Duration.second * 10
let CLUSTER_WAIT = Duration.second * 25

/** 单例实例：计数 + 定时 tick；回复 "node|count|ticks" */
class JobActor <: Actor {
    let node: String
    var count: Int64 = 0
    var ticks: Int64 = 0

    init(node: String) {
        this.node = node
    }

    public prop name: String { get() { "reconcile-job" } }
    public prop description: String { get() { "cluster singleton job" } }

    public func onStarted(context: ActorContext): Unit {
        // 单例实例自己的周期任务：每 100ms 一次（真实场景是每小时对账）
        context.timers.startTimerWithFixedDelay("tick", StringMessage("tick"), Duration.millisecond * 100)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            match (s.getContent()) {
                case "inc" =>
                    count++
                    context.reply(StringMessage("${node}|${count}|${ticks}"))
                case "status" => context.reply(StringMessage("${node}|${count}|${ticks}"))
                case "tick" => ticks++
                case _ => context.reply(StringMessage("?|0|0"))
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
    let singleton: ClusterSingleton
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.index = index
        this.system = CActorSystemFactory.create("ex08-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
        // factory 在"当选的那个节点"上创建实例；node 地址写进回复，便于断言实例位置
        let self = cluster.selfAddress
        this.singleton = ClusterSingleton(cluster, system, remote, NAME, { => JobActor(self) })
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

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

func textOf(message: Message): String {
    match (message as StringMessage) {
        case Some(s) => s.getContent()
        case None => "?|0|0"
    }
}

/** "node|count|ticks" 的字段访问 */
func field(text: String, index: Int64): String {
    let parts = text.split("|")
    if (index < Int64(parts.size)) {
        parts[index]
    } else {
        "?"
    }
}

func runningCount(nodes: Array<Node>): Int64 {
    var n: Int64 = 0
    for (node in nodes) {
        if (node.singleton.isRunningHere()) {
            n++
        }
    }
    n
}

func buildApp(node: Node, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/job/status", { ctx =>
        let r = textOf(node.singleton.ask(StringMessage("status"), ASK).get(ASK))
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"node\":\"${field(r, 0)}\",\"count\":\"${field(r, 1)}\",\"ticks\":\"${field(r, 2)}\"}")
    })
    app.post("/job/inc", { ctx =>
        let r = textOf(node.singleton.ask(StringMessage("inc"), ASK).get(ASK))
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"node\":\"${field(r, 0)}\",\"count\":\"${field(r, 1)}\"}")
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

func startCluster(settings: ClusterSettings): Array<Node> {
    let first = Node(0, None<NetworkAddress>, settings)
    let seed = first.remote.getLocalAddress()
    [first, Node(1, Some(seed), settings), Node(2, Some(seed), settings)]
}

main(args: Array<String>): Int64 {
    let settings = ClusterSettings(Duration.millisecond * 80, Duration.millisecond * 80, 8.0,
        Duration.second * 2, Some(Duration.second * 3))
    let nodes = startCluster(settings)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(nodes[0], port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex08：端口绑定失败")
            return 1
        }
        println("ex08 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let app = buildApp(nodes[0], 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        let formed = waitUntil(CLUSTER_WAIT, { =>
            var ok = true
            for (n in nodes) {
                ok = ok && n.cluster.state().upCount() == 3 && n.cluster.state().converged
            }
            ok
        })
        c.check(formed, "三节点集群形成并收敛")

        // ---- 1. 唯一实例 ----
        println("\n[1] 唯一实例与位置一致")
        let elected = waitUntil(CLUSTER_WAIT, { => runningCount(nodes) == 1 })
        c.check(elected, "恰好一个节点在运行实例")
        let locations = ArrayList<String>()
        for (n in nodes) {
            match (n.singleton.location()) {
                case Some(a) => locations.add(a)
                case None => ()
            }
        }
        c.check(locations.size == 3, "三个句柄都报告了实例位置（${locations.size}/3）")
        var same = locations.size == 3
        if (same) {
            for (a in locations) {
                same = same && a == locations[0]
            }
        }
        c.check(same, "三个句柄位置一致：${if (locations.size > 0) { locations[0] } else { "?" }}")

        // ---- 2. 任意节点发命令，落到同一实例 ----
        println("\n[2] 从三个节点各 inc 一次")
        var replies = ArrayList<String>()
        for (n in nodes) {
            replies.add(textOf(n.singleton.ask(StringMessage("inc"), ASK).get(ASK)))
        }
        c.check(field(replies[0], 1) == "1" && field(replies[1], 1) == "2" && field(replies[2], 1) == "3",
            "计数连续 1/2/3：${field(replies[0], 1)}/${field(replies[1], 1)}/${field(replies[2], 1)}")
        c.check(field(replies[0], 0) == field(replies[2], 0), "三次回复来自同一节点")

        // ---- 3. 定时 tick ----
        println("\n[3] 实例自己的定时任务在跑")
        let ticking = waitUntil(Duration.second * 5, { =>
            let r = textOf(nodes[0].singleton.ask(StringMessage("status"), ASK).get(ASK))
            Int64.parse(field(r, 2)) >= 3
        })
        let status = textOf(nodes[0].singleton.ask(StringMessage("status"), ASK).get(ASK))
        c.check(ticking, "ticks 在增长（当前 ${field(status, 2)}）")

        // ---- 4. Lou 端点 ----
        println("\n[4] Lou 端点读写单例")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let viaHttp = client.post("http://127.0.0.1:${port}/job/inc", "")
        let httpBody = readBody(viaHttp)
        c.check(viaHttp.status == 200 && httpBody.contains("\"count\":\"4\""), "POST /job/inc 返回计数 4")
        let st = client.get("http://127.0.0.1:${port}/job/status")
        c.check(st.status == 200 && readBody(st).contains("\"count\":\"4\""), "GET /job/status 一致")
        client.close()

        // ---- 5. 故障接管 ----
        println("\n[5] 宿主节点宕机后接管")
        let ownerAddr = field(status, 0)
        var ownerIndex: Int64 = -1
        for (n in nodes) {
            if (n.cluster.selfAddress == ownerAddr) {
                ownerIndex = n.index
            }
        }
        c.check(ownerIndex >= 0, "定位到宿主节点 index=${ownerIndex}")
        if (ownerIndex >= 0) {
            let before = runningCount(nodes)
            nodes[ownerIndex].shutdown()
            let survivors = ArrayList<Node>()
            for (n in nodes) {
                if (!n.stopped) {
                    survivors.add(n)
                }
            }
            let failedOver = waitUntil(CLUSTER_WAIT, { =>
                var n: Int64 = 0
                for (s in survivors) {
                    if (s.singleton.isRunningHere()) {
                        n++
                    }
                }
                n == 1
            })
            c.check(failedOver, "另一个节点接管（宕机前运行实例数 ${before}）")
            if (failedOver) {
                let survivor = survivors[0]
                var pick = survivor
                for (s in survivors) {
                    if (s.singleton.isRunningHere()) {
                        pick = s
                    }
                }
                let after = textOf(pick.singleton.ask(StringMessage("inc"), ASK).get(ASK))
                c.check(field(after, 0) != ownerAddr, "新实例在另一个节点：${field(after, 0)}")
                c.check(field(after, 1) == "1", "新实例计数从 0 开始（inc 后为 1，内存状态不迁移）")
            }
        }
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {
            // 宿主节点可能已经在故障接管用例里停掉了
        }
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex08：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
