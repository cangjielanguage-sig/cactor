/*
 * CActor Ex 53 - 分布式计算 Worker（对标 Akka Distributed Workers 示例）
 *
 * 真实约束：
 * - 计算任务在多台机器上并行；前端只负责拆分与收集，不在本地闷头算；
 * - 应答必须回到发起方（可能是另一个节点），而不是依赖全局的"结果表"轮询；
 * - 前端要能观测任务落到了哪些节点（本机 / 远端），便于发现分配倾斜。
 *
 * 设计：
 * - 前端把每个任务 `tell` 给 worker，并把本地 Collector 作为 sender；
 *   worker 用 `context.reply` 把结果送回 Collector——跨节点时回复沿网络直接回到发起方；
 * - 任务在本地 worker 与远端 worker 之间交替，结果里带执行节点标签。
 *
 * 组件：经典 Actor、SimpleRemoteManager（远程引用 + sender 回复）、Lou
 *
 * 自检（退出码 0/1）：
 * 1. 2 节点收敛，远端 worker 暴露成功；
 * 2. 10 个任务（每个 0..99 求和）→ 总和正确，本地/远端都参与；
 * 3. 0 个任务 → 总和 0；
 * 4. Lou /compute 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex53_distributed_workers
 *   ./target/release/bin/cactor_examples.ex53_distributed_workers serve 18198
 */
package cactor_examples.ex53_distributed_workers

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
import cactor_cluster.membership.{Cluster, ClusterSettings}

let WAIT = Duration.second * 20

/** 结果收集（线程安全）：按任务号去重，统计本地/远端执行数 */
class Results {
    private let lock = Mutex()
    private let sums = HashMap<String, Int64>()
    private let hosts = HashMap<String, Int64>()

    func add(id: String, sum: Int64, host: String): Unit {
        synchronized(lock) {
            if (!sums.contains(id)) {
                sums[id] = sum
                hosts[host] = (hosts.get(host) ?? 0) + 1
            }
        }
    }

    func size(): Int64 {
        synchronized(lock) {
            Int64(sums.size)
        }
    }

    func total(): Int64 {
        synchronized(lock) {
            var t: Int64 = 0
            for ((_, v) in sums) {
                t += v
            }
            t
        }
    }

    func hostCount(host: String): Int64 {
        synchronized(lock) {
            hosts.get(host) ?? 0
        }
    }

    func reset(): Unit {
        synchronized(lock) {
            sums.clear()
            hosts.clear()
        }
    }

    func json(): String {
        synchronized(lock) {
            var t: Int64 = 0
            for ((_, v) in sums) {
                t += v
            }
            let l = hosts.get("local") ?? 0
            let r = hosts.get("remote") ?? 0
            "{\"done\":${sums.size},\"total\":${t},\"local\":${l},\"remote\":${r}}"
        }
    }
}

/** 把任务 "job|id|lo|hi" 算成 "result|id|sum"，用 context.reply 送回发送方 */
class SumWorker <: Actor {
    let host: String

    init(host: String) {
        this.host = host
    }

    public prop name: String { get() { "sum-worker" } }
    public prop description: String { get() { "distributed sum worker" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage =>
                let parts = s.getContent().split("|")
                if (parts.size == 4 && parts[0] == "job") {
                    let lo = Int64.parse(parts[2])
                    let hi = Int64.parse(parts[3])
                    var sum: Int64 = 0
                    var i = lo
                    while (i < hi) {
                        sum += i
                        i++
                    }
                    context.reply(StringMessage("result|${parts[1]}|${sum}|${host}"))
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

/** 收集 worker 的回复 */
class Collector <: Actor {
    let results: Results

    init(results: Results) {
        this.results = results
    }

    public prop name: String { get() { "collector" } }
    public prop description: String { get() { "result collector" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        match (message) {
            case s: StringMessage =>
                let parts = s.getContent().split("|")
                if (parts.size == 4 && parts[0] == "result") {
                    results.add(parts[1], Int64.parse(parts[2]), parts[3])
                }
                MessageResult.Handled
            case _ => MessageResult.Unhandled
        }
    }
}

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let cluster: Cluster
    var stopped = false

    init(index: Int64, seed: Option<NetworkAddress>, settings: ClusterSettings) {
        this.system = CActorSystemFactory.create("ex53-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        let bootstrap = match (seed) {
            case Some(s) => s
            case None => remote.getLocalAddress()
        }
        this.cluster = Cluster(system, remote, [bootstrap], settings)
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

/** 前端：拆分任务、交替投递、等待收集齐 */
class Frontend {
    let localWorker: ActorRef
    let remoteWorker: ActorRef
    let collector: ActorRef
    let results: Results

    init(localWorker: ActorRef, remoteWorker: ActorRef, collector: ActorRef, results: Results) {
        this.localWorker = localWorker
        this.remoteWorker = remoteWorker
        this.collector = collector
        this.results = results
    }

    /** 提交 jobs 个 [j*size, (j+1)*size) 区间求和任务；返回是否在期限内收齐 */
    func compute(jobs: Int64, size: Int64): Bool {
        let before = results.size()
        for (j in 0..jobs) {
            let lo = j * size
            let hi = lo + size
            let payload = StringMessage("job|${j}|${lo}|${hi}")
            if (j % 2 == 0) {
                localWorker.tell(payload, collector)
            } else {
                remoteWorker.tell(payload, collector)
            }
        }
        let target = before + jobs
        waitUntil(WAIT, { => results.size() >= target })
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

func buildApp(frontend: Frontend, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/compute", { ctx =>
        let jobs = ctx.queryInt("jobs") ?? 10
        let size = ctx.queryInt("size") ?? 100
        // 每次请求独立统计，便于客户端直接读本次结果
        frontend.results.reset()
        let ok = frontend.compute(jobs, size)
        ctx.setSingleHeader("Content-Type", "application/json")
        if (ok) {
            ctx.writeString(frontend.results.json())
        } else {
            ctx.writeString("{\"error\":\"timeout\",\"done\":${frontend.results.size()}}")
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

func clusterSettings(): ClusterSettings {
    ClusterSettings(Duration.millisecond * 100, Duration.millisecond * 100, 8.0,
        Duration.millisecond * 300, Duration.second * 2).withSeedNodeTimeout(Duration.second * 2)
}

main(args: Array<String>): Int64 {
    let settings = clusterSettings()
    let nodes = ArrayList<Node>()
    nodes.add(Node(0, None<NetworkAddress>, settings))
    nodes.add(Node(1, Some<NetworkAddress>(nodes[0].remote.getLocalAddress()), settings))

    // node0 本地 worker；node1 的 worker 暴露给前端
    let localWorker = nodes[0].system.actorOf({ => SumWorker("local") }, "worker-local")
    let remoteWorkerRef = nodes[1].system.actorOf({ => SumWorker("remote") }, "worker-remote")
    nodes[1].remote.exposeActor("worker", remoteWorkerRef)
    let results = Results()
    let collector = nodes[0].system.actorOf({ => Collector(results) }, "collector")
    let remoteWorker = nodes[0].remote.remoteActorOf(nodes[1].remote.getLocalAddress(), "worker")
    let frontend = Frontend(localWorker, remoteWorker, collector, results)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let _ = waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 2 })
        let server = buildApp(frontend, port)
        let actual = startServer(server)
        if (actual == 0u16) {
            println("ex53：端口绑定失败")
            return 1
        }
        println("ex53 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    try {
        println("\n[1] 集群收敛")
        c.check(waitUntil(WAIT, { => nodes[0].cluster.state().upCount() == 2 }), "2 节点 Up")

        println("\n[2] 10 个任务分摊到两节点")
        c.check(frontend.compute(10, 100), "任务全部完成")
        // 任务 j 处理 [j*100, (j+1)*100)：合计 0..999 求和 = 499500
        c.check(results.total() == 499500, "总和 ${results.total()}")
        c.check(results.hostCount("local") == 5, "本地执行 5 个")
        c.check(results.hostCount("remote") == 5, "远端执行 5 个：${results.json()}")

        println("\n[3] 空任务")
        c.check(frontend.compute(0, 100), "0 个任务立即完成")

        println("\n[4] Lou 端点")
        let server = buildApp(frontend, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let body = readBody(client.post("http://127.0.0.1:${port}/compute?jobs=4&size=10", ""))
            // 任务 j 处理 [j*10, (j+1)*10)：合计 0..39 求和 = 780
            c.check(body.contains("\"total\":780") && body.contains("\"done\":4"), "POST /compute：${body}")
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

    println("\nex53：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
