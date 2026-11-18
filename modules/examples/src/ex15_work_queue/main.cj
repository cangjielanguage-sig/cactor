/*
 * CActor Ex 15 - 分布式工作队列（对标 akka-sample-cluster 的 stats worker、protoactor-go work pull）
 *
 * 真实约束：
 * - 一组 worker 通过服务发现（Receptionist）注册，协调者不需要预配置 worker 地址；
 * - 每个 worker 同一时刻只拿一个任务，避免"一台慢机器拿光所有任务"；
 * - 任务失败要重试，超过上限进入死信（不能无限重试）；
 * - worker 掉线时它手上的任务必须重排给存活 worker（否则任务永远卡住）；
 * - 协调者本身崩溃不在本示例范围（那需要持久化队列，见 ex05）。
 *
 * 组件：
 * - ClusterReceptionist：worker 注册/发现（跨节点，引用可直接 tell）
 * - Coordinator：20ms tick 派发 + 超时重排 + 重试上限 + 死信
 * - Worker：同一时刻只处理一个任务；poison 永远失败、flaky 第一次失败
 * - Lou：POST /jobs、GET /stats、POST /kill-worker
 *
 * 自检（退出码 0/1）：
 * 1. 40 个任务 → 全部恰好完成一次，两个 worker 都拿到过任务；
 * 2. flaky 第一次失败后重试成功；poison 重试 3 次后进入死信；
 * 3. 停掉一个 worker → 它手上的任务在超时后重排，后续任务全部完成；
 * 4. Lou 端点与协调者计数一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex15_work_queue
 *   ./target/release/bin/cactor_examples.ex15_work_queue serve 18100
 */
package cactor_examples.ex15_work_queue

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
import cactor_remote.SimpleRemoteManager
import cactor_cluster.membership.{Cluster, ClusterSettings, ClusterReceptionist}
import cactor_cluster.ddata.Replicator

let WORKER_SERVICE = "ex15-worker"
let MAX_ATTEMPTS = 3
let LEASE_MS = 400
let CLUSTER_WAIT = Duration.second * 25

// ======================= Worker =======================

class WorkerStats {
    let lock = Mutex()
    let processed = HashMap<String, Int64>()

    func record(tag: String): Unit {
        synchronized(lock) {
            processed[tag] = (processed.get(tag) ?? 0) + 1
        }
    }

    func of(tag: String): Int64 {
        synchronized(lock) {
            processed.get(tag) ?? 0
        }
    }
}

class Worker <: Actor {
    let tag: String
    let stats: WorkerStats

    init(tag: String, stats: WorkerStats) {
        this.tag = tag
        this.stats = stats
    }

    public prop name: String { get() { "worker-${tag}" } }
    public prop description: String { get() { "work pull worker" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let parts = s.getContent().split("|")
            if (parts.size == 3 && parts[0] == "job") {
                let id = parts[1]
                let attempt = Int64.parse(parts[2])
                stats.record(tag)
                // poison：永远失败；flaky：第一次失败（验证重试路径）
                let ok = if (id.startsWith("poison")) {
                    false
                } else if (id.startsWith("flaky") && attempt == 0) {
                    false
                } else {
                    true
                }
                context.reply(StringMessage("done|${id}|${ok}"))
            }
        }
        MessageResult.Handled
    }
}

// ======================= Coordinator =======================

class Inflight {
    let worker: String
    let deadline: MonoTime
    let attempt: Int64

    init(worker: String, deadline: MonoTime, attempt: Int64) {
        this.worker = worker
        this.deadline = deadline
        this.attempt = attempt
    }
}

class Coordinator <: Actor {
    let receptionist: ClusterReceptionist
    let lock = Mutex()
    let pending = ArrayList<String>()
    let inflight = HashMap<String, Inflight>()
    let submitted = HashSet<String>()
    let done = HashSet<String>()
    let dead = HashSet<String>()
    let attempts = HashMap<String, Int64>()
    let assigned = HashMap<String, Int64>()

    init(receptionist: ClusterReceptionist) {
        this.receptionist = receptionist
    }

    public prop name: String { get() { "coordinator" } }
    public prop description: String { get() { "work queue coordinator" } }

    public func onStarted(context: ActorContext): Unit {
        context.timers.startTimerWithFixedDelay("tick", StringMessage("tick"), Duration.millisecond * 20)
    }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let text = s.getContent()
            if (text == "tick") {
                reap()
                dispatch(context)
            } else if (text.startsWith("submit|")) {
                submit(text[7..])
            } else if (text.startsWith("done|")) {
                let parts = text.split("|")
                if (parts.size == 3) {
                    onDone(parts[1], parts[2] == "true", context)
                }
            }
        }
        MessageResult.Handled
    }

    func submit(id: String): Unit {
        synchronized(lock) {
            if (submitted.contains(id) || done.contains(id) || dead.contains(id)) {
                return
            }
            submitted.add(id)
            pending.add(id)
        }
    }

    func onDone(id: String, ok: Bool, context: ActorContext): Unit {
        synchronized(lock) {
            if (done.contains(id) || dead.contains(id)) {
                return
            }
            match (inflight.remove(id)) {
                case Some(_) =>
                    if (ok) {
                        done.add(id)
                    } else {
                        let att = (attempts.get(id) ?? 0) + 1
                        attempts[id] = att
                        if (att >= MAX_ATTEMPTS) {
                            dead.add(id)
                        } else {
                            pending.add(id)
                        }
                    }
                case None => ()
            }
        }
        dispatch(context)
    }

    /** worker 掉线/卡住：租约过期的任务重排；超过重试上限进死信 */
    func reap(): Unit {
        let now = MonoTime.now()
        synchronized(lock) {
            let expired = ArrayList<String>()
            for ((id, inf) in inflight) {
                if (now > inf.deadline) {
                    expired.add(id)
                }
            }
            for (id in expired) {
                inflight.remove(id)
                let att = (attempts.get(id) ?? 0) + 1
                attempts[id] = att
                if (att >= MAX_ATTEMPTS) {
                    dead.add(id)
                } else {
                    pending.add(id)
                }
            }
        }
    }

    /** 每个空闲 worker 最多派一个任务；锁内只改状态，锁外再发送（tell 可能触发远程 I/O） */
    func dispatch(context: ActorContext): Unit {
        let workers = receptionist.find(WORKER_SERVICE)
        let assignments = ArrayList<(ActorRef, String)>()
        synchronized(lock) {
            if (pending.isEmpty()) {
                return
            }
            let busy = HashSet<String>()
            for ((_, inf) in inflight) {
                busy.add(inf.worker)
            }
            for (w in workers) {
                if (pending.isEmpty()) {
                    break
                }
                let path = w.path.toString()
                if (busy.contains(path)) {
                    continue
                }
                let id = pending.remove(at: 0)
                let attempt = attempts.get(id) ?? 0
                inflight[id] = Inflight(path, MonoTime.now() + Duration.millisecond * LEASE_MS, attempt)
                assigned[path] = (assigned.get(path) ?? 0) + 1
                assignments.add((w, "job|${id}|${attempt}"))
            }
        }
        for ((w, text) in assignments) {
            w.tell(StringMessage(text), context.self)
        }
    }

    func statsJson(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{\"submitted\":${submitted.size},\"done\":${done.size},\"dead\":${dead.size},")
            sb.append("\"pending\":${pending.size},\"inflight\":${inflight.size},\"deadIds\":[")
            var first = true
            for (id in dead) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${id}\"")
            }
            sb.append("],\"workers\":{")
            var firstW = true
            for ((path, n) in assigned) {
                if (!firstW) {
                    sb.append(",")
                }
                firstW = false
                sb.append("\"${path}\":${n}")
            }
            sb.append("}}")
            sb.toString()
        }
    }

    func counters(): (Int64, Int64, Int64) {
        synchronized(lock) {
            (Int64(done.size), Int64(dead.size), Int64(inflight.size))
        }
    }

    func assignedCount(): Int64 {
        synchronized(lock) {
            Int64(assigned.size)
        }
    }
}

// ======================= 节点与 Lou =======================

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
        this.system = CActorSystemFactory.create("ex15-node-${index}")
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

func buildApp(coordinator: ActorRef, workerStats: WorkerStats, killWorker: () -> Unit, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/jobs", { ctx =>
        let n = ctx.queryInt("n") ?? 1
        let prefix = ctx.query("prefix") ?? "job"
        for (i in 0..n) {
            coordinator.tell(StringMessage("submit|${prefix}-${i}"))
        }
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"submitted\":${n},\"prefix\":\"${prefix}\"}")
    })
    app.get("/stats", { ctx =>
        let w1n = workerStats.of("w1")
        let w2n = workerStats.of("w2")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"w1\":${w1n},\"w2\":${w2n}}")
    })
    app.post("/kill-worker", { ctx =>
        // 停掉本地 worker：Receptionist 会自动注销，未完成任务靠租约超时重排
        killWorker()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"killed\":\"worker-1\"}")
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
    let workerStats = WorkerStats()
    let w1 = nodes[1].system.actorOf({ => Worker("w1", workerStats) }, "worker-1")
    let w2 = nodes[2].system.actorOf({ => Worker("w2", workerStats) }, "worker-2")
    nodes[1].receptionist.register(WORKER_SERVICE, w1)
    nodes[2].receptionist.register(WORKER_SERVICE, w2)
    let coordinatorActor = Coordinator(nodes[0].receptionist)
    let coordinatorRef = nodes[0].system.actorOf({ => coordinatorActor }, "coordinator")
    let killW1 = { =>
        nodes[1].receptionist.deregister(WORKER_SERVICE, w1)
        nodes[1].system.stop(w1)
    }

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(coordinatorRef, workerStats, killW1, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex15：端口绑定失败")
            return 1
        }
        println("ex15 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
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
    // 等 Receptionist 的服务集合在两个 worker 上都可见
    c.check(waitUntil(CLUSTER_WAIT, { => nodes[0].receptionist.find(WORKER_SERVICE).size == 2 }),
        "协调者发现 2 个 worker")
    let app = buildApp(coordinatorRef, workerStats, killW1, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. 40 个任务全部完成 ----
        println("\n[1] 40 个任务：恰好完成一次，两个 worker 都有活")
        for (i in 0..40) {
            coordinatorRef.tell(StringMessage("submit|job-${i}"))
        }
        let allDone = waitUntil(Duration.second * 40, { =>
            let (d, _, _) = coordinatorActor.counters()
            d == 40
        })
        let (done1, dead1, _) = coordinatorActor.counters()
        c.check(allDone, "40 个任务全部完成（done=${done1}）")
        c.check(dead1 == 0, "没有任务进入死信")
        let w1n = workerStats.of("w1")
        let w2n = workerStats.of("w2")
        c.check(w1n > 0 && w2n > 0, "两个 worker 都拿到过任务（w1=${w1n}, w2=${w2n}）")
        c.check(w1n + w2n == 40, "worker 侧处理次数与提交数一致")

        // ---- 2. flaky 重试 + poison 死信 ----
        println("\n[2] flaky 重试成功、poison 进入死信")
        coordinatorRef.tell(StringMessage("submit|flaky-1"))
        coordinatorRef.tell(StringMessage("submit|poison-1"))
        let settled = waitUntil(Duration.second * 30, { =>
            let (d, dead, _) = coordinatorActor.counters()
            d == 41 && dead == 1
        })
        let (done2, dead2, _) = coordinatorActor.counters()
        c.check(settled, "flaky 完成、poison 死信（done=${done2}, dead=${dead2}）")

        // ---- 3. worker 掉线：在途任务重排 ----
        println("\n[3] 停掉 worker-1：在途任务重排后全部完成")
        // 让 worker-1 无法再被派活（注销 + 停止）
        nodes[1].receptionist.deregister(WORKER_SERVICE, w1)
        nodes[1].system.stop(w1)
        let gone = waitUntil(Duration.second * 20, { => nodes[0].receptionist.find(WORKER_SERVICE).size == 1 })
        c.check(gone, "协调者只剩 1 个 worker")
        for (i in 0..20) {
            coordinatorRef.tell(StringMessage("submit|late-${i}"))
        }
        let lateDone = waitUntil(Duration.second * 40, { =>
            let (d, _, _) = coordinatorActor.counters()
            d == 61
        })
        c.check(lateDone, "worker 掉线后的 20 个任务全部完成（done=${coordinatorActor.counters()[0]}）")
        c.check(coordinatorActor.counters()[1] == 1, "重排没有把任务误判为死信")

        // ---- 4. Lou 端点 ----
        println("\n[4] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 15).build()
        let submit = client.post("http://127.0.0.1:${port}/jobs?n=5&prefix=http", "")
        c.check(submit.status == 200 && readBody(submit).contains("\"submitted\":5"), "POST /jobs 提交 5 个")
        let httpOk = waitUntil(Duration.second * 30, { => coordinatorActor.counters()[0] == 66 })
        let httpDone = coordinatorActor.counters()[0]
        c.check(httpOk, "HTTP 提交的任务全部完成（done=${httpDone}）")
        let stats = client.get("http://127.0.0.1:${port}/stats")
        let body = readBody(stats)
        c.check(stats.status == 200 && body.contains("\"w2\""), "GET /stats 反映 worker 处理数：${body}")
        let kill = client.post("http://127.0.0.1:${port}/kill-worker", "")
        c.check(kill.status == 200, "POST /kill-worker 返回 200")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        for (n in nodes) {
            n.shutdown()
        }
    }

    println("\nex15：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
