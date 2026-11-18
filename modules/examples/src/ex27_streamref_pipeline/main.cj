/*
 * CActor Ex 27 - 跨节点流式传输 StreamRef（对标 Akka StreamRefs；CActor 的实现比 SourceRef 更完整）
 *
 * 真实约束：
 * - 一次传输可能在两节点之间跑很久，消费方慢时生产方必须被背压，而不是无界堆内存；
 * - 消费方取消后，生产方必须停止生成（例如停止一次付费的模型调用）；
 * - 完成、失败、取消三种终态必须明确；
 * - 流本身可以当普通消息传地址（wire string），不需要调用方预先知道对端内部结构。
 *
 * 组件：
 * - StreamRefs：sourceRef / subscribe（回调式消费）+ StreamRefSettings.withBufferSize
 * - Lou：GET /stream?n=（本地 SSE）、POST /cross?n=（跨节点传输）、GET /stats
 *
 * 自检（退出码 0/1）：
 * 1. A→B 传输 200 条：B 按序全部收到、onComplete 触发、生产方终态 Completed；
 * 2. 消费方取消：B 收到 10 条后 cancel，生产方终态为 Cancelled；
 * 3. 本地 SSE 收到 50 条且有序；
 * 4. Lou /stats /cross 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex27_streamref_pipeline
 *   ./target/release/bin/cactor_examples.ex27_streamref_pipeline serve 18112
 */
package cactor_examples.ex27_streamref_pipeline

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.typed.TypedPromise
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.SimpleRemoteManager
import cactor_cluster.streamref.*

let WAIT = Duration.second * 20

class Stats {
    let lock = Mutex()
    let counts = HashMap<String, Int64>()
    let completed = HashSet<String>()
    let errors = HashMap<String, String>()
    let orderOk = HashMap<String, Bool>()
    let cancelled = HashSet<String>()

    func onNext(id: String, x: String): Unit {
        synchronized(lock) {
            let n = counts.get(id) ?? 0
            let expect = "e${n}"
            orderOk[id] = (orderOk.get(id) ?? true) && (x == expect)
            counts[id] = n + 1
        }
    }

    func onComplete(id: String): Unit {
        synchronized(lock) {
            completed.add(id)
        }
    }

    func onError(id: String, msg: String): Unit {
        synchronized(lock) {
            errors[id] = msg
        }
    }

    func onCancel(id: String): Unit {
        synchronized(lock) {
            cancelled.add(id)
        }
    }

    func count(id: String): Int64 {
        synchronized(lock) {
            counts.get(id) ?? 0
        }
    }

    func isCompleted(id: String): Bool {
        synchronized(lock) {
            completed.contains(id)
        }
    }

    func ordered(id: String): Bool {
        synchronized(lock) {
            orderOk.get(id) ?? false
        }
    }

    func error(id: String): String {
        synchronized(lock) {
            errors.get(id) ?? ""
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{\"streams\":{")
            var first = true
            for ((id, n) in counts) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${id}\":{\"received\":${n},\"completed\":${completed.contains(id)},\"ordered\":${orderOk.get(id) ?? false},\"cancelled\":${cancelled.contains(id)}}")
            }
            sb.append("}}")
            sb.toString()
        }
    }
}

/** 消费端：收到 "sub|<id>\n<wire>" 订阅；"cancel|<id>" 取消（用于验证取消传播） */
class Collector <: Actor {
    let refs: StreamRefs
    let stats: Stats
    let lock = Mutex()
    let subs = HashMap<String, StreamRefSubscription>()
    var cancelAfter: Int64 = 0

    init(refs: StreamRefs, stats: Stats, cancelAfter: Int64) {
        this.refs = refs
        this.stats = stats
        this.cancelAfter = cancelAfter
    }

    public prop name: String { get() { "streamref-collector" } }
    public prop description: String { get() { "streamref consumer" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let text = s.getContent()
            if (text.startsWith("sub|")) {
                let nl = text.indexOf("\n")
                match (nl) {
                    case Some(i) =>
                        let id = text[4..i]
                        let wire = text[i + 1..]
                        let ref = SourceRef<String>.fromWireString(wire)
                        let sub = refs.subscribe<String>(ref,
                            onNext: { x =>
                                stats.onNext(id, x)
                                if (cancelAfter > 0 && stats.count(id) >= cancelAfter) {
                                    synchronized(lock) {
                                        match (subs.get(id)) {
                                            case Some(sub) => sub.cancel("示例：消费方取消")
                                            case None => ()
                                        }
                                    }
                                    stats.onCancel(id)
                                }
                            },
                            onComplete: { => stats.onComplete(id) },
                            onError: { e => stats.onError(id, e.message) })
                        synchronized(lock) {
                            subs[id] = sub
                        }
                    case None => ()
                }
            } else if (text.startsWith("cancel|")) {
                let id = text[7..]
                synchronized(lock) {
                    match (subs.get(id)) {
                        case Some(sub) => sub.cancel("示例：消费方取消")
                        case None => ()
                    }
                }
                stats.onCancel(id)
            }
        }
        MessageResult.Handled
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let refs: StreamRefs
    var stopped = false

    init(index: Int64, withRemote: Bool) {
        this.index = index
        this.system = CActorSystemFactory.create("ex27-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        this.refs = StreamRefs(system, remote: Some(remote))
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
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

/** 生产端：offer 失败（背压）时等 whenReady，Closed 时停止 */
func produce(producer: StreamProducer<String>, n: Int64): Unit {
    var i: Int64 = 0
    while (i < n) {
        match (producer.offer("e${i}")) {
            case OfferResult.Enqueued => i++
            case OfferResult.Backpressured =>
                if (!producer.whenReady().get(WAIT)) {
                    return
                }
            case OfferResult.Closed => return
        }
    }
    producer.complete()
}

func terminationName(t: StreamTermination): String {
    match (t) {
        case StreamTermination.Completed => "completed"
        case StreamTermination.Cancelled(_) => "cancelled"
        case StreamTermination.Failed(_) => "failed"
    }
}

/** A→B 起一条流，返回 (生产方, 终态) */
func startCross(nodeA: Node, target: ActorRef, id: String, n: Int64): (StreamProducer<String>, TypedPromise<StreamTermination>) {
    let (ref, producer) = nodeA.refs.sourceRef<String>(settings: StreamRefSettings().withBufferSize(4))
    target.tell(StringMessage("sub|${id}\n${ref.toWireString()}"))
    let term = producer.termination()
    spawn { produce(producer, n) }
    (producer, term)
}

func buildApp(nodeA: Node, target: ActorRef, stats: Stats, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/stream", { ctx =>
        let n = ctx.queryInt("n") ?? 50
        ctx.sse({ stream =>
            let (ref, producer) = nodeA.refs.sourceRef<String>(settings: StreamRefSettings().withBufferSize(4))
            let completed = AtomicBool(false)
            let sub = nodeA.refs.subscribe<String>(ref,
                onNext: { x => stream.sendEvent("msg", x) },
                onComplete: { => completed.store(true) },
                onError: { _ => completed.store(true) })
            produce(producer, n)
            for (_ in 0..1000) {
                if (completed.load()) {
                    break
                }
                sleep(Duration.millisecond * 10)
            }
            sub.cancel("SSE 客户端结束")
            stream.close()
        })
    })
    app.post("/cross", { ctx =>
        let n = ctx.queryInt("n") ?? 50
        let id = "http-${MonoTime.now().hashCode()}"
        let (_, term) = startCross(nodeA, target, id, n)
        let ok = waitUntil(WAIT, { => stats.isCompleted(id) })
        ctx.setSingleHeader("Content-Type", "application/json")
        if (ok) {
            ctx.writeString("{\"stream\":\"${id}\",\"received\":${stats.count(id)},\"termination\":\"${terminationName(term.get(WAIT))}\"}")
        } else {
            ctx.writeStringWithCode(500u16, "{\"error\":\"stream_incomplete\"}")
        }
    })
    app.get("/stats", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(stats.json())
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

func sseData(body: String): Array<String> {
    let out = ArrayList<String>()
    for (line in body.split("\n")) {
        let t = line.trimAscii()
        if (t.startsWith("data:")) {
            out.add(t[5..].trimAscii())
        }
    }
    out.toArray()
}

main(args: Array<String>): Int64 {
    let stats = Stats()
    let nodeA = Node(0, true)
    let nodeB = Node(1, true)
    let collector = nodeB.system.actorOf({ => Collector(nodeB.refs, stats, 0) }, "collector")
    nodeB.remote.exposeActor("collector", collector)
    let target = nodeA.remote.remoteActorOf(nodeB.remote.getLocalAddress(), "collector")

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(nodeA, target, stats, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex27：端口绑定失败")
            return 1
        }
        println("ex27 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let app = buildApp(nodeA, target, stats, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. 跨节点 200 条，背压 ----
        println("\n[1] A→B 传输 200 条（buffer=4，背压）")
        let (_, term1) = startCross(nodeA, target, "s1", 200)
        let done1 = waitUntil(WAIT, { => stats.isCompleted("s1") })
        c.check(done1, "B 侧 onComplete（收到 ${stats.count("s1")} 条）")
        c.check(stats.count("s1") == 200, "收到 200 条（实际 ${stats.count("s1")}）")
        c.check(stats.ordered("s1"), "顺序正确（e0…e199）")
        c.check(terminationName(term1.get(WAIT)) == "completed", "生产方终态 Completed")
        c.check(stats.error("s1").isEmpty(), "没有错误（${stats.error("s1")}）")

        // ---- 2. 取消传播 ----
        println("\n[2] 消费方取消后生产方停止")
        let collectorCancel = nodeB.system.actorOf({ => Collector(nodeB.refs, stats, 10) }, "collector-cancel")
        nodeB.remote.exposeActor("collector-cancel", collectorCancel)
        let targetCancel = nodeA.remote.remoteActorOf(nodeB.remote.getLocalAddress(), "collector-cancel")
        let (_, term2) = startCross(nodeA, targetCancel, "s2", 1000)
        let cancelled = waitUntil(WAIT, { => terminationName(term2.get(WAIT)) == "cancelled" })
        c.check(cancelled, "生产方终态 Cancelled（收到 ${stats.count("s2")} 条后取消）")
        c.check(stats.count("s2") <= 20, "取消后不再大量传输（${stats.count("s2")}）")

        // ---- 3. 本地 SSE ----
        println("\n[3] 本地 SSE：50 条有序")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 30).build()
        let resp = client.get("http://127.0.0.1:${port}/stream?n=50")
        let data = sseData(readBody(resp))
        var ordered = data.size == 50
        for (i in 0..data.size) {
            if (data[i] != "e${i}") {
                ordered = false
            }
        }
        c.check(ordered, "SSE 收到 50 条且有序（实际 ${data.size}）")

        // ---- 4. Lou 跨节点 / 统计 ----
        println("\n[4] Lou /cross 与 /stats")
        let cross = client.post("http://127.0.0.1:${port}/cross?n=30", "")
        let body = readBody(cross)
        c.check(cross.status == 200 && body.contains("\"received\":30") && body.contains("\"termination\":\"completed\""),
            "POST /cross 完成 30 条：${body}")
        let st = readBody(client.get("http://127.0.0.1:${port}/stats"))
        c.check(st.contains("\"s1\"") && st.contains("\"completed\":true"), "GET /stats 含 s1 完成状态")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        nodeA.shutdown()
        nodeB.shutdown()
    }

    println("\nex27：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
