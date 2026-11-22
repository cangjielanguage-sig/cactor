/*
 * CActor Ex 61 - GraphDSL 扇出到 Actor（对标 Akka Streams → Actor 的 Sink.actorRef 用法）
 *
 * 真实约束：
 * - 同一份事件要同时驱动审计、指标、归档三个 Actor，每个 Actor 独立维护自己的状态；
 * - 流只负责投递，业务状态在 Actor 内串行演进；投递完成（Promise）后即可查询各 Actor；
 * - 三个 Actor 的计数必须一致，且与源元素总数一致。
 *
 * 组件：
 * - `GraphDSL.broadcast` + `Sinks.foreach`（每个汇把元素 tell 给一个 Actor）
 * - 经典 Actor（每 Actor 独立计数器）
 * - Lou：GET /fanout?n=、GET /state
 *
 * 自检（退出码 0/1）：
 * 1. 广播 10 条：三个 Actor 各收到 10 条、各自求和 55；
 * 2. 再广播 5 条：各累计 15 条；
 * 3. Lou /fanout 与 /state 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex61_stream_actor_fanout
 *   ./target/release/bin/cactor_examples.ex61_stream_actor_fanout serve 18205
 */
package cactor_examples.ex61_stream_actor_fanout

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.typed.{TypedPromise}
import cactor_streams.*

let WAIT = Duration.second * 20

/** 三个消费者共享的统计（Actor 内串行，读取方加锁） */
class Stats {
    private let lock = Mutex()
    private let counts = HashMap<String, Int64>()
    private let sums = HashMap<String, Int64>()

    func record(name: String, value: Int64): Unit {
        synchronized(lock) {
            counts[name] = (counts.get(name) ?? 0) + 1
            sums[name] = (sums.get(name) ?? 0) + value
        }
    }

    func countOf(name: String): Int64 {
        synchronized(lock) { counts.get(name) ?? 0 }
    }

    func sumOf(name: String): Int64 {
        synchronized(lock) { sums.get(name) ?? 0 }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{")
            var first = true
            for ((k, v) in counts) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${k}\":{\"count\":${v},\"sum\":${sums.get(k) ?? 0}}")
            }
            sb.append("}")
            sb.toString()
        }
    }
}

class CountingActor <: Actor {
    let label: String
    let stats: Stats

    init(label: String, stats: Stats) {
        this.label = label
        this.stats = stats
    }

    public prop name: String { get() { "counting-${label}" } }
    public prop description: String { get() { "counting consumer" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            match (Int64.tryParse(s.getContent())) {
                case Some(v) => stats.record(label, v)
                case None => ()
            }
        }
        MessageResult.Handled
    }
}

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

/** 把 1..n 广播给三个 Actor，等投递 Promise 完成（Actor 处理随后完成） */
func fanout(system: CActorSystem, n: Int64, refs: Array<ActorRef>, stats: Stats): Bool {
    let sinks = Array<Sink<Int64, TypedPromise<Unit>>>(refs.size, { i =>
        let ref = refs[i]
        Sinks.foreach<Int64>({ x => ref.tell(StringMessage("${x}")) })
    })
    let graph = GraphDSL.broadcast<Int64, Unit, TypedPromise<Unit>>(Sources.range(1, n), sinks)
    let (_, mats) = graph.run(system)
    for (m in mats) {
        m.get(WAIT)
    }
    // 投递完成不等于 Actor 已处理：等三个计数器都到 n（调用方按增量判断）
    true
}

func buildApp(system: CActorSystem, refs: Array<ActorRef>, stats: Stats, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/fanout", { ctx =>
        let n = ctx.queryInt("n") ?? 10
        let before = [stats.countOf("audit"), stats.countOf("metrics"), stats.countOf("archive")]
        let _ = fanout(system, n, refs, stats)
        // 投递完成不代表三个 Actor 都已处理：等到各自的增量都到齐再返回
        let ok = waitUntil(WAIT, { => stats.countOf("audit") >= before[0] + n &&
            stats.countOf("metrics") >= before[1] + n && stats.countOf("archive") >= before[2] + n })
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"ok\":${ok},\"stats\":${stats.json()}}")
    })
    app.get("/state", { ctx =>
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

main(args: Array<String>): Int64 {
    let system = CActorSystemFactory.create("ex61-stream-actor")
    let stats = Stats()
    let audit = system.actorOf({ => CountingActor("audit", stats) }, "audit")
    let metrics = system.actorOf({ => CountingActor("metrics", stats) }, "metrics")
    let archive = system.actorOf({ => CountingActor("archive", stats) }, "archive")
    let refs = [audit, metrics, archive]
    try {
        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            let server = buildApp(system, refs, stats, port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex61：端口绑定失败")
                return 1
            }
            println("ex61 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        println("\n[1] 广播 10 条到三个 Actor")
        let _ = fanout(system, 10, refs, stats)
        c.check(waitUntil(WAIT, { => stats.countOf("audit") == 10 && stats.countOf("metrics") == 10 &&
            stats.countOf("archive") == 10 }), "三个 Actor 各收到 10 条")
        c.check(stats.sumOf("audit") == 55 && stats.sumOf("metrics") == 55 && stats.sumOf("archive") == 55,
            "三个 Actor 各自求和 55")

        println("\n[2] 再广播 5 条，状态在 Actor 内累加")
        let _ = fanout(system, 5, refs, stats)
        c.check(waitUntil(WAIT, { => stats.countOf("audit") == 15 && stats.countOf("archive") == 15 }),
            "累计 15 条")

        println("\n[3] Lou 端点")
        let server = buildApp(system, refs, stats, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let fan = readBody(client.get("http://127.0.0.1:${port}/fanout?n=3"))
            c.check(fan.contains("\"ok\":true") && fan.contains("\"audit\":{\"count\":18,\"sum\":76}"),
                "GET /fanout：${fan}")
            let state = readBody(client.get("http://127.0.0.1:${port}/state"))
            c.check(state.contains("\"metrics\":{\"count\":18"), "GET /state：${state}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex61：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        system.terminate()
    }
}
