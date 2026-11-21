/*
 * CActor Ex 50 - 本地 Actor 链路的 trace 自动传播（差距 4.10 剩余部分）
 *
 * 真实约束：
 * - 一次外部请求进入系统后，往往在多个 Actor 之间接力（校验 → 库存 → 审计）；
 *   排障时要能只凭 traceId 把这几个 Actor 的日志串起来，而不是每个 Actor 手工传 context；
 * - trace 只有整条链路一致才有意义：任一环断掉（没有续接），日志就无法关联；
 * - 没有发起追踪的调用（后台定时、系统消息）不应凭空产生 traceId。
 *
 * 组件：
 * - 框架：runtime 在 `ActorCell.enqueue` 自动把"当前线程处于追踪作用域"的消息包成
 *   `TracedMessage`；接收方 `invoke` 续接出本地子 span 并建立 MDC 作用域
 *   （`foundation/logging` 的 `Mdc.currentTraceParent`，本轮新增）
 * - 应用：`TraceReport` 记录每个 Actor 在 receive 期间看到的 traceId/spanId
 * - Lou：GET /order、GET /healthz
 *
 * 自检（退出码 0/1）：
 * 1. 三跳链路：三个 Actor 的 traceId 一致，spanId 两两不同；
 * 2. 两次请求得到不同的 traceId（根 span 不复用）；
 * 3. Lou /order 返回的 hops 与内存一致；
 * 4. 不经追踪作用域直接 tell：接收方 MDC 为空。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex50_trace_chain
 *   ./target/release/bin/cactor_examples.ex50_trace_chain serve 18195
 */
package cactor_examples.ex50_trace_chain

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.logging.{Mdc}
import cactor_foundation.monitoring.{TraceContext, generateTraceId, withTraceMdc}

let WAIT = Duration.second * 10

/** 链路上每个 Actor 记录一次：actor|traceId|spanId */
class TraceReport {
    private let lock = Mutex()
    private var root = ""
    private let hops = ArrayList<String>()

    func reset(rootTrace: String): Unit {
        synchronized(lock) {
            root = rootTrace
            hops.clear()
        }
    }

    func record(actor: String): Unit {
        let traceId = Mdc.get("traceId") ?? ""
        let spanId = Mdc.get("spanId") ?? ""
        synchronized(lock) {
            hops.add("${actor}|${traceId}|${spanId}")
        }
    }

    func size(): Int64 {
        synchronized(lock) {
            Int64(hops.size)
        }
    }

    func snapshot(): (String, Array<String>) {
        synchronized(lock) {
            (root, hops.toArray())
        }
    }
}

/** 收到消息就记录当前追踪上下文，再告诉下一跳（没有下一跳就结束） */
class ChainActor <: Actor {
    let id: String
    let report: TraceReport
    let next: Option<ActorRef>

    init(id: String, report: TraceReport, next: Option<ActorRef>) {
        this.id = id
        this.report = report
        this.next = next
    }

    public prop name: String { get() { "chain-${id}" } }
    public prop description: String { get() { "trace chain ${id}" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        report.record(id)
        match (next) {
            case Some(n) => n.tell(StringMessage("next"))
            case None => ()
        }
        MessageResult.Handled
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

func field(line: String, index: Int64): String {
    let parts = line.split("|")
    if (index < parts.size) {
        parts[index]
    } else {
        ""
    }
}

func hopsJson(report: TraceReport): String {
    let (root, hops) = report.snapshot()
    let sb = StringBuilder()
    sb.append("{\"traceId\":\"${root}\",\"hops\":[")
    var first = true
    for (line in hops) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("{\"actor\":\"${field(line, 0)}\",\"traceId\":\"${field(line, 1)}\"," +
            "\"spanId\":\"${field(line, 2)}\"}")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(entry: ActorRef, report: TraceReport, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/order", { ctx =>
        let root = TraceContext.createRoot(generateTraceId())
        report.reset(root.traceId)
        withTraceMdc(root, { => entry.tell(StringMessage("order")) })
        let _ = waitUntil(WAIT, { => report.size() == 3 })
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(hopsJson(report))
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

func distinctSpans(hops: Array<String>): Int64 {
    let spans = HashSet<String>()
    for (line in hops) {
        spans.add(field(line, 2))
    }
    Int64(spans.size)
}

func allShareTrace(hops: Array<String>): Bool {
    let t = field(hops[0], 1)
    if (t.isEmpty()) {
        return false
    }
    for (line in hops) {
        if (field(line, 1) != t) {
            return false
        }
    }
    true
}

main(args: Array<String>): Int64 {
    let system = CActorSystemFactory.create("ex50")
    let report = TraceReport()
    let audit = system.actorOf({ => ChainActor("audit", report, None<ActorRef>) }, "chain-audit")
    let inventory = system.actorOf({ => ChainActor("inventory", report, Some<ActorRef>(audit)) }, "chain-inventory")
    let entry = system.actorOf({ => ChainActor("validate", report, Some<ActorRef>(inventory)) }, "chain-validate")

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let server = buildApp(entry, report, port)
        let actual = startServer(server)
        if (actual == 0u16) {
            println("ex50：端口绑定失败")
            return 1
        }
        println("ex50 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    try {
        println("\n[1] 三跳链路共享同一 traceId")
        let root1 = TraceContext.createRoot(generateTraceId())
        report.reset(root1.traceId)
        withTraceMdc(root1, { => entry.tell(StringMessage("order-1")) })
        c.check(waitUntil(WAIT, { => report.size() == 3 }), "三个 Actor 都处理了消息")
        let (rootSeen, hops1) = report.snapshot()
        c.check(rootSeen == root1.traceId, "根 traceId 一致")
        c.check(hops1.size == 3 && allShareTrace(hops1), "三跳 traceId 相同：${hops1}")
        c.check(distinctSpans(hops1) == 3, "三跳 spanId 两两不同")

        println("\n[2] 两次请求根 span 不同")
        let root2 = TraceContext.createRoot(generateTraceId())
        report.reset(root2.traceId)
        withTraceMdc(root2, { => entry.tell(StringMessage("order-2")) })
        c.check(waitUntil(WAIT, { => report.size() == 3 }), "第二次三跳完成")
        let (root2Seen, hops2) = report.snapshot()
        c.check(root2Seen == root2.traceId && root2.traceId != root1.traceId, "traceId 随请求变化")
        c.check(allShareTrace(hops2), "第二次三跳仍共享 traceId")

        println("\n[3] Lou /order")
        let server = buildApp(entry, report, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let body = readBody(client.get("http://127.0.0.1:${port}/order"))
            c.check(body.contains("\"validate\"") && body.contains("\"inventory\"") && body.contains("\"audit\""),
                "响应含三跳：${body}")
            c.check(!body.contains("\"traceId\":\"\""), "响应带 traceId")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\n[4] 无追踪作用域不产生 traceId")
        report.reset("")
        entry.tell(StringMessage("plain"))
        c.check(waitUntil(WAIT, { => report.size() == 3 }), "plain 链路完成")
        let (_, plainHops) = report.snapshot()
        c.check(field(plainHops[0], 1).isEmpty(), "直接 tell 时 receive 的 MDC 为空")
    } finally {
        system.terminate()
    }

    println("\nex50：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
