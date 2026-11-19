/*
 * CActor Ex 32 - 跨节点追踪关联（差距 4.10 的应用层补法：W3C traceparent + Mdc）
 *
 * 真实约束：
 * - 线上排障要知道"这一条请求在两台机器上的日志是哪几条"：必须有一个贯穿两侧的 traceId；
 * - traceId 由入口生成，随消息传播；每一跳用上游 span 作 parent 生成自己的 span，
 *   否则只能看到"同一个 trace"，看不到调用先后；
 * - 传播字段必须兼容 W3C traceparent，方便与网关 / OTel 生态对接；坏输入不能打断业务。
 *
 * 框架已提供（本轮补齐的原语）：
 * - `TraceContext` / `generateTraceId` / `generateSpanId`（foundation，已有）
 * - `toTraceParent` / `parseTraceParent` / `continueTrace`（本轮新增，W3C 格式）
 * - `traceMdcFields` / `withTraceMdc`（本轮新增：块内把 traceId/spanId 注入日志字段）
 *
 * 组件：
 * - 2 个真实 TCP 节点：A = HTTP 网关，B = 订单服务
 * - 日志用 `Logging.setSink` 捕获（证明两侧日志记录带同一个 traceId）
 * - Lou：POST /orders、GET /traces、GET /logs、POST /orders?bad=1（坏 traceparent）
 *
 * 自检（退出码 0/1）：
 * 1. 3 个订单：每条 trace 的 gateway / orders / reply 记录 traceId 相同，
 *    orders 的 parentSpanId 等于网关根 span，reply 的 parent 等于 orders 的 span；
 * 2. 捕获的日志：网关与订单服务两侧的记录都带同一个 traceId（Mdc 生效）；
 * 3. 坏 traceparent：订单服务新建根上下文，不崩溃；
 * 4. Lou /traces、/logs 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex32_distributed_trace
 *   ./target/release/bin/cactor_examples.ex32_distributed_trace serve 18117
 */
package cactor_examples.ex32_distributed_trace

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.logging.{LogLevel, LogRecord, LogSink, Logging}
import cactor_foundation.monitoring.{TraceContext, continueTrace, generateTraceId, toTraceParent, withTraceMdc}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager, encodeRemoteSender}

let WAIT = Duration.second * 20

/** 捕获日志记录（证明 Mdc 字段确实出现在日志里） */
class CaptureSink <: LogSink {
    private let lock = Mutex()
    private let records = ArrayList<LogRecord>()

    public func write(record: LogRecord): Unit {
        synchronized(lock) {
            records.add(record)
        }
    }

    func snapshot(): Array<LogRecord> {
        synchronized(lock) {
            records.toArray()
        }
    }

    /** 某来源的记录里出现了多少个不同 traceId */
    func traceIdsFor(source: String): HashSet<String> {
        let out = HashSet<String>()
        synchronized(lock) {
            for (r in records) {
                if (r.source == source) {
                    if (let Some(t) <- r.field("traceId")) {
                        out.add(t)
                    }
                }
            }
        }
        out
    }
}

/** 一次链路记录：谁、哪个 trace/span、parent 是谁、走到哪一步 */
class TraceRecord {
    let node: String
    let traceId: String
    let spanId: String
    let parentSpanId: String
    let event: String
    let orderId: String

    init(node: String, traceId: String, spanId: String, parentSpanId: String, event: String, orderId: String) {
        this.node = node
        this.traceId = traceId
        this.spanId = spanId
        this.parentSpanId = parentSpanId
        this.event = event
        this.orderId = orderId
    }
}

class Traces {
    private let lock = Mutex()
    private let items = ArrayList<TraceRecord>()

    func add(r: TraceRecord): Unit {
        synchronized(lock) {
            items.add(r)
        }
    }

    func snapshot(): Array<TraceRecord> {
        synchronized(lock) {
            items.toArray()
        }
    }

    func countFor(orderId: String): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for (r in items) {
                if (r.orderId == orderId) {
                    n++
                }
            }
            n
        }
    }

    /** 某订单的四条事件（按写入顺序） */
    func eventsFor(orderId: String): Array<TraceRecord> {
        let out = ArrayList<TraceRecord>()
        synchronized(lock) {
            for (r in items) {
                if (r.orderId == orderId) {
                    out.add(r)
                }
            }
        }
        out.toArray()
    }
}

/** 网关侧：收到订单服务的结果回复 */
class GatewayReply <: Actor {
    let traces: Traces
    let log = Logging.logger("ex32.gateway.reply")

    init(traces: Traces) {
        this.traces = traces
    }

    public prop name: String { get() { "gateway-reply" } }
    public prop description: String { get() { "gateway reply handler" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let text = s.getContent()
            // reply|<traceparent>|<orderId>|<status>
            if (text.startsWith("reply|")) {
                let rest = text[6..]
                match (rest.indexOf("|")) {
                    case Some(i) =>
                        let tp = rest[0..i]
                        let tail = rest[(i + 1)..]
                        match (tail.indexOf("|")) {
                            case Some(j) =>
                                let orderId = tail[0..j]
                                let ctx = match (continueTrace(tp)) {
                                    case Some(c) => c
                                    case None => TraceContext.createRoot(generateTraceId())
                                }
                                let parent = match (ctx.parentSpanId) {
                                    case Some(p) => p
                                    case None => ""
                                }
                                withTraceMdc(ctx, { =>
                                    log.info("订单 ${orderId} 处理完成")
                                })
                                traces.add(TraceRecord("gateway", ctx.traceId, ctx.spanId, parent, "completed", orderId))
                            case None => ()
                        }
                    case None => ()
                }
            }
        }
        MessageResult.Handled
    }
}

/** 订单服务侧：接续上游 trace，处理后经 context.reply 回网关 */
class OrderService <: Actor {
    let traces: Traces
    let log = Logging.logger("ex32.orders")

    init(traces: Traces) {
        this.traces = traces
    }

    public prop name: String { get() { "order-service" } }
    public prop description: String { get() { "order service" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let text = s.getContent()
            // order|<traceparent|bad>|<orderId>
            if (text.startsWith("order|")) {
                let rest = text[6..]
                match (rest.indexOf("|")) {
                    case Some(i) =>
                        let tp = rest[0..i]
                        let orderId = rest[(i + 1)..]
                        let ctx = match (continueTrace(tp)) {
                            case Some(c) => c
                            case None => TraceContext.createRoot(generateTraceId())
                        }
                        let parent = match (ctx.parentSpanId) {
                            case Some(p) => p
                            case None => ""
                        }
                        withTraceMdc(ctx, { =>
                            log.info("开始处理订单 ${orderId}")
                        })
                        traces.add(TraceRecord("orders", ctx.traceId, ctx.spanId, parent, "processed", orderId))
                        context.reply(StringMessage("reply|${toTraceParent(ctx)}|${orderId}|ok"))
                    case None => ()
                }
            }
        }
        MessageResult.Handled
    }
}

class Node {
    let system: CActorSystem
    let remote: SimpleRemoteManager
    var stopped = false

    init(name: String) {
        this.system = CActorSystemFactory.create(name)
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
    }

    func address(): String {
        remote.getLocalAddress().toString()
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

/** 网关发起一笔订单：生成根 trace，带着 traceparent 发给订单服务 */
func issueOrder(gateway: Node, orders: Node, traces: Traces, orderId: String, badTrace: Bool): Unit {
    let log = Logging.logger("ex32.gateway")
    let root = TraceContext.createRoot(generateTraceId())
    let tp = if (badTrace) { "garbage-traceparent" } else { toTraceParent(root) }
    withTraceMdc(root, { =>
        log.info("受理订单 ${orderId}")
    })
    traces.add(TraceRecord("gateway", root.traceId, root.spanId, "", "received", orderId))
    let sender = encodeRemoteSender(gateway.remote.getLocalAddress(), "gateway-reply")
    gateway.remote.sendMessage(orders.remote.getLocalAddress(), "orders",
        StringMessage("order|${tp}|${orderId}"), Some(sender))
}

func tracesJson(traces: Traces): String {
    let sb = StringBuilder()
    sb.append("{\"events\":[")
    var first = true
    for (r in traces.snapshot()) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("{\"node\":\"${r.node}\",\"traceId\":\"${r.traceId}\",\"spanId\":\"${r.spanId}\"," +
            "\"parentSpanId\":\"${r.parentSpanId}\",\"event\":\"${r.event}\",\"orderId\":\"${r.orderId}\"}")
    }
    sb.append("]}")
    sb.toString()
}

func logsJson(sink: CaptureSink): String {
    let sb = StringBuilder()
    sb.append("{\"logs\":[")
    var first = true
    for (r in sink.snapshot()) {
        if (!r.source.startsWith("ex32.")) {
            continue
        }
        if (!first) {
            sb.append(",")
        }
        first = false
        let traceId = r.field("traceId") ?? ""
        let spanId = r.field("spanId") ?? ""
        sb.append("{\"source\":\"${r.source}\",\"traceId\":\"${traceId}\",\"spanId\":\"${spanId}\",\"message\":\"${r.message}\"}")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(gateway: Node, orders: Node, traces: Traces, sink: CaptureSink, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/orders", { ctx =>
        let n = ctx.queryInt("n") ?? 1
        let bad = (ctx.query("bad") ?? "0") == "1"
        var issued = 0
        for (i in 0..n) {
            let id = "order-${traces.snapshot().size}-${i}"
            issueOrder(gateway, orders, traces, id, bad)
            issued++
        }
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"issued\":${issued},\"bad\":${bad}}")
    })
    app.get("/traces", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(tracesJson(traces))
    })
    app.get("/logs", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(logsJson(sink))
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
    let traces = Traces()
    let sink = CaptureSink()
    Logging.setSink(sink)
    // 日志被捕获到内存用于断言，同时显式设成 info：CI 常以 CACTOR_LOG_LEVEL=error 启动，
    // 否则 info 日志在 emit 前就被过滤，捕获不到 traceId 字段
    Logging.setLevel(LogLevel.Info)

    let gateway = Node("ex32-gateway")
    let orders = Node("ex32-orders")

    let orderService = orders.system.actorOf({ => OrderService(traces) }, "orders")
    orders.remote.exposeActor("orders", orderService)
    let reply = gateway.system.actorOf({ => GatewayReply(traces) }, "gateway-reply")
    gateway.remote.exposeActor("gateway-reply", reply)

    let app = buildApp(gateway, orders, traces, sink, 0u16)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let real = buildApp(gateway, orders, traces, sink, port)
        let actual = startServer(real)
        if (actual == 0u16) {
            println("ex32：端口绑定失败")
            return 1
        }
        println("ex32 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        println("\n[1] 3 笔订单：traceId 贯穿两侧、span 逐跳接续")
        for (i in 0..3) {
            issueOrder(gateway, orders, traces, "order-${i}", false)
        }
        var complete = true
        for (i in 0..3) {
            complete = complete && waitUntil(WAIT, { => traces.countFor("order-${i}") == 3 })
        }
        c.check(complete, "3 笔订单各产生 gateway/orders/reply 三条记录")
        var chainOk = true
        for (i in 0..3) {
            let events = traces.eventsFor("order-${i}")
            let gatewayEvent = events[0]
            let ordersEvent = events[1]
            let replyEvent = events[2]
            chainOk = chainOk && gatewayEvent.node == "gateway" && ordersEvent.node == "orders" &&
                replyEvent.node == "gateway" &&
                gatewayEvent.traceId == ordersEvent.traceId && ordersEvent.traceId == replyEvent.traceId &&
                ordersEvent.parentSpanId == gatewayEvent.spanId &&
                replyEvent.parentSpanId == ordersEvent.spanId
        }
        c.check(chainOk, "同一 traceId，父 span 逐跳相接")

        println("\n[2] Mdc：两侧日志都带同一个 traceId")
        let gatewayIds = sink.traceIdsFor("ex32.gateway")
        let ordersIds = sink.traceIdsFor("ex32.orders")
        let replyIds = sink.traceIdsFor("ex32.gateway.reply")
        c.check(gatewayIds.size == 3 && ordersIds.size == 3 && replyIds.size == 3,
            "三个来源各出现 3 个 traceId（网关 ${gatewayIds.size} / 订单 ${ordersIds.size} / 回复 ${replyIds.size}）")
        var overlap = true
        for (t in ordersIds) {
            overlap = overlap && gatewayIds.contains(t) && replyIds.contains(t)
        }
        c.check(overlap, "订单服务与回复侧的 traceId 全部来自网关")

        println("\n[3] 坏 traceparent：不崩溃，另起根上下文")
        issueOrder(gateway, orders, traces, "order-bad", true)
        c.check(waitUntil(WAIT, { => traces.countFor("order-bad") == 3 }),
            "坏输入仍然完成处理（${traces.countFor("order-bad")} 条）")
        let badEvents = traces.eventsFor("order-bad")
        c.check(badEvents[1].parentSpanId.isEmpty() && !ordersIds.contains(badEvents[1].traceId),
            "订单服务为坏输入新建了根上下文")

        println("\n[4] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let tr = readBody(client.get("http://127.0.0.1:${port}/traces"))
        c.check(tr.contains("\"orderId\":\"order-0\"") && tr.contains("\"parentSpanId\":"),
            "GET /traces 含链路记录")
        let lg = readBody(client.get("http://127.0.0.1:${port}/logs"))
        c.check(lg.contains("ex32.gateway") && lg.contains("ex32.orders") && lg.contains("\"traceId\":\""),
            "GET /logs 两侧日志都带 traceId")
        let bad = readBody(client.post("http://127.0.0.1:${port}/orders?n=1&bad=1", ""))
        c.check(bad.contains("\"bad\":true"), "POST /orders?bad=1 受理坏 traceparent")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        gateway.shutdown()
        orders.shutdown()
    }

    println("\nex32：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
