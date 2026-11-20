/*
 * CActor Ex 35 - OTLP 导出（差距 4.10 的导出部分：导出器接口 + 批量 + 失败隔离 + OTLP/JSON）
 *
 * 真实约束：
 * - 采集端抖动、collector 不可达是常态：导出失败绝不能拖慢或打断业务；
 * - 批量发送：每条 span 一次 HTTP 请求会把业务线程拖死，必须缓冲；
 * - 导出内容是标准 OTLP/JSON，能直接喂给 collector（本示例用 curl 取 body 验证结构）；
 * - 默认不导出：没显式安装导出器时行为与之前完全一致。
 *
 * 组件：
 * - 框架：`SpanExporter` / `BatchSpanExporter` / `InMemorySpanExporter` / `ThrowingSpanExporter` /
 *   `toOtlpJson`；`Tracer.attachExporter`
 * - 拓扑：2 个真实 TCP 节点（网关 + 订单服务），W3C traceparent 逐跳接续（复用 ex32 的传播原语）
 * - Lou：GET /v1/traces?node=、GET /spans、POST /orders、GET /healthz
 *
 * 自检（退出码 0/1）：
 * 1. 2 笔订单：两侧 span 的 traceId 相同，订单侧 parent = 网关的发送 span；
 * 2. 每侧 BatchSpanExporter 导出数量与 span 数一致、无失败；
 * 3. 注入总抛异常的导出器：finishSpan 不抛、失败被计数、本地缓冲不受影响；
 * 4. GET /v1/traces 返回的 OTLP JSON 含 service.name / traceId / parentSpanId。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex35_otlp_export
 *   ./target/release/bin/cactor_examples.ex35_otlp_export serve 18120
 */
package cactor_examples.ex35_otlp_export

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.monitoring.{BatchSpanExporter, InMemorySpanExporter, Span, SpanRecord,
    ThrowingSpanExporter, TraceContext, Tracer, continueTrace, toOtlpJson, toTraceParent}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{SimpleRemoteManager, encodeRemoteSender}

let WAIT = Duration.second * 20

class Node {
    let name: String
    let service: String
    let system: CActorSystem
    let remote: SimpleRemoteManager
    let sink = InMemorySpanExporter()
    let batch = BatchSpanExporter(sink, batchSize: 1)
    let tracer: Tracer
    var stopped = false

    init(name: String, service: String) {
        this.name = name
        this.service = service
        this.system = CActorSystemFactory.create(name)
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        this.tracer = Tracer(service)
        this.tracer.attachExporter(batch)
    }

    func address(): String {
        remote.getLocalAddress().toString()
    }

    /** 把完成的 span 导出并取快照（演示抓取时 flush 一次） */
    func records(): Array<SpanRecord> {
        batch.flush()
        sink.snapshot()
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            remote.stop()
            system.terminate()
        }
    }
}

/** 网关侧：发起订单 + 接收订单服务的完成回复 */
class GatewayReply <: Actor {
    let gateway: Node

    init(gateway: Node) {
        this.gateway = gateway
    }

    public prop name: String { get() { "gateway-reply" } }
    public prop description: String { get() { "gateway reply" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let text = s.getContent()
            // done|<traceparent>|<orderId>
            if (text.startsWith("done|")) {
                let rest = text[5..]
                match (rest.indexOf("|")) {
                    case Some(i) =>
                        let tp = rest[0..i]
                        let orderId = rest[(i + 1)..]
                        match (continueTrace(tp)) {
                            case Some(ctx) =>
                                let span = Span(ctx.traceId, ctx.spanId, ctx.parentSpanId, "complete-order")
                                span.setTag("order.id", orderId)
                                gateway.tracer.finishSpan(span)
                            case None => ()
                        }
                    case None => ()
                }
            }
        }
        MessageResult.Handled
    }
}

/** 订单服务侧：接续上游 trace，处理并回复 */
class OrderService <: Actor {
    let orders: Node
    let gatewayAddress: String

    init(orders: Node, gatewayAddress: String) {
        this.orders = orders
        this.gatewayAddress = gatewayAddress
    }

    public prop name: String { get() { "order-service" } }
    public prop description: String { get() { "order service" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            let text = s.getContent()
            // order|<traceparent>|<orderId>
            if (text.startsWith("order|")) {
                let rest = text[6..]
                match (rest.indexOf("|")) {
                    case Some(i) =>
                        let tp = rest[0..i]
                        let orderId = rest[(i + 1)..]
                        match (continueTrace(tp)) {
                            case Some(ctx) =>
                                let span = Span(ctx.traceId, ctx.spanId, ctx.parentSpanId, "process-order")
                                span.setTag("order.id", orderId)
                                orders.tracer.finishSpan(span)
                                // 回复：把本地 span 作为上游 parent 发回网关
                                orders.remote.sendMessage(NetworkAddress.parse(gatewayAddress), "gateway-reply",
                                    StringMessage("done|${toTraceParent(ctx)}|${orderId}"))
                            case None => ()
                        }
                    case None => ()
                }
            }
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

/** 网关发起一笔订单（同步生成两个 span：受理 + 发送） */
func issueOrder(gateway: Node, orders: Node, orderId: String): Unit {
    let root = gateway.tracer.startSpan("handle-order")
    root.setTag("order.id", orderId)
    let hop = gateway.tracer.startChildSpan(root, "send-to-orders")
    // 用 hop 的 spanId 作为上游 parent-id
    let ctx = TraceContext(root.traceId, hop.spanId, Some(root.spanId), HashMap<String, String>())
    gateway.tracer.finishSpan(hop)
    gateway.tracer.finishSpan(root)
    let sender = encodeRemoteSender(gateway.remote.getLocalAddress(), "gateway-reply")
    gateway.remote.sendMessage(orders.remote.getLocalAddress(), "orders",
        StringMessage("order|${toTraceParent(ctx)}|${orderId}"), Some(sender))
}

func spansJson(records: Array<SpanRecord>): String {
    let sb = StringBuilder()
    sb.append("{\"spans\":[")
    var first = true
    for (r in records) {
        if (!first) {
            sb.append(",")
        }
        first = false
        let parent = match (r.parentSpanId) {
            case Some(p) => p
            case None => ""
        }
        sb.append("{\"service\":\"${r.service}\",\"traceId\":\"${r.traceId}\",\"spanId\":\"${r.spanId}\"," +
            "\"parentSpanId\":\"${parent}\",\"name\":\"${r.name}\",\"durationMs\":${r.durationMillis}}")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(orders: Node, gateway: Node, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/orders", { ctx =>
        let n = ctx.queryInt("n") ?? 1
        for (i in 0..n) {
            issueOrder(gateway, orders, "order-${i}")
        }
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"issued\":${n}}")
    })
    app.get("/v1/traces", { ctx =>
        let node = ctx.query("node") ?? "gateway"
        let target = if (node == "orders") { orders } else { gateway }
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(toOtlpJson(target.service, target.records()))
    })
    app.get("/spans", { ctx =>
        let node = ctx.query("node") ?? "gateway"
        let target = if (node == "orders") { orders } else { gateway }
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(spansJson(target.records()))
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
    let gateway = Node("ex35-gateway", "gateway")
    let orders = Node("ex35-orders", "orders")
    let service = orders.system.actorOf({ => OrderService(orders, gateway.address()) }, "orders")
    orders.remote.exposeActor("orders", service)
    let reply = gateway.system.actorOf({ => GatewayReply(gateway) }, "gateway-reply")
    gateway.remote.exposeActor("gateway-reply", reply)

    let app = buildApp(orders, gateway, 0u16)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let real = buildApp(orders, gateway, port)
        let actual = startServer(real)
        if (actual == 0u16) {
            println("ex35：端口绑定失败")
            return 1
        }
        println("ex35 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        println("\n[1] 2 笔订单：两侧 span 同一 trace、父子相接")
        issueOrder(gateway, orders, "order-a")
        issueOrder(gateway, orders, "order-b")
        // 每笔订单：网关 handle-order + send-to-orders + complete-order（3 个），订单侧 process-order（1 个）
        c.check(waitUntil(WAIT, { => orders.records().size == 2 && gateway.records().size == 6 }),
            "订单侧 2 个 process span、网关侧 6 个 span")
        let gwSpans = gateway.records()
        let ordSpans = orders.records()
        var linked = true
        for (o in ordSpans) {
            var parentFound = false
            for (g in gwSpans) {
                if (g.traceId == o.traceId && g.spanId == (o.parentSpanId ?? "")) {
                    parentFound = true
                }
            }
            linked = linked && parentFound
        }
        c.check(linked, "每个订单 span 的 parent 都是网关的 send-to-orders span")

        println("\n[2] 批量导出：数量正确、无失败")
        c.check(gateway.batch.exportedCount() == 6, "网关导出 6 条（实际 ${gateway.batch.exportedCount()}）")
        c.check(orders.batch.exportedCount() == 2, "订单导出 2 条（实际 ${orders.batch.exportedCount()}）")
        c.check(gateway.batch.failureCount() == 0 && orders.batch.failureCount() == 0, "无导出失败")

        println("\n[3] 导出器抛异常不影响业务")
        let bad = ThrowingSpanExporter()
        let badTracer = Tracer("broken-exporter")
        badTracer.attachExporter(BatchSpanExporter(bad, batchSize: 1))
        let s1 = badTracer.startSpan("business-work")
        badTracer.finishSpan(s1)  // 不得抛异常
        c.check(bad.attempts.load() == 1, "下游收到一次导出尝试")
        c.check(badTracer.getFinishedSpanCount() == 1, "本地缓冲仍然完整")
        // 失败计数通过 BatchSpanExporter 自身暴露
        let failingBatch = BatchSpanExporter(ThrowingSpanExporter(), batchSize: 1)
        failingBatch.enqueue(SpanRecord("svc", "t", "s", None<String>, "op", 0, 1, HashMap<String, String>()))
        c.check(failingBatch.failureCount() == 1, "失败被计数（1）")

        println("\n[4] OTLP JSON 结构")
        let otlp = toOtlpJson(gateway.service, gateway.records())
        c.check(otlp.contains("\"service.name\"") && otlp.contains("\"traceId\"") &&
            otlp.contains("\"parentSpanId\"") && otlp.contains("send-to-orders"),
            "OTLP/JSON 含 service.name / traceId / parentSpanId / span 名")

        println("\n[5] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let body = readBody(client.get("http://127.0.0.1:${port}/v1/traces?node=orders"))
        c.check(body.contains("\"service.name\"") && body.contains("\"stringValue\":\"orders\""),
            "GET /v1/traces?node=orders 返回订单服务的 OTLP JSON")
        let spans = readBody(client.get("http://127.0.0.1:${port}/spans?node=gateway"))
        c.check(spans.contains("send-to-orders") && spans.contains("\"parentSpanId\""),
            "GET /spans 含网关 span 与父子关系")
        let post = readBody(client.post("http://127.0.0.1:${port}/orders?n=1", ""))
        c.check(post.contains("\"issued\":1"), "POST /orders 触发新链路")
        c.check(waitUntil(WAIT, { => gateway.records().size >= 9 }),
            "新链路再产生 3 个网关 span")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        gateway.shutdown()
        orders.shutdown()
    }

    println("\nex35：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
