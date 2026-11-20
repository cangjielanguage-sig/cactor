/*
 * CActor Ex 43 - 多租户推理网关（docs/plan/2026117-lz.md §31 第 2 项）
 *
 * 真实约束：
 * - 租户配额必须强约束：超额请求不能触达上游（省钱、防滥用）；
 * - 上游会故障：连续失败后快速失败（断路器），但仍要把请求转移到健康副本；
 * - 流式输出：SSE 逐 token 返回，客户端拿到首字节的时间要短；
 * - 计费按成功请求计一次：发送方重试（同一请求）不能重复扣费；
 * - 运维要能看：每个租户的请求/拒绝/费用与上游断路器状态。
 *
 * 组件：
 * - 2 个真实 TCP 上游节点（经典 Actor，可注入失败）+ 网关发起 ask
 * - `BasicCircuitBreaker`（foundation）：每上游一个断路器
 * - `AtLeastOnceDelivery` + `IdempotentReceiver`（patterns，ex40）：计费幂等
 * - `FileDurableStateStore`：租户配额持久化
 * - Lou：POST /infer、GET /stream（SSE）、POST /upstream/fail、GET /metrics、GET /state
 *
 * 自检（退出码 0/1）：
 * 1. 租户 t1 配额 2：前两次成功，第三次 429 且上游调用数不增加；
 * 2. 上游 a 故障：请求转移到 b；连续失败后 a 的断路器打开，后续直接走 b；
 * 3. 计费：每次成功只扣一次（发送方重试两次，去重表生效）；
 * 4. SSE：t3 逐 token 输出 n 个事件，配额计 1 次；
 * 5. /metrics 输出可解析的 Prometheus 文本，/state 与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex43_inference_gateway
 *   ./target/release/bin/cactor_examples.ex43_inference_gateway serve 18188
 */
package cactor_examples.ex43_inference_gateway

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.circuit_breaker.{BasicCircuitBreaker, CircuitBreakerConfig, CircuitBreakerOpenException,
    CircuitBreakerState}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.{RemoteActorRef, SimpleRemoteManager}
import cactor_persistence.{DurableStateStore, FileDurableStateStore}
import cactor_patterns.delivery.*

let QUOTA_LIMIT: Int64 = 2
let ASK_TIMEOUT = Duration.second * 5
let WAIT = Duration.second * 15

/** 为简单起见使用独立函数构造 Actor，避免闭包捕获可变状态 */
class InferActor <: Actor {
    let node: String
    let failing: AtomicBool
    let calls: AtomicInt64

    init(node: String, failing: AtomicBool, calls: AtomicInt64) {
        this.node = node
        this.failing = failing
        this.calls = calls
    }

    public prop name: String { get() { "infer-${node}" } }
    public prop description: String { get() { "inference ${node}" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        let payload = if (let Some(t) <- (message as StringMessage)) { t.getContent() } else { "" }
        if (failing.load()) {
            calls.fetchAdd(1)
            context.reply(StringMessage("error:upstream-down"))
        } else {
            calls.fetchAdd(1)
            let parts = payload.split("|")
            let n = if (parts.size > 1) { Int64.parse(parts[1]) } else { 1 }
            let sb = StringBuilder()
            sb.append("by-${node}|")
            for (i in 1..(n + 1)) {
                if (i > 1) {
                    sb.append(" ")
                }
                sb.append("tok-${i}")
            }
            context.reply(StringMessage(sb.toString()))
        }
        MessageResult.Handled
    }
}

class Worker {
    let node: String
    let remote: SimpleRemoteManager
    let address: NetworkAddress
    let failing: AtomicBool
    let calls: AtomicInt64
    let breaker: BasicCircuitBreaker<String>

    init(system: CActorSystem, node: String) {
        this.node = node
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        this.address = remote.getLocalAddress()
        this.failing = AtomicBool(false)
        this.calls = AtomicInt64(0)
        let f = failing
        let c = calls
        let actor = system.actorOf({ => InferActor(node, f, c) }, "infer-${node}")
        remote.exposeActor("infer", actor)
        this.breaker = BasicCircuitBreaker<String>(
            CircuitBreakerConfig(2, 1, Duration.millisecond * 400, Duration.second * 2))
    }

    func stateName(): String {
        match (breaker.getState()) {
            case CircuitBreakerState.Closed => "closed"
            case CircuitBreakerState.Open => "open"
            case CircuitBreakerState.HalfOpen => "half_open"
        }
    }

    func stop(): Unit {
        remote.stop()
    }
}

/** 租户配额：持久化 used 计数，超额请求直接拒绝 */
class Quota {
    private let store: DurableStateStore
    private let lock = Mutex()

    init(dir: String) {
        this.store = FileDurableStateStore(dir)
    }

    private func key(tenant: String): String {
        "quota-${tenant}"
    }

    func usedOf(tenant: String): Int64 {
        synchronized(lock) {
            match (store.get(key(tenant))) {
                case Some(r) => Int64.parse(r.state)
                case None => 0
            }
        }
    }

    func take(tenant: String, limit: Int64): Bool {
        synchronized(lock) {
            let k = key(tenant)
            let used = match (store.get(k)) {
                case Some(r) => Int64.parse(r.state)
                case None => 0
            }
            if (used >= limit) {
                return false
            }
            store.upsert(k, store.revisionOf(k) + 1, "${used + 1}")
            true
        }
    }
}

/** 计费接收端：按 deliveryId 去重，只累计一次 */
class Billing {
    let remote: SimpleRemoteManager
    let receiver: IdempotentReceiver
    private let lock = Mutex()
    private let charges = HashMap<String, Int64>()

    init(dir: String) {
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
        this.receiver = IdempotentReceiver(DurableDeliveryIdStore(FileDurableStateStore("${dir}/dedup")))
        let book = charges
        let lockRef = lock
        let sink = receiver
        remote.registerHandler("billing", { m: Message =>
            if (let Some(e) <- (m as DeliveryEnvelope)) {
                if (let Some(text) <- (e.payload as StringMessage)) {
                    let tenant = text.getContent()
                    sink.handle(e, { _ => synchronized(lockRef) { book[tenant] = (book.get(tenant) ?? 0) + 1 } })
                }
            }
        })
    }

    func chargeOf(tenant: String): Int64 {
        synchronized(lock) {
            charges.get(tenant) ?? 0
        }
    }

    func total(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for ((_, v) in charges) {
                n += v
            }
            n
        }
    }

    func stop(): Unit {
        remote.stop()
    }
}

class Gateway {
    let workers: Array<Worker>
    let upstreamSystem: CActorSystem
    let quota: Quota
    let billing: Billing
    let delivery: AtLeastOnceDelivery
    let billingTarget: RemoteActorRef
    private let remoteForAsk: SimpleRemoteManager
    private let lock = Mutex()
    private var requests: Int64 = 0
    private var rejected: Int64 = 0
    private var successful: Int64 = 0
    private var upstreamFailures: Int64 = 0

    init(dir: String) {
        this.upstreamSystem = CActorSystemFactory.create("ex43-upstreams")
        let a = Worker(upstreamSystem, "a")
        let b = Worker(upstreamSystem, "b")
        this.workers = [a, b]
        this.quota = Quota("${dir}/quota")
        this.billing = Billing(dir)
        let gatewayRemote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        gatewayRemote.start()
        // 网关自己的发送管理器：ask 上游 + 投递计费
        this.delivery = AtLeastOnceDelivery(gatewayRemote)
        this.billingTarget = gatewayRemote.remoteActorOf(billing.remote.getLocalAddress(), "billing")
        this.remoteForAsk = gatewayRemote
    }

    /** 返回 (ok, 结果/错误码, 节点)，并完成计费（成功时） */
    func infer(tenant: String, prompt: String, n: Int64): (String, String) {
        synchronized(lock) {
            requests++
        }
        if (!quota.take(tenant, QUOTA_LIMIT)) {
            synchronized(lock) {
                rejected++
            }
            return ("429", "quota_exceeded")
        }
        var lastError = ""
        for (w in workers) {
            try {
                let target = remoteForAsk.remoteActorOf(w.address, "infer")
                // 校验必须放在 breaker.execute 内部：否则"收到了错误应答"会被当成成功，断路器永远不打开
                let text = w.breaker.execute({ =>
                    let reply = remoteForAsk.ask(target, StringMessage("${prompt}|${n}"), ASK_TIMEOUT)
                        .get(ASK_TIMEOUT)
                    let t = if (let Some(m) <- (reply as StringMessage)) { m.getContent() } else { "" }
                    if (t.startsWith("error:")) {
                        throw IllegalStateException(t)
                    }
                    t
                })
                synchronized(lock) {
                    successful++
                }
                // 计费：同一 deliveryId 发送两次，模拟发送方重试
                let env = delivery.envelope(StringMessage(tenant))
                delivery.resend(billingTarget, env)
                delivery.resend(billingTarget, env)
                return ("200", text)
            } catch (_: CircuitBreakerOpenException) {
                lastError = "circuit_open:${w.node}"
            } catch (e: Exception) {
                synchronized(lock) {
                    upstreamFailures++
                }
                lastError = "upstream_failed:${w.node}"
            }
        }
        ("503", lastError)
    }

    func setFailing(node: String, fail: Bool): Unit {
        for (w in workers) {
            if (w.node == node) {
                w.failing.store(fail)
            }
        }
    }

    func metrics(): String {
        let sb = StringBuilder()
        synchronized(lock) {
            sb.append("# TYPE cactor_gateway_requests_total counter\n")
            sb.append("cactor_gateway_requests_total ${requests}\n")
            sb.append("# TYPE cactor_gateway_rejected_total counter\n")
            sb.append("cactor_gateway_rejected_total ${rejected}\n")
            sb.append("# TYPE cactor_gateway_success_total counter\n")
            sb.append("cactor_gateway_success_total ${successful}\n")
            sb.append("# TYPE cactor_gateway_upstream_failures_total counter\n")
            sb.append("cactor_gateway_upstream_failures_total ${upstreamFailures}\n")
        }
        for (w in workers) {
            let open = if (w.stateName() == "open") { 1 } else { 0 }
            sb.append("cactor_gateway_breaker{node=\"${w.node}\"} ${open}\n")
            sb.append("cactor_gateway_upstream_calls{node=\"${w.node}\"} ${w.calls.load()}\n")
        }
        sb.append("cactor_gateway_charges{tenant=\"t1\"} ${billing.chargeOf("t1")}\n")
        sb.toString()
    }

    func stateJson(): String {
        let sb = StringBuilder()
        synchronized(lock) {
            sb.append("{\"requests\":${requests},\"rejected\":${rejected},\"success\":${successful},")
            sb.append("\"upstreamFailures\":${upstreamFailures},")
        }
        sb.append("\"breakers\":{")
        var first = true
        for (w in workers) {
            if (!first) {
                sb.append(",")
            }
            first = false
            sb.append("\"${w.node}\":\"${w.stateName()}\"")
        }
        sb.append("},\"quotas\":{\"t1\":${quota.usedOf("t1")},\"t2\":${quota.usedOf("t2")}},")
        sb.append("\"charges\":${billing.total()}}")
        sb.toString()
    }

    func stop(): Unit {
        for (w in workers) {
            w.stop()
        }
        billing.stop()
        upstreamSystem.terminate()
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

class Gate {
    let lock = Mutex()
    let cond: Condition
    var open = false

    init() {
        this.cond = synchronized(this.lock) {
            this.lock.condition()
        }
    }

    func release(): Unit {
        synchronized(lock) {
            open = true
            cond.notifyAll()
        }
    }

    func await(timeout: Duration): Unit {
        let deadline = MonoTime.now() + timeout
        synchronized(lock) {
            while (!open) {
                let left = deadline - MonoTime.now()
                if (left <= Duration.Zero) {
                    return
                }
                cond.wait(timeout: left)
            }
        }
    }
}

func buildApp(gateway: Gateway, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/infer", { ctx =>
        let tenant = ctx.query("tenant") ?? "t1"
        let prompt = ctx.query("prompt") ?? "hello"
        let n = ctx.queryInt("n") ?? 1
        let (code, body) = gateway.infer(tenant, prompt, n)
        ctx.setSingleHeader("Content-Type", "application/json")
        if (code == "200") {
            ctx.writeString("{\"tenant\":\"${tenant}\",\"result\":\"${body}\"}")
        } else {
            ctx.writeStringWithCode(UInt16.parse(code), "{\"error\":\"${body}\"}")
        }
    })
    app.get("/stream", { ctx =>
        let tenant = ctx.query("tenant") ?? "t3"
        let prompt = ctx.query("prompt") ?? "stream"
        let n = ctx.queryInt("n") ?? 3
        ctx.setSingleHeader("Cache-Control", "no-cache")
        ctx.sse({ stream =>
            let gate = Gate()
            let (code, body) = gateway.infer(tenant, prompt, n)
            if (code != "200") {
                stream.sendEvent("error", body)
                stream.close()
                return
            }
            // body 形如 "by-a|tok-1 tok-2 tok-3"：按空格拆出 token 逐个推送
            let parts = body.split("|")
            let tokens = if (parts.size > 1) { parts[1].split(" ") } else { [body] }
            for (t in tokens) {
                stream.sendEvent("token", t)
                sleep(Duration.millisecond * 10)
            }
            gate.release()
            gate.await(Duration.second)
            stream.close()
        })
    })
    app.post("/upstream/fail", { ctx =>
        let node = ctx.query("node") ?? "a"
        let fail = (ctx.query("fail") ?? "1") == "1"
        gateway.setFailing(node, fail)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"node\":\"${node}\",\"failing\":${fail}}")
    })
    app.get("/metrics", { ctx =>
        ctx.setSingleHeader("Content-Type", "text/plain; version=0.0.4")
        ctx.writeString(gateway.metrics())
    })
    app.get("/state", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(gateway.stateJson())
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
    let dir = "/tmp/cactor-ex43-${MonoTime.now().hashCode()}"

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let gateway = Gateway(dir)
        let app = buildApp(gateway, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex43：端口绑定失败")
            return 1
        }
        println("ex43 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let gateway = Gateway(dir)
    let app = buildApp(gateway, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    let client = ClientBuilder().noProxy().readTimeout(Duration.second * 15).build()

    try {
        println("\n[1] 租户配额")
        let a = readBody(client.post("http://127.0.0.1:${port}/infer?tenant=t1&n=2", ""))
        let b = readBody(client.post("http://127.0.0.1:${port}/infer?tenant=t1&n=2", ""))
        c.check(a.contains("by-a") && b.contains("by-a"), "前两次成功（默认走 a）")
        let over = client.post("http://127.0.0.1:${port}/infer?tenant=t1&n=2", "")
        let body = readBody(over)
        c.check(over.status == 429 && body.contains("quota_exceeded"), "第三次 429 配额拒绝：${body}")
        let callsA = gateway.workers[0].calls.load()
        c.check(callsA == 2, "拒绝的请求没有触达上游（a 调用 ${callsA}）")

        println("\n[2] 上游故障与断路器")
        gateway.setFailing("a", true)
        let r1 = readBody(client.post("http://127.0.0.1:${port}/infer?tenant=t2&n=1", ""))
        c.check(r1.contains("by-b"), "故障时转移到 b：${r1}")
        // 再来一次：a 连续失败 2 次后断路器打开
        let r2 = readBody(client.post("http://127.0.0.1:${port}/infer?tenant=t2&n=1", ""))
        c.check(r2.contains("by-b"), "第二次仍然成功（b）")
        c.check(gateway.workers[0].stateName() == "open", "a 的断路器已打开")
        gateway.setFailing("a", false)

        println("\n[3] 计费幂等")
        c.check(waitUntil(WAIT, { => gateway.billing.total() == 4 }), "4 次成功只计费 4 次")
        c.check(gateway.billing.chargeOf("t1") == 2 && gateway.billing.chargeOf("t2") == 2,
            "按租户各 2 次（发送方重试未重复扣费）")

        println("\n[4] SSE 流式")
        let sse = readBody(client.get("http://127.0.0.1:${port}/stream?tenant=t3&n=3"))
        c.check(sse.contains("event: token") && sse.contains("tok-1") && sse.contains("tok-3"),
            "逐 token 推送")
        c.check(gateway.quota.usedOf("t3") == 1, "一次流式请求只计 1 次配额")

        println("\n[5] 指标与状态")
        let metrics = readBody(client.get("http://127.0.0.1:${port}/metrics"))
        c.check(metrics.contains("cactor_gateway_requests_total") &&
            metrics.contains("cactor_gateway_rejected_total") &&
            metrics.contains("cactor_gateway_breaker{node=\"a\"}"), "Prometheus 文本含关键指标")
        let state = readBody(client.get("http://127.0.0.1:${port}/state"))
        c.check(state.contains("\"breakers\":{\"a\":\"open\"") && state.contains("\"quotas\":"),
            "GET /state 与内存一致")
    } finally {
        client.close()
        try {
            app.stop()
        } catch (_: Exception) {}
        gateway.stop()
    }

    println("\nex43：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
