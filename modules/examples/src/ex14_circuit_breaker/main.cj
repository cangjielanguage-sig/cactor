/*
 * CActor Ex 14 - 断路器 + 指数退避重启（对标 Akka CircuitBreaker / protoactor-go middleware）
 *
 * 真实约束：
 * - 下游依赖连续失败时，调用方应快速失败，而不是把线程和连接都耗在超时上；
 * - 断路器要能从"开启"自动过渡到"半开"试探，成功一次后恢复；
 * - Actor 级故障（处理消息就抛异常）要用退避重启隔离，重启期间不处理消息，
 *   但重启后仍能继续服务（而不是让监督者无限重启打爆 CPU）。
 *
 * 组件：
 * - `cactor_foundation.circuit_breaker`：BasicCircuitBreaker / CircuitBreakerConfig / 三种状态
 * - `TypedSupervisorStrategy.restartWithBackoff(min, max, randomFactor)`：指数退避重启
 * - Lou：GET /call、POST /toggle、GET /state、POST /boom、GET /actor
 *
 * 自检（退出码 0/1）：
 * 1. 正常调用 200；
 * 2. 依赖连续失败 2 次 → 每次 503 dependency_failed，断路器进入 Open；
 * 3. Open 期间的调用 503 circuit_open，且下游调用次数不再增长（快速失败生效）；
 * 4. 依赖恢复 + 等待 resetTimeout → 半开试探成功 → Closed，调用恢复 200；
 * 5. Actor 连续抛异常 → 退避重启，启动次数增加且重启后仍能响应。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex14_circuit_breaker
 *   ./target/release/bin/cactor_examples.ex14_circuit_breaker serve 18099
 */
package cactor_examples.ex14_circuit_breaker

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.*
import cactor_foundation.circuit_breaker.*

let ASK = Duration.second * 10
let RESET_TIMEOUT = Duration.millisecond * 400

/** 会失败的下游依赖（模拟数据库/第三方 API） */
class FlakyService {
    let lock = Mutex()
    var failing = false
    var calls: Int64 = 0

    func setFailing(v: Bool): Unit {
        synchronized(lock) {
            failing = v
        }
    }

    func call(): String {
        let shouldFail = synchronized(lock) {
            calls++
            failing
        }
        if (shouldFail) {
            throw IllegalStateException("下游依赖不可用")
        }
        "ok"
    }

    func callCount(): Int64 {
        synchronized(lock) {
            calls
        }
    }
}

/** 退避重启的 Actor：setup 里记录启动次数，Boom 抛异常，Work 正常回复 */
enum BlipCmd {
    | Work(TypedActorRef<String>)
    | Boom
}

class Starts {
    let n = AtomicInt64(0)
    func inc(): Unit {
        n.fetchAdd(1)
    }
    func value(): Int64 {
        n.load()
    }
}

func startCounter(s: Starts): Unit {
    s.inc()
}

func blipBehavior(starts: Starts): TypedBehavior<BlipCmd> {
    TypedBehaviors.setup<BlipCmd>({ _ =>
        startCounter(starts)
        TypedBehaviors.receive<BlipCmd>({ _, msg =>
            match (msg) {
                case BlipCmd.Work(reply) =>
                    reply.tell("ok|${starts.value()}")
                    TypedBehaviors.same<BlipCmd>()
                case BlipCmd.Boom =>
                    throw IllegalStateException("演示：消息处理失败，交给退避策略")
            }
        })
    })
}

func stateName(s: CircuitBreakerState): String {
    match (s) {
        case CircuitBreakerState.Closed => "closed"
        case CircuitBreakerState.Open => "open"
        case CircuitBreakerState.HalfOpen => "half_open"
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

func callService(breaker: BasicCircuitBreaker<String>, service: FlakyService): (UInt16, String) {
    try {
        let r = breaker.execute({ => service.call() })
        (200u16, r)
    } catch (_: CircuitBreakerOpenException) {
        (503u16, "circuit_open")
    } catch (e: Exception) {
        (503u16, "dependency_failed")
    }
}

func buildApp(breaker: BasicCircuitBreaker<String>, service: FlakyService, blip: TypedActorRef<BlipCmd>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/call", { ctx =>
        let (code, body) = callService(breaker, service)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeStringWithCode(code, "{\"result\":\"${body}\"}")
    })
    app.post("/toggle", { ctx =>
        let fail = (ctx.query("fail") ?? "0") == "1"
        service.setFailing(fail)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"failing\":${fail}}")
    })
    app.get("/state", { ctx =>
        let st = breaker.getStats()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(
            "{\"state\":\"${stateName(st.state)}\",\"failures\":${st.failureCount},\"successes\":${st.successCount},\"total\":${st.totalRequests},\"downstreamCalls\":${service.callCount()}}")
    })
    app.post("/boom", { ctx =>
        blip.tell(BlipCmd.Boom)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"sent\":true}")
    })
    app.get("/actor", { ctx =>
        let r = TypedAsk.ask<BlipCmd, String>(blip, { x => BlipCmd.Work(x) }, ASK).get(ASK)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"result\":\"${r}\",\"starts\":${blipStarts(blip)}}")
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
    app
}

/** 观测用：从回复里解析启动次数（避免再共享一个全局计数） */
func blipStarts(blip: TypedActorRef<BlipCmd>): Int64 {
    match (TypedAsk.ask<BlipCmd, String>(blip, { x => BlipCmd.Work(x) }, ASK).get(ASK).split("|")) {
        case parts where parts.size >= 2 => Int64.parse(parts[1])
        case _ => -1
    }
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

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

main(args: Array<String>): Int64 {
    let system = CActorSystemFactory.create("ex14-resilience")
    let service = FlakyService()
    // 连续失败 2 次即打开；半开状态下成功 1 次即关闭
    let breaker = BasicCircuitBreaker<String>(
        CircuitBreakerConfig(2, 1, Duration.millisecond * 500, RESET_TIMEOUT))
    let starts = Starts()
    let blip = system.typedActorOf<BlipCmd>(
        TypedBehaviors.supervise(blipBehavior(starts))
            .onFailure(TypedSupervisorStrategy.restartWithBackoff(
                Duration.millisecond * 50, Duration.millisecond * 300, 0.0)),
        "blip")

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(breaker, service, blip, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex14：端口绑定失败")
            return 1
        }
        println("ex14 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let app = buildApp(breaker, service, blip, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. 正常调用 ----
        println("\n[1] 正常调用")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let ok = client.get("http://127.0.0.1:${port}/call")
        c.check(ok.status == 200 && readBody(ok).contains("\"result\":\"ok\""), "GET /call 返回 ok")

        // ---- 2. 连续失败打开断路器 ----
        println("\n[2] 依赖失败 → 断路器打开")
        service.setFailing(true)
        let f1 = client.get("http://127.0.0.1:${port}/call")
        let f2 = client.get("http://127.0.0.1:${port}/call")
        c.check(f1.status == 503 && readBody(f1).contains("dependency_failed"), "第 1 次失败：503 dependency_failed")
        c.check(f2.status == 503 && readBody(f2).contains("dependency_failed"), "第 2 次失败：503 dependency_failed")
        let stOpen = readBody(client.get("http://127.0.0.1:${port}/state"))
        c.check(stOpen.contains("\"state\":\"open\""), "状态为 open（${stOpen}）")

        // ---- 3. 快速失败：不再调用下游 ----
        println("\n[3] Open 期间快速失败，不消耗下游")
        let beforeCalls = service.callCount()
        let fast = client.get("http://127.0.0.1:${port}/call")
        c.check(fast.status == 503 && readBody(fast).contains("circuit_open"), "Open 期间返回 circuit_open")
        c.check(service.callCount() == beforeCalls, "下游调用次数没有增加（${beforeCalls} → ${service.callCount()}）")

        // ---- 4. 半开试探后恢复 ----
        println("\n[4] 依赖恢复 + 等待 ${RESET_TIMEOUT.toMilliseconds()} ms → 半开 → 关闭")
        service.setFailing(false)
        // 等到进入半开（或直接调用触发 advance）
        sleep(RESET_TIMEOUT + Duration.millisecond * 100)
        let halfOpenCall = client.get("http://127.0.0.1:${port}/call")
        c.check(halfOpenCall.status == 200, "半开试探成功，调用恢复")
        let stClosed = readBody(client.get("http://127.0.0.1:${port}/state"))
        c.check(stClosed.contains("\"state\":\"closed\""), "状态回到 closed（${stClosed}）")
        c.check(client.get("http://127.0.0.1:${port}/call").status == 200, "后续调用保持 200")

        // ---- 5. 退避重启 ----
        println("\n[5] Actor 故障 → 指数退避重启")
        let startsBefore = starts.value()
        let boom = client.post("http://127.0.0.1:${port}/boom", "")
        c.check(boom.status == 200, "POST /boom 已发送")
        let restarted = waitUntil(Duration.second * 5, { => starts.value() > startsBefore })
        c.check(restarted, "退避后完成重启（starts ${startsBefore} → ${starts.value()}）")
        // 再触发两次，间隔递增
        client.post("http://127.0.0.1:${port}/boom", "")
        c.check(waitUntil(Duration.second * 5, { => starts.value() >= startsBefore + 2 }), "第二次重启完成")
        let actor = client.get("http://127.0.0.1:${port}/actor")
        c.check(actor.status == 200 && readBody(actor).contains("\"result\":\"ok|"), "重启后 Actor 仍能正常处理消息")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        system.terminate()
    }

    println("\nex14：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
