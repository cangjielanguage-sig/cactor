/*
 * CActor Ex 68 - 属性匹配（平台 matchmaker + Lou，对标 Rivet/Matchmaker 的 ListLobbies/配对）
 *
 * 真实约束：
 * - 只有同一 mode、共同属性相同（且 rank 在容差内）的两张票才配对，不能乱配；
 * - 等待超时必须是明确的 Expired，调用方不能永久挂起；
 * - 并发提交下同一张票不能被配两次；等待队列可按 mode 观测。
 *
 * 组件：
 * - `cactor_platform.matchmaker.Matchmaker.submit/expireNow/waitingByMode`
 * - Lou：POST /join、GET /result、GET /lobbies、GET /healthz
 *
 * 自检（退出码 0/1）：
 * 1. 同 mode/同 region/rank 接近 → 两张票互相配对，partner 正确，等待数归零；
 * 2. 不同 region 不配；TTL 到期后两张都 Expired；
 * 3. rank 差超过容差不配（tolerance=2）；
 * 4. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex68_matchmaker
 *   ./target/release/bin/cactor_examples.ex68_matchmaker serve 18212
 */
package cactor_examples.ex68_matchmaker

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.{AskOutcome}
import cactor_platform.matchmaker.*

let WAIT = Duration.second * 25
let TOLERANCE: Float64 = 2.0

/** 每张票的最终/当前状态（回调线程写入，HTTP 线程读取） */
class Outcomes {
    private let lock = Mutex()
    private let states = HashMap<String, String>()

    func record(id: String, json: String): Unit {
        synchronized(lock) {
            states[id] = json
        }
    }

    func of(id: String): String {
        synchronized(lock) {
            states.get(id) ?? "{\"state\":\"unknown\"}"
        }
    }
}

let SEQ = AtomicInt64(0)

func attrsOf(region: String): HashMap<String, String> {
    let m = HashMap<String, String>()
    m["region"] = region
    m
}

/** 提交一张票并把结果写进 Outcomes；返回票 id */
func join(m: Matchmaker, out: Outcomes, mode: String, region: String, rank: Float64, ttl: Duration): String {
    let id = "t${SEQ.fetchAdd(1) + 1}"
    // 先记 waiting：配对/过期回调可能在 submit 返回前后立刻触发，初始状态不能是 unknown
    out.record(id, "{\"state\":\"waiting\",\"id\":\"${id}\"}")
    let promise = m.submit(MatchTicket(id, mode, attrsOf(region), rank: rank), ttl: ttl)
    promise.onComplete({ outcome =>
        match (outcome) {
            case Replied(r) =>
                match (r) {
                    case Matched(me, partner) =>
                        let pr = partner.attrs.get("region") ?? "?"
                        out.record(id, "{\"state\":\"matched\",\"id\":\"${me.id}\",\"partner\":\"${partner.id}\"," +
                            "\"partner_region\":\"${pr}\",\"partner_rank\":${partner.rank}}")
                    case Expired(t) =>
                        out.record(id, "{\"state\":\"expired\",\"id\":\"${t.id}\"}")
                }
            case Failed(e) =>
                out.record(id, "{\"state\":\"failed\",\"error\":\"${e.message}\"}")
        }
    })
    id
}

func stateOf(out: Outcomes, id: String): String {
    out.of(id)
}

func lobbiesJson(m: Matchmaker): String {
    let sb = StringBuilder()
    sb.append("{")
    var first = true
    for ((mode, n) in m.waitingByMode()) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${mode}\":${n}")
    }
    sb.append("}")
    sb.toString()
}

func waitUntil(timeout: Duration, cond: () -> Bool): Bool {
    let deadline = MonoTime.now() + timeout
    while (!cond() && MonoTime.now() < deadline) {
        sleep(Duration.millisecond * 20)
    }
    cond()
}

func buildApp(m: Matchmaker, out: Outcomes, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/join", { ctx =>
        let mode = ctx.query("mode") ?? "1v1"
        let region = ctx.query("region") ?? "cn"
        let rank = Float64.parse(ctx.query("rank") ?? "0")
        let ttl = ctx.queryInt("ttl") ?? 5000
        let id = join(m, out, mode, region, rank, Duration.millisecond * ttl)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"id\":\"${id}\",\"result\":${stateOf(out, id)}}")
    })
    app.get("/result", { ctx =>
        let id = ctx.query("id") ?? ""
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(stateOf(out, id))
    })
    app.get("/lobbies", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(lobbiesJson(m))
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
    let system = CActorSystemFactory.create("ex68-match")
    let m = Matchmaker.create(rankTolerance: TOLERANCE, sweepInterval: Duration.millisecond * 20)
    let out = Outcomes()
    try {
        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            let server = buildApp(m, out, port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex68：端口绑定失败")
                return 1
            }
            println("ex68 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        println("\n[1] 兼容的票互相配对")
        let a = join(m, out, "1v1", "cn", 10.0, Duration.second * 5)
        c.check(waitUntil(WAIT, { => m.waitingByMode().get("1v1") == Some(1) }), "第一张进入等待")
        let b = join(m, out, "1v1", "cn", 11.0, Duration.second * 5)
        c.check(waitUntil(WAIT, { => stateOf(out, a).contains("\"state\":\"matched\"") &&
            stateOf(out, b).contains("\"state\":\"matched\"") }), "两张票都匹配")
        c.check(stateOf(out, a).contains("\"partner\":\"${b}\""), "a 的 partner 是 b")
        c.check(stateOf(out, b).contains("\"partner\":\"${a}\""), "b 的 partner 是 a")
        c.check(m.waitingCount() == 0, "等待数归零")

        println("\n[2] 属性不同不配；TTL 到期 Expired")
        let us = join(m, out, "1v1", "us", 0.0, Duration.millisecond * 300)
        let eu = join(m, out, "1v1", "eu", 0.0, Duration.millisecond * 300)
        c.check(waitUntil(WAIT, { => m.waitingByMode().get("1v1") == Some(2) }), "不同 region 的两张票在等待")
        c.check(waitUntil(WAIT, { => stateOf(out, us).contains("expired") &&
            stateOf(out, eu).contains("expired") }), "TTL 到期后都 Expired")

        println("\n[3] rank 超出容差不配")
        let low = join(m, out, "1v1", "cn", 10.0, Duration.millisecond * 300)
        let high = join(m, out, "1v1", "cn", 20.0, Duration.millisecond * 300)
        c.check(waitUntil(WAIT, { => m.waitingByMode().get("1v1") == Some(2) }), "rank 差 10 > 容差 2，仍在等待")
        c.check(waitUntil(WAIT, { => stateOf(out, low).contains("expired") &&
            stateOf(out, high).contains("expired") }), "随后各自 Expired，而不是错配")

        println("\n[4] Lou 端点")
        let server = buildApp(m, out, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let j1 = readBody(client.post("http://127.0.0.1:${port}/join?mode=2v2&region=cn&rank=1&ttl=5000", ""))
            let j2 = readBody(client.post("http://127.0.0.1:${port}/join?mode=2v2&region=cn&rank=2&ttl=5000", ""))
            let id1 = j1.split("\"id\":\"")[1].split("\"")[0]
            let id2 = j2.split("\"id\":\"")[1].split("\"")[0]
            c.check(waitUntil(WAIT, { => stateOf(out, id1).contains("\"partner\":\"${id2}\"") }),
                "POST /join 两张票配对")
            let r = readBody(client.get("http://127.0.0.1:${port}/result?id=${id1}"))
            c.check(r.contains("\"state\":\"matched\""), "GET /result：${r}")
            let lobbies = readBody(client.get("http://127.0.0.1:${port}/lobbies"))
            c.check(!lobbies.contains("\"2v2\":1"), "GET /lobbies 配对后不残留：${lobbies}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex68：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        m.stop()
        system.terminate()
    }
}
