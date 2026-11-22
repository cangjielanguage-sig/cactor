/*
 * CActor Ex 60 - GraphDSL 负载均衡与配对（对标 Akka Balance / Zip，差距 4.8 增量）
 *
 * 真实约束：
 * - 同一类任务要分给多个 worker（轮询/按需），每个元素只处理一次，不能丢也不能重复；
 * - 两路数据要按位置配对（请求/响应、指标/时间戳），较短的一侧结束即结束；
 * - 全程有界：balance 最多缓冲 1 个元素，zip 每个入口最多预取 1 个。
 *
 * 组件：
 * - `GraphDSL.balance` / `GraphDSL.zip`（本轮新增）
 * - Lou：GET /balance?n=、GET /zip?n=
 *
 * 自检（退出码 0/1）：
 * 1. balance：12 个元素分给 3 个汇，总数为 12、去重后仍为 12、每个汇都非空；
 * 2. zip：1..8 与 v1..v8 配对正确；
 * 3. zip 长度不等时按较短的一侧结束；
 * 4. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex60_graph_balance_zip
 *   ./target/release/bin/cactor_examples.ex60_graph_balance_zip serve 18204
 */
package cactor_examples.ex60_graph_balance_zip

import std.collection.*
import std.convert.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.*
import cactor_streams.*

let WAIT = Duration.second * 20

func arrayJson(xs: Array<Int64>): String {
    let sb = StringBuilder()
    sb.append("[")
    var first = true
    for (x in xs) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("${x}")
    }
    sb.append("]")
    sb.toString()
}

func pairsJson(pairs: Array<(Int64, String)>): String {
    let sb = StringBuilder()
    sb.append("[")
    var first = true
    for (p in pairs) {
        if (!first) {
            sb.append(",")
        }
        first = false
        let (a, b) = p
        sb.append("{\"n\":${a},\"v\":\"${b}\"}")
    }
    sb.append("]")
    sb.toString()
}

func runBalance(system: CActorSystem, n: Int64): Array<Array<Int64>> {
    let graph = GraphDSL.balance<Int64, Unit, TypedPromise<Array<Int64>>>(Sources.range(1, n),
        [Sinks.seq<Int64>(), Sinks.seq<Int64>(), Sinks.seq<Int64>()])
    let (_, mats) = graph.run(system)
    Array<Array<Int64>>(mats.size, { i => mats[i].get(WAIT) })
}

func runZip(system: CActorSystem, n: Int64): Array<(Int64, String)> {
    let labels = Array<String>(n, { i => "v${i + 1}" })
    let graph = GraphDSL.zip<Int64, String, Unit, Unit, TypedPromise<Array<(Int64, String)>>>(
        Sources.range(1, n), Sources.from<String>(labels), Sinks.seq<(Int64, String)>())
    let (_, _, m) = graph.run(system)
    m.get(WAIT)
}

func runZipMismatch(system: CActorSystem): Array<(Int64, String)> {
    let graph = GraphDSL.zip<Int64, String, Unit, Unit, TypedPromise<Array<(Int64, String)>>>(
        Sources.range(1, 2), Sources.from<String>(["x", "y", "z"]), Sinks.seq<(Int64, String)>())
    let (_, _, m) = graph.run(system)
    m.get(WAIT)
}

func buildApp(system: CActorSystem, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/balance", { ctx =>
        let n = ctx.queryInt("n") ?? 12
        let parts = runBalance(system, n)
        let seen = HashSet<Int64>()
        var total: Int64 = 0
        let perPart = StringBuilder()
        perPart.append("[")
        for (i in 0..parts.size) {
            if (i > 0) {
                perPart.append(",")
            }
            perPart.append("${parts[i].size}")
            for (x in parts[i]) {
                total++
                seen.add(x)
            }
        }
        perPart.append("]")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"parts\":${parts.size},\"total\":${total},\"distinct\":${seen.size},\"per_part\":${perPart.toString()}}")
    })
    app.get("/zip", { ctx =>
        let n = ctx.queryInt("n") ?? 8
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"pairs\":${pairsJson(runZip(system, n))}}")
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
    let system = CActorSystemFactory.create("ex60-graph-balance-zip")
    try {
        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            let server = buildApp(system, port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex60：端口绑定失败")
                return 1
            }
            println("ex60 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        println("\n[1] balance：元素只处理一次且分布到多个汇")
        let parts = runBalance(system, 12)
        let seen = HashSet<Int64>()
        var total: Int64 = 0
        var nonEmpty: Int64 = 0
        for (p in parts) {
            if (!p.isEmpty()) {
                nonEmpty++
            }
            for (x in p) {
                total++
                seen.add(x)
            }
        }
        c.check(parts.size == 3, "物化了 3 个汇")
        c.check(total == 12 && seen.size == 12, "总数 12、去重 12（无丢失无重复）")
        c.check(nonEmpty == 3, "3 个汇都分到元素")

        println("\n[2] zip：按位置配对")
        let pairs = runZip(system, 8)
        var zipOk = pairs.size == 8
        for (i in 0..pairs.size) {
            let (n, v) = pairs[i]
            if (n != Int64(i + 1) || v != "v${i + 1}") {
                zipOk = false
            }
        }
        c.check(zipOk, "8 对 (i, vi) 全部正确")

        println("\n[3] zip：长度不等按较短结束")
        let short = runZipMismatch(system)
        c.check(short.size == 2, "取较短的 2 对")

        println("\n[4] Lou 端点")
        let server = buildApp(system, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let bal = readBody(client.get("http://127.0.0.1:${port}/balance?n=12"))
            c.check(bal.contains("\"total\":12") && bal.contains("\"distinct\":12") && bal.contains("\"parts\":3"),
                "GET /balance：${bal}")
            let zip = readBody(client.get("http://127.0.0.1:${port}/zip?n=4"))
            c.check(zip.contains("\"n\":1,\"v\":\"v1\"") && zip.contains("\"n\":4,\"v\":\"v4\""),
                "GET /zip：${zip}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex60：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        system.terminate()
    }
}
