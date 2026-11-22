/*
 * CActor Ex 59 - GraphDSL 扇出编排（对标 Akka GraphDSL 的 Broadcast / Partition，差距 4.8）
 *
 * 真实约束：
 * - 同一份数据要同时送进多个下游（审计 / 指标 / 归档），不能复制源、也不能破坏背压；
 * - 必须能按 key 稳定分区，同一 key 永远落到同一个下游；
 * - 下游全部取消时，无限源也要被取消，不能留下跑不完的流。
 *
 * 组件：
 * - `GraphDSL.broadcast` / `GraphDSL.partitionBy`（本轮新增，基于现有 Source/Flow/Sink）
 * - Lou：GET /fanout、GET /partition、GET /cancel
 *
 * 自检（退出码 0/1）：
 * 1. broadcast：1 个源 → 3 个汇，三份内容完全一致（1..20）；
 * 2. partitionBy：偶数/奇数分别落到两个分区，且内容正确；
 * 3. 两个 head 消费无限源：各自取到第一个元素后取消，上游被取消（流能结束）；
 * 4. Lou 端点结果与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex59_graph_dsl
 *   ./target/release/bin/cactor_examples.ex59_graph_dsl serve 18203
 */
package cactor_examples.ex59_graph_dsl

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

func sumOf(xs: Array<Int64>): Int64 {
    var s: Int64 = 0
    for (x in xs) {
        s += x
    }
    s
}

func sameContent(parts: Array<Array<Int64>>): Bool {
    if (parts.isEmpty()) {
        return true
    }
    let first = parts[0]
    for (i in 1..parts.size) {
        if (parts[i].size != first.size) {
            return false
        }
        for (k in 0..first.size) {
            if (parts[i][k] != first[k]) {
                return false
            }
        }
    }
    true
}

func runFanout(system: CActorSystem, n: Int64): Array<Array<Int64>> {
    let graph = GraphDSL.broadcast<Int64, Unit, TypedPromise<Array<Int64>>>(Sources.range(1, n),
        [Sinks.seq<Int64>(), Sinks.seq<Int64>(), Sinks.seq<Int64>()])
    let (_, mats) = graph.run(system)
    Array<Array<Int64>>(mats.size, { i => mats[i].get(WAIT) })
}

func runPartition(system: CActorSystem, n: Int64): (Array<Int64>, Array<Int64>) {
    let graph = GraphDSL.partitionBy<Int64, Unit, TypedPromise<Array<Int64>>>(Sources.range(1, n), 2,
        { x => x % 2 }, [Sinks.seq<Int64>(), Sinks.seq<Int64>()])
    let (_, mats) = graph.run(system)
    (mats[0].get(WAIT), mats[1].get(WAIT))
}

func runCancel(system: CActorSystem): (Option<Int64>, Option<Int64>) {
    let graph = GraphDSL.broadcast<Int64, Unit, TypedPromise<Option<Int64>>>(
        Sources.iterate<Int64>(1, { x => x + 1 }),
        [Sinks.headOption<Int64>(), Sinks.headOption<Int64>()])
    let (_, mats) = graph.run(system)
    (mats[0].get(WAIT), mats[1].get(WAIT))
}

func buildApp(system: CActorSystem, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/fanout", { ctx =>
        let n = ctx.queryInt("n") ?? 20
        let parts = runFanout(system, n)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"parts\":${parts.size},\"count\":${parts[0].size},\"same\":${sameContent(parts)},\"sum\":${sumOf(parts[0])}}")
    })
    app.get("/partition", { ctx =>
        let n = ctx.queryInt("n") ?? 20
        let (even, odd) = runPartition(system, n)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"even\":${arrayJson(even)},\"odd\":${arrayJson(odd)}}")
    })
    app.get("/cancel", { ctx =>
        let (a, b) = runCancel(system)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"a\":${optJson(a)},\"b\":${optJson(b)}}")
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
    app
}

func optJson(o: Option<Int64>): String {
    match (o) {
        case Some(v) => "${v}"
        case None => "null"
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
    let system = CActorSystemFactory.create("ex59-graph-dsl")
    try {
        if (args.size > 0 && args[0] == "serve") {
            let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
            let server = buildApp(system, port)
            let actual = startServer(server)
            if (actual == 0u16) {
                println("ex59：端口绑定失败")
                return 1
            }
            println("ex59 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
            while (true) {
                sleep(Duration.second)
            }
            return 0
        }

        let c = Checker()
        println("\n[1] broadcast：一份数据三份落地")
        let parts = runFanout(system, 20)
        c.check(parts.size == 3, "物化了 3 个汇")
        c.check(parts[0].size == 20 && parts[0][0] == 1 && parts[0][19] == 20, "第一份是 1..20")
        c.check(sameContent(parts), "三份内容完全一致")

        println("\n[2] partitionBy：同 key 恒定同分区")
        let (even, odd) = runPartition(system, 20)
        c.check(arrayEq(even, [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]), "偶数分区：${arrayJson(even)}")
        c.check(arrayEq(odd, [1, 3, 5, 7, 9, 11, 13, 15, 17, 19]), "奇数分区：${arrayJson(odd)}")

        println("\n[3] 全部出口取消后上游结束")
        let (a, b) = runCancel(system)
        c.check(a == Some<Int64>(1) && b == Some<Int64>(1), "两个 head 各取到 1 并结束")

        println("\n[4] Lou 端点")
        let server = buildApp(system, 0u16)
        let port = startServer(server)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        try {
            let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
            let fan = readBody(client.get("http://127.0.0.1:${port}/fanout?n=20"))
            c.check(fan.contains("\"parts\":3") && fan.contains("\"same\":true") && fan.contains("\"sum\":210"),
                "GET /fanout：${fan}")
            let part = readBody(client.get("http://127.0.0.1:${port}/partition?n=10"))
            c.check(part.contains("\"even\":[2,4,6,8,10]") && part.contains("\"odd\":[1,3,5,7,9]"),
                "GET /partition：${part}")
            let cancel = readBody(client.get("http://127.0.0.1:${port}/cancel"))
            c.check(cancel.contains("\"a\":1") && cancel.contains("\"b\":1"), "GET /cancel：${cancel}")
            client.close()
        } finally {
            try {
                server.stop()
            } catch (_: Exception) {}
        }

        println("\nex59：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
        if (c.failures == 0) { 0 } else { 1 }
    } finally {
        system.terminate()
    }
}

func arrayEq(a: Array<Int64>, b: Array<Int64>): Bool {
    if (a.size != b.size) {
        return false
    }
    for (i in 0..a.size) {
        if (a[i] != b[i]) {
            return false
        }
    }
    true
}
