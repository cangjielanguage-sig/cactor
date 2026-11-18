/*
 * CActor Ex 12 - 流式管道 + SSE（对标 akka-sample-streams、protoactor-go streams 示例）
 *
 * 真实约束：
 * - 数据是"推到 HTTP 客户端"的流（SSE），必须保持顺序；
 * - 上下游速度不一致时要有背压或明确的丢弃策略，不能无界堆内存；
 * - `async` 边界把生产与消费解耦，但不破坏背压与顺序；
 * - 客户端拿到的是逐条、有序、可增量消费的数据。
 *
 * 组件：
 * - cactor_streams：Sources / Flows / Sinks、async(buffer)、buffer(size, 策略)、runForeach
 * - Lou：GET /stream?n=20（SSE）
 *
 * 自检（退出码 0/1）：
 * 1. SSE 收到 n 条，顺序为 1,4,9,...（平方），无丢失无重复；
 * 2. Source.queue(buffer=2, DropNew) + 慢消费者：快速 offer 8 条，
 *    必须观测到 Dropped（有界缓冲，丢弃策略明确），而不是无界堆积；
 * 3. 队列 complete 后下游正常完成；
 * 4. async 边界不改变顺序。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex12_streams_sse
 *   ./target/release/bin/cactor_examples.ex12_streams_sse serve 18097
 */
package cactor_examples.ex12_streams_sse

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_streams.*

let WAIT = Duration.second * 20

func buildApp(system: CActorSystem, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/stream", { ctx =>
        let n = ctx.queryInt("n") ?? 10
        ctx.setSingleHeader("Cache-Control", "no-cache")
        ctx.sse({ stream =>
            // 管道：range（含端点）→ map → async 边界（缓冲 4）→ 逐条写 SSE
            let done = Sources.range(1, n)
                .map<Int64>({ x => x * x })
                .async(4)
                .runForeach({ x => stream.sendEvent("num", "${x}") }, system)
            try {
                done.get(WAIT)
            } catch (_: Exception) {
                stream.sendEvent("error", "stream_failed")
            }
            stream.close()
        })
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
    let system = CActorSystemFactory.create("ex12-streams")

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(system, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex12：端口绑定失败")
            return 1
        }
        println("ex12 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let app = buildApp(system, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. SSE 有序输出 ----
        println("\n[1] SSE：20 条平方数，有序无丢失")
        let client = ClientBuilder().noProxy().readTimeout(WAIT).build()
        let resp = client.get("http://127.0.0.1:${port}/stream?n=20")
        c.check(resp.status == 200, "HTTP 200")
        let data = sseData(readBody(resp))
        c.check(data.size == 20, "收到 20 条（实际 ${data.size}）")
        var ordered = data.size == 20
        for (i in 0..data.size) {
            if (data[i] != "${(i + 1) * (i + 1)}") {
                ordered = false
            }
        }
        c.check(ordered, "顺序为 1,4,9,...（越界或乱序会失败）")
        let empty = client.get("http://127.0.0.1:${port}/stream?n=0")
        c.check(empty.status == 200 && sseData(readBody(empty)).isEmpty(), "n=0 正常结束（无数据、无挂起）")
        client.close()

        // ---- 2. 有界队列的背压/丢弃 ----
        println("\n[2] Source.queue(buffer=2, DropNew) + 慢消费者")
        let (queue, finished) = Sources.queue<Int64>(2, OverflowStrategy.DropNew)
            .toMat(Sinks.foreach<Int64>({ _ => sleep(Duration.millisecond * 80) }), { q, p => (q, p) })
            .run(system)
        var enqueued: Int64 = 0
        var dropped: Int64 = 0
        for (i in 0..8) {
            match (queue.offer(Int64(i)).get(Duration.second * 2)) {
                case QueueOfferResult.Enqueued => enqueued++
                case QueueOfferResult.Dropped => dropped++
                case QueueOfferResult.QueueClosed => ()
                case QueueOfferResult.QueueFailure(e) =>
                    c.check(false, "offer 意外失败：${e.message}")
            }
        }
        c.check(dropped > 0, "缓冲区满后新元素被明确丢弃（enqueued=${enqueued}, dropped=${dropped}）")
        c.check(enqueued + dropped == 8, "每次 offer 都有明确结果（${enqueued}+${dropped}=8）")
        c.check(queue.bufferedCount() <= 2, "缓冲区不超过 2（实际 ${queue.bufferedCount()}）")
        queue.complete()
        finished.get(WAIT)
        c.check(true, "complete 后下游正常完成")

        // ---- 3. async 边界保持顺序 ----
        println("\n[3] async 边界保持顺序")
        let result = Sources.range(1, 50)
            .map<Int64>({ x => x * 2 })
            .async(2)
            .map<Int64>({ x => x + 1 })
            .runWith(Sinks.seq<Int64>(), system)
            .get(WAIT)
        var seqOk = result.size == 50
        for (i in 0..result.size) {
            if (result[i] != (i + 1) * 2 + 1) {
                seqOk = false
            }
        }
        c.check(seqOk, "async 边界两侧 50 条仍有序")
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        system.terminate()
    }

    println("\nex12：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
