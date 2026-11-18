/*
 * CActor Ex 25 - 广播中心 + 每订阅者背压（对标 Akka BroadcastHub / protoactor pub-sub）
 *
 * 真实约束：
 * - 一个事件源要同时推给多个在线订阅者（SSE 客户端）；
 * - 慢订阅者不能拖垮快订阅者，也不能让服务端无界堆积：每个订阅者有独立的有界队列，
 *   满了按明确策略丢弃并计数（这里是 DropNew）；
 * - 订阅者断开/完成后要从广播列表移除，不再计入。
 *
 * 说明：CActor 的流没有 GraphDSL / BroadcastHub（差距 4.8）；这里用
 * `Source.queue`（每订阅者一个有界队列）在应用层实现等价语义。
 *
 * 组件：
 * - Hub：订阅者注册/注销 + 广播 + 每订阅者 offered/dropped 计数
 * - Sources.queue + OverflowStrategy.DropNew：每订阅者独立的背压边界
 * - Lou：GET /subscribe/:id?max=、POST /publish、GET /stats
 *
 * 自检（退出码 0/1）：
 * 1. 快/慢两个订阅者（容量 16 / 2）：并发广播 20 条；
 * 2. 快订阅者全部收到、0 丢弃；
 * 3. 慢订阅者部分收到、丢弃 > 0（有界缓冲明确生效）；
 * 4. 完成后注销，不再计入；
 * 5. Lou SSE 订阅者能收到广播。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex25_broadcast_hub
 *   ./target/release/bin/cactor_examples.ex25_broadcast_hub serve 18110
 */
package cactor_examples.ex25_broadcast_hub

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_streams.*

let PUBLISH_WAIT = Duration.second * 10

class Subscription {
    let id: String
    let queue: SourceQueue<String>
    var offered: Int64 = 0
    var dropped: Int64 = 0

    init(id: String, queue: SourceQueue<String>) {
        this.id = id
        this.queue = queue
    }
}

class Hub {
    let lock = Mutex()
    let subs = ArrayList<Subscription>()
    var published: Int64 = 0

    func register(id: String, queue: SourceQueue<String>): Unit {
        synchronized(lock) {
            subs.add(Subscription(id, queue))
        }
    }

    func remove(id: String): Unit {
        synchronized(lock) {
            let kept = ArrayList<Subscription>()
            for (s in subs) {
                if (s.id != id) {
                    kept.add(s)
                }
            }
            subs.clear()
            for (s in kept) {
                subs.add(s)
            }
        }
    }

    func publish(message: String): Int64 {
        let current = synchronized(lock) {
            published++
            subs.toArray()
        }
        for (s in current) {
            let result = s.queue.offer(message).get(Duration.second)
            synchronized(lock) {
                match (result) {
                    case QueueOfferResult.Enqueued => s.offered++
                    case QueueOfferResult.Dropped => s.dropped++
                    case _ => ()
                }
            }
        }
        Int64(current.size)
    }

    func statsJson(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{\"published\":${published},\"subscribers\":[")
            var first = true
            for (s in subs) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("{\"id\":\"${s.id}\",\"offered\":${s.offered},\"dropped\":${s.dropped}}")
            }
            sb.append("]}")
            sb.toString()
        }
    }

    func subscriberCount(): Int64 {
        synchronized(lock) {
            Int64(subs.size)
        }
    }

    func droppedOf(id: String): Int64 {
        synchronized(lock) {
            for (s in subs) {
                if (s.id == id) {
                    return s.dropped
                }
            }
            -1
        }
    }
}

/** SSE 订阅者的队列句柄：队列在回调里需要 complete，构造时还拿不到 */
class QueueBox {
    var q: Option<SourceQueue<String>> = None<SourceQueue<String>>
}

class Recorder {
    let lock = Mutex()
    let seen = ArrayList<String>()

    func add(x: String): Unit {
        synchronized(lock) {
            seen.add(x)
        }
    }

    func count(): Int64 {
        synchronized(lock) {
            Int64(seen.size)
        }
    }

    func all(): Array<String> {
        synchronized(lock) {
            seen.toArray()
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

func buildApp(hub: Hub, system: CActorSystem, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/publish", { ctx =>
        let msg = ctx.query("msg") ?? "event"
        let n = hub.publish(msg)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"subscribers\":${n}}")
    })
    app.get("/stats", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(hub.statsJson())
    })
    app.get("/subscribe/:id", { ctx =>
        let id = ctx.param("id")
        let max = ctx.queryInt("max") ?? 10
        ctx.setSingleHeader("Cache-Control", "no-cache")
        ctx.sse({ stream =>
            // 测试用：收到 max 条后关闭；真实订阅者通常由客户端断开（Lou 目前不暴露断连回调）
            let counter = AtomicInt64(0)
            let box = QueueBox()
            let (queue, done) = Sources.queue<String>(4, OverflowStrategy.DropNew)
                .toMat(Sinks.foreach<String>({ x =>
                    stream.sendEvent("msg", x)
                    if (counter.fetchAdd(1) + 1 >= max) {
                        stream.close()
                        match (box.q) {
                            case Some(q) => q.complete()
                            case None => ()
                        }
                    }
                }), { q, p => (q, p) })
                .run(system)
            box.q = Some(queue)
            hub.register(id, queue)
            try {
                done.get(Duration.second * 30)
            } catch (_: Exception) {}
            hub.remove(id)
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

main(args: Array<String>): Int64 {
    let system = CActorSystemFactory.create("ex25-hub")
    let hub = Hub()

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(hub, system, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex25：端口绑定失败")
            return 1
        }
        println("ex25 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let fastSeen = Recorder()
    let slowSeen = Recorder()
    // 每订阅者一个独立的有界队列：快 64（足够容纳全部 20 条）、慢 2
    let (fastQ, fastDone) = Sources.queue<String>(64, OverflowStrategy.DropNew)
        .toMat(Sinks.foreach<String>({ x => fastSeen.add(x) }), { q, p => (q, p) })
        .run(system)
    let (slowQ, slowDone) = Sources.queue<String>(2, OverflowStrategy.DropNew)
        .toMat(Sinks.foreach<String>({ x =>
            sleep(Duration.millisecond * 60)
            slowSeen.add(x)
        }), { q, p => (q, p) })
        .run(system)
    hub.register("fast", fastQ)
    hub.register("slow", slowQ)

    try {
        // ---- 1. 广播 20 条 ----
        println("\n[1] 广播 20 条：快订阅者不受慢订阅者影响")
        for (i in 0..20) {
            hub.publish("e${i}")
        }
        c.check(waitUntil(PUBLISH_WAIT, { => hub.statsJson().contains("\"id\":\"fast\",\"offered\":20,\"dropped\":0") }),
            "快订阅者 offered=20、dropped=0：${hub.statsJson()}")
        c.check(waitUntil(PUBLISH_WAIT, { => hub.statsJson().contains("\"id\":\"slow\"") && hub.statsJson().contains("\"dropped\":") }),
            "慢订阅者计数可观测")

        // ---- 2. 慢订阅者有明确丢弃 ----
        println("\n[2] 慢订阅者：有界缓冲 + 明确丢弃")
        let slowDropped = waitUntil(PUBLISH_WAIT, { => hub.droppedOf("slow") > 0 })
        c.check(slowDropped, "慢订阅者 dropped > 0（实际 ${hub.droppedOf("slow")}）")
        c.check(waitUntil(PUBLISH_WAIT, { => fastSeen.count() == 20 }), "快订阅者收到全部 20 条（${fastSeen.count()}）")
        c.check(waitUntil(PUBLISH_WAIT, { => slowSeen.count() > 0 }), "慢订阅者开始消费（${slowSeen.count()}）")
        c.check(slowSeen.count() < 20, "慢订阅者只收到部分（${slowSeen.count()}）")

        // ---- 3. 完成并注销 ----
        println("\n[3] 完成后注销")
        fastQ.complete()
        slowQ.complete()
        fastDone.get(Duration.second * 10)
        slowDone.get(Duration.second * 10)
        hub.remove("fast")
        hub.remove("slow")
        c.check(hub.subscriberCount() == 0, "广播列表已清空")

        // ---- 4. Lou SSE ----
        println("\n[4] Lou SSE 订阅者")
        let app = buildApp(hub, system, 0u16)
        let port = startServer(app)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 20).build()
        let streamFuture = spawn {
            let sseClient = ClientBuilder().noProxy().readTimeout(Duration.second * 20).build()
            let resp = sseClient.get("http://127.0.0.1:${port}/subscribe/http?max=3")
            let got = sseData(readBody(resp))
            sseClient.close()
            got
        }
        c.check(waitUntil(PUBLISH_WAIT, { => hub.subscriberCount() == 1 }), "SSE 订阅者已注册")
        for (i in 0..3) {
            client.post("http://127.0.0.1:${port}/publish?msg=s${i}", "")
        }
        let got = streamFuture.get()
        c.check(got.contains("s0") && got.contains("s2"), "SSE 收到广播：${got}")
        client.close()
        app.stop()
    } finally {
        try {
            fastQ.complete()
        } catch (_: Exception) {}
        try {
            slowQ.complete()
        } catch (_: Exception) {}
        system.terminate()
    }

    println("\nex25：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
