/*
 * CActor Ex 41 - 持久订阅（差距 4.8：Durable Subscriber）
 *
 * 真实约束：
 * - 订单/审计事件不能因为订阅方短暂离线而丢：离线期间的消息要在重连后按序补齐；
 * - 订阅进度必须落盘：订阅方进程重启后不能从头再消费（重复），也不能跳过（漏）；
 * - 处理失败不能推进游标：否则那条消息永远丢失（至少一次语义）；
 * - 日志不能无限增长：可配置只保留最近 N 条。
 *
 * 组件：
 * - 框架：`DurableTopic`（`persistence/durable_topic.cj`，本次新增）+ `FileDurableStateStore`（消息与游标）
 * - Lou：POST /publish、GET /history、GET /audit、POST /audit/offline、POST /audit/online、
 *        GET /stream（SSE 实时订阅：先补历史再接实时）
 *
 * 自检（退出码 0/1）：
 * 1. 发布 3 条 → 在线订阅方按序收到 3 条，游标 3；
 * 2. 订阅方离线 → 再发布 2 条 → 重连补齐（共 5 条，游标 5）；
 * 3. 新建 DurableTopic（同一存储）模拟重启 → 游标仍在，不重复消费；新消息照常收到；
 * 4. 处理失败不推进游标（单元测试覆盖语义，这里验证游标可观测）；
 * 5. Lou 端点与 SSE 输出一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex41_durable_subscriber
 *   ./target/release/bin/cactor_examples.ex41_durable_subscriber serve 18186
 */
package cactor_examples.ex41_durable_subscriber

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor_persistence.{DurableStateStore, DurableSubscription, DurableTopic, FileDurableStateStore}

let TOPIC = "orders"
let WAIT = Duration.second * 15

/** SSE 收尾门闩：收到 limit 条后释放 */
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

/** 审计订阅方：可显式离线/上线，上线时由 DurableTopic 补齐离线期间的消息 */
class Audit {
    private let topic: DurableTopic
    private let offsets: DurableStateStore
    private let lock = Mutex()
    private let lines = ArrayList<String>()
    private var sub: Option<DurableSubscription> = None<DurableSubscription>

    init(topic: DurableTopic, offsets: DurableStateStore) {
        this.topic = topic
        this.offsets = offsets
    }

    func online(): Unit {
        var start = false
        synchronized(lock) {
            if (sub.isNone()) {
                start = true
            }
        }
        if (!start) {
            return
        }
        // 订阅会同步回放历史，回放回调需要拿 lock；不能在持锁时调用 subscribe
        let s = topic.subscribe("audit", offsets, { seq, payload =>
            synchronized(lock) {
                lines.add("${seq}|${payload}")
            }
        })
        synchronized(lock) {
            this.sub = Some<DurableSubscription>(s)
        }
    }

    func offline(): Unit {
        synchronized(lock) {
            match (sub) {
                case Some(s) =>
                    s.unsubscribe()
                    this.sub = None<DurableSubscription>
                case None => ()
            }
        }
    }

    func count(): Int64 {
        synchronized(lock) {
            Int64(lines.size)
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("[")
            var first = true
            for (line in lines) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${line}\"")
            }
            sb.append("]")
            sb.toString()
        }
    }

    func cursor(): Int64 {
        topic.offsetOf("audit", offsets)
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

func historyJson(topic: DurableTopic, from: Int64): String {
    let sb = StringBuilder()
    sb.append("[")
    var first = true
    for ((seq, payload) in topic.history(from)) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("{\"seq\":${seq},\"payload\":\"${payload}\"}")
    }
    sb.append("]")
    sb.toString()
}

func buildApp(app: Lou, topic: DurableTopic, audit: Audit, offsets: DurableStateStore): Unit {
    app.post("/publish", { ctx =>
        let event = ctx.query("event") ?? "order-created"
        let seq = topic.publish(event)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"seq\":${seq}}")
    })
    app.get("/history", { ctx =>
        let from = ctx.queryInt("from") ?? 1
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(historyJson(topic, from))
    })
    app.get("/audit", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(audit.json())
    })
    app.post("/audit/offline", { ctx =>
        audit.offline()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"online\":false,\"processed\":${audit.count()}}")
    })
    app.post("/audit/online", { ctx =>
        audit.online()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"online\":true,\"processed\":${audit.count()},\"cursor\":${audit.cursor()}}")
    })
    app.get("/stats", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"published\":${topic.highestSequenceNr()},\"processed\":${audit.count()}," +
            "\"cursor\":${audit.cursor()}}")
    })
    // SSE：先补 from 之后的历史，再接实时；limit 条后主动结束，便于测试与客户端收尾
    app.get("/stream", { ctx =>
        let from = ctx.queryInt("from") ?? 1
        let limit = ctx.queryInt("limit") ?? 1
        ctx.setSingleHeader("Cache-Control", "no-cache")
        ctx.sse({ stream =>
            let gate = Gate()
            let seen = AtomicInt64(0)
            let name = "web-${MonoTime.now().hashCode()}"
            let sub = topic.subscribe(name, offsets, { seq, payload =>
                stream.sendEvent("order", "${seq}|${payload}")
                if (seen.fetchAdd(1) + 1 >= limit) {
                    gate.release()
                }
            }, fromSeq: from)
            gate.await(WAIT)
            sub.unsubscribe()
            stream.close()
        })
    })
    app.get("/healthz", { ctx => ctx.writeString("ok") })
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
    let dir = "/tmp/cactor-ex41-${MonoTime.now().hashCode()}"

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let store = FileDurableStateStore("${dir}/topic")
        let offsets = FileDurableStateStore("${dir}/offsets")
        let topic = DurableTopic(TOPIC, store)
        let audit = Audit(topic, offsets)
        audit.online()
        let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
        buildApp(app, topic, audit, offsets)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex41：端口绑定失败")
            return 1
        }
        println("ex41 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let store = FileDurableStateStore("${dir}/topic")
    let offsets = FileDurableStateStore("${dir}/offsets")
    let topic = DurableTopic(TOPIC, store)
    let audit = Audit(topic, offsets)
    audit.online()

    println("\n[1] 在线发布与消费")
    topic.publish("order-1")
    topic.publish("order-2")
    topic.publish("order-3")
    c.check(audit.count() == 3, "在线收到 3 条")
    c.check(audit.cursor() == 3, "游标推进到 3")
    c.check(topic.highestSequenceNr() == 3, "主题最高序号 3")

    println("\n[2] 离线期间的消息在重连后补齐")
    audit.offline()
    topic.publish("order-4")
    topic.publish("order-5")
    c.check(audit.count() == 3, "离线时没有消费")
    audit.online()
    c.check(audit.count() == 5, "重连补齐到 5 条")
    c.check(audit.cursor() == 5, "游标推进到 5")
    c.check(audit.json().contains("\"4|order-4\"") && audit.json().contains("\"5|order-5\""),
        "补齐的是离线期间的 4、5")

    println("\n[3] 同一存储上的新主题实例（模拟进程重启）")
    let reopened = DurableTopic(TOPIC, FileDurableStateStore("${dir}/topic"))
    let audit2 = Audit(reopened, FileDurableStateStore("${dir}/offsets"))
    audit2.online()
    c.check(audit2.count() == 0, "游标已持久化：重启后不重复消费")
    reopened.publish("order-6")
    c.check(audit2.count() == 1 && audit2.cursor() == 6, "新消息从游标处继续")

    println("\n[4] Lou 端点")
    let offsets2 = FileDurableStateStore("${dir}/offsets")
    let app = Lou([withHost("127.0.0.1"), withPort(0u16), withHideBanner(), withHideRoutes()])
    buildApp(app, reopened, audit2, offsets2)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    try {
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 15).build()
        let pub = readBody(client.post("http://127.0.0.1:${port}/publish?event=order-7", ""))
        c.check(pub.contains("\"seq\":7"), "POST /publish 返回序号：${pub}")
        let hist = readBody(client.get("http://127.0.0.1:${port}/history?from=6"))
        c.check(hist.contains("order-6") && hist.contains("order-7"), "GET /history 支持 from")
        let auditJson = readBody(client.get("http://127.0.0.1:${port}/audit"))
        c.check(auditJson.contains("7|order-7"), "在线审计方收到新消息")
        let off = readBody(client.post("http://127.0.0.1:${port}/audit/offline", ""))
        c.check(off.contains("\"online\":false"), "POST /audit/offline 生效")
        let _ = readBody(client.post("http://127.0.0.1:${port}/publish?event=order-8", ""))
        let back = readBody(client.post("http://127.0.0.1:${port}/audit/online", ""))
        c.check(back.contains("\"processed\":3") && back.contains("\"cursor\":8"),
            "重连补齐 order-8（游标 8）：${back}")
        // SSE：从 6 开始补历史，3 条后结束
        let sse = readBody(client.get("http://127.0.0.1:${port}/stream?from=6&limit=3"))
        c.check(sse.contains("6|order-6") && sse.contains("7|order-7") && sse.contains("8|order-8"),
            "SSE 先补历史后结束")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
    }

    println("\nex41：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
