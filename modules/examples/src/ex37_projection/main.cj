/*
 * CActor Ex 37 - 投影进度与全量重建（差距 4.12：Projection）
 *
 * 真实约束：
 * - CQRS 读模型从事件流派生，必须记录"处理到哪"：重启后不能重复应用，也不能漏；
 * - 读模型可能被要求全量重建（口径变更/修复 bug）：清除进度后重放，读模型必须可重建；
 * - 进度要落盘（DurableStateStore），事件在文件日志里（FileEventStore）。
 *
 * 组件：
 * - 框架：`Projection`（ReadJournal.currentEventsByTag + 每标识偏移 + DurableStateStore 持久化）
 * - 存储：FileEventStore（事件）+ FileDurableStateStore（偏移），进程内重启复用同一目录
 * - Lou：POST /orders、GET /carts、POST /rebuild、GET /stats
 *
 * 自检（退出码 0/1）：
 * 1. 20 条事件（两个购物车）→ 读模型与事件一致；
 * 2. 再次 run 幂等（应用 0 条）；
 * 3. 重新打开同一目录：偏移仍在，run 应用 0 条；追加 2 条后只应用 2 条；
 * 4. rebuild：清空读模型 + 清除偏移 + 重放 → 22 条全部重新应用，数值正确；
 * 5. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex37_projection
 *   ./target/release/bin/cactor_examples.ex37_projection serve 18122
 */
package cactor_examples.ex37_projection

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor_persistence.{BasicEvent, FileDurableStateStore, FileEventStore, Projection, ReadJournal}

let WAIT = Duration.second * 20

/** 读模型：购物车 → 总金额与事件数 */
class Carts {
    private let lock = Mutex()
    private let totals = HashMap<String, Int64>()
    private let counts = HashMap<String, Int64>()

    func apply(cart: String, amount: Int64): Unit {
        synchronized(lock) {
            totals[cart] = (totals.get(cart) ?? 0) + amount
            counts[cart] = (counts.get(cart) ?? 0) + 1
        }
    }

    func clear(): Unit {
        synchronized(lock) {
            totals.clear()
            counts.clear()
        }
    }

    func total(cart: String): Int64 {
        synchronized(lock) {
            totals.get(cart) ?? 0
        }
    }

    func count(cart: String): Int64 {
        synchronized(lock) {
            counts.get(cart) ?? 0
        }
    }

    func size(): Int64 {
        synchronized(lock) {
            totals.size
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{")
            var first = true
            for ((cart, total) in totals) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${cart}\":{\"total\":${total},\"events\":${counts.get(cart) ?? 0}}")
            }
            sb.append("}")
            sb.toString()
        }
    }
}

class Env {
    let eventDir: String
    let offsetDir: String
    let store: FileEventStore
    let offsets: FileDurableStateStore
    let carts: Carts
    let projection: Projection

    /** model 由调用方传入：重启后读模型应可从事件重建（这里进程内复用同一实例） */
    init(dir: String, model: Carts) {
        this.eventDir = "${dir}/events"
        this.offsetDir = "${dir}/offsets"
        this.store = FileEventStore(eventDir)
        this.offsets = FileDurableStateStore(offsetDir)
        this.carts = model
        this.projection = Projection("cart-totals", ReadJournal(store), offsets, { env =>
            let amount = Int64.parse(env.event.getEventData())
            model.apply(env.persistenceId, amount)
        })
    }

    /** 追加一条事件（tag=cart） */
    func append(cart: String, amount: Int64): Unit {
        let seq = store.highestSequenceNr(cart)
        let event = BasicEvent("e-${cart}-${seq + 1}", cart, "AmountAdded", amount.toString()).withTags(["cart"])
        store.append(cart, [event], seq)
    }

    func drain(): Int64 {
        projection.runOnce("cart")
    }

    /** 全量重建：清空读模型 + 清除偏移 + 重放 */
    func rebuild(): Int64 {
        carts.clear()
        projection.reset()
        projection.runOnce("cart")
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

func buildApp(env: Env, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/orders", { ctx =>
        let cart = ctx.query("cart") ?? "cart-a"
        let amount = ctx.queryInt("amount") ?? 1
        env.append(cart, amount)
        env.drain()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"cart\":\"${cart}\",\"amount\":${amount},\"total\":${env.carts.total(cart)}}")
    })
    app.get("/carts", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(env.carts.json())
    })
    app.post("/rebuild", { ctx =>
        let applied = env.rebuild()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"reapplied\":${applied},\"carts\":${env.carts.size()}}")
    })
    app.get("/stats", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"cartA\":${env.projection.offsetOf("cart-a")}," +
            "\"cartB\":${env.projection.offsetOf("cart-b")}," +
            "\"carts\":${env.carts.size()}}")
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
    let dir = "/tmp/cactor-ex37-${MonoTime.now().hashCode()}"
    let model = Carts()
    let env = Env(dir, model)
    let app = buildApp(env, 0u16)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let real = buildApp(env, port)
        let actual = startServer(real)
        if (actual == 0u16) {
            println("ex37：端口绑定失败")
            return 1
        }
        println("ex37 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        println("\n[1] 20 条事件 → 读模型")
        for (_ in 0..12) {
            env.append("cart-a", 1)
        }
        for (_ in 0..8) {
            env.append("cart-b", 2)
        }
        c.check(env.drain() == 20, "首次应用 20 条")
        c.check(env.carts.total("cart-a") == 12 && env.carts.total("cart-b") == 16,
            "读模型数值正确（a=12, b=16）")
        c.check(env.projection.offsetOf("cart-a") == 12 && env.projection.offsetOf("cart-b") == 8,
            "偏移落盘：a=12, b=8")

        println("\n[2] 幂等：再次运行")
        c.check(env.drain() == 0, "无新事件时应用 0 条")

        println("\n[3] 重新打开同一目录：从断点续跑")
        let reopened = Env(dir, model)
        c.check(reopened.drain() == 0, "偏移仍在：重新打开后应用 0 条")
        reopened.append("cart-a", 5)
        reopened.append("cart-b", 3)
        c.check(reopened.drain() == 2, "追加 2 条只应用 2 条")

        println("\n[4] 全量重建")
        let reapplied = reopened.rebuild()
        c.check(reapplied == 22, "重建重放 22 条（实际 ${reapplied}）")
        c.check(reopened.carts.total("cart-a") == 17 && reopened.carts.total("cart-b") == 19,
            "重建后数值正确（a=17, b=19）")

        println("\n[5] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let post = readBody(client.post("http://127.0.0.1:${port}/orders?cart=cart-a&amount=2", ""))
        c.check(post.contains("\"total\":19"), "POST /orders 追加并投影（a→19）：${post}")
        let carts = readBody(client.get("http://127.0.0.1:${port}/carts"))
        c.check(carts.contains("\"cart-a\"") && carts.contains("\"total\":19"), "GET /carts 反映读模型")
        let stats = readBody(client.get("http://127.0.0.1:${port}/stats"))
        c.check(stats.contains("\"cartA\":14"), "GET /stats 反映偏移（a=14）")
        let rb = readBody(client.post("http://127.0.0.1:${port}/rebuild", ""))
        c.check(rb.contains("\"reapplied\":23"), "POST /rebuild 重放全部 23 条")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
    }

    println("\nex37：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
