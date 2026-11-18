/*
 * CActor Ex 13 - 事件投影读模型（对标 Akka Projection / CQRS 示例）
 *
 * 真实约束：
 * - 写侧只追加事件（不可变、按 persistenceId 有序），不更新读侧；
 * - 读侧（投影）消费事件流，维护可查询的读模型；
 * - 投影必须**幂等**：重放同一段事件不能重复计数；
 * - 支持"全量重建"（从 seq 1 重放）与"增量投影"（只消费新事件）；
 * - 读侧挂了不影响写侧，重建后与事件流一致。
 *
 * 说明：CActor 目前没有内建 `Projection`（差距 4.12，`CQRSBus` 已弃用），
 * 本示例用 `FileEventStore.replay` + 本地读模型手写投影——这正是移植 Akka Projection 时需要自己补的部分。
 *
 * 组件：
 * - `FileEventStore.append / replay / highestSequenceNr`（journal_v2，fsync 落盘）
 * - Projection：按 (persistenceId, seq) 幂等消费；全量重建与增量共用同一套 apply
 * - Lou：POST /orders/:id/place、POST /orders/:id/cancel、GET /report、POST /report/rebuild
 *
 * 自检（退出码 0/1）：
 * 1. 三笔订单 → 读模型 totals/placed/orders 正确；
 * 2. 取消一笔 → totals 相应回退；
 * 3. 全量重建两次 → 两次报告完全一致（幂等、与事件流一致）；
 * 4. 重建后新增事件 → 增量投影只计一次（重复查询不重复加）；
 * 5. Lou 端点与读模型一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex13_projection_read_model
 *   ./target/release/bin/cactor_examples.ex13_projection_read_model serve 18098
 */
package cactor_examples.ex13_projection_read_model

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor_persistence.{EventStore, FileEventStore, BasicEvent}

let HTTP_TIMEOUT = Duration.second * 15

// ======================= 写侧：只追加事件 =======================

class OrderWriter {
    let store: EventStore

    init(store: EventStore) {
        this.store = store
    }

    /** 返回追加后的序号；并发写同一 id 时 expectedSeq 会冲突（单写者约束） */
    func appendEvent(id: String, eventType: String, data: String): Int64 {
        let seq = store.highestSequenceNr(id)
        let event = BasicEvent("${id}-${seq + 1}", id, eventType, data)
        store.append(id, [event], seq)
    }

    func place(id: String, sku: String, qty: Int64): Int64 {
        appendEvent(id, "Placed", "${sku}|${qty}")
    }

    func cancel(id: String, sku: String, qty: Int64): Int64 {
        appendEvent(id, "Cancelled", "${sku}|${qty}")
    }

    func ids(): Array<String> {
        store.persistenceIds()
    }
}

// ======================= 读侧：投影 =======================

class Projection {
    let store: EventStore
    let lock = Mutex()
    // 每个 persistenceId 已消费到的序号：重放/重复投递据此幂等
    let lastSeq = HashMap<String, Int64>()
    // 每个 sku 的净数量（Placed 加、Cancelled 减）
    let totals = HashMap<String, Int64>()
    var placed: Int64 = 0
    var cancelled: Int64 = 0
    var lastEventSeq: Int64 = 0   // 最近一次消费的全局序号（示例观测用）

    init(store: EventStore) {
        this.store = store
    }

    /** 幂等消费一条事件：同一 (id, seq) 重复调用不会重复计数 */
    func apply(id: String, seq: Int64, eventType: String, data: String): Bool {
        synchronized(lock) {
            let seen = lastSeq.get(id) ?? 0
            if (seq <= seen) {
                return false
            }
            let parts = data.split("|")
            let sku = if (parts.size >= 1) { parts[0] } else { "?" }
            let qty = if (parts.size >= 2) { Int64.parse(parts[1]) } else { 0 }
            match (eventType) {
                case "Placed" =>
                    placed++
                    totals[sku] = (totals.get(sku) ?? 0) + qty
                case "Cancelled" =>
                    cancelled++
                    totals[sku] = (totals.get(sku) ?? 0) - qty
                case _ => ()
            }
            lastSeq[id] = seq
            lastEventSeq = seq
            true
        }
    }

    /** 全量重建：清空读模型，按每个 id 从 seq 1 重放（Akka Projection 的 "replay" 等价物） */
    func rebuild(): Unit {
        synchronized(lock) {
            lastSeq.clear()
            totals.clear()
            placed = 0
            cancelled = 0
            lastEventSeq = 0
        }
        rebuildUnsafe()
    }

    private func rebuildUnsafe(): Unit {
        // 先取快照再逐条 apply（apply 内部自己加锁）
        for (id in store.persistenceIds()) {
            for (env in store.replay(id, 1, Int64.Max)) {
                apply(id, env.sequenceNr, env.event.getEventType(), env.event.getEventData())
            }
        }
    }

    /** 增量：只消费每个 id 尚未处理的事件（重启后从 lastSeq 续跑） */
    func catchUp(): Int64 {
        var applied: Int64 = 0
        for (id in store.persistenceIds()) {
            let from = synchronized(lock) { (lastSeq.get(id) ?? 0) + 1 }
            for (env in store.replay(id, from, Int64.Max)) {
                if (apply(id, env.sequenceNr, env.event.getEventType(), env.event.getEventData())) {
                    applied++
                }
            }
        }
        applied
    }

    func report(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{\"placed\":${placed},\"cancelled\":${cancelled},\"orders\":${lastSeq.size},\"totals\":{")
            var first = true
            for ((sku, qty) in totals) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${sku}\":${qty}")
            }
            sb.append("}}")
            sb.toString()
        }
    }
}

// ======================= Lou + 自检 =======================

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

func buildApp(writer: OrderWriter, projection: Projection, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/orders/:id/place", { ctx =>
        let sku = ctx.query("sku") ?? ""
        let qty = ctx.queryInt("qty") ?? 0
        if (sku.isEmpty() || qty <= 0) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(400u16, "{\"error\":\"bad_sku_or_qty\"}")
        } else {
            let seq = writer.place(ctx.param("id"), sku, qty)
            projection.catchUp()
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString("{\"seq\":${seq}}")
        }
    })
    app.post("/orders/:id/cancel", { ctx =>
        let sku = ctx.query("sku") ?? ""
        let qty = ctx.queryInt("qty") ?? 0
        if (sku.isEmpty() || qty <= 0) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(400u16, "{\"error\":\"bad_sku_or_qty\"}")
        } else {
            let seq = writer.cancel(ctx.param("id"), sku, qty)
            projection.catchUp()
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString("{\"seq\":${seq}}")
        }
    })
    app.get("/report", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(projection.report())
    })
    app.post("/report/rebuild", { ctx =>
        projection.rebuild()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(projection.report())
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
    let dir = "/tmp/cactor-ex13-${MonoTime.now().hashCode()}"
    let store = FileEventStore("${dir}/journal")
    let writer = OrderWriter(store)
    let projection = Projection(store)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(writer, projection, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex13：端口绑定失败")
            return 1
        }
        println("ex13 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let app = buildApp(writer, projection, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. 写事件 → 投影 ----
        println("\n[1] 三笔订单进入读模型")
        writer.place("o1", "apple", 2)
        writer.place("o2", "banana", 3)
        writer.place("o3", "apple", 1)
        projection.catchUp()
        let r1 = projection.report()
        c.check(r1.contains("\"placed\":3") && r1.contains("\"orders\":3"), "placed=3、orders=3")
        c.check(r1.contains("\"apple\":3") && r1.contains("\"banana\":3"), "totals 正确：${r1}")

        // ---- 2. 取消回退 ----
        println("\n[2] 取消一笔")
        writer.cancel("o2", "banana", 3)
        projection.catchUp()
        let r2 = projection.report()
        c.check(r2.contains("\"cancelled\":1"), "cancelled=1")
        c.check(r2.contains("\"banana\":0") && r2.contains("\"apple\":3"), "banana 回退为 0，apple 不变：${r2}")

        // ---- 3. 全量重建幂等 ----
        println("\n[3] 全量重建两次结果一致")
        projection.rebuild()
        let rebuilt1 = projection.report()
        projection.rebuild()
        let rebuilt2 = projection.report()
        c.check(rebuilt1 == rebuilt2, "两次重建报告完全相同")
        c.check(rebuilt1 == r2, "重建结果与增量结果一致（${rebuilt1}）")

        // ---- 4. 重建后的增量投影 ----
        println("\n[4] 重建后新增事件只计一次")
        writer.place("o4", "cherry", 5)
        let applied = projection.catchUp()
        c.check(applied == 1, "增量只消费 1 条新事件（实际 ${applied}）")
        let r4 = projection.report()
        c.check(r4.contains("\"cherry\":5") && r4.contains("\"placed\":4"), "cherry=5、placed=4")
        let appliedAgain = projection.catchUp()
        c.check(appliedAgain == 0, "重复 catchUp 不再消费（幂等）")
        c.check(projection.report() == r4, "重复查询报告不变")

        // ---- 5. Lou 端点 ----
        println("\n[5] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).build()
        let place = client.post("http://127.0.0.1:${port}/orders/o5/place?sku=date&qty=7", "")
        c.check(place.status == 200 && readBody(place).contains("\"seq\":1"), "POST place 返回 seq=1")
        let report = client.get("http://127.0.0.1:${port}/report")
        c.check(report.status == 200 && readBody(report).contains("\"date\":7"), "GET /report 含 date=7")
        let rebuild = client.post("http://127.0.0.1:${port}/report/rebuild", "")
        c.check(rebuild.status == 200 && readBody(rebuild).contains("\"date\":7"), "POST rebuild 与事件流一致")
        let bad = client.post("http://127.0.0.1:${port}/orders/o5/place?sku=&qty=0", "")
        c.check(bad.status == 400, "非法参数返回 400")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        store.close()
    }

    println("\nex13：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
