/*
 * CActor Ex 18 - 持久化发布订阅（对标 Akka DistributedPubSub 的 durable subscriber，差距 4.8 的应用层补法）
 *
 * 真实约束：
 * - 订阅者离线期间发布的消息不能丢（普通 PubSub 只保证在线投递）；
 * - 每个订阅者有自己的游标，互不影响；
 * - 同一订阅者重复拉取不能重复生效（至少一次 + 幂等消费）；
 * - 订阅者/服务进程重启后游标不能回退。
 *
 * 实现方式（全部用仓内已有件）：
 * - 消息持久化：`FileEventStore`，每个 topic 一个 persistenceId（journal_v2，fsync）
 * - 订阅游标：`FileDurableStateStore`，键 `sub|<topic>|<subscriber>`，值为已消费的最大序号
 * - 拉取：`replay(topic, cursor+1, ∞)`，读完后用期望修订号更新游标（CAS，重启不回退）
 *
 * 说明：CActor 的 `DistributedPubSub` 本身不持久化订阅（差距 4.8）；本示例是推荐的补法：
 * 在线通知仍可用 PubSub，落库与重放由业务用 journal + 游标完成。
 *
 * 组件：
 * - FileEventStore / FileDurableStateStore
 * - DurableTopic：publish / poll（每订阅者恰好一次）
 * - Lou：POST /topics/:t/publish、GET /topics/:t/messages?sub=
 *
 * 自检（退出码 0/1）：
 * 1. 离线期间发布 3 条 → 首次拉取按序拿到 3 条；
 * 2. 再次拉取为空（游标推进）；
 * 3. 再发布 2 条 → 只拿到新增的 2 条；
 * 4. 重建存储对象（模拟重启）→ 游标不回退，仍为空；
 * 5. 新订阅者从 0 开始，能拿到全部 5 条（订阅者之间隔离）；
 * 6. Lou 端点一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex18_durable_pubsub
 *   ./target/release/bin/cactor_examples.ex18_durable_pubsub serve 18103
 */
package cactor_examples.ex18_durable_pubsub

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor_persistence.{EventStore, FileEventStore, BasicEvent, DurableStateStore, FileDurableStateStore}

let HTTP_TIMEOUT = Duration.second * 15

func topicId(topic: String): String {
    "topic|${topic}"
}

func cursorId(topic: String, subscriber: String): String {
    "sub|${topic}|${subscriber}"
}

/** 持久化主题：发布写日志，拉取按订阅者游标重放 */
class DurableTopic {
    let journal: EventStore
    let cursors: DurableStateStore
    let lock = Mutex()

    init(journal: EventStore, cursors: DurableStateStore) {
        this.journal = journal
        this.cursors = cursors
    }

    func publish(topic: String, payload: String): Int64 {
        if (topic.isEmpty() || topic.contains("|")) {
            throw IllegalArgumentException("非法 topic：${topic}")
        }
        let pid = topicId(topic)
        let seq = journal.highestSequenceNr(pid)
        journal.append(pid, [BasicEvent("${pid}-${seq + 1}", pid, "msg", payload)], seq)
    }

    func cursorOf(topic: String, subscriber: String): Int64 {
        match (cursors.get(cursorId(topic, subscriber))) {
            case Some(r) => Int64.parse(r.state)
            case None => 0
        }
    }

    /** 拉取游标之后、最多 max 条；推进游标后再返回（重复调用不会重复返回） */
    func poll(topic: String, subscriber: String, max: Int64): Array<String> {
        synchronized(lock) {
            let key = cursorId(topic, subscriber)
            let last = cursorOf(topic, subscriber)
            let envs = journal.replay(topicId(topic), last + 1, Int64.Max)
            let out = ArrayList<String>()
            var consumed: Int64 = last
            for (env in envs) {
                if (Int64(out.size) >= max) {
                    break
                }
                out.add(env.event.getEventData())
                consumed = env.sequenceNr
            }
            if (consumed > last) {
                cursors.upsert(key, cursors.revisionOf(key) + 1, "${consumed}")
            }
            out.toArray()
        }
    }

    func total(topic: String): Int64 {
        journal.highestSequenceNr(topicId(topic))
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

func jsonEscape(s: String): String {
    let sb = StringBuilder()
    for (b in s.toArray()) {
        if (b == UInt8(UInt32(r'"'))) {
            sb.append("\\\"")
        } else if (b == UInt8(UInt32(r'\\'))) {
            sb.append("\\\\")
        } else if (b == UInt8(10)) {
            sb.append("\\n")
        } else {
            sb.append(String.fromUtf8([b]))
        }
    }
    sb.toString()
}

func messagesJson(messages: Array<String>, total: Int64, cursor: Int64): String {
    let sb = StringBuilder()
    sb.append("{\"total\":${total},\"cursor\":${cursor},\"messages\":[")
    var first = true
    for (m in messages) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${jsonEscape(m)}\"")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(topic: DurableTopic, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/topics/:t/publish", { ctx =>
        let t = ctx.param("t")
        let payload = ctx.query("payload") ?? ""
        if (payload.isEmpty()) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(400u16, "{\"error\":\"empty_payload\"}")
        } else {
            let seq = topic.publish(t, payload)
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString("{\"seq\":${seq}}")
        }
    })
    app.get("/topics/:t/messages", { ctx =>
        let t = ctx.param("t")
        let sub = ctx.query("sub") ?? ""
        let max = ctx.queryInt("max") ?? 100
        if (sub.isEmpty() || max <= 0) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(400u16, "{\"error\":\"bad_sub_or_max\"}")
        } else {
            let cursor = topic.cursorOf(t, sub)
            let messages = topic.poll(t, sub, max)
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString(messagesJson(messages, topic.total(t), cursor))
        }
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
    let dir = "/tmp/cactor-ex18-${MonoTime.now().hashCode()}"
    let journalPath = "${dir}/journal"
    let cursorPath = "${dir}/cursors"

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let topic = DurableTopic(FileEventStore(journalPath), FileDurableStateStore(cursorPath))
        let app = buildApp(topic, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex18：端口绑定失败")
            return 1
        }
        println("ex18 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let journal = FileEventStore(journalPath)
    let cursors = FileDurableStateStore(cursorPath)
    let topic = DurableTopic(journal, cursors)
    let topicName = "orders"

    try {
        // ---- 1. 离线期间发布，上线后按序补齐 ----
        println("\n[1] 订阅者离线期间发布 3 条")
        topic.publish(topicName, "order-1")
        topic.publish(topicName, "order-2")
        topic.publish(topicName, "order-3")
        let first = topic.poll(topicName, "alice", 100)
        c.check(first.size == 3 && first[0] == "order-1" && first[2] == "order-3", "按序补齐 3 条：${first}")

        // ---- 2. 重复拉取不重复 ----
        println("\n[2] 游标推进后重复拉取为空")
        let again = topic.poll(topicName, "alice", 100)
        c.check(again.isEmpty(), "第二次拉取为空（实际 ${again.size}）")

        // ---- 3. 只拿新增 ----
        println("\n[3] 新增事件只拉新增部分")
        topic.publish(topicName, "order-4")
        topic.publish(topicName, "order-5")
        let incremental = topic.poll(topicName, "alice", 100)
        c.check(incremental.size == 2 && incremental[0] == "order-4", "只拿到新增 2 条：${incremental}")

        // ---- 4. 重启后游标不回退 ----
        println("\n[4] 重建存储对象（模拟重启）")
        journal.close()
        let journal2 = FileEventStore(journalPath)
        let cursors2 = FileDurableStateStore(cursorPath)
        let topic2 = DurableTopic(journal2, cursors2)
        let restoredCursor = topic2.cursorOf(topicName, "alice")
        c.check(restoredCursor == 5, "游标恢复为 5（实际 ${restoredCursor}）")
        c.check(topic2.poll(topicName, "alice", 100).isEmpty(), "重启后重复拉取仍为空")

        // ---- 5. 订阅者隔离 ----
        println("\n[5] 新订阅者从 0 开始")
        let bobs = topic2.poll(topicName, "bob", 100)
        c.check(bobs.size == 5 && bobs[4] == "order-5", "bob 拿到全部 5 条（${bobs.size}）")
        c.check(topic2.poll(topicName, "alice", 100).isEmpty(), "bob 的拉取不影响 alice 的游标")

        // ---- 6. Lou ----
        println("\n[6] Lou 端点")
        let app = buildApp(topic2, 0u16)
        let port = startServer(app)
        c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
        let client = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).build()
        let pub = client.post("http://127.0.0.1:${port}/topics/orders/publish?payload=order-6", "")
        c.check(pub.status == 200 && readBody(pub).contains("\"seq\":6"), "POST publish 返回 seq=6")
        let aliceMsgs = client.get("http://127.0.0.1:${port}/topics/orders/messages?sub=alice")
        c.check(readBody(aliceMsgs).contains("order-6"), "alice 通过 HTTP 拉到 order-6")
        let aliceAgain = client.get("http://127.0.0.1:${port}/topics/orders/messages?sub=alice")
        c.check(readBody(aliceAgain).contains("\"messages\":[]"), "再次拉取为空")
        let bad = client.get("http://127.0.0.1:${port}/topics/orders/messages?sub=")
        c.check(bad.status == 400, "缺少订阅者返回 400")
        client.close()
        app.stop()
    } finally {
        // 已在第 4 步关闭旧句柄；这里兜底关闭新句柄
        try {
            journal.close()
        } catch (_: Exception) {}
    }

    println("\nex18：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
