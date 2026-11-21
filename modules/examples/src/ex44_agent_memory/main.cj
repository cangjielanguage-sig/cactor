/*
 * CActor Ex 44 - Agent 记忆与画像（docs/plan/2026117-lz.md §31 第 3 项）
 *
 * 真实约束：
 * - 记忆是追加的事实流：写侧幂等（同一 deliveryId 重试只记一次），读侧要能重建；
 * - schema 会演进：早期只存文本，后来加类型/重要度，旧事件必须能被新代码读回；
 * - 读模型有两种形态：时间线（逐条）与画像（按类型聚合），口径变化后可全量重建；
 * - 下游（推荐/审计）可能离线，变更流要能补齐；
 * - 多租户强隔离：租户内的事件绝不能出现在另一个租户的读模型里。
 *
 * 组件：
 * - FileEventStore（每租户一个）+ `BasicEvent.withTags`：事件流
 * - `Projection`（ex37）：一个投影同时维护时间线与画像，支持 reset 全量重建
 * - `EventAdapter.upcast`（ex38）：v1（纯文本）/v2（类型|文本）→ 当前（类型|文本|重要度）
 * - `DurableTopic`（ex41）：变更流，SSE 先补历史再接实时
 * - `AtLeastOnceDelivery` / `IdempotentReceiver`（ex40）：按 deliveryId 幂等写入
 * - Lou：POST /memories、GET /timeline、GET /profile、POST /rebuild、
 *        GET /changes/stream（SSE）、GET /raw
 *
 * 自检（退出码 0/1）：
 * 1. v1/v2/当前格式混合恢复，时间线与画像正确（upcast 生效）；
 * 2. 同一 deliveryId 重试只写一条；
 * 3. rebuild 后读模型与重建前一致；
 * 4. 变更流离线后重连补齐；租户隔离；
 * 5. Lou 端点与 SSE 一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex44_agent_memory
 *   ./target/release/bin/cactor_examples.ex44_agent_memory serve 18189
 */
package cactor_examples.ex44_agent_memory

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor_core.message.{Message, StringMessage}
import cactor_persistence.{BasicEvent, DurableStateStore, DurableSubscription, DurableTopic, EventEnvelope,
    FileDurableStateStore, FileEventStore, Projection, ReadJournal}
import cactor_persistence.typed.{EventAdapter}
import cactor_patterns.delivery.*

let TAG = "memory"
let WAIT = Duration.second * 15

/** 当前事件格式：kind|text|importance */
class MemoryEvent {
    let kind: String
    let text: String
    let importance: Int64

    init(kind: String, text: String, importance: Int64) {
        this.kind = kind
        this.text = text
        this.importance = importance
    }
}

/** 只认当前格式；历史格式由 upcast 升级 */
class MemoryAdapter <: EventAdapter<MemoryEvent> {
    public func manifest(event: MemoryEvent): String {
        "MemoryRecorded"
    }

    public func toJournal(event: MemoryEvent): String {
        "${event.kind}|${event.text}|${event.importance}"
    }

    public func fromJournal(manifest: String, data: String): Array<MemoryEvent> {
        match (manifest) {
            case "MemoryRecorded" =>
                let parts = data.split("|")
                if (parts.size == 3) {
                    [MemoryEvent(parts[0], parts[1], Int64.parse(parts[2]))]
                } else {
                    throw IllegalArgumentException("MemoryRecorded 字段数错误")
                }
            case other => throw IllegalArgumentException("未知记忆事件：${other}")
        }
    }

    public func upcast(manifest: String, data: String): (String, String) {
        match (manifest) {
            case "MemoryV1" => ("MemoryRecorded", "note|${data}|1")
            case "MemoryV2" => ("MemoryRecorded", "${data}|1")
            case _ => (manifest, data)
        }
    }
}

/** 读模型：时间线（逐条）+ 画像（按类型聚合） */
class MemoryModels {
    private let lock = Mutex()
    private let timeline = HashMap<String, ArrayList<String>>()
    private let kinds = HashMap<String, HashMap<String, Int64>>()
    private let totals = HashMap<String, Int64>()
    private let maxImportance = HashMap<String, Int64>()

    func apply(user: String, kind: String, text: String, importance: Int64, seq: Int64): Unit {
        synchronized(lock) {
            match (timeline.get(user)) {
                case Some(lines) => lines.add("#${seq} ${kind}:${text}(${importance})")
                case None =>
                    let lines = ArrayList<String>()
                    lines.add("#${seq} ${kind}:${text}(${importance})")
                    timeline[user] = lines
            }
            let counts = match (kinds.get(user)) {
                case Some(c) => c
                case None =>
                    let c = HashMap<String, Int64>()
                    kinds[user] = c
                    c
            }
            counts[kind] = (counts.get(kind) ?? 0) + 1
            totals[user] = (totals.get(user) ?? 0) + 1
            let previous = maxImportance.get(user) ?? 0
            maxImportance[user] = if (importance > previous) { importance } else { previous }
        }
    }

    func clear(): Unit {
        synchronized(lock) {
            timeline.clear()
            kinds.clear()
            totals.clear()
            maxImportance.clear()
        }
    }

    func timelineJson(user: String): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("[")
            var first = true
            match (timeline.get(user)) {
                case Some(lines) =>
                    for (line in lines) {
                        if (!first) {
                            sb.append(",")
                        }
                        first = false
                        sb.append("\"${line}\"")
                    }
                case None => ()
            }
            sb.append("]")
            sb.toString()
        }
    }

    func profileJson(user: String): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{\"user\":\"${user}\",\"total\":${totals.get(user) ?? 0},")
            sb.append("\"maxImportance\":${maxImportance.get(user) ?? 0},\"kinds\":{")
            var first = true
            match (kinds.get(user)) {
                case Some(counts) =>
                    for ((kind, n) in counts) {
                        if (!first) {
                            sb.append(",")
                        }
                        first = false
                        sb.append("\"${kind}\":${n}")
                    }
                case None => ()
            }
            sb.append("}}")
            sb.toString()
        }
    }

    func sizeOf(user: String): Int64 {
        synchronized(lock) {
            totals.get(user) ?? 0
        }
    }
}

/** 一个租户：独立事件库、游标、变更流、去重表与读模型 */
class Tenant {
    let name: String
    let store: FileEventStore
    let offsets: DurableStateStore
    let changes: DurableTopic
    let models: MemoryModels
    let projection: Projection
    let receiver: IdempotentReceiver
    private let lock = Mutex()

    init(dir: String, name: String) {
        this.name = name
        this.store = FileEventStore("${dir}/events-${name}")
        this.offsets = FileDurableStateStore("${dir}/offsets-${name}")
        this.changes = DurableTopic("memory-${name}", FileDurableStateStore("${dir}/topics-${name}"), retention: 500)
        this.models = MemoryModels()
        let model = models
        let adapter = MemoryAdapter()
        this.projection = Projection("memory-view-${name}", ReadJournal(store), offsets, { env =>
            // upcast 在这里显式执行（投影拿的是原始信封）
            let (manifest, data) = adapter.upcast(env.event.getEventType(), env.event.getEventData())
            let events = adapter.fromJournal(manifest, data)
            if (events.size == 1) {
                let ev = events[0]
                let user = env.persistenceId[5..]
                model.apply(user, ev.kind, ev.text, ev.importance, env.sequenceNr)
            }
        })
        this.receiver = IdempotentReceiver(DurableDeliveryIdStore(FileDurableStateStore("${dir}/dedup-${name}"),
            prefix: "mem-${name}"))
    }

    private func pidOf(user: String): String {
        "user-${user}"
    }

    /** 直接追加事件（用于预置旧格式数据） */
    func appendRaw(user: String, manifest: String, data: String): Unit {
        synchronized(lock) {
            let pid = pidOf(user)
            let seq = store.highestSequenceNr(pid)
            store.append(pid, [BasicEvent("e-${pid}-${seq + 1}", pid, manifest, data).withTags([TAG])], seq)
            changes.publish("${user}|raw|${data}")
        }
    }

    /** 幂等写入：同一 deliveryId 只追加一次 */
    func record(user: String, kind: String, text: String, importance: Int64, deliveryId: String): Bool {
        synchronized(lock) {
            // 先把数据 append 与变更发布放在 handler 内，handler 成功后才标记 deliveryId
            return receiver.handle(DeliveryEnvelope(deliveryId,
                StringMessage("${user}|${kind}|${text}|${importance}")), { payload =>
                let content = (payload as StringMessage).getOrThrow().getContent()
                let parts = content.split("|")
                let pid = pidOf(parts[0])
                let seq = store.highestSequenceNr(pid)
                let event = BasicEvent("e-${pid}-${seq + 1}", pid, "MemoryRecorded",
                    "${parts[1]}|${parts[2]}|${parts[3]}").withTags([TAG])
                store.append(pid, [event], seq)
                changes.publish("${parts[0]}|${parts[1]}|${parts[2]}")
            })
        }
    }

    /** 投影追赶；返回本次应用条数 */
    func refresh(): Int64 {
        synchronized(lock) {
            projection.runOnce(TAG)
        }
    }

    func rebuild(): Int64 {
        synchronized(lock) {
            models.clear()
            projection.reset()
            projection.runOnce(TAG)
        }
    }

    func timelineJson(user: String): String {
        models.timelineJson(user)
    }

    func profileJson(user: String): String {
        models.profileJson(user)
    }

    func rawJson(user: String): String {
        let pid = pidOf(user)
        let sb = StringBuilder()
        sb.append("[")
        var first = true
        for (env in store.replay(pid, 1, Int64.Max)) {
            if (!first) {
                sb.append(",")
            }
            first = false
            sb.append("{\"seq\":${env.sequenceNr},\"manifest\":\"${env.event.getEventType()}\"," +
                "\"data\":\"${env.event.getEventData()}\"}")
        }
        sb.append("]")
        sb.toString()
    }
}

class Env {
    let dir: String
    private let lock = Mutex()
    private let tenants = HashMap<String, Tenant>()

    init(dir: String) {
        this.dir = dir
    }

    func tenant(name: String): Tenant {
        synchronized(lock) {
            match (tenants.get(name)) {
                case Some(t) => t
                case None =>
                    let t = Tenant(dir, name)
                    tenants[name] = t
                    t
            }
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

func buildApp(env: Env, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/memories", { ctx =>
        let tenant = ctx.query("tenant") ?? "t1"
        let user = ctx.query("user") ?? "u1"
        let kind = ctx.query("kind") ?? "note"
        let text = ctx.query("text") ?? ""
        let importance = ctx.queryInt("importance") ?? 1
        let deliveryId = ctx.query("deliveryId") ?? "d-${MonoTime.now().hashCode()}"
        let t = env.tenant(tenant)
        let first = t.record(user, kind, text, importance, deliveryId)
        let applied = t.refresh()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"duplicate\":${!first},\"applied\":${applied},\"deliveryId\":\"${deliveryId}\"}")
    })
    app.get("/timeline", { ctx =>
        let t = env.tenant(ctx.query("tenant") ?? "t1")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(t.timelineJson(ctx.query("user") ?? "u1"))
    })
    app.get("/profile", { ctx =>
        let t = env.tenant(ctx.query("tenant") ?? "t1")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(t.profileJson(ctx.query("user") ?? "u1"))
    })
    app.post("/rebuild", { ctx =>
        let t = env.tenant(ctx.query("tenant") ?? "t1")
        let applied = t.rebuild()
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"reapplied\":${applied}}")
    })
    app.get("/raw", { ctx =>
        let t = env.tenant(ctx.query("tenant") ?? "t1")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(t.rawJson(ctx.query("user") ?? "u1"))
    })
    app.get("/changes/stream", { ctx =>
        let t = env.tenant(ctx.query("tenant") ?? "t1")
        let from = ctx.queryInt("from") ?? 1
        let limit = ctx.queryInt("limit") ?? 50
        ctx.setSingleHeader("Cache-Control", "no-cache")
        ctx.sse({ stream =>
            let gate = Gate()
            let seen = AtomicInt64(0)
            let name = "sse-${MonoTime.now().hashCode()}"
            let sub = t.changes.subscribe(name, t.offsets, { seq, payload =>
                stream.sendEvent("change", payload)
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
    let dir = "/tmp/cactor-ex44-${MonoTime.now().hashCode()}"

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let env = Env(dir)
        let app = buildApp(env, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex44：端口绑定失败")
            return 1
        }
        println("ex44 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let env = Env(dir)
    let t1 = env.tenant("t1")

    println("\n[1] 混合 schema 恢复")
    t1.appendRaw("u1", "MemoryV1", "likes tea")
    t1.appendRaw("u1", "MemoryV2", "preference|prefers dark mode")
    c.check(t1.record("u1", "fact", "works at ACME", 5, "d1"), "新格式写入成功")
    c.check(t1.record("u2", "note", "hello", 2, "d2"), "第二个用户写入成功")
    let applied = t1.refresh()
    c.check(applied == 4, "首次投影 4 条（含旧格式）")
    let timeline = t1.timelineJson("u1")
    c.check(timeline.contains("note:likes tea(1)") && timeline.contains("preference:prefers dark mode(1)") &&
        timeline.contains("fact:works at ACME(5)"), "时间线按 upcast 后的格式呈现")
    let profile = t1.profileJson("u1")
    c.check(profile.contains("\"total\":3") && profile.contains("\"maxImportance\":5") &&
        profile.contains("\"note\":1") && profile.contains("\"fact\":1"), "画像聚合正确")

    println("\n[2] 幂等写入")
    c.check(!t1.record("u1", "fact", "works at ACME", 5, "d1"), "同一 deliveryId 不重复写")
    c.check(t1.refresh() == 0 && t1.models.sizeOf("u1") == 3, "既有事件与读模型不增长")

    println("\n[3] 全量重建")
    let rebuilt = t1.rebuild()
    c.check(rebuilt == 4, "重建重放 4 条")
    c.check(t1.timelineJson("u1") == timeline, "重建后时间线一致")
    let profileAfter = t1.profileJson("u1")
    c.check(profileAfter.contains("\"total\":3") && profileAfter.contains("\"maxImportance\":5"),
        "重建后画像数值一致")

    println("\n[4] 变更流离线补齐与租户隔离")
    let offsets = FileDurableStateStore("${dir}/audit")
    let seen = ArrayList<String>()
    let audit = t1.changes.subscribe("audit", offsets, { seq, payload => seen.add(payload) })
    c.check(seen.size == 4, "在线订阅补齐已有 4 条变更")
    audit.unsubscribe()
    c.check(t1.record("u1", "event", "met Bob", 3, "d3"), "离线期间写入 d3")
    c.check(t1.record("u1", "event", "moved to Shanghai", 4, "d4"), "离线期间写入 d4")
    t1.refresh()
    c.check(seen.size == 4, "离线期间没有收到实时事件")
    let resumed = ArrayList<String>()
    let audit2 = t1.changes.subscribe("audit", offsets, { seq, payload => resumed.add(payload) })
    c.check(resumed.size == 2 && resumed[1].contains("moved to Shanghai"), "重连补齐 2 条变更")
    audit2.unsubscribe()
    let t2 = env.tenant("t2")
    c.check(t2.record("u1", "note", "other tenant", 1, "d5"), "t2 写入成功")
    t2.refresh()
    c.check(t2.timelineJson("u1").contains("other tenant"), "t2 有自己的读模型")
    c.check(!t1.timelineJson("u1").contains("other tenant"), "t1 看不到 t2 的记忆")

    println("\n[5] Lou 端点")
    let app = buildApp(env, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    try {
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 15).build()
        let created = readBody(client.post(
            "http://127.0.0.1:${port}/memories?tenant=t1&user=u1&kind=note&text=via-http&importance=2&deliveryId=h1", ""))
        c.check(created.contains("\"duplicate\":false"), "POST /memories 首次写入：${created}")
        let dup = readBody(client.post(
            "http://127.0.0.1:${port}/memories?tenant=t1&user=u1&kind=note&text=via-http&importance=2&deliveryId=h1", ""))
        c.check(dup.contains("\"duplicate\":true"), "同一 deliveryId 幂等")
        let tline = readBody(client.get("http://127.0.0.1:${port}/timeline?tenant=t1&user=u1"))
        c.check(tline.contains("via-http"), "GET /timeline 反映新记忆")
        let prof = readBody(client.get("http://127.0.0.1:${port}/profile?tenant=t1&user=u1"))
        c.check(prof.contains("\"note\":2"), "GET /profile 聚合")
        let raw = readBody(client.get("http://127.0.0.1:${port}/raw?tenant=t1&user=u1"))
        c.check(raw.contains("MemoryV1") && raw.contains("MemoryRecorded"), "GET /raw 显示混合 schema")
        let rb = readBody(client.post("http://127.0.0.1:${port}/rebuild?tenant=t1", ""))
        c.check(rb.contains("\"reapplied\":7"), "POST /rebuild 重放 7 条")
        let sse = readBody(client.get("http://127.0.0.1:${port}/changes/stream?tenant=t1&from=1&limit=7"))
        c.check(sse.contains("works at ACME") && sse.contains("via-http"), "SSE 输出变更")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
    }

    println("\nex44：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
