/*
 * CActor Ex 06 - HTTP + Actor REST API（对标 protoactor-go examples/web、Akka HTTP 教程）
 *
 * 真实约束：
 * - 对外是一个 REST 服务（Lou 提供 HTTP），所有读写都经 Actor 邮箱串行化，不加锁；
 * - 请求处理不阻塞邮箱：HTTP 线程 ask 存储 Actor，存储 Actor 不会回调 HTTP；
 * - 存储 Actor 抛异常时由监督策略重启，服务仍然可用（这是"HTTP 层不能因为一次坏请求宕掉"）；
 * - 每个响应都有明确状态码（200/201/404/409/400）。
 *
 * 组件：
 * - Lou：GET/POST/PUT/DELETE /items/:id、GET /items、GET /stats、POST /crash
 * - StoreActor：类型化行为 + 状态随"返回新行为"演进
 * - 监督：supervise(...).onFailure(restart())，/crash 后 Actor 以初始状态重启
 *
 * 自检（退出码 0/1）：
 * 1. 创建 → 读取 → 更新 → 列表，状态码与内容正确；
 * 2. 重复创建返回 409，读不存在的 key 返回 404；
 * 3. 50 个并发创建全部成功且计数正确（Actor 串行化，无丢失）；
 * 4. /crash 触发异常后服务仍可用（监督重启生效）。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex06_http_actor_api        # 自检
 *   ./target/release/bin/cactor_examples.ex06_http_actor_api serve 18086                   # 仅提供服务，供 curl
 */
package cactor_examples.ex06_http_actor_api

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.typed.*

let ASK = Duration.second * 10
let HTTP_TIMEOUT = Duration.second * 10

// ======================= 协议 =======================

enum ItemCmd {
    | Create(String, String, TypedActorRef<String>)     // id, value -> "created" / "exists"
    | Read(String, TypedActorRef<Option<String>>)
    | Update(String, String, TypedActorRef<Bool>)
    | Delete(String, TypedActorRef<Bool>)
    | All(TypedActorRef<Array<String>>)                 // 每项 "id\u0001value"
    | Crash(TypedActorRef<String>)                      // 故意抛异常，验证监督
}

// ======================= 状态（返回新行为） =======================

func withEntry(items: HashMap<String, String>, id: String, value: String): HashMap<String, String> {
    let next = HashMap<String, String>()
    for ((k, v) in items) {
        next[k] = v
    }
    next[id] = value
    next
}

func withoutEntry(items: HashMap<String, String>, id: String): HashMap<String, String> {
    let next = HashMap<String, String>()
    for ((k, v) in items) {
        if (k != id) {
            next[k] = v
        }
    }
    next
}

func storeBehavior(items: HashMap<String, String>): TypedBehavior<ItemCmd> {
    TypedBehaviors.receive<ItemCmd>({ _, msg =>
        match (msg) {
            case ItemCmd.Create(id, value, reply) =>
                if (items.contains(id)) {
                    reply.tell("exists")
                    TypedBehaviors.same<ItemCmd>()
                } else {
                    reply.tell("created")
                    storeBehavior(withEntry(items, id, value))
                }
            case ItemCmd.Read(id, reply) =>
                reply.tell(items.get(id))
                TypedBehaviors.same<ItemCmd>()
            case ItemCmd.Update(id, value, reply) =>
                if (items.contains(id)) {
                    reply.tell(true)
                    storeBehavior(withEntry(items, id, value))
                } else {
                    reply.tell(false)
                    TypedBehaviors.same<ItemCmd>()
                }
            case ItemCmd.Delete(id, reply) =>
                if (items.contains(id)) {
                    reply.tell(true)
                    storeBehavior(withoutEntry(items, id))
                } else {
                    reply.tell(false)
                    TypedBehaviors.same<ItemCmd>()
                }
            case ItemCmd.All(reply) =>
                let out = ArrayList<String>()
                for ((k, v) in items) {
                    out.add("${k}\u{0001}${v}")
                }
                reply.tell(out.toArray())
                TypedBehaviors.same<ItemCmd>()
            case ItemCmd.Crash(reply) =>
                reply.tell("about-to-crash")
                throw IllegalStateException("演示：存储 Actor 处理坏请求时抛出")
        }
    })
}

// ======================= HTTP 辅助 =======================

/**
 * JSON 字符串转义，并且把非 ASCII 字符转成 \\uXXXX。
 * 原因：Lou 的普通响应体（responseBuilder.body(String)）在 1.2.0 下遇到非 ASCII 会返回 500
 * （SSE 路径不受影响，见 ex01）。输出纯 ASCII 的 JSON 转义既合法又绕开了这个问题。
 */
func jsonEscape(s: String): String {
    let sb = StringBuilder()
    for (r in s.runes()) {
        let cp = UInt32(r)
        if (cp == UInt32(r'"')) {
            sb.append("\\\"")
        } else if (cp == UInt32(r'\\')) {
            sb.append("\\\\")
        } else if (cp == 10u32) {
            sb.append("\\n")
        } else if (cp == 13u32) {
            sb.append("\\r")
        } else if (cp == 9u32) {
            sb.append("\\t")
        } else if (cp < 0x20u32) {
            sb.append("\\u${hex4(cp)}")
        } else if (cp <= 0x7Eu32) {
            sb.append(String([r]))
        } else if (cp <= 0xFFFFu32) {
            sb.append("\\u${hex4(cp)}")
        } else {
            // 基本平面之外：UTF-16 代理对
            let v = cp - 0x10000u32
            let hi = 0xD800u32 + (v >> 10)
            let lo = 0xDC00u32 + (v & 0x3FFu32)
            sb.append("\\u${hex4(hi)}\\u${hex4(lo)}")
        }
    }
    sb.toString()
}

func hex4(v: UInt32): String {
    let digits = "0123456789abcdef"
    let b = digits.toArray()
    let out = Array<UInt8>(4, repeat: UInt8(UInt32(r'0')))
    var n = v
    var i = 3
    while (i >= 0) {
        out[i] = b[Int64(n & 0xFu32)]
        n = n >> 4
        i--
    }
    String.fromUtf8(out)
}

func bodyText(ctx: LouHttpContext): String {
    String.fromUtf8(ctx.bodyRaw()).trimAscii()
}

func askCreate(ref: TypedActorRef<ItemCmd>, id: String, value: String): String {
    TypedAsk.ask<ItemCmd, String>(ref, { r => ItemCmd.Create(id, value, r) }, ASK).get(ASK)
}

func askRead(ref: TypedActorRef<ItemCmd>, id: String): Option<String> {
    TypedAsk.ask<ItemCmd, Option<String>>(ref, { r => ItemCmd.Read(id, r) }, ASK).get(ASK)
}

func askUpdate(ref: TypedActorRef<ItemCmd>, id: String, value: String): Bool {
    TypedAsk.ask<ItemCmd, Bool>(ref, { r => ItemCmd.Update(id, value, r) }, ASK).get(ASK)
}

func askDelete(ref: TypedActorRef<ItemCmd>, id: String): Bool {
    TypedAsk.ask<ItemCmd, Bool>(ref, { r => ItemCmd.Delete(id, r) }, ASK).get(ASK)
}

func askAll(ref: TypedActorRef<ItemCmd>): Array<String> {
    TypedAsk.ask<ItemCmd, Array<String>>(ref, { r => ItemCmd.All(r) }, ASK).get(ASK)
}

func askCrash(ref: TypedActorRef<ItemCmd>): String {
    TypedAsk.ask<ItemCmd, String>(ref, { r => ItemCmd.Crash(r) }, ASK).get(ASK)
}

func errorJson(msg: String): String {
    "{\"error\":\"${jsonEscape(msg)}\"}"
}

func itemJson(id: String, value: String): String {
    "{\"id\":\"${jsonEscape(id)}\",\"value\":\"${jsonEscape(value)}\"}"
}

/** 建 Lou 应用并注册路由；返回 (app, store 引用) */
func buildApp(store: TypedActorRef<ItemCmd>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.get("/items/:id", { ctx =>
        let id = ctx.param("id")
        match (askRead(store, id)) {
            case Some(v) =>
                ctx.setSingleHeader("Content-Type", "application/json")
                ctx.writeString(itemJson(id, v))
            case None =>
                ctx.setSingleHeader("Content-Type", "application/json")
                ctx.writeStringWithCode(404u16, errorJson("not_found"))
        }
    })
    app.post("/items/:id", { ctx =>
        let id = ctx.param("id")
        let value = bodyText(ctx)
        if (value.isEmpty()) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(400u16, errorJson("empty_value"))
        } else {
            let r = askCreate(store, id, value)
            ctx.setSingleHeader("Content-Type", "application/json")
            if (r == "created") {
                ctx.writeStringWithCode(201u16, itemJson(id, value))
            } else {
                ctx.writeStringWithCode(409u16, errorJson("already_exists"))
            }
        }
    })
    app.put("/items/:id", { ctx =>
        let id = ctx.param("id")
        let value = bodyText(ctx)
        if (value.isEmpty()) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(400u16, errorJson("empty_value"))
        } else if (askUpdate(store, id, value)) {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeString(itemJson(id, value))
        } else {
            ctx.setSingleHeader("Content-Type", "application/json")
            ctx.writeStringWithCode(404u16, errorJson("not_found"))
        }
    })
    app.delete("/items/:id", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        if (askDelete(store, ctx.param("id"))) {
            ctx.writeString("{\"deleted\":true}")
        } else {
            ctx.writeStringWithCode(404u16, errorJson("not_found"))
        }
    })
    app.get("/items", { ctx =>
        let items = askAll(store)
        let sb = StringBuilder()
        sb.append("{\"items\":[")
        var first = true
        for (entry in items) {
            let parts = entry.split("\u{0001}")
            if (parts.size == 2) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append(itemJson(parts[0], parts[1]))
            }
        }
        sb.append("],\"count\":${items.size}}")
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(sb.toString())
    })
    app.get("/stats", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"count\":${askAll(store).size}}")
    })
    app.post("/crash", { ctx =>
        let ack = askCrash(store)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"ack\":\"${jsonEscape(ack)}\"}")
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
    let system = CActorSystemFactory.create("ex06-http")
    // 监督：坏请求导致异常时重启存储 Actor（Akka 默认语义：状态回到初始值，服务保持可用）
    let store = system.typedActorOf<ItemCmd>(
        TypedBehaviors.supervise(storeBehavior(HashMap<String, String>()))
            .onFailure(TypedSupervisorStrategy.restart()),
        "store")

    // serve 模式：只提供服务，供外部 curl / 压测
    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(store, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex06：端口绑定失败")
            return 1
        }
        println("ex06 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let app = buildApp(store, 0u16)
    let port = startServer(app)
    let c = Checker()
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    let base = "http://127.0.0.1:${port}"
    let client = ClientBuilder().noProxy().readTimeout(HTTP_TIMEOUT).build()

    try {
        // ---- 1. CRUD ----
        println("\n[1] 创建 / 读取 / 更新 / 列表")
        let created = client.post("${base}/items/apple", "red-fuji")
        c.check(created.status == 201, "创建返回 201")
        c.check(readBody(created).contains("\"id\":\"apple\""), "创建响应含 id")
        let duplicated = client.post("${base}/items/apple", "dup")
        c.check(duplicated.status == 409, "重复创建返回 409")
        let read = client.get("${base}/items/apple")
        c.check(read.status == 200 && readBody(read).contains("red-fuji"), "读取返回 200 且内容正确")
        // 非 ASCII 值：响应必须是合法 JSON（\\uXXXX 转义），且不能 500
        let cjk = client.post("${base}/items/cjk", "红富士")
        c.check(cjk.status == 201, "非 ASCII 值创建返回 201（不是 500）")
        let cjkRead = client.get("${base}/items/cjk")
        c.check(cjkRead.status == 200 && readBody(cjkRead).contains("\\u7ea2"), "非 ASCII 值以 \\uXXXX 返回")
        client.delete("${base}/items/cjk")
        let missing = client.get("${base}/items/ghost")
        c.check(missing.status == 404, "不存在的 key 返回 404")
        let updated = client.put("${base}/items/apple", "green-fuji")
        c.check(updated.status == 200 && readBody(updated).contains("green-fuji"), "更新返回 200")
        let updateMissing = client.put("${base}/items/ghost", "x")
        c.check(updateMissing.status == 404, "更新不存在的 key 返回 404")
        let removed = client.delete("${base}/items/apple")
        c.check(removed.status == 200, "删除返回 200")
        c.check(client.get("${base}/items/apple").status == 404, "删除后读取 404")

        // ---- 2. 并发创建：Actor 串行化不丢 ----
        println("\n[2] 50 个并发创建（Ask，Actor 串行化）")
        let futures = ArrayList<TypedPromise<String>>()
        for (i in 0..50) {
            futures.add(TypedAsk.ask<ItemCmd, String>(store,
                { r => ItemCmd.Create("k${i}", "v${i}", r) }, ASK))
        }
        var okCount = 0
        for (f in futures) {
            if (f.get(ASK) == "created") {
                okCount++
            }
        }
        c.check(okCount == 50, "50 个创建全部 201（实际 ${okCount}）")
        let list = client.get("${base}/items")
        c.check(list.status == 200 && readBody(list).contains("\"count\":50"), "列表 count == 50")
        let stats = client.get("${base}/stats")
        c.check(readBody(stats).contains("\"count\":50"), "stats 计数一致")

        // ---- 3. 监督：坏请求后服务可用 ----
        println("\n[3] /crash 触发异常后服务仍可用")
        let crash = client.post("${base}/crash", "")
        c.check(crash.status == 200 && readBody(crash).contains("about-to-crash"), "崩溃请求得到确认")
        // 重启后 Actor 以初始状态运行：服务必须能再次响应（HTTP 200/404 都算存活）
        var alive = false
        for (_ in 0..100) {
            let probe = client.get("${base}/items/k1")
            if (probe.status == 200 || probe.status == 404) {
                alive = true
                break
            }
            sleep(Duration.millisecond * 20)
        }
        c.check(alive, "重启后服务仍能响应")
        c.check(client.get("${base}/stats").status == 200, "stats 端点仍可用")
    } finally {
        client.close()
        app.stop()
        system.terminate()
    }

    println("\nex06：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
