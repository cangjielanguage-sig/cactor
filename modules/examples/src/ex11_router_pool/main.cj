/*
 * CActor Ex 11 - 路由器池与广播（对标 Akka Routers.pool / protoactor-go router 示例）
 *
 * 真实约束：
 * - 一组工作 Actor 共享流量，轮询分发（不依赖调用方自己算）；
 * - 广播消息必须到每个目标；
 * - 目标数量可以动态增减；目标停止后必须立刻从池里摘除，流量只给存活目标；
 * - 路由器本身是 Actor：转发保留原发送方（ask 经路由器也能往返）。
 *
 * 组件：
 * - cactor_patterns.routing：Routers.pool / RouterActor / RoundRobinRoutingStrategy /
 *   Broadcast / GetRoutees / AddRoutee / RemoveRoutee
 * - Lou：POST /dispatch?n=、POST /broadcast、GET /stats
 *
 * 自检（退出码 0/1）：
 * 1. 池 4 个目标，派发 8 条 → 每个恰好 2 条（轮询）；
 * 2. 广播 1 条 → 每个再加 1；
 * 3. 停掉 1 个目标 → 路由表自动变成 3，后续 8 条只给存活目标；
 * 4. 动态加入新目标 → 路由表回到 4，新目标能收到流量；
 * 5. Lou 端点与路由器状态一致。
 *
 * 说明：CActor 目前没有"集群感知路由器"（按成员负载跨节点选目标，差距 4.5）；
 * 本示例展示单节点路由器，跨节点请用 Receptionist + 自建负载视图。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex11_router_pool
 *   ./target/release/bin/cactor_examples.ex11_router_pool serve 18096
 */
package cactor_examples.ex11_router_pool

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext, ActorRef}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_core.dsl.AskInbox
import cactor_patterns.routing.*

let ROUTE_TIMEOUT = Duration.second * 10

/** 共享计数（示例观测用；业务状态应留在 Actor 内） */
class Counts {
    let lock = Mutex()
    let hits = HashMap<String, Int64>()

    func hit(tag: String): Unit {
        synchronized(lock) {
            hits[tag] = (hits.get(tag) ?? 0) + 1
        }
    }

    func of(tag: String): Int64 {
        synchronized(lock) {
            hits.get(tag) ?? 0
        }
    }

    func total(): Int64 {
        synchronized(lock) {
            var n: Int64 = 0
            for ((_, v) in hits) {
                n += v
            }
            n
        }
    }

    func tags(): Array<String> {
        synchronized(lock) {
            hits.keys().toArray()
        }
    }
}

class Worker <: Actor {
    let tag: String
    let counts: Counts

    init(tag: String, counts: Counts) {
        this.tag = tag
        this.counts = counts
    }

    public prop name: String { get() { "worker-${tag}" } }
    public prop description: String { get() { "routee" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            counts.hit(tag)
            context.reply(StringMessage("${tag}|${s.getContent()}"))
        }
        MessageResult.Handled
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
        sleep(Duration.millisecond * 10)
    }
    cond()
}

func routeeCount(router: ActorRef): Int64 {
    match (AskInbox.ask(router, GetRoutees()).get(ROUTE_TIMEOUT) as Routees) {
        case Some(r) => Int64(r.routees.size)
        case None => -1
    }
}

func statsJson(counts: Counts, tags: Array<String>, router: ActorRef): String {
    let sb = StringBuilder()
    sb.append("{\"routees\":${routeeCount(router)},\"total\":${counts.total()},\"workers\":[")
    var first = true
    for (t in tags) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("{\"tag\":\"${t}\",\"count\":${counts.of(t)}}")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(router: ActorRef, counts: Counts, tags: Array<String>, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    app.post("/dispatch", { ctx =>
        let n = ctx.queryInt("n") ?? 1
        for (_ in 0..n) {
            router.tell(StringMessage("job"))
        }
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"sent\":${n}}")
    })
    app.post("/broadcast", { ctx =>
        let text = ctx.query("text") ?? "all"
        router.tell(Broadcast(StringMessage(text)))
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString("{\"broadcast\":\"${text}\"}")
    })
    app.get("/stats", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(statsJson(counts, tags, router))
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
    let system = CActorSystemFactory.create("ex11-router")
    let counts = Counts()
    let tags = ["w0", "w1", "w2", "w3"]
    // 组路由器：目标是已有 Actor（每个有自己的 tag，便于断言分布）；
    // 池路由器（Routers.pool）用法相同，只是目标由路由器创建/管理。
    let refs = Array<ActorRef>(4, { i => system.actorOf({ => Worker(tags[i], counts) }, "worker-${i}") })
    let group = system.actorOf(Routers.group(refs, RoundRobinRoutingStrategy()), "router-group")

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let app = buildApp(group, counts, tags, port)
        let actual = startServer(app)
        if (actual == 0u16) {
            println("ex11：端口绑定失败")
            return 1
        }
        println("ex11 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let app = buildApp(group, counts, tags, 0u16)
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")

    try {
        // ---- 1. 轮询 ----
        println("\n[1] 4 个目标，派发 8 条（轮询）")
        for (_ in 0..8) {
            group.tell(StringMessage("job"))
        }
        let balanced = waitUntil(Duration.second * 5, { => counts.total() == 8 })
        c.check(balanced, "8 条全部送达（total=${counts.total()}）")
        var even = true
        for (t in tags) {
            even = even && counts.of(t) == 2
        }
        c.check(even, "每个目标恰好 2 条：${counts.of("w0")}/${counts.of("w1")}/${counts.of("w2")}/${counts.of("w3")}")

        // ---- 2. 广播 ----
        println("\n[2] 广播")
        group.tell(Broadcast(StringMessage("all")))
        let broadcasted = waitUntil(Duration.second * 5, { => counts.total() == 12 })
        c.check(broadcasted, "广播到达全部 4 个目标（total=${counts.total()}）")

        // ---- 3. 目标停止后自动摘除 ----
        println("\n[3] 停掉一个目标后自动摘除")
        system.stop(refs[0])
        let shrunk = waitUntil(Duration.second * 5, { => routeeCount(group) == 3 })
        c.check(shrunk, "路由表自动变为 3（实际 ${routeeCount(group)}）")
        let beforeW0 = counts.of("w0")
        for (_ in 0..8) {
            group.tell(StringMessage("after-stop"))
        }
        let delivered = waitUntil(Duration.second * 5, { => counts.total() == 20 })
        c.check(delivered, "后续 8 条全部送达存活目标（total=${counts.total()}）")
        c.check(counts.of("w0") == beforeW0, "已停止的目标不再收到消息")

        // ---- 4. 动态加入目标 ----
        println("\n[4] 动态加入新目标")
        let newcomer = system.actorOf({ => Worker("w4", counts) }, "worker-4")
        group.tell(AddRoutee(newcomer))
        let grew = waitUntil(Duration.second * 5, { => routeeCount(group) == 4 })
        c.check(grew, "路由表回到 4（实际 ${routeeCount(group)}）")
        for (_ in 0..8) {
            group.tell(StringMessage("after-add"))
        }
        let ok = waitUntil(Duration.second * 5, { => counts.total() == 28 })
        c.check(ok, "新增目标后 8 条全部送达（total=${counts.total()}）")
        c.check(counts.of("w4") > 0, "新目标收到了流量（${counts.of("w4")} 条）")

        // ---- 5. Lou 端点 ----
        println("\n[5] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 10).build()
        let dispatch = client.post("http://127.0.0.1:${port}/dispatch?n=4", "")
        c.check(dispatch.status == 200 && readBody(dispatch).contains("\"sent\":4"), "POST /dispatch 返回 sent=4")
        let stats = client.get("http://127.0.0.1:${port}/stats")
        let statsBody = readBody(stats)
        c.check(stats.status == 200 && statsBody.contains("\"total\":32"), "GET /stats 总数 32（实际正文 ${statsBody}）")
        let bc = client.post("http://127.0.0.1:${port}/broadcast?text=hello", "")
        c.check(bc.status == 200, "POST /broadcast 返回 200")
        c.check(waitUntil(Duration.second * 5, { => counts.total() == 36 }), "广播经 HTTP 再投递 4 条（total=${counts.total()}）")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        system.terminate()
    }

    println("\nex11：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
