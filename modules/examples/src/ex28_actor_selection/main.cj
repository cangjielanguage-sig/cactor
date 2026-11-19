/*
 * CActor Ex 28 - 远程 ActorSelection（对标 Akka ActorSelection / protoactor 的按路径寻址）
 *
 * 真实约束：
 * - 运维/编排往往只知道"目标属于哪类角色"（worker-*、某前缀的分片实体），
 *   不应该为了发一条消息先去中心目录查全部名称；
 * - 选择必须只落在业务 Actor 上：`$sys`、ask 回复等内部入口不能被通配命中，
 *   否则一次广播就能触发内部协议；
 * - 需要"先看有谁、再发"的场景（resolveOne）必须能区分 0 个与多个匹配。
 *
 * 组件：
 * - 框架：`SimpleRemoteManager.actorSelection(address, path)`、`ActorSelection.tell/resolveNames/resolveOne`
 *   - `tell` 把目标写成 `$sel:<pattern>`，由对端本地展开通配（一次网络帧）；
 *   - `resolveNames` 走 `$sys SELECT`，排序返回业务名称；
 * - Lou：GET /workers、POST /broadcast、POST /send、GET /stats
 *
 * 自检（退出码 0/1）：
 * 1. `worker-*` 命中 4 个 worker，各收到 10 条且按序；counter 不受影响；
 * 2. `worker-?` 只命中单字符后缀的 3 个，worker-10 不增加；
 * 3. resolveNames 排序正确；`*` 不返回任何 `$` 开头的内部入口；
 * 4. resolveOne 唯一匹配可投递；多匹配 / 零匹配报错；
 * 5. 无匹配的选择计入对端死信；
 * 6. Lou 端点与内存一致。
 *
 * 运行：
 *   source tools/env.sh && cjpm build
 *   CACTOR_LOG_LEVEL=error ./target/release/bin/cactor_examples.ex28_actor_selection
 *   ./target/release/bin/cactor_examples.ex28_actor_selection serve 18113
 */
package cactor_examples.ex28_actor_selection

import std.collection.*
import std.convert.*
import std.sync.*
import std.time.*
import stdx.net.http.*
import lou.*
import cactor.api.{CActorSystem, CActorSystemFactory}
import cactor_core.actor.{Actor, ActorContext}
import cactor_core.message.{Message, MessageResult, StringMessage}
import cactor_foundation.network.{NetworkAddress, TransportProtocol}
import cactor_remote.*

let WAIT = Duration.second * 20

class Stats {
    let lock = Mutex()
    let counts = HashMap<String, Int64>()
    let ordered = HashMap<String, Bool>()
    let last = HashMap<String, String>()

    func onMessage(id: String, content: String): Unit {
        synchronized(lock) {
            let n = counts.get(id) ?? 0
            // 自检消息用 t<序号> 验证按序；HTTP 端点的 b<序号> 不计入顺序判定
            if (content.startsWith("t")) {
                ordered[id] = (ordered.get(id) ?? true) && (content == "t${n}")
            }
            counts[id] = n + 1
            last[id] = content
        }
    }

    func count(id: String): Int64 {
        synchronized(lock) {
            counts.get(id) ?? 0
        }
    }

    func isOrdered(id: String): Bool {
        synchronized(lock) {
            ordered.get(id) ?? false
        }
    }

    func lastOf(id: String): String {
        synchronized(lock) {
            last.get(id) ?? ""
        }
    }

    func json(): String {
        synchronized(lock) {
            let sb = StringBuilder()
            sb.append("{\"actors\":{")
            var first = true
            for ((id, n) in counts) {
                if (!first) {
                    sb.append(",")
                }
                first = false
                sb.append("\"${id}\":{\"count\":${n},\"ordered\":${ordered.get(id) ?? false},\"last\":\"${last.get(id) ?? ""}\"}")
            }
            sb.append("}}")
            sb.toString()
        }
    }
}

class Worker <: Actor {
    let id: String
    let stats: Stats

    init(id: String, stats: Stats) {
        this.id = id
        this.stats = stats
    }

    public prop name: String { get() { "selection-worker" } }
    public prop description: String { get() { "selection target" } }

    public func receive(message: Message, context: ActorContext): MessageResult {
        if (let Some(s) <- (message as StringMessage)) {
            stats.onMessage(id, s.getContent())
        }
        MessageResult.Handled
    }
}

class Node {
    let index: Int64
    let system: CActorSystem
    let remote: SimpleRemoteManager
    var stopped = false

    init(index: Int64) {
        this.index = index
        this.system = CActorSystemFactory.create("ex28-node-${index}")
        this.remote = SimpleRemoteManager(NetworkAddress("127.0.0.1", 0), TransportProtocol.TCP)
        this.remote.start()
    }

    func shutdown(): Unit {
        if (!stopped) {
            stopped = true
            remote.stop()
            system.terminate()
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

func namesJson(names: Array<String>): String {
    let sb = StringBuilder()
    sb.append("{\"matched\":${names.size},\"names\":[")
    var first = true
    for (n in names) {
        if (!first) {
            sb.append(",")
        }
        first = false
        sb.append("\"${n}\"")
    }
    sb.append("]}")
    sb.toString()
}

func buildApp(nodeA: Node, nodeB: Node, stats: Stats, port: UInt16): Lou {
    let app = Lou([withHost("127.0.0.1"), withPort(port), withHideBanner(), withHideRoutes()])
    let target = nodeB.remote.getLocalAddress()

    app.get("/workers", { ctx =>
        let pattern = ctx.query("pattern") ?? "worker-*"
        let sel = nodeA.remote.actorSelection(target, pattern)
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(namesJson(sel.resolveNames()))
    })

    app.post("/broadcast", { ctx =>
        let pattern = ctx.query("pattern") ?? "worker-*"
        let n = ctx.queryInt("n") ?? 5
        let sel = nodeA.remote.actorSelection(target, pattern)
        let names = sel.resolveNames()
        let before = HashMap<String, Int64>()
        for (name in names) {
            before[name] = stats.count(name)
        }
        for (i in 0..n) {
            sel.tell(StringMessage("b${i}"))
        }
        var ok = true
        for (name in names) {
            let expect = (before.get(name) ?? 0) + n
            ok = ok && waitUntil(WAIT, { => stats.count(name) == expect })
        }
        ctx.setSingleHeader("Content-Type", "application/json")
        if (ok) {
            ctx.writeString("{\"pattern\":\"${pattern}\",\"matched\":${names.size},\"sent\":${n}}")
        } else {
            ctx.writeStringWithCode(500u16, "{\"error\":\"selection_incomplete\"}")
        }
    })

    app.post("/send", { ctx =>
        let name = ctx.query("name") ?? "counter"
        let msg = ctx.query("msg") ?? "ping"
        let sel = nodeA.remote.actorSelection(target, name)
        ctx.setSingleHeader("Content-Type", "application/json")
        try {
            let ref = sel.resolveOne()
            ref.tell(StringMessage(msg))
            ctx.writeString("{\"sent\":\"${name}\",\"msg\":\"${msg}\"}")
        } catch (e: IllegalStateException) {
            ctx.writeStringWithCode(409u16, "{\"error\":\"not_unique\"}")
        }
    })

    app.get("/stats", { ctx =>
        ctx.setSingleHeader("Content-Type", "application/json")
        ctx.writeString(stats.json())
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
    let stats = Stats()
    let nodeA = Node(0)
    let nodeB = Node(1)

    let workerNames = ["worker-1", "worker-2", "worker-3", "worker-10"]
    for (name in workerNames) {
        let ref = nodeB.system.actorOf({ => Worker(name, stats) }, name)
        nodeB.remote.exposeActor(name, ref)
    }
    let counter = nodeB.system.actorOf({ => Worker("counter", stats) }, "counter")
    nodeB.remote.exposeActor("counter", counter)

    let app = buildApp(nodeA, nodeB, stats, 0u16)

    if (args.size > 0 && args[0] == "serve") {
        let port = if (args.size > 1) { UInt16.parse(args[1]) } else { 8080u16 }
        let real = buildApp(nodeA, nodeB, stats, port)
        let actual = startServer(real)
        if (actual == 0u16) {
            println("ex28：端口绑定失败")
            return 1
        }
        println("ex28 正在监听 http://127.0.0.1:${actual}（Ctrl-C 结束）")
        while (true) {
            sleep(Duration.second)
        }
        return 0
    }

    let c = Checker()
    let port = startServer(app)
    c.check(port != 0u16, "Lou 服务已绑定端口 ${port}")
    let target = nodeB.remote.getLocalAddress()

    try {
        // ---- 1. 通配投递 worker-* ----
        println("\n[1] worker-* 广播 10 条")
        let sel = nodeA.remote.actorSelection(target, "cactor://placeholder/user/worker-*")
        for (i in 0..10) {
            sel.tell(StringMessage("t${i}"))
        }
        var all10 = true
        for (name in workerNames) {
            all10 = all10 && waitUntil(WAIT, { => stats.count(name) == 10 })
        }
        c.check(all10, "4 个 worker 各收到 10 条")
        var ordered = true
        for (name in workerNames) {
            ordered = ordered && stats.isOrdered(name)
        }
        c.check(ordered, "每个 worker 内按序（t0…t9）")
        c.check(stats.count("counter") == 0, "counter 未被 worker-* 命中")

        // ---- 2. ? 单字节通配 ----
        println("\n[2] worker-? 只命中单字符后缀")
        let selOne = nodeA.remote.actorSelection(target, "/user/worker-?")
        for (i in 0..5) {
            selOne.tell(StringMessage("t${i}"))
        }
        c.check(waitUntil(WAIT, { => stats.count("worker-10") == 10 }), "worker-10 不匹配 worker-?")
        c.check(waitUntil(WAIT, { =>
            stats.count("worker-1") == 15 && stats.count("worker-2") == 15 && stats.count("worker-3") == 15
        }), "worker-1/2/3 各再收到 5 条")

        // ---- 3. 名称解析 ----
        println("\n[3] resolveNames 排序与内部入口隔离")
        let names = nodeA.remote.actorSelection(target, "worker-*").resolveNames()
        let expectNames = ["worker-1", "worker-10", "worker-2", "worker-3"]
        var sameNames = names.size == 4
        for (i in 0..names.size) {
            if (i >= expectNames.size || names[i] != expectNames[i]) {
                sameNames = false
            }
        }
        c.check(sameNames, "worker-* 解析为 ${names}（字典序）")
        let all = nodeA.remote.actorSelection(target, "*").resolveNames()
        var noInternal = true
        for (n in all) {
            if (n.startsWith("$")) {
                noInternal = false
            }
        }
        c.check(all.size == 5 && noInternal, "* 只返回 5 个业务名称，无内部入口")

        // ---- 4. resolveOne ----
        println("\n[4] resolveOne 唯一性")
        let counterRef = nodeA.remote.actorSelection(target, "counter").resolveOne()
        counterRef.tell(StringMessage("hello-selection"))
        c.check(waitUntil(WAIT, { => stats.lastOf("counter") == "hello-selection" }),
            "唯一匹配可投递到 counter")
        var multiThrew = false
        try {
            nodeA.remote.actorSelection(target, "worker-*").resolveOne()
        } catch (_: IllegalStateException) {
            multiThrew = true
        }
        c.check(multiThrew, "多匹配时 resolveOne 报错")
        var noneThrew = false
        try {
            nodeA.remote.actorSelection(target, "nobody-*").resolveOne()
        } catch (_: IllegalStateException) {
            noneThrew = true
        }
        c.check(noneThrew, "零匹配时 resolveOne 报错")

        // ---- 5. 无匹配计入死信 ----
        println("\n[5] 无匹配计入对端死信")
        let before = nodeB.remote.getReceiver().getUndeliverableCount()
        nodeA.remote.actorSelection(target, "nobody-*").tell(StringMessage("lost"))
        c.check(waitUntil(WAIT, { => nodeB.remote.getReceiver().getUndeliverableCount() > before }),
            "死信计数 +1（${before} → ${nodeB.remote.getReceiver().getUndeliverableCount()}）")

        // ---- 6. Lou ----
        println("\n[6] Lou 端点")
        let client = ClientBuilder().noProxy().readTimeout(Duration.second * 30).build()
        let workers = readBody(client.get("http://127.0.0.1:${port}/workers?pattern=worker-*"))
        c.check(workers.contains("\"matched\":4") && workers.contains("worker-10"),
            "GET /workers 返回 4 个名称")
        let broadcast = readBody(client.post("http://127.0.0.1:${port}/broadcast?pattern=worker-*&n=2", ""))
        c.check(broadcast.contains("\"matched\":4") && broadcast.contains("\"sent\":2"),
            "POST /broadcast 命中 4 个并发送 2 条：${broadcast}")
        let send = readBody(client.post("http://127.0.0.1:${port}/send?name=counter&msg=http-hi", ""))
        c.check(send.contains("\"sent\":\"counter\""), "POST /send 经 resolveOne 投递")
        let conflict = client.post("http://127.0.0.1:${port}/send?name=worker-*&msg=x", "")
        c.check(conflict.status == 409, "POST /send 多匹配返回 409")
        let st = readBody(client.get("http://127.0.0.1:${port}/stats"))
        c.check(st.contains("\"worker-1\"") && st.contains("\"ordered\":true"), "GET /stats 与内存一致")
        client.close()
    } finally {
        try {
            app.stop()
        } catch (_: Exception) {}
        nodeA.shutdown()
        nodeB.shutdown()
    }

    println("\nex28：" + if (c.failures == 0) { "通过" } else { "失败（${c.failures} 项）" })
    if (c.failures == 0) { 0 } else { 1 }
}
